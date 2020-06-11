import json
import os
import time
from asyncio import get_event_loop
from unittest.mock import Mock

import pytest
from jupyterhub.objects import Hub, Server
from jupyterhub.orm import Spawner
from kubernetes.client.models import (
    V1SecurityContext,
    V1Container,
    V1Capabilities,
    V1Pod,
)
from traitlets.config import Config

from kubespawner import KubeSpawner

from conftest import ExecError


class MockUser(Mock):
    name = 'fake'
    server = Server()

    @property
    def escaped_name(self):
        return self.name

    @property
    def url(self):
        return self.server.url

def test_deprecated_config():
    """Deprecated config is handled correctly"""
    with pytest.warns(DeprecationWarning):
        c = Config()
        # both set, non-deprecated wins
        c.KubeSpawner.singleuser_fs_gid = 5
        c.KubeSpawner.fs_gid = 10
        # only deprecated set, should still work
        c.KubeSpawner.hub_connect_ip = '10.0.1.1'
        c.KubeSpawner.singleuser_extra_pod_config = extra_pod_config = {"key": "value"}
        c.KubeSpawner.image_spec = 'abc:123'
        spawner = KubeSpawner(hub=Hub(), config=c, _mock=True)
        assert spawner.hub.connect_ip == '10.0.1.1'
        assert spawner.fs_gid == 10
        assert spawner.extra_pod_config == extra_pod_config
        # deprecated access gets the right values, too
        assert spawner.singleuser_fs_gid == spawner.fs_gid
        assert spawner.singleuser_extra_pod_config == spawner.extra_pod_config
        assert spawner.image == 'abc:123'


def test_deprecated_runtime_access():
    """Runtime access/modification of deprecated traits works"""
    spawner = KubeSpawner(_mock=True)
    spawner.singleuser_uid = 10
    assert spawner.uid == 10
    assert spawner.singleuser_uid == 10
    spawner.uid = 20
    assert spawner.uid == 20
    assert spawner.singleuser_uid == 20
    spawner.image_spec = 'abc:latest'
    assert spawner.image_spec == 'abc:latest'
    assert spawner.image == 'abc:latest'
    spawner.image = 'abc:123'
    assert spawner.image_spec == 'abc:123'
    assert spawner.image == 'abc:123'


def test_spawner_values():
    """Spawner values are set correctly"""
    spawner = KubeSpawner(_mock=True)

    def set_id(spawner):
        return 1

    spawner.uid = 10
    assert spawner.uid == 10
    spawner.uid = set_id
    assert spawner.uid == set_id
    spawner.uid = None
    assert spawner.uid == None

    spawner.gid = 20
    assert spawner.gid == 20
    spawner.gid = set_id
    assert spawner.gid == set_id
    spawner.gid = None
    assert spawner.gid == None

    spawner.fs_gid = 30
    assert spawner.fs_gid == 30
    spawner.fs_gid = set_id
    assert spawner.fs_gid == set_id
    spawner.fs_gid = None
    assert spawner.fs_gid == None


def check_up(url):
    """Check that a url responds with a non-error code"""
    from urllib import request
    # disable redirects (this would be easier if we ran exec in an image with requests)
    class NoRedirect(request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = request.build_opener(NoRedirect)
    try:
        u = opener.open(url)
    except request.HTTPError as e:
        if e.status >= 400:
            raise
        u = e
    print(u.status)


@pytest.mark.asyncio
async def test_spawn_start(kube_ns, kube_client, config, hub_pod, exec_python):
    print(hub_pod)
    spawner = KubeSpawner(
        hub=Hub(ip=hub_pod.status.pod_ip),
        user=MockUser(),
        config=config,
        api_token="abc123",
        oauth_client_id="unused",
    )
    # empty spawner isn't running
    status = await spawner.poll()
    # assert isinstance(status, int)

    pod_name = spawner.pod_name

    # start the spawner
    url = await spawner.start()

    # verify the pod exists
    pods = kube_client.list_namespaced_pod(kube_ns).items
    pod_names = [p.metadata.name for p in pods]
    assert pod_name in pod_names

    # pod should be running when start returns
    pod = kube_client.read_namespaced_pod(namespace=kube_ns, name=pod_name)
    assert pod.status.phase == "Running"

    # verify poll while running
    status = await spawner.poll()
    assert status is None

    # make sure spawn url is correct
    r = exec_python(check_up, {"url": url})
    assert r == "302"

    # stop the pod
    await spawner.stop()

    # verify pod is gone
    pods = kube_client.list_namespaced_pod(kube_ns).items
    pod_names = [p.metadata.name for p in pods]
    assert pod_name not in pod_names

    # verify exit status
    status = await spawner.poll()
    assert isinstance(status, int)


@pytest.mark.asyncio
async def test_spawn_internal_ssl(kube_ns, kube_client, ssl_app, hub_pod_ssl, config, exec_python):
    print(hub_pod_ssl)
    spawner = KubeSpawner(
        config=config,
        hub=Hub(proto="https", ip="{name}.{namespace}.svc.cluster.local"),
        user=MockUser(),
        api_token="abc123",
        oauth_client_id="unused",
        internal_ssl=True,
        internal_trust_bundles=ssl_app.internal_trust_bundles,
        internal_certs_location=ssl_app.internal_certs_location,
    )

    # initialize ssl config
    hub_paths = await spawner.create_certs()
    spawner.cert_paths = await spawner.move_certs(hub_paths)

    # start the spawner
    url = await spawner.start()
    pod_name = "jupyter-%s" % spawner.user.name
    # verify the pod exists
    pods = kube_client.list_namespaced_pod(kube_ns).items
    pod_names = [p.metadata.name for p in pods]
    assert pod_name in pod_names
    # verify poll while running
    status = await spawner.poll()
    assert status is None

    # verify service and secret exist
    secret_name = spawner.secret_name
    secrets = kube_client.list_namespaced_secret(kube_ns).items
    secret_names = [s.metadata.name for s in secrets]
    assert secret_name in secret_names

    service_name = pod_name
    services = kube_client.list_namespaced_service(kube_ns).items
    service_names = [s.metadata.name for s in services]
    assert service_name in service_names

    # make sure spawn url is correct
    r = exec_python(check_up, {"url": url})
    assert r == "302"

    # stop the pod
    await spawner.stop()

    # verify pod is gone
    pods = kube_client.list_namespaced_pod(kube_ns).items
    pod_names = [p.metadata.name for p in pods]
    assert "jupyter-%s" % spawner.user.name not in pod_names

    # verify service and secret are gone
    secrets = kube_client.list_namespaced_secret(kube_ns).items
    secret_names = [s.metadata.name for s in secrets]
    assert secret_name not in secret_names

    services = kube_client.list_namespaced_service(kube_ns).items
    service_names = [s.metadata.name for s in services]
    assert service_name not in service_names


@pytest.mark.asyncio
async def test_spawn_progress(kube_ns, kube_client, config):
    spawner = KubeSpawner(hub=Hub(), user=MockUser(name="progress"), config=config)
    # empty spawner isn't running
    status = await spawner.poll()
    assert isinstance(status, int)

    # start the spawner
    start_future = spawner.start()
    # check progress events
    messages = []
    async for progress in spawner.progress():
        assert 'progress' in progress
        assert isinstance(progress['progress'], int)
        assert 'message' in progress
        assert isinstance(progress['message'], str)
        messages.append(progress['message'])

        # ensure we can serialize whatever we return
        with open(os.devnull, "w") as devnull:
            json.dump(progress, devnull)
    assert 'Started container' in '\n'.join(messages)

    await start_future
    # stop the pod
    await spawner.stop()


def test_get_pod_manifest_tolerates_mixed_input():
    """
    Test that the get_pod_manifest function can handle a either a dictionary or
    an object both representing V1Container objects and that the function
    returns a V1Pod object containing V1Container objects.
    """
    c = Config()

    dict_model = {
        'name': 'mock_name_1',
        'image': 'mock_image_1',
        'command': ['mock_command_1']
    }
    object_model = V1Container(
        name="mock_name_2",
        image="mock_image_2",
        command=['mock_command_2'],
        security_context=V1SecurityContext(
            privileged=True,
            run_as_user=0,
            capabilities=V1Capabilities(add=['NET_ADMIN'])
        )
    )
    c.KubeSpawner.init_containers = [dict_model, object_model]

    spawner = KubeSpawner(config=c, _mock=True)

    # this test ensures the following line doesn't raise an error
    manifest = sync_wait(spawner.get_pod_manifest())

    # and tests the return value's types
    assert isinstance(manifest, V1Pod)
    assert isinstance(manifest.spec.init_containers[0], V1Container)
    assert isinstance(manifest.spec.init_containers[1], V1Container)


_test_profiles = [
    {
        'display_name': 'Training Env - Python',
        'slug': 'training-python',
        'default': True,
        'kubespawner_override': {
            'image': 'training/python:label',
            'cpu_limit': 1,
            'mem_limit': 512 * 1024 * 1024,
            }
    },
    {
        'display_name': 'Training Env - Datascience',
        'slug': 'training-datascience',
        'kubespawner_override': {
            'image': 'training/datascience:label',
            'cpu_limit': 4,
            'mem_limit': 8 * 1024 * 1024 * 1024,
            }
    },
]


@pytest.mark.asyncio
async def test_user_options_set_from_form():
    spawner = KubeSpawner(_mock=True)
    spawner.profile_list = _test_profiles
    # render the form
    await spawner.get_options_form()
    spawner.user_options = spawner.options_from_form({'profile': [_test_profiles[1]['slug']]})
    assert spawner.user_options == {
        'profile': _test_profiles[1]['slug'],
    }
    # nothing should be loaded yet
    assert spawner.cpu_limit is None
    await spawner.load_user_options()
    for key, value in _test_profiles[1]['kubespawner_override'].items():
        assert getattr(spawner, key) == value


@pytest.mark.asyncio
async def test_user_options_api():
    spawner = KubeSpawner(_mock=True)
    spawner.profile_list = _test_profiles
    # set user_options directly (e.g. via api)
    spawner.user_options = {'profile': _test_profiles[1]['slug']}

    # nothing should be loaded yet
    assert spawner.cpu_limit is None
    await spawner.load_user_options()
    for key, value in _test_profiles[1]['kubespawner_override'].items():
        assert getattr(spawner, key) == value


@pytest.mark.asyncio
async def test_default_profile():
    spawner = KubeSpawner(_mock=True)
    spawner.profile_list = _test_profiles
    spawner.user_options = {}
    # nothing should be loaded yet
    assert spawner.cpu_limit is None
    await spawner.load_user_options()
    for key, value in _test_profiles[0]['kubespawner_override'].items():
        assert getattr(spawner, key) == value


def test_pod_name_no_named_servers():
    c = Config()
    c.JupyterHub.allow_named_servers = False

    user = Config()
    user.name = "user"

    orm_spawner = Spawner()

    spawner = KubeSpawner(config=c, user=user, orm_spawner=orm_spawner, _mock=True)

    assert spawner.pod_name == "jupyter-user"


def test_pod_name_named_servers():
    c = Config()
    c.JupyterHub.allow_named_servers = True

    user = Config()
    user.name = "user"

    orm_spawner = Spawner()
    orm_spawner.name = "server"

    spawner = KubeSpawner(config=c, user=user, orm_spawner=orm_spawner, _mock=True)

    assert spawner.pod_name == "jupyter-user-server"


def test_pod_name_escaping():
    c = Config()
    c.JupyterHub.allow_named_servers = True

    user = Config()
    user.name = "some_user"

    orm_spawner = Spawner()
    orm_spawner.name = "test-server!"

    spawner = KubeSpawner(config=c, user=user, orm_spawner=orm_spawner, _mock=True)

    assert spawner.pod_name == "jupyter-some-5fuser-test-2dserver-21"


def test_pod_name_custom_template():
    c = Config()
    c.JupyterHub.allow_named_servers = False

    user = Config()
    user.name = "some_user"

    pod_name_template = "prefix-{username}-suffix"

    spawner = KubeSpawner(config=c, user=user, pod_name_template=pod_name_template, _mock=True)

    assert spawner.pod_name == "prefix-some-5fuser-suffix"
