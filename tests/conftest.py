"""pytest fixtures for kubespawner"""

import base64
import inspect
import io
import os
import sys
import tarfile
import time

import pytest
from certipy import Certipy, CertNotFoundError
from kubernetes.client import V1Namespace, V1ConfigMap, V1Pod, V1PodSpec, V1Secret
from kubernetes.config import load_kube_config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from traitlets.config import Config

from jupyterhub.app import JupyterHub
from kubespawner.clients import shared_client

here = os.path.abspath(os.path.dirname(__file__))
jupyterhub_config_py = os.path.join(here, "jupyterhub_config.py")
jupyterhub_ssl_config_py = os.path.join(here, "jupyterhub_ssl_config.py")


@pytest.fixture(scope="session")
def kube_ns():
    """Fixture for the kubernetes namespace"""
    return os.environ.get("KUBESPAWNER_TEST_NAMESPACE") or "kubespawner-test"


@pytest.fixture
def config(kube_ns):
    """Return a traitlets Config object

    The base configuration for testing.
    Use when constructing Spawners for tests
    """
    cfg = Config()
    cfg.KubeSpawner.namespace = kube_ns
    cfg.KubeSpawner.cmd = ["jupyterhub-singleuser"]
    return cfg


@pytest.fixture
def app(config, tmpdir):
    tmpdir.chdir()
    app = JupyterHub(config=config)
    return app


@pytest.fixture(scope="session")
def ssl_app(tmpdir_factory):
    tmpdir = tmpdir_factory.mktemp("ssl")
    tmpdir.chdir()
    config = Config()
    config.JupyterHub.internal_ssl = True
    tmpdir.mkdir("internal-ssl")
    # use relative
    config.JupyterHub.internal_certs_location = "internal-ssl"
    app = JupyterHub(config=config)
    app.init_internal_ssl()
    return app


@pytest.fixture(scope="session")
def kube_client(request, kube_ns):
    """fixture for the Kubernetes client object.

    skips test that require kubernetes if kubernetes cannot be contacted
    """
    load_kube_config()
    client = shared_client("CoreV1Api")
    try:
        namespaces = client.list_namespace(_request_timeout=3)
    except Exception as e:
        pytest.skip("Kubernetes not found: %s" % e)

    if not any(ns.metadata.name == kube_ns for ns in namespaces.items):
        print("Creating namespace %s" % kube_ns)
        client.create_namespace(V1Namespace(metadata=dict(name=kube_ns)))
    else:
        print("Using existing namespace %s" % kube_ns)
    # delete the test namespace when we finish

    def cleanup_namespace():
        client.delete_namespace(kube_ns, body={}, grace_period_seconds=0)
        for i in range(3):
            try:
                ns = client.read_namespace(kube_ns)
            except ApiException as e:
                if e.status == 404:
                    return
                else:
                    raise
            else:
                print("waiting for %s to delete" % kube_ns)
                time.sleep(1)

    # request.addfinalizer(cleanup_namespace)
    return client


def wait_for_pod(kube_client, kube_ns, pod_name, timeout=30):
    """Wait for a pod to be running"""
    for i in range(int(timeout)):
        pod = kube_client.read_namespaced_pod(namespace=kube_ns, name=pod_name)
        if pod.status.phase == "Pending":
            print(
                "Waiting for pod {ns}/{pod_name}; current status: {status}".format(
                    ns=kube_ns, pod_name=pod_name, status=pod.status.phase
                )
            )
            time.sleep(1)
        else:
            break

    if pod.status.phase != "Running":
        raise TimeoutError(
            "pod {ns}/{pod_name} failed to start: {status}".format(
                ns=kube_ns, pod_name=pod_name, status=pod.status
            )
        )
    return pod


def ensure_not_exists(kube_client, kube_ns, name, resource_type, timeout=30):
    """Ensure an object doesn't exist

    Request deletion and wait for it to be gone
    """
    delete = getattr(kube_client, "delete_namespaced_{}".format(resource_type))
    read = getattr(kube_client, "read_namespaced_{}".format(resource_type))
    try:
        delete(namespace=kube_ns, name=name)
    except ApiException as e:
        if e.status != 404:
            raise

    while True:
        # wait for delete
        try:
            read(namespace=kube_ns, name=name)
        except ApiException as e:
            if e.status == 404:
                # deleted
                break
            else:
                raise
        else:
            print("waiting for {}/{} to delete".format(resource_type, name))
            time.sleep(1)


def create_hub_pod(kube_client, kube_ns, pod_name="hub", ssl=False):
    config_map_name = pod_name + "-config"
    secret_name = pod_name + "-secret"
    with open(jupyterhub_config_py) as f:
        config = f.read()

    print("Creating hub config map")
    ensure_not_exists(kube_client, kube_ns, config_map_name, "config_map")

    config_map_manifest = V1ConfigMap(
        metadata={"name": config_map_name}, data={"jupyterhub_config.py": config}
    )

    config_map = kube_client.create_namespaced_config_map(
        body=config_map_manifest, namespace=kube_ns,
    )

    print("Creating hub pod {}".format(pod_name))
    ensure_not_exists(kube_client, kube_ns, pod_name, "pod")

    volumes = [{"name": "config", "configMap": {"name": config_map_name}}]
    volume_mounts = [
        {
            "mountPath": "/etc/jupyterhub/jupyterhub_config.py",
            "subPath": "jupyterhub_config.py",
            "name": "config",
        }
    ]
    if ssl:
        volumes.append({"name": "secret", "secret": {"secretName": secret_name}})
        volume_mounts.append(
            {"mountPath": "/etc/jupyterhub/secret", "name": "secret",}
        )

    pod_manifest = V1Pod(
        metadata={"name": pod_name},
        spec=V1PodSpec(
            volumes=volumes,
            containers=[
                {
                    "image": "jupyterhub/jupyterhub:1.1",
                    "name": "hub",
                    "volumeMounts": volume_mounts,
                    "args": [
                        "jupyterhub",
                        "-f",
                        "/etc/jupyterhub/jupyterhub_config.py",
                    ],
                    "env": [{"name": "PYTHONUNBUFFERED", "value": "1"}],
                }
            ],
            termination_grace_period_seconds=0,
        ),
    )
    pod = kube_client.create_namespaced_pod(body=pod_manifest, namespace=kube_ns)
    return wait_for_pod(kube_client, kube_ns, pod_name)


@pytest.fixture(scope="session")
def hub_pod(kube_client, kube_ns):
    return create_hub_pod(kube_client, kube_ns)


@pytest.fixture(scope="session")
def hub_pod_ssl(kube_client, kube_ns, ssl_app):
    with open(jupyterhub_ssl_config_py) as f:
        ssl_config = f.read()

    # load ssl dir to tarfile
    buf = io.BytesIO()
    tf = tarfile.TarFile(fileobj=buf, mode="w")
    tf.add(ssl_app.internal_certs_location, arcname="internal-ssl", recursive=True)

    # store tarfile in a secret
    b64_certs = base64.b64encode(buf.getvalue()).decode("ascii")
    print("Creating hub ssl secret")
    secret_name = "hub-ssl-secret"
    ensure_not_exists(kube_client, kube_ns, secret_name, "secret")
    secret_manifest = V1Secret(
        metadata={"name": secret_name}, data={"internal-ssl.tar": b64_certs}
    )

    kube_client.create_namespaced_secret(body=secret_manifest, namespace=kube_ns)
    return create_hub_pod(kube_client, kube_ns, pod_name="hub-ssl", ssl=True,)


class ExecError(Exception):
    """Error raised when a kubectl exec fails"""

    def __init__(self, exit_code, message="", command="exec"):
        self.exit_code = exit_code
        self.message = message
        self.command = command

    def __str__(self):
        return "{command} exited with status {exit_code}: {message}".format(
            command=self.command, exit_code=self.exit_code, message=self.message,
        )


@pytest.fixture(scope="session")
def exec_python(kube_ns, kube_client):
    """Return a callable to execute Python code"""
    pod_name = "kubespawner-test-exec"
    pod_manifest = V1Pod(
        metadata={"name": pod_name},
        spec=V1PodSpec(
            containers=[
                {
                    "image": "python:3.8",
                    "name": "python",
                    "args": ["/bin/sh", "-c", "while true; do sleep 5; done"],
                }
            ],
            termination_grace_period_seconds=0,
        ),
    )
    print("Creating exec pod", pod_name)
    pod_error = None
    for i in range(10):
        try:
            kube_client.create_namespaced_pod(
                body=pod_manifest, namespace=kube_ns,
            )
        except ApiException as e:
            if e.status == 409:
                break
            pod_error = e
            # need to retry since this can fail if run too soon after namespace creation
            print(e, file=sys.stderr)
            time.sleep(int(e.headers.get("Retry-After", 1)))
        else:
            break
    else:
        raise pod_error

    pod = wait_for_pod(kube_client, kube_ns, pod_name)

    def exec_python_code(code, kwargs=None):
        if not isinstance(code, str):
            # allow simple self-contained (no globals or args) functions
            func = code
            code = "\n".join(
                [
                    inspect.getsource(func),
                    "_kw = %r" % (kwargs or {}),
                    "{}(**_kw)".format(func.__name__),
                    "",
                ]
            )
        elif kwargs:
            raise ValueError(
                "kwargs can only be passed to functions, not code strings."
            )

        exec_command = [
            "python3",
            "-c",
            code,
        ]
        print("Running {} in {}".format(code, pod_name))
        # need to create ws client to get returncode,
        # see https://github.com/kubernetes-client/python/issues/812
        client = stream(
            kube_client.connect_get_namespaced_pod_exec,
            pod_name,
            namespace=kube_ns,
            command=exec_command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )
        client.run_forever(timeout=60)

        # let pytest capture stderr
        stderr = client.read_stderr()
        print(stderr, file=sys.stderr)

        returncode = client.returncode
        if returncode:
            print(client.read_stdout())
            raise ExecError(exit_code=returncode, message=stderr, command=code)
        else:
            return client.read_stdout().rstrip()

    yield exec_python_code
