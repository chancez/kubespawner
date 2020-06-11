"""pytest fixtures for kubespawner"""

import inspect
import os
import sys
import time

import pytest
from certipy import Certipy, CertNotFoundError
from kubernetes.client import V1Namespace
from kubernetes.config import load_kube_config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from traitlets.config import Config

from jupyterhub.app import JupyterHub
from kubespawner.clients import shared_client


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
def ssl_hub(config, tmpdir):
    config.JupyterHub.internal_ssl = True
    config.JupyterHub.internal_certs_location = str(tmpdir.mkdir("internal-ssl"))
    hub = JupyterHub(config=config)
    hub.init_internal_ssl()
    return hub


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
    # request.addfinalizer(lambda: client.delete_namespace(kube_ns, body={}))
    return client


class ExecError(Exception):
    """Error raised when a kubectl exec fails"""
    def __init__(self, exit_code, message="", command="exec"):
        self.exit_code = exit_code
        self.message = message
        self.command = command

    def __str__(self):
        return "{command} exited with status {exit_code}: {message}".format(
            command=self.command,
            exit_code=self.exit_code,
            message=self.message,
        )



@pytest.fixture(scope="session")
def exec_python(kube_ns, kube_client):
    """Return a callable to execute Python code"""
    pod_name = "kubespawner-test-exec"
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name},
        "spec": {
            "containers": [
                {
                    "image": "python:3.8",
                    "name": "python",
                    "args": ["/bin/sh", "-c", "while true; do sleep 5; done"],
                }
            ]
        },
    }
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

    for i in range(30):
        pod = kube_client.read_namespaced_pod(name=pod_name, namespace=kube_ns)
        if pod.status.phase != "Pending":
            break
        time.sleep(1)
    print("exec pod {} is {}".format(pod_name, pod.status.phase))
    if pod.status.phase != "Running":
        raise TimeoutError(
            "pod {ns}/{pod_name} failed to start: {status}".format(
                ns=kube_ns, pod_name=pod_name, status=pod.status
            )
        )

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
            raise ValueError("kwargs can only be passed to functions, not code strings.")

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
        stderr = client.read_stderr()
        print(stderr, file=sys.stderr)
        returncode = client.returncode
        if returncode:
            print(client.read_stdout())
            raise ExecError(exit_code=returncode, message=stderr, command=code)
        else:
            return client.read_stdout().rstrip()

    yield exec_python_code
