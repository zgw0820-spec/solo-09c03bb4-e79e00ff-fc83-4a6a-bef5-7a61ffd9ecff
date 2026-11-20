"""HTTP-level tests.

Two layers are covered:

1. The handover API with an injected in-memory store (fast, no network).
2. The full stack -- real ``kubernetes`` Python client -> HTTP -> mock API
   server -- which is what Docker Compose runs. This proves the client path
   (merge patches, Status errors, 404/409 translation) works against a server
   that speaks the real API's wire format.
"""

from __future__ import annotations

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import pytest
from fastapi.testclient import TestClient

from app.kube import KubernetesClient
from app.main import create_app as create_handover_app
from mock_kube.app import create_app as create_mock_app
from mock_kube.store import InMemoryKube
from tests.conftest import (
    NEW_UID,
    OLD_UID,
    OTHER_UID,
    PV_NAME,
    pv_fixture,
    pvc_fixture,
)

BODY = {
    "pv_name": PV_NAME,
    "old_pvc_uid": OLD_UID,
    "new_pvc": {"namespace": "monitoring", "name": "prometheus-data-new", "uid": NEW_UID},
}


# ------------------------------------------------------------- API with mock store
@pytest.fixture
def api_client() -> TestClient:
    store = InMemoryKube()
    store.create_pv(pv_fixture())
    store.create_pvc("monitoring", pvc_fixture())
    app = create_handover_app(kube=store)
    with TestClient(app) as client:
        client.store = store  # type: ignore[attr-defined]
        yield client


def test_openapi_and_docs_served(api_client):
    assert api_client.get("/openapi.json").status_code == 200
    assert api_client.get("/docs").status_code == 200
    schema = api_client.get("/openapi.json").json()
    assert "/handovers" in schema["paths"]
    assert "post" in schema["paths"]["/handovers"]


def test_post_returns_202_pending_then_200_completed(api_client):
    first = api_client.post("/handovers", json=BODY)
    assert first.status_code == 202
    assert first.json()["status"] == "pending_binding"

    api_client.store.bind_pv(PV_NAME)

    second = api_client.post("/handovers", json=BODY)
    assert second.status_code == 200
    assert second.json()["status"] == "completed"

    third = api_client.post("/handovers", json=BODY)
    assert third.status_code == 200
    assert third.json()["status"] == "completed"


def test_status_endpoint_is_read_only(api_client):
    api_client.post("/handovers", json=BODY)
    response = api_client.get(
        "/handovers/status",
        params={
            "pv_name": PV_NAME,
            "old_pvc_uid": OLD_UID,
            "namespace": "monitoring",
            "name": "prometheus-data-new",
            "uid": NEW_UID,
        },
    )
    assert response.status_code == 200
    assert response.json()["status"] == "pending_binding"


def test_rejection_body_shape(api_client):
    api_client.store.delete_pv(PV_NAME)
    api_client.store.create_pv(pv_fixture(reclaim_policy="Delete"))
    response = api_client.post("/handovers", json=BODY)
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "reclaim_policy_not_retain"
    assert error["details"]["reclaim_policy"] == "Delete"


def test_validation_errors_are_422(api_client):
    response = api_client.post("/handovers", json={"pv_name": PV_NAME})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"


def test_unknown_field_is_rejected(api_client):
    response = api_client.post("/handovers", json={**BODY, "force": True})
    assert response.status_code == 422


def test_not_found_pv_is_404(api_client):
    response = api_client.post("/handovers", json={**BODY, "pv_name": "missing"})
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "pv_not_found"


def test_healthz(api_client):
    response = api_client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


# -------------------------------------------------- full stack over real HTTP
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def live_servers():
    """Run the mock API server on a random port, as Compose does."""
    import uvicorn

    port = _free_port()
    store = InMemoryKube()
    store.create_pv(pv_fixture())
    store.create_pvc("monitoring", pvc_fixture())
    server_app = create_mock_app(store=store)
    config = uvicorn.Config(server_app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.02)
    if not server.started:
        raise RuntimeError("mock API server failed to start")
    yield store, f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def wire_client(live_servers) -> KubernetesClient:
    store, base_url = live_servers
    store.reset()
    store.create_pv(pv_fixture())
    store.create_pvc("monitoring", pvc_fixture())
    from kubernetes import client as k8s_client

    configuration = k8s_client.Configuration()
    configuration.host = base_url
    configuration.api_key = {}
    configuration.api_key_prefix = {}
    api = k8s_client.CoreV1Api(k8s_client.ApiClient(configuration))
    return KubernetesClient(core=api)


def test_wire_client_end_to_end_handover(wire_client, live_servers):
    store, base_url = live_servers
    app = create_handover_app(kube=wire_client)
    with TestClient(app) as client:
        # Before the binder runs the volume is Reserved-but-not-Bound.
        first = client.post("/handovers", json=BODY)
        assert first.status_code == 202, first.text
        assert first.json()["status"] == "pending_binding"

        # Binder runs out-of-band, exactly like the PV controller would.
        urllib.request.urlopen(
            urllib.request.Request(f"{base_url}/_mock/bind/{PV_NAME}", method="POST")
        ).read()

        second = client.post("/handovers", json=BODY)
        assert second.status_code == 200, second.text
        assert second.json()["status"] == "completed"

    snapshot = json.loads(urllib.request.urlopen(f"{base_url}/_mock/state").read())
    pv = next(item for item in snapshot["persistentVolumes"] if item["metadata"]["name"] == PV_NAME)
    pvc = next(
        item for item in snapshot["persistentVolumeClaims"]
        if item["metadata"]["name"] == "prometheus-data-new"
    )
    assert pv["status"]["phase"] == "Bound"
    assert pv["spec"]["claimRef"]["uid"] == pvc["metadata"]["uid"]
    assert pvc["spec"]["volumeName"] == PV_NAME


def test_wire_client_sees_conflict_on_competing_claim(wire_client, live_servers):
    store, base_url = live_servers
    app = create_handover_app(kube=wire_client)
    with TestClient(app) as client:
        assert client.post("/handovers", json=BODY).status_code == 202

    other_app = create_handover_app(kube=wire_client)
    other_body = {
        **BODY,
        "new_pvc": {"namespace": "monitoring", "name": "rival", "uid": OTHER_UID},
    }
    with TestClient(other_app) as client:
        response = client.post("/handovers", json=other_body)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "pv_reserved_for_other_claim"

    snapshot = json.loads(urllib.request.urlopen(f"{base_url}/_mock/state").read())
    pv = next(item for item in snapshot["persistentVolumes"] if item["metadata"]["name"] == PV_NAME)
    assert pv["spec"]["claimRef"]["uid"] == NEW_UID


def test_wire_client_rejections_do_not_write(wire_client, live_servers):
    """Every rejection must surface as a 409 with no reservation written."""
    store, _ = live_servers
    rv_before = store.get_pv(PV_NAME)["metadata"]["resourceVersion"]
    app = create_handover_app(kube=wire_client)

    cases = [
        ({**BODY, "old_pvc_uid": OTHER_UID}, "pv_reserved_for_other_claim"),
        ({**BODY, "new_pvc": {**BODY["new_pvc"], "uid": OTHER_UID}}, "new_pvc_uid_mismatch"),
    ]
    with TestClient(app) as client:
        for body, expected in cases:
            response = client.post("/handovers", json=body)
            assert response.status_code == 409, (body, response.text)
            assert response.json()["error"]["code"] == expected

    assert store.get_pv(PV_NAME)["metadata"]["resourceVersion"] == rv_before


def test_wire_client_translates_api_server_errors(wire_client):
    app = create_handover_app(kube=wire_client)
    with TestClient(app) as client:
        response = client.post("/handovers", json={**BODY, "pv_name": "does-not-exist"})
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "pv_not_found"


def test_binder_refuses_incompatible_pair(wire_client, live_servers):
    """The mock server's binder enforces the same rules, so a bad pre-bind
    cannot be forced through behind the service's back."""
    store, base_url = live_servers
    store.delete_pvc("monitoring", "prometheus-data-new")
    store.create_pvc("monitoring", pvc_fixture(capacity="500Gi"))
    app = create_handover_app(kube=wire_client)
    with TestClient(app) as client:
        response = client.post("/handovers", json=BODY)
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "volume_incompatible"

    # Even if the reservation is forced by hand, binding still fails.
    store.update_pv(PV_NAME, {
        "metadata": {"resourceVersion": store.get_pv(PV_NAME)["metadata"]["resourceVersion"]},
        "spec": {"claimRef": {"namespace": "monitoring", "name": "prometheus-data-new",
                             "uid": NEW_UID}},
    })
    with pytest.raises(Exception):
        store.bind_pv(PV_NAME)
