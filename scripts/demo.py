"""End-to-end demo over HTTP.

Runs the mock Kubernetes API stand-in and the handover API in-process, then
walks the exact flow an on-call engineer follows in Swagger:

1. submit a handover          -> 202 pending_binding
2. repeated submit            -> 202, no extra writes
3. let the binder do its job  -> bind the PV
4. submit again               -> 200 completed
5. rebuild the new PVC        -> 409 new_pvc_uid_mismatch

Run with ``python3 scripts/demo.py``.
"""

from __future__ import annotations

import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import uvicorn

from app.main import create_app as create_handover_app
from mock_kube.app import create_app as create_mock_app
from mock_kube.seed import DEMO_BODIES as BODIES, seed
from mock_kube.store import InMemoryKube


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def call(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else {}


def run(server) -> None:
    threading.Thread(target=server.run, daemon=True).start()
    while not server.started:
        time.sleep(0.02)


def main() -> int:
    store = InMemoryKube()
    seed(store)

    mock_port, api_port = free_port(), free_port()
    run(uvicorn.Server(uvicorn.Config(
        create_mock_app(store=store), host="127.0.0.1", port=mock_port, log_level="warning"
    )))

    import os

    os.environ["KUBE_MODE"] = "remote"
    os.environ["KUBECONFIG"] = _write_kubeconfig(mock_port)
    run(uvicorn.Server(uvicorn.Config(
        create_handover_app(), host="127.0.0.1", port=api_port, log_level="warning"
    )))

    base = f"http://127.0.0.1:{api_port}"
    mock = f"http://127.0.0.1:{mock_port}"
    ok = True

    def show(label: str, status: int, payload: dict) -> None:
        summary = payload.get("status") or payload.get("error", {}).get("code")
        text = payload.get("status_text") or payload.get("error", {}).get("message")
        print(f"{label:<32} HTTP {status}  {summary}\n{'':<32}{text}")

    status, payload = call("POST", f"{base}/handovers", BODIES["handover"])
    show("1. submit", status, payload)
    ok &= status == 202 and payload["status"] == "pending_binding"

    status, payload = call("POST", f"{base}/handovers", BODIES["handover"])
    show("2. repeat submit (retry)", status, payload)
    ok &= status == 202 and payload["status"] == "pending_binding"

    call("POST", f"{mock}/_mock/bind/{BODIES['handover']['pv_name']}")

    status, payload = call("POST", f"{base}/handovers", BODIES["handover"])
    show("3. after binder ran", status, payload)
    ok &= status == 200 and payload["status"] == "completed"

    status, payload = call("POST", f"{base}/handovers", BODIES["rival"])
    show("4. rival claim", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "pv_reserved_for_other_claim"

    status, payload = call("POST", f"{base}/handovers", BODIES["delete_policy"])
    show("5. Delete reclaim policy", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "reclaim_policy_not_retain"

    status, payload = call("POST", f"{base}/handovers", BODIES["wrong_volume_mode"])
    show("6. volume mode mismatch", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "volume_incompatible"

    # Rebuild the new PVC under the same name: the old request must conflict.
    pvc = BODIES["handover"]["new_pvc"]
    call("DELETE", f"{mock}/_mock/namespaces/{pvc['namespace']}/pvcs/{pvc['name']}")
    call("POST", f"{mock}/_mock/namespaces/{pvc['namespace']}/pvcs", {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": pvc["name"], "namespace": pvc["namespace"]},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "150Gi"}},
            "storageClassName": "fast-ssd",
            "volumeName": BODIES["handover"]["pv_name"],
        },
    })
    status, payload = call("POST", f"{base}/handovers", BODIES["handover"])
    show("7. after same-name PVC rebuild", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "new_pvc_uid_mismatch"

    print("\nDEMO", "OK" if ok else "FAILED")
    return 0 if ok else 1


def _write_kubeconfig(port: int) -> str:
    path = Path("/tmp/handover-demo-kubeconfig")
    path.write_text(
        f"""apiVersion: v1
kind: Config
clusters:
- name: mock
  cluster:
    server: http://127.0.0.1:{port}
contexts:
- name: mock
  context:
    cluster: mock
    user: mock
current-context: mock
users:
- name: mock
  user: {{}}
"""
    )
    return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
