"""End-to-end demo over HTTP.

Runs the mock Kubernetes API stand-in and the handover API in-process, then
walks the exact flow an on-call engineer follows in Swagger:

1. submit a handover          -> 202 pending_binding
2. repeated submit            -> 202, no extra writes
3. reconcile round 1          -> binds every volume registered so far
4. submit again               -> 200 completed
5. unchanged round 2          -> zero attempts
6. change two volumes         -> round 3 only attempts those two
7. rebuild one PVC same-name  -> next round attempts it, binds nothing
8. rebuild the new PVC        -> 409 new_pvc_uid_mismatch

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
    headers = {"Content-Type": "application/json"}
    if "/_mock/" not in url and method == "PATCH":
        headers["Content-Type"] = "application/merge-patch+json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
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

    status, round1 = call("POST", f"{mock}/_mock/reconcile")
    print(f"{'3. reconcile round 1 (incremental)':<32} HTTP {status}  "
          f"registered={round1['registered']} attempted={len(round1['attempted'])} "
          f"bound={round1['bound']}")
    ok &= BODIES["handover"]["pv_name"] in round1["attempted"]
    ok &= round1["bound"] == [BODIES["handover"]["pv_name"]]

    status, payload = call("POST", f"{base}/handovers", BODIES["handover"])
    show("4. after binder ran", status, payload)
    ok &= status == 200 and payload["status"] == "completed"

    status, round2 = call("POST", f"{mock}/_mock/reconcile")
    print(f"{'5. reconcile round 2 (no changes)':<32} HTTP {status}  "
          f"registered={round2['registered']} attempted={len(round2['attempted'])} "
          f"bound={round2['bound']}")
    ok &= round2["attempted"] == []

    # Change two volumes only: give each seeded claim its ``volumeName`` via a
    # same-name recreate. The next round must attempt exactly those two.
    from mock_kube.seed import EXTRA_CLAIMS, EXTRA_NEW_UIDS, EXTRA_PVS

    first_pv, second_pv = EXTRA_PVS[0], EXTRA_PVS[1]
    first_claim, second_claim = EXTRA_CLAIMS[0], EXTRA_CLAIMS[1]
    first_uid, second_uid = EXTRA_NEW_UIDS[0], EXTRA_NEW_UIDS[1]
    for name, claim, uid in (
        (first_pv, first_claim, first_uid),
        (second_pv, second_claim, second_uid),
    ):
        call("DELETE", f"{mock}/_mock/namespaces/monitoring/pvcs/{claim}")
        call("POST", f"{mock}/_mock/namespaces/monitoring/pvcs",
             _pvc_body(claim, name, uid))

    status, state = call("GET", f"{mock}/_mock/reconcile/state")
    print(f"{'6. state before round 3':<32} HTTP {status}  registered={state['registered']}")
    ok &= sorted(state["registered"]) == sorted([first_pv, second_pv])

    status, round3 = call("POST", f"{mock}/_mock/reconcile")
    print(f"{'7. reconcile round 3 (2 changed)':<32} HTTP {status}  "
          f"registered={round3['registered']} attempted={len(round3['attempted'])} "
          f"bound={round3['bound']}")
    ok &= sorted(round3["attempted"]) == sorted([first_pv, second_pv])
    ok &= sorted(round3["bound"]) == sorted([first_pv, second_pv])

    # Another same-name PVC rebuild (new UID): only that claim's volume is
    # registered, it is attempted, and—because the reservation still names the
    # old UID—it binds nothing. The following round stays quiet.
    call("DELETE", f"{mock}/_mock/namespaces/monitoring/pvcs/{second_claim}")
    call("POST", f"{mock}/_mock/namespaces/monitoring/pvcs",
         _pvc_body(second_claim, second_pv, "c0ffee00-0000-4000-8000-000000000001"))
    status, round4 = call("POST", f"{mock}/_mock/reconcile")
    print(f"{'8. round 4 after claim rebuild':<32} HTTP {status}  "
          f"registered={round4['registered']} attempted={len(round4['attempted'])} "
          f"bound={round4['bound']}")
    ok &= round4["attempted"] == [second_pv]
    ok &= round4["bound"] == []

    status, round5 = call("POST", f"{mock}/_mock/reconcile")
    print(f"{'9. round 5 (no change)':<32} HTTP {status}  "
          f"registered={round5['registered']} attempted={len(round5['attempted'])}")
    ok &= round5["attempted"] == []

    status, payload = call("POST", f"{base}/handovers", BODIES["rival"])
    show("10. rival claim", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "pv_reserved_for_other_claim"

    status, payload = call("POST", f"{base}/handovers", BODIES["delete_policy"])
    show("11. Delete reclaim policy", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "reclaim_policy_not_retain"

    status, payload = call("POST", f"{base}/handovers", BODIES["wrong_volume_mode"])
    show("12. volume mode mismatch", status, payload)
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
    show("13. after same-name PVC rebuild", status, payload)
    ok &= status == 409 and payload["error"]["code"] == "new_pvc_uid_mismatch"

    print("\nDEMO", "OK" if ok else "FAILED")
    return 0 if ok else 1


def _pvc_body(
    name: str, volume_name: str, uid: str | None = None, namespace: str = "monitoring"
) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "150Gi"}},
            "storageClassName": "fast-ssd",
            "volumeName": volume_name,
        },
    }
    if uid is not None:
        body["metadata"]["uid"] = uid
    return body


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
