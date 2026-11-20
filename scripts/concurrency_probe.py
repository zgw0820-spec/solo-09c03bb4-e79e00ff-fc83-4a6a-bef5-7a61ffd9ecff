"""Ad-hoc concurrency exercise for the handover service.

Not part of the shipped test suite: this runs many simultaneous submissions
through the *HTTP* API against the mock API server to shake out races that a
deterministic unit test can miss. Run it with:

    python3 scripts/concurrency_probe.py
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

from mock_kube.app import create_app as create_mock_app
from mock_kube.store import InMemoryKube
from tests.conftest import NEW_UID, OLD_UID, OTHER_UID, PV_NAME, pv_fixture, pvc_fixture


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def post(base: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else b""
    request = urllib.request.Request(
        base + path, data=data, method="POST", headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def main() -> int:
    store = InMemoryKube()
    store.create_pv(pv_fixture())
    store.create_pvc("monitoring", pvc_fixture(name="claim-a", uid=NEW_UID))
    store.create_pvc("monitoring", pvc_fixture(name="claim-b", uid=OTHER_UID))

    mock_port = free_port()
    mock_server = uvicorn.Server(
        uvicorn.Config(create_mock_app(store=store), host="127.0.0.1", port=mock_port, log_level="warning")
    )
    threading.Thread(target=mock_server.run, daemon=True).start()
    while not mock_server.started:
        time.sleep(0.02)

    # Point the real handover app at the mock API server over the wire.
    import os

    os.environ["KUBE_MODE"] = "remote"
    os.environ["KUBECONFIG"] = _write_kubeconfig(mock_port)
    from app.main import create_app as create_handover_app

    app = create_handover_app()
    api_port = free_port()
    api_server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=api_port, log_level="warning")
    )
    threading.Thread(target=api_server.run, daemon=True).start()
    while not api_server.started:
        time.sleep(0.02)

    base = f"http://127.0.0.1:{api_port}"

    failures: list[str] = []
    for round_index in range(20):
        for claim, uid in (("claim-a", NEW_UID), ("claim-b", OTHER_UID)):
            store.delete_pvc("monitoring", claim)
        store.create_pvc("monitoring", pvc_fixture(name="claim-a", uid=NEW_UID))
        store.create_pvc("monitoring", pvc_fixture(name="claim-b", uid=OTHER_UID))
        store.update_pv(PV_NAME, {
            "metadata": {"resourceVersion": store.get_pv(PV_NAME)["metadata"]["resourceVersion"]},
            "spec": {"claimRef": {"namespace": "monitoring", "name": "old", "uid": OLD_UID}},
            "status": {"phase": "Released"},
        })

        results: list[tuple[str, int, str]] = []
        lock = threading.Lock()
        start = threading.Barrier(10)

        def attempt(index: int) -> None:
            claim, uid = ("claim-a", NEW_UID) if index % 2 == 0 else ("claim-b", OTHER_UID)
            body = {
                "pv_name": PV_NAME,
                "old_pvc_uid": OLD_UID,
                "new_pvc": {"namespace": "monitoring", "name": claim, "uid": uid},
            }
            start.wait()
            status, payload = post(base, "/handovers", body)
            with lock:
                results.append((claim, status, payload.get("error", {}).get("code") or payload.get("status")))

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        accepted = [r for r in results if r[1] in (200, 202)]
        rejected = [r for r in results if r[1] == 409]
        if len(accepted) == 0 or len(rejected) == 0:
            failures.append(f"round {round_index}: no contention happened ({results})")
            continue
        winning_claims = {r[0] for r in accepted}
        if len(winning_claims) != 1:
            failures.append(f"round {round_index}: more than one claimant won {results}")
        final_uid = store.get_pv(PV_NAME)["spec"]["claimRef"]["uid"]
        expected = NEW_UID if winning_claims == {"claim-a"} else OTHER_UID
        if final_uid != expected:
            failures.append(f"round {round_index}: reservation UID {final_uid} != {expected}")

    # Retry storm for a single claim: all must succeed and write only once.
    store.update_pv(PV_NAME, {
        "metadata": {"resourceVersion": store.get_pv(PV_NAME)["metadata"]["resourceVersion"]},
        "spec": {"claimRef": {"namespace": "monitoring", "name": "old", "uid": OLD_UID}},
        "status": {"phase": "Released"},
    })
    store.delete_pvc("monitoring", "claim-a")
    store.create_pvc("monitoring", pvc_fixture(name="claim-a", uid=NEW_UID))
    body = {
        "pv_name": PV_NAME,
        "old_pvc_uid": OLD_UID,
        "new_pvc": {"namespace": "monitoring", "name": "claim-a", "uid": NEW_UID},
    }
    retry_results: list[int] = []
    retry_lock = threading.Lock()
    gate = threading.Barrier(12)

    def retry() -> None:
        gate.wait()
        status, _ = post(base, "/handovers", body)
        with retry_lock:
            retry_results.append(status)

    threads = [threading.Thread(target=retry) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    bad = [status for status in retry_results if status not in (200, 202)]
    if bad:
        failures.append(f"retry storm produced unexpected statuses: {bad}")
    writes = sum(
        1
        for entry in store.patch_log
        if (entry["body"].get("spec") or {}).get("claimRef", {}).get("uid")
        in (NEW_UID, OTHER_UID)
    )
    if writes != 21:
        failures.append(f"expected 21 reservation writes (20 rounds + 1 retry storm), got {writes}")

    mock_server.should_exit = True
    api_server.should_exit = True
    time.sleep(0.2)

    if failures:
        print("FAILURES:")
        for failure in failures:
            print(" -", failure)
        return 1
    print("concurrency probe passed: 20 contention rounds + 12-way retry storm")
    return 0


def _write_kubeconfig(port: int) -> str:
    path = Path("/tmp/handover-probe-kubeconfig")
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
