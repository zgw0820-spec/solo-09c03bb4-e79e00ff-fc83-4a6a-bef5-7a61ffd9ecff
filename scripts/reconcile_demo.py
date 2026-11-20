"""Drive incremental reconcile rounds against a running stack.

Intended for the Docker Compose setup (or the same two services started by
hand). It talks to the public HTTP surfaces only -- the handover API on port
8000 (the Swagger endpoints) and the mock Kubernetes operator API on 8001 -- so
it works whether the services run in containers or locally:

    docker compose up --build
    python3 scripts/reconcile_demo.py

Every fixture name carries a random run id, so the script can be run against a
seeded or an already-used mock without collisions. It:

1. creates and reserves 6 volumes through ``POST /handovers`` (Swagger flow),
   then runs round 1 -- all 6 are attempted and bound;
2. runs an unchanged round -- nothing is attempted;
3. touches 2 already-bound volumes -- the next round attempts only those 2;
4. creates and reserves 3 new volumes -- the next round attempts only those 3
   and binds them;
5. rebuilds one claim under the same name with a new UID -- the next round
   attempts just that volume and binds nothing, and the round after that is
   quiet again;
6. while rounds keep running, concurrently reads volumes and submits a
   handover, proving both stay available for the whole round.
"""

from __future__ import annotations

import argparse
import json
import threading
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass

NAMESPACE = "monitoring"


def call(method: str, url: str, body: dict | None = None,
         content_type: str = "application/json") -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": content_type}
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else {}


@dataclass
class Volume:
    pv: str
    old_uid: str
    claim: str
    new_uid: str


def build_volumes(run_id: str, prefix: str, count: int, offset: int = 0) -> list[Volume]:
    return [
        Volume(
            pv=f"pvc-{prefix}-{run_id}-{index:02d}",
            old_uid=str(uuid.uuid5(uuid.NAMESPACE_OID, f"old-{prefix}-{run_id}-{index}")),
            claim=f"demo-{prefix}-{run_id}-{index:02d}",
            new_uid=str(uuid.uuid5(uuid.NAMESPACE_OID, f"new-{prefix}-{run_id}-{index}")),
        )
        for index in range(offset, offset + count)
    ]


def pv_body(volume: Volume) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": volume.pv},
        "spec": {
            "capacity": {"storage": "100Gi"},
            "accessModes": ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": "Retain",
            "storageClassName": "fast-ssd",
            "volumeMode": "Filesystem",
            "claimRef": {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "namespace": NAMESPACE,
                "name": f"old-{volume.claim}",
                "uid": volume.old_uid,
            },
        },
        "status": {"phase": "Released"},
    }


def pvc_body(volume: Volume, *, uid: str | None = None) -> dict:
    body = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": volume.claim, "namespace": NAMESPACE,
                     "uid": uid or volume.new_uid},
        "spec": {
            "accessModes": ["ReadWriteOnce"],
            "resources": {"requests": {"storage": "80Gi"}},
            "storageClassName": "fast-ssd",
            "volumeMode": "Filesystem",
            "volumeName": volume.pv,
        },
    }
    return body


class Demo:
    def __init__(self, api: str, mock: str) -> None:
        self.api = api.rstrip("/")
        self.mock = mock.rstrip("/")
        self.ok = True

    # ---------------------------------------------------------------- helpers
    def check(self, condition: bool, message: str) -> None:
        if not condition:
            self.ok = False
            print(f"  !! {message}")

    def reconcile(self) -> dict:
        status, payload = call("POST", f"{self.mock}/_mock/reconcile")
        assert status == 200, (status, payload)
        return payload

    def state(self) -> dict:
        status, payload = call("GET", f"{self.mock}/_mock/reconcile/state")
        assert status == 200, (status, payload)
        return payload

    def create_and_reserve(self, volume: Volume) -> None:
        status, payload = call("POST", f"{self.mock}/_mock/pvs", pv_body(volume))
        assert status == 201, (status, payload)
        status, payload = call(
            "POST", f"{self.mock}/_mock/namespaces/{NAMESPACE}/pvcs", pvc_body(volume)
        )
        assert status == 201, (status, payload)
        status, payload = call("POST", f"{self.api}/handovers", {
            "pv_name": volume.pv,
            "old_pvc_uid": volume.old_uid,
            "new_pvc": {"namespace": NAMESPACE, "name": volume.claim,
                        "uid": volume.new_uid},
        })
        assert status == 202, (status, payload)
        assert payload["status"] == "pending_binding", payload

    def report(self, label: str, result: dict) -> None:
        print(f"{label:<40} round={result['round']:<3} "
              f"registered={result['registered']:<3} "
              f"attempted={len(result['attempted']):<3} "
              f"bound={len(result['bound']):<3} "
              f"deferred={len(result['deferred'])}")

    # ------------------------------------------------------------------- steps
    def run(self) -> int:
        run_id = uuid.uuid4().hex[:8]
        base = build_volumes(run_id, "base", 6)
        print(f"run id: {run_id}")

        # Start from a clean mock so the per-round attempt counts below are
        # exact. (Drop this call to run against the seeded scenario instead.)
        status, _ = call("POST", f"{self.mock}/_mock/reset")
        assert status == 200, status
        print("mock state reset\n")

        print("1. create 6 volumes and reserve them via POST /handovers")
        for volume in base:
            self.create_and_reserve(volume)
        pending = self.state()["registered"]
        self.check(sorted(pending) == sorted(v.pv for v in base),
                   f"expected all 6 new volumes registered, got {pending}")
        print(f"   registered before round 1: {len(pending)}")

        round1 = self.reconcile()
        self.report("   round 1 (6 volumes registered)", round1)
        self.check(sorted(round1["attempted"]) == sorted(v.pv for v in base),
                   "round 1 should attempt exactly the 6 registered volumes")
        self.check(sorted(round1["bound"]) == sorted(v.pv for v in base),
                   "round 1 should bind all 6 volumes")

        round2 = self.reconcile()
        self.report("   round 2 (nothing changed)", round2)
        self.check(round2["attempted"] == [], "unchanged round must attempt nothing")

        print("\n2. touch 2 already-bound volumes (merge-patch annotations)")
        touched = base[:2]
        for volume in touched:
            status, payload = call(
                "PATCH", f"{self.mock}/api/v1/persistentvolumes/{volume.pv}",
                {"metadata": {"annotations": {"demo.example.com/touched": run_id}}},
                content_type="application/merge-patch+json",
            )
            assert status == 200, (status, payload)
        state = self.state()
        self.check(sorted(state["registered"]) == sorted(v.pv for v in touched),
                   f"only the 2 touched volumes should be registered, got {state['registered']}")
        round3 = self.reconcile()
        self.report("   round 3 (2 volumes changed)", round3)
        self.check(sorted(round3["attempted"]) == sorted(v.pv for v in touched),
                   "round 3 should attempt only the 2 changed volumes")
        self.check(round3["bound"] == [], "already-bound volumes must not bind again")

        print("\n3. create and reserve 3 new volumes")
        extra = build_volumes(run_id, "extra", 3, offset=6)
        for volume in extra:
            self.create_and_reserve(volume)
        round4 = self.reconcile()
        self.report("   round 4 (3 new volumes)", round4)
        self.check(sorted(round4["attempted"]) == sorted(v.pv for v in extra),
                   "round 4 should attempt only the 3 new volumes")
        self.check(sorted(round4["bound"]) == sorted(v.pv for v in extra),
                   "round 4 should bind the 3 new volumes")

        print("\n4. rebuild one claim under the same name with a new UID")
        rebuilt = extra[-1]
        status, _ = call(
            "DELETE", f"{self.mock}/_mock/namespaces/{NAMESPACE}/pvcs/{rebuilt.claim}"
        )
        assert status == 200, status
        status, payload = call(
            "POST", f"{self.mock}/_mock/namespaces/{NAMESPACE}/pvcs",
            pvc_body(rebuilt, uid=str(uuid.uuid4())),
        )
        assert status == 201, (status, payload)
        round5 = self.reconcile()
        self.report("   round 5 (same-name claim rebuild)", round5)
        self.check(round5["attempted"] == [rebuilt.pv],
                   "round 5 should attempt only the rebuilt claim's volume")
        self.check(round5["bound"] == [],
                   "a stale reservation (old UID) must not bind the rebuilt claim")
        round6 = self.reconcile()
        self.report("   round 6 (nothing changed)", round6)
        self.check(round6["attempted"] == [], "unmet conditions are not retried blindly")

        print("\n5. reads and handover submits while rounds keep running")
        concurrently_ok = self._availability_probe(run_id)
        self.check(concurrently_ok, "reads/submits failed while a round was running")
        print(f"   concurrent probe: {'ok' if concurrently_ok else 'FAILED'}")

        print("\nRESULT", "OK" if self.ok else "FAILED")
        return 0 if self.ok else 1

    def _availability_probe(self, run_id: str) -> bool:
        """Keep reading and submitting from a second thread across many rounds."""
        volume = build_volumes(run_id, "probe", 1, offset=100)[0]
        self.create_and_reserve(volume)
        failures: list[str] = []
        stop = threading.Event()

        def worker() -> None:
            i = 0
            while not stop.is_set():
                i += 1
                status, _ = call("GET", f"{self.mock}/_mock/state")
                if status != 200:
                    failures.append(f"state read -> {status}")
                    break
                status, payload = call(
                    "GET", f"{self.api}/handovers/status?"
                    f"pv_name={volume.pv}&old_pvc_uid={volume.old_uid}"
                    f"&namespace={NAMESPACE}&name={volume.claim}&uid={volume.new_uid}"
                )
                if status != 200:
                    failures.append(f"status read -> {status} {payload}")
                    break
                status, payload = call("POST", f"{self.api}/handovers", {
                    "pv_name": volume.pv,
                    "old_pvc_uid": volume.old_uid,
                    "new_pvc": {"namespace": NAMESPACE, "name": volume.claim,
                                "uid": volume.new_uid},
                })
                if status not in (200, 202):
                    failures.append(f"handover submit -> {status} {payload}")
                    break

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        try:
            for _ in range(30):
                self.reconcile()
        finally:
            stop.set()
            thread.join(timeout=10)
        if thread.is_alive():
            failures.append("concurrent worker did not finish")
        return not failures


def main() -> int:
    parser = argparse.ArgumentParser(description="incremental reconcile demo")
    parser.add_argument("--api", default="http://localhost:8000",
                        help="handover service base URL (default: %(default)s)")
    parser.add_argument("--mock", default="http://localhost:8001",
                        help="mock Kubernetes operator API (default: %(default)s)")
    args = parser.parse_args()

    for name, url in (("handover", f"{args.api}/healthz"),
                      ("mock-kube", f"{args.mock}/_mock/state")):
        try:
            status, _ = call("GET", url)
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            print(f"cannot reach {name} at {url}: {exc}")
            return 2
        if status != 200:
            print(f"{name} at {url} is not healthy (HTTP {status})")
            return 2

    return Demo(args.api, args.mock).run()


if __name__ == "__main__":
    raise SystemExit(main())
