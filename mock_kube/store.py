"""A small, faithful-enough in-memory Kubernetes core/v1 store.

It implements exactly the slice of behaviour the handover service relies on:

* read a PersistentVolume / PersistentVolumeClaim,
* JSON merge-patch a PersistentVolume with optimistic concurrency
  (``metadata.resourceVersion``) and UID immutability,
* delete a claim and let the volume go ``Released`` (Retain) or disappear
  (Delete),
* run an incremental binder that binds a PV/PVC pair only when the claim's
  UID matches the PV's pre-bound ``claimRef`` and the PVC names the volume.

Reconciliation is incremental. Every create/update/delete of a PV, and every
create/delete of a PVC, registers the *existing* volumes affected by that
change in a pending set. PVC changes are mapped to volumes through a
namespace/name index of ``spec.claimRef`` (plus the PVC's ``spec.volumeName``),
so no full PV scan is needed. ``reconcile()`` takes a snapshot of the pending
set (changes made during the round go to the next round), attempts each volume
at most once, and skips a volume if the PV or its claim changed again after
the snapshot. A volume whose binding conditions are not met simply waits until
another change re-registers it.

The same object is used by the mock HTTP API server (``mock_kube.app``) and
directly by the test suite, so the service is exercised against identical
semantics either way.
"""

from __future__ import annotations

import copy
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from app.kube import Conflict, Invalid, KubeClient, NotFound


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def merge_patch(target: Any, patch: Any) -> Any:
    """RFC 7386 JSON merge patch."""
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    result = copy.deepcopy(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        elif isinstance(value, dict):
            result[key] = merge_patch(result.get(key), value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def new_uid() -> str:
    return str(uuid.uuid4())


def claim_key(ref: dict[str, Any] | None) -> tuple[str, str] | None:
    """(namespace, name) of a claimRef, or None when it is incomplete."""
    if not ref:
        return None
    namespace, name = ref.get("namespace"), ref.get("name")
    if not namespace or not name:
        return None
    return str(namespace), str(name)


def _binding_mismatch(pv: dict[str, Any], pvc: dict[str, Any]) -> str | None:
    """The subset of the PV controller's binding checks this mock enforces."""
    pv_spec = pv.get("spec") or {}
    pvc_spec = pvc.get("spec") or {}

    pv_modes = {str(m) for m in pv_spec.get("accessModes") or []}
    claim_modes = {str(m) for m in pvc_spec.get("accessModes") or []}
    if not claim_modes:
        return "claim requests no access modes"
    if not claim_modes.issubset(pv_modes):
        return f"access modes {sorted(claim_modes)} are not a subset of {sorted(pv_modes)}"

    pv_class = str(pv_spec.get("storageClassName") or "")
    pvc_class = str(pvc_spec.get("storageClassName") or "")
    if pv_class != pvc_class:
        return f"storage class mismatch ({pv_class!r} != {pvc_class!r})"

    pv_mode = str(pv_spec.get("volumeMode") or "Filesystem")
    pvc_mode = str(pvc_spec.get("volumeMode") or "Filesystem")
    if pv_mode != pvc_mode:
        return f"volume mode mismatch ({pv_mode} != {pvc_mode})"

    from app.quantity import QuantityError, parse_quantity

    try:
        capacity = parse_quantity((pv_spec.get("capacity") or {}).get("storage", "0"))
        requested = parse_quantity(
            ((pvc_spec.get("resources") or {}).get("requests") or {}).get("storage", "0")
        )
    except QuantityError as exc:
        return f"unparseable storage quantity: {exc}"
    if capacity < requested:
        return f"volume capacity {capacity} is smaller than requested {requested}"
    return None


class InMemoryKube(KubeClient):
    """Thread-safe stand-in for the Kubernetes API server."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._lock = threading.RLock()
        # Serialises reconcile rounds. The object lock above is only held for
        # the short per-volume check+attempt, so reads and handover submits
        # keep working while a round is running.
        self._round_lock = threading.Lock()
        self._clock = clock or _utcnow
        self._pvs: dict[str, dict[str, Any]] = {}
        self._pvcs: dict[tuple[str, str], dict[str, Any]] = {}
        self.patch_log: list[dict[str, Any]] = []

        # Incremental reconcile bookkeeping.
        # ``_pending`` holds PV names affected by a change and not yet attempted.
        # ``_claim_refs`` maps a claim's namespace/name to the PVs whose
        # ``spec.claimRef`` points at it, and ``_claim_volumes`` maps it to the
        # PVs named by the claim's ``spec.volumeName``; together they let a PVC
        # change find its volumes by namespace/name without scanning all PVs.
        # The change counters are monotonic per object key; they (unlike
        # resourceVersion) still detect a delete + same-name recreate.
        self._pending: set[str] = set()
        self._claim_refs: dict[tuple[str, str], set[str]] = {}
        self._claim_volumes: dict[tuple[str, str], set[str]] = {}
        self._pv_claim: dict[str, tuple[str, str]] = {}
        self._pv_changes: dict[str, int] = {}
        self._claim_changes: dict[tuple[str, str], int] = {}
        self._round = 0

    # ------------------------------------------------------------------ reads
    def get_pv(self, name: str) -> dict[str, Any]:
        with self._lock:
            if name not in self._pvs:
                raise NotFound(f'persistentvolumes "{name}" not found')
            return copy.deepcopy(self._pvs[name])

    def get_pvc(self, namespace: str, name: str) -> dict[str, Any]:
        with self._lock:
            key = (namespace, name)
            if key not in self._pvcs:
                raise NotFound(
                    f'persistentvolumeclaims "{name}" not found in namespace "{namespace}"'
                )
            return copy.deepcopy(self._pvcs[key])

    def list_pvs(self, limit: int | None = None) -> list[dict[str, Any]]:
        with self._lock:
            items = [copy.deepcopy(pv) for pv in self._pvs.values()]
        return items[:limit] if limit else items

    def list_pvcs(self, namespace: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            return [
                copy.deepcopy(pvc)
                for (ns, _), pvc in self._pvcs.items()
                if namespace is None or ns == namespace
            ]

    # ----------------------------------------------------------------- writes
    def update_pv(self, name: str, body: dict[str, Any]) -> dict[str, Any]:
        """Apply a JSON merge patch with resourceVersion + UID preconditions."""
        if not isinstance(body, dict):
            raise Invalid("patch body must be a JSON object")
        with self._lock:
            if name not in self._pvs:
                raise NotFound(f'persistentvolumes "{name}" not found')
            current = self._pvs[name]
            meta = body.get("metadata") or {}

            actual_uid = (current.get("metadata") or {}).get("uid")
            if "uid" in meta and meta["uid"] != actual_uid:
                raise Conflict(
                    "Precondition failed: UID in precondition: "
                    f"{meta['uid']!r}, UID in object meta: {actual_uid!r}"
                )

            actual_rv = (current.get("metadata") or {}).get("resourceVersion")
            if "resourceVersion" in meta and str(meta["resourceVersion"]) != str(actual_rv):
                raise Conflict(
                    "Operation cannot be fulfilled on persistentvolumes "
                    f'"{name}": the object has been modified; please apply your '
                    "changes to the latest version and try again"
                )

            patched = merge_patch(current, body)
            patched.setdefault("metadata", {})
            patched["metadata"]["name"] = name
            patched["metadata"]["uid"] = actual_uid
            patched["metadata"]["resourceVersion"] = str(int(actual_rv) + 1)
            self.patch_log.append(
                {
                    "name": name,
                    "at": _iso(self._clock()),
                    "body": copy.deepcopy(body),
                    "resourceVersion": patched["metadata"]["resourceVersion"],
                }
            )
            self._pvs[name] = patched
            self._note_pv_change(name)
            return copy.deepcopy(patched)

    def create_pv(self, body: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            obj = copy.deepcopy(body)
            meta = obj.setdefault("metadata", {})
            name = meta.get("name")
            if not name:
                raise Invalid("metadata.name is required")
            if name in self._pvs:
                raise Conflict(f'persistentvolumes "{name}" already exists')
            meta.setdefault("uid", new_uid())
            meta["resourceVersion"] = "1"
            meta.setdefault("creationTimestamp", _iso(self._clock()))
            obj.setdefault("apiVersion", "v1")
            obj.setdefault("kind", "PersistentVolume")
            spec = obj.setdefault("spec", {})
            status = obj.setdefault("status", {})
            status.setdefault(
                "phase", "Released" if spec.get("claimRef") else "Available"
            )
            self._pvs[name] = obj
            self._note_pv_change(name)
            return copy.deepcopy(obj)

    def create_pvc(
        self, namespace: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        with self._lock:
            obj = copy.deepcopy(body)
            meta = obj.setdefault("metadata", {})
            name = meta.get("name")
            if not name:
                raise Invalid("metadata.name is required")
            meta["namespace"] = namespace
            key = (namespace, name)
            if key in self._pvcs:
                raise Conflict(
                    f'persistentvolumeclaims "{name}" already exists in namespace "{namespace}"'
                )
            meta.setdefault("uid", new_uid())
            meta["resourceVersion"] = "1"
            meta.setdefault("creationTimestamp", _iso(self._clock()))
            obj.setdefault("apiVersion", "v1")
            obj.setdefault("kind", "PersistentVolumeClaim")
            obj.setdefault("spec", {})
            status = obj.setdefault("status", {})
            status.setdefault("phase", "Pending")
            self._pvcs[key] = obj
            self._note_claim_change(key)
            return copy.deepcopy(obj)

    def delete_pvc(self, namespace: str, name: str) -> dict[str, Any]:
        """Delete a claim and let the volume controller react to it."""
        with self._lock:
            key = (namespace, name)
            if key not in self._pvcs:
                raise NotFound(
                    f'persistentvolumeclaims "{name}" not found in namespace "{namespace}"'
                )
            pvc = self._pvcs.pop(key)
            # The affected volumes are the ones indexed for this claim (no scan
            # of all PVs needed); the change is registered *after* the volumes
            # have been updated so the next round sees the final state.
            affected = self._volumes_for_claim(key)
            uid = (pvc.get("metadata") or {}).get("uid")
            for pv_name in affected:
                pv = self._pvs.get(pv_name)
                if pv is None:
                    continue
                ref = (pv.get("spec") or {}).get("claimRef") or {}
                if ref.get("uid") != uid:
                    continue
                policy = (pv.get("spec") or {}).get("reclaimPolicy", "Retain")
                if policy == "Delete":
                    del self._pvs[pv_name]
                    self._note_pv_change(pv_name)
                else:
                    pv.setdefault("status", {})["phase"] = "Released"
                    pv["status"].pop("message", None)
                    pv["metadata"]["resourceVersion"] = str(
                        int(pv["metadata"]["resourceVersion"]) + 1
                    )
                    self._note_pv_change(pv_name)
            self._note_claim_change(key)
            return copy.deepcopy(pvc)

    def delete_pv(self, name: str) -> dict[str, Any]:
        with self._lock:
            if name not in self._pvs:
                raise NotFound(f'persistentvolumes "{name}" not found')
            removed = self._pvs.pop(name)
            self._note_pv_change(name)
            return copy.deepcopy(removed)

    # ----------------------------------------------------------------- binder
    def bind_pv(self, name: str) -> dict[str, Any]:
        """Run one bind attempt for a volume, mirroring the PV controller."""
        with self._lock:
            if name not in self._pvs:
                raise NotFound(f'persistentvolumes "{name}" not found')
            pv = self._pvs[name]
            spec = pv.setdefault("spec", {})
            status = pv.setdefault("status", {})
            ref = spec.get("claimRef") or {}
            if not ref:
                raise Conflict(f'persistentvolumes "{name}" has no pre-bound claimRef')
            if not ref.get("uid") or not ref.get("namespace") or not ref.get("name"):
                raise Conflict(
                    f'persistentvolumes "{name}" claimRef lacks namespace/name/uid'
                )
            key = (ref["namespace"], ref["name"])
            pvc = self._pvcs.get(key)
            if pvc is None:
                raise Conflict(
                    f'persistentvolumeclaims "{ref["name"]}" not found in namespace '
                    f'"{ref["namespace"]}"'
                )
            pvc_meta = pvc.setdefault("metadata", {})
            pvc_spec = pvc.setdefault("spec", {})
            pvc_status = pvc.setdefault("status", {})
            if pvc_meta.get("uid") != ref["uid"]:
                raise Conflict(
                    "claimRef UID does not match the live claim "
                    f'({ref["uid"]} != {pvc_meta.get("uid")})'
                )
            if pvc_spec.get("volumeName") != name:
                raise Conflict(
                    f'persistentvolumeclaim "{ref["namespace"]}/{ref["name"]}" '
                    f'does not pre-bind volume "{name}"'
                )

            mismatch = _binding_mismatch(pv, pvc)
            if mismatch:
                raise Conflict(
                    f'persistentvolumeclaim "{ref["namespace"]}/{ref["name"]}" '
                    f"cannot bind volume \"{name}\": {mismatch}"
                )

            if status.get("phase") == "Bound":
                if (spec.get("claimRef") or {}).get("uid") == ref["uid"]:
                    return copy.deepcopy(pv)
                raise Conflict(f'persistentvolumes "{name}" is already bound')

            status["phase"] = "Bound"
            new_ref = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "namespace": ref["namespace"],
                "name": ref["name"],
                "uid": ref["uid"],
            }
            spec["claimRef"] = new_ref
            pv["metadata"]["resourceVersion"] = str(
                int(pv["metadata"]["resourceVersion"]) + 1
            )

            pvc_status["phase"] = "Bound"
            pvc_spec["volumeName"] = name
            pvc_status["accessModes"] = copy.deepcopy(spec.get("accessModes") or [])
            pvc_status["capacity"] = copy.deepcopy(spec.get("capacity") or {})
            pvc_meta["resourceVersion"] = str(
                int(pvc_meta.get("resourceVersion", "1")) + 1
            )
            # This write is the round's own doing, so it must not be mistaken
            # for a concurrent edit when the round checks whether the object
            # changed mid-attempt. Refresh the tracked state without touching
            # the change counters.
            self._track_pv(name)
            self._track_claim(key)
            return copy.deepcopy(pv)

    def reconcile(self) -> dict[str, Any]:
        """Attempt the volumes registered since the previous round.

        Only volumes already registered when the round starts are considered;
        anything created/updated during the round is left for the next one.
        Right before each attempt the volume and its claim are compared with
        the state captured at round start: if either changed meanwhile (a
        same-name PVC rebuild counts as a change) the volume is deferred. A
        volume whose binding preconditions are not met is not retried here;
        it waits for the next relevant change to register it again.
        """
        with self._round_lock:
            with self._lock:
                self._round += 1
                round_no = self._round
                names = sorted(name for name in self._pending if name in self._pvs)
                self._pending.difference_update(names)
                snapshots = {name: self._round_snapshot(name) for name in names}

            attempted: list[str] = []
            bound: list[str] = []
            deferred: list[str] = []
            skipped: list[str] = []

            for name in names:
                with self._lock:
                    pv = self._pvs.get(name)
                    if pv is None:
                        skipped.append(name)
                        continue
                    if self._changed_since(name, snapshots[name]):
                        deferred.append(name)
                        if name in self._pvs:
                            # Re-read it next round using the latest objects.
                            self._pending.add(name)
                        continue
                    before = (pv.get("status") or {}).get("phase")
                    # Baseline taken after the change check and before the
                    # attempt, so the bind's own writes can be excluded.
                    pv_before = self._pv_changes.get(name, 0)
                    claim_before = self._pv_claim.get(name)
                    claim_changes_before = (
                        self._claim_changes.get(claim_before, 0)
                        if claim_before is not None
                        else 0
                    )

                attempted.append(name)
                try:
                    # bind_pv validates against the live objects under its own
                    # lock; the round never holds the store lock while binding,
                    # so reads and handover writes keep flowing.
                    updated = self.bind_pv(name)
                except (Conflict, NotFound):
                    # Conditions not met: leave the volume unregistered until
                    # the next change to it or its claim registers it again.
                    continue
                with self._lock:
                    # The bind's own writes do not bump the change counters.
                    # A non-zero delta therefore means a concurrent edit
                    # landed during the attempt and already re-registered the
                    # volume, so it stays queued for a fresh attempt next round.
                    pv_delta = self._pv_changes.get(name, 0) - pv_before
                    claim_delta = (
                        self._claim_changes.get(claim_before, 0)
                        if claim_before is not None
                        else 0
                    ) - claim_changes_before
                    if pv_delta or claim_delta:
                        self._pending.add(name)
                    else:
                        self._pending.discard(name)
                if (updated.get("status") or {}).get("phase") == "Bound" and before != "Bound":
                    bound.append(name)

            return {
                "round": round_no,
                "registered": len(names),
                "attempted": attempted,
                "bound": bound,
                "deferred": deferred,
                "skipped": skipped,
                "pending": len(self._pending),
            }

    # ------------------------------------------------- incremental registration
    def _round_snapshot(self, name: str) -> tuple[int, tuple[str, str] | None, int]:
        """(PV change count, claim key, claim change count) at round start."""
        claim = self._pv_claim.get(name)
        changes = self._claim_changes.get(claim, 0) if claim is not None else 0
        return self._pv_changes.get(name, 0), claim, changes

    def _changed_since(
        self, name: str, snapshot: tuple[int, tuple[str, str] | None, int]
    ) -> bool:
        pv_change, claim, claim_change = snapshot
        if self._pv_changes.get(name, 0) != pv_change:
            return True
        current = self._pv_claim.get(name)
        if current != claim:
            return True
        if claim is not None and self._claim_changes.get(claim, 0) != claim_change:
            return True
        return False

    def _note_pv_change(self, name: str) -> None:
        """Register an affected volume, if it still exists, for the next round."""
        self._pv_changes[name] = self._pv_changes.get(name, 0) + 1
        if not self._track_pv(name):
            # Deleted volumes must not stay in the next round's work list.
            return
        self._pending.add(name)

    def _track_pv(self, name: str) -> bool:
        """Re-index a volume's claimRef; return False if it no longer exists."""
        previous = self._pv_claim.pop(name, None)
        if previous is not None:
            owners = self._claim_refs.get(previous)
            if owners is not None:
                owners.discard(name)
                if not owners:
                    del self._claim_refs[previous]

        if name not in self._pvs:
            self._pending.discard(name)
            return False
        claim = claim_key((self._pvs[name].get("spec") or {}).get("claimRef"))
        if claim is not None:
            self._claim_refs.setdefault(claim, set()).add(name)
            self._pv_claim[name] = claim
        return True

    def _track_claim(self, key: tuple[str, str]) -> None:
        """Re-index which volumes a claim's ``spec.volumeName`` names."""
        pvc = self._pvcs.get(key)
        wanted = (pvc.get("spec") or {}).get("volumeName") if pvc else None
        indexed = {
            name
            for name in self._claim_volumes.pop(key, set())
            if name in self._pvs
        }
        if wanted and wanted in self._pvs:
            indexed.add(str(wanted))
        if indexed:
            self._claim_volumes[key] = indexed

    def _note_claim_change(self, key: tuple[str, str]) -> None:
        """Register the volumes associated with this claim (by ns/name)."""
        self._claim_changes[key] = self._claim_changes.get(key, 0) + 1
        self._track_claim(key)
        for name in self._claim_refs.get(key, ()):
            if name in self._pvs:
                self._pending.add(name)
        for name in self._claim_volumes.get(key, ()):
            if name in self._pvs:
                self._pending.add(name)

    def _volumes_for_claim(self, key: tuple[str, str]) -> list[str]:
        """Volumes associated with a claim, located by namespace/name index."""
        names = set(self._claim_refs.get(key, ()))
        names.update(self._claim_volumes.get(key, ()))
        return [name for name in names if name in self._pvs]

    def reconcile_state(self) -> dict[str, Any]:
        """Read-only view of what the next round would consider."""
        with self._lock:
            return {
                "round": self._round,
                "registered": sorted(name for name in self._pending if name in self._pvs),
                "pending": len(self._pending),
            }

    # ----------------------------------------------------------------- helpers
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "persistentVolumes": self.list_pvs(),
                "persistentVolumeClaims": self.list_pvcs(),
            }

    def reset(self, pvs: Iterable[dict[str, Any]] | None = None,
              pvcs: Iterable[tuple[str, dict[str, Any]]] | None = None) -> None:
        with self._lock:
            self._pvs.clear()
            self._pvcs.clear()
            self.patch_log.clear()
            self._pending.clear()
            self._claim_refs.clear()
            self._claim_volumes.clear()
            self._pv_claim.clear()
            self._pv_changes.clear()
            self._claim_changes.clear()
            self._round = 0
            for pv in pvs or []:
                self.create_pv(pv)
            for namespace, pvc in pvcs or []:
                self.create_pvc(namespace, pvc)
