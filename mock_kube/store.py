"""A small, faithful-enough in-memory Kubernetes core/v1 store.

It implements exactly the slice of behaviour the handover service relies on:

* read a PersistentVolume / PersistentVolumeClaim,
* JSON merge-patch a PersistentVolume with optimistic concurrency
  (``metadata.resourceVersion``) and UID immutability,
* delete a claim and let the volume go ``Released`` (Retain) or disappear
  (Delete),
* run a binder that binds a PV/PVC pair only when the claim's UID matches
  the PV's pre-bound ``claimRef`` and the PVC names the volume.

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
        self._clock = clock or _utcnow
        self._pvs: dict[str, dict[str, Any]] = {}
        self._pvcs: dict[tuple[str, str], dict[str, Any]] = {}
        self.patch_log: list[dict[str, Any]] = []

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
            uid = (pvc.get("metadata") or {}).get("uid")
            for pv_name, pv in list(self._pvs.items()):
                ref = (pv.get("spec") or {}).get("claimRef") or {}
                if ref.get("uid") != uid:
                    continue
                policy = (pv.get("spec") or {}).get("reclaimPolicy", "Retain")
                if policy == "Delete":
                    del self._pvs[pv_name]
                else:
                    pv.setdefault("status", {})["phase"] = "Released"
                    pv["status"].pop("message", None)
                    pv["metadata"]["resourceVersion"] = str(
                        int(pv["metadata"]["resourceVersion"]) + 1
                    )
            return copy.deepcopy(pvc)

    def delete_pv(self, name: str) -> dict[str, Any]:
        with self._lock:
            if name not in self._pvs:
                raise NotFound(f'persistentvolumes "{name}" not found')
            return copy.deepcopy(self._pvs.pop(name))

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
            spec["claimRef"] = {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "namespace": ref["namespace"],
                "name": ref["name"],
                "uid": ref["uid"],
            }
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
            return copy.deepcopy(pv)

    def reconcile(self) -> list[str]:
        """Bind every volume whose reservation is now complete."""
        bound: list[str] = []
        with self._lock:
            for name in list(self._pvs):
                try:
                    before = self._pvs[name]["status"].get("phase")
                    self.bind_pv(name)
                    if self._pvs[name]["status"].get("phase") == "Bound" and before != "Bound":
                        bound.append(name)
                except (Conflict, NotFound):
                    continue
        return bound

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
            for pv in pvs or []:
                self.create_pv(pv)
            for namespace, pvc in pvcs or []:
                self.create_pvc(namespace, pvc)
