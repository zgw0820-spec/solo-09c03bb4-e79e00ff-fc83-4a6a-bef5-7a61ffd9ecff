"""Shared helpers for normalising Kubernetes objects into plain shapes."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.quantity import QuantityError, parse_quantity


def claim_identity(ref: dict[str, Any] | None) -> tuple[str, str, str] | None:
    """(namespace, name, uid) or None when incomplete."""
    if not ref:
        return None
    namespace = ref.get("namespace")
    name = ref.get("name")
    uid = ref.get("uid")
    if not namespace or not name or not uid:
        return None
    return str(namespace), str(name), str(uid)


def access_modes(obj: dict[str, Any]) -> list[str]:
    return sorted(str(mode) for mode in ((obj.get("spec") or {}).get("accessModes") or []))


def volume_mode(obj: dict[str, Any]) -> str:
    return str((obj.get("spec") or {}).get("volumeMode") or "Filesystem")


def storage_class(obj: dict[str, Any]) -> str:
    return str((obj.get("spec") or {}).get("storageClassName") or "")


def capacity(pv: dict[str, Any]) -> str | None:
    value = ((pv.get("spec") or {}).get("capacity") or {}).get("storage")
    return str(value) if value is not None else None


def requested_storage(pvc: dict[str, Any]) -> str | None:
    resources = (pvc.get("spec") or {}).get("resources") or {}
    for section in ("requests", "limits"):
        value = (resources.get(section) or {}).get("storage")
        if value is not None:
            return str(value)
    return None


def parse_storage(value: str | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return parse_quantity(value)
    except QuantityError:
        return None


def choose_requested_access_modes(
    pvc: dict[str, Any], pv: dict[str, Any]
) -> tuple[str | None, str]:
    """Check the claim's access modes against the volume's.

    Kubernetes' PV controller requires the claim's requested modes to be a
    subset of the volume's (``claimModes.IsSubset(volumeModes)``), so that is
    the rule enforced here. Returns ``(first_mode, "")`` on success or
    ``(None, reason)`` on failure.
    """
    wanted = [str(m) for m in ((pvc.get("spec") or {}).get("accessModes") or [])]
    supported = [str(m) for m in ((pv.get("spec") or {}).get("accessModes") or [])]
    if not wanted:
        return None, "新 PVC 未声明访问模式（spec.accessModes 为空）"
    if not supported:
        return None, "PV 未声明访问模式"
    unsupported = [m for m in wanted if m not in supported]
    if unsupported:
        return None, (
            f"PV 支持的访问模式为 {supported}，新 PVC 请求 {wanted}，"
            f"其中 {unsupported} 不受支持"
        )
    return wanted[0], ""
