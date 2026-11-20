"""Volume handover rules.

The operation re-points a ``Released``, ``Retain`` PersistentVolume at a
pre-created replacement claim. Kubernetes represents "this volume is reserved
for claim X" with ``spec.claimRef`` on the PV, so that single field is the only
place a reservation is ever written, and every write carries the
``resourceVersion`` that was read moments earlier. Two competing handovers
therefore cannot both succeed: the loser gets a 409 from the API server and
re-decides against fresh state.

The service keeps **no state of its own**. All progress is derived from the
live PV and PVC objects, so it is safe to run several replicas and safe to
restart mid-handover.

Invariants
----------
* The volume is always reserved: ``spec.claimRef`` is never cleared. Before a
  handover it names the old claim; after it names the new claim.
* Only a ``Retain`` volume in phase ``Released`` can be handed over.
* The replacement claim must already exist, carry the UID from the request,
  and be pre-bound to this volume (``spec.volumeName``).
* The volume must satisfy the new claim's capacity, storage class, access
  modes and volume mode.
* A submission is idempotent for the claim that already holds the
  reservation; anything else is a conflict and never rewrites the reservation.

Statuses
--------
``pending_binding``  reservation in place, PV and PVC are not both Bound yet.
``completed``        PV and PVC are both ``Bound`` and point at each other.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app import kube_objects as ko
from app.errors import ApiError
from app.kube import Conflict, KubeClient, KubeError, NotFound, Unavailable
from app.schemas import (
    CheckResult,
    HandoverRequest,
    HandoverResponse,
    PVCSummary,
    PVSummary,
)

log = logging.getLogger("handover")

PREVIOUS_PVC_UID_ANNOTATION = "handover.example.com/previous-pvc-uid"
RESERVED_AT_ANNOTATION = "handover.example.com/reserved-at"
MAX_RESERVE_ATTEMPTS = 5


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class HandoverService:
    def __init__(self, kube: KubeClient) -> None:
        self.kube = kube

    # ----------------------------------------------------------------- public
    def submit(self, request: HandoverRequest) -> HandoverResponse:
        """Hand the volume over, or report progress of an identical retry."""
        pv = self._read_pv(request.pv_name)
        _require_retain(pv, request)

        current = ko.claim_identity((pv.get("spec") or {}).get("claimRef"))
        if current is None:
            if (pv.get("spec") or {}).get("claimRef"):
                raise ApiError(
                    409,
                    "pv_claim_ref_incomplete",
                    "该卷的 claimRef 缺少 namespace/name/uid，无法确认旧声明身份。",
                    _volume_details(pv),
                )
            raise ApiError(
                409,
                "volume_not_released",
                "该卷尚无关联声明（claimRef 为空），没有可交接的旧声明。",
                _volume_details(pv),
            )

        if current == _new_ident(request):
            # Already reserved for exactly this claim: report progress only.
            pvc, pvc_error = self._read_pvc(request)
            if pvc is not None and _pvc_uid(pvc) != request.new_pvc.uid:
                raise ApiError(
                    409,
                    "new_pvc_uid_mismatch",
                    f"同名新 PVC 已被重建，当前 UID 为 {_pvc_uid(pvc)}，"
                    f"与请求中的 {request.new_pvc.uid} 不一致，本次请求已失效。",
                    _volume_details(pv),
                )
            return self._respond(
                request, self._read_pv(request.pv_name), pvc=pvc, pvc_error=pvc_error
            )

        if current[2] != request.old_pvc_uid:
            raise ApiError(
                409,
                "pv_reserved_for_other_claim",
                "该卷当前预留给其他声明，不能交接给本次请求的新声明。",
                _volume_details(pv),
            )

        _require_released(pv, request)
        pvc = self._require_live_pvc_uid(request)
        self._require_compatible(pv, pvc, request)

        pv = self._reserve(pv, request)
        return self._respond(request, pv)

    def status(self, request: HandoverRequest) -> HandoverResponse:
        """Read-only progress query. Never mutates the cluster."""
        pv = self._read_pv(request.pv_name)
        if ko.claim_identity((pv.get("spec") or {}).get("claimRef")) != _new_ident(request):
            raise ApiError(
                409,
                "no_such_handover",
                "该卷当前并未预留给请求中的新声明，无法查询本次交接进度。",
                _volume_details(pv),
            )
        pvc, pvc_error = self._read_pvc(request)
        if pvc is not None and _pvc_uid(pvc) != request.new_pvc.uid:
            raise ApiError(
                409,
                "new_pvc_uid_mismatch",
                f"同名新 PVC 已被重建，当前 UID 为 {_pvc_uid(pvc)}，"
                f"与请求中的 {request.new_pvc.uid} 不一致，本次请求已失效。",
                _volume_details(pv),
            )
        return self._respond(request, pv, pvc=pvc, pvc_error=pvc_error)

    # ----------------------------------------------------------------- submit
    def _reserve(self, pv: dict[str, Any], request: HandoverRequest) -> dict[str, Any]:
        """Write the new claimRef under an optimistic-concurrency precondition."""
        pv_name = request.pv_name
        for _attempt in range(MAX_RESERVE_ATTEMPTS):
            meta = pv.get("metadata") or {}
            patch = {
                "metadata": {
                    "uid": meta.get("uid"),
                    "resourceVersion": meta.get("resourceVersion"),
                    "annotations": {
                        PREVIOUS_PVC_UID_ANNOTATION: request.old_pvc_uid,
                        RESERVED_AT_ANNOTATION: _now_iso(),
                    },
                },
                "spec": {
                    "claimRef": {
                        "apiVersion": "v1",
                        "kind": "PersistentVolumeClaim",
                        "namespace": request.new_pvc.namespace,
                        "name": request.new_pvc.name,
                        "uid": request.new_pvc.uid,
                    }
                },
            }
            try:
                updated = self.kube.update_pv(pv_name, patch)
            except Conflict as exc:
                # Someone wrote between our read and our write. Re-read and
                # decide again rather than blindly overwriting.
                latest = self._read_pv(pv_name)
                latest_ref = ko.claim_identity((latest.get("spec") or {}).get("claimRef"))
                if latest_ref == _new_ident(request):
                    log.info("handover of %s resolved as duplicate submit", pv_name)
                    return latest
                if latest_ref and latest_ref[2] == request.old_pvc_uid:
                    # The competing write did not touch ownership; retry with
                    # the fresh resourceVersion.
                    pv = latest
                    continue
                raise ApiError(
                    409,
                    "pv_reserved_for_other_claim",
                    "并发交接中该卷已被另一方预留，本次请求未获得预留。",
                    _volume_details(latest),
                ) from exc
            except NotFound as exc:
                raise ApiError(
                    404, "pv_not_found", f"持久卷 {pv_name} 不存在。"
                ) from exc
            except KubeError as exc:
                raise _kube_api_error(exc, "预留持久卷失败") from exc

            log.info(
                "reserved pv=%s for %s/%s uid=%s (previous claim uid=%s)",
                pv_name,
                request.new_pvc.namespace,
                request.new_pvc.name,
                request.new_pvc.uid,
                request.old_pvc_uid,
            )
            return updated
        raise ApiError(
            409, "concurrent_update", "该卷正被频繁修改，预留未完成，请重试。"
        )

    # ------------------------------------------------------------------ response
    def _respond(
        self,
        request: HandoverRequest,
        pv: dict[str, Any],
        *,
        pvc: dict[str, Any] | None = None,
        pvc_error: str | None = None,
    ) -> HandoverResponse:
        if pvc is None and pvc_error is None:
            pvc, pvc_error = self._read_pvc(request)

        checks = self._compatibility_checks(pv, pvc, request, pvc_error)
        completed, pending_reason = _completion(pv, pvc, request)

        pv_phase = (pv.get("status") or {}).get("phase")
        pvc_phase = (pvc.get("status") or {}).get("phase") if pvc else None

        if completed:
            status = "completed"
            text = "交接完成：PV 与 PVC 均已 Bound 且相互指向。"
        else:
            status = "pending_binding"
            text = (
                "预留已生效，尚未绑定完成"
                f"（PV={pv_phase or '<未知>'}，PVC={pvc_phase or '<不存在>'}）：{pending_reason}"
            )

        return HandoverResponse(
            status=status,
            status_text=text,
            pv_name=request.pv_name,
            old_pvc_uid=request.old_pvc_uid,
            new_pvc=_pvc_summary(pvc) if pvc else _pvc_summary_from_request(request),
            reservation=_reservation_snapshot(pv),
            checks=checks,
            pv=_pv_summary(pv),
            message=text,
        )

    # ------------------------------------------------------------------- reads
    def _read_pv(self, name: str) -> dict[str, Any]:
        try:
            return self.kube.get_pv(name)
        except NotFound as exc:
            raise ApiError(
                404, "pv_not_found", f"持久卷 {name} 不存在。", {"pv_name": name}
            ) from exc
        except KubeError as exc:
            raise _kube_api_error(exc, "读取持久卷失败") from exc

    def _read_pvc(
        self, request: HandoverRequest
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            return self.kube.get_pvc(request.new_pvc.namespace, request.new_pvc.name), None
        except NotFound:
            return None, (
                f"新 PVC {request.new_pvc.namespace}/{request.new_pvc.name} 不存在"
            )
        except KubeError as exc:
            raise _kube_api_error(exc, "读取新 PVC 失败") from exc

    def _require_live_pvc_uid(self, request: HandoverRequest) -> dict[str, Any]:
        try:
            pvc = self.kube.get_pvc(request.new_pvc.namespace, request.new_pvc.name)
        except NotFound as exc:
            raise ApiError(
                409,
                "new_pvc_not_found",
                f"新 PVC {request.new_pvc.namespace}/{request.new_pvc.name} 不存在，"
                "请先创建并预先指定该卷（spec.volumeName）。",
                {"new_pvc": request.new_pvc.model_dump()},
            ) from exc
        except KubeError as exc:
            raise _kube_api_error(exc, "读取新 PVC 失败") from exc
        if _pvc_uid(pvc) != request.new_pvc.uid:
            raise ApiError(
                409,
                "new_pvc_uid_mismatch",
                f"集群中同名 PVC 的 UID 为 {_pvc_uid(pvc)}，与请求中的 "
                f"{request.new_pvc.uid} 不一致（同名 PVC 可能已被重建）。",
                {"new_pvc": request.new_pvc.model_dump(),
                 "actual_uid": _pvc_uid(pvc)},
            )
        return pvc

    def _require_compatible(
        self, pv: dict[str, Any], pvc: dict[str, Any], request: HandoverRequest
    ) -> None:
        checks = self._compatibility_checks(pv, pvc, request)
        failed = [c for c in checks if not c.passed]
        if failed:
            raise ApiError(
                409,
                "volume_incompatible",
                "该卷不满足新声明的绑定要求：" + "；".join(c.detail for c in failed),
                {"checks": [c.model_dump() for c in checks]},
            )

    def _compatibility_checks(
        self,
        pv: dict[str, Any],
        pvc: dict[str, Any] | None,
        request: HandoverRequest,
        pvc_error: str | None = None,
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []
        checks.append(
            CheckResult(
                name="pvc_exists",
                passed=pvc is not None,
                detail="新 PVC 存在" if pvc else (pvc_error or "新 PVC 不存在"),
            )
        )
        if pvc is None:
            return checks

        # Capacity: the volume must cover the claim's request.
        pv_capacity = ko.parse_storage(ko.capacity(pv))
        requested_raw = ko.requested_storage(pvc)
        requested = ko.parse_storage(requested_raw)
        if requested_raw is None:
            checks.append(
                CheckResult(
                    name="capacity_satisfied", passed=False, detail="新 PVC 未声明存储容量"
                )
            )
        elif pv_capacity is None or requested is None:
            checks.append(
                CheckResult(
                    name="capacity_satisfied",
                    passed=False,
                    detail=f"容量无法比较（PV={ko.capacity(pv)!r}，PVC={requested_raw!r}）",
                )
            )
        else:
            ok = pv_capacity >= requested
            checks.append(
                CheckResult(
                    name="capacity_satisfied",
                    passed=ok,
                    detail=(
                        f"PV 容量 {ko.capacity(pv)} 满足新 PVC 请求 {requested_raw}"
                        if ok
                        else f"PV 容量 {ko.capacity(pv)} 小于新 PVC 请求 {requested_raw}"
                    ),
                )
            )

        pv_sc, pvc_sc = ko.storage_class(pv), ko.storage_class(pvc)
        checks.append(
            CheckResult(
                name="storage_class_matches",
                passed=pv_sc == pvc_sc,
                detail=(
                    f"存储类一致：{pv_sc or '<空>'}"
                    if pv_sc == pvc_sc
                    else f"存储类不一致：PV={pv_sc or '<空>'}，新 PVC={pvc_sc or '<空>'}"
                ),
            )
        )

        chosen, reason = ko.choose_requested_access_modes(pvc, pv)
        checks.append(
            CheckResult(
                name="access_modes_supported",
                passed=chosen is not None,
                detail=(
                    f"访问模式 {ko.access_modes(pvc)} 均受支持，将按 {chosen} 绑定"
                    if chosen
                    else reason
                ),
            )
        )

        pv_mode, pvc_mode = ko.volume_mode(pv), ko.volume_mode(pvc)
        checks.append(
            CheckResult(
                name="volume_mode_matches",
                passed=pv_mode == pvc_mode,
                detail=(
                    f"卷模式一致：{pv_mode}"
                    if pv_mode == pvc_mode
                    else f"卷模式不一致：PV={pv_mode}，新 PVC={pvc_mode}"
                ),
            )
        )

        volume_name = (pvc.get("spec") or {}).get("volumeName")
        checks.append(
            CheckResult(
                name="pvc_prebound_to_pv",
                passed=volume_name == request.pv_name,
                detail=(
                    f"新 PVC 已预先指定卷 {request.pv_name}"
                    if volume_name == request.pv_name
                    else f"新 PVC 的 spec.volumeName={volume_name!r}，"
                    f"未预先指定 {request.pv_name}"
                ),
            )
        )

        uid = _pvc_uid(pvc)
        checks.append(
            CheckResult(
                name="pvc_uid_matches",
                passed=uid == request.new_pvc.uid,
                detail=(
                    "新 PVC UID 匹配"
                    if uid == request.new_pvc.uid
                    else f"集群中同名 PVC 的 UID 为 {uid}，与请求中的 "
                    f"{request.new_pvc.uid} 不一致"
                ),
            )
        )
        return checks


# --------------------------------------------------------------------- checks
def _new_ident(request: HandoverRequest) -> tuple[str, str, str]:
    return (request.new_pvc.namespace, request.new_pvc.name, request.new_pvc.uid)


def _pvc_uid(pvc: dict[str, Any]) -> str | None:
    uid = (pvc.get("metadata") or {}).get("uid")
    return str(uid) if uid is not None else None


def _require_retain(pv: dict[str, Any], request: HandoverRequest) -> None:
    policy = (pv.get("spec") or {}).get("persistentVolumeReclaimPolicy")
    if policy != "Retain":
        raise ApiError(
            409,
            "reclaim_policy_not_retain",
            f"该卷的回收策略为 {policy or '<未设置>'}，只有 Retain 的卷可以交接。",
            _volume_details(pv),
        )


def _require_released(pv: dict[str, Any], request: HandoverRequest) -> None:
    phase = (pv.get("status") or {}).get("phase")
    if phase != "Released":
        raise ApiError(
            409,
            "volume_not_released",
            f"该卷当前阶段为 {phase or '<未知>'}，只有 Released 的卷可以交接。",
            _volume_details(pv),
        )


def _completion(
    pv: dict[str, Any], pvc: dict[str, Any] | None, request: HandoverRequest
) -> tuple[bool, str]:
    if pvc is None:
        return False, "新 PVC 尚未创建"
    pv_phase = (pv.get("status") or {}).get("phase")
    pvc_phase = (pvc.get("status") or {}).get("phase")
    if pv_phase != "Bound":
        return False, f"PV 尚未 Bound（当前 {pv_phase or '<未知>'}）"
    if pvc_phase != "Bound":
        return False, f"PVC 尚未 Bound（当前 {pvc_phase or '<未知>'}）"
    ref = ko.claim_identity((pv.get("spec") or {}).get("claimRef"))
    if ref != (request.new_pvc.namespace, request.new_pvc.name, _pvc_uid(pvc)):
        return False, "PV 的 claimRef 未指向该 PVC 的 UID"
    if (pvc.get("spec") or {}).get("volumeName") != request.pv_name:
        return False, "PVC 的 spec.volumeName 未指向该 PV"
    return True, ""


# -------------------------------------------------------------------- helpers
def _volume_details(pv: dict[str, Any]) -> dict[str, Any]:
    spec = pv.get("spec") or {}
    ref = spec.get("claimRef") or {}
    return {
        "pv_name": (pv.get("metadata") or {}).get("name"),
        "phase": (pv.get("status") or {}).get("phase"),
        "reclaim_policy": spec.get("persistentVolumeReclaimPolicy"),
        "claim_ref": {
            "namespace": ref.get("namespace"),
            "name": ref.get("name"),
            "uid": ref.get("uid"),
        }
        if ref
        else None,
    }


def _reservation_snapshot(pv: dict[str, Any]) -> dict[str, Any] | None:
    ref = (pv.get("spec") or {}).get("claimRef")
    if not ref:
        return None
    annotations = (pv.get("metadata") or {}).get("annotations") or {}
    return {
        "namespace": ref.get("namespace"),
        "name": ref.get("name"),
        "uid": ref.get("uid"),
        "phase": (pv.get("status") or {}).get("phase"),
        "previous_pvc_uid": annotations.get(PREVIOUS_PVC_UID_ANNOTATION),
        "reserved_at": annotations.get(RESERVED_AT_ANNOTATION),
    }


def _pv_summary(pv: dict[str, Any]) -> PVSummary:
    meta = pv.get("metadata") or {}
    spec = pv.get("spec") or {}
    return PVSummary(
        name=str(meta.get("name")),
        uid=str(meta.get("uid")),
        phase=(pv.get("status") or {}).get("phase"),
        reclaim_policy=spec.get("persistentVolumeReclaimPolicy"),
        storage_class=str(spec.get("storageClassName") or ""),
        capacity=ko.capacity(pv),
        access_modes=ko.access_modes(pv),
        volume_mode=ko.volume_mode(pv),
        claim_ref=_reservation_snapshot(pv),
    )


def _pvc_summary(pvc: dict[str, Any]) -> PVCSummary:
    meta = pvc.get("metadata") or {}
    return PVCSummary(
        namespace=str(meta.get("namespace")),
        name=str(meta.get("name")),
        uid=str(meta.get("uid")),
        phase=(pvc.get("status") or {}).get("phase"),
        volume_name=(pvc.get("spec") or {}).get("volumeName"),
        storage_class=ko.storage_class(pvc),
        requested_storage=ko.requested_storage(pvc),
        access_modes=ko.access_modes(pvc),
        volume_mode=ko.volume_mode(pvc),
    )


def _pvc_summary_from_request(request: HandoverRequest) -> PVCSummary:
    return PVCSummary(
        namespace=request.new_pvc.namespace,
        name=request.new_pvc.name,
        uid=request.new_pvc.uid,
        phase=None,
    )


def _kube_api_error(exc: KubeError, what: str) -> ApiError:
    if isinstance(exc, NotFound):
        return ApiError(404, "kube_not_found", f"{what}：{exc}")
    if isinstance(exc, Conflict):
        return ApiError(409, "concurrent_update", f"{what}：{exc}")
    if isinstance(exc, Unavailable):
        return ApiError(503, "kube_unavailable", f"{what}：{exc}")
    return ApiError(502, "kube_error", f"{what}：{exc}")
