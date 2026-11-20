"""Request/response models for the volume handover API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

DNS_SUBDOMAIN = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
DNS_LABEL = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"


class ClaimRef(BaseModel):
    """Identifies one PersistentVolumeClaim instance (UID disambiguates renames)."""

    model_config = ConfigDict(extra="forbid")

    namespace: str = Field(..., pattern=DNS_LABEL, max_length=63, examples=["monitoring"])
    name: str = Field(..., pattern=DNS_LABEL, max_length=253, examples=["prometheus-data-0"])
    uid: str = Field(..., min_length=1, max_length=128, examples=["0f7a9a53-9d1f-4a1a-9f43-3a7c1f0d2b11"])


class HandoverRequest(BaseModel):
    """Request to hand a Released, Retain PV over to a pre-bound new claim."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "pv_name": "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab",
                    "old_pvc_uid": "6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33",
                    "new_pvc": {
                        "namespace": "monitoring",
                        "name": "prometheus-data-rebuilt",
                        "uid": "9c2e4a71-2222-4d3b-8e55-bb33cc44dd55",
                    },
                }
            ]
        },
    )

    pv_name: str = Field(
        ..., pattern=DNS_SUBDOMAIN, max_length=253, examples=["pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab"]
    )
    old_pvc_uid: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="UID of the claim the volume is currently reserved for "
        "(the deleted original claim, or a previously designated claim that is gone).",
        examples=["6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33"],
    )
    new_pvc: ClaimRef = Field(..., description="Pre-created, pre-bound replacement claim.")

    @property
    def new_claim_key(self) -> str:
        return f"{self.new_pvc.namespace}/{self.new_pvc.name}"


class PVSummary(BaseModel):
    name: str
    uid: str
    phase: str | None = None
    reclaim_policy: str | None = None
    storage_class: str = ""
    capacity: str | None = None
    access_modes: list[str] = Field(default_factory=list)
    volume_mode: str = "Filesystem"
    claim_ref: dict[str, Any] | None = None


class PVCSummary(BaseModel):
    namespace: str
    name: str
    uid: str
    phase: str | None = None
    volume_name: str | None = None
    storage_class: str = ""
    requested_storage: str | None = None
    access_modes: list[str] = Field(default_factory=list)
    volume_mode: str = "Filesystem"


class CheckResult(BaseModel):
    name: str
    passed: bool
    detail: str = ""


class HandoverResponse(BaseModel):
    """Outcome of a handover submission or status query."""

    status: Literal["pending_binding", "completed"] = Field(
        ...,
        description="pending_binding = the volume is reserved for the new claim but the "
        "PV/PVC pair is not fully bound yet; completed = both objects are Bound and "
        "point at each other.",
    )
    status_text: str = Field(..., description="Operator-facing status in Chinese.")
    pv_name: str
    old_pvc_uid: str
    new_pvc: PVCSummary | None
    reservation: dict[str, Any] | None = Field(
        default=None,
        description="Snapshot of the PV claimRef after this call.",
    )
    checks: list[CheckResult] = Field(default_factory=list)
    pv: PVSummary | None = None
    message: str = ""


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody


class HealthResponse(BaseModel):
    status: str
    kubernetes: str
    detail: str = ""
