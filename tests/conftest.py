"""Fixtures and helpers shared by the test suite."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.schemas import ClaimRef, HandoverRequest  # noqa: E402
from app.service import HandoverService  # noqa: E402
from mock_kube.store import InMemoryKube  # noqa: E402

OLD_UID = "11111111-1111-4111-8111-111111111111"
NEW_UID = "22222222-2222-4222-8222-222222222222"
OTHER_UID = "33333333-3333-4333-8333-333333333333"

PV_NAME = "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab"


def pv_fixture(
    *,
    name: str = PV_NAME,
    phase: str = "Released",
    reclaim_policy: str = "Retain",
    claim_uid: str | None = OLD_UID,
    claim_ns: str = "monitoring",
    claim_name: str = "prometheus-data",
    capacity: str = "100Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict:
    spec: dict = {
        "capacity": {"storage": capacity},
        "accessModes": access_modes or ["ReadWriteOnce"],
        "persistentVolumeReclaimPolicy": reclaim_policy,
        "storageClassName": storage_class,
        "volumeMode": volume_mode,
    }
    if claim_uid is not None:
        spec["claimRef"] = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "namespace": claim_ns,
            "name": claim_name,
            "uid": claim_uid,
        }
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": spec,
        "status": {"phase": phase},
    }


def pvc_fixture(
    *,
    name: str = "prometheus-data-new",
    namespace: str = "monitoring",
    uid: str = NEW_UID,
    volume_name: str | None = PV_NAME,
    phase: str = "Pending",
    capacity: str = "100Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict:
    spec: dict = {
        "accessModes": access_modes or ["ReadWriteOnce"],
        "resources": {"requests": {"storage": capacity}},
        "storageClassName": storage_class,
        "volumeMode": volume_mode,
    }
    if volume_name is not None:
        spec["volumeName"] = volume_name
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": namespace, "uid": uid},
        "spec": spec,
        "status": {"phase": phase},
    }


def make_request(
    *,
    pv_name: str = PV_NAME,
    old_uid: str = OLD_UID,
    namespace: str = "monitoring",
    name: str = "prometheus-data-new",
    uid: str = NEW_UID,
) -> HandoverRequest:
    return HandoverRequest(
        pv_name=pv_name,
        old_pvc_uid=old_uid,
        new_pvc=ClaimRef(namespace=namespace, name=name, uid=uid),
    )


@pytest.fixture
def kube() -> InMemoryKube:
    store = InMemoryKube()
    store.create_pv(pv_fixture())
    store.create_pvc("monitoring", pvc_fixture())
    return store


@pytest.fixture
def service(kube: InMemoryKube) -> HandoverService:
    return HandoverService(kube)

