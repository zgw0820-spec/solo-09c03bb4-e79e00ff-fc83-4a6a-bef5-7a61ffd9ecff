"""Demo fixtures for the local Kubernetes API stand-in.

With ``MOCK_SEED=1`` the mock server starts pre-populated with the scenario an
operator would meet during a monitoring rebuild:

* a ``Released`` ``Retain`` volume that used to belong to a Prometheus claim
  whose PVC has already been deleted,
* a replacement PVC that is pre-bound to that volume but not yet Bound,
* a rival claim and a ``Delete``-policy volume to demonstrate the rejection
  paths.

UIDs are stable, so the JSON bodies in the README and the Swagger examples can
be copied verbatim.
"""

from __future__ import annotations

from typing import Any

from mock_kube.store import InMemoryKube

PROM_PV = "pvc-5f0c1c9e-aaaa-4bbb-8ccc-0123456789ab"
PROM_OLD_UID = "6b1d0f2e-1111-4c2a-9c33-aa11bb22cc33"
PROM_NEW_UID = "9c2e4a71-2222-4d3b-8e55-bb33cc44dd55"
RIVAL_UID = "3d8f6b20-3333-4a7c-9d11-cc55dd66ee77"

DELETE_PV = "pvc-delete-policy-0000-0000-000000000000"
DELETE_OLD_UID = "7a2f3c11-4444-4b5c-8d66-dd77ee88ff99"

VOLUME_MODE_PV = "pvc-wrong-mode-1111-1111-111111111111"
WRONG_MODE_OLD_UID = "8b3a4d22-5555-4c6d-9e77-ee88ff99aa00"

NAMESPACE = "monitoring"
OLD_CLAIM = "prometheus-data"
NEW_CLAIM = "prometheus-data-rebuilt"
RIVAL_CLAIM = "prometheus-data-rival"
WRONG_MODE_CLAIM = "prometheus-data-block"

# Extra pre-reserved volumes for demonstrating an incremental reconcile round
# (and the "change a few volumes, then look at the next round" flow). Both PVs
# already carry a claimRef to their replacement claim, but the claims are not
# pre-bound yet (no ``spec.volumeName``), so the first round attempts them and
# leaves them unbound until the claims themselves change.
EXTRA_PVS = (
    "pvc-batch-aaaa-0000-0000-000000000001",
    "pvc-batch-bbbb-0000-0000-000000000002",
)
EXTRA_OLD_UIDS = (
    "b1a1c0de-0001-4a01-8a01-000000000001",
    "b1a1c0de-0002-4a02-8a02-000000000002",
)
EXTRA_NEW_UIDS = (
    "b1a1c0de-0001-4a01-8a01-000000000011",
    "b1a1c0de-0002-4a02-8a02-000000000012",
)
EXTRA_CLAIMS = ("grafana-data-rebuilt", "alertmanager-data-rebuilt")


def _extra_pvs() -> list[dict[str, Any]]:
    """Volumes already reserved for their rebuilt claim (claimRef = new UID)."""
    return [
        _pv(name, claim_name=claim, claim_uid=new_uid)
        for name, new_uid, claim in zip(EXTRA_PVS, EXTRA_NEW_UIDS, EXTRA_CLAIMS)
    ]


def _extra_pvcs() -> list[tuple[str, dict[str, Any]]]:
    """Replacement claims, created but not yet pre-bound (no volumeName)."""
    return [
        (
            NAMESPACE,
            _pvc(claim, new_uid, volume_name=None, capacity="50Gi"),
        )
        for name, new_uid, claim in zip(EXTRA_PVS, EXTRA_NEW_UIDS, EXTRA_CLAIMS)
    ]


def _pv(
    name: str,
    *,
    claim_name: str,
    claim_uid: str,
    phase: str = "Released",
    policy: str = "Retain",
    capacity: str = "200Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolume",
        "metadata": {"name": name},
        "spec": {
            "capacity": {"storage": capacity},
            "accessModes": access_modes or ["ReadWriteOnce"],
            "persistentVolumeReclaimPolicy": policy,
            "storageClassName": storage_class,
            "volumeMode": volume_mode,
            "claimRef": {
                "apiVersion": "v1",
                "kind": "PersistentVolumeClaim",
                "namespace": NAMESPACE,
                "name": claim_name,
                "uid": claim_uid,
            },
        },
        "status": {"phase": phase},
    }


def _pvc(
    name: str,
    uid: str,
    *,
    volume_name: str,
    capacity: str = "150Gi",
    storage_class: str = "fast-ssd",
    access_modes: list[str] | None = None,
    volume_mode: str = "Filesystem",
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": {"name": name, "namespace": NAMESPACE, "uid": uid},
        "spec": {
            "accessModes": access_modes or ["ReadWriteOnce"],
            "resources": {"requests": {"storage": capacity}},
            "storageClassName": storage_class,
            "volumeMode": volume_mode,
            "volumeName": volume_name,
        },
        "status": {"phase": "Pending"},
    }


DEMO_BODIES = {
    "handover": {
        "pv_name": PROM_PV,
        "old_pvc_uid": PROM_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": NEW_CLAIM, "uid": PROM_NEW_UID},
    },
    "rival": {
        "pv_name": PROM_PV,
        "old_pvc_uid": PROM_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": RIVAL_CLAIM, "uid": RIVAL_UID},
    },
    "delete_policy": {
        "pv_name": DELETE_PV,
        "old_pvc_uid": DELETE_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": NEW_CLAIM, "uid": PROM_NEW_UID},
    },
    "wrong_volume_mode": {
        "pv_name": VOLUME_MODE_PV,
        "old_pvc_uid": WRONG_MODE_OLD_UID,
        "new_pvc": {"namespace": NAMESPACE, "name": WRONG_MODE_CLAIM, "uid": RIVAL_UID},
    },
    "extra_handovers": [
        {
            "pv_name": name,
            "old_pvc_uid": old_uid,
            "new_pvc": {"namespace": NAMESPACE, "name": claim, "uid": new_uid},
        }
        for name, old_uid, new_uid, claim in zip(
            EXTRA_PVS, EXTRA_OLD_UIDS, EXTRA_NEW_UIDS, EXTRA_CLAIMS
        )
    ],
}


def seed(store: InMemoryKube) -> None:
    store.reset()

    store.create_pv(_pv(PROM_PV, claim_name=OLD_CLAIM, claim_uid=PROM_OLD_UID))
    store.create_pvc(
        NAMESPACE,
        _pvc(NEW_CLAIM, PROM_NEW_UID, volume_name=PROM_PV, capacity="150Gi"),
    )
    store.create_pvc(
        NAMESPACE,
        _pvc(RIVAL_CLAIM, RIVAL_UID, volume_name=PROM_PV, capacity="150Gi"),
    )

    store.create_pv(
        _pv(
            DELETE_PV,
            claim_name="loki-data",
            claim_uid=DELETE_OLD_UID,
            policy="Delete",
        )
    )

    store.create_pv(
        _pv(
            VOLUME_MODE_PV,
            claim_name="thanos-data",
            claim_uid=WRONG_MODE_OLD_UID,
            volume_mode="Block",
        )
    )
    store.create_pvc(
        NAMESPACE,
        _pvc(
            WRONG_MODE_CLAIM,
            RIVAL_UID,
            volume_name=VOLUME_MODE_PV,
            volume_mode="Filesystem",
        ),
    )

    # Two extra volumes already reserved for their rebuilt claims, so a
    # reconcile round has more than one volume to work through.
    for pv in _extra_pvs():
        store.create_pv(pv)
    for namespace, pvc in _extra_pvcs():
        store.create_pvc(namespace, pvc)
