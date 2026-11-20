"""Tests for the optimistic-concurrency path.

Under normal scheduling the read→write window is too short to collide, so
these tests drive the conflict branch deterministically by making the fake
API server accept a competing write between the service's read and its patch.
"""

from __future__ import annotations

import pytest

from app.errors import ApiError
from app.kube import Conflict
from app.service import HandoverService
from mock_kube.store import InMemoryKube
from tests.conftest import (
    NEW_UID,
    OLD_UID,
    OTHER_UID,
    PV_NAME,
    make_request,
    pv_fixture,
    pvc_fixture,
)


class RacingKube(InMemoryKube):
    """Injects one competing reservation just before the next PV write."""

    def __init__(self, rival_uid: str | None = None, normalize: bool = False) -> None:
        super().__init__()
        self.rival_uid = rival_uid
        self.normalize = normalize
        self.injected = False
        self.conflicts = 0

    def update_pv(self, name, body):
        if not self.injected:
            self.injected = True
            if self.rival_uid is not None:
                # Another operator wins the race with a different claimant.
                pv = self.get_pv(name)
                super().update_pv(
                    name,
                    {
                        "metadata": {"resourceVersion": pv["metadata"]["resourceVersion"]},
                        "spec": {"claimRef": {
                            "namespace": "monitoring",
                            "name": "rival",
                            "uid": self.rival_uid,
                        }},
                    },
                )
            elif self.normalize:
                # A controller touches the object (fresh resourceVersion) but
                # ownership is unchanged: the service should retry and win.
                pv = self.get_pv(name)
                super().update_pv(
                    name,
                    {
                        "metadata": {
                            "resourceVersion": pv["metadata"]["resourceVersion"],
                            "annotations": {"example.com/touched-by": "controller"},
                        }
                    },
                )
        try:
            return super().update_pv(name, body)
        except Conflict:
            self.conflicts += 1
            raise


def _seed(kube: InMemoryKube) -> None:
    kube.create_pv(pv_fixture())
    kube.create_pvc("monitoring", pvc_fixture())


def test_loser_of_a_race_gets_conflict_and_does_not_overwrite():
    kube = RacingKube(rival_uid=OTHER_UID)
    _seed(kube)
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())

    assert exc.value.status_code == 409
    assert exc.value.code == "pv_reserved_for_other_claim"
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == OTHER_UID


def test_unrelated_write_retries_with_fresh_resource_version():
    kube = RacingKube(normalize=True)
    _seed(kube)
    service = HandoverService(kube)

    response = service.submit(make_request())

    assert response.status == "pending_binding"
    pv = kube.get_pv(PV_NAME)
    assert pv["spec"]["claimRef"]["uid"] == NEW_UID
    assert pv["metadata"]["annotations"]["example.com/touched-by"] == "controller"
    assert kube.conflicts == 1, "exactly one conflict should have been retried"


class _ExhaustedKube(InMemoryKube):
    """Every write conflicts, to exercise the give-up branch."""

    def update_pv(self, name, body):
        pv = self.get_pv(name)
        super().update_pv(
            name,
            {
                "metadata": {
                    "resourceVersion": pv["metadata"]["resourceVersion"],
                    "annotations": {"example.com/noisy": "controller"},
                }
            },
        )
        return super().update_pv(name, body)


def test_persistent_conflicts_surface_as_retryable_409():
    kube = _ExhaustedKube()
    _seed(kube)
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())

    assert exc.value.status_code == 409
    assert exc.value.code == "concurrent_update"
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == OLD_UID, "never overwritten"


def test_reservation_annotation_records_previous_uid():
    kube = InMemoryKube()
    _seed(kube)
    service = HandoverService(kube)
    service.submit(make_request())

    annotations = kube.get_pv(PV_NAME)["metadata"]["annotations"]
    assert annotations["handover.example.com/previous-pvc-uid"] == OLD_UID
    assert annotations["handover.example.com/reserved-at"].endswith("Z")
