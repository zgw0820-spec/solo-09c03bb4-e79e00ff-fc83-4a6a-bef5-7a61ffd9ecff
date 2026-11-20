"""Unit tests for the handover rules, run directly against the in-memory store."""

from __future__ import annotations

import threading

import pytest

from app.errors import ApiError
from app.service import HandoverService
from tests.conftest import (
    NEW_UID,
    OLD_UID,
    OTHER_UID,
    PV_NAME,
    make_request,
    pv_fixture,
    pvc_fixture,
)


# ----------------------------------------------------------------- happy paths
def test_submit_reserves_volume_and_reports_pending(service, kube):
    response = service.submit(make_request())

    assert response.status == "pending_binding"
    assert response.pv_name == PV_NAME
    pv = kube.get_pv(PV_NAME)
    assert pv["spec"]["claimRef"]["uid"] == NEW_UID
    assert pv["spec"]["claimRef"]["name"] == "prometheus-data-new"
    assert pv["metadata"]["annotations"]["handover.example.com/previous-pvc-uid"] == OLD_UID


def test_status_completes_after_both_sides_bound(service, kube):
    service.submit(make_request())
    assert service.status(make_request()).status == "pending_binding"

    kube.bind_pv(PV_NAME)

    response = service.status(make_request())
    assert response.status == "completed"
    assert "交接完成" in response.status_text

    pv = kube.get_pv(PV_NAME)
    pvc = kube.get_pvc("monitoring", "prometheus-data-new")
    assert pv["status"]["phase"] == "Bound"
    assert pvc["status"]["phase"] == "Bound"
    assert pv["spec"]["claimRef"]["uid"] == pvc["metadata"]["uid"]
    assert pvc["spec"]["volumeName"] == pv["metadata"]["name"]


def test_repeated_submit_is_idempotent_and_reports_progress(service, kube):
    first = service.submit(make_request())
    rv_after_first = kube.get_pv(PV_NAME)["metadata"]["resourceVersion"]
    patches_after_first = len(kube.patch_log)

    second = service.submit(make_request())
    kube.bind_pv(PV_NAME)
    third = service.submit(make_request())

    assert first.status == second.status == "pending_binding"
    assert third.status == "completed"
    assert len(kube.patch_log) == patches_after_first, "retries must not rewrite the PV"
    assert kube.get_pv(PV_NAME)["metadata"]["resourceVersion"] != rv_after_first  # binder wrote


def test_completed_submit_returns_200_semantics_on_retry(service, kube):
    service.submit(make_request())
    kube.bind_pv(PV_NAME)
    response = service.submit(make_request())
    assert response.status == "completed"


# ------------------------------------------------------------- precondition gates
def test_rejects_non_retain_policy(kube):
    kube.delete_pv(PV_NAME)
    kube.create_pv(pv_fixture(reclaim_policy="Delete"))
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "reclaim_policy_not_retain"


@pytest.mark.parametrize("phase", ["Bound", "Available", "Pending"])
def test_rejects_volume_not_released(kube, phase):
    kube.delete_pv(PV_NAME)
    kube.create_pv(pv_fixture(phase=phase))
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "volume_not_released"


def test_rejects_pv_without_claim_ref(kube):
    kube.delete_pv(PV_NAME)
    kube.create_pv(pv_fixture(claim_uid=None, phase="Available"))
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "volume_not_released"


def test_rejects_wrong_old_uid(kube):
    service = HandoverService(kube)
    with pytest.raises(ApiError) as exc:
        service.submit(make_request(old_uid=OTHER_UID))
    assert exc.value.code == "pv_reserved_for_other_claim"
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == OLD_UID


def test_missing_pv_is_404(service):
    with pytest.raises(ApiError) as exc:
        service.submit(make_request(pv_name="nope"))
    assert exc.value.status_code == 404
    assert exc.value.code == "pv_not_found"


# ------------------------------------------------------------ new claim gates
def test_requires_new_pvc_to_exist(kube):
    kube.delete_pvc("monitoring", "prometheus-data-new")
    service = HandoverService(kube)
    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "new_pvc_not_found"
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == OLD_UID


def test_requires_new_pvc_to_be_prebound(kube):
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(volume_name=None))
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "volume_incompatible"
    failed = {c["name"] for c in exc.value.details["checks"] if not c["passed"]}
    assert "pvc_prebound_to_pv" in failed


def test_requires_new_pvc_to_point_at_this_volume(kube):
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(volume_name="some-other-pv"))
    service = HandoverService(kube)
    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "volume_incompatible"


@pytest.mark.parametrize(
    "kwargs, expected_check",
    [
        ({"capacity": "150Gi"}, "capacity_satisfied"),
        ({"storage_class": "slow-hdd"}, "storage_class_matches"),
        ({"access_modes": ["ReadWriteMany"]}, "access_modes_supported"),
        ({"volume_mode": "Block"}, "volume_mode_matches"),
    ],
)
def test_capacity_class_modes_and_volume_mode_are_enforced(kube, kwargs, expected_check):
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(**kwargs))
    service = HandoverService(kube)

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "volume_incompatible"
    failed = {c["name"] for c in exc.value.details["checks"] if not c["passed"]}
    assert expected_check in failed
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == OLD_UID, "no write on rejection"


def test_capacity_comparison_is_numeric_not_lexical(kube):
    # 100Gi == 107374182400 bytes; a "1000Gi" string must be larger, and
    # "100G" must be smaller than "100Gi" despite the longer string.
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(capacity="100G"))
    service = HandoverService(kube)
    response = service.submit(make_request())
    assert response.status == "pending_binding"

    failed = {c.name for c in response.checks if not c.passed}
    assert "capacity_satisfied" not in failed

    kube.delete_pv(PV_NAME)
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pv(pv_fixture())
    kube.create_pvc("monitoring", pvc_fixture(capacity="100Gi"))
    service2 = HandoverService(kube)
    response2 = service2.submit(make_request())
    assert response2.status == "pending_binding"


def test_access_mode_subset_is_accepted(kube):
    kube.delete_pv(PV_NAME)
    kube.create_pv(pv_fixture(access_modes=["ReadWriteOnce", "ReadOnlyMany"]))
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(access_modes=["ReadOnlyMany"]))
    service = HandoverService(kube)
    response = service.submit(make_request())
    assert response.status == "pending_binding"


# --------------------------------------------------------------- concurrency
def test_concurrent_different_claimants_only_one_wins(kube):
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(name="claim-a", uid=NEW_UID))
    kube.create_pvc("monitoring", pvc_fixture(name="claim-b", uid=OTHER_UID))
    service = HandoverService(kube)

    results: dict[str, object] = {}
    barrier = threading.Barrier(2)

    def attempt(label: str, name: str, uid: str) -> None:
        barrier.wait()
        try:
            results[label] = service.submit(
                make_request(name=name, uid=uid)
            ).status
        except ApiError as exc:
            results[label] = exc.code

    threads = [
        threading.Thread(target=attempt, args=("a", "claim-a", NEW_UID)),
        threading.Thread(target=attempt, args=("b", "claim-b", OTHER_UID)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [label for label, outcome in results.items() if outcome == "pending_binding"]
    losers = [label for label, outcome in results.items() if outcome == "pv_reserved_for_other_claim"]
    assert len(winners) == 1, results
    assert len(losers) == 1, results

    winner_uid = NEW_UID if winners[0] == "a" else OTHER_UID
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == winner_uid


def test_concurrent_retries_of_same_claim_all_succeed(kube):
    service = HandoverService(kube)
    outcomes: list[str] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(6)

    def attempt() -> None:
        barrier.wait()
        try:
            outcomes.append(service.submit(make_request()).status)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=attempt) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    assert outcomes == ["pending_binding"] * 6
    assert len(kube.patch_log) == 1, "only one reservation write for identical requests"


# ------------------------------------------------------- same-name rebuild rule
def test_recreated_new_pvc_makes_old_request_conflict(kube, service):
    service.submit(make_request())

    # The new PVC is deleted and recreated under the same name with a new UID.
    kube.delete_pvc("monitoring", "prometheus-data-new")
    rebuilt = "44444444-4444-4444-8444-444444444444"
    kube.create_pvc("monitoring", pvc_fixture(uid=rebuilt))

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.status_code == 409
    assert exc.value.code == "new_pvc_uid_mismatch"
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == NEW_UID

    with pytest.raises(ApiError) as exc2:
        service.status(make_request())
    assert exc2.value.status_code == 409

    # The rebuilt claim is a different claimant; it cannot take the volume
    # either, because the volume is still reserved for the original UID.
    with pytest.raises(ApiError) as exc3:
        service.submit(make_request(uid=rebuilt))
    assert exc3.value.code == "pv_reserved_for_other_claim"


def test_recreated_old_claim_uid_invalidates_request(kube, service):
    # New claim holds the reservation already; if the cluster's same-named new
    # PVC has a different UID the retry must not silently report success.
    service.submit(make_request())
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(uid="55555555-5555-4555-8555-555555555555"))

    with pytest.raises(ApiError) as exc:
        service.submit(make_request())
    assert exc.value.code == "new_pvc_uid_mismatch"


# ------------------------------------------------------------------ misc status
def test_status_before_any_handover_is_rejected(service):
    with pytest.raises(ApiError) as exc:
        service.status(make_request())
    assert exc.value.code == "no_such_handover"


def test_idempotent_retry_reports_missing_new_pvc(service, kube):
    """The reservation survives the new PVC briefly disappearing."""
    service.submit(make_request())
    kube.delete_pvc("monitoring", "prometheus-data-new")

    response = service.submit(make_request())
    assert response.status == "pending_binding"
    failed = {c.name for c in response.checks if not c.passed}
    assert "pvc_exists" in failed
    assert kube.get_pv(PV_NAME)["spec"]["claimRef"]["uid"] == NEW_UID


def test_status_does_not_mutate(service, kube):
    service.submit(make_request())
    before = len(kube.patch_log)
    service.status(make_request())
    assert len(kube.patch_log) == before


def test_reservation_snapshot_exposes_previous_uid(service):
    response = service.submit(make_request())
    assert response.reservation is not None
    assert response.reservation["previous_pvc_uid"] == OLD_UID
    assert response.reservation["uid"] == NEW_UID


def test_health_of_inmemory_probe(kube):
    # sanity: list_pvs is available so the API probe path works
    assert kube.list_pvs()
