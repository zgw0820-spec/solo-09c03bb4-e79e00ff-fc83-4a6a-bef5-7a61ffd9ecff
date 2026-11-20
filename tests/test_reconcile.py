"""Incremental reconcile in the mock Kubernetes API server.

The round must only touch volumes that were registered by a create/update/
delete before the round started, must locate a claim's volumes by its
namespace/name (never by scanning every PV), and must defer a volume whose PV
or claim changed after the round snapshot. Objects that are not bindable stay
quiet until another change re-registers them.
"""

from __future__ import annotations

import threading

import pytest

from app.errors import ApiError
from app.service import HandoverService
from mock_kube.store import InMemoryKube
from tests.conftest import (
    NEW_UID,
    OLD_UID,
    PV_NAME,
    make_request,
    pv_fixture,
    pvc_fixture,
)

OTHER_PV = "pvc-other-0000-0000-000000000000"
OTHER_OLD_UID = "99999999-9999-4999-8999-999999999999"
OTHER_PVC_NAME = "other-data-new"
OTHER_NEW_UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _seed_two_volumes() -> InMemoryKube:
    kube = InMemoryKube()
    kube.create_pv(pv_fixture())
    kube.create_pvc("monitoring", pvc_fixture())
    kube.create_pv(
        pv_fixture(name=OTHER_PV, claim_uid=OTHER_OLD_UID, claim_name="other-data")
    )
    kube.create_pvc(
        "monitoring",
        pvc_fixture(name=OTHER_PVC_NAME, uid=OTHER_NEW_UID, volume_name=OTHER_PV),
    )
    return kube


def _reserve_both(kube: InMemoryKube) -> None:
    service = HandoverService(kube)
    service.submit(make_request())
    service.submit(
        make_request(
            pv_name=OTHER_PV,
            old_uid=OTHER_OLD_UID,
            name=OTHER_PVC_NAME,
            uid=OTHER_NEW_UID,
        )
    )


def test_round_attempts_only_volumes_registered_since_last_round():
    kube = _seed_two_volumes()
    _reserve_both(kube)

    first = kube.reconcile()
    assert first["round"] == 1
    assert sorted(first["attempted"]) == sorted([PV_NAME, OTHER_PV])
    assert sorted(first["bound"]) == sorted([PV_NAME, OTHER_PV])

    # Nothing changed: the next round has nothing registered and attempts nothing.
    second = kube.reconcile()
    assert second["attempted"] == []
    assert second["bound"] == []
    assert second["registered"] == 0


def test_update_of_one_volume_registers_only_that_volume():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    kube.reconcile()

    pv = kube.get_pv(OTHER_PV)
    kube.update_pv(
        OTHER_PV,
        {
            "metadata": {"resourceVersion": pv["metadata"]["resourceVersion"]},
            "spec": {"claimRef": {"namespace": "monitoring", "name": OTHER_PVC_NAME,
                                  "uid": OTHER_NEW_UID}},
        },
    )

    state = kube.reconcile_state()
    assert state["registered"] == [OTHER_PV]

    round_result = kube.reconcile()
    assert round_result["attempted"] == [OTHER_PV]


def test_pvc_change_registers_associated_volumes_by_namespace_and_name():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    kube.reconcile()

    # Recreating the claim under the same name is a change to the same key.
    kube.delete_pvc("monitoring", OTHER_PVC_NAME)
    assert kube.reconcile_state()["registered"] == [OTHER_PV]

    kube.reconcile()
    kube.create_pvc(
        "monitoring",
        pvc_fixture(name=OTHER_PVC_NAME, uid=OTHER_NEW_UID, volume_name=OTHER_PV),
    )
    assert kube.reconcile_state()["registered"] == [OTHER_PV]
    assert kube.reconcile()["attempted"] == [OTHER_PV]


def test_pvc_change_does_not_scan_unrelated_volumes():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    kube.reconcile()
    before = kube.reconcile_state()["registered"]

    # A claim created out of the blue is not associated with any volume yet;
    # it must not make every PV pending.
    kube.create_pvc(
        "monitoring", pvc_fixture(name="unrelated", uid="b" * 32, volume_name=None)
    )
    assert kube.reconcile_state()["registered"] == before


def test_volume_deleted_before_round_is_dropped_and_not_attempted():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    kube.delete_pv(OTHER_PV)

    state = kube.reconcile_state()
    assert state["registered"] == [PV_NAME]

    result = kube.reconcile()
    assert result["attempted"] == [PV_NAME]
    assert OTHER_PV not in result["skipped"]


def test_volume_created_mid_round_waits_for_next_round():
    kube = _seed_two_volumes()
    _reserve_both(kube)

    third_pv = "pvc-third-0000-0000-000000000000"
    third_old_uid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    third_new_uid = "ffffffff-ffff-4fff-8fff-ffffffffffff"
    third_claim = "third-data-new"

    original_bind = kube.bind_pv
    created = {"done": False}

    def bind_then_create(name: str):
        result = original_bind(name)
        if not created["done"]:
            created["done"] = True
            kube.create_pv(
                pv_fixture(name=third_pv, claim_uid=third_old_uid, claim_name="third-data")
            )
            kube.create_pvc(
                "monitoring",
                pvc_fixture(name=third_claim, uid=third_new_uid, volume_name=third_pv),
            )
            HandoverService(kube).submit(
                make_request(
                    pv_name=third_pv,
                    old_uid=third_old_uid,
                    name=third_claim,
                    uid=third_new_uid,
                )
            )
        return result

    kube.bind_pv = bind_then_create  # type: ignore[method-assign]

    first = kube.reconcile()
    assert first["registered"] == 2
    assert third_pv not in first["attempted"], "new work waits for the next round"
    assert third_pv not in first["bound"]

    state = kube.reconcile_state()
    assert state["registered"] == [third_pv]

    second = kube.reconcile()
    assert second["attempted"] == [third_pv]
    assert second["bound"] == [third_pv]


def test_volume_changed_after_snapshot_but_before_its_attempt_is_deferred():
    kube = _seed_two_volumes()
    _reserve_both(kube)

    # While the first volume is being attempted, the second one is modified.
    # The round already took its snapshot, so the second volume must be
    # deferred, not attempted with stale pre-change observations.
    original_bind = kube.bind_pv
    touched = {"done": False}

    def bind_then_touch(name: str):
        result = original_bind(name)
        if name == PV_NAME and not touched["done"]:
            touched["done"] = True
            pv = kube.get_pv(OTHER_PV)
            kube.update_pv(
                OTHER_PV,
                {
                    "metadata": {
                        "resourceVersion": pv["metadata"]["resourceVersion"],
                        "annotations": {"example.com/touched": "controller"},
                    }
                },
            )
        return result

    kube.bind_pv = bind_then_touch  # type: ignore[method-assign]

    first = kube.reconcile()
    assert first["attempted"] == [PV_NAME]
    assert first["deferred"] == [OTHER_PV]
    assert first["bound"] == [PV_NAME]
    assert kube.get_pv(OTHER_PV)["status"]["phase"] == "Released"

    second = kube.reconcile()
    assert second["attempted"] == [OTHER_PV]
    assert second["bound"] == [OTHER_PV]


def test_delete_and_same_name_recreate_between_rounds_is_detected():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    kube.reconcile()

    # Delete + recreate under the same name between rounds: the new object
    # must be registered again even though its resourceVersion restarts at 1.
    kube.delete_pv(OTHER_PV)
    kube.create_pv(
        pv_fixture(name=OTHER_PV, claim_uid=OTHER_NEW_UID, claim_name=OTHER_PVC_NAME)
    )
    assert kube.reconcile_state()["registered"] == [OTHER_PV]
    result = kube.reconcile()
    assert result["attempted"] == [OTHER_PV]
    assert result["bound"] == [OTHER_PV]


def test_same_name_rebuild_between_snapshot_and_attempt_defers_round():
    kube = _seed_two_volumes()
    _reserve_both(kube)

    # The claim is rebuilt under the same name after the round snapshot but
    # before the volume is attempted. ``bind_pv`` then sees the new UID, so
    # the attempt fails; the round must not treat that stale observation as
    # final and must leave the volume for the next round.
    original_bind = kube.bind_pv
    rebuilt_uid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    state = {"done": False}

    def rebuild_then_bind(name: str):
        if name == OTHER_PV and not state["done"]:
            state["done"] = True
            kube.delete_pvc("monitoring", OTHER_PVC_NAME)
            kube.create_pvc(
                "monitoring",
                pvc_fixture(
                    name=OTHER_PVC_NAME, uid=rebuilt_uid, volume_name=OTHER_PV
                ),
            )
        return original_bind(name)

    kube.bind_pv = rebuild_then_bind  # type: ignore[method-assign]
    kube.reconcile()

    # The rebuilt claim belongs to a different PVC instance, so its UID does
    # not match the reservation; the volume is still released and pending.
    assert kube.get_pv(OTHER_PV)["status"]["phase"] == "Released"
    assert kube.reconcile_state()["registered"] == [OTHER_PV]

    # The next round re-reads the latest claim and gives up cleanly; it only
    # attempts again when another change arrives.
    retry = kube.reconcile()
    assert retry["attempted"] == [OTHER_PV]
    assert retry["bound"] == []
    assert kube.reconcile()["attempted"] == []


def test_unmet_conditions_wait_for_the_next_related_change():
    kube = InMemoryKube()
    kube.create_pv(pv_fixture())
    # The reservation is already in place (as written by a handover earlier),
    # but the claim is not pre-bound to the volume: bind must fail.
    kube.create_pvc("monitoring", pvc_fixture(volume_name=None))
    pv = kube.get_pv(PV_NAME)
    kube.update_pv(
        PV_NAME,
        {
            "metadata": {"resourceVersion": pv["metadata"]["resourceVersion"]},
            "spec": {
                "claimRef": {"namespace": "monitoring", "name": "prometheus-data-new",
                             "uid": NEW_UID}
            },
        },
    )

    first = kube.reconcile()
    assert first["attempted"] == [PV_NAME]
    assert first["bound"] == []
    assert kube.reconcile()["attempted"] == [], "no retry without a change"

    # Recreate the claim pre-bound to the volume: that is a related change,
    # so it registers the volume again and the next round binds.
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture())
    second = kube.reconcile()
    assert second["attempted"] == [PV_NAME]
    assert second["bound"] == [PV_NAME]


def test_expired_reservation_uid_does_not_bind_rebuilt_claim():
    kube = InMemoryKube()
    kube.create_pv(pv_fixture())
    kube.create_pvc("monitoring", pvc_fixture())
    service = HandoverService(kube)
    service.submit(make_request())
    # Round 1 binds the original pair; then the claim is rebuilt under the
    # same name. The PV still carries the old UID in claimRef, so it must not
    # be silently re-bound to the new claim instance.
    kube.reconcile()
    assert kube.get_pv(PV_NAME)["status"]["phase"] == "Bound"

    rebuilt_uid = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
    kube.delete_pvc("monitoring", "prometheus-data-new")
    kube.create_pvc("monitoring", pvc_fixture(uid=rebuilt_uid))
    result = kube.reconcile()
    assert result["attempted"] == [PV_NAME]
    assert result["bound"] == []
    # The volume goes back to Released when its claim is deleted (Retain), and
    # the rebuilt claim has a different UID, so it is not re-bound.
    assert kube.get_pv(PV_NAME)["status"]["phase"] == "Released"
    assert kube.get_pvc("monitoring", "prometheus-data-new")["status"]["phase"] == "Pending"

    # The request for the old PVC instance is rejected instead of reporting
    # success against the rebuilt claim.
    with pytest.raises(ApiError) as exc:
        service.status(make_request())
    assert exc.value.code == "new_pvc_uid_mismatch"


def test_reads_and_handover_submits_proceed_while_a_round_is_running():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    rebuilt_uid = "eeeeeeee-1111-4eee-8eee-eeeeeeeeeeee"
    kube.delete_pvc("monitoring", OTHER_PVC_NAME)
    kube.create_pvc(
        "monitoring",
        pvc_fixture(name=OTHER_PVC_NAME, uid=rebuilt_uid, volume_name=OTHER_PV),
    )

    observed: dict[str, object] = {}
    original_bind = kube.bind_pv

    def slow_bind(name: str):
        result = original_bind(name)
        if "submit" not in observed:
            # While the round is in progress, a read, a status query and a
            # handover submit must still complete instead of blocking.
            observed["read"] = kube.get_pv(name)["metadata"]["name"]
            observed["list"] = len(kube.list_pvs())
            observed["round"] = kube.reconcile_state()["round"]

            outcomes: list[str] = []

            def submit() -> None:
                try:
                    outcomes.append(
                        HandoverService(kube).submit(
                            make_request(
                                pv_name=OTHER_PV,
                                old_uid=OTHER_NEW_UID,
                                name=OTHER_PVC_NAME,
                                uid=rebuilt_uid,
                            )
                        ).status
                    )
                except Exception as exc:  # noqa: BLE001
                    outcomes.append(type(exc).__name__)

            worker = threading.Thread(target=submit)
            worker.start()
            worker.join(timeout=5)
            observed["submit"] = outcomes
        return result

    kube.bind_pv = slow_bind  # type: ignore[method-assign]

    round_thread = threading.Thread(target=kube.reconcile)
    round_thread.start()
    round_thread.join(timeout=10)

    assert not round_thread.is_alive(), "round must not deadlock"
    assert observed["read"] in (PV_NAME, OTHER_PV)
    assert observed["list"] == 2
    assert observed["round"] == 1
    assert observed["submit"] == ["pending_binding"]


def test_round_result_reports_registered_attempted_and_bound():
    kube = _seed_two_volumes()
    _reserve_both(kube)
    result = kube.reconcile()

    assert set(result) == {
        "round",
        "registered",
        "attempted",
        "bound",
        "deferred",
        "skipped",
        "pending",
    }
    assert result["registered"] == 2
    assert result["pending"] == 0


def test_old_uid_reservation_survives_and_round_reports_no_binding():
    # A volume reserved for a claim that no longer exists at all: the round
    # attempts it, fails, and reports no binding without raising.
    kube = InMemoryKube()
    kube.create_pv(pv_fixture(claim_uid=OLD_UID))
    kube.create_pvc("monitoring", pvc_fixture())
    service = HandoverService(kube)
    service.submit(make_request())
    kube.delete_pvc("monitoring", "prometheus-data-new")

    result = kube.reconcile()
    assert result["attempted"] == [PV_NAME]
    assert result["bound"] == []
    assert kube.get_pv(PV_NAME)["status"]["phase"] != "Bound"
