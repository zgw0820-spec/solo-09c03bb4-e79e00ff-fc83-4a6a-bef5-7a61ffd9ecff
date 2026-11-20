"""In-memory Kubernetes core/v1 API server (a local stand-in for a real cluster).

It listens on the same paths a real API server uses for the handful of
operations the handover service needs, so pointing the service at it with a
plain ``kubeconfig`` (or ``KUBE_MODE=remote``) exercises the real HTTP client
path end to end.

It is deliberately *not* a general-purpose Kubernetes emulator: only PV/PVC
read, PV merge-patch and a small operator surface (create/delete fixtures,
run an incremental reconcile round) are implemented. Anything else returns 404.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.kube import Conflict, Invalid, NotFound
from mock_kube.store import InMemoryKube

log = logging.getLogger("mock_kube")


def _status_error(status: int, reason: str, message: str, details: dict | None = None) -> JSONResponse:
    body: dict[str, Any] = {
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": status,
    }
    if details:
        body["details"] = details
    return JSONResponse(status_code=status, content=body)


def create_app(store: InMemoryKube | None = None) -> FastAPI:
    state = store or InMemoryKube()
    if os.environ.get("MOCK_SEED", "").lower() in ("1", "true", "yes"):
        from mock_kube.seed import seed

        seed(state)
        log.info("mock API server seeded with the monitoring handover scenario")
    app = FastAPI(
        title="Local Kubernetes API stand-in",
        description=(
            "本地 Kubernetes API 模拟端，仅供交接服务联调。\n\n"
            "### 运维接口\n"
            "* `POST /_mock/pvs` 创建 PV\n"
            "* `POST /_mock/namespaces/{namespace}/pvcs` 创建 PVC\n"
            "* `DELETE /_mock/namespaces/{namespace}/pvcs/{name}` 删除 PVC（模拟旧声明被删）\n"
            "* `POST /_mock/bind/{pv}` 触发一次绑定\n"
            "* `POST /_mock/reconcile` 增量绑定一轮（只处理自上一轮以来变更过的卷）\n"
            "* `GET /_mock/reconcile/state` 查看本轮待处理的卷\n"
            "* `GET /_mock/state` 查看全部 PV/PVC\n"
            "* `POST /_mock/reset` 清空状态\n"
        ),
        version="1.0.0",
    )
    app.state.store = state
    _install(app, state)
    return app


def _install(app: FastAPI, store: InMemoryKube) -> None:
    # ---------------------------------------------------------- core/v1 reads
    @app.get("/api/v1/persistentvolumes")
    async def list_pvs(limit: int | None = None) -> dict[str, Any]:
        return _list("PersistentVolumeList", store.list_pvs(limit=limit))

    @app.get("/api/v1/persistentvolumes/{name}")
    async def read_pv(name: str):
        try:
            return store.get_pv(name)
        except NotFound as exc:
            return _status_error(
                404, "NotFound", str(exc), {"name": name, "kind": "persistentvolumes"}
            )

    @app.patch("/api/v1/persistentvolumes/{name}")
    async def patch_pv(name: str, request: Request):
        content_type = request.headers.get("content-type", "")
        if "merge-patch" not in content_type:
            return _status_error(
                415,
                "UnsupportedMediaType",
                f"mock server only accepts application/merge-patch+json, got {content_type!r}",
            )
        try:
            body = await request.json()
        except ValueError:
            return _status_error(400, "BadRequest", "request body is not valid JSON")
        try:
            return await run_in_threadpool(store.update_pv, name, body)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))
        except Conflict as exc:
            return _status_error(409, "Conflict", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.get("/api/v1/namespaces/{namespace}/persistentvolumeclaims")
    async def list_pvcs(namespace: str) -> dict[str, Any]:
        return _list("PersistentVolumeClaimList", store.list_pvcs(namespace))

    @app.get("/api/v1/namespaces/{namespace}/persistentvolumeclaims/{name}")
    async def read_pvc(namespace: str, name: str):
        try:
            return store.get_pvc(namespace, name)
        except NotFound as exc:
            return _status_error(
                404,
                "NotFound",
                str(exc),
                {"name": name, "kind": "persistentvolumeclaims"},
            )

    # ------------------------------------------------------ operator fixture API
    @app.post("/_mock/pvs", status_code=201)
    async def create_pv(request: Request):
        body = await request.json()
        try:
            return store.create_pv(body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.post("/_mock/namespaces/{namespace}/pvcs", status_code=201)
    async def create_pvc(namespace: str, request: Request):
        body = await request.json()
        try:
            return store.create_pvc(namespace, body)
        except Conflict as exc:
            return _status_error(409, "AlreadyExists", str(exc))
        except Invalid as exc:
            return _status_error(422, "Invalid", str(exc))

    @app.delete("/_mock/namespaces/{namespace}/pvcs/{name}")
    async def delete_pvc(namespace: str, name: str):
        try:
            return store.delete_pvc(namespace, name)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))

    @app.delete("/_mock/pvs/{name}")
    async def delete_pv(name: str):
        try:
            return store.delete_pv(name)
        except NotFound as exc:
            return _status_error(404, "NotFound", str(exc))

    @app.post("/_mock/bind/{pv_name}")
    async def bind(pv_name: str):
        try:
            return store.bind_pv(pv_name)
        except (Conflict, NotFound) as exc:
            return _status_error(409, "Conflict", str(exc))

    @app.post("/_mock/reconcile")
    async def reconcile() -> dict[str, Any]:
        # The round serialises on the store's round lock, but the underlying
        # object lock is only held per volume, so reads and handover submits
        # keep being served while it runs.
        return await run_in_threadpool(store.reconcile)

    @app.get("/_mock/reconcile/state")
    async def reconcile_state() -> dict[str, Any]:
        return store.reconcile_state()

    @app.get("/_mock/state")
    async def state() -> dict[str, Any]:
        return store.snapshot()

    @app.post("/_mock/reset")
    async def reset() -> dict[str, str]:
        store.reset()
        return {"status": "reset"}


def _list(kind: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "kind": kind,
        "apiVersion": "v1",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


app = create_app()


def main() -> None:  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run(
        "mock_kube.app:app",
        host=os.environ.get("MOCK_KUBE_HOST", "0.0.0.0"),
        port=int(os.environ.get("MOCK_KUBE_PORT", "8001")),
        log_level=os.environ.get("LOG_LEVEL", "info").lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
