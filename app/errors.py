"""API error type shared by the service layer and the HTTP layer."""

from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """An error that maps onto a JSON error response.

    ``code`` is stable and machine-readable; ``message`` is meant for the
    operator on duty and is written in Chinese to match the runbook.
    """

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            payload["details"] = self.details
        return payload
