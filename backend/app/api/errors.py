from __future__ import annotations

from typing import Any

import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException


class ApiErrorPayload(BaseModel):
    code: str
    message: str
    retryable: bool = False
    details: Any | None = None


class ApiErrorBody(BaseModel):
    error: ApiErrorPayload


class ApiError(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details
        self.headers = headers


def api_error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    retryable: bool = False,
    details: Any | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    body = ApiErrorBody(
        error=ApiErrorPayload(
            code=code,
            message=message,
            retryable=retryable,
            details=details,
        )
    )
    return JSONResponse(status_code=status_code, content=body.model_dump(), headers=headers)


def from_api_error(exc: ApiError) -> JSONResponse:
    return api_error_response(
        exc.status_code,
        exc.code,
        exc.message,
        retryable=exc.retryable,
        details=exc.details,
        headers=exc.headers,
    )


def from_http_exception(exc: HTTPException | StarletteHTTPException) -> JSONResponse:
    if isinstance(exc.detail, dict):
        error = exc.detail.get("error")
        if isinstance(error, dict):
            return api_error_response(
                exc.status_code,
                str(error.get("code") or _default_code_for_status(exc.status_code)),
                str(error.get("message") or _coerce_message(exc.detail, exc.status_code)),
                retryable=bool(error.get("retryable", _default_retryable_for_status(exc.status_code))),
                details=error.get("details"),
                headers=exc.headers,
            )

    message = _coerce_message(exc.detail, exc.status_code)
    details = _coerce_details(exc.detail)
    return api_error_response(
        exc.status_code,
        _default_code_for_status(exc.status_code),
        message,
        retryable=_default_retryable_for_status(exc.status_code),
        details=details,
        headers=exc.headers,
    )


def from_validation_error(exc: RequestValidationError) -> JSONResponse:
    issues = []
    fields: dict[str, str] = {}

    for error in exc.errors():
        loc = list(error.get("loc", []))
        message = _clean_validation_message(error.get("msg"))
        field = ".".join(str(part) for part in loc[1:]) if len(loc) > 1 else None
        issue = {
            "location": [str(part) for part in loc],
            "field": field,
            "message": message,
            "type": error.get("type"),
        }
        issues.append(issue)
        if field and field not in fields:
            fields[field] = message

    first_message = issues[0]["message"] if issues else "Request validation failed"
    details: dict[str, Any] = {"issues": issues}
    if fields:
        details["fields"] = fields

    return api_error_response(
        422,
        "validation_error",
        first_message,
        retryable=False,
        details=details,
    )


def from_unhandled_exception() -> JSONResponse:
    return api_error_response(
        500,
        "internal_error",
        "Internal server error",
        retryable=True,
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def handle_api_error(_: Request, exc: ApiError):
        if exc.status_code >= 500:
            logging.getLogger(__name__).error("API error: %s (%s)", exc.message, exc.code)
        return from_api_error(exc)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_: Request, exc: RequestValidationError):
        return from_validation_error(exc)

    @app.exception_handler(HTTPException)
    async def handle_http_exception(_: Request, exc: HTTPException):
        return from_http_exception(exc)

    @app.exception_handler(StarletteHTTPException)
    async def handle_starlette_http_exception(_: Request, exc: StarletteHTTPException):
        return from_http_exception(exc)

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(_: Request, exc: Exception):
        logging.getLogger(__name__).exception("Unhandled API exception")
        return from_unhandled_exception()


def _default_code_for_status(status_code: int) -> str:
    return {
        400: "invalid_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "validation_error",
        429: "rate_limited",
    }.get(status_code, "internal_error" if status_code >= 500 else "invalid_request")


def _default_retryable_for_status(status_code: int) -> bool:
    return status_code in {429, 500, 502, 503, 504}


def _coerce_message(detail: Any, status_code: int) -> str:
    if isinstance(detail, str) and detail.strip():
        return detail
    if isinstance(detail, dict):
        message = detail.get("message")
        if isinstance(message, str) and message.strip():
            return message
    if status_code == 404:
        return "Resource not found"
    if status_code >= 500:
        return "Internal server error"
    return "Request failed"


def _coerce_details(detail: Any) -> Any | None:
    if isinstance(detail, dict):
        if "error" in detail:
            return None
        return detail
    if isinstance(detail, list):
        return detail
    return None


def _clean_validation_message(message: Any) -> str:
    if not isinstance(message, str) or not message.strip():
        return "Request validation failed"
    if message.startswith("Value error, "):
        return message[len("Value error, ") :]
    return message
