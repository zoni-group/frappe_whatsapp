"""Safe helpers for authenticated Meta Graph API requests."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import frappe
import requests
from frappe import _


DEFAULT_TIMEOUT = 30
MAX_PAGES = 100


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _response_error(response: requests.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except (TypeError, ValueError):
        return {}
    return _as_dict(_as_dict(payload).get("error"))


def _meta_error_message(
    response: requests.Response,
    *,
    account_name: str,
    operation: str,
) -> str:
    error = _response_error(response)
    details = []
    meta_message = error.get("message")
    user_message = error.get("error_user_msg")
    if meta_message:
        details.append(str(meta_message))
    if user_message and str(user_message) != str(meta_message or ""):
        details.append(_("User message: {0}").format(str(user_message)))
    error_data = _as_dict(error.get("error_data"))
    error_details = error_data.get("details")
    if error_details:
        details.append(_("Details: {0}").format(str(error_details)))
    detail = " ".join(details) or _("Meta returned HTTP {0}.").format(
        response.status_code)

    identifiers = []
    if error.get("code") is not None:
        identifiers.append(
            _("code {0}").format(error.get("code")))
    if error.get("error_subcode") is not None:
        identifiers.append(
            _("subcode {0}").format(error.get("error_subcode")))
    suffix = f" ({', '.join(identifiers)})" if identifiers else ""

    return _("WhatsApp Account {0}: {1} failed. {2}{3}").format(
        account_name,
        operation,
        detail,
        suffix,
    )


def request_meta_json(
    method: str,
    url: str,
    *,
    account_name: str,
    operation: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Make a Graph API request without leaking credentials on failure."""
    try:
        response = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        # Requests exceptions may contain the prepared URL. In particular,
        # debug_token carries the inspected token as a query parameter, so do
        # not include str(exc) in user-facing errors or logs.
        frappe.throw(
            _("WhatsApp Account {0}: {1} could not reach Meta ({2}).").format(
                account_name,
                operation,
                type(exc).__name__,
            )
        )

    frappe.flags.integration_request = response
    if response.status_code >= 400:
        frappe.throw(
            _meta_error_message(
                response,
                account_name=account_name,
                operation=operation,
            )
        )

    try:
        payload = response.json() if response.content else {}
    except (TypeError, ValueError):
        frappe.throw(
            _("WhatsApp Account {0}: {1} returned invalid JSON.").format(
                account_name,
                operation,
            )
        )

    if not isinstance(payload, dict):
        frappe.throw(
            _("WhatsApp Account {0}: {1} returned an invalid response.").format(
                account_name,
                operation,
            )
        )
    return payload


def _same_origin(first_url: str, next_url: str) -> bool:
    first = urlparse(first_url)
    following = urlparse(next_url)
    return (
        following.scheme == first.scheme
        and following.netloc == first.netloc
        and following.scheme in {"http", "https"}
    )


def get_paginated_data(
    url: str,
    *,
    account_name: str,
    operation: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return every object in a Graph API collection."""
    first_url = url
    next_url: str | None = url
    next_params = params
    results: list[dict[str, Any]] = []

    for _page in range(MAX_PAGES):
        if not next_url:
            return results
        if not _same_origin(first_url, next_url):
            frappe.throw(
                _("WhatsApp Account {0}: Meta returned an unsafe pagination URL.").format(
                    account_name
                )
            )

        payload = request_meta_json(
            "GET",
            next_url,
            account_name=account_name,
            operation=operation,
            headers=headers,
            params=next_params,
        )
        data = payload.get("data")
        if not isinstance(data, list):
            frappe.throw(
                _("WhatsApp Account {0}: {1} returned an invalid data list.").format(
                    account_name,
                    operation,
                )
            )
        results.extend(item for item in data if isinstance(item, dict))

        paging = _as_dict(payload.get("paging"))
        raw_next = paging.get("next")
        next_url = str(raw_next) if raw_next else None
        next_params = None

    frappe.throw(
        _("WhatsApp Account {0}: {1} exceeded {2} pages.").format(
            account_name,
            operation,
            MAX_PAGES,
        )
    )
    return results
