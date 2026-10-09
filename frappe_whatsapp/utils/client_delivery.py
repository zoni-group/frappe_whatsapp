from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import timedelta
from typing import Any, cast

import frappe
import requests
from frappe.utils import add_to_date, now_datetime

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_app.whatsapp_client_app import (
    WhatsAppClientApp,
)
from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_webhook_delivery.whatsapp_client_webhook_delivery import (
    WhatsAppClientWebhookDelivery,
)


DELIVERY_DOCTYPE = "WhatsApp Client Webhook Delivery"
MAX_ATTEMPTS = 8


def _body(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def queue_client_event(
    *,
    client_app: str,
    whatsapp_account: str,
    event_type: str,
    event_id: str,
    payload: dict[str, Any],
) -> str | None:
    if not client_app or not frappe.db.table_exists(DELIVERY_DOCTYPE):
        return None
    app = cast(
        WhatsAppClientApp,
        frappe.get_doc("WhatsApp Client App", client_app),
    )
    if not app.enabled or not _subscribed(app, event_type):
        return None
    envelope = {
        "schema_version": 2,
        "event": event_type,
        "event_id": event_id,
        "occurred_at": str(now_datetime()),
        "app_id": app.app_id or "",
        "whatsapp_account": whatsapp_account,
        **payload,
    }
    try:
        delivery = frappe.get_doc({
            "doctype": DELIVERY_DOCTYPE,
            "event_id": event_id,
            "event_type": event_type,
            "client_app": client_app,
            "whatsapp_account": whatsapp_account,
            "delivery_status": "Pending",
            "payload": _body(envelope),
            "attempts": 0,
            "next_retry_at": add_to_date(now_datetime(), minutes=2),
        }).insert(ignore_permissions=True)
    except frappe.UniqueValidationError:
        return None
    frappe.enqueue(
        "frappe_whatsapp.utils.client_delivery.deliver_client_event",
        queue="short",
        delivery_name=delivery.name,
        enqueue_after_commit=True,
    )
    return str(delivery.name)


def _subscribed(app: Any, event_type: str) -> bool:
    field = {
        "whatsapp.incoming": "subscribe_incoming",
        "whatsapp.message_status": "subscribe_status",
        "whatsapp.identity_updated": "subscribe_identity_updates",
    }.get(event_type)
    return not field or bool(app.get(field))


def _url(app: Any, event_type: str) -> str:
    if event_type == "whatsapp.message_status":
        return str(app.status_webhook_url or app.inbound_webhook_url or "")
    return str(app.inbound_webhook_url or "")


def _headers(app: Any, event_id: str, body: bytes) -> dict[str, str]:
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-WhatsApp-App-ID": str(app.app_id or ""),
        "X-WhatsApp-Event-ID": event_id,
        "X-WhatsApp-Timestamp": timestamp,
    }
    secret = app.get_password("webhook_secret", raise_exception=False)
    if secret:
        signature = hmac.new(
            str(secret).encode("utf-8"),
            timestamp.encode("ascii") + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        headers["X-WhatsApp-Signature"] = f"sha256={signature}"
    elif app.get("require_webhook_signature"):
        frappe.throw("WhatsApp Client App requires a webhook secret.")
    return headers


def deliver_client_event(delivery_name: str) -> None:
    delivery = cast(
        WhatsAppClientWebhookDelivery,
        frappe.get_doc(DELIVERY_DOCTYPE, delivery_name),
    )
    if delivery.delivery_status in {"Delivered", "Skipped"}:
        return
    app = cast(
        WhatsAppClientApp,
        frappe.get_doc("WhatsApp Client App", delivery.client_app),
    )
    url = _url(app, str(delivery.event_type))
    if not app.enabled or not url:
        delivery.db_set({"delivery_status": "Skipped", "next_retry_at": None})
        return
    body = str(delivery.payload or "{}").encode("utf-8")
    attempts = int(delivery.attempts or 0) + 1
    try:
        response = requests.post(
            url,
            data=body,
            headers=_headers(app, str(delivery.event_id), body),
            timeout=30,
        )
        retryable = response.status_code == 429 or response.status_code >= 500
        success = 200 <= response.status_code < 300
        terminal = success or not retryable or attempts >= MAX_ATTEMPTS
        delivery.db_set({
            "delivery_status": "Delivered" if success else "Failed",
            "attempts": attempts,
            "last_attempted_at": now_datetime(),
            "next_retry_at": None if terminal else _next_retry(attempts),
            "response_code": str(response.status_code),
            "response_body": (response.text or "")[:500],
            "error": "" if success else f"HTTP {response.status_code}",
        })
    except requests.RequestException as exc:
        terminal = attempts >= MAX_ATTEMPTS
        delivery.db_set({
            "delivery_status": "Failed",
            "attempts": attempts,
            "last_attempted_at": now_datetime(),
            "next_retry_at": None if terminal else _next_retry(attempts),
            "error": str(exc)[:500],
        })


def _next_retry(attempt: int):
    minutes = min(2 ** max(attempt - 1, 0), 60)
    return now_datetime() + timedelta(minutes=minutes)


def retry_failed_client_events() -> None:
    from frappe_whatsapp.utils.routing import recover_unqueued_incoming_events

    recover_unqueued_incoming_events()
    now = now_datetime()
    rows = frappe.get_all(
        DELIVERY_DOCTYPE,
        filters={
            "delivery_status": ["in", ["Pending", "Failed"]],
            "attempts": ["<", MAX_ATTEMPTS],
            "next_retry_at": ["<=", now],
        },
        fields=["name"],
    )
    for row in rows:
        frappe.enqueue(
            "frappe_whatsapp.utils.client_delivery.deliver_client_event",
            queue="short",
            delivery_name=row.name,
        )


@frappe.whitelist()
def retry_client_event(delivery_name: str) -> None:
    frappe.only_for("System Manager")
    frappe.db.set_value(
        DELIVERY_DOCTYPE,
        delivery_name,
        {"delivery_status": "Pending", "next_retry_at": now_datetime()},
    )
    frappe.enqueue(
        "frappe_whatsapp.utils.client_delivery.deliver_client_event",
        queue="short",
        delivery_name=delivery_name,
    )
