from __future__ import annotations

import hashlib
import hmac
import time
from typing import cast
from unittest.mock import patch

import frappe
import requests
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_client_webhook_delivery.whatsapp_client_webhook_delivery import (
    WhatsAppClientWebhookDelivery,
)

from frappe_whatsapp.utils.client_delivery import (
    deliver_client_event,
    queue_client_event,
)


class TestClientWebhookDelivery(FrappeTestCase):
    def _app(self, *, url: str = "https://example.com/inbound"):
        suffix = frappe.generate_hash(length=8)
        return frappe.get_doc({
            "doctype": "WhatsApp Client App",
            "app_id": f"delivery-{suffix}",
            "enabled": 1,
            "inbound_webhook_url": url,
            "status_webhook_url": f"{url}/status",
            "webhook_secret": "test-webhook-secret",
            "require_webhook_signature": 1,
            "subscribe_incoming": 1,
            "subscribe_status": 1,
            "subscribe_identity_updates": 1,
        }).insert(ignore_permissions=True)

    def _queue(self, app, *, event_id: str | None = None) -> str:
        suffix = frappe.generate_hash(length=8)
        account = frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Delivery Test Account {suffix}",
            "status": "Active",
            "phone_id": f"delivery-phone-{suffix}",
        }).insert(ignore_permissions=True)
        name = queue_client_event(
            client_app=app.name,
            whatsapp_account=str(account.name),
            event_type="whatsapp.incoming",
            event_id=event_id or frappe.generate_hash(length=24),
            payload={"message": {"message_id": "wamid.test"}},
        )
        self.assertTrue(name)
        return str(name)

    def test_delivery_resolves_current_url_and_signs_exact_body(self):
        app = self._app(url="https://old.example.com/inbound")
        delivery_name = self._queue(app)
        delivery = cast(
            WhatsAppClientWebhookDelivery,
            frappe.get_doc("WhatsApp Client Webhook Delivery", delivery_name),
        )
        app.db_set(
            "inbound_webhook_url",
            "https://new.example.com/inbound",
        )
        response = frappe._dict({
            "status_code": 200,
            "text": "ok",
        })

        with patch(
            "frappe_whatsapp.utils.client_delivery.requests.post",
            return_value=response,
        ) as mock_post:
            deliver_client_event(delivery_name)

        call = mock_post.call_args
        self.assertEqual(call.args[0], "https://new.example.com/inbound")
        body = call.kwargs["data"]
        headers = call.kwargs["headers"]
        expected = hmac.new(
            b"test-webhook-secret",
            headers["X-WhatsApp-Timestamp"].encode("ascii") + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        self.assertEqual(headers["X-WhatsApp-Event-ID"], delivery.event_id)
        self.assertLess(
            abs(time.time() - int(headers["X-WhatsApp-Timestamp"])),
            5,
        )
        self.assertEqual(headers["X-WhatsApp-Signature"], f"sha256={expected}")
        delivery.reload()
        self.assertEqual(delivery.delivery_status, "Delivered")
        self.assertEqual(delivery.attempts, 1)

    def test_500_is_retryable_and_400_is_terminal(self):
        app = self._app()
        retry_name = self._queue(app)
        terminal_name = self._queue(app)
        responses = [
            frappe._dict({"status_code": 503, "text": "unavailable"}),
            frappe._dict({"status_code": 400, "text": "bad request"}),
        ]

        with patch(
            "frappe_whatsapp.utils.client_delivery.requests.post",
            side_effect=responses,
        ):
            deliver_client_event(retry_name)
            deliver_client_event(terminal_name)

        retry = cast(
            WhatsAppClientWebhookDelivery,
            frappe.get_doc("WhatsApp Client Webhook Delivery", retry_name),
        )
        terminal = cast(
            WhatsAppClientWebhookDelivery,
            frappe.get_doc("WhatsApp Client Webhook Delivery", terminal_name),
        )
        self.assertEqual(retry.delivery_status, "Failed")
        self.assertTrue(retry.next_retry_at)
        self.assertEqual(terminal.delivery_status, "Failed")
        self.assertFalse(terminal.next_retry_at)

    def test_transport_error_is_retryable(self):
        app = self._app()
        delivery_name = self._queue(app)

        with patch(
            "frappe_whatsapp.utils.client_delivery.requests.post",
            side_effect=requests.ConnectionError("offline"),
        ):
            deliver_client_event(delivery_name)

        delivery = cast(
            WhatsAppClientWebhookDelivery,
            frappe.get_doc("WhatsApp Client Webhook Delivery", delivery_name),
        )
        self.assertEqual(delivery.delivery_status, "Failed")
        self.assertEqual(delivery.attempts, 1)
        self.assertTrue(delivery.next_retry_at)
