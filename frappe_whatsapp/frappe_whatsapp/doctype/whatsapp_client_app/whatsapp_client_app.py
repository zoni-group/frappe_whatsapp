# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class WhatsAppClientApp(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        app_id: DF.Data | None
        enabled: DF.Check
        inbound_webhook_url: DF.Data | None
        outbound_default_account: DF.Link | None
        status_webhook_url: DF.Data | None
    # end: auto-generated types
    def validate(self) -> None:
        if self.get("require_webhook_signature") and not self.get_password(
            "webhook_secret", raise_exception=False
        ):
            frappe.throw(
                _(
                    "Webhook Secret is required when webhook signature "
                    "verification is enabled."
                )
            )
