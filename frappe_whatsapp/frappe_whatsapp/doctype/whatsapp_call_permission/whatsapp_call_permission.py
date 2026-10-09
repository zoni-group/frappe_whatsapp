from __future__ import annotations

from frappe.model.document import Document
import frappe


class WhatsAppCallPermission(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        contact_profile: DF.Link | None
        expires_at: DF.Datetime | None
        is_permanent: DF.Check
        last_checked_at: DF.Datetime | None
        last_request_message: DF.Link | None
        last_requested_at: DF.Datetime | None
        permission_status: DF.Literal["No Permission", "Temporary", "Permanent", "Rejected", "Expired", "Unknown"]
        phone_number: DF.Data | None
        raw_meta_state: DF.JSON | None
        recipient: DF.Data | None
        response_source: DF.Data | None
        whatsapp_account: DF.Link
    # end: auto-generated types

    def autoname(self):
        if self.phone_number:
            self.name = f"{self.phone_number}-{self.whatsapp_account}"
        else:
            self.name = frappe.generate_hash(length=20)
    pass
