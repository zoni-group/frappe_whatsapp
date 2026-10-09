# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

from frappe.model.document import Document
from frappe_whatsapp.utils import format_number


class WhatsAppBlockedContact(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        blocked_at: DF.Datetime | None
        contact_number: DF.Data | None
        contact_profile: DF.Link | None
        is_blocked: DF.Check
        last_error: DF.SmallText | None
        last_error_payload: DF.JSON | None
        last_synced_at: DF.Datetime | None
        meta_input: DF.Data | None
        meta_status: DF.Literal["Not Synced", "Blocked", "Unblocked", "Failed"]
        meta_wa_id: DF.Data | None
        reason: DF.SmallText | None
        source_app: DF.Link | None
        source_message: DF.Link | None
        unblocked_at: DF.Datetime | None
        user_id: DF.Data | None
        whatsapp_account: DF.Link
    # end: auto-generated types

    def validate(self):
        if self.contact_number:
            self.contact_number = format_number(str(self.contact_number))
