from __future__ import annotations

import hashlib

from frappe.model.document import Document


class WhatsAppProfileAccountState(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profile_consent.whatsapp_profile_consent import WhatsAppProfileConsent

        category_consents: DF.Table[WhatsAppProfileConsent]
        consent_status: DF.Literal["Unknown", "Opted In", "Opted Out", "Partial"]
        do_not_contact: DF.Check
        do_not_contact_reason: DF.SmallText | None
        is_opted_in: DF.Check
        is_opted_out: DF.Check
        opted_in_at: DF.Datetime | None
        opted_in_method: DF.Literal["", "Explicit Form", "API", "Imported", "Web Widget", "WhatsApp Reply", "Legacy"]
        opted_in_source: DF.Data | None
        opted_out_at: DF.Datetime | None
        opted_out_reason: DF.Data | None
        opted_out_source: DF.Literal["", "User Request", "Keyword", "Manual", "Complaint", "Bounce"]
        state_key: DF.Data | None
        whatsapp_account: DF.Link
        whatsapp_profile: DF.Link
    # end: auto-generated types
    def autoname(self) -> None:
        raw = f"{self.whatsapp_profile}\0{self.whatsapp_account}"
        self.state_key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        self.name = self.state_key

