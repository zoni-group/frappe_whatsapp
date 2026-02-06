# Copyright (c) 2025, Shridhar Patil and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe_whatsapp.utils import format_number

class WhatsAppProfiles(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        consent_ip_address: DF.Data | None
        consent_status: DF.Literal["Unknown", "Opted In", "Opted Out", "Partial"]
        consent_version: DF.Data | None
        contact: DF.Link | None
        do_not_contact: DF.Check
        do_not_contact_reason: DF.SmallText | None
        gdpr_consent: DF.Check
        is_opted_in: DF.Check
        is_opted_out: DF.Check
        number: DF.Data
        opted_in_at: DF.Datetime | None
        opted_in_method: DF.Literal["Explicit Form", "API", "Imported", "Web Widget", "WhatsApp Reply", "Legacy"]
        opted_in_source: DF.Data | None
        opted_out_at: DF.Datetime | None
        opted_out_reason: DF.Data | None
        opted_out_source: DF.Literal["", "User Request", "Keyword", "Manual", "Complaint", "Bounce"]
        profile_name: DF.Data | None
        title: DF.Data | None
        whatsapp_account: DF.Link | None
    # end: auto-generated types
    def validate(self):
        self.format_whatsapp_number()
        self.set_title()

    def format_whatsapp_number(self):
        if self.number:
            self.number = format_number(self.number)

    def set_title(self):
        self.title = " - ".join(filter(None, [self.profile_name, self.number])) or "Unnamed Profile"
