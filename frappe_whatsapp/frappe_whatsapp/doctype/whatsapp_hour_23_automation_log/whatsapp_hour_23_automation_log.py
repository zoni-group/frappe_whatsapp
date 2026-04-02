# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class WhatsAppHour23AutomationLog(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        anchor_message: DF.Link | None
        automation_type: DF.Literal["consent_request", "status_follow_up"]
        contact_number: DF.Data | None
        outgoing_message: DF.Link | None
        sent_at: DF.Datetime | None
        template: DF.Link | None
        whatsapp_account: DF.Link | None
    # end: auto-generated types
    pass
