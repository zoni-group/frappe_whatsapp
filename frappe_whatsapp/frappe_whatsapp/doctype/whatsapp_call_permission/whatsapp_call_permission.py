from __future__ import annotations

from frappe.model.document import Document


class WhatsAppCallPermission(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        expires_at: DF.Datetime | None
        is_permanent: DF.Check
        last_checked_at: DF.Datetime | None
        last_request_message: DF.Link | None
        last_requested_at: DF.Datetime | None
        permission_status: DF.Literal["No Permission", "Temporary", "Permanent", "Rejected", "Expired", "Unknown"]
        phone_number: DF.Data
        raw_meta_state: DF.JSON | None
        response_source: DF.Data | None
        whatsapp_account: DF.Link
    # end: auto-generated types
    pass
