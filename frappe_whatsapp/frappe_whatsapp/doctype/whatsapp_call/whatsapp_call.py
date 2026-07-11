from __future__ import annotations

from frappe.model.document import Document


class WhatsAppCall(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        agent_extension: DF.Data | None
        agent_user: DF.Link | None
        ami_action_id: DF.Data | None
        cancelled_at: DF.Datetime | None
        contact: DF.Data | None
        failure_reason: DF.SmallText | None
        last_error_payload: DF.JSON | None
        pbx_queued_at: DF.Datetime | None
        permission_request_message: DF.Link | None
        permission_responded_at: DF.Datetime | None
        phone_number: DF.Data
        requested_at: DF.Datetime | None
        status: DF.Literal["Permission Requested", "Permission Accepted", "Permission Rejected", "PBX Queued", "Failed", "Cancelled"]
        whatsapp_account: DF.Link
    # end: auto-generated types
    pass
