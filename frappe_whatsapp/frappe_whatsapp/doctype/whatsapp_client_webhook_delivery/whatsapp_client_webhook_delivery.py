from frappe.model.document import Document


class WhatsAppClientWebhookDelivery(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        attempts: DF.Int
        client_app: DF.Link
        delivery_status: DF.Literal["Pending", "Delivered", "Failed", "Skipped"]
        error: DF.SmallText | None
        event_id: DF.Data
        event_type: DF.Data
        last_attempted_at: DF.Datetime | None
        next_retry_at: DF.Datetime | None
        payload: DF.LongText
        response_body: DF.SmallText | None
        response_code: DF.Data | None
        whatsapp_account: DF.Link
    # end: auto-generated types
    pass
