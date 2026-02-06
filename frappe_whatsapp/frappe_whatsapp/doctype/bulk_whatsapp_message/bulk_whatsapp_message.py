# Bulk WhatsApp Messaging for Frappe WhatsApp
# bulk_whatsapp_messaging.py

import frappe
from frappe import _
import json
from frappe.utils import cint
from frappe.model.document import Document
from frappe.model.naming import make_autoname
from typing import cast

# Add these files to your frappe_whatsapp app

# 1. First, create a new DocType for Bulk WhatsApp Messaging
# Save this as a Python file in your app's folder:
# frappe_whatsapp/frappe_whatsapp/doctype/bulk_whatsapp_message/bulk_whatsapp_message.py


class BulkWhatsAppMessage(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_recipient.whatsapp_recipient import WhatsAppRecipient

        amended_from: DF.Link | None
        attach: DF.Attach | None
        filter_by_consent: DF.Check
        from_number: DF.Data | None
        recipient_count: DF.Int
        recipient_list: DF.Link | None
        recipient_type: DF.Literal["", "Individual", "Recipient List"]
        recipients: DF.Table[WhatsAppRecipient]
        required_consent_category: DF.Link | None
        scheduled_time: DF.Datetime | None
        sent_count: DF.Int
        skip_opted_out: DF.Check
        skipped_count: DF.Int
        status: DF.Literal["Draft", "Queued", "In Progress", "Completed", "Partially Failed"]
        template: DF.Link | None
        template_variables: DF.Code | None
        title: DF.Data
        use_template: DF.Check
        variable_type: DF.Literal["Common", "Unique"]
        whatsapp_account: DF.Link | None
    # end: auto-generated types

    def autoname(self):
        self.name = make_autoname("BULK-WA-.YYYY.-.#####")

    def validate(self):
        # self.validate_message()
        self.validate_recipients()

    def validate_message(self):
        pass

    def validate_recipients(self):
        if not self.recipients and not self.recipient_list:
            frappe.throw(
                _("At least one recipient or a recipient list is required"))

        # If recipient list is provided, count recipients
        if self.recipient_type == 'Recipient List' and self.recipient_list:
            recipient_count = frappe.db.count(
                "WhatsApp Recipient", {"parent": self.recipient_list})
            if recipient_count == 0:
                frappe.throw(_("Selected recipient list has no recipients"))
            self.recipient_count = recipient_count
        # If individual recipients are provided
        elif self.recipients:
            self.recipient_count = len(self.recipients)

    def on_submit(self):
        self.db_set("status", "Queued")
        self.queue_messages()

    def queue_messages(self):
        """Queue messages for sending"""
        if self.recipient_type == 'Recipient List' and self.recipient_list:
            # Fetch recipients from the recipient list
            recipients = frappe.get_all(
                "WhatsApp Recipient",
                filters={"parent": self.recipient_list},
                fields=[
                    "mobile_number",
                    "name", "recipient_name", "recipient_data"]
            )

            for recipient in recipients:
                frappe.enqueue_doc(
                    self.doctype, self.name,
                    "create_single_message",
                    queue="long",
                    timeout=4000,
                    recipient=recipient
                )
        else:
            # Use recipients from the current document
            for recipient in self.recipients:
                frappe.enqueue_doc(
                    self.doctype, self.name,
                    "create_single_message",
                    queue="long",
                    timeout=4000,
                    recipient=recipient
                )

    def create_single_message(self, recipient):
        """Create a single message in the queue"""
        # message_content = self.message_content

        # Replace variables in the message if any
        self.status = "In Progress"

        # Create WhatsApp message
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa
        wa_message = cast(
            WhatsAppMessage,
            frappe.new_doc("WhatsApp Message"))
        # wa_message.from_number = self.from_number
        wa_message.to = recipient.get("mobile_number")
        wa_message.message_type = "Manual"
        # wa_message.message = message_content
        wa_message.flags.custom_ref_doc = json.loads(
            recipient.get("recipient_data", "{}"))
        wa_message.bulk_message_reference = self.name
        if self.whatsapp_account:
            wa_message.whatsapp_account = self.whatsapp_account

        # If template is being used
        if self.use_template:
            wa_message.template = self.template
            wa_message.message_type = 'Template'
            wa_message.use_template = self.use_template
            # Handle template variables if needed

            if recipient.get("recipient_data"
                             ) and self.variable_type == 'Unique':
                wa_message.body_param = recipient.get("recipient_data")
            elif self.template_variables and self.variable_type == 'Common':
                wa_message.body_param = self.template_variables
            if self.attach:
                wa_message.attach = self.attach

        # Set status to queued
        wa_message.status = "Queued"
        try:
            wa_message.insert(ignore_permissions=True)
        except Exception:
            self.db_set("status", "Partially Failed")
        # Update message count
        self.db_set("sent_count", cint(self.sent_count) + 1)
        if self.recipient_count == self.sent_count:
            self.db_set("status", "Completed")

    def retry_failed(self):
        """Retry failed messages"""
        failed_messages = frappe.get_all(
            "WhatsApp Message",
            filters={
                "bulk_message_reference": self.name,
                "status": "Failed"
            },
            fields=["name"]
        )

        count = 0
        for msg in failed_messages:
            from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import WhatsAppMessage  # noqa
            message_doc = cast(
                WhatsAppMessage,
                frappe.get_doc("WhatsApp Message", msg.name))
            message_doc.status = "Queued"
            message_doc.save(ignore_permissions=True)
            count += 1

        frappe.msgprint(
            _("{0} messages have been requeued for sending").format(count))

    def get_progress(self):
        """Get sending progress for this bulk message"""
        total = self.recipient_count
        sent = frappe.db.count("WhatsApp Message", {
            "bulk_message_reference": self.name,
            "status": ["in", ["sent", "delivered", "Success", "read"]]
        })
        failed = frappe.db.count("WhatsApp Message", {
            "bulk_message_reference": self.name,
            "status": "Failed"
        })
        queued = frappe.db.count("WhatsApp Message", {
            "bulk_message_reference": self.name,
            "status": "Queued"
        })

        return {
            "total": total,
            "sent": sent,
            "failed": failed,
            "queued": queued,
            "percent": (sent / total * 100) if total else 0
        }
