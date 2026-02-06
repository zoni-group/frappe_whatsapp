"""Notification."""

import json
import frappe

from frappe import _
from frappe.model.document import Document
from frappe.utils.safe_exec import get_safe_globals, safe_exec
from frappe.integrations.utils import make_post_request
from frappe.desk.form.utils import get_pdf_link
from frappe.utils import add_to_date, now_datetime, datetime, \
    get_url, cint, get_datetime
from frappe.model import numeric_fieldtypes
from datetime import datetime as py_datetime, time as py_time

from frappe_whatsapp.utils import get_whatsapp_account
from typing import Any, cast, TypedDict, Optional


class WhatsAppAPIMessage(TypedDict, total=False):
    id: str


class WhatsAppAPISendResponse(TypedDict, total=False):
    messages: list[WhatsAppAPIMessage]
    error: Any


def _as_dict(value: Any) -> dict[str, Any]:
    """Narrow unknown values into a dict for safer access."""
    return value if isinstance(value, dict) else {}


def _first_message_id(resp: dict[str, Any]) -> Optional[str]:
    """Extract Meta message id from response safely."""
    messages = resp.get("messages")
    if not isinstance(messages, list) or not messages:
        return None

    first = messages[0]
    if not isinstance(first, dict):
        return None

    msg_id = first.get("id")
    return msg_id if isinstance(msg_id, str) and msg_id else None


def _integration_request_json() -> dict[str, Any]:
    """Safely read frappe.flags.integration_request.json() if present."""
    ir = getattr(frappe.flags, "integration_request", None)
    if not ir:
        return {}
    json_func = getattr(ir, "json", None)
    if not callable(json_func):
        return {}
    raw = json_func()
    return _as_dict(raw)


class WhatsAppNotification(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message_fields.whatsapp_message_fields import WhatsAppMessageFields

        attach: DF.Attach | None
        attach_document_print: DF.Check
        attach_from_field: DF.Data | None
        button_fields: DF.Data | None
        check_consent_before_send: DF.Check
        code: DF.Code | None
        condition: DF.Code | None
        custom_attachment: DF.Check
        date_changed: DF.Literal[None]
        days_in_advance: DF.Int
        disabled: DF.Check
        doctype_event: DF.Literal["Before Insert", "Before Validate", "Before Save", "After Insert", "After Save", "Before Submit", "After Submit", "Before Cancel", "After Cancel", "Before Delete", "After Delete", "Before Save (Submitted Document)", "After Save (Submitted Document)", "Days After", "Days Before"]
        event_frequency: DF.Literal["All", "Hourly", "Daily", "Weekly", "Monthly", "Yearly", "Hourly Long", "Daily Long", "Weekly Long", "Monthly Long"]
        field_name: DF.Data | None
        fields: DF.Table[WhatsAppMessageFields]
        file_name: DF.Data | None
        header_type: DF.Data | None
        is_transactional: DF.Check
        notification_name: DF.Data
        notification_type: DF.Literal["DocType Event", "Scheduler Event"]
        property_value: DF.Data | None
        reference_doctype: DF.Link
        required_consent_category: DF.Link | None
        set_property_after_alert: DF.Literal[None]
        skip_opted_out_recipients: DF.Check
        template: DF.Link
    # end: auto-generated types
    """Notification."""

    def validate(self) -> None:
        """Validate notification configuration before saving.

        Goals:
        - Avoid mixed types (DocField objects + dict rows) that break
          type checkers.
        - Use meta as the authoritative list of fields (includes custom
          fields).
        - Provide clear, actionable error messages.
        """
        self._validate_doctype_event_field()
        self._validate_custom_attachment_config()
        self._validate_set_property_after_alert_field()

    def _validate_doctype_event_field(self) -> None:
        """For DocType Event notifications, validate that field_name exists
        on reference_doctype."""
        if self.notification_type != "DocType Event":
            return

        # If it's DocType Event but no field_name configured, fail early with
        # a clear message.
        if not self.field_name:
            frappe.throw(
                _("Please set the Field Name for DocType Event notifications.")
            )

        if not self.reference_doctype:
            frappe.throw(_("Please set Reference DocType."))

        meta = frappe.get_meta(self.reference_doctype)

        # meta.get_field returns DocField | None; safe and type-checker
        # friendly.
        if meta.get_field(self.field_name) is None:
            frappe.throw(
                _("Field {0} not found on DocType {1}.").format(
                    frappe.bold(self.field_name),
                    frappe.bold(self.reference_doctype),
                )
            )

    def _validate_custom_attachment_config(self) -> None:
        """If custom_attachment is enabled, require either attach
        or attach_from_field."""
        if not self.custom_attachment:
            return

        # Must supply one source.
        if not self.attach and not self.attach_from_field:
            frappe.throw(
                _("Either {0} a file or add a {1} to send attachment.").format(
                    frappe.bold(_("Attach")),
                    frappe.bold(_("Attach from field")),
                )
            )

        # If using attach_from_field, ensure it exists on the reference
        # doctype (when provided).
        if self.attach_from_field:
            if not self.reference_doctype:
                frappe.throw(
                    _("Please set Reference DocType"
                      " to use Attach from field."))

            meta = frappe.get_meta(self.reference_doctype)
            if meta.get_field(self.attach_from_field) is None:
                frappe.throw(
                    _("Attach from field {0} not found on DocType {1}."
                      ).format(
                        frappe.bold(self.attach_from_field),
                        frappe.bold(self.reference_doctype),
                    )
                )

    def _validate_set_property_after_alert_field(self) -> None:
        """If set_property_after_alert is set, ensure the field exists on
        reference_doctype."""
        if not self.set_property_after_alert:
            return

        if not self.reference_doctype:
            frappe.throw(_("Please set Reference DocType."))

        meta = frappe.get_meta(self.reference_doctype)
        if meta.get_field(self.set_property_after_alert) is None:
            frappe.throw(
                _("Field {0} not found on DocType {1}.").format(
                    frappe.bold(self.set_property_after_alert),
                    frappe.bold(self.reference_doctype),
                )
            )

    def send_scheduled_message(self) -> dict[str, Any]:
        """Execute scheduled/server-script config and send messages.

        This method is intended to be used by scheduler / server scripts.

        Returns:
            dict with status + counters for observability.
        """
        # 1) Run the "condition" server script (
        # it can populate _contact_list/_data_list)
        if self.condition:
            safe_exec(
                str(self.condition),
                get_safe_globals(),
                {"doc": self},  # explicit dict for typing clarity
            )

        # 2) Load template in a type-checker-friendly way
        if not self.template:
            return {
                "status": "skipped",
                "reason": "no_template_selected",
                "sent": 0}

        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import WhatsAppTemplates  # noqa

        template = cast(
            WhatsAppTemplates,
            frappe.get_doc("WhatsApp Templates", self.template))

        if not getattr(template, "language_code", None):
            return {
                "status": "skipped",
                "reason": "template_missing_language_code",
                "sent": 0}

        sent = 0

        # 3) Read dynamic lists via Document.get(...) (safe for type checkers)
        contact_list = self.get("_contact_list")
        data_list = self.get("_data_list")

        # 4) Send based on whichever payload exists
        if isinstance(contact_list, list) and contact_list:
            # send simple template without a doc to get field data.
            # (send_simple_template expects self._contact_list)
            # If you want to remove reliance on dynamic attributes, pass it in.
            self._contact_list = contact_list  # keep backward compatibility
            self.send_simple_template(template)
            sent = len(contact_list)

            return {
                "status": "ok",
                "mode": "contact_list",
                "sent": sent}

        if isinstance(data_list, list) and data_list:
            # allow send a dynamic template using schedule event config
            # expected list items: {"name": "...", "phone_no": "..."}
            for item in data_list:
                if not isinstance(item, dict):
                    continue

                docname = item.get("name")
                phone_no = item.get("phone_no")

                if not docname:
                    continue

                doc = frappe.get_doc(self.reference_doctype, docname)
                self.send_template_message(
                    doc,
                    phone_no=phone_no,
                    default_template=template,
                    ignore_condition=True,
                )
                sent += 1

            return {"status": "ok", "mode": "data_list", "sent": sent}

        # 5) Nothing configured by the script
        return {
            "status": "skipped",
            "reason": "no_recipients_provided",
            "sent": 0}

    def send_simple_template(self, template):
        """ send simple template without a doc to get field data """
        for contact in self._contact_list:
            data = {
                "messaging_product": "whatsapp",
                "to": self.format_number(contact),
                "type": "template",
                "template": {
                    "name": template.actual_name,
                    "language": {
                        "code": template.language_code
                    },
                    "components": []
                }
            }
            self.content_type = template.get("header_type", "text").lower()
            self.notify(
                data, template_account=template.get("whatsapp_account"))

    def send_template_message(
            self, doc: Document, phone_no=None,
            default_template=None, ignore_condition=False):
        """Specific to Document Event triggered Server Scripts."""
        if self.disabled:
            return

        doc_data = doc.as_dict()
        if self.condition and not ignore_condition:
            # check if condition satisfies
            if not frappe.safe_eval(
                self.condition, get_safe_globals(), dict(doc=doc_data)
            ):
                return

        template = default_template or frappe.get_doc(
            "WhatsApp Templates", self.template)
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import WhatsAppTemplates  # noqa
        template = cast(WhatsAppTemplates, template)

        if template:
            if self.field_name:
                phone_number = phone_no or doc_data[self.field_name]
            else:
                phone_number = phone_no

            data = {
                "messaging_product": "whatsapp",
                "to": self.format_number(phone_number),
                "type": "template",
                "template": {
                    "name": template.actual_name,
                    "language": {
                        "code": template.language_code
                    },
                    "components": []
                }
            }

            # Pass parameter values
            if self.fields:
                parameters = []
                for field in self.fields:
                    if isinstance(doc, Document):
                        # get field with prettier value.
                        value = doc.get_formatted(field.field_name)
                    else:
                        value = doc_data[field.field_name]
                        if isinstance(
                                doc_data[field.field_name],
                                (datetime.date, datetime.datetime)):
                            value = str(doc_data[field.field_name])

                    parameters.append({
                        "type": "text",
                        "text": value
                    })

                data['template']["components"] = [{
                    "type": "body",
                    "parameters": parameters
                }]

            url = ""
            filename = ""

            if self.attach_document_print:
                key = doc.get_document_share_key()  # noqa
                frappe.db.commit()

                doctype_name = cast(str, doc_data.get("doctype") or "")
                doc_name = cast(str, doc_data.get("name") or "")

                if not doctype_name or not doc_name:
                    frappe.throw(_(
                        "Missing doctype or name for PDF attachment."))

                print_format: str = "Standard"

                from frappe.core.doctype.doctype.doctype import DocType
                doctype = cast(
                    DocType,
                    frappe.get_doc("DocType", doctype_name))

                if doctype.custom:
                    # default_print_format is usually str|None, but
                    # we still narrow
                    if (isinstance(doctype.default_print_format, str) and
                            doctype.default_print_format):
                        print_format = doctype.default_print_format
                else:
                    raw_default: Any = frappe.db.get_value(
                        "Property Setter",
                        filters={
                            "doc_type": doctype_name,
                            "property": "default_print_format"},
                        fieldname="value",
                    )
                    # The important part: enforce str
                    if isinstance(raw_default, str) and raw_default:
                        print_format = raw_default

                link = get_pdf_link(
                    doctype_name,
                    doc_name,
                    print_format=print_format,  # always str now
                )

                filename = f"{doc_name}.pdf"
                url = f"{get_url()}{link}&key={key}"

            elif self.custom_attachment:
                filename = self.file_name

                if self.attach_from_field:
                    file_url = doc_data[self.attach_from_field]
                    if not file_url.startswith("http"):
                        # get share key so that private files can be sent
                        key = doc.get_document_share_key()
                        file_url = f'{get_url()}{file_url}&key={key}'
                else:
                    file_url = self.attach

                if file_url and file_url.startswith("http"):
                    url = f'{file_url}'
                else:
                    url = f'{get_url()}{file_url}'

            if template.header_type == 'DOCUMENT':
                data['template']['components'].append({
                    "type": "header",
                    "parameters": [{
                        "type": "document",
                        "document": {
                            "link": url,
                            "filename": filename
                        }
                    }]
                })
                self.content_type = "document"
            elif template.header_type == 'IMAGE':
                data['template']['components'].append({
                    "type": "header",
                    "parameters": [{
                        "type": "image",
                        "image": {
                            "link": url
                        }
                    }]
                })
                self.content_type = "image"
            else:
                # Default to text for empty or TEXT header types
                self.content_type = "text"

            if template.buttons:
                button_fields = self.button_fields.split(
                    ",") if self.button_fields else []
                for idx, btn in enumerate(template.buttons):
                    if (btn.button_type == "Visit Website"
                            and btn.url_type == "Dynamic"):
                        if button_fields:
                            data['template']['components'].append({
                                "type": "button",
                                "sub_type": "url",
                                "index": str(idx),
                                "parameters": [
                                    {"type": "text",
                                     "text": doc.get(button_fields.pop(0))}
                                ]
                            })

            self.notify(
                data, doc_data,
                template_account=template.whatsapp_account)

    def notify(
            self,
            data: dict[str, Any],
            doc_data: dict[str, Any] | None = None,
            template_account: str | None = None) -> None:
        """Notify.

        Sends a WhatsApp template message via Meta endpoint and logs result.

        Type-safety goals:
        - avoid subscripting unknown/None responses
        - avoid assuming integration_request exists
        - ensure message_id extraction is safe
        """
        # Use template's whatsapp account if available, otherwise default
        # outgoing account
        if template_account:
            whatsapp_account = frappe.get_doc(
                "WhatsApp Account",
                template_account)
        else:
            whatsapp_account = get_whatsapp_account(account_type="outgoing")

        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa
        wa = cast(Optional[WhatsAppAccount], whatsapp_account)

        if not wa:
            frappe.throw(_("Please set a default outgoing WhatsApp Account"))
            return

        token = wa.get_password("token")

        headers: dict[str, str] = {
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        }

        error_message = ""
        meta_json = ""

        try:
            raw_response: Any = make_post_request(
                f"{wa.url}/{wa.version}/{wa.phone_id}/messages",
                headers=headers,
                data=json.dumps(data),
            )
            response = _as_dict(raw_response)

            # Ensure content_type is always a string
            if not isinstance(
                    self.get("content_type"), str
                    ) or not self.get("content_type"):
                self.content_type = "text"

            # Pull body parameters (safe)
            parameters: Optional[str] = None
            template_payload = data.get("template")
            if isinstance(template_payload, dict):
                components = template_payload.get("components", [])
                if isinstance(components, list):
                    for comp in components:
                        if not isinstance(comp, dict):
                            continue
                        if comp.get("type") != "body":
                            continue
                        params = comp.get("parameters") or []
                        if not isinstance(params, list):
                            break
                        texts: list[str] = []
                        for p in params:
                            if not isinstance(p, dict):
                                continue
                            if p.get("type") == "text":
                                t = p.get("text")
                                if isinstance(t, str) and t:
                                    texts.append(t)
                        parameters = frappe.json.dumps(texts, default=str)
                        break

            message_id = _first_message_id(response)
            if not message_id:
                # Meta can return errors without "messages"; keep it observable
                raise frappe.ValidationError(
                    _("WhatsApp API did not return a message id."))

            new_doc: dict[str, Any] = {
                "doctype": "WhatsApp Message",
                "type": "Outgoing",
                "message": str(data.get("template")),
                "to": data.get("to"),
                "message_type": "Template",
                "message_id": message_id,
                "content_type": self.content_type,
                "use_template": 1,
                "template": self.template,
                "template_parameters": parameters,
                "whatsapp_account": wa.name,
            }

            if isinstance(doc_data, dict):
                new_doc.update(
                    {
                        "reference_doctype": doc_data.get("doctype"),
                        "reference_name": doc_data.get("name"),
                    }
                )

            frappe.get_doc(new_doc).save(ignore_permissions=True)

            # Set property after alert (type-safe-ish)
            if (
                    isinstance(doc_data, dict)
                    and self.set_property_after_alert
                    and self.property_value
                    ):
                doctype = doc_data.get("doctype")
                name = doc_data.get("name")
                if (
                        isinstance(doctype, str)
                        and doctype
                        and isinstance(name, str) and name
                        ):
                    fieldname = cast(str, self.set_property_after_alert)
                    value: Any = self.property_value

                    meta = frappe.get_meta(doctype)
                    df = meta.get_field(fieldname)
                    if df and df.fieldtype in numeric_fieldtypes:
                        value = cint(value)

                    frappe.db.set_value(doctype, name, fieldname, value)

            frappe.msgprint(
                "WhatsApp Message Triggered",
                indicator="green", alert=True)

            # integration_request payload for log (optional)
            meta_json = frappe.as_json(_integration_request_json())

        except Exception as e:
            error_message = str(e)

            # Try to read Meta error from integration_request, safely
            ir_json = _integration_request_json()
            err = ir_json.get("error")
            if isinstance(err, dict):
                # Some payloads use Error/message keys
                maybe = err.get("Error") or err.get("message")
                if isinstance(maybe, str) and maybe:
                    error_message = maybe

            frappe.msgprint(
                f"Failed to trigger whatsapp message: {error_message}",
                indicator="red",
                alert=True,
            )

            meta_json = frappe.as_json({"error": error_message})

        finally:
            frappe.get_doc(
                {
                    "doctype": "WhatsApp Notification Log",
                    "template": self.template,
                    "meta_data": meta_json,
                }
            ).insert(ignore_permissions=True)

    def on_trash(self):
        """On delete remove from schedule."""
        frappe.cache().delete_value("whatsapp_notification_map")

    def format_number(self, number):
        """Format number."""
        if (number.startswith("+")):
            number = number[1:len(number)]

        return number

    def get_documents_for_today(self) -> int:
        """Send scheduled notifications for documents that match today's
           window.

        Returns:
            Number of documents processed/sent.
        """
        if not self.reference_doctype:
            frappe.throw(_("Please set Reference DocType."))

        # Your generated typing says date_changed is Literal[None] (incorrect),
        # so we treat it as a dynamic attribute and narrow it.
        date_field_raw = getattr(self, "date_changed", None)
        date_field = date_field_raw if isinstance(date_field_raw, str) else ""
        if not date_field:
            frappe.throw(
                _("Please set the Date Changed field for "
                  "scheduler notifications.")
            )

        diff_days = int(self.days_in_advance or 0)
        if self.doctype_event == "Days After":
            diff_days = -diff_days

        # 1) Compute target day using Frappe helper (typed)
        base_dt = now_datetime()
        target_dt = add_to_date(base_dt, days=diff_days)

        # add_to_date() stubs may return datetime|date|str depending on
        # version;
        # we normalize to a Python datetime for consistent typing.
        if isinstance(target_dt, py_datetime):
            target_date = target_dt.date()
        elif hasattr(target_dt, "date"):  # very defensive
            target_date = cast(Any, target_dt).date()
        else:
            # last resort: if some stub says str, let Frappe parse it
            parsed = get_datetime(target_dt)
            if parsed is None:
                raise frappe.ValidationError(
                    _("Could not parse target date from {0}.").format(
                        frappe.bold(str(target_dt))
                    )
                )
            target_date = parsed.date()

        # 2) Build start/end datetimes with stdlib (well-typed)
        start_dt = py_datetime.combine(target_date, py_time.min)
        # Use 23:59:59 to match your previous logic (not microseconds)
        end_dt = py_datetime.combine(target_date, py_time(23, 59, 59))

        # 3) Query
        doc_list = frappe.get_all(
            self.reference_doctype,
            fields=["name"],
            filters=[
                {date_field: (">=", start_dt)},
                {date_field: ("<=", end_dt)},
            ],
        )

        sent = 0
        for row in doc_list:
            name = row.get("name") if isinstance(row, dict) else None
            if not isinstance(name, str) or not name:
                continue

            doc = frappe.get_doc(self.reference_doctype, name)
            self.send_template_message(doc)
            sent += 1

        return sent


@frappe.whitelist()
def call_trigger_notifications():
    """Trigger notifications."""
    try:
        # Directly call the trigger_notifications function
        trigger_notifications()
    except Exception as e:
        # Log the error but do not show any popup or alert
        frappe.log_error(
            frappe.get_traceback(),
            "Error in call_trigger_notifications")
        # Optionally, you could raise the exception to be handled elsewhere if
        # needed
        raise e


def trigger_notifications(method="daily"):
    if frappe.flags.in_import or frappe.flags.in_patch:
        # don't send notifications while syncing or patching
        return

    if method == "daily":
        doc_list = frappe.get_all(
            "WhatsApp Notification",
            filters={"doctype_event": ("in", ("Days Before", "Days After")),
                     "disabled": 0}
        )
        for d in doc_list:
            alert = cast(
                WhatsAppNotification,
                frappe.get_doc("WhatsApp Notification", d.name))
            alert.get_documents_for_today()
