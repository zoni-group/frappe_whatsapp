# Copyright (c) 2022, Shridhar Patil and contributors
# For license information, please see license.txt
import json
import frappe
from frappe import _, throw
from frappe.model.document import Document
from frappe.integrations.utils import make_post_request
from frappe.utils import get_url
from typing import cast, Any
from frappe_whatsapp.utils.routing import set_last_sender_app

from frappe_whatsapp.utils import get_whatsapp_account, format_number
from frappe_whatsapp.utils.consent import (
    verify_consent_for_send,
    is_within_conversation_window,
    enforce_marketing_template_compliance,
)


def _get_integration_request_json() -> dict:
    integration_request = getattr(frappe.flags, "integration_request", None)
    if not integration_request:
        return {}

    json_method = getattr(integration_request, "json", None)
    if not callable(json_method):
        return {}

    try:
        data = json_method()
    except Exception:
        return {}

    return data if isinstance(data, dict) else {}


class WhatsAppMessage(Document):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from frappe.types import DF

        attach: DF.Attach | None
        body_param: DF.JSON | None
        bulk_message_reference: DF.Data | None
        buttons: DF.JSON | None
        consent_bypass_reason: DF.Data | None
        consent_checked: DF.Check
        consent_status_at_send: DF.Literal["Opted In", "Opted Out", "Unknown", "Bypassed"]
        content_type: DF.Literal["text", "document", "image", "video", "audio", "flow", "reaction", "location", "contact", "button", "interactive"]
        conversation_id: DF.Data | None
        external_reference: DF.Data | None
        flow: DF.Link | None
        flow_cta: DF.Data | None
        flow_response: DF.JSON | None
        flow_screen: DF.Data | None
        flow_token: DF.Data | None
        is_opt_in_request: DF.Check
        is_opt_out_request: DF.Check
        is_reply: DF.Check
        label: DF.Data | None
        message: DF.HTMLEditor | None
        message_id: DF.Data | None
        message_type: DF.Literal["Manual", "Template"]
        profile_name: DF.Data | None
        reference_doctype: DF.Link | None
        reference_name: DF.DynamicLink | None
        reply_to_message_id: DF.Data | None
        routed_app: DF.Link | None
        source_app: DF.Link | None
        status: DF.Data | None
        template: DF.Link | None
        template_header_parameters: DF.SmallText | None
        template_parameters: DF.SmallText | None
        to: DF.Data | None
        type: DF.Literal["Outgoing", "Incoming"]
        use_template: DF.Check
        whatsapp_account: DF.Link | None
        within_conversation_window: DF.Check
    # end: auto-generated types

    def validate(self):
        self.set_whatsapp_account()

    def on_update(self):
        self.update_profile_name()

    def update_profile_name(self):
        number = self.get("from")
        if not number:
            return
        from_number = format_number(str(number))

        if (
            self.has_value_changed("profile_name")
            and self.profile_name
            and from_number
            and frappe.db.exists("WhatsApp Profiles", {"number": from_number})
        ):
            profile_id = frappe.get_value(
                "WhatsApp Profiles", {"number": from_number}, "name")
            frappe.db.set_value(
                "WhatsApp Profiles",
                profile_id, "profile_name", self.profile_name)

    def create_whatsapp_profile(self):
        number = format_number(str(self.get("from") or self.to))
        if not frappe.db.exists("WhatsApp Profiles", {"number": number}):
            frappe.get_doc({
                "doctype": "WhatsApp Profiles",
                "profile_name": self.profile_name,
                "number": number,
                "whatsapp_account": self.whatsapp_account
            }).insert(ignore_permissions=True)

    def set_whatsapp_account(self):
        """Set whatsapp account to default if missing"""
        if not self.whatsapp_account:
            account_type = (
                'outgoing' if self.type == 'Outgoing' else 'incoming')
            default_whatsapp_account = get_whatsapp_account(
                account_type=account_type)
            if not default_whatsapp_account:
                throw(_(
                    "Please set a default outgoing WhatsApp Account"
                    " or Select available WhatsApp Account"))
            else:
                self.whatsapp_account = default_whatsapp_account.name

    def _check_consent(self):
        """Verify consent before sending an outgoing message."""
        # Determine if this template is transactional
        is_transactional = False
        consent_category: str | None = None
        if self.template:
            tmpl_data: dict[str, Any] | None = frappe.db.get_value(
                "WhatsApp Templates", self.template,
                fieldname={"is_transactional", "required_consent_category"},
                as_dict=True,
            )
            if isinstance(tmpl_data, dict):
                is_transactional = bool(tmpl_data.get("is_transactional"))
                consent_category = tmpl_data.get("required_consent_category")

        result = verify_consent_for_send(
            str(self.to or ""),
            consent_category=consent_category,
            is_transactional=is_transactional,
        )

        # Record consent status on the message
        self.consent_checked = 1
        self.consent_status_at_send = cast(Any, result.status)
        if not result.allowed:
            self.consent_bypass_reason = result.reason
            frappe.throw(
                _("Cannot send message: {0}").format(result.reason),
                title=_("Consent Required"))

    def _check_conversation_window(self):
        """Enforce 24-hour window for non-template messages."""
        from frappe_whatsapp.utils.consent import get_compliance_settings

        within, reason = is_within_conversation_window(
            str(self.to or ""),
            whatsapp_account=self.whatsapp_account,
        )

        self.within_conversation_window = 1 if within else 0

        if not within:
            settings = get_compliance_settings()
            if not settings.allow_reply_outside_window:
                frappe.throw(
                    _(
                        "Cannot send free-form message outside"
                        " the conversation window. Use an approved"
                        " template instead. {0}"
                    ).format(reason),
                    title=_("Outside Conversation Window"),
                )

    """Record last sender app"""
    def after_insert(self):
        if (self.type == "Outgoing" and self.source_app and
                self.to and self.whatsapp_account):
            set_last_sender_app(
                whatsapp_account=self.whatsapp_account,
                to_number=self.to,
                source_app=str(self.source_app),
                message_name=self.name,
            )

    """Send whats app messages."""
    def before_insert(self):
        """Send message."""
        self.set_whatsapp_account()

        # Consent + window checks only for messages not yet sent.
        # Docs created with message_id already set (e.g. from
        # notification.notify()) are log records of already-sent messages.
        if self.type == "Outgoing" and self.to and not self.message_id:
            self._check_consent()

            # 24-hour window: non-template messages need recent incoming
            if self.message_type != "Template":
                self._check_conversation_window()

        if self.type == "Outgoing" and self.message_type != "Template":
            if self.attach and not self.attach.startswith("http"):
                link = get_url() + "/" + self.attach
            else:
                link = self.attach

            data: dict[str, Any] = {
                "messaging_product": "whatsapp",
                "to": format_number(self.to),
                "type": self.content_type,
            }
            if self.is_reply and self.reply_to_message_id:
                data["context"] = {"message_id": self.reply_to_message_id}
            if self.content_type in ["document", "image", "video"]:
                data[self.content_type.lower()] = {
                    "link": link,
                    "caption": self.message,
                }
            elif self.content_type == "reaction":
                data["reaction"] = {
                    "message_id": self.reply_to_message_id,
                    "emoji": self.message,
                }
            elif self.content_type == "text":
                data["text"] = {"preview_url": True, "body": self.message}

            elif self.content_type == "audio":
                data["audio"] = {"link": link}

            elif self.content_type == "interactive":
                # Interactive message (buttons or list)
                data["type"] = "interactive"

                if isinstance(self.buttons, str):
                    buttons_data = json.loads(
                        self.buttons) if self.buttons else []
                else:
                    buttons_data = self.buttons or []

                if not isinstance(buttons_data, list):
                    frappe.throw(
                        _("Buttons must be a list for interactive messages"))

                if not buttons_data:
                    frappe.throw(
                        _("Buttons are required for interactive messages"))

                if len(buttons_data) > 3:
                    # Use list message for more than 3 options (max 10)
                    data["interactive"] = {
                        "type": "list",
                        "body": {"text": self.message},
                        "action": {
                            "button": "Select Option",
                            "sections": [{
                                "title": "Options",
                                "rows": [
                                    {
                                        "id": btn["id"],
                                        "title": btn["title"],
                                        "description": btn.get(
                                            "description", "")}
                                    for btn in buttons_data[:10]
                                ]
                            }]
                        }
                    }
                else:
                    # Use button message for 3 or fewer options
                    data["interactive"] = {
                        "type": "button",
                        "body": {"text": self.message},
                        "action": {
                            "buttons": [
                                {
                                    "type": "reply",
                                    "reply": {
                                        "id": btn["id"],
                                        "title": btn["title"]}
                                }
                                for btn in buttons_data[:3]
                            ]
                        }
                    }

            elif self.content_type == "flow":
                # WhatsApp Flow message
                if not self.flow:
                    frappe.throw(
                        _("WhatsApp Flow is required for flow content type"))
                from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_flow.whatsapp_flow import WhatsAppFlow  # noqa: E501
                flow_doc = cast(
                    WhatsAppFlow,
                    frappe.get_doc(
                        "WhatsApp Flow",
                        str(self.flow)))

                if not flow_doc.flow_id:
                    frappe.throw(_(
                        "Flow must be created on WhatsApp before sending"))

                # Determine flow mode - draft flows can be tested with mode:
                # "draft"
                flow_mode = None
                if flow_doc.status != "Published":
                    flow_mode = "draft"
                    frappe.msgprint(
                        _("Sending flow in draft mode (for testing only)"),
                        indicator="orange")

                # Get first screen if not specified
                flow_screen = self.flow_screen
                if not flow_screen and flow_doc.screens:
                    first_screen = flow_doc.screens[0]
                    flow_screen = (
                        getattr(first_screen, "screen_id", None)
                        or getattr(first_screen, "screen", None)
                        or getattr(first_screen, "screen_name", None)
                        or getattr(first_screen, "name", None)
                    )

                if not flow_screen:
                    frappe.throw(
                        _("Flow screen is required to send flow message"))

                data["type"] = "interactive"
                data["interactive"] = {
                    "type": "flow",
                    "body": {
                        "text": self.message or "Please fill out the form"},
                    "action": {
                        "name": "flow",
                        "parameters": {
                            "flow_message_version": "3",
                            "flow_id": flow_doc.flow_id,
                            "flow_cta": (
                                self.flow_cta or flow_doc.flow_cta or "Open"),
                            "flow_action": "navigate",
                            "flow_action_payload": {
                                "screen": flow_screen
                            }
                        }
                    }
                }

                # Add draft mode for testing unpublished flows
                if flow_mode:
                    data["interactive"]["action"]["parameters"][
                        "mode"] = flow_mode

                # Add flow token - generate one if not provided (required by
                # WhatsApp)
                flow_token = self.flow_token or frappe.generate_hash(length=16)
                data["interactive"]["action"]["parameters"][
                    "flow_token"] = flow_token

            try:
                self.notify(data)
                self.status = "Success"
            except Exception as e:
                self.status = "Failed"
                frappe.throw(f"Failed to send message {str(e)}")
        elif self.type == "Outgoing" and self.message_type == "Template" and \
                not self.message_id:
            self.send_template()

        self.create_whatsapp_profile()

    def send_template(self):
        """Send template."""
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import WhatsAppTemplates  # noqa: E501
        if not self.template:
            frappe.throw(_("Template is required to send template message"))
            return
        template = cast(
            WhatsAppTemplates,
            frappe.get_doc("WhatsApp Templates", self.template)
        )
        enforce_marketing_template_compliance(template)
        data: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "to": format_number(self.to),
            "type": "template",
            "template": {
                "name": template.actual_name or template.template_name,
                "language": {"code": template.language_code},
                "components": [],
            },
        }

        if template.sample_values:
            field_names = (template.field_names.split(",")
                           if template.field_names else
                           template.sample_values.split(","))
            parameters = []
            template_parameters = []

            if self.body_param is not None:
                params = list(json.loads(self.body_param).values())
                for param in params:
                    parameters.append({"type": "text", "text": param})
                    template_parameters.append(param)
            elif self.flags.custom_ref_doc:
                custom_values = self.flags.custom_ref_doc
                for field_name in field_names:
                    value = custom_values.get(field_name.strip())
                    parameters.append({"type": "text", "text": value})
                    template_parameters.append(value)

            else:
                if not (self.reference_doctype and self.reference_name):
                    frappe.throw(
                        _("Reference Doctype and Reference Name are required"
                          " to fetch template parameters"))
                    return
                ref_doc = frappe.get_doc(
                    self.reference_doctype, self.reference_name)
                for field_name in field_names:
                    value = ref_doc.get_formatted(field_name.strip())
                    parameters.append({"type": "text", "text": value})
                    template_parameters.append(value)

            self.template_parameters = json.dumps(template_parameters)
            data["template"]["components"].append(
                {
                    "type": "body",
                    "parameters": parameters,
                }
            )

        if template.header_type:
            if self.attach:
                if template.header_type == 'IMAGE':

                    if self.attach.startswith("http"):
                        url = f'{self.attach}'
                    else:
                        url = f'{get_url()}{self.attach}'
                    data['template']['components'].append({
                        "type": "header",
                        "parameters": [{
                            "type": "image",
                            "image": {
                                "link": url
                            }
                        }]
                    })

            elif template.sample:
                if template.header_type == 'IMAGE':
                    if template.sample.startswith("http"):
                        url = f'{template.sample}'
                    else:
                        url = f'{get_url()}{template.sample}'
                    data['template']['components'].append({
                        "type": "header",
                        "parameters": [{
                            "type": "image",
                            "image": {
                                "link": url
                            }
                        }]
                    })

        if template.buttons:
            button_parameters = []
            for idx, btn in enumerate(template.buttons):
                if btn.button_type == "Quick Reply":
                    button_parameters.append({
                        "type": "button",
                        "sub_type": "quick_reply",
                        "index": str(idx),
                        "parameters": [
                            {"type": "payload",
                             "payload": btn.button_label}]
                    })
                elif btn.button_type == "Call Phone":
                    button_parameters.append({
                        "type": "button",
                        "sub_type": "phone_number",
                        "index": str(idx),
                        "parameters": [
                            {"type": "text", "text": btn.phone_number}]
                    })
                elif btn.button_type == "Visit Website":
                    url = btn.website_url
                    if btn.url_type == "Dynamic":
                        if not (self.reference_doctype and
                                self.reference_name):
                            frappe.throw(
                                _("Reference Doctype and Reference Name are"
                                  " required to fetch dynamic url"))
                            return
                        ref_doc = frappe.get_doc(
                            self.reference_doctype, self.reference_name)
                        url = ref_doc.get_formatted(btn.website_url)
                    button_parameters.append({
                        "type": "button",
                        "sub_type": "url",
                        "index": str(idx),
                        "parameters": [{"type": "text", "text": url}]
                    })

            if button_parameters:
                data['template']['components'].extend(button_parameters)

        self.notify(data)

    def notify(self, data):
        """Notify."""
        if not self.whatsapp_account:
            frappe.throw(_("WhatsApp Account is required to send message"))
            return

        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa: E501
        whatsapp_account = cast(
            WhatsAppAccount,
            frappe.get_doc(
                "WhatsApp Account",
                self.whatsapp_account,
            )
        )

        token = whatsapp_account.get_password("token")

        headers = {
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        }
        try:
            response = make_post_request(
                (f"{whatsapp_account.url}/{whatsapp_account.version}"
                 f"/{whatsapp_account.phone_id}/messages"),
                headers=headers,
                data=json.dumps(data),
            )

            response_dict: dict[str, Any] = {}
            if response is None:
                response_dict = {}
            elif isinstance(response, str):
                try:
                    parsed = json.loads(response)
                    response_dict = parsed if isinstance(parsed, dict) else {}
                except Exception:
                    response_dict = {}
            elif isinstance(response, dict):
                response_dict = response
            else:
                response_dict = {}

            messages = response_dict.get("messages")
            if isinstance(messages, list) and messages:
                first = messages[0]
                if isinstance(first, dict):
                    message_id = first.get("id")
                    if isinstance(message_id, str) and message_id:
                        self.message_id = message_id

        except Exception:
            integration_json = _get_integration_request_json()
            res = integration_json.get("error", {}) if isinstance(
                integration_json, dict) else {}
            error_message = res.get("Error", res.get("message"))
            frappe.get_doc(
                {
                    "doctype": "WhatsApp Notification Log",
                    "template": "Text Message",
                    "meta_data": integration_json,
                }
            ).insert(ignore_permissions=True)

            frappe.throw(
                msg=error_message or _("Failed to send WhatsApp message"),
                title=res.get("error_user_title", "Error"))

    def format_number(self, number):
        """Format number."""
        if number.startswith("+"):
            number = number[1: len(number)]

        return number

    @frappe.whitelist()
    def send_read_receipt(self):
        data: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": self.message_id
        }
        if not self.whatsapp_account:
            frappe.throw(_("WhatsApp Account is required to send message"))
            return

        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_account.whatsapp_account import WhatsAppAccount  # noqa: E501
        settings = cast(
            WhatsAppAccount,
            frappe.get_doc(
                "WhatsApp Account",
                self.whatsapp_account
                )
        )

        token = settings.get_password("token")

        headers = {
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        }
        try:
            response = make_post_request(
                f"{settings.url}/{settings.version}/" +
                f"{settings.phone_id}/messages",
                headers=headers,
                data=json.dumps(data),
            )

            if response is None:
                return None

            response_dict = response
            if isinstance(response_dict, str):
                try:
                    response_dict = json.loads(response_dict)
                except Exception:
                    response_dict = {}

            if not isinstance(response_dict, dict):
                response_dict = {}

            success = response_dict.get("success")

            if success:
                self.status = "marked as read"
                self.save()
                return response_dict.get("success")

        except Exception:
            integration_json = _get_integration_request_json()
            res = integration_json.get("error", {}) if isinstance(
                integration_json, dict) else {}
            error_message = res.get("Error", res.get("message"))
            frappe.log_error("WhatsApp API Error", f"{error_message}\n{res}")


def on_doctype_update():
    frappe.db.add_index(
        "WhatsApp Message", ["reference_doctype", "reference_name"])


@frappe.whitelist()
def send_template(to, reference_doctype, reference_name, template):
    try:
        doc = frappe.get_doc({
            "doctype": "WhatsApp Message",
            "to": to,
            "type": "Outgoing",
            "message_type": "Template",
            "reference_doctype": reference_doctype,
            "reference_name": reference_name,
            "content_type": "text",
            "template": template
        })

        doc.save()
    except Exception as e:
        raise e
