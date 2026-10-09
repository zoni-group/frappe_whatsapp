from __future__ import annotations

import json
from typing import Protocol, cast
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message.whatsapp_message import (
    WhatsAppMessage,
)
from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profile_alias.whatsapp_profile_alias import (
    WhatsAppProfileAlias,
)
from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_profiles.whatsapp_profiles import (
    WhatsAppProfiles,
)

from frappe_whatsapp.utils.identity import (
    apply_user_id_update,
    alias_key,
    get_identity_scope,
    resolve_identity,
    set_meta_recipient,
)
from frappe_whatsapp.utils.consent import (
    get_or_create_account_state,
    verify_consent_for_send,
)
from frappe_whatsapp.utils.webhook import process_webhook_payload
from frappe_whatsapp.utils.routing import (
    serialize_incoming_message_for_forwarding,
)


class _AccountFixture(Protocol):
    name: str
    phone_id: str


class TestBusinessScopedIdentity(FrappeTestCase):
    def _account(
        self, *, portfolio_id: str | None = None
    ) -> _AccountFixture:
        suffix = frappe.generate_hash(length=8)
        return cast(
            _AccountFixture,
            frappe.get_doc({
                "doctype": "WhatsApp Account",
                "account_name": f"BSUID Test {suffix}",
                "status": "Active",
                "phone_id": f"bsuid-phone-{suffix}",
                "business_portfolio_id": portfolio_id,
            }).insert(ignore_permissions=True),
        )

    def test_meta_recipient_prefers_phone_and_supports_parent_id(self):
        payload = {"recipient": "US.13491208655302741918"}
        recipient_type = set_meta_recipient(
            payload,
            to="+1 (650) 555-1234",
            recipient="US.13491208655302741918",
        )
        self.assertEqual(recipient_type, "phone")
        self.assertEqual(payload["to"], "16505551234")
        self.assertNotIn("recipient", payload)

        payload = {}
        recipient_type = set_meta_recipient(
            payload,
            to=None,
            recipient="US.ENT.11815799212886844830",
        )
        self.assertEqual(recipient_type, "parent_user_id")
        self.assertEqual(
            payload["recipient"], "US.ENT.11815799212886844830"
        )

    def test_aliases_resolve_within_portfolio_but_not_across_portfolios(self):
        portfolio = f"portfolio-{frappe.generate_hash(length=8)}"
        first_account = self._account(portfolio_id=portfolio)
        second_account = self._account(portfolio_id=portfolio)
        other_account = self._account(
            portfolio_id=f"other-{frappe.generate_hash(length=8)}"
        )
        user_id = "US.13491208655302741918"
        parent_user_id = "US.ENT.11815799212886844830"

        profile = resolve_identity(
            whatsapp_account=first_account.name,
            identity={
                "phone": "+1 (650) 555-1234",
                "user_id": user_id,
                "parent_user_id": parent_user_id,
                "username": "first_handle",
                "profile_name": "First User",
            },
        )
        same_profile = resolve_identity(
            whatsapp_account=second_account.name,
            identity={"user_id": user_id, "username": "new_handle"},
        )
        separate_profile = resolve_identity(
            whatsapp_account=other_account.name,
            identity={"user_id": user_id},
        )

        self.assertEqual(same_profile.name, profile.name)
        self.assertNotEqual(separate_profile.name, profile.name)
        self.assertEqual(same_profile.username, "new_handle")
        scope = get_identity_scope(first_account.name)
        for alias_type, value in (
            ("phone", "16505551234"),
            ("user_id", user_id),
            ("parent_user_id", parent_user_id),
        ):
            alias = cast(
                WhatsAppProfileAlias,
                frappe.get_doc(
                    "WhatsApp Profile Alias",
                    alias_key(scope, alias_type, value),
                ),
            )
            self.assertEqual(alias.whatsapp_profile, profile.name)
            self.assertEqual(alias.is_current, 1)

    def test_consent_is_account_specific_for_shared_portfolio_profile(self):
        portfolio = f"portfolio-{frappe.generate_hash(length=8)}"
        first_account = self._account(portfolio_id=portfolio)
        second_account = self._account(portfolio_id=portfolio)
        profile = resolve_identity(
            whatsapp_account=first_account.name,
            identity={"user_id": "US.13491208655302741918"},
        )
        same_profile = resolve_identity(
            whatsapp_account=second_account.name,
            identity={"user_id": "US.13491208655302741918"},
        )
        self.assertEqual(same_profile.name, profile.name)

        opted_out = get_or_create_account_state(
            profile.name, first_account.name
        )
        opted_out.db_set({
            "is_opted_out": 1,
            "is_opted_in": 0,
            "consent_status": "Opted Out",
        })
        opted_in = get_or_create_account_state(
            profile.name, second_account.name
        )
        opted_in.db_set({
            "is_opted_out": 0,
            "is_opted_in": 1,
            "consent_status": "Opted In",
        })
        settings = frappe._dict({
            "consent_check_mode": "Strict",
            "enforce_consent_check": 1,
            "allow_transactional_without_consent": 0,
        })

        with patch(
            "frappe_whatsapp.utils.consent.get_compliance_settings",
            return_value=settings,
        ):
            first_result = verify_consent_for_send(
                "",
                contact_profile=profile.name,
                whatsapp_account=first_account.name,
            )
            second_result = verify_consent_for_send(
                "",
                contact_profile=profile.name,
                whatsapp_account=second_account.name,
            )

        self.assertFalse(first_result.allowed)
        self.assertEqual(first_result.status, "Opted Out")
        self.assertTrue(second_result.allowed)
        self.assertEqual(second_result.status, "Opted In")

    def test_explicit_identity_merge_keeps_more_restrictive_consent(self):
        account = self._account()
        phone = "16505551235"
        user_id = "US.13491208655302741919"
        phone_profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": phone},
        )
        user_profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": user_id},
        )
        opted_in = get_or_create_account_state(
            phone_profile.name, account.name
        )
        opted_in.db_set({
            "is_opted_in": 1,
            "is_opted_out": 0,
            "consent_status": "Opted In",
        })
        get_or_create_account_state(user_profile.name, account.name)

        merged = resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": phone, "user_id": user_id},
            explicit_link=True,
        )

        self.assertEqual(merged.name, phone_profile.name)
        merged_state = get_or_create_account_state(
            merged.name, account.name
        )
        self.assertFalse(merged_state.is_opted_in)
        self.assertEqual(merged_state.consent_status, "Unknown")
        scope = get_identity_scope(account.name)
        self.assertEqual(
            frappe.db.get_value(
                "WhatsApp Profile Alias",
                alias_key(scope, "user_id", user_id),
                "whatsapp_profile",
            ),
            merged.name,
        )

    @patch(
        "frappe_whatsapp.utils.routing."
        "forward_identity_update_to_app_async"
    )
    def test_nested_user_id_update_rotates_aliases(
        self, mock_forward_identity_update
    ):
        account = self._account()
        old_user_id = "US.1033196179544075"
        new_user_id = "US.2033196179544075"
        old_parent_id = "US.ENT.1033196179544075"
        new_parent_id = "US.ENT.2033196179544075"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={
                "phone": "16505550199",
                "user_id": old_user_id,
                "parent_user_id": old_parent_id,
            },
        )

        process_webhook_payload({
            "entry": [{
                "changes": [{
                    "field": "user_id_update",
                    "value": {
                        "metadata": {"phone_number_id": account.phone_id},
                        "contacts": [{
                            "wa_id": "16505550199",
                            "profile": {"username": "rotated_handle"},
                        }],
                        "user_id_update": [{
                            "wa_id": "16505550199",
                            "user_id": {
                                "previous": old_user_id,
                                "current": new_user_id,
                            },
                            "parent_user_id": {
                                "previous": old_parent_id,
                                "current": new_parent_id,
                            },
                        }],
                    },
                }],
            }],
        })

        profile.reload()
        self.assertEqual(profile.user_id, new_user_id)
        self.assertEqual(profile.parent_user_id, new_parent_id)
        self.assertEqual(profile.username, "rotated_handle")
        scope = get_identity_scope(account.name)
        for alias_type, old_value, new_value in (
            ("user_id", old_user_id, new_user_id),
            ("parent_user_id", old_parent_id, new_parent_id),
        ):
            self.assertEqual(
                frappe.db.get_value(
                    "WhatsApp Profile Alias",
                    alias_key(scope, alias_type, old_value),
                    "is_current",
                ),
                0,
            )
            self.assertEqual(
                frappe.db.get_value(
                    "WhatsApp Profile Alias",
                    alias_key(scope, alias_type, new_value),
                    "is_current",
                ),
                1,
            )
        mock_forward_identity_update.assert_called_once()

    @patch(
        "frappe_whatsapp.utils.routing."
        "forward_identity_update_to_app_async"
    )
    def test_system_user_id_change_links_new_identity_to_old_profile(
        self, mock_forward_identity_update
    ):
        account = self._account()
        old_phone = "16505550198"
        new_phone = "16505550199"
        old_user_id = "US.1033196179544075"
        new_user_id = "US.2033196179544075"
        new_parent_id = "US.ENT.2033196179544075"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": old_phone, "user_id": old_user_id},
        )

        process_webhook_payload({
            "entry": [{
                "changes": [{
                    "field": "messages",
                    "value": {
                        "metadata": {"phone_number_id": account.phone_id},
                        "messages": [{
                            "from": new_phone,
                            "id": f"wamid.{frappe.generate_hash(length=8)}",
                            "type": "system",
                            "system": {
                                "type": "user_changed_user_id",
                                "body": (
                                    f"User changed from {old_user_id} "
                                    f"to {new_user_id}"
                                ),
                                "wa_id": new_phone,
                                "user_id": new_user_id,
                                "parent_user_id": new_parent_id,
                            },
                        }],
                    },
                }],
            }],
        })

        profile.reload()
        self.assertEqual(profile.number, new_phone)
        self.assertEqual(profile.user_id, new_user_id)
        self.assertEqual(profile.parent_user_id, new_parent_id)
        scope = get_identity_scope(account.name)
        for alias_type, old_value, new_value in (
            ("phone", old_phone, new_phone),
            ("user_id", old_user_id, new_user_id),
        ):
            self.assertEqual(
                frappe.db.get_value(
                    "WhatsApp Profile Alias",
                    alias_key(scope, alias_type, old_value),
                    "is_current",
                ),
                0,
            )
            self.assertEqual(
                frappe.db.get_value(
                    "WhatsApp Profile Alias",
                    alias_key(scope, alias_type, new_value),
                    "is_current",
                ),
                1,
            )
        self.assertEqual(
            frappe.db.get_value(
                "WhatsApp Profile Alias",
                alias_key(scope, "parent_user_id", new_parent_id),
                "whatsapp_profile",
            ),
            profile.name,
        )
        mock_forward_identity_update.assert_called_once()

    def test_rotation_queues_active_block_reapplication(self):
        account = self._account()
        old_user_id = "US.1033196179544075"
        new_user_id = "US.2033196179544075"
        profile = resolve_identity(
            whatsapp_account=account.name,
            identity={"user_id": old_user_id},
        )
        from frappe_whatsapp.utils.blocking import block_contact

        block_contact(
            whatsapp_account=account.name,
            user_id=old_user_id,
            contact_profile=profile.name,
            sync_meta=False,
        )
        with patch("frappe_whatsapp.utils.identity.frappe.enqueue") as enqueue:
            rotated = apply_user_id_update(
                whatsapp_account=account.name,
                update={
                    "previous_user_id": old_user_id,
                    "current_user_id": new_user_id,
                },
            )

        enqueue.assert_called_once_with(
            "frappe_whatsapp.utils.blocking.reapply_block_after_rotation",
            queue="short",
            enqueue_after_commit=True,
            whatsapp_account=account.name,
            contact_profile=rotated.name,
            user_id=new_user_id,
        )

    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    @patch("frappe_whatsapp.utils.webhook.is_contact_blocked", return_value=False)
    def test_bsuid_only_message_is_persisted(
        self, _mock_blocked, mock_forward
    ):
        account = self._account()
        message_id = f"wamid.{frappe.generate_hash(length=12)}"
        user_id = "US.13491208655302741918"
        parent_user_id = "US.ENT.11815799212886844830"

        process_webhook_payload({
            "entry": [{
                "changes": [{
                    "field": "messages",
                    "value": {
                        "metadata": {"phone_number_id": account.phone_id},
                        "contacts": [{
                            "profile": {
                                "name": "Pablo M.",
                                "username": "pablomorales",
                            },
                            "user_id": user_id,
                            "parent_user_id": parent_user_id,
                        }],
                        "messages": [{
                            "from_user_id": user_id,
                            "from_parent_user_id": parent_user_id,
                            "id": message_id,
                            "type": "text",
                            "text": {"body": "BSUID only"},
                        }],
                    },
                }],
            }],
        })

        message = cast(
            WhatsAppMessage,
            frappe.get_doc(
                "WhatsApp Message",
                str(frappe.db.get_value(
                    "WhatsApp Message", {"message_id": message_id}, "name"
                )),
            ),
        )
        self.assertFalse(message.get("from"))
        self.assertEqual(message.from_user_id, user_id)
        self.assertEqual(message.from_parent_user_id, parent_user_id)
        self.assertEqual(message.username, "pablomorales")
        self.assertTrue(message.contact_profile)
        mock_forward.assert_called_once_with(
            incoming_message_name=message.name
        )

    @patch("frappe_whatsapp.utils.webhook.forward_incoming_to_app_async")
    @patch("frappe_whatsapp.utils.webhook.is_contact_blocked", return_value=False)
    def test_contact_request_reply_links_shared_phone(
        self, _mock_blocked, _mock_forward
    ):
        account = self._account()
        user_id = "US.13491208655302741918"
        message_id = f"wamid.{frappe.generate_hash(length=12)}"
        process_webhook_payload({
            "entry": [{"changes": [{
                "field": "messages",
                "value": {
                    "metadata": {"phone_number_id": account.phone_id},
                    "contacts": [{"user_id": user_id}],
                    "messages": [{
                        "from_user_id": user_id,
                        "id": message_id,
                        "type": "contacts",
                        "contacts": [{
                            "origin": "contact_request",
                            "phones": [{"phone": "+1 650 555 1234"}],
                        }],
                    }],
                },
            }]}],
        })

        message = cast(
            WhatsAppMessage,
            frappe.get_doc(
                "WhatsApp Message",
                str(frappe.db.get_value(
                    "WhatsApp Message", {"message_id": message_id}, "name"
                )),
            ),
        )
        profile = cast(
            WhatsAppProfiles,
            frappe.get_doc(
                "WhatsApp Profiles", str(message.contact_profile)
            ),
        )
        self.assertEqual(message.contact_origin, "contact_request")
        forwarded = serialize_incoming_message_for_forwarding(
            incoming_message_doc=message
        )
        self.assertEqual(
            forwarded["contact_payload"]["contacts"][0]["origin"],
            "contact_request",
        )
        self.assertEqual(profile.user_id, user_id)
        self.assertEqual(profile.number, "16505551234")
        scope = get_identity_scope(account.name)
        self.assertEqual(
            frappe.db.get_value(
                "WhatsApp Profile Alias",
                alias_key(scope, "phone", "16505551234"),
                "whatsapp_profile",
            ),
            profile.name,
        )

    def test_status_persists_raw_contact_and_recipient_identifiers(self):
        account = self._account()
        initial_user_id = f"US.{frappe.generate_hash(length=20)}"
        resolve_identity(
            whatsapp_account=account.name,
            identity={"phone": "16505551234", "user_id": initial_user_id},
        )
        message_id = f"wamid.{frappe.generate_hash(length=12)}"
        with patch(
            "frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_message."
            "whatsapp_message.WhatsAppMessage.notify"
        ):
            message = cast(
                WhatsAppMessage,
                frappe.get_doc({
                    "doctype": "WhatsApp Message",
                    "type": "Outgoing",
                    "to": "16505551234",
                    "message": "hello",
                    "message_id": message_id,
                    "content_type": "text",
                    "whatsapp_account": account.name,
                }).insert(ignore_permissions=True),
            )
        user_id = "US.13491208655302741918"
        parent_user_id = "US.ENT.11815799212886844830"
        contacts = [{
            "profile": {"name": "Pablo M.", "username": "pablomorales"},
            "wa_id": "16505551234",
            "user_id": user_id,
            "parent_user_id": parent_user_id,
        }]

        process_webhook_payload({
            "entry": [{"changes": [{
                "field": "messages",
                "value": {
                    "metadata": {"phone_number_id": account.phone_id},
                    "contacts": contacts,
                    "statuses": [{
                        "id": message_id,
                        "status": "delivered",
                        "recipient_id": "16505551234",
                        "recipient_user_id": user_id,
                        "recipient_parent_user_id": parent_user_id,
                    }],
                },
            }]}],
        })

        message.reload()
        self.assertEqual(message.status, "delivered")
        self.assertEqual(message.status_recipient_phone, "16505551234")
        self.assertEqual(message.recipient_user_id, user_id)
        self.assertEqual(message.recipient_parent_user_id, parent_user_id)
        self.assertEqual(message.recipient, initial_user_id)
        self.assertEqual(
            json.loads(str(message.status_contacts)), {"contacts": contacts}
        )
