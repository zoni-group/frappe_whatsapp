# Copyright (c) 2026, Shridhar Patil and Contributors
# See license.txt

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from threading import Event
from typing import Any, Protocol, TypedDict, cast
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase
from frappe.utils import get_datetime

from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_conversation_route.whatsapp_conversation_route import (
    WhatsAppConversationRoute,
)
from frappe_whatsapp.utils.routing import (
    ROUTE_DOCTYPE,
    get_last_sender_app,
    resolve_incoming_routed_app,
    set_last_sender_app,
)


class _NamedFixture(Protocol):
    name: str


class _RouteLookupArgs(TypedDict):
    whatsapp_account: str
    contact_number: str
    contact_profile: str | None


class TestWhatsAppConversationRoute(FrappeTestCase):
    def setUp(self):
        self.apps = []
        self.accounts = []
        self.profiles = []
        self.committed = False
        self.addCleanup(self._cleanup_records)
        self.default_app = self._app()
        self.sender_app = self._app()
        self.account = self._account(self.default_app.name)

    def _app(self) -> _NamedFixture:
        app = cast(_NamedFixture, frappe.get_doc({
            "doctype": "WhatsApp Client App",
            "app_id": f"route-test-{frappe.generate_hash(length=10)}",
            "enabled": 1,
        }).insert(ignore_permissions=True))
        self.apps.append(app.name)
        return app

    def _account(self, default_app: str | None = None) -> _NamedFixture:
        account = cast(_NamedFixture, frappe.get_doc({
            "doctype": "WhatsApp Account",
            "account_name": f"Route Test {frappe.generate_hash(length=10)}",
            "status": "Active",
            "whatsapp_client_app": default_app,
        }).insert(ignore_permissions=True))
        self.accounts.append(account.name)
        return account

    def _profile(self) -> _NamedFixture:
        profile = cast(_NamedFixture, frappe.get_doc({
            "doctype": "WhatsApp Profiles",
            "profile_name": "Route Test",
            "whatsapp_account": self.account.name,
        }).insert(ignore_permissions=True))
        self.profiles.append(profile.name)
        return profile

    def _route(
        self, *, number: str = "15551234567", profile: str | None = None,
        **values: Any,
    ) -> WhatsAppConversationRoute:
        return cast(WhatsAppConversationRoute, frappe.get_doc({
            "doctype": ROUTE_DOCTYPE,
            "whatsapp_account": self.account.name,
            "contact_number": number,
            "contact_profile": profile,
            "last_source_app": self.sender_app.name,
            **values,
        }).insert(ignore_permissions=True))

    def _cleanup_records(self):
        frappe.db.rollback()
        if self.committed:
            # Concurrency fixtures must be visible to another connection.
            frappe.db.delete(ROUTE_DOCTYPE, {"whatsapp_account": ["in", self.accounts]})
            for doctype, names in (
                ("WhatsApp Profiles", self.profiles),
                ("WhatsApp Account", self.accounts),
                ("WhatsApp Client App", self.apps),
            ):
                if names:
                    frappe.db.delete(doctype, {"name": ["in", names]})
            frappe.db.commit()

    def test_phone_fallback_reuses_legacy_route_and_preserves_sender(self):
        current = self._profile()
        previous = self._profile()
        for index, old_profile in enumerate((None, previous.name)):
            with self.subTest(old_profile=old_profile):
                number = f"1555123456{index}"
                route = self._route(
                    number=number, profile=old_profile,
                    last_outgoing_message="outgoing-original",
                    last_outgoing_at="2026-06-10 14:13:55",
                )
                args = dict(
                    whatsapp_account=self.account.name,
                    contact_number=f"+{number}",
                    contact_profile=current.name,
                )
                self.assertEqual(get_last_sender_app(**args), self.sender_app.name)
                self.assertEqual(resolve_incoming_routed_app(**args), self.sender_app.name)
                route.reload()
                self.assertEqual(route.contact_profile, old_profile or current.name)
                self.assertEqual(route.last_outgoing_message, "outgoing-original")
                self.assertEqual(route.last_outgoing_at, get_datetime("2026-06-10 14:13:55"))
                self.assertEqual(frappe.db.count(ROUTE_DOCTYPE, {
                    "whatsapp_account": self.account.name, "contact_number": number,
                }), 1)
                # Keep the next subcase independent of profile precedence.
                frappe.db.delete(ROUTE_DOCTYPE, {"name": route.name})

    def test_profile_route_takes_precedence_over_phone_fallback(self):
        profile = self._profile()
        route = self._route(number="15551234568", profile=profile.name)
        phone_route = self._route(last_source_app=self.default_app.name)
        assert isinstance(phone_route.contact_number, str)
        args = dict(
            whatsapp_account=self.account.name,
            contact_number=phone_route.contact_number,
            contact_profile=profile.name,
        )
        self.assertEqual(get_last_sender_app(**args), route.last_source_app)
        self.assertEqual(resolve_incoming_routed_app(**args), route.last_source_app)
        self.assertEqual(frappe.db.count(ROUTE_DOCTYPE, {
            "whatsapp_account": self.account.name,
        }), 2)

    def test_phone_fallback_stays_within_account(self):
        self._route()
        other_account = self._account()
        args = dict(whatsapp_account=other_account.name, contact_number="15551234567")
        self.assertIsNone(get_last_sender_app(**args))
        self.assertIsNone(resolve_incoming_routed_app(**args))
        self.assertFalse(frappe.db.exists(ROUTE_DOCTYPE, {"whatsapp_account": other_account.name}))

    def test_outgoing_updates_existing_route_without_replacing_profile(self):
        previous, current = self._profile(), self._profile()
        route = self._route(profile=previous.name, last_source_app=self.default_app.name)
        set_last_sender_app(
            whatsapp_account=self.account.name, to_number="+15551234567",
            contact_profile=current.name, source_app=self.sender_app.name,
            message_name="outgoing-new",
        )
        route.reload()
        self.assertEqual(route.last_source_app, self.sender_app.name)
        self.assertEqual(route.last_outgoing_message, "outgoing-new")
        self.assertIsNotNone(route.last_outgoing_at)
        self.assertEqual(route.contact_profile, previous.name)
        self.assertEqual(frappe.db.count(ROUTE_DOCTYPE, {"whatsapp_account": self.account.name}), 1)

    def test_profile_only_route_is_reused_by_incoming_and_outgoing(self):
        profile = self._profile()
        args = dict(whatsapp_account=self.account.name, contact_profile=profile.name)
        self.assertEqual(resolve_incoming_routed_app(**args), self.default_app.name)
        set_last_sender_app(**args, source_app=self.sender_app.name, message_name="outgoing-profile")
        self.assertEqual(resolve_incoming_routed_app(**args), self.sender_app.name)
        rows = frappe.get_all(ROUTE_DOCTYPE, filters=args, fields=["name", "contact_number"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0].name), 20)
        self.assertFalse(rows[0].contact_number)

    def test_route_naming_uses_controller_without_hash_collision_retries(self):
        self.assertNotEqual(frappe.get_meta(ROUTE_DOCTYPE).get("autoname"), "hash")
        route = self._route()
        self.assertEqual(route.name, f"15551234567-{self.account.name}")
        duplicate = frappe.copy_doc(route)
        frappe.db.savepoint("route_duplicate")
        try:
            with self.assertRaises(frappe.DuplicateEntryError):
                duplicate.insert(ignore_permissions=True)
            self.assertFalse(duplicate.flags.retry_count)
        finally:
            frappe.db.rollback(save_point="route_duplicate")

    def test_database_errors_propagate(self):
        error = frappe.db.OperationalError(2003, "connection unavailable")
        with patch.object(frappe.db, "get_value", side_effect=error):
            with self.assertRaises(frappe.db.OperationalError):
                resolve_incoming_routed_app(
                    whatsapp_account=self.account.name, contact_number="15551234567",
                )
            with self.assertRaises(frappe.db.OperationalError):
                set_last_sender_app(
                    whatsapp_account=self.account.name, to_number="15551234567",
                    source_app=self.sender_app.name,
                )

    def _assert_concurrent_initialization(self, *, profile_only=False, outgoing=False):
        profile = self._profile() if profile_only else None
        args: _RouteLookupArgs = {
            "whatsapp_account": self.account.name,
            "contact_number": "" if profile_only else "15551234567",
            "contact_profile": profile.name if profile else None,
        }
        site, sites_path = frappe.local.site, frappe.local.sites_path
        self.committed = True
        frappe.db.commit()
        started = Event()

        def competing_incoming():
            frappe.init(site=site, sites_path=sites_path)
            try:
                frappe.connect()
                frappe.flags.in_test = True
                frappe.db.multisql({
                    "mariadb": "SET SESSION innodb_lock_wait_timeout = 5",
                    "postgres": "SET lock_timeout = '5s'",
                })
                # Establish an older repeatable-read snapshot before waiting
                # for the uncommitted writer's account lock.
                self.assertIsNone(get_last_sender_app(**args))
                original_get_value = frappe.db.get_value

                def observe_account_lock(doctype, *values, **options):
                    if doctype == "WhatsApp Account" and options.get("for_update"):
                        started.set()
                    return original_get_value(doctype, *values, **options)

                with patch.object(frappe.db, "get_value", side_effect=observe_account_lock):
                    result = resolve_incoming_routed_app(**args)
                frappe.db.commit()
                return result
            finally:
                frappe.db.rollback()
                frappe.destroy()

        expected = self.sender_app.name if outgoing else self.default_app.name
        if outgoing:
            set_last_sender_app(
                whatsapp_account=self.account.name, to_number=args["contact_number"],
                contact_profile=args["contact_profile"], source_app=self.sender_app.name,
                message_name="outgoing-concurrent",
            )
        else:
            self.assertEqual(resolve_incoming_routed_app(**args), expected)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(competing_incoming)
            try:
                self.assertTrue(started.wait(timeout=10), "Competing connection did not reach the lock")
                with self.assertRaises(TimeoutError):
                    future.result(timeout=0.2)
            finally:
                # Release the writer even if an assertion fails, so the
                # executor can always join its worker.
                frappe.db.commit()
            self.assertEqual(future.result(timeout=10), expected)

        rows = frappe.get_all(ROUTE_DOCTYPE, filters={
            "whatsapp_account": self.account.name,
        }, fields=["last_source_app", "last_outgoing_message"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].last_source_app, expected)
        if outgoing:
            self.assertEqual(rows[0].last_outgoing_message, "outgoing-concurrent")
        else:
            self.assertFalse(rows[0].last_outgoing_message)

    def test_concurrent_phone_route_initialization(self):
        self._assert_concurrent_initialization()

    def test_concurrent_profile_only_route_initialization(self):
        self._assert_concurrent_initialization(profile_only=True)

    def test_incoming_initialization_preserves_concurrent_outgoing_sender(self):
        self._assert_concurrent_initialization(outgoing=True)
