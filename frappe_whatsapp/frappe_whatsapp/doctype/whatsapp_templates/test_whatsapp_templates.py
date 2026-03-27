# Copyright (c) 2022, Shridhar Patil and Contributors
# See license.txt

from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase
from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates.whatsapp_templates import (  # noqa: E501
    _COMPLIANCE_FIELDS,
    _derive_sync_compliance,
    _footer_looks_like_unsubscribe,
    _normalize_meta_language_code,
    _resolve_language_link,
    WhatsAppTemplates
)


_MOD = (
    "frappe_whatsapp.frappe_whatsapp.doctype"
    ".whatsapp_templates.whatsapp_templates"
)


class TestWhatsAppTemplates(FrappeTestCase):
    def test_normalize_meta_language_code_uses_underscores(self):
        self.assertEqual(_normalize_meta_language_code("en-US"), "en_US")
        self.assertEqual(_normalize_meta_language_code("es"), "es")

    def test_resolve_language_link_accepts_meta_separator_variants(self):
        with patch(
            "frappe_whatsapp.frappe_whatsapp.doctype."
            "whatsapp_templates.whatsapp_templates.frappe.db.exists"
        ) as mock_exists:
            mock_exists.side_effect = lambda doctype, name: name in {
                "en-US", "es"}

            self.assertEqual(_resolve_language_link("en_US"), "en-US")
            self.assertEqual(_resolve_language_link("es"), "es")
            self.assertEqual(_resolve_language_link("pt_BR"), "")


# ---------------------------------------------------------------------------
# Helpers shared by TestComplianceSyncDefaults
# ---------------------------------------------------------------------------

def _make_doc(**kwargs):
    """Return a minimal mock template document."""
    defaults = dict(
        category="MARKETING",
        footer="",
        actual_name="test_template",
        template_name="test_template",
        whatsapp_account="",
        compliance_auto_managed=0,
        requires_opt_in=0,
        include_unsubscribe_instructions=0,
        unsubscribe_text="",
        is_consent_request=0,
        required_consent_category=None,
    )
    defaults.update(kwargs)

    class _Doc:
        pass

    doc = _Doc()
    for k, v in defaults.items():
        setattr(doc, k, v)
    return cast(WhatsAppTemplates, doc)


def _make_settings(**kwargs):
    defaults = dict(
        default_unsubscribe_text="Reply STOP to unsubscribe",
        consent_request_template_prefixes="",
    )
    defaults.update(kwargs)

    class _Settings:
        pass

    s = _Settings()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


# ---------------------------------------------------------------------------
# Compliance sync default tests
# ---------------------------------------------------------------------------

class TestComplianceSyncDefaults(FrappeTestCase):
    """Tests for _derive_sync_compliance() and
    _footer_looks_like_unsubscribe()."""

    # ------------------------------------------------------------------
    # 1. New MARKETING template with recognizable unsubscribe footer
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_marketing_with_unsubscribe_footer(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_settings()
        doc = _make_doc(
            category="MARKETING",
            footer="Reply STOP to unsubscribe",
        )
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.requires_opt_in, 1)
        self.assertEqual(doc.include_unsubscribe_instructions, 1)
        self.assertEqual(doc.unsubscribe_text, "Reply STOP to unsubscribe")
        self.assertEqual(doc.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # 2. New MARKETING template with no footer
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_marketing_with_no_footer(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_settings()
        doc = _make_doc(category="MARKETING", footer="")
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.requires_opt_in, 1)
        self.assertEqual(doc.include_unsubscribe_instructions, 0)
        self.assertEqual(doc.unsubscribe_text, "")
        self.assertEqual(doc.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # 3. New UTILITY template → requires_opt_in = 0
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_utility_sets_no_opt_in(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_settings()
        doc = _make_doc(category="UTILITY", footer="")
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.requires_opt_in, 0)
        self.assertEqual(doc.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # 4. Template name matches consent-request prefix
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_consent_request_prefix_detected(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_settings(
            consent_request_template_prefixes="consent_,optin_")
        doc = _make_doc(
            actual_name="consent_welcome",
            template_name="consent_welcome",
            category="MARKETING",
        )
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.is_consent_request, 1)
        self.assertEqual(doc.requires_opt_in, 0)
        self.assertIsNone(doc.required_consent_category)
        self.assertEqual(doc.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # 5. Existing template with compliance_auto_managed=0 → always skip
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_compliance_settings")
    def test_existing_template_with_auto_managed_false_always_preserved(
        self, mock_settings
    ):
        """compliance_auto_managed=0 must always be skipped — regardless of
        whether compliance fields look default or not.  Pre-existing templates
        are never backfilled."""
        for doc in [
            # all fields at defaults (looks "untouched")
            _make_doc(category="MARKETING", compliance_auto_managed=0),
            # non-default field (clearly manually curated)
            _make_doc(category="MARKETING", compliance_auto_managed=0,
                      requires_opt_in=1),
        ]:
            with self.subTest(requires_opt_in=doc.requires_opt_in):
                original_opt_in = doc.requires_opt_in
                _derive_sync_compliance(doc, is_new=False)
                mock_settings.assert_not_called()
                self.assertEqual(doc.requires_opt_in, original_opt_in)
                self.assertEqual(doc.compliance_auto_managed, 0)
            mock_settings.reset_mock()

    # ------------------------------------------------------------------
    # 6. Existing auto-managed template refreshes when category changes
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_auto_managed_existing_refreshes_on_category_change(
        self, mock_settings, _mock_kw
    ):
        mock_settings.return_value = _make_settings()
        # Was MARKETING + auto-managed; Meta reclassified to UTILITY
        doc = _make_doc(
            category="UTILITY",
            compliance_auto_managed=1,
            requires_opt_in=1,  # stale value from previous MARKETING sync
        )
        _derive_sync_compliance(doc, is_new=False)

        self.assertEqual(doc.requires_opt_in, 0)
        self.assertEqual(doc.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # 7. Footer with configured opt-out keyword triggers detection
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords")
    @patch(f"{_MOD}.get_compliance_settings")
    def test_footer_detection_via_opt_out_keyword(
        self, mock_settings, mock_kw
    ):
        # No default_unsubscribe_text so only keyword path fires
        mock_settings.return_value = _make_settings(
            default_unsubscribe_text="")
        mock_kw.return_value = [
            {
                "keyword": "STOP",
                "case_sensitive": False,
                "match_type": "Contains",
            },
        ]
        doc = _make_doc(
            category="MARKETING",
            footer="Text STOP to opt out",
        )
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.include_unsubscribe_instructions, 1)
        self.assertEqual(doc.unsubscribe_text, "Text STOP to opt out")

    # ------------------------------------------------------------------
    # 8. Footer that does NOT match → unsubscribe fields stay empty
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords")
    @patch(f"{_MOD}.get_compliance_settings")
    def test_non_unsubscribe_footer_leaves_fields_empty(
        self, mock_settings, mock_kw
    ):
        mock_settings.return_value = _make_settings(
            default_unsubscribe_text="Reply STOP to unsubscribe")
        mock_kw.return_value = [
            {
                "keyword": "STOP",
                "case_sensitive": False,
                "match_type": "Contains",
            },
        ]
        doc = _make_doc(
            category="MARKETING",
            footer="Powered by Acme Corp",
        )
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.include_unsubscribe_instructions, 0)
        self.assertEqual(doc.unsubscribe_text, "")

    # ------------------------------------------------------------------
    # _footer_looks_like_unsubscribe unit tests
    # ------------------------------------------------------------------
    def test_footer_match_via_default_unsubscribe_text(self):
        settings = _make_settings(
            default_unsubscribe_text="Reply STOP to unsubscribe")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "Reply STOP to unsubscribe", settings)
        self.assertTrue(result)

    def test_footer_no_match_returns_false(self):
        settings = _make_settings(
            default_unsubscribe_text="Reply STOP to unsubscribe")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe("Hello world", settings)
        self.assertFalse(result)

    def test_blank_footer_never_matches(self):
        settings = _make_settings()
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe("", settings)
        self.assertFalse(result)

    # ------------------------------------------------------------------
    # Regex pass 3: naturally-worded opt-out footers
    # ------------------------------------------------------------------
    def test_footer_with_stop_matches_via_regex(self):
        """Footer containing 'STOP' (no exact configured text, no keywords)
        must match via the regex heuristic — the con_req_zoni-en scenario."""
        settings = _make_settings(default_unsubscribe_text="")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "You can opt out at any time by replying STOP.", settings)
        self.assertTrue(result)

    def test_footer_with_unsubscribe_word_matches_via_regex(self):
        settings = _make_settings(default_unsubscribe_text="")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "Click here to unsubscribe from these messages.", settings)
        self.assertTrue(result)

    def test_footer_with_opt_out_phrase_matches_via_regex(self):
        settings = _make_settings(default_unsubscribe_text="")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "To opt out reply with your number.", settings)
        self.assertTrue(result)

    def test_unrelated_footer_not_matched_by_regex(self):
        """A footer with no opt-out terms must still return False."""
        settings = _make_settings(default_unsubscribe_text="")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "Powered by Acme Corp", settings)
        self.assertFalse(result)

    def test_stop_in_non_opt_out_context_not_matched(self):
        """'Stop by our office for help' must NOT match — bare STOP without
        a preceding communication verb is not actionable opt-out language."""
        settings = _make_settings(default_unsubscribe_text="")
        with patch(f"{_MOD}.get_opt_out_keywords", return_value=[]):
            result = _footer_looks_like_unsubscribe(
                "Stop by our office for help.", settings)
        self.assertFalse(result)

    # ------------------------------------------------------------------
    # _derive_sync_compliance with naturally-worded footer (regex path)
    # ------------------------------------------------------------------
    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_derive_sync_detects_natural_footer_via_regex(
        self, mock_settings, _mock_kw
    ):
        """_derive_sync_compliance must set include_unsubscribe_instructions
        for footers that pass only the regex heuristic."""
        mock_settings.return_value = _make_settings(
            default_unsubscribe_text="Reply STOP to unsubscribe")
        doc = _make_doc(
            category="MARKETING",
            footer="You can opt out at any time by replying STOP.",
        )
        _derive_sync_compliance(doc, is_new=True)

        self.assertEqual(doc.include_unsubscribe_instructions, 1)
        self.assertEqual(
            doc.unsubscribe_text,
            "You can opt out at any time by replying STOP.")


# ===========================================================================
# Fix 1: Stale consent-request state cleared on re-sync
# ===========================================================================

class TestStaleConsentRequestCleared(FrappeTestCase):
    """is_consent_request must be cleared when the prefix no longer matches."""

    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_consent_request_flag_cleared_when_prefix_removed(
        self, mock_settings, _mock_kw
    ):
        """An auto-managed template that previously matched a consent prefix
        must have is_consent_request reset to 0 when the prefix is gone."""
        # Settings no longer contain any prefix
        mock_settings.return_value = _make_settings(
            consent_request_template_prefixes="")
        doc = _make_doc(
            actual_name="consent_welcome",
            template_name="consent_welcome",
            category="MARKETING",
            compliance_auto_managed=1,
            is_consent_request=1,   # stale from previous sync
            requires_opt_in=0,       # stale from previous sync
        )
        _derive_sync_compliance(doc, is_new=False)

        self.assertEqual(
            doc.is_consent_request, 0,
            "is_consent_request must be cleared when prefix is gone",
        )
        self.assertEqual(doc.requires_opt_in, 1,
                         "MARKETING without prefix match → requires_opt_in=1")
        self.assertEqual(doc.compliance_auto_managed, 1)

    @patch(f"{_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_MOD}.get_compliance_settings")
    def test_all_owned_fields_reset_before_rederivation(
        self, mock_settings, _mock_kw
    ):
        """Every owned compliance field must be reset before re-derivation
        so stale values never leak through."""
        mock_settings.return_value = _make_settings()
        # Doc has stale values across all owned fields
        doc = _make_doc(
            category="UTILITY",
            compliance_auto_managed=1,
            is_consent_request=1,
            requires_opt_in=1,
            include_unsubscribe_instructions=1,
            unsubscribe_text="old text",
            required_consent_category="OldCategory",
            footer="",  # no footer → unsubscribe fields must be cleared
        )
        _derive_sync_compliance(doc, is_new=False)

        self.assertEqual(doc.is_consent_request, 0)
        self.assertEqual(doc.requires_opt_in, 0)  # UTILITY
        self.assertEqual(doc.include_unsubscribe_instructions, 0)
        self.assertEqual(doc.unsubscribe_text, "")
        self.assertIsNone(doc.required_consent_category)


# ===========================================================================
# Fix 3: Account scoping and match_type semantics
# ===========================================================================

class TestFooterDetectionAccountScopingAndMatchType(FrappeTestCase):
    """_footer_looks_like_unsubscribe must scope keywords by account and
    honour match_type semantics identical to check_opt_out_keyword()."""

    # ------------------------------------------------------------------
    # Account scoping
    # ------------------------------------------------------------------

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_account_specific_keyword_not_used_for_different_account(
        self, mock_kw
    ):
        """Keyword configured for account B must not fire for account A.

        Uses a custom footer/keyword ("DEACTIVATE") that does not contain any
        of the regex-heuristic opt-out terms (stop/unsubscribe/opt-out), so
        that account-scoping is the *only* detection path that could fire.
        """
        # get_opt_out_keywords is called with account A; it returns no rows
        # (simulating that account A has no keywords, B's are filtered out)
        mock_kw.return_value = []
        settings = _make_settings(default_unsubscribe_text="")

        result = _footer_looks_like_unsubscribe(
            "Reply DEACTIVATE to cancel your messages",
            settings,
            whatsapp_account="account_a",
        )

        mock_kw.assert_called_once_with("account_a")
        self.assertFalse(
            result,
            "No matching keywords for account_a → must return False",
        )

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_account_scoped_keyword_fires_for_correct_account(
        self, mock_kw
    ):
        """Keyword returned for the correct account must still fire."""
        mock_kw.return_value = [
            {"keyword": "DEACTIVATE", "case_sensitive": False,
             "match_type": "Contains"},
        ]
        settings = _make_settings(default_unsubscribe_text="")

        result = _footer_looks_like_unsubscribe(
            "Reply DEACTIVATE to cancel your messages",
            settings,
            whatsapp_account="account_a",
        )

        mock_kw.assert_called_once_with("account_a")
        self.assertTrue(result)

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_no_account_passes_none_to_get_keywords(self, mock_kw):
        """When no account is given, get_opt_out_keywords must receive None."""
        mock_kw.return_value = []
        settings = _make_settings(default_unsubscribe_text="")
        _footer_looks_like_unsubscribe("some text", settings)
        mock_kw.assert_called_once_with(None)

    # ------------------------------------------------------------------
    # match_type: Exact
    # ------------------------------------------------------------------

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_exact_match_type_matches_full_footer(self, mock_kw):
        mock_kw.return_value = [
            {
                "keyword": "STOP",
                "case_sensitive": False,
                "match_type": "Exact",
            },
        ]
        settings = _make_settings(default_unsubscribe_text="")
        # Full footer == keyword (case-insensitive)
        self.assertTrue(
            _footer_looks_like_unsubscribe("stop", settings))

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_exact_match_type_does_not_match_substring(self, mock_kw):
        mock_kw.return_value = [
            {
                "keyword": "DEACTIVATE",
                "case_sensitive": False,
                "match_type": "Exact",
            },
        ]
        settings = _make_settings(default_unsubscribe_text="")
        # Keyword is a substring of the footer but not the whole footer, and
        # the footer has no regex-heuristic opt-out terms → must not match.
        self.assertFalse(
            _footer_looks_like_unsubscribe(
                "Reply DEACTIVATE to cancel", settings))

    # ------------------------------------------------------------------
    # match_type: Starts With
    # ------------------------------------------------------------------

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_starts_with_match_type_fires_on_prefix(self, mock_kw):
        mock_kw.return_value = [
            {"keyword": "STOP", "case_sensitive": False,
             "match_type": "Starts With"},
        ]
        settings = _make_settings(default_unsubscribe_text="")
        self.assertTrue(
            _footer_looks_like_unsubscribe("stop to unsubscribe", settings))

    @patch(f"{_MOD}.get_opt_out_keywords")
    def test_starts_with_does_not_match_non_prefix(self, mock_kw):
        mock_kw.return_value = [
            {"keyword": "DEACTIVATE", "case_sensitive": False,
             "match_type": "Starts With"},
        ]
        settings = _make_settings(default_unsubscribe_text="")
        # Footer does not start with keyword and has no regex opt-out terms
        self.assertFalse(
            _footer_looks_like_unsubscribe(
                "Reply DEACTIVATE to cancel", settings))

    # ------------------------------------------------------------------
    # Verify _derive_sync_compliance passes account to footer detection
    # ------------------------------------------------------------------

    @patch(f"{_MOD}.get_opt_out_keywords")
    @patch(f"{_MOD}.get_compliance_settings")
    def test_derive_sync_compliance_passes_account_to_footer_detection(
        self, mock_settings, mock_kw
    ):
        """_derive_sync_compliance must forward whatsapp_account so that
        footer detection is correctly scoped to the template's account."""
        mock_settings.return_value = _make_settings(
            default_unsubscribe_text="")
        mock_kw.return_value = []

        doc = _make_doc(
            category="MARKETING",
            footer="Reply STOP to unsubscribe",
            whatsapp_account="acme_account",
        )
        _derive_sync_compliance(doc, is_new=True)

        mock_kw.assert_called_once_with("acme_account")


# ===========================================================================
# Manual-override path: _detect_manual_compliance_change()
# ===========================================================================

# Default (unchanged) value for each compliance field.
_FIELD_DEFAULT: dict = {
    "requires_opt_in": 0,
    "include_unsubscribe_instructions": 0,
    "unsubscribe_text": "",
    "is_consent_request": 0,
    "required_consent_category": None,
}

# A clearly different value for each compliance field.
_FIELD_CHANGED: dict = {
    "requires_opt_in": 1,
    "include_unsubscribe_instructions": 1,
    "unsubscribe_text": "Reply STOP",
    "is_consent_request": 1,
    "required_consent_category": "Marketing",
}


def _ns(**kw):
    """Shorthand: build a SimpleNamespace from kwargs."""
    return SimpleNamespace(**kw)


def _base_compliance(compliance_auto_managed=1):
    """All compliance fields at default + given auto_managed flag."""
    d = dict(_FIELD_DEFAULT)
    d["compliance_auto_managed"] = compliance_auto_managed
    return d


class TestDetectManualComplianceChange(FrappeTestCase):
    """Unit tests for WhatsAppTemplates._detect_manual_compliance_change().

    The method is called on ``self`` (the in-memory doc being saved) with the
    ``before`` snapshot from the database.  We call it as an unbound method
    using SimpleNamespace objects so no Frappe DB is needed.
    """

    def _call(self, self_ns, before_ns):
        # Called as an unbound method with SimpleNamespace stand-ins;
        # the type: ignore silences the intentional type mismatch.
        _fn = WhatsAppTemplates._detect_manual_compliance_change
        _fn(self_ns, before_ns)  # type: ignore[arg-type]
        return self_ns

    # ------------------------------------------------------------------
    # Editing each compliance field must clear compliance_auto_managed
    # ------------------------------------------------------------------

    def test_each_compliance_field_change_clears_flag(self):
        """Changing any single compliance field on an auto-managed template
        must set compliance_auto_managed to 0."""
        for field in _COMPLIANCE_FIELDS:
            with self.subTest(field=field):
                before = _ns(**_base_compliance(compliance_auto_managed=1))

                self_state = dict(_base_compliance(compliance_auto_managed=1))
                self_state[field] = _FIELD_CHANGED[field]
                current = _ns(**self_state)

                self._call(current, before)
                self.assertEqual(
                    current.compliance_auto_managed, 0,
                    f"Changing '{field}' must clear compliance_auto_managed",
                )

    # ------------------------------------------------------------------
    # No compliance field changed → flag is preserved
    # ------------------------------------------------------------------

    def test_no_compliance_field_changed_preserves_flag(self):
        """When all compliance fields are identical to before, the flag must
        not be cleared — even if non-compliance fields like footer changed."""
        before = _ns(**_base_compliance(compliance_auto_managed=1))
        # Compliance fields unchanged; only a non-compliance field differs
        current = _ns(**_base_compliance(compliance_auto_managed=1))
        # footer and category are NOT in _COMPLIANCE_FIELDS
        current.footer = "new footer text"
        current.category = "UTILITY"

        self._call(current, before)
        self.assertEqual(current.compliance_auto_managed, 1)

    # ------------------------------------------------------------------
    # Guard conditions: method must be a no-op in these cases
    # ------------------------------------------------------------------

    def test_before_none_is_noop(self):
        """New document (before=None): method must not touch the flag."""
        current = _ns(**_base_compliance(compliance_auto_managed=1))
        # Change a compliance field too — still must not fire with before=None
        current.requires_opt_in = 1
        self._call(current, None)
        self.assertEqual(current.compliance_auto_managed, 1)

    def test_before_not_auto_managed_is_noop(self):
        """Previous record had compliance_auto_managed=0: method must not
        clear the flag even if compliance fields differ."""
        before = _ns(**_base_compliance(compliance_auto_managed=0))
        current = _ns(**_base_compliance(compliance_auto_managed=0))
        current.requires_opt_in = 1  # field changed

        self._call(current, before)
        self.assertEqual(current.compliance_auto_managed, 0)
