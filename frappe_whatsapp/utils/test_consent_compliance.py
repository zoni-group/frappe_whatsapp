"""Targeted tests for send-time consent-template compliance.

Covers:
- Marketing consent-request template with equivalent opt-out footer wording
  (the con_req_zoni-en scenario)
- send-time compliance validation (enforce_marketing_template_compliance)
- Consent-request bypass before opt-in (verify_consent_for_send)
- YES quick reply → opt-in via _handle_consent_keywords
- STOP → opt-out via _handle_consent_keywords
- NO quick reply → documented no-op behavior
"""

from types import SimpleNamespace
from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase
from frappe_whatsapp.utils.consent import enforce_marketing_template_compliance

_CONSENT_MOD = "frappe_whatsapp.utils.consent"
_WEBHOOK_MOD = "frappe_whatsapp.utils.webhook"
_TEMPLATES_MOD = (
    "frappe_whatsapp.frappe_whatsapp.doctype"
    ".whatsapp_templates.whatsapp_templates"
)


def _make_template(**kwargs):
    defaults = dict(
        category="MARKETING",
        footer="",
        unsubscribe_text="",
        include_unsubscribe_instructions=0,
        is_consent_request=0,
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_compliance_settings(**kwargs):
    defaults = dict(
        include_unsubscribe_in_marketing=1,
        default_unsubscribe_text="Reply STOP to unsubscribe",
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


# ===========================================================================
# send-time marketing compliance validation
# ===========================================================================

class TestEnforceMarketingTemplateCompliance(FrappeTestCase):
    """enforce_marketing_template_compliance send-time checks."""

    # -------------------------------------------------------------------
    # Happy path: exact configured text in footer
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_exact_unsubscribe_text_in_footer_passes(
        self, mock_settings, _mock_kw
    ):
        mock_settings.return_value = _make_compliance_settings()
        t = _make_template(footer="Reply STOP to unsubscribe")
        enforce_marketing_template_compliance(t)  # must not raise

    # -------------------------------------------------------------------
    # Happy path: equivalent opt-out wording — the con_req_zoni-en scenario
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_equivalent_opt_out_footer_wording_passes(
        self, mock_settings, _mock_kw
    ):
        """'You can opt out at any time by replying STOP.' must pass even
        when it does not exactly match 'Reply STOP to unsubscribe', and even
        when include_unsubscribe_instructions=0 and unsubscribe_text=''."""
        mock_settings.return_value = _make_compliance_settings()
        t = _make_template(
            category="MARKETING",
            is_consent_request=1,
            footer="You can opt out at any time by replying STOP.",
            include_unsubscribe_instructions=0,
            unsubscribe_text="",
        )
        enforce_marketing_template_compliance(t)  # must not raise

    # -------------------------------------------------------------------
    # Failure: STOP in a non-opt-out context (false-positive guard)
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_stop_in_non_opt_out_context_raises(
        self, mock_settings, _mock_kw
    ):
        """'Stop by our office for help' must NOT pass.
        Bare STOP without a preceding send verb is not actionable opt-out
        wording."""
        mock_settings.return_value = _make_compliance_settings(
            default_unsubscribe_text="")
        t = _make_template(footer="Stop by our office for help")
        with self.assertRaises(Exception):
            enforce_marketing_template_compliance(t)

    # -------------------------------------------------------------------
    # Failure: include_unsubscribe_instructions=1 with generic footer
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_flag_set_but_no_actionable_footer_raises(
        self, mock_settings, _mock_kw
    ):
        """Setting include_unsubscribe_instructions=1 must NOT bypass
        send-time validation.  The footer content is always re-verified."""
        mock_settings.return_value = _make_compliance_settings(
            default_unsubscribe_text="")
        t = _make_template(
            footer="Some footer text",
            include_unsubscribe_instructions=1,
        )
        with self.assertRaises(Exception):
            enforce_marketing_template_compliance(t)

    # -------------------------------------------------------------------
    # Failure: no footer at all
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_no_footer_raises(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_compliance_settings()
        t = _make_template(footer="")
        with self.assertRaises(Exception):
            enforce_marketing_template_compliance(t)

    # -------------------------------------------------------------------
    # Failure: footer with no opt-out language
    # -------------------------------------------------------------------
    @patch(f"{_TEMPLATES_MOD}.get_opt_out_keywords", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_unrelated_footer_raises(self, mock_settings, _mock_kw):
        mock_settings.return_value = _make_compliance_settings()
        t = _make_template(footer="Powered by Acme Corp")
        with self.assertRaises(Exception):
            enforce_marketing_template_compliance(t)

    # -------------------------------------------------------------------
    # Enforcement disabled → always passes
    # -------------------------------------------------------------------
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_enforcement_disabled_passes_any_footer(self, mock_settings):
        mock_settings.return_value = _make_compliance_settings(
            include_unsubscribe_in_marketing=0)
        t = _make_template(footer="")  # would fail if enabled
        enforce_marketing_template_compliance(t)  # must not raise

    # -------------------------------------------------------------------
    # Non-MARKETING category always passes
    # -------------------------------------------------------------------
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_utility_category_skipped(self, mock_settings):
        mock_settings.return_value = _make_compliance_settings()
        t = _make_template(category="UTILITY", footer="")
        enforce_marketing_template_compliance(t)  # must not raise


# ===========================================================================
# Consent-request bypass before opt-in (verify_consent_for_send)
# ===========================================================================

class TestConsentRequestBypassBeforeOptIn(FrappeTestCase):
    """Consent-request templates bypass unknown-consent checks; DNC/opted-out
    still block."""

    @patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890")
    @patch(f"{_CONSENT_MOD}.frappe.db.get_all", return_value=[])
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_no_profile_consent_request_is_bypassed(
        self, mock_settings, _mock_db, _mock_fmt
    ):
        """No profile + is_consent_request=True → Bypassed, allowed=True."""
        from frappe_whatsapp.utils.consent import verify_consent_for_send
        mock_settings.return_value = SimpleNamespace(
            consent_check_mode="Strict",
            enforce_consent_check=True,
        )
        result = verify_consent_for_send(
            "+1234567890", is_consent_request=True)

        self.assertTrue(result.allowed)
        self.assertEqual(result.status, "Bypassed")
        self.assertIn("Consent request", result.reason)

    @patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890")
    @patch(f"{_CONSENT_MOD}.frappe.db.get_all",
           return_value=[SimpleNamespace(name="p1")])
    @patch(f"{_CONSENT_MOD}.frappe.get_doc")
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_opted_out_blocks_even_consent_request(
        self, mock_settings, mock_get_doc, _mock_db, _mock_fmt
    ):
        """Opted-out contacts must be blocked even for consent-request
        templates."""
        from frappe_whatsapp.utils.consent import verify_consent_for_send
        mock_settings.return_value = SimpleNamespace(
            consent_check_mode="Strict",
            enforce_consent_check=True,
        )
        mock_get_doc.return_value = SimpleNamespace(
            name="p1",
            do_not_contact=False,
            is_opted_out=True,
            is_opted_in=False,
        )
        result = verify_consent_for_send(
            "+1234567890", is_consent_request=True)

        self.assertFalse(result.allowed)
        self.assertEqual(result.status, "Opted Out")

    @patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890")
    @patch(f"{_CONSENT_MOD}.frappe.db.get_all",
           return_value=[SimpleNamespace(name="p2")])
    @patch(f"{_CONSENT_MOD}.frappe.get_doc")
    @patch(f"{_CONSENT_MOD}.get_compliance_settings")
    def test_dnc_blocks_even_consent_request(
        self, mock_settings, mock_get_doc, _mock_db, _mock_fmt
    ):
        """DNC contacts must be blocked even for consent-request templates."""
        from frappe_whatsapp.utils.consent import verify_consent_for_send
        mock_settings.return_value = SimpleNamespace(
            consent_check_mode="Strict",
            enforce_consent_check=True,
        )
        mock_get_doc.return_value = SimpleNamespace(
            name="p2",
            do_not_contact=True,
            is_opted_out=False,
            is_opted_in=False,
        )
        result = verify_consent_for_send(
            "+1234567890", is_consent_request=True)

        self.assertFalse(result.allowed)
        self.assertEqual(result.status, "Opted Out")


# ===========================================================================
# Webhook consent keyword processing
# ===========================================================================

class TestConsentKeywordHandling(FrappeTestCase):
    """_handle_consent_keywords processes YES / STOP / NO correctly."""

    @patch(f"{_WEBHOOK_MOD}.send_opt_in_confirmation")
    @patch(f"{_WEBHOOK_MOD}.process_opt_in")
    @patch(f"{_WEBHOOK_MOD}.check_opt_out_keyword", return_value=None)
    @patch(f"{_WEBHOOK_MOD}.check_opt_in_keyword", return_value=True)
    def test_yes_reply_triggers_opt_in(
        self, _ck_in, _ck_out, mock_opt_in, mock_confirm
    ):
        """YES / START / SUBSCRIBE must call process_opt_in and send
        opt-in confirmation."""
        from frappe_whatsapp.utils.webhook import _handle_consent_keywords
        _handle_consent_keywords(
            body_text="YES",
            contact_number="+1234567890",
            whatsapp_account_name="TestAccount",
            message_doc_name="MSG-0001",
            profile_name=None,
        )
        mock_opt_in.assert_called_once()
        call_kwargs = mock_opt_in.call_args.kwargs
        self.assertEqual(call_kwargs["contact_number"], "+1234567890")
        self.assertEqual(call_kwargs["whatsapp_account"], "TestAccount")
        mock_confirm.assert_called_once()

    @patch(f"{_WEBHOOK_MOD}.send_opt_out_confirmation")
    @patch(f"{_WEBHOOK_MOD}.process_opt_out")
    @patch(f"{_WEBHOOK_MOD}.check_opt_out_keyword")
    def test_stop_reply_triggers_opt_out(
        self, mock_ck_out, mock_opt_out, mock_confirm
    ):
        """STOP must call process_opt_out and send opt-out confirmation."""
        mock_ck_out.return_value = {
            "keyword": "STOP",
            "action": "Full Opt-Out",
            "target_category": None,
        }
        from frappe_whatsapp.utils.webhook import _handle_consent_keywords
        _handle_consent_keywords(
            body_text="STOP",
            contact_number="+1234567890",
            whatsapp_account_name="TestAccount",
            message_doc_name="MSG-0002",
            profile_name=None,
        )
        mock_opt_out.assert_called_once()
        call_kwargs = mock_opt_out.call_args.kwargs
        self.assertEqual(call_kwargs["contact_number"], "+1234567890")
        mock_confirm.assert_called_once()

    @patch(f"{_WEBHOOK_MOD}.process_opt_in")
    @patch(f"{_WEBHOOK_MOD}.process_opt_out")
    @patch(f"{_WEBHOOK_MOD}.check_opt_in_keyword", return_value=False)
    @patch(f"{_WEBHOOK_MOD}.check_opt_out_keyword", return_value=None)
    def test_no_reply_is_a_no_op(
        self, _ck_out, _ck_in, mock_opt_out, mock_opt_in
    ):
        """'NO' quick reply to a consent-request template must not change
        consent state.

        Declining a consent invitation is not the same as opting out.  The
        contact's status stays Unknown and no consent audit log is created.
        (Add explicit NO handling in _handle_consent_keywords if a 'declined'
        message or status update is needed in future.)
        """
        from frappe_whatsapp.utils.webhook import _handle_consent_keywords
        _handle_consent_keywords(
            body_text="NO",
            contact_number="+1234567890",
            whatsapp_account_name="TestAccount",
            message_doc_name="MSG-0003",
            profile_name=None,
        )
        mock_opt_out.assert_not_called()
        mock_opt_in.assert_not_called()
