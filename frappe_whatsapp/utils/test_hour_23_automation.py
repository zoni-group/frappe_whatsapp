"""Tests for the hour-23 follow-up automation.

Covers:
- exact language match
- English fallback when language is missing or unmapped
- candidate selection in the final service-window hour using window_hours
- atomic idempotency: claim-before-send, concurrent-worker overlap
- unknown consent → consent template is selected
- consent present → status follow-up template is selected
- opted-out / DNC contacts are skipped
- missing mapping / missing English row is logged and skipped
- category opt-in on YES reply when marketing_consent_category is configured
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from frappe.tests.utils import FrappeTestCase

_MOD = "frappe_whatsapp.utils.hour_23_automation"
_WEBHOOK_MOD = "frappe_whatsapp.utils.webhook"
_CONSENT_MOD = "frappe_whatsapp.utils.consent"


# ── Shared helpers ──────────────────────────────────────────────────────────

def _make_row(
    lang_code,
    consent_tmpl="CONSENT-TMPL",
    followup_tmpl="FOLLOWUP-TMPL",
):
    return SimpleNamespace(
        language_code=lang_code,
        consent_template=consent_tmpl,
        status_follow_up_template=followup_tmpl,
    )


def _make_settings(**kwargs):
    defaults = dict(
        enable_hour_23_follow_up=1,
        window_hours=24,
        marketing_consent_category=None,
        hour_23_language_map=[
            _make_row("en"),
            _make_row("es", "CONSENT-ES", "FOLLOWUP-ES"),
        ],
    )
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _make_profile(
    name="WP-001",
    do_not_contact=False,
    is_opted_out=False,
    is_opted_in=False,
    detected_language="en",
):
    return SimpleNamespace(
        name=name,
        do_not_contact=do_not_contact,
        is_opted_out=is_opted_out,
        is_opted_in=is_opted_in,
        detected_language=detected_language,
    )


def _make_template(
    name="CONSENT-TMPL",
    status="APPROVED",
    is_consent_request=1,
    sample_values=None,
    field_names=None,
    header_type=None,
):
    return SimpleNamespace(
        name=name,
        status=status,
        is_consent_request=is_consent_request,
        sample_values=sample_values,
        field_names=field_names,
        header_type=header_type,
    )


def _make_candidate(
    contact_number="+1234567890",
    whatsapp_account="WA-001",
    anchor_message="WM-IN-001",
):
    return {
        "contact_number": contact_number,
        "whatsapp_account": whatsapp_account,
        "anchor_message": anchor_message,
        "anchor_time": "2026-04-01 00:00:00",
    }


# ── _build_language_map ─────────────────────────────────────────────────────

class TestBuildLanguageMap(FrappeTestCase):
    """_build_language_map normalises codes and ignores blank rows."""

    def _build(self, rows):
        from frappe_whatsapp.utils.hour_23_automation import (
            _build_language_map,
        )
        settings = SimpleNamespace(hour_23_language_map=rows)
        return _build_language_map(settings)

    def test_basic_map(self):
        rows = [_make_row("en"), _make_row("es")]
        result = self._build(rows)
        self.assertIn("en", result)
        self.assertIn("es", result)
        row = result["en"]
        assert row is not None
        self.assertEqual(row.consent_template, "CONSENT-TMPL")

    def test_normalises_to_lower(self):
        rows = [_make_row("EN"), _make_row("ES")]
        result = self._build(rows)
        self.assertIn("en", result)
        self.assertIn("es", result)

    def test_skips_blank_language_code(self):
        rows = [_make_row(""), _make_row("en")]
        result = self._build(rows)
        self.assertEqual(list(result.keys()), ["en"])

    def test_empty_table(self):
        self.assertEqual(self._build([]), {})


# ── _resolve_template_row ───────────────────────────────────────────────────

class TestResolveTemplateRow(FrappeTestCase):
    """_resolve_template_row returns the right row with English fallback."""

    def _resolve(self, lang_map, detected_language):
        from frappe_whatsapp.utils.hour_23_automation import (
            _resolve_template_row,
        )
        return _resolve_template_row(lang_map, detected_language)

    def test_exact_match_es(self):
        lang_map = {
            "en": _make_row("en"),
            "es": _make_row("es", "C-ES", "F-ES"),
        }
        row = self._resolve(lang_map, "es")
        assert row is not None
        self.assertEqual(row.consent_template, "C-ES")

    def test_exact_match_en(self):
        lang_map = {"en": _make_row("en", "C-EN", "F-EN")}
        row = self._resolve(lang_map, "en")
        assert row is not None
        self.assertEqual(row.consent_template, "C-EN")

    def test_fallback_when_language_missing(self):
        """Unmapped language code falls back to English row."""
        lang_map = {"en": _make_row("en", "C-EN", "F-EN")}
        row = self._resolve(lang_map, "pt")
        assert row is not None
        self.assertEqual(row.consent_template, "C-EN")

    def test_fallback_when_detected_language_blank(self):
        """No detected language falls back to English row."""
        lang_map = {"en": _make_row("en", "C-EN", "F-EN")}
        row = self._resolve(lang_map, "")
        assert row is not None
        self.assertEqual(row.consent_template, "C-EN")

    def test_fallback_when_detected_language_none(self):
        lang_map = {"en": _make_row("en", "C-EN", "F-EN")}
        row = self._resolve(lang_map, None)
        assert row is not None
        self.assertEqual(row.consent_template, "C-EN")

    def test_returns_none_when_no_en_fallback(self):
        """No match AND no 'en' row → None."""
        lang_map = {"es": _make_row("es")}
        row = self._resolve(lang_map, "pt")
        self.assertIsNone(row)


# ── _get_candidates ─────────────────────────────────────────────────────────

class TestGetCandidates(FrappeTestCase):
    """_get_candidates selects only contacts in the final window hour.

    Target range:
    ``window_hours - 1 <= hours_since_last_incoming < window_hours``

    In timestamp terms (hours_since = now - creation):
      - at exactly 23h: creation == now - 23h == upper_bound → included (<=)
      - at exactly 24h: creation == now - 24h == lower_bound → excluded (>)
    """

    def test_sql_called_with_correct_bounds(self):
        """window_hours=24 → lower_bound=now-24h (exclusive),
        upper_bound=now-23h (inclusive).
        """
        from frappe.utils import add_to_date
        from frappe_whatsapp.utils.hour_23_automation import _get_candidates

        fake_now = "2026-04-01 12:00:00"
        expected_lower = add_to_date(fake_now, hours=-24)
        expected_upper = add_to_date(fake_now, hours=-23)
        mock_results = [_make_candidate()]

        with (
            patch(f"{_MOD}.now_datetime", return_value=fake_now),
            patch(
                f"{_MOD}.frappe.db.sql",
                return_value=mock_results,
            ) as mock_sql,
        ):
            result = _get_candidates(24)

        mock_sql.assert_called_once()
        params = mock_sql.call_args[0][1]
        self.assertEqual(params["lower_bound"], expected_lower)
        self.assertEqual(params["upper_bound"], expected_upper)
        self.assertEqual(result, mock_results)

    def test_custom_window_hours(self):
        """window_hours=48 → lower_bound=now-48h (exclusive),
        upper_bound=now-47h (inclusive).
        """
        from frappe.utils import add_to_date
        from frappe_whatsapp.utils.hour_23_automation import _get_candidates

        fake_now = "2026-04-01 12:00:00"
        expected_lower = add_to_date(fake_now, hours=-48)
        expected_upper = add_to_date(fake_now, hours=-47)

        with (
            patch(f"{_MOD}.now_datetime", return_value=fake_now),
            patch(f"{_MOD}.frappe.db.sql", return_value=[]) as mock_sql,
        ):
            _get_candidates(48)

        params = mock_sql.call_args[0][1]
        self.assertEqual(params["lower_bound"], expected_lower)
        self.assertEqual(params["upper_bound"], expected_upper)

    def test_sql_uses_exclusive_lower_and_inclusive_upper(self):
        """Verify SQL operators match the intended boundary semantics.

        ``creation > lower_bound`` → 24h-old messages are excluded.
        ``creation <= upper_bound`` → exactly-23h-old messages are included.
        """
        from frappe_whatsapp.utils.hour_23_automation import _get_candidates

        with (
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.frappe.db.sql", return_value=[]) as mock_sql,
        ):
            _get_candidates(24)

        sql_text = mock_sql.call_args[0][0]
        # Allow any whitespace between operator and placeholder
        self.assertRegex(sql_text, r">\s+%\(lower_bound\)s")
        self.assertRegex(sql_text, r"<=\s+%\(upper_bound\)s")


# ── _process_candidate — template selection ─────────────────────────────────

class TestProcessCandidateTemplateSelection(FrappeTestCase):
    """_process_candidate selects the right template based on consent state."""

    def _run_process(
        self,
        profile=None,
        template=None,
        already_logged=False,
        marketing_consent_category=None,
        lang_map=None,
    ):
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        if profile is None:
            profile = _make_profile()
        if template is None:
            template = _make_template()
        if lang_map is None:
            lang_map = {"en": _make_row("en")}

        mock_msg_doc = SimpleNamespace(name="WM-OUT-001")

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template,
                mock_msg_doc]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=marketing_consent_category,
            )

        return mock_msg_doc

    def test_no_consent_uses_consent_template(self):
        """Contact without marketing consent → consent_template is used."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        template = _make_template(name="CONSENT-TMPL", is_consent_request=1)
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}

        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-001"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template, mock_msg_doc,
            ]) as mock_get_doc,
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_msg_doc.insert.assert_called_once()
        msg_doc_call_args = mock_get_doc.call_args_list[2][0][0]
        self.assertEqual(msg_doc_call_args["template"], "CONSENT-TMPL")
        self.assertEqual(msg_doc_call_args["content_type"], "text")

    def test_with_master_consent_uses_followup_template(self):
        """is_opted_in=True (no category) → status_follow_up_template."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=True)
        followup_tmpl = _make_template(
            name="FOLLOWUP-TMPL", is_consent_request=0)
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-002"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, followup_tmpl, mock_msg_doc,
            ]) as mock_get_doc,
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_msg_doc.insert.assert_called_once()
        msg_doc_call_args = mock_get_doc.call_args_list[2][0][0]
        self.assertEqual(msg_doc_call_args["template"], "FOLLOWUP-TMPL")

    def test_category_consent_present_uses_followup(self):
        """Category consent=1 → status_follow_up_template."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        followup_tmpl = _make_template(
            name="FOLLOWUP-TMPL", is_consent_request=0)
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-003"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, followup_tmpl, mock_msg_doc,
            ]) as mock_get_doc,
            # Category consent row exists and is consented (call 1);
            # _reconcile_if_already_sent: name lookup returns None (call 2);
            # _post_claim_checks Step-B: automation_type=None (call 3),
            # template=None (call 4) → stored not in DB, check skipped.
            patch(
                f"{_MOD}.frappe.db.get_value",
                side_effect=[1, None, None, None],
            ),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category="MARKETING",
            )

        mock_msg_doc.insert.assert_called_once()
        msg_doc_call_args = mock_get_doc.call_args_list[2][0][0]
        self.assertEqual(msg_doc_call_args["template"], "FOLLOWUP-TMPL")

    def test_category_consent_absent_uses_consent_template(self):
        """Category consent row missing → consent_template."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        consent_tmpl = _make_template(
            name="CONSENT-TMPL", is_consent_request=1)
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-004"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, consent_tmpl, mock_msg_doc,
            ]) as mock_get_doc,
            # Category consent row: None (not found)
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category="MARKETING",
            )

        mock_msg_doc.insert.assert_called_once()
        msg_doc_call_args = mock_get_doc.call_args_list[2][0][0]
        self.assertEqual(msg_doc_call_args["template"], "CONSENT-TMPL")


# ── _process_candidate — skip conditions ────────────────────────────────────

class TestProcessCandidateSkipConditions(FrappeTestCase):
    """_process_candidate skips opted-out, DNC, already-logged contacts."""

    def _run_with_profile(self, profile, lang_map=None):
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate
        if lang_map is None:
            lang_map = {"en": _make_row("en")}
        mock_insert = MagicMock()
        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )
        return mock_insert

    def test_dnc_contact_is_skipped(self):
        profile = _make_profile(do_not_contact=True)
        mock_claim = MagicMock()
        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}._claim_anchor", mock_claim),
        ):
            from frappe_whatsapp.utils.hour_23_automation import (
                _process_candidate,
            )
            _process_candidate(
                candidate=_make_candidate(),
                lang_map={"en": _make_row("en")},
                marketing_consent_category=None,
            )
        # Skipped before reaching the claim step
        mock_claim.assert_not_called()

    def test_opted_out_contact_is_skipped(self):
        profile = _make_profile(is_opted_out=True)
        mock_claim = MagicMock()
        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}._claim_anchor", mock_claim),
        ):
            from frappe_whatsapp.utils.hour_23_automation import (
                _process_candidate,
            )
            _process_candidate(
                candidate=_make_candidate(),
                lang_map={"en": _make_row("en")},
                marketing_consent_category=None,
            )
        mock_claim.assert_not_called()

    def test_claim_taken_is_skipped(self):
        """Idempotency: _claim_anchor returns None → no send."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile()
        template = _make_template()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value=None),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template, mock_msg_doc,
            ]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map={"en": _make_row("en")},
                marketing_consent_category=None,
            )
        # Claim failed → outgoing message must not be created
        mock_msg_doc.insert.assert_not_called()


# ── Idempotency / _claim_anchor ─────────────────────────────────────────────

class TestClaimAnchor(FrappeTestCase):
    """_claim_anchor: insert-first, commit, and conflict detection."""

    def test_returns_log_name_on_success(self):
        """First call inserts the log row and returns its name."""
        from frappe_whatsapp.utils.hour_23_automation import _claim_anchor

        mock_log_doc = MagicMock()
        mock_log_doc.name = "LOG-001"

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_doc),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            result = _claim_anchor(
                anchor_message="WM-IN-001",
                whatsapp_account="WA-001",
                contact_number="+1234567890",
                automation_type="consent_request",
                template_name="CONSENT-TMPL",
            )

        self.assertEqual(result, "LOG-001")
        mock_log_doc.insert.assert_called_once()

    def test_duplicate_with_live_lease_returns_none(self):
        """UniqueValidationError + active lease (reclaimed=0) → None, no send.
        """
        import frappe as _frappe
        from frappe_whatsapp.utils.hour_23_automation import _claim_anchor

        mock_log_doc = MagicMock()
        mock_log_doc.insert.side_effect = (
            _frappe.exceptions.UniqueValidationError
        )

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_doc),
            patch(f"{_MOD}.frappe.db.rollback") as mock_rollback,
            # UPDATE-WHERE finds no stale row (live claim held by worker 1)
            patch(f"{_MOD}.frappe.db.sql"),
            patch(f"{_MOD}._get_sql_row_count", return_value=0),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            result = _claim_anchor(
                anchor_message="WM-IN-001",
                whatsapp_account="WA-001",
                contact_number="+1234567890",
                automation_type="consent_request",
                template_name="CONSENT-TMPL",
            )

        self.assertIsNone(result)
        mock_rollback.assert_called_once()

    def test_stale_lease_is_reclaimed(self):
        """UniqueValidationError + expired lease → stale re-claim succeeds."""
        import frappe as _frappe
        from frappe_whatsapp.utils.hour_23_automation import _claim_anchor

        mock_log_doc = MagicMock()
        mock_log_doc.insert.side_effect = (
            _frappe.exceptions.UniqueValidationError
        )

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_doc),
            patch(f"{_MOD}.frappe.db.rollback"),
            # UPDATE-WHERE reclaims 1 stale row
            patch(f"{_MOD}.frappe.db.sql"),
            patch(f"{_MOD}._get_sql_row_count", return_value=1),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.frappe.db.get_value", return_value="LOG-STALE"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            result = _claim_anchor(
                anchor_message="WM-IN-001",
                whatsapp_account="WA-001",
                contact_number="+1234567890",
                automation_type="consent_request",
                template_name="CONSENT-TMPL",
            )

        self.assertEqual(result, "LOG-STALE")

    def test_concurrent_worker_loses_claim(self):
        """Overlapping scheduler runs: second worker gets None (live lease)."""
        import frappe as _frappe
        from frappe_whatsapp.utils.hour_23_automation import _claim_anchor

        # Worker 1: fresh INSERT succeeds
        mock_log_w1 = MagicMock()
        mock_log_w1.name = "LOG-W1"

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_w1),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            result_w1 = _claim_anchor(
                anchor_message="WM-IN-001",
                whatsapp_account="WA-001",
                contact_number="+1234567890",
                automation_type="consent_request",
                template_name="CONSENT-TMPL",
            )

        self.assertEqual(result_w1, "LOG-W1")

        # Worker 2: UniqueValidationError → stale check finds live claim → None
        mock_log_w2 = MagicMock()
        mock_log_w2.insert.side_effect = (
            _frappe.exceptions.UniqueValidationError
        )

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_w2),
            patch(f"{_MOD}.frappe.db.rollback"),
            patch(f"{_MOD}.frappe.db.sql"),
            patch(f"{_MOD}._get_sql_row_count", return_value=0),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            result_w2 = _claim_anchor(
                anchor_message="WM-IN-001",
                whatsapp_account="WA-001",
                contact_number="+1234567890",
                automation_type="consent_request",
                template_name="CONSENT-TMPL",
            )

        self.assertIsNone(result_w2)

    def test_unexpected_exception_is_logged_and_raised(self):
        """A non-UniqueValidationError in _claim_anchor is logged and raised.

        This distinguishes a real failure (e.g. DB connection error) from a
        benign lost race, so the outer error handler records it rather than
        silently skipping the contact.
        """
        from frappe_whatsapp.utils.hour_23_automation import _claim_anchor

        mock_log_doc = MagicMock()
        mock_log_doc.insert.side_effect = RuntimeError("db connection lost")

        with (
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_log_doc),
            patch(f"{_MOD}.frappe.db.rollback"),
            patch(f"{_MOD}.frappe.log_error") as mock_log_error,
            patch(f"{_MOD}.frappe.get_traceback", return_value="tb"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            with self.assertRaises(RuntimeError):
                _claim_anchor(
                    anchor_message="WM-IN-001",
                    whatsapp_account="WA-001",
                    contact_number="+1234567890",
                    automation_type="consent_request",
                    template_name="CONSENT-TMPL",
                )

        mock_log_error.assert_called_once()

    def test_second_scheduler_run_is_noop(self):
        """Second run must not create a new outgoing message."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile()
        template = _make_template()
        lang_map = {"en": _make_row("en")}

        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-001"

        # First run: claim succeeds
        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template, mock_msg_doc,
            ]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_msg_doc.insert.assert_called_once()

        # Second run: claim returns None → no send
        mock_msg_doc2 = MagicMock()
        with (
            patch(f"{_MOD}._claim_anchor", return_value=None),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                _make_profile(), _make_template(), mock_msg_doc2,
            ]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_msg_doc2.insert.assert_not_called()


# ── Crash recovery ──────────────────────────────────────────────────────────

class TestCrashRecovery(FrappeTestCase):
    """Send failure after a successful claim leaves the row Pending
    (recoverable), and the recovery scheduler re-sends on the next run.
    """

    def test_send_failure_after_claim_leaves_row_pending(self):
        """msg_doc.insert() failure after a successful claim must propagate the
        exception and must NOT mark the row Sent. The row stays Pending so that
        recover_stale_hour_23_claims() can pick it up after the lease expires.
        """
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile()
        template = _make_template()
        lang_map = {"en": _make_row("en")}

        mock_msg_doc = MagicMock()
        mock_msg_doc.insert.side_effect = RuntimeError("send failed")
        mock_set_value = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template, mock_msg_doc,
            ]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            with self.assertRaises(RuntimeError):
                _process_candidate(
                    candidate=_make_candidate(),
                    lang_map=lang_map,
                    marketing_consent_category=None,
                )

        # set_value was never called → row stays Pending, not marked Sent
        mock_set_value.assert_not_called()

    def test_stale_claim_recovery_resends_message(self):
        """recover_stale_hour_23_claims() picks up an expired Pending row,
        re-claims it, re-checks eligibility, and successfully re-sends.
        """
        from frappe_whatsapp.utils.hour_23_automation import (
            recover_stale_hour_23_claims,
        )

        settings = _make_settings()
        stale_row = {
            "name": "LOG-STALE",
            "anchor_message": "WM-IN-001",
            "whatsapp_account": "WA-001",
            "contact_number": "+1234567890",
            "automation_type": "consent_request",
            "template": "CONSENT-TMPL",
            "claim_expires_at": "2026-04-01 10:00:00",  # expired
        }
        template = _make_template(name="CONSENT-TMPL")
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-RETRY"
        mock_set_value = MagicMock()

        with (
            patch(f"{_MOD}.frappe.get_cached_doc", return_value=settings),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.frappe.db.get_all", return_value=[stale_row]),
            patch(f"{_MOD}._claim_anchor", return_value="LOG-STALE"),
            # No prior outbound message (normal recovery path)
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            # Eligible: not opted-out, no consent (consent_request appropriate)
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            # get_doc: template validation, then msg_doc
            patch(
                f"{_MOD}.frappe.get_doc",
                side_effect=[template, mock_msg_doc],
            ),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit"),
        ):
            recover_stale_hour_23_claims()

        mock_msg_doc.insert.assert_called_once()
        set_value_kwargs = mock_set_value.call_args[0]
        self.assertEqual(set_value_kwargs[1], "LOG-STALE")
        sent_fields = set_value_kwargs[2]
        self.assertEqual(sent_fields["send_status"], "Sent")
        self.assertIsNone(sent_fields["claim_expires_at"])

    def test_stale_claim_with_live_lease_is_skipped(self):
        """A Pending row whose claim_expires_at is in the future is skipped
        — another worker is still in-flight.
        """
        from frappe_whatsapp.utils.hour_23_automation import (
            recover_stale_hour_23_claims,
        )

        settings = _make_settings()
        live_row = {
            "name": "LOG-LIVE",
            "anchor_message": "WM-IN-002",
            "whatsapp_account": "WA-001",
            "contact_number": "+9999999999",
            "automation_type": "consent_request",
            "template": "CONSENT-TMPL",
            "claim_expires_at": "2026-04-01 14:00:00",  # future
        }
        mock_claim = MagicMock()

        with (
            patch(f"{_MOD}.frappe.get_cached_doc", return_value=settings),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.frappe.db.get_all", return_value=[live_row]),
            patch(f"{_MOD}._claim_anchor", mock_claim),
        ):
            recover_stale_hour_23_claims()

        # Live lease → no re-claim attempted
        mock_claim.assert_not_called()

    # ── New targeted tests (eligibility re-check + terminal Skipped state) ───

    def _make_stale_row(
        self,
        automation_type="consent_request",
        template="CONSENT-TMPL",
    ):
        return {
            "name": "LOG-T",
            "anchor_message": "WM-IN-T",
            "whatsapp_account": "WA-001",
            "contact_number": "+1234567890",
            "automation_type": automation_type,
            "template": template,
            "claim_expires_at": "2026-04-01 10:00:00",
        }

    def test_recovery_skips_opted_out_contact(self):
        """Contact is now DNC/opted-out → no send, row marked Skipped."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # _load_contact_state returns None → DNC/opted-out
            patch(f"{_MOD}._load_contact_state", return_value=None),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_msg_doc.insert.assert_not_called()
        mock_set_value.assert_called_once()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")
        self.assertIsNone(fields["claim_expires_at"])

    def test_recovery_skips_consent_request_when_contact_now_has_consent(self):
        """Contact gained consent since original claim — consent_request no
        longer needed → no send, row marked Skipped.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # Contact now has consent (has_consent=True)
            patch(f"{_MOD}._load_contact_state", return_value=(None, True)),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(automation_type="consent_request"),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    def test_recovery_skips_status_followup_when_contact_lost_consent(self):
        """Contact lost consent since original claim — status_follow_up no
        longer appropriate → no send, row marked Skipped.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # Contact now has NO consent (has_consent=False)
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(automation_type="status_follow_up"),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    def test_recovery_skips_unapproved_template(self):
        """Template is no longer APPROVED → no send, row marked Skipped."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        # status="PAUSED" → _check_template_shape returns a reason string
        paused_template = _make_template(name="CONSENT-TMPL", status="PAUSED")
        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(
                f"{_MOD}.frappe.get_doc",
                side_effect=[paused_template, mock_msg_doc],
            ),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    # ── Durable skip (crash-safety) ──────────────────────────────────────────

    def test_recovery_dnc_skip_is_committed(self):
        """DNC/opt-out path: _mark_log_skipped commits so the terminal state
        survives a worker crash before the surrounding transaction closes.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}._load_contact_state", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_commit.assert_called_once()

    def test_recovery_missing_mapping_skip_is_committed(self):
        """Missing-mapping path: _mark_log_skipped commits so the terminal
        state survives a worker crash.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={},  # no mapping → terminal skip
            )

        mock_commit.assert_called_once()

    def test_primary_path_metadata_mismatch_skip_is_committed(self):
        """Primary-path stored-metadata mismatch: _post_claim_checks marks
        Skipped and commits so the terminal state is crash-durable.
        """
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-MISMATCH"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                _make_profile(),
                _make_template(name="CONSENT-TMPL"),
                MagicMock(),
            ]),
            # call 1: _reconcile_if_already_sent lookup → None (no prior msg)
            # calls 2-3: _post_claim_checks Step-B → mismatching stored values
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=[None, "status_follow_up", "FOLLOWUP-TMPL"]),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            patch(f"{_MOD}.frappe.log_error"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map={
                    "en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL"),
                },
                marketing_consent_category=None,
            )

        mock_commit.assert_called_once()

    def test_recovery_and_primary_both_call_check_template_shape(self):
        """Both _retry_stale_claim and _process_candidate delegate template
        validation to the shared _check_template_shape function, so the two
        paths cannot silently diverge on template eligibility.
        """
        from frappe_whatsapp.utils.hour_23_automation import (
            _process_candidate,
            _retry_stale_claim,
        )

        shape_error = "template 'T' is not APPROVED (status: PAUSED)"
        template = _make_template()
        lang_map = {"en": _make_row("en")}

        # ── Primary path ──────────────────────────────────────────────────
        mock_shape_primary = MagicMock(return_value=shape_error)
        mock_claim = MagicMock()

        with (
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=template),
            patch(f"{_MOD}._check_template_shape", mock_shape_primary),
            patch(f"{_MOD}._claim_anchor", mock_claim),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_shape_primary.assert_called_once()
        # Shape check failed → claim must never be attempted
        mock_claim.assert_not_called()

        # ── Recovery path ─────────────────────────────────────────────────
        mock_shape_recovery = MagicMock(return_value=shape_error)
        mock_set_value = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=template),
            patch(f"{_MOD}._check_template_shape", mock_shape_recovery),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_shape_recovery.assert_called_once()
        # Shape check failed → row marked Skipped
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    # ── Language/template recomputation tests ────────────────────────────────

    def test_recovery_skips_when_contact_language_changed(self):
        """Contact language changed so recomputed template differs → Skipped.

        Stored row: consent_request / CONSENT-TMPL (no detected language at
        claim time → English fallback was used).
        Current state: contact now has detected_language='es' but no consent
        → recomputed template would be CONSENT-ES, not CONSENT-TMPL → mismatch.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        # lang_map has both 'en' and 'es'; contact now reports 'es'
        lang_map = {
            "en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL"),
            "es": _make_row("es", "CONSENT-ES", "FOLLOWUP-ES"),
        }

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # Contact now has detected_language='es', no consent
            patch(f"{_MOD}._load_contact_state", return_value=("es", False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(
                    automation_type="consent_request",
                    # stored from original English claim
                    template="CONSENT-TMPL",
                ),
                marketing_consent_category=None,
                lang_map=lang_map,
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    def test_recovery_skips_when_lang_map_template_changed(self):
        """Lang map was reconfigured; recomputed template no longer matches
        what was stored → Skipped.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        # Current lang map maps 'en' consent to NEW-CONSENT-TMPL
        lang_map = {"en": _make_row("en", "NEW-CONSENT-TMPL", "FOLLOWUP-TMPL")}

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # Contact has no consent; language resolves to 'en' fallback
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(
                    automation_type="consent_request",
                    template="CONSENT-TMPL",  # stored original
                ),
                marketing_consent_category=None,
                lang_map=lang_map,
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    def test_recovery_skips_missing_current_language_mapping(self):
        """Lang map has no 'en' fallback and no match for the contact's
        language → _resolve_template_row returns None → Skipped.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_msg_doc = MagicMock()

        # Only 'es' in map; contact language is None → no fallback
        lang_map = {"es": _make_row("es", "CONSENT-ES", "FOLLOWUP-ES")}

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(f"{_MOD}.frappe.get_doc", return_value=mock_msg_doc),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error"),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map=lang_map,
            )

        mock_msg_doc.insert.assert_not_called()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")

    def test_recovery_sends_when_recomputed_template_matches_stored(self):
        """Happy path: recomputed template/type matches stored → msg sent."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        template = _make_template(name="CONSENT-TMPL")
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-RETRY"
        mock_set_value = MagicMock()

        # lang_map maps 'en' consent to CONSENT-TMPL — same as stored row
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # No prior outbound message (normal recovery path)
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            # No consent → recomputed: consent_request / CONSENT-TMPL (matches)
            patch(f"{_MOD}._load_contact_state", return_value=(None, False)),
            patch(
                f"{_MOD}.frappe.get_doc",
                side_effect=[template, mock_msg_doc],
            ),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.frappe.log_error"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _retry_stale_claim(
                self._make_stale_row(
                    automation_type="consent_request",
                    template="CONSENT-TMPL",
                ),
                marketing_consent_category=None,
                lang_map=lang_map,
            )

        mock_msg_doc.insert.assert_called_once()
        set_value_kwargs = mock_set_value.call_args[0]
        sent_fields = set_value_kwargs[2]
        self.assertEqual(sent_fields["send_status"], "Sent")
        self.assertIsNone(sent_fields["claim_expires_at"])

    def test_recovery_reconciles_instead_of_resending_after_send_crash(self):
        """Failure seam: msg_doc.insert() succeeded and was committed
        (Phase 2), but the process crashed before the log row was updated
        to Sent (Phase 3).

        On the next recovery run _reconcile_if_already_sent() detects the
        existing WhatsApp Message via reference_name, finalises the log row to
        Sent, and returns without calling msg_doc.insert() again.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_commit = MagicMock()
        # must NOT be called (no new msg_doc created)
        mock_get_doc = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # _reconcile_if_already_sent (step 1a): call 1 returns name,
            # call 2 returns creation.  Fires before _load_contact_state.
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=["WM-EXISTING-001", "2026-04-01 12:05:00"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            # frappe.get_doc must not be called (reconcile fires in step 1a)
            patch(f"{_MOD}.frappe.get_doc", mock_get_doc),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        # Reconciliation path: log finalised to Sent using the existing message
        mock_set_value.assert_called_once()
        log_name_arg, fields = (
            mock_set_value.call_args[0][1],
            mock_set_value.call_args[0][2],
        )
        self.assertEqual(log_name_arg, "LOG-T")
        self.assertEqual(fields["send_status"], "Sent")
        self.assertEqual(fields["outgoing_message"], "WM-EXISTING-001")
        self.assertEqual(fields["sent_at"], "2026-04-01 12:05:00")
        self.assertIsNone(fields["claim_expires_at"])
        # Commit must be called to durably finalise the reconciliation
        mock_commit.assert_called_once()
        # No new WhatsApp Message was created
        mock_get_doc.assert_not_called()

    def test_recovery_reconciles_despite_contact_now_opted_out(self):
        """step 1a fires before the DNC check: a contact who opted out after
        Phase 2 was committed still ends up with send_status='Sent', not
        'Skipped', so the audit trail is correct and _maybe_do_category_opt_in
        can find the log row on a subsequent YES reply.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            # step 1a: call 1 returns msg name, call 2 returns creation
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=["WM-SENT-DNC", "2026-04-01 11:50:00"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            # _load_contact_state must NOT be called (we returned in step 1a)
            patch(f"{_MOD}._load_contact_state") as mock_load,
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={"en": _make_row("en")},
            )

        mock_load.assert_not_called()
        mock_set_value.assert_called_once()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Sent")
        self.assertEqual(fields["outgoing_message"], "WM-SENT-DNC")
        self.assertIsNone(fields["claim_expires_at"])

    def test_recovery_reconciles_despite_mapping_now_missing(self):
        """step 1a fires before the mapping/template check: even when the
        language map is empty (config removed), a prior Phase-2 send is still
        correctly reconciled to Sent rather than Skipped.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-T"),
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=["WM-SENT-NOMAP", "2026-04-01 11:51:00"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map={},  # empty — no mapping for any language
            )

        mock_set_value.assert_called_once()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Sent")
        self.assertEqual(fields["outgoing_message"], "WM-SENT-NOMAP")
        self.assertIsNone(fields["claim_expires_at"])

    def test_reconciliation_preserves_outgoing_msg_for_category_opt_in(self):
        """Reconciliation always sets outgoing_message on the log row.

        _maybe_do_category_opt_in queries the log by outgoing_message +
        automation_type='consent_request'.  This test verifies that a
        consent_request row reconciled in step 1a has the correct
        outgoing_message so that lookup will succeed.
        """
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        mock_set_value = MagicMock()
        mock_commit = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-CONSENT"),
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=["WM-CONSENT-001", "2026-04-01 11:52:00"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
        ):
            _retry_stale_claim(
                self._make_stale_row(automation_type="consent_request",
                                     template="CONSENT-TMPL"),
                marketing_consent_category="MARKETING",
                lang_map={"en": _make_row("en")},
            )

        mock_set_value.assert_called_once()
        log_name_arg, fields = (
            mock_set_value.call_args[0][1],
            mock_set_value.call_args[0][2],
        )
        self.assertEqual(log_name_arg, "LOG-CONSENT")
        # outgoing_message must be set so _maybe_do_category_opt_in can find it
        self.assertEqual(fields["outgoing_message"], "WM-CONSENT-001")
        self.assertEqual(fields["sent_at"], "2026-04-01 11:52:00")
        self.assertEqual(fields["send_status"], "Sent")
        self.assertIsNone(fields["claim_expires_at"])

    def test_primary_path_reclaim_reconciles_no_resend(self):
        """Primary path: _process_candidate re-claims a stale Pending row and
        _post_claim_checks detects the prior send via reference_name.

        Scenario: worker A committed the outbound WhatsApp Message (Phase 2)
        but crashed before marking the log Sent (Phase 3).  The hourly job
        re-runs _process_candidate for the same candidate.  _claim_anchor
        returns the existing log row name (stale re-claim path), and
        _post_claim_checks reconciles it to Sent without re-sending.
        """
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        mock_set_value = MagicMock()
        mock_commit = MagicMock()
        mock_msg_doc = MagicMock()

        profile = _make_profile()
        template = _make_template()
        lang_map = {"en": _make_row("en")}

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-RECLAIM"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template,
                mock_msg_doc]),
            # _reconcile_if_already_sent: call 1=name, call 2=creation
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=["WM-PRIOR-001", "2026-04-01 11:55:00"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.db.commit", mock_commit),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        # Reconciliation path: log finalised to Sent using the existing message
        mock_msg_doc.insert.assert_not_called()
        mock_set_value.assert_called_once()
        log_name_arg, fields = (
            mock_set_value.call_args[0][1],
            mock_set_value.call_args[0][2],
        )
        self.assertEqual(log_name_arg, "LOG-RECLAIM")
        self.assertEqual(fields["send_status"], "Sent")
        self.assertEqual(fields["outgoing_message"], "WM-PRIOR-001")
        self.assertEqual(fields["sent_at"], "2026-04-01 11:55:00")
        self.assertIsNone(fields["claim_expires_at"])
        mock_commit.assert_called_once()

    def test_primary_path_reclaim_skips_on_stored_metadata_mismatch(self):
        """Primary path: _process_candidate re-claims a Pending row whose
        stored template/type no longer matches the current decision.

        Scenario: the language map was reconfigured between the original claim
        and this run.  _post_claim_checks detects the mismatch and marks the
        row Skipped so no inconsistent message is sent.
        """
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        mock_set_value = MagicMock()
        mock_log_error = MagicMock()
        mock_msg_doc = MagicMock()

        profile = _make_profile()
        # Current decision: consent_request / CONSENT-TMPL
        template = _make_template(name="CONSENT-TMPL")
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-MISMATCH"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template,
                mock_msg_doc]),
            # call 1: _reconcile lookup → None (no prior msg)
            # calls 2-3: _post_claim_checks Step-B → mismatching stored values
            patch(f"{_MOD}.frappe.db.get_value",
                  side_effect=[None, "status_follow_up", "FOLLOWUP-TMPL"]),
            patch(f"{_MOD}.frappe.db.set_value", mock_set_value),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        # Mismatch detected: no send, row marked Skipped
        mock_msg_doc.insert.assert_not_called()
        mock_set_value.assert_called_once()
        fields = mock_set_value.call_args[0][2]
        self.assertEqual(fields["send_status"], "Skipped")
        self.assertIsNone(fields["claim_expires_at"])
        mock_log_error.assert_called_once()


# ── Missing mapping ─────────────────────────────────────────────────────────

class TestMissingMapping(FrappeTestCase):
    """Missing language row logs an error and skips without crashing."""

    def test_no_en_fallback_logs_and_skips(self):
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(detected_language="pt")
        # No 'en' row and no 'pt' row
        lang_map = {"es": _make_row("es")}

        mock_log_error = MagicMock()
        mock_get_doc = MagicMock()

        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(
                f"{_MOD}.frappe.get_doc",
                side_effect=[profile, mock_get_doc],
            ),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_log_error.assert_called_once()
        error_msg = mock_log_error.call_args[0][0]
        self.assertIn("no language mapping", error_msg.lower())
        # No outgoing message was created
        mock_get_doc.assert_not_called()

    def test_no_template_configured_logs_and_skips(self):
        """Row exists but consent_template is blank → log and skip."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False, detected_language="en")
        # Row has no consent_template set
        lang_map = {"en": SimpleNamespace(
            language_code="en",
            consent_template=None,
            status_follow_up_template="FOLLOWUP-TMPL",
        )}

        mock_log_error = MagicMock()
        mock_get_doc = MagicMock()

        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(
                f"{_MOD}.frappe.get_doc",
                side_effect=[profile, mock_get_doc],
            ),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_log_error.assert_called_once()
        mock_get_doc.assert_not_called()

    def test_dynamic_url_button_template_is_skipped(self):
        """Template with a dynamic URL button is logged and skipped.

        send_template() requires reference_doctype/reference_name for dynamic
        URL buttons, which the automation cannot supply.
        """
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        dyn_btn = SimpleNamespace(
            button_type="Visit Website",
            url_type="Dynamic",
            website_url="{{url}}",
            button_label="Open",
        )
        template = _make_template(name="CONSENT-TMPL", is_consent_request=1)
        template = SimpleNamespace(
            name="CONSENT-TMPL",
            status="APPROVED",
            is_consent_request=1,
            sample_values=None,
            field_names=None,
            header_type=None,
            buttons=[dyn_btn],
        )
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        mock_log_error = MagicMock()
        mock_msg_doc = MagicMock()

        with (
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc", side_effect=[
                profile, template, mock_msg_doc,
            ]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
            )

        mock_log_error.assert_called_once()
        error_msg = mock_log_error.call_args[0][0]
        self.assertIn("dynamic url", error_msg.lower())
        mock_msg_doc.insert.assert_not_called()


# ── run_hour_23_automation — integration ────────────────────────────────────

class TestRunHour23Automation(FrappeTestCase):
    """run_hour_23_automation: feature flag, empty map, and candidate loop."""

    def test_disabled_flag_returns_immediately(self):
        settings = _make_settings(enable_hour_23_follow_up=0)
        mock_candidates = MagicMock()
        with (
            patch(f"{_MOD}.frappe.get_cached_doc", return_value=settings),
            patch(f"{_MOD}._get_candidates", mock_candidates),
        ):
            from frappe_whatsapp.utils.hour_23_automation import (
                run_hour_23_automation,
            )
            run_hour_23_automation()
        mock_candidates.assert_not_called()

    def test_empty_language_map_logs_and_returns(self):
        settings = _make_settings(hour_23_language_map=[])
        mock_candidates = MagicMock()
        mock_log = MagicMock()
        with (
            patch(f"{_MOD}.frappe.get_cached_doc", return_value=settings),
            patch(f"{_MOD}._get_candidates", mock_candidates),
            patch(f"{_MOD}.frappe.log_error", mock_log),
        ):
            from frappe_whatsapp.utils.hour_23_automation import (
                run_hour_23_automation,
            )
            run_hour_23_automation()
        mock_candidates.assert_not_called()
        mock_log.assert_called_once()

    def test_candidate_exception_is_caught_and_logged(self):
        """An error for one candidate must not abort the whole job."""
        settings = _make_settings()
        mock_log = MagicMock()

        with (
            patch(f"{_MOD}.frappe.get_cached_doc", return_value=settings),
            patch(f"{_MOD}._get_candidates",
                  return_value=[_make_candidate(), _make_candidate("other")]),
            patch(f"{_MOD}._process_candidate",
                  side_effect=Exception("boom")),
            patch(f"{_MOD}.frappe.get_traceback", return_value="tb"),
            patch(f"{_MOD}.frappe.log_error", mock_log),
        ):
            from frappe_whatsapp.utils.hour_23_automation import (
                run_hour_23_automation,
            )
            run_hour_23_automation()

        # Both candidates attempted, both errors logged
        self.assertEqual(mock_log.call_count, 2)


# ── Category opt-in on YES reply ────────────────────────────────────────────

class TestCategoryOptInOnYesReply(FrappeTestCase):
    """_maybe_do_category_opt_in fires category opt-in for consent replies."""

    def test_yes_reply_to_hour23_consent_msg_triggers_category_opt_in(self):
        """YES reply to a hour-23 consent_request message → category opt-in."""
        from frappe_whatsapp.utils.webhook import _maybe_do_category_opt_in

        mock_settings = SimpleNamespace(marketing_consent_category="MARKETING")
        mock_process = MagicMock()

        with (
            # frappe.db.get_value → outgoing message name
            patch(f"{_WEBHOOK_MOD}.frappe.db.get_value",
                  return_value="WM-OUT-001"),
            # frappe.db.exists → automation log row exists
            patch(f"{_WEBHOOK_MOD}.frappe.db.exists",
                  return_value="LOG-001"),
            patch(
                "frappe_whatsapp.utils.consent.get_compliance_settings",
                return_value=mock_settings,
            ),
            patch(
                "frappe_whatsapp.utils.consent.process_category_opt_in",
                mock_process,
            ),
        ):
            _maybe_do_category_opt_in(
                reply_to_message_id="wamid.abc123",
                contact_number="+1234567890",
                whatsapp_account_name="WA-001",
                message_doc_name="WM-IN-001",
                profile_name=None,
            )

        mock_process.assert_called_once()
        kwargs = mock_process.call_args.kwargs
        self.assertEqual(kwargs["consent_category"], "MARKETING")
        self.assertEqual(kwargs["contact_number"], "+1234567890")

    def test_no_marketing_category_configured_skips(self):
        """When marketing_consent_category is blank, opt-in is skipped."""
        from frappe_whatsapp.utils.webhook import _maybe_do_category_opt_in

        mock_settings = SimpleNamespace(marketing_consent_category=None)
        mock_process = MagicMock()

        with (
            patch(
                "frappe_whatsapp.utils.consent.get_compliance_settings",
                return_value=mock_settings,
            ),
            patch(
                "frappe_whatsapp.utils.consent.process_category_opt_in",
                mock_process,
            ),
        ):
            _maybe_do_category_opt_in(
                reply_to_message_id="wamid.abc123",
                contact_number="+1234567890",
                whatsapp_account_name="WA-001",
                message_doc_name="WM-IN-001",
                profile_name=None,
            )

        mock_process.assert_not_called()

    def test_no_matching_outgoing_message_skips(self):
        """No outgoing message with that message_id → skip silently."""
        from frappe_whatsapp.utils.webhook import _maybe_do_category_opt_in

        mock_settings = SimpleNamespace(marketing_consent_category="MARKETING")
        mock_process = MagicMock()

        with (
            patch(f"{_WEBHOOK_MOD}.frappe.db.get_value", return_value=None),
            patch(
                "frappe_whatsapp.utils.consent.get_compliance_settings",
                return_value=mock_settings,
            ),
            patch(
                "frappe_whatsapp.utils.consent.process_category_opt_in",
                mock_process,
            ),
        ):
            _maybe_do_category_opt_in(
                reply_to_message_id="wamid.unknown",
                contact_number="+1234567890",
                whatsapp_account_name="WA-001",
                message_doc_name="WM-IN-002",
                profile_name=None,
            )

        mock_process.assert_not_called()

    def test_reply_not_linked_to_hour23_log_skips(self):
        """YES reply to a consent-request template that is NOT from the hour-23
        automation does not grant marketing category consent.

        This is the key scoping test: only hour-23 automation log rows with
        automation_type='consent_request' trigger category opt-in; a manual
        or other-system consent-request template must not.
        """
        from frappe_whatsapp.utils.webhook import _maybe_do_category_opt_in

        mock_settings = SimpleNamespace(marketing_consent_category="MARKETING")
        mock_process = MagicMock()

        with (
            # Outgoing message found
            patch(f"{_WEBHOOK_MOD}.frappe.db.get_value",
                  return_value="WM-OUT-MANUAL"),
            # But there is NO matching hour-23 automation log row
            patch(f"{_WEBHOOK_MOD}.frappe.db.exists", return_value=None),
            patch(
                "frappe_whatsapp.utils.consent.get_compliance_settings",
                return_value=mock_settings,
            ),
            patch(
                "frappe_whatsapp.utils.consent.process_category_opt_in",
                mock_process,
            ),
        ):
            _maybe_do_category_opt_in(
                reply_to_message_id="wamid.manual",
                contact_number="+1234567890",
                whatsapp_account_name="WA-001",
                message_doc_name="WM-IN-003",
                profile_name=None,
            )

        mock_process.assert_not_called()

    def test_exception_in_category_opt_in_is_logged_not_raised(self):
        """An exception inside _maybe_do_category_opt_in must not propagate."""
        from frappe_whatsapp.utils.webhook import _maybe_do_category_opt_in

        with (
            patch(
                "frappe_whatsapp.utils.consent.get_compliance_settings",
                side_effect=Exception("db error"),
            ),
            patch(f"{_WEBHOOK_MOD}.frappe.log_error") as mock_log,
            patch(f"{_WEBHOOK_MOD}.frappe.get_traceback", return_value="tb"),
        ):
            # Must not raise
            _maybe_do_category_opt_in(
                reply_to_message_id="wamid.err",
                contact_number="+1234567890",
                whatsapp_account_name="WA-001",
                message_doc_name="WM-IN-004",
                profile_name=None,
            )

        mock_log.assert_called_once()


# ── process_category_opt_in (consent.py) ───────────────────────────────────

class TestProcessCategoryOptIn(FrappeTestCase):
    """process_category_opt_in creates / updates category consent and logs."""

    def _make_profile_doc(self, category_consents=None):
        profile = MagicMock()
        profile.name = "WP-001"
        profile.number = "+1234567890"
        profile.is_opted_in = 0
        profile.consent_status = "Unknown"
        profile.category_consents = category_consents or []
        profile.get = (
            lambda field, default=None: getattr(profile, field, default)
        )
        return profile

    def test_creates_new_category_row(self):
        """When no existing row, a new category_consents entry is appended."""
        from frappe_whatsapp.utils.consent import process_category_opt_in

        profile = self._make_profile_doc(category_consents=[])

        with (
            patch(f"{_CONSENT_MOD}._get_or_create_profile",
                  return_value="WP-001"),
            patch(f"{_CONSENT_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_CONSENT_MOD}._log_consent") as mock_log,
            patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_CONSENT_MOD}.now_datetime",
                  return_value="2026-04-01 12:00:00"),
        ):
            process_category_opt_in(
                contact_number="+1234567890",
                whatsapp_account="WA-001",
                consent_category="MARKETING",
                message_doc_name="WM-IN-001",
            )

        profile.append.assert_called_once()
        append_kwargs = profile.append.call_args[0]
        self.assertEqual(append_kwargs[0], "category_consents")
        row_data = append_kwargs[1]
        self.assertEqual(row_data["consent_category"], "MARKETING")
        self.assertTrue(row_data["consented"])
        profile.save.assert_called_once()
        mock_log.assert_called_once()
        log_kwargs = mock_log.call_args.kwargs
        self.assertEqual(log_kwargs["action_type"], "Category Opt-In")
        self.assertEqual(log_kwargs["consent_category"], "MARKETING")
        self.assertFalse(log_kwargs["previous_status"])
        self.assertTrue(log_kwargs["new_status"])

    def test_updates_existing_row(self):
        """Existing unconsented row is updated in-place."""
        from frappe_whatsapp.utils.consent import process_category_opt_in

        existing_row = SimpleNamespace(
            consent_category="MARKETING",
            consented=0,
            consented_at=None,
            consent_method="",
        )
        profile = self._make_profile_doc(category_consents=[existing_row])
        profile.get = lambda f, d=None: (
            [existing_row] if f == "category_consents" else d)

        with (
            patch(f"{_CONSENT_MOD}._get_or_create_profile",
                  return_value="WP-001"),
            patch(f"{_CONSENT_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_CONSENT_MOD}._log_consent") as mock_log,
            patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_CONSENT_MOD}.now_datetime",
                  return_value="2026-04-01 12:00:00"),
        ):
            process_category_opt_in(
                contact_number="+1234567890",
                whatsapp_account="WA-001",
                consent_category="MARKETING",
            )

        self.assertTrue(existing_row.consented)
        self.assertEqual(existing_row.consent_method, "WhatsApp Reply")
        profile.save.assert_called_once()
        mock_log.assert_called_once()
        self.assertFalse(mock_log.call_args.kwargs["previous_status"])
        self.assertTrue(mock_log.call_args.kwargs["new_status"])

    def test_consent_status_set_to_partial_when_not_fully_opted_in(self):
        """Profile that is not fully opted-in gets consent_status='Partial'."""
        from frappe_whatsapp.utils.consent import process_category_opt_in

        profile = self._make_profile_doc()
        profile.is_opted_in = 0

        with (
            patch(f"{_CONSENT_MOD}._get_or_create_profile",
                  return_value="WP-001"),
            patch(f"{_CONSENT_MOD}.frappe.get_doc", return_value=profile),
            patch(f"{_CONSENT_MOD}._log_consent"),
            patch(f"{_CONSENT_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_CONSENT_MOD}.now_datetime",
                  return_value="2026-04-01 12:00:00"),
        ):
            process_category_opt_in(
                contact_number="+1234567890",
                whatsapp_account="WA-001",
                consent_category="MARKETING",
            )

        self.assertEqual(profile.consent_status, "Partial")
