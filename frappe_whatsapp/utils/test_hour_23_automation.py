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


# ── Parameterized template helpers ──────────────────────────────────────────

_PARAMS_MOD = "frappe_whatsapp.utils.hour_23_params"


def _make_param_row(
    template="CONSENT-TMPL",
    parameter_index=1,
    source_type="First Name",
    source_field="",
    literal_value="",
    fallback_value="",
):
    return SimpleNamespace(
        template=template,
        parameter_index=parameter_index,
        source_type=source_type,
        source_field=source_field,
        literal_value=literal_value,
        fallback_value=fallback_value,
    )


# ── extract_first_name ───────────────────────────────────────────────────────

class TestExtractFirstName(FrappeTestCase):
    """extract_first_name strips salutations and returns the first real token."""

    def _fn(self, name):
        from frappe_whatsapp.utils.hour_23_params import extract_first_name
        return extract_first_name(name)

    def test_plain_name_returns_first_token(self):
        self.assertEqual(self._fn("Alice Smith"), "Alice")

    def test_salutation_dot_stripped(self):
        self.assertEqual(self._fn("Mr. John Doe"), "John")

    def test_salutation_only_returns_empty(self):
        # All tokens are recognised salutations → no usable first name.
        self.assertEqual(self._fn("Dr."), "")
        self.assertEqual(self._fn("Mr"), "")
        self.assertEqual(self._fn("Mrs. Miss"), "")

    def test_blank_returns_empty(self):
        self.assertEqual(self._fn(""), "")

    def test_none_returns_empty(self):
        self.assertEqual(self._fn(None), "")

    def test_multiple_salutations_then_name(self):
        self.assertEqual(self._fn("Mrs. Dr. Jane"), "Jane")


# ── _build_param_mapping ─────────────────────────────────────────────────────

class TestBuildParamMapping(FrappeTestCase):
    """_build_param_mapping groups rows by template and sorts by index."""

    def _build(self, rows):
        from frappe_whatsapp.utils.hour_23_automation import (
            _build_param_mapping,
        )
        settings = SimpleNamespace(hour_23_template_parameters=rows)
        return _build_param_mapping(settings)

    def test_groups_by_template(self):
        rows = [
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-B", 1),
            _make_param_row("TMPL-A", 2),
        ]
        result = self._build(rows)
        self.assertIn("TMPL-A", result)
        self.assertIn("TMPL-B", result)
        self.assertEqual(len(result["TMPL-A"]), 2)

    def test_sorted_by_parameter_index(self):
        rows = [
            _make_param_row("TMPL-A", 3),
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-A", 2),
        ]
        result = self._build(rows)
        indices = [r.parameter_index for r in result["TMPL-A"]]
        self.assertEqual(indices, [1, 2, 3])

    def test_blank_template_row_ignored(self):
        rows = [_make_param_row("", 1), _make_param_row("TMPL-A", 1)]
        result = self._build(rows)
        self.assertNotIn("", result)
        self.assertIn("TMPL-A", result)

    def test_empty_table_returns_empty_dict(self):
        result = self._build([])
        self.assertEqual(result, {})


# ── Parameterized templates — _process_candidate ────────────────────────────

def _make_param_template(name="CONSENT-TMPL", is_consent_request=1):
    """Template with a body parameter declared (sample_values set)."""
    return SimpleNamespace(
        name=name,
        status="APPROVED",
        is_consent_request=is_consent_request,
        sample_values="first_name",
        field_names=None,
        header_type=None,
        buttons=[],
    )


class TestParameterizedTemplatesProcessCandidate(FrappeTestCase):
    """_process_candidate attaches body_param when param mapping exists."""

    def test_body_param_attached_to_outgoing_message(self):
        """Resolved body_param JSON is set on the outgoing WhatsApp Message."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        template = _make_param_template()
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        param_mapping = {
            "CONSENT-TMPL": [_make_param_row("CONSENT-TMPL", 1)]
        }
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
            patch(
                f"{_MOD}.build_hour_23_body_params",
                return_value=('{"1":"Alice"}', None),
            ),
            patch(
                f"{_MOD}.load_contact_context",
                return_value={"profile": profile, "contact": None},
            ),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
                param_mapping=param_mapping,
            )

        msg_dict = mock_get_doc.call_args_list[2][0][0]
        self.assertEqual(msg_dict["body_param"], '{"1":"Alice"}')

    def test_unresolvable_param_logs_and_skips(self):
        """Param resolution failure → log error, mark Skipped, no send."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        template = _make_param_template()
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        param_mapping = {
            "CONSENT-TMPL": [_make_param_row("CONSENT-TMPL", 1)]
        }
        mock_msg_doc = MagicMock()
        mock_log_error = MagicMock()
        mock_mark_skipped = MagicMock()

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
            patch(
                f"{_MOD}.build_hour_23_body_params",
                return_value=(None, "param 1 could not be resolved"),
            ),
            patch(
                f"{_MOD}.load_contact_context",
                return_value={"profile": profile, "contact": None},
            ),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
            patch(f"{_MOD}._mark_log_skipped", mock_mark_skipped),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=lang_map,
                marketing_consent_category=None,
                param_mapping=param_mapping,
            )

        mock_log_error.assert_called_once()
        mock_mark_skipped.assert_called_once_with("LOG-001")
        mock_msg_doc.insert.assert_not_called()

    def test_parameterized_template_without_mapping_rejected(self):
        """Template with body params but no mapping → shape error, skipped."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        template = _make_param_template()  # has sample_values
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}

        mock_msg_doc = MagicMock()
        mock_log_error = MagicMock()

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
                param_mapping={},  # no mapping configured
            )

        mock_log_error.assert_called_once()
        error_msg = mock_log_error.call_args[0][0]
        self.assertIn("body parameters", error_msg.lower())
        mock_msg_doc.insert.assert_not_called()

    def test_no_param_rows_sends_without_body_param(self):
        """Template without body params sends normally (body_param=None)."""
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
                param_mapping={},
            )

        msg_dict = mock_get_doc.call_args_list[2][0][0]
        self.assertIsNone(msg_dict["body_param"])
        mock_msg_doc.insert.assert_called_once()


# ── Parameterized templates — recovery path ──────────────────────────────────

class TestParameterizedTemplatesRecovery(FrappeTestCase):
    """_retry_stale_claim recomputes params from current state."""

    def _make_stale_row(self):
        return {
            "name": "LOG-001",
            "anchor_message": "WM-IN-001",
            "whatsapp_account": "WA-001",
            "contact_number": "+1234567890",
            "automation_type": "consent_request",
            "template": "CONSENT-TMPL",
            "claim_expires_at": None,
        }

    def test_recovery_attaches_body_param(self):
        """Recovery path resolves params and attaches body_param to message."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        template = _make_param_template()
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        param_mapping = {
            "CONSENT-TMPL": [_make_param_row("CONSENT-TMPL", 1)]
        }
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-001"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}._load_contact_state",
                  return_value=("en", False)),
            patch(f"{_MOD}.frappe.get_doc",
                  side_effect=[template, mock_msg_doc]) as mock_get_doc,
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(
                f"{_MOD}.build_hour_23_body_params",
                return_value=('{"1":"Alice"}', None),
            ),
            patch(
                f"{_MOD}.load_contact_context",
                return_value={"profile": None, "contact": None},
            ),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map=lang_map,
                param_mapping=param_mapping,
            )

        msg_dict = mock_get_doc.call_args_list[1][0][0]
        self.assertEqual(msg_dict["body_param"], '{"1":"Alice"}')

    def test_recovery_param_error_marks_skipped(self):
        """Unresolvable param during recovery → Skipped, no send."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        template = _make_param_template()
        lang_map = {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}
        param_mapping = {
            "CONSENT-TMPL": [_make_param_row("CONSENT-TMPL", 1)]
        }
        mock_msg_doc = MagicMock()
        mock_log_error = MagicMock()
        mock_mark_skipped = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}._load_contact_state",
                  return_value=("en", False)),
            patch(f"{_MOD}.frappe.get_doc",
                  side_effect=[template, mock_msg_doc]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(
                f"{_MOD}.build_hour_23_body_params",
                return_value=(None, "param 1 unresolvable"),
            ),
            patch(
                f"{_MOD}.load_contact_context",
                return_value={"profile": None, "contact": None},
            ),
            patch(f"{_MOD}.frappe.log_error", mock_log_error),
            patch(f"{_MOD}._mark_log_skipped", mock_mark_skipped),
        ):
            _retry_stale_claim(
                self._make_stale_row(),
                marketing_consent_category=None,
                lang_map=lang_map,
                param_mapping=param_mapping,
            )

        mock_mark_skipped.assert_called_once_with("LOG-001")
        mock_msg_doc.insert.assert_not_called()


# ── First Name fallback_value ────────────────────────────────────────────────

class TestFirstNameFallback(FrappeTestCase):
    """_resolve_first_name returns '' when no name is available, so
    fallback_value on the param row is used instead of a hardcoded string.
    """

    def _run(self, profile=None, contact=None, fallback_value=""):
        """Call build_hour_23_body_params with a single First Name row."""
        from frappe_whatsapp.utils.hour_23_params import (
            build_hour_23_body_params,
        )
        template = SimpleNamespace(
            name="TMPL",
            template=None,
            sample_values="first_name",  # Meta-owned fallback; count = 1
            field_names=None,
        )
        row = _make_param_row(
            "TMPL", 1, "First Name", fallback_value=fallback_value
        )
        context = {"profile": profile, "contact": contact}
        return build_hour_23_body_params(template, context, [row])

    def test_configured_fallback_used_when_no_name(self):
        """No profile, no contact → configured fallback_value is used."""
        json_str, err = self._run(fallback_value="amigo")
        self.assertIsNone(err)
        import json
        self.assertEqual(json.loads(json_str)["1"], "amigo")

    def test_no_name_no_fallback_returns_error(self):
        """No profile, no contact, no fallback → unresolved-parameter error."""
        json_str, err = self._run(fallback_value="")
        self.assertIsNone(json_str)
        self.assertIsNotNone(err)
        self.assertIn("could not be resolved", err)

    def test_fallback_not_hardcoded_english(self):
        """A non-English fallback is preserved exactly (no 'there' override)."""
        json_str, err = self._run(fallback_value="estimado")
        self.assertIsNone(err)
        import json
        self.assertNotEqual(json.loads(json_str)["1"], "there")
        self.assertEqual(json.loads(json_str)["1"], "estimado")

    def test_real_name_takes_priority_over_fallback(self):
        """When a real first name is found, fallback_value is not used."""
        contact = SimpleNamespace(first_name="Carlos", full_name="Carlos R.")
        json_str, err = self._run(
            contact=contact, fallback_value="amigo"
        )
        self.assertIsNone(err)
        import json
        self.assertEqual(json.loads(json_str)["1"], "Carlos")

    def test_salutation_only_full_name_uses_fallback(self):
        """Contact whose full_name is only a salutation uses fallback_value."""
        contact = SimpleNamespace(
            first_name="", full_name="Dr."
        )
        json_str, err = self._run(contact=contact, fallback_value="amigo")
        self.assertIsNone(err)
        import json
        self.assertEqual(json.loads(json_str)["1"], "amigo")

    def test_salutation_only_profile_name_uses_fallback(self):
        """profile_name that is only a salutation uses fallback_value."""
        profile = SimpleNamespace(profile_name="Mr.")
        json_str, err = self._run(profile=profile, fallback_value="estimado")
        self.assertIsNone(err)
        import json
        self.assertEqual(json.loads(json_str)["1"], "estimado")

    def test_salutation_only_with_no_fallback_returns_error(self):
        """Salutation-only name with empty fallback → unresolved-param error."""
        contact = SimpleNamespace(first_name="", full_name="Mrs.")
        json_str, err = self._run(contact=contact, fallback_value="")
        self.assertIsNone(json_str)
        self.assertIsNotNone(err)
        self.assertIn("could not be resolved", err)


# ── load_contact_context resilience ─────────────────────────────────────────

class TestLoadContactContextResiliency(FrappeTestCase):
    """load_contact_context returns contact=None for stale profile links."""

    def test_missing_contact_returns_none_not_exception(self):
        """DoesNotExistError on Contact → contact=None, no crash."""
        import frappe
        from frappe_whatsapp.utils.hour_23_params import load_contact_context

        profile = SimpleNamespace(
            name="WP-001",
            contact="STALE-CONTACT-001",
            profile_name="Martinez",
        )
        with (
            patch(f"{_PARAMS_MOD}.format_number",
                  return_value="+1234567890"),
            patch(f"{_PARAMS_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_PARAMS_MOD}.frappe.get_doc",
                  side_effect=[
                      profile,
                      frappe.exceptions.DoesNotExistError,
                  ]),
        ):
            context = load_contact_context("+1234567890")

        self.assertIsNotNone(context["profile"])
        self.assertIsNone(context["contact"])

    def test_stale_contact_falls_through_to_profile_name(self):
        """contact=None → resolution uses profile.profile_name via fallback."""
        from frappe_whatsapp.utils.hour_23_params import (
            build_hour_23_body_params,
        )
        import json

        template = SimpleNamespace(
            name="T", template=None, sample_values="first_name", field_names=None
        )
        row = _make_param_row("T", 1, "First Name", fallback_value="amigo")
        profile = SimpleNamespace(profile_name="Martinez", contact="STALE")
        context = {"profile": profile, "contact": None}

        json_str, err = build_hour_23_body_params(template, context, [row])

        self.assertIsNone(err)
        # "Martinez" is a real first name (not a salutation).
        self.assertEqual(json.loads(json_str)["1"], "Martinez")

    def test_stale_contact_no_usable_name_uses_fallback(self):
        """contact=None + salutation-only profile_name → fallback_value."""
        from frappe_whatsapp.utils.hour_23_params import (
            build_hour_23_body_params,
        )
        import json

        template = SimpleNamespace(
            name="T", template=None, sample_values="first_name", field_names=None
        )
        row = _make_param_row("T", 1, "First Name", fallback_value="estimado")
        profile = SimpleNamespace(profile_name="Dr.", contact="STALE")
        context = {"profile": profile, "contact": None}

        json_str, err = build_hour_23_body_params(template, context, [row])

        self.assertIsNone(err)
        self.assertEqual(json.loads(json_str)["1"], "estimado")

    def test_stale_contact_no_name_no_fallback_returns_error(self):
        """contact=None + no usable name + no fallback → param error."""
        from frappe_whatsapp.utils.hour_23_params import (
            build_hour_23_body_params,
        )

        template = SimpleNamespace(
            name="T", template=None, sample_values="first_name", field_names=None
        )
        row = _make_param_row("T", 1, "First Name", fallback_value="")
        profile = SimpleNamespace(profile_name="", contact="STALE")
        context = {"profile": profile, "contact": None}

        json_str, err = build_hour_23_body_params(template, context, [row])

        self.assertIsNone(json_str)
        self.assertIsNotNone(err)
        self.assertIn("could not be resolved", err)


# ── Stale contact in primary and recovery paths ──────────────────────────────

class TestStaleContactInAutomationPaths(FrappeTestCase):
    """Stale profile.contact link does not prevent send or leave row Pending."""

    def _lang_map(self):
        return {"en": _make_row("en", "CONSENT-TMPL", "FOLLOWUP-TMPL")}

    def _param_template(self):
        return SimpleNamespace(
            name="CONSENT-TMPL",
            status="APPROVED",
            is_consent_request=1,
            sample_values="first_name",
            field_names=None,
            header_type=None,
            buttons=[],
        )

    def test_primary_stale_contact_with_profile_name_sends(self):
        """Stale contact link + usable profile_name → message is sent."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate
        import json

        profile = _make_profile(is_opted_in=False)
        template = self._param_template()
        # load_contact_context returns contact=None (stale link),
        # but profile has a usable profile_name.
        stale_context = {
            "profile": SimpleNamespace(profile_name="Rosa", contact="STALE"),
            "contact": None,
        }
        mock_msg_doc = MagicMock()
        mock_msg_doc.name = "WM-OUT-001"

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc",
                  side_effect=[profile, template, mock_msg_doc]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.load_contact_context", return_value=stale_context),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=self._lang_map(),
                marketing_consent_category=None,
                param_mapping={
                    "CONSENT-TMPL": [
                        _make_param_row("CONSENT-TMPL", 1, "First Name")
                    ]
                },
            )

        mock_msg_doc.insert.assert_called_once()

    def test_primary_stale_contact_no_fallback_marks_skipped(self):
        """Stale contact + no name + no fallback → row Skipped, not Pending."""
        from frappe_whatsapp.utils.hour_23_automation import _process_candidate

        profile = _make_profile(is_opted_in=False)
        template = self._param_template()
        # No usable name, no fallback.
        stale_context = {
            "profile": SimpleNamespace(profile_name="", contact="STALE"),
            "contact": None,
        }
        mock_msg_doc = MagicMock()
        mock_mark_skipped = MagicMock()

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}.format_number", return_value="+1234567890"),
            patch(f"{_MOD}.frappe.db.get_all",
                  return_value=[SimpleNamespace(name="WP-001")]),
            patch(f"{_MOD}.frappe.get_doc",
                  side_effect=[profile, template, mock_msg_doc]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.load_contact_context", return_value=stale_context),
            patch(f"{_MOD}.frappe.log_error"),
            patch(f"{_MOD}._mark_log_skipped", mock_mark_skipped),
        ):
            _process_candidate(
                candidate=_make_candidate(),
                lang_map=self._lang_map(),
                marketing_consent_category=None,
                param_mapping={
                    "CONSENT-TMPL": [
                        _make_param_row("CONSENT-TMPL", 1, "First Name",
                                        fallback_value="")
                    ]
                },
            )

        # Row must be Skipped — not left Pending for infinite recovery loops.
        mock_mark_skipped.assert_called_once_with("LOG-001")
        mock_msg_doc.insert.assert_not_called()

    def test_recovery_stale_contact_marks_skipped_not_pending(self):
        """Recovery path: stale contact + no fallback → Skipped, not Pending."""
        from frappe_whatsapp.utils.hour_23_automation import _retry_stale_claim

        template = self._param_template()
        stale_context = {
            "profile": SimpleNamespace(profile_name="", contact="STALE"),
            "contact": None,
        }
        mock_msg_doc = MagicMock()
        mock_mark_skipped = MagicMock()

        stale_row = {
            "name": "LOG-001",
            "anchor_message": "WM-IN-001",
            "whatsapp_account": "WA-001",
            "contact_number": "+1234567890",
            "automation_type": "consent_request",
            "template": "CONSENT-TMPL",
            "claim_expires_at": None,
        }

        with (
            patch(f"{_MOD}._claim_anchor", return_value="LOG-001"),
            patch(f"{_MOD}._load_contact_state",
                  return_value=("en", False)),
            patch(f"{_MOD}.frappe.get_doc",
                  side_effect=[template, mock_msg_doc]),
            patch(f"{_MOD}.frappe.db.get_value", return_value=None),
            patch(f"{_MOD}.frappe.db.set_value"),
            patch(f"{_MOD}.frappe.db.commit"),
            patch(f"{_MOD}.now_datetime", return_value="2026-04-01 12:00:00"),
            patch(f"{_MOD}.load_contact_context", return_value=stale_context),
            patch(f"{_MOD}.frappe.log_error"),
            patch(f"{_MOD}._mark_log_skipped", mock_mark_skipped),
        ):
            _retry_stale_claim(
                stale_row,
                marketing_consent_category=None,
                lang_map=self._lang_map(),
                param_mapping={
                    "CONSENT-TMPL": [
                        _make_param_row("CONSENT-TMPL", 1, "First Name",
                                        fallback_value="")
                    ]
                },
            )

        mock_mark_skipped.assert_called_once_with("LOG-001")
        mock_msg_doc.insert.assert_not_called()


# ── Duplicate parameter index validation ─────────────────────────────────────

class TestDuplicateParamIndexValidation(FrappeTestCase):
    """WhatsAppComplianceSettings.validate() rejects duplicate indexes."""

    def _make_settings_doc(self, param_rows, lang_map_rows=None):
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_compliance_settings\
            .whatsapp_compliance_settings import WhatsAppComplianceSettings
        doc = WhatsAppComplianceSettings.__new__(
            WhatsAppComplianceSettings
        )
        doc.hour_23_template_parameters = param_rows
        doc.hour_23_language_map = lang_map_rows or []
        return doc

    def test_duplicate_index_same_template_raises(self):
        """Two rows for the same template and index → ValidationError."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-A", 1),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError):
            doc.validate()

    def test_same_index_different_templates_allowed(self):
        """Duplicate index across different templates is not an error."""
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-B", 1),
        ])
        doc.validate()  # must not raise

    def test_unique_indexes_same_template_allowed(self):
        """Distinct indexes for the same template pass validation."""
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-A", 2),
            _make_param_row("TMPL-A", 3),
        ])
        doc.validate()  # must not raise

    def test_error_message_names_template_and_index(self):
        """Validation error message identifies the offending template and index."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("CONSENT-TMPL", 2),
            _make_param_row("CONSENT-TMPL", 2),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError) as ctx:
            doc.validate()
        msg = str(ctx.exception)
        self.assertIn("2", msg)
        self.assertIn("CONSENT-TMPL", msg)

    def test_index_zero_raises(self):
        """parameter_index = 0 is not a valid 1-based placeholder."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 0),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError):
            doc.validate()

    def test_negative_index_raises(self):
        """Negative parameter_index is rejected."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", -1),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError):
            doc.validate()

    def test_gapped_mapping_single_row_at_two_raises(self):
        """A single row with index 2 (gap before 1) is rejected."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 2),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError):
            doc.validate()

    def test_gapped_mapping_skips_middle_raises(self):
        """Indexes {1, 3} (missing 2) are rejected as non-contiguous."""
        import frappe
        doc = self._make_settings_doc([
            _make_param_row("TMPL-A", 1),
            _make_param_row("TMPL-A", 3),
        ])
        with self.assertRaises(frappe.exceptions.ValidationError):
            doc.validate()

    def test_mapping_for_zero_param_template_rejected(self):
        """Rows configured for a template with no body params are rejected."""
        import frappe
        from unittest.mock import patch as _patch

        zero_param_template = SimpleNamespace(
            name="STATIC-TMPL",
            field_names=None,
            sample_values=None,
        )
        doc = self._make_settings_doc([
            _make_param_row("STATIC-TMPL", 1),
        ])
        settings_mod = (
            "frappe_whatsapp.frappe_whatsapp.doctype"
            ".whatsapp_compliance_settings"
            ".whatsapp_compliance_settings"
        )
        with _patch(f"{settings_mod}.frappe.get_doc",
                    return_value=zero_param_template):
            with self.assertRaises(frappe.exceptions.ValidationError):
                doc.validate()

    def test_incomplete_mapping_wrong_count_rejected(self):
        """1 row configured but template declares 2 params → rejected."""
        import frappe
        from unittest.mock import patch as _patch

        two_param_template = SimpleNamespace(
            name="TWO-PARAM-TMPL",
            template=None,
            sample_values="first_name,last_name",  # Meta-owned; count = 2
            field_names=None,
        )
        doc = self._make_settings_doc([
            _make_param_row("TWO-PARAM-TMPL", 1),
        ])
        settings_mod = (
            "frappe_whatsapp.frappe_whatsapp.doctype"
            ".whatsapp_compliance_settings"
            ".whatsapp_compliance_settings"
        )
        with _patch(f"{settings_mod}.frappe.get_doc",
                    return_value=two_param_template):
            with self.assertRaises(frappe.exceptions.ValidationError):
                doc.validate()


# ── Language map template param validation ───────────────────────────────────

class TestLanguageMapParamValidation(FrappeTestCase):
    """_validate_hour_23_language_map_templates catches missing mappings."""

    _SETTINGS_MOD = (
        "frappe_whatsapp.frappe_whatsapp.doctype"
        ".whatsapp_compliance_settings"
        ".whatsapp_compliance_settings"
    )

    def _make_settings_doc(self, lang_map_rows, param_rows=None):
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_compliance_settings\
            .whatsapp_compliance_settings import WhatsAppComplianceSettings
        doc = WhatsAppComplianceSettings.__new__(
            WhatsAppComplianceSettings
        )
        doc.hour_23_language_map = lang_map_rows
        doc.hour_23_template_parameters = param_rows or []
        return doc

    def test_parameterized_template_without_mapping_raises(self):
        """Language map references a parameterized template with no rows."""
        import frappe
        from unittest.mock import patch as _patch

        param_template = SimpleNamespace(
            name="CONSENT-TMPL",
            template=None,
            sample_values="first_name",  # Meta-owned; count = 1
            field_names=None,
        )
        lang_rows = [_make_row("en", consent_tmpl="CONSENT-TMPL")]
        doc = self._make_settings_doc(lang_rows, param_rows=[])

        with _patch(f"{self._SETTINGS_MOD}.frappe.get_doc",
                    return_value=param_template):
            with self.assertRaises(frappe.exceptions.ValidationError) as ctx:
                doc.validate()

        self.assertIn("CONSENT-TMPL", str(ctx.exception))

    def test_parameterized_template_with_mapping_passes(self):
        """Language map references a parameterized template that has rows."""
        from unittest.mock import patch as _patch

        param_template = SimpleNamespace(
            name="CONSENT-TMPL",
            template=None,
            sample_values="first_name",  # Meta-owned; count = 1
            field_names=None,
        )
        # Use only consent_template; leave follow-up blank to isolate.
        lang_rows = [
            SimpleNamespace(
                language_code="en",
                consent_template="CONSENT-TMPL",
                status_follow_up_template=None,
            )
        ]
        mapping_rows = [_make_param_row("CONSENT-TMPL", 1)]
        doc = self._make_settings_doc(lang_rows, param_rows=mapping_rows)

        with _patch(f"{self._SETTINGS_MOD}.frappe.get_doc",
                    return_value=param_template):
            doc.validate()  # must not raise

    def test_parameterless_template_without_mapping_passes(self):
        """Language map references a parameterless template; no rows needed."""
        from unittest.mock import patch as _patch

        static_template = SimpleNamespace(
            name="STATIC-TMPL",
            field_names=None,
            sample_values=None,
        )
        lang_rows = [
            SimpleNamespace(
                language_code="en",
                consent_template="STATIC-TMPL",
                status_follow_up_template=None,
            )
        ]
        doc = self._make_settings_doc(lang_rows, param_rows=[])

        with _patch(f"{self._SETTINGS_MOD}.frappe.get_doc",
                    return_value=static_template):
            doc.validate()  # must not raise

    def test_follow_up_template_also_checked(self):
        """status_follow_up_template is checked, not just consent_template."""
        import frappe
        from unittest.mock import patch as _patch

        param_template = SimpleNamespace(
            name="FOLLOWUP-TMPL",
            template=None,
            sample_values="promo_code",  # Meta-owned; count = 1
            field_names=None,
        )
        # consent_template is None; status_follow_up_template is parameterized.
        lang_rows = [_make_row("en",
                               consent_tmpl=None,
                               followup_tmpl="FOLLOWUP-TMPL")]
        doc = self._make_settings_doc(lang_rows, param_rows=[])

        with _patch(f"{self._SETTINGS_MOD}.frappe.get_doc",
                    return_value=param_template):
            with self.assertRaises(frappe.exceptions.ValidationError):
                doc.validate()


# ── Hour-23 drift detection after template sync ──────────────────────────────

_COMPLIANCE_MOD = (
    "frappe_whatsapp.frappe_whatsapp.doctype"
    ".whatsapp_compliance_settings"
    ".whatsapp_compliance_settings"
)


def _make_settings_with_params(
    lang_map_rows,
    param_rows=None,
    enable=1,
):
    """Build a settings SimpleNamespace for drift-detection tests."""
    return SimpleNamespace(
        enable_hour_23_follow_up=enable,
        hour_23_language_map=lang_map_rows,
        hour_23_template_parameters=param_rows or [],
    )


class TestHour23DriftDetection(FrappeTestCase):
    """get_hour_23_drift_messages detects config drift after template sync."""

    _TMPL_MOD = (
        "frappe_whatsapp.frappe_whatsapp.doctype"
        ".whatsapp_templates.whatsapp_templates"
    )

    def _drift(self, settings, template_name, declared_params):
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_compliance_settings\
            .whatsapp_compliance_settings import get_hour_23_drift_messages
        with patch(
            f"{_COMPLIANCE_MOD}.frappe.get_cached_doc",
            return_value=settings,
        ):
            return get_hour_23_drift_messages(template_name, declared_params)

    # ── core drift-logic tests (use doc name as the key) ──────────────────

    def test_template_absent_then_synced_as_parameterized_triggers_drift(self):
        """Settings saved while template absent; sync adds 1-param template.

        The lookup key is the WhatsApp Templates document name
        (e.g. CONSENT-TMPL-en), not actual_name.
        """
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
        )
        # Bare actual_name is NOT the lookup key → no drift.
        msgs = self._drift(settings, "CONSENT-TMPL", 1)
        self.assertEqual(msgs, [])

        # Suffixed doc name matches the language-map entry → drift detected.
        msgs = self._drift(settings, "CONSENT-TMPL-en", 1)
        self.assertEqual(len(msgs), 1)
        self.assertIn("CONSENT-TMPL-en", msgs[0])
        self.assertIn("no parameter mapping", msgs[0])

    def test_param_count_change_from_one_to_two_triggers_drift(self):
        """Template had 1 param (1 mapping row); sync changes it to 2."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[_make_param_row("CONSENT-TMPL-en", 1)],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 2)
        self.assertEqual(len(msgs), 1)
        self.assertIn("CONSENT-TMPL-en", msgs[0])
        self.assertIn("2", msgs[0])
        self.assertIn("1", msgs[0])

    def test_parameterless_becomes_parameterized_triggers_drift(self):
        """Template had 0 params (no rows); sync adds a body placeholder."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 1)
        self.assertEqual(len(msgs), 1)
        self.assertIn("no parameter mapping", msgs[0])

    def test_valid_parameterized_sync_with_matching_mapping_is_clean(self):
        """Template declares 2 params and 2 matching rows exist → no drift."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[
                _make_param_row("CONSENT-TMPL-en", 1),
                _make_param_row("CONSENT-TMPL-en", 2),
            ],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 2)
        self.assertEqual(msgs, [])

    def test_parameterless_template_without_rows_is_clean(self):
        """Parameterless template (0 params, 0 rows) → no drift."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 0)
        self.assertEqual(msgs, [])

    def test_template_not_in_language_map_is_ignored(self):
        """Templates not referenced by the language map produce no drift."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "OTHER-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 1)
        self.assertEqual(msgs, [])

    def test_feature_flag_off_returns_empty(self):
        """enable_hour_23_follow_up=0 → no drift messages regardless."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
            enable=0,
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 1)
        self.assertEqual(msgs, [])

    def test_stale_rows_after_template_loses_params_triggers_drift(self):
        """Template lost all body params; stale mapping rows still configured."""
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[_make_param_row("CONSENT-TMPL-en", 1)],
        )
        msgs = self._drift(settings, "CONSENT-TMPL-en", 0)
        self.assertEqual(len(msgs), 1)
        self.assertIn("stale", msgs[0])

    # ── _check_hour_23_drift_after_sync uses doc.name, not actual_name ────

    def test_sync_helper_uses_doc_name_not_actual_name(self):
        """doc.name (suffixed) is passed to get_hour_23_drift_messages."""
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates\
            .whatsapp_templates import _check_hour_23_drift_after_sync

        template_doc = SimpleNamespace(
            name="CONSENT-TMPL-en",        # final document name after upsert
            actual_name="CONSENT-TMPL",    # bare Meta name — must NOT be used
            template_name="CONSENT-TMPL",
            template="Hello {{1}}!",       # body text — declared param count = 1
            sample_values=None,
            field_names=None,
        )
        mock_drift = MagicMock(return_value=[])

        with patch(f"{_COMPLIANCE_MOD}.get_hour_23_drift_messages", mock_drift):
            _check_hour_23_drift_after_sync(template_doc)

        # First positional arg must be the doc name, not actual_name.
        mock_drift.assert_called_once()
        called_name = mock_drift.call_args[0][0]
        self.assertEqual(called_name, "CONSENT-TMPL-en")
        self.assertNotEqual(called_name, "CONSENT-TMPL")

    def test_drift_detected_via_sync_helper_logs_error(self):
        """_check_hour_23_drift_after_sync logs when drift is detected."""
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates\
            .whatsapp_templates import _check_hour_23_drift_after_sync

        template_doc = SimpleNamespace(
            name="CONSENT-TMPL-en",
            actual_name="CONSENT-TMPL",
            template_name="CONSENT-TMPL",
            template="Hello {{1}}!",   # body text — declared param count = 1
            sample_values=None,
            field_names=None,
        )
        mock_log = MagicMock()

        with (
            patch(
                f"{_COMPLIANCE_MOD}.get_hour_23_drift_messages",
                return_value=["drift warning message"],
            ),
            patch(f"{self._TMPL_MOD}.frappe.log_error", mock_log),
        ):
            _check_hour_23_drift_after_sync(template_doc)

        mock_log.assert_called_once()
        self.assertIn("drift warning message", mock_log.call_args[0][0])

    def test_no_drift_does_not_log(self):
        """_check_hour_23_drift_after_sync does not log when drift is empty."""
        from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_templates\
            .whatsapp_templates import _check_hour_23_drift_after_sync

        template_doc = SimpleNamespace(
            name="CONSENT-TMPL-en",
            actual_name="CONSENT-TMPL",
            template_name="CONSENT-TMPL",
            field_names=None,
            sample_values=None,
        )
        mock_log = MagicMock()

        with (
            patch(
                f"{_COMPLIANCE_MOD}.get_hour_23_drift_messages",
                return_value=[],
            ),
            patch(f"{self._TMPL_MOD}.frappe.log_error", mock_log),
        ):
            _check_hour_23_drift_after_sync(template_doc)

        mock_log.assert_not_called()

    def test_actual_name_alone_does_not_trigger_drift(self):
        """Passing actual_name (unsuffixed) when map uses doc name → no drift.

        This proves that using doc.name (not actual_name) is the correct key.
        """
        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "CONSENT-TMPL-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[],
        )
        # "CONSENT-TMPL" is the actual_name; the settings store "CONSENT-TMPL-en".
        msgs = self._drift(settings, "CONSENT-TMPL", 1)
        self.assertEqual(msgs, [])


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


# ── count_declared_meta_params ───────────────────────────────────────────────

class TestCountDeclaredMetaParams(FrappeTestCase):
    """count_declared_meta_params uses body text / sample_values; ignores
    field_names.
    """

    def _count(self, body=None, sample_values=None, field_names=None):
        from frappe_whatsapp.utils.hour_23_params import count_declared_meta_params
        t = SimpleNamespace(
            template=body,
            sample_values=sample_values,
            field_names=field_names,
        )
        return count_declared_meta_params(t)

    # ── body-text path ────────────────────────────────────────────────────────

    def test_one_placeholder_in_body(self):
        self.assertEqual(self._count(body="Hello {{1}}!"), 1)

    def test_two_placeholders_in_body(self):
        self.assertEqual(self._count(body="Hi {{1}}, your code is {{2}}."), 2)

    def test_duplicate_placeholder_counted_once(self):
        self.assertEqual(self._count(body="{{1}} and again {{1}}"), 1)

    def test_body_no_placeholders_returns_zero(self):
        self.assertEqual(self._count(body="Hello! No params."), 0)

    def test_body_empty_string_falls_through(self):
        # empty body → fall through to sample_values
        self.assertEqual(self._count(body="", sample_values="a,b"), 2)

    # ── field_names is never read ─────────────────────────────────────────────

    def test_field_names_ignored_when_body_is_parameterless(self):
        """Stale field_names must not be treated as declared params."""
        # body has no placeholders, field_names is set (stale), sample_values clear
        result = self._count(body="Hello!", field_names="first_name")
        self.assertEqual(result, 0,
                         "field_names must not be counted as declared params")

    def test_field_names_ignored_when_body_absent(self):
        """No body text, no sample_values, but field_names set → 0."""
        result = self._count(body=None, sample_values=None,
                             field_names="first_name")
        self.assertEqual(result, 0)

    def test_field_names_does_not_override_zero_body(self):
        """body present with no placeholders wins over field_names."""
        result = self._count(body="Plain text.", field_names="a,b,c")
        self.assertEqual(result, 0)

    # ── sample_values fallback ────────────────────────────────────────────────

    def test_sample_values_used_when_no_body(self):
        self.assertEqual(self._count(sample_values="Alice,Bob"), 2)

    def test_all_absent_returns_zero(self):
        self.assertEqual(self._count(), 0)

    # ── regression: backward-compat alias ────────────────────────────────────

    def test_count_template_params_alias_still_works(self):
        """count_template_params is an alias for count_declared_meta_params."""
        from frappe_whatsapp.utils.hour_23_params import count_template_params
        t = SimpleNamespace(template="Hello {{1}}!", sample_values=None,
                            field_names=None)
        self.assertEqual(count_template_params(t), 1)


# ── field_names isolation (drift / validation / shape) ───────────────────────

class TestFieldNamesIgnored(FrappeTestCase):
    """Stale field_names must not affect drift detection, settings validation,
    or hour-23 runtime shape checks when the current Meta body declares no
    placeholders.
    """

    _SETTINGS_MOD = (
        "frappe_whatsapp.frappe_whatsapp.doctype"
        ".whatsapp_compliance_settings"
        ".whatsapp_compliance_settings"
    )

    def _make_parameterless_doc(self, field_names="first_name"):
        """Template whose body text has no {{n}} but still has stale
        field_names set.  sample_values was cleared by the sync pre-reset.
        """
        return SimpleNamespace(
            name="info_tmpl-en",
            actual_name="info_tmpl",
            template="Hello! This message has no placeholders.",
            sample_values=None,          # cleared by sync pre-reset
            field_names=field_names,     # stale manual field
            field_names_raw=field_names,
        )

    # ── 1. count_declared_meta_params is 0 despite stale field_names ─────────

    def test_parameterless_body_with_stale_field_names_counts_zero(self):
        from frappe_whatsapp.utils.hour_23_params import count_declared_meta_params
        doc = self._make_parameterless_doc()
        self.assertEqual(count_declared_meta_params(doc), 0)

    # ── 2. drift detection: stale mapping rows are flagged, not missing rows ──

    def test_drift_detects_stale_rows_not_missing_rows_with_field_names(self):
        """Template had params, sync cleared sample_values → body now has
        no {{n}}.  Stale mapping rows in settings → drift reports "stale
        rows remain", NOT "missing mapping rows".

        field_names still set → must not change this result.
        """
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_compliance_settings\
            .whatsapp_compliance_settings import get_hour_23_drift_messages

        settings = _make_settings_with_params(
            lang_map_rows=[
                _make_row("en", "info_tmpl-en", "FOLLOWUP-TMPL-en")
            ],
            param_rows=[_make_param_row("info_tmpl-en", 1)],
        )
        with patch(
            f"{_COMPLIANCE_MOD}.frappe.get_cached_doc",
            return_value=settings,
        ):
            # declared_params=0 because body has no {{n}} and sample_values=None
            msgs = get_hour_23_drift_messages("info_tmpl-en", 0)

        self.assertEqual(len(msgs), 1)
        self.assertIn("stale", msgs[0].lower(),
                      "drift message must say 'stale', not 'missing mapping'")

    # ── 3. settings validation: no rows required for parameterless template ───

    def test_settings_validation_does_not_require_rows_for_stale_field_names(self):
        """WhatsAppComplianceSettings.validate() must not throw when the
        language map references a template that has no body params
        (body text has no {{n}}) even though field_names is set.
        """
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_compliance_settings\
            .whatsapp_compliance_settings import WhatsAppComplianceSettings

        # Template with stale field_names but parameterless body
        static_doc = self._make_parameterless_doc()

        doc = WhatsAppComplianceSettings.__new__(WhatsAppComplianceSettings)
        doc.hour_23_template_parameters = []
        doc.hour_23_language_map = [
            SimpleNamespace(
                language_code="en",
                consent_template="info_tmpl-en",
                status_follow_up_template=None,
            )
        ]

        with patch(
            f"{self._SETTINGS_MOD}.frappe.get_doc",
            return_value=static_doc,
        ):
            doc.validate()   # must NOT raise

    # ── 4. runtime shape check does not reject stale-field_names template ─────

    def test_shape_check_does_not_reject_parameterless_template_with_field_names(self):
        """_check_template_shape must return None (= no error) for a template
        whose body has no {{n}} placeholders, even when field_names is set.
        """
        from frappe_whatsapp.utils.hour_23_automation import _check_template_shape

        template = SimpleNamespace(
            name="info_tmpl-en",
            status="APPROVED",
            is_consent_request=1,
            template="Hello! This message has no placeholders.",
            sample_values=None,
            field_names="first_name",   # stale — must NOT trigger a shape error
            header_type=None,
            buttons=[],
        )

        result = _check_template_shape(
            template,
            template_name="info_tmpl-en",
            automation_type="consent_request",
            has_body_param_mapping=False,
        )

        self.assertIsNone(result,
                          "stale field_names must not cause a shape error "
                          "when body text has no {{n}} placeholders")

    # ── 5. regression: genuine {{n}} template still counted correctly ─────────

    def test_genuine_placeholder_still_counted_for_shape_check(self):
        """A template with {{1}} in the body is correctly identified as
        requiring a body-param mapping.
        """
        from frappe_whatsapp.utils.hour_23_automation import _check_template_shape

        template = SimpleNamespace(
            name="param_tmpl-en",
            status="APPROVED",
            is_consent_request=1,
            template="Hello {{1}}!",
            sample_values=None,
            field_names=None,
            header_type=None,
            buttons=[],
        )

        # Without mapping → shape check must flag this as needing params
        result_no_mapping = _check_template_shape(
            template,
            template_name="param_tmpl-en",
            automation_type="consent_request",
            has_body_param_mapping=False,
        )
        self.assertIsNotNone(result_no_mapping,
                             "{{1}} in body text must trigger missing-mapping error")

        # With mapping → shape check must pass
        result_with_mapping = _check_template_shape(
            template,
            template_name="param_tmpl-en",
            automation_type="consent_request",
            has_body_param_mapping=True,
        )
        self.assertIsNone(result_with_mapping,
                          "{{1}} in body with mapping configured must pass shape check")


# ── Sync component pre-reset ─────────────────────────────────────────────────

_TMPL_SYNC_MOD = (
    "frappe_whatsapp.frappe_whatsapp.doctype"
    ".whatsapp_templates.whatsapp_templates"
)


class _StaleDoc:
    """Minimal doc stub that supports Frappe-style field access.

    Supports ``doc.set(field, value)``, ``doc.get(field)``, and
    ``doc.append(field, row)`` so the component-loop code in ``fetch()``
    can manipulate the object just like a real Frappe Document.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            object.__setattr__(self, k, v)

    def set(self, field, value):
        object.__setattr__(self, field, value)

    def get(self, field):
        return getattr(self, field, None)

    def append(self, field, row):
        lst = getattr(self, field, None)
        if lst is None:
            object.__setattr__(self, field, [])
            lst = getattr(self, field)
        lst.append(row)


class TestSyncComponentReset(FrappeTestCase):
    """fetch() pre-resets component-owned fields before rebuilding from Meta.

    Strategy: mock all external calls; capture the doc state at the moment
    ``upsert_doc_without_hooks`` is called so we can assert on the fields that
    were set (or cleared) by the component loop.
    """

    def _run_fetch(self, existing_doc, components):
        """Run fetch() with *existing_doc* as the stored template and the
        given Meta *components* list.  Returns the doc as it was when
        ``upsert_doc_without_hooks`` was called (i.e. after the component loop
        but before the DB write).
        """
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_templates.whatsapp_templates import fetch

        # Fake WhatsApp Account — only needs ``get_password`` callable
        # and simple data fields.
        fake_account = SimpleNamespace(
            name="WA-001",
            url="https://graph.facebook.com",
            version="v16.0",
            business_id="BIZ-001",
            get_password=MagicMock(return_value="fake-token"),
        )

        meta_response = {
            "data": [{
                "name": existing_doc.actual_name,
                "language": "en",
                "status": "APPROVED",
                "category": "MARKETING",
                "id": "tmpl-id-1",
                "components": components,
            }]
        }

        captured = []

        def _fake_upsert(doc, child_dt, child_field):
            captured.append(doc)

        def _fake_get_doc(doctype, name=None):
            if doctype == "WhatsApp Account":
                return fake_account
            return existing_doc

        with (
            patch(f"{_TMPL_SYNC_MOD}.frappe.get_all",
                  return_value=[{
                      "name": "WA-001",
                      "url": "https://graph.facebook.com",
                      "version": "v16.0",
                      "business_id": "BIZ-001",
                  }]),
            patch(f"{_TMPL_SYNC_MOD}.frappe.get_doc",
                  side_effect=_fake_get_doc),
            patch(f"{_TMPL_SYNC_MOD}.frappe.db.get_value",
                  return_value=existing_doc.name),
            patch(f"{_TMPL_SYNC_MOD}.make_request",
                  return_value=meta_response),
            patch(f"{_TMPL_SYNC_MOD}.frappe.db.delete"),
            patch(f"{_TMPL_SYNC_MOD}._derive_sync_compliance"),
            patch(f"{_TMPL_SYNC_MOD}.upsert_doc_without_hooks",
                  side_effect=_fake_upsert),
            patch(f"{_TMPL_SYNC_MOD}._check_hour_23_drift_after_sync"),
        ):
            fetch()

        self.assertEqual(len(captured), 1,
                         "upsert_doc_without_hooks must be called exactly once")
        return captured[0]

    # ── Test 1: sample_values cleared when BODY loses params ─────────────────

    def test_sample_values_cleared_when_body_loses_params(self):
        """Existing template had sample_values; new BODY has no examples.

        After pre-reset ``sample_values`` must be ``None``, which makes
        ``count_template_params`` return 0 — correct input for
        drift detection to flag stale mapping rows.
        """
        from frappe_whatsapp.utils.hour_23_params import count_template_params

        existing_doc = _StaleDoc(
            name="consent_tmpl-en",
            actual_name="consent_tmpl",
            sample_values="first_name",   # stale — had one param before
            field_names=None,
            footer=None,
            header_type="",
            header=None,
            buttons=[],
        )

        # New Meta payload: BODY with no example (parameterless)
        components = [{"type": "BODY", "text": "Hello! No params here."}]

        doc = self._run_fetch(existing_doc, components)

        self.assertIsNone(doc.sample_values,
                          "sample_values must be cleared when BODY has no examples")
        self.assertEqual(count_template_params(doc), 0,
                         "count_template_params must return 0 after pre-reset")

    # ── Test 2: buttons cleared when BUTTONS component is removed ────────────

    def test_buttons_cleared_when_meta_removes_buttons_component(self):
        """Existing template had a dynamic URL button; new payload has none.

        After pre-reset ``doc.buttons`` must be empty — no stale button rows
        are passed to ``upsert_doc_without_hooks``.
        """
        stale_button = SimpleNamespace(
            button_type="Visit Website",
            url_type="Dynamic",
            website_url="{{url}}",
            button_label="Open",
        )
        existing_doc = _StaleDoc(
            name="promo_tmpl-en",
            actual_name="promo_tmpl",
            sample_values=None,
            field_names=None,
            footer=None,
            header_type="",
            header=None,
            buttons=[stale_button],   # stale dynamic-URL button
        )

        # New Meta payload: no BUTTONS component
        components = [{"type": "BODY", "text": "Promo without button."}]

        doc = self._run_fetch(existing_doc, components)

        self.assertEqual(doc.buttons, [],
                         "buttons must be empty after pre-reset when Meta "
                         "payload has no BUTTONS component")

    # ── Test 3: footer and header cleared when components removed ────────────

    def test_footer_and_header_cleared_when_removed_from_payload(self):
        """Existing template had footer and text header; new payload omits them.

        After pre-reset both ``doc.footer`` and ``doc.header`` must be
        ``None``, and ``doc.header_type`` must be ``""`` so downstream
        logic (compliance derivation, shape validation) does not see stale
        metadata.
        """
        existing_doc = _StaleDoc(
            name="info_tmpl-en",
            actual_name="info_tmpl",
            sample_values=None,
            field_names=None,
            footer="Reply STOP to unsubscribe",   # stale
            header_type="TEXT",                    # stale
            header="Important Update",             # stale
            buttons=[],
        )

        # New Meta payload: plain BODY only — no HEADER, no FOOTER
        components = [{"type": "BODY", "text": "Simple body text."}]

        doc = self._run_fetch(existing_doc, components)

        self.assertIsNone(doc.footer,
                          "footer must be None after pre-reset")
        self.assertEqual(doc.header_type, "",
                         "header_type must be '' after pre-reset")
        self.assertIsNone(doc.header,
                          "header must be None after pre-reset")

    # ── Test 4: regression — parameterized template preserves new values ─────

    def test_parameterized_template_sync_preserves_sample_values(self):
        """Happy path: BODY still includes examples → sample_values is set.

        Pre-reset clears ``sample_values`` first, but the BODY component
        loop then assigns the new value.  The net result must match the
        new Meta payload, not the stale value.
        """
        from frappe_whatsapp.utils.hour_23_params import count_template_params

        existing_doc = _StaleDoc(
            name="consent_tmpl-en",
            actual_name="consent_tmpl",
            sample_values="old_first_name",   # stale — will be replaced
            field_names=None,
            footer=None,
            header_type="",
            header=None,
            buttons=[],
        )

        # New Meta payload: BODY with a fresh example (one param)
        components = [{
            "type": "BODY",
            "text": "Hello {{1}}!",
            "example": {"body_text": [["Alice"]]},
        }]

        doc = self._run_fetch(existing_doc, components)

        self.assertEqual(doc.sample_values, "Alice",
                         "sample_values must reflect the new Meta payload "
                         "even after pre-reset cleared the stale value")
        self.assertEqual(count_template_params(doc), 1,
                         "count_template_params must return 1 for the fresh param")


# ── send_template() body-parameter emission ──────────────────────────────────

_MSG_MOD = (
    "frappe_whatsapp.frappe_whatsapp.doctype"
    ".whatsapp_message.whatsapp_message"
)


class TestSendTemplateBodyParam(FrappeTestCase):
    """send_template() body-parameter emission for hour-23 sends.

    For hour-23 parameterized sends, body_param is computed ahead of time
    by build_hour_23_body_params() and set on the message document.
    body_param is the authoritative trigger: it is sufficient to emit the
    body-parameters component in the outbound Meta payload, regardless of
    whether sample_values is set.
    """

    def _make_msg_doc(self, body_param=None, reference_doctype=None,
                      reference_name=None):
        """Minimal WhatsApp Message mock with just enough attributes."""
        from frappe_whatsapp.frappe_whatsapp.doctype\
            .whatsapp_message.whatsapp_message import WhatsAppMessage

        doc = WhatsAppMessage.__new__(WhatsAppMessage)
        doc.template = "TMPL-en"
        doc.to = "+1234567890"
        doc.within_conversation_window = 0
        doc.body_param = body_param
        doc.reference_doctype = reference_doctype
        doc.reference_name = reference_name
        doc.template_parameters = None
        doc.flags = MagicMock()
        doc.flags.custom_ref_doc = None
        return doc

    def _run_send_template(self, msg_doc, template_doc):
        """Call send_template() and return the data dict passed to notify()."""
        captured = []

        def _fake_notify(data):
            captured.append(data)

        msg_doc.notify = _fake_notify

        with (
            patch(f"{_MSG_MOD}.frappe.get_doc", return_value=template_doc),
            patch(f"{_MSG_MOD}.enforce_marketing_template_compliance"),
            patch(f"{_MSG_MOD}.enforce_template_send_rules"),
            patch(f"{_MSG_MOD}.format_number", return_value="1234567890"),
        ):
            msg_doc.send_template()

        self.assertEqual(len(captured), 1)
        return captured[0]

    def _body_components(self, data):
        """Return the body-type components from the outgoing payload."""
        components = data["template"]["components"]
        return [c for c in components if c.get("type") == "body"]

    # ── Test 1: body_param set, sample_values blank → body component emitted ─

    def test_body_param_without_sample_values_emits_body_component(self):
        """Template has {{1}} in body, sample_values cleared by sync.
        body_param is pre-built by hour-23 automation.
        send_template() must still emit the body component.
        """
        template_doc = SimpleNamespace(
            name="TMPL-en",
            actual_name="TMPL",
            template_name="TMPL",
            language_code="en",
            template="Hello {{1}}!",   # declared placeholder
            sample_values=None,        # cleared by sync pre-reset
            field_names=None,
            header_type=None,
            sample=None,
            buttons=[],
        )
        msg_doc = self._make_msg_doc(body_param='{"1": "Alice"}')

        data = self._run_send_template(msg_doc, template_doc)

        body_comps = self._body_components(data)
        self.assertEqual(len(body_comps), 1,
                         "exactly one body component must be in the payload")
        params = body_comps[0]["parameters"]
        self.assertEqual(len(params), 1)
        self.assertEqual(params[0]["text"], "Alice")

    # ── Test 2: two body params via body_param, sample_values blank ───────────

    def test_two_body_params_via_body_param_without_sample_values(self):
        """Multi-param template: body_param has two values, sample_values=None."""
        template_doc = SimpleNamespace(
            name="TMPL-en",
            actual_name="TMPL",
            template_name="TMPL",
            language_code="en",
            template="Hi {{1}}, your code is {{2}}.",
            sample_values=None,
            field_names=None,
            header_type=None,
            sample=None,
            buttons=[],
        )
        msg_doc = self._make_msg_doc(body_param='{"1": "Bob", "2": "XY99"}')

        data = self._run_send_template(msg_doc, template_doc)

        body_comps = self._body_components(data)
        self.assertEqual(len(body_comps), 1)
        params = body_comps[0]["parameters"]
        self.assertEqual(len(params), 2)
        self.assertEqual(params[0]["text"], "Bob")
        self.assertEqual(params[1]["text"], "XY99")

    # ── Test 3: parameterless template → no body component ───────────────────

    def test_parameterless_template_sends_no_body_component(self):
        """Parameterless template with body_param=None and sample_values=None
        must produce no body component in the outgoing payload.
        """
        template_doc = SimpleNamespace(
            name="STATIC-TMPL-en",
            actual_name="STATIC-TMPL",
            template_name="STATIC-TMPL",
            language_code="en",
            template="Hello! This is a static message.",
            sample_values=None,
            field_names=None,
            header_type=None,
            sample=None,
            buttons=[],
        )
        msg_doc = self._make_msg_doc(body_param=None)

        data = self._run_send_template(msg_doc, template_doc)

        body_comps = self._body_components(data)
        self.assertEqual(body_comps, [],
                         "no body component must appear for a parameterless template")

    # ── Test 4: sample_values set → body component emitted ───────────────────

    def test_legacy_sample_values_with_reference_doc_still_works(self):
        """sample_values set: ref-doc path resolves params and emits the
        body component.  Confirms the gate condition covers this case.
        """
        template_doc = SimpleNamespace(
            name="LEGACY-TMPL-en",
            actual_name="LEGACY-TMPL",
            template_name="LEGACY-TMPL",
            language_code="en",
            template="Hello {{1}}!",
            sample_values="first_name",   # legacy: drives the field lookup
            field_names=None,
            header_type=None,
            sample=None,
            buttons=[],
        )
        msg_doc = self._make_msg_doc(
            body_param=None,
            reference_doctype="Contact",
            reference_name="CONT-001",
        )

        mock_ref_doc = MagicMock()
        mock_ref_doc.get_formatted = MagicMock(return_value="Charlie")

        # Dispatch: first call returns template, subsequent calls return ref_doc.
        def _get_doc_side_effect(doctype, name=None):
            if doctype == "WhatsApp Templates":
                return template_doc
            return mock_ref_doc

        captured = []

        def _fake_notify(data):
            captured.append(data)

        msg_doc.notify = _fake_notify

        with (
            patch(f"{_MSG_MOD}.frappe.get_doc",
                  side_effect=_get_doc_side_effect),
            patch(f"{_MSG_MOD}.enforce_marketing_template_compliance"),
            patch(f"{_MSG_MOD}.enforce_template_send_rules"),
            patch(f"{_MSG_MOD}.format_number", return_value="1234567890"),
        ):
            msg_doc.send_template()

        self.assertEqual(len(captured), 1)
        data = captured[0]

        body_comps = self._body_components(data)
        self.assertEqual(len(body_comps), 1,
                         "body component must be present when sample_values is set")
        params = body_comps[0]["parameters"]
        self.assertEqual(len(params), 1)
        self.assertEqual(params[0]["text"], "Charlie")

    # ── Test 5: neither trigger set → no body component ──────────────────────

    def test_no_body_param_no_sample_values_no_body_component(self):
        """body_param=None and sample_values=None: gate is False,
        no body component emitted, no crash.
        """
        template_doc = SimpleNamespace(
            name="BARE-TMPL-en",
            actual_name="BARE-TMPL",
            template_name="BARE-TMPL",
            language_code="en",
            template="Plain text, no placeholders.",
            sample_values=None,
            field_names=None,
            header_type=None,
            sample=None,
            buttons=[],
        )
        msg_doc = self._make_msg_doc(body_param=None)

        data = self._run_send_template(msg_doc, template_doc)

        self.assertEqual(self._body_components(data), [])
