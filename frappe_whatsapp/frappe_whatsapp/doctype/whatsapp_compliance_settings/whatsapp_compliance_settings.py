# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.model.document import Document


class WhatsAppComplianceSettings(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF
		from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_hour_23_language_map.whatsapp_hour_23_language_map import WhatsAppHour23LanguageMap
		from frappe_whatsapp.frappe_whatsapp.doctype.whatsapp_hour_23_template_parameter.whatsapp_hour_23_template_parameter import WhatsAppHour23TemplateParameter

		allow_transactional_without_consent: DF.Check
		consent_check_mode: DF.Literal["Strict", "Warning Only", "Disabled"]
		consent_request_template_prefixes: DF.SmallText | None
		default_unsubscribe_text: DF.SmallText | None
		enable_hour_23_follow_up: DF.Check
		enable_opt_in_detection: DF.Check
		enable_opt_out_detection: DF.Check
		enforce_24_hour_window: DF.Check
		enforce_consent_check: DF.Check
		hour_23_language_map: DF.Table[WhatsAppHour23LanguageMap]
		hour_23_template_parameters: DF.Table[WhatsAppHour23TemplateParameter]
		include_unsubscribe_in_marketing: DF.Check
		marketing_consent_category: DF.Link | None
		opt_in_confirmation_message: DF.SmallText | None
		opt_in_keywords: DF.SmallText | None
		opt_out_confirmation_message: DF.SmallText | None
		opt_out_confirmation_template: DF.Link | None
		privacy_policy_url: DF.Data | None
		send_opt_in_confirmation: DF.Check
		send_opt_out_confirmation: DF.Check
		terms_of_service_url: DF.Data | None
		window_hours: DF.Int
	# end: auto-generated types

	def validate(self):
		self._validate_hour_23_param_indexes()
		self._validate_hour_23_language_map_templates()

	def _validate_hour_23_param_indexes(self):
		"""Validate parameter_index values for the hour-23 template mapping.

		Checks performed per template (in order; first failure throws):
		  1. Each index is a positive integer (>= 1).
		  2. No duplicate indexes for the same template.
		  3. Indexes form a contiguous sequence 1, 2, … N (no gaps,
		     no sequence that starts above 1).
		  4. N matches the template's declared body-parameter count.
		     Rows for a template with zero body params are also rejected.
		     (Skipped when the WhatsApp Templates document does not exist
		     yet — e.g. during initial data migration.)
		"""
		from frappe_whatsapp.utils.hour_23_params import count_declared_meta_params

		rows = self.hour_23_template_parameters or []

		# Pass 1 — coerce indexes and group by template.
		by_template: dict[str, list[int]] = {}
		for row in rows:
			tmpl = (getattr(row, "template", None) or "").strip()
			if not tmpl:
				continue
			try:
				idx = int(getattr(row, "parameter_index", 0) or 0)
			except (TypeError, ValueError):
				frappe.throw(
					_(
						"Hour-23 Template Parameter Mapping: "
						"parameter index for template \"{0}\" "
						"is not a valid integer."
					).format(tmpl)
				)
				return  # unreachable; keeps type-checkers happy
			by_template.setdefault(tmpl, []).append(idx)

		# Pass 2 — per-template structural checks.
		for tmpl, indexes in by_template.items():
			sorted_idx = sorted(indexes)

			# (1) All indexes >= 1
			if sorted_idx[0] < 1:
				frappe.throw(
					_(
						"Hour-23 Template Parameter Mapping: "
						"parameter index {0} for template \"{1}\" "
						"is invalid. Indexes must be 1 or greater."
					).format(sorted_idx[0], tmpl)
				)

			# (2) No duplicates
			seen: set[int] = set()
			for idx in sorted_idx:
				if idx in seen:
					frappe.throw(
						_(
							"Hour-23 Template Parameter Mapping: duplicate "
							"parameter index {0} for template \"{1}\". "
							"Each index must appear at most once per template."
						).format(idx, tmpl)
					)
				seen.add(idx)

			# (3) Contiguous 1..N
			n = len(sorted_idx)
			if sorted_idx != list(range(1, n + 1)):
				frappe.throw(
					_(
						"Hour-23 Template Parameter Mapping: indexes for "
						"template \"{0}\" are not a contiguous sequence "
						"starting at 1 (found: {1}). "
						"Configure exactly {{1}}, {{2}}, \u2026 up to {{N}}."
					).format(tmpl, sorted_idx)
				)

			# (4) Count matches the template's declared body-param count.
			try:
				tmpl_doc = frappe.get_doc("WhatsApp Templates", tmpl)
			except frappe.exceptions.DoesNotExistError:
				continue  # Template not yet in DB — skip count check.
			declared = count_declared_meta_params(tmpl_doc)
			if declared == 0:
				frappe.throw(
					_(
						"Hour-23 Template Parameter Mapping: "
						"template \"{0}\" has no body parameters but "
						"{1} mapping row(s) are configured."
					).format(tmpl, n)
				)
			if n != declared:
				frappe.throw(
					_(
						"Hour-23 Template Parameter Mapping: "
						"template \"{0}\" declares {1} body parameter(s) "
						"but {2} mapping row(s) are configured. "
						"Provide exactly {1} row(s)."
					).format(tmpl, declared, n)
				)

	def _validate_hour_23_language_map_templates(self):
		"""Ensure every parameterized template in the language map has rows.

		Walks ``consent_template`` and ``status_follow_up_template`` on
		every language-map row.  If a template declares body parameters it
		must already have a complete mapping in
		``hour_23_template_parameters``; forgetting to add those rows is
		only caught at send time otherwise.

		Parameterless templates and unknown templates (not yet synced from
		Meta) are always accepted without requiring mapping rows.
		"""
		from frappe_whatsapp.utils.hour_23_params import count_declared_meta_params

		# Build the set of templates that already have mapping rows.
		mapped: set[str] = {
			(getattr(row, "template", None) or "").strip()
			for row in (
				getattr(self, "hour_23_template_parameters", None) or []
			)
			if (getattr(row, "template", None) or "").strip()
		}

		lang_rows = getattr(self, "hour_23_language_map", None) or []
		for lang_row in lang_rows:
			for field in ("consent_template", "status_follow_up_template"):
				tmpl = (getattr(lang_row, field, None) or "").strip()
				if not tmpl:
					continue
				try:
					tmpl_doc = frappe.get_doc("WhatsApp Templates", tmpl)
				except frappe.exceptions.DoesNotExistError:
					continue  # Not in DB yet — skip.
				if count_declared_meta_params(tmpl_doc) > 0 and tmpl not in mapped:
					frappe.throw(
						_(
							"Hour-23 Language Map: template \"{0}\" declares "
							"body parameters but has no parameter mapping in "
							"Hour-23 Template Parameter Mapping. "
							"Add a complete mapping or use a parameterless "
							"template."
						).format(tmpl)
					)


def get_hour_23_drift_messages(
	template_name: str,
	declared_params: int,
) -> list[str]:
	"""Return drift-warning strings for *template_name* after a Meta sync.

	Called from the template-sync path after a template's body-parameter
	count has been written to the DB (which bypasses normal document hooks
	and therefore bypasses ``WhatsAppComplianceSettings.validate()``).
	Returns an empty list when there are no issues.

	Checks (only for templates referenced by ``hour_23_language_map``):
	  1. Parameterized template with no mapping rows at all.
	  2. Param count changed — mapping row count no longer matches.
	  3. Template lost all its params — stale mapping rows remain.

	Does nothing when ``enable_hour_23_follow_up`` is off or when the
	template is not referenced by the language map.
	"""
	settings = frappe.get_cached_doc("WhatsApp Compliance Settings")
	if not getattr(settings, "enable_hour_23_follow_up", 0):
		return []

	# Is this template referenced anywhere in the language map?
	lang_rows = getattr(settings, "hour_23_language_map", None) or []
	referenced = any(
		(getattr(row, field, None) or "") == template_name
		for row in lang_rows
		for field in ("consent_template", "status_follow_up_template")
	)
	if not referenced:
		return []

	# Count configured mapping rows for this specific template.
	param_rows = getattr(settings, "hour_23_template_parameters", None) or []
	mapped_count = sum(
		1 for row in param_rows
		if (getattr(row, "template", None) or "").strip() == template_name
	)

	messages: list[str] = []

	if declared_params > 0 and mapped_count == 0:
		messages.append(_(
			"Hour-23 configuration drift: template \"{0}\" now declares "
			"{1} body parameter(s) but has no parameter mapping in "
			"Hour-23 Template Parameter Mapping. "
			"Update WhatsApp Compliance Settings."
		).format(template_name, declared_params))

	elif declared_params > 0 and mapped_count != declared_params:
		messages.append(_(
			"Hour-23 configuration drift: template \"{0}\" now declares "
			"{1} body parameter(s) but {2} mapping row(s) are configured. "
			"Update Hour-23 Template Parameter Mapping in "
			"WhatsApp Compliance Settings."
		).format(template_name, declared_params, mapped_count))

	elif declared_params == 0 and mapped_count > 0:
		messages.append(_(
			"Hour-23 configuration drift: template \"{0}\" no longer has "
			"body parameters but {1} stale mapping row(s) remain. "
			"Remove them from Hour-23 Template Parameter Mapping in "
			"WhatsApp Compliance Settings."
		).format(template_name, mapped_count))

	return messages
