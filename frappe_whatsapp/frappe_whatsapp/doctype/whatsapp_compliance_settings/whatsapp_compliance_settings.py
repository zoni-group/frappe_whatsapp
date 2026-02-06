# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppComplianceSettings(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		allow_reply_outside_window: DF.Check
		allow_transactional_without_consent: DF.Check
		consent_check_mode: DF.Literal["Strict", "Warning Only", "Disabled"]
		default_unsubscribe_text: DF.SmallText | None
		enable_opt_in_detection: DF.Check
		enable_opt_out_detection: DF.Check
		enforce_24_hour_window: DF.Check
		enforce_consent_check: DF.Check
		include_unsubscribe_in_marketing: DF.Check
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
	pass
