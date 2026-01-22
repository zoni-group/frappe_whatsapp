# Copyright (c) 2026, Shridhar Patil and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class WhatsAppClientApp(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		app_id: DF.Data | None
		enabled: DF.Check
		inbound_webhook_url: DF.Data | None
		outbound_default_account: DF.Link | None
	# end: auto-generated types
	pass
