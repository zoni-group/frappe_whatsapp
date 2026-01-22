# Copyright (c) 2025, Shridhar Patil and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class WhatsAppAccount(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		account_name: DF.Data | None
		allow_auto_read_receipt: DF.Check
		app_id: DF.Data | None
		business_id: DF.Data | None
		is_default_incoming: DF.Check
		is_default_outgoing: DF.Check
		phone_id: DF.Data | None
		status: DF.Literal["Active", "Inactive"]
		token: DF.Password | None
		url: DF.Data | None
		version: DF.Data | None
		webhook_verify_token: DF.Data | None
	# end: auto-generated types
	def on_update(self):
		"""Check there is only one default of each type."""
		self.there_must_be_only_one_default()

	def there_must_be_only_one_default(self):
		"""If current WhatsApp Account is default, un-default all other accounts."""
		for field in ("is_default_incoming", "is_default_outgoing"):
			if not self.get(field):
				continue

			for whatsapp_account in frappe.get_all("WhatsApp Account", filters={field: 1}):
				if whatsapp_account.name == self.name:
					continue

				whatsapp_account = frappe.get_doc("WhatsApp Account", whatsapp_account.name)
				whatsapp_account.set(field, 0)
				whatsapp_account.save()