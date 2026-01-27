# Copyright (c) 2025, KCSC and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class CostZone(Document):

    def validate(self):
        self.reindex_child_table("warehouses")

    def reindex_child_table(self, fieldname):
        if not self.get(fieldname):
            return

        for i, row in enumerate(self.get(fieldname), start=1):
            row.idx = i