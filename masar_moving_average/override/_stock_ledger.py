# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
# License: GNU General Public License v3. See license.txt

import copy
import json
import frappe
from frappe import _, bold
from frappe.model.meta import get_field_precision
from frappe.query_builder.functions import Sum
from frappe.utils import (
	cint,
	flt,
	format_date,
	get_link_to_form,
	getdate,
	now
)

import erpnext
from erpnext.stock.doctype.bin.bin import update_qty as update_bin_qty
from erpnext.stock.doctype.inventory_dimension.inventory_dimension import get_inventory_dimensions

from masar_moving_average.override._utils import get_incoming_rate 
from erpnext.stock.utils import (
	get_combine_datetime,
	get_or_make_bin,
	get_serial_nos_data,
	get_valuation_method,
)
from erpnext.stock.stock_ledger import (
    get_reposting_data, 
    get_items_to_be_repost,
    get_distinct_item_warehouse,
    get_affected_transactions,
    get_current_index,
    validate_item_warehouse,
    update_args_in_repost_item_valuation,
    is_negative_stock_allowed,
    get_previous_sle_of_current_voucher,
    get_sle_by_voucher_detail_no,
    is_internal_transfer,
    get_incoming_rate_for_inter_company_transfer,
    get_stock_value_difference,
    get_previous_sle,
    get_stock_ledger_entries
    
)
from erpnext.stock.valuation import FIFOValuation, LIFOValuation, round_off_if_near_zero


class NegativeStockError(frappe.ValidationError):
	pass


class SerialNoExistsInFutureTransaction(frappe.ValidationError):
	pass


class CostZoneNotDefinedError(frappe.ValidationError):
	pass


def get_warehouse_cost_zone(warehouse):
	"""Get the cost zone for a warehouse"""
	cost_zone = frappe.db.get_value("Warehouse", warehouse, "custom_cost_zone")
	if not cost_zone:
		frappe.throw(
			_("Cost Zone is not defined for warehouse {0}. Please set the custom_cost_zone field for this warehouse.").format(
				frappe.bold(warehouse)
			),
			exc=CostZoneNotDefinedError,
			title=_("Cost Zone Missing")
		)
	return cost_zone


def get_warehouses_in_cost_zone(cost_zone, company=None):
	"""Get all warehouses in the same cost zone"""
	filters = {"custom_cost_zone": cost_zone}
	if company:
		filters["company"] = company
	
	warehouses = frappe.get_all("Warehouse", filters=filters, pluck="name")
	return warehouses


def get_cost_zone_stock_data(item_code, cost_zone, company=None):
	"""Get aggregated stock data for all warehouses in a cost zone"""
	warehouses = get_warehouses_in_cost_zone(cost_zone, company)
	
	if not warehouses:
		return {"total_qty": 0, "total_stock_value": 0, "avg_valuation_rate": 0}
	
	# Get bin data for all warehouses in the cost zone
	bin_data = frappe.db.sql("""
		SELECT 
			SUM(actual_qty) as total_qty,
			SUM(stock_value) as total_stock_value
		FROM `tabBin`
		WHERE item_code = %s 
		AND warehouse IN ({})
	""".format(','.join(['%s'] * len(warehouses))), 
	[item_code] + warehouses, as_dict=1)
	
	if bin_data and bin_data[0]:
		total_qty = flt(bin_data[0].total_qty)
		total_stock_value = flt(bin_data[0].total_stock_value)
		avg_valuation_rate = flt(total_stock_value / total_qty) if total_qty else 0
		
		return {
			"total_qty": total_qty,
			"total_stock_value": total_stock_value,
			"avg_valuation_rate": avg_valuation_rate,
			"warehouses": warehouses
		}
	
	return {"total_qty": 0, "total_stock_value": 0, "avg_valuation_rate": 0, "warehouses": warehouses}


def update_cost_zone_valuation_rate(item_code, cost_zone, new_valuation_rate, company=None):
	"""Update valuation rate for all warehouses in a cost zone"""
	warehouses = get_warehouses_in_cost_zone(cost_zone, company)
	
	for warehouse in warehouses:
		bin_name = get_or_make_bin(item_code, warehouse)
		
		# Get current bin data
		bin_data = frappe.db.get_value("Bin", bin_name, 
			["actual_qty", "stock_value"], as_dict=1)
		
		if bin_data and bin_data.actual_qty:
			# Calculate new stock value based on new valuation rate
			new_stock_value = flt(bin_data.actual_qty) * flt(new_valuation_rate)
			
			# Update bin with new valuation rate and stock value
			frappe.db.set_value("Bin", bin_name, {
				"valuation_rate": new_valuation_rate,
				"stock_value": new_stock_value
			})

def repost_future_sle(
	args=None,
	voucher_type=None,
	voucher_no=None,
	allow_negative_stock=None,
	via_landed_cost_voucher=False,
	doc=None,
):
	if not args:
		args = []  # set args to empty list if None to avoid enumerate error

	reposting_data = {}
	if doc and doc.reposting_data_file:
		reposting_data = get_reposting_data(doc.reposting_data_file)

	items_to_be_repost = get_items_to_be_repost(
		voucher_type=voucher_type, voucher_no=voucher_no, doc=doc, reposting_data=reposting_data
	)
	if items_to_be_repost:
		args = items_to_be_repost

	distinct_item_warehouses = get_distinct_item_warehouse(args, doc, reposting_data=reposting_data)
	affected_transactions = get_affected_transactions(doc, reposting_data=reposting_data)

	i = get_current_index(doc) or 0
	while i < len(args):
		validate_item_warehouse(args[i])

		obj = update_entries_after(
			{
				"item_code": args[i].get("item_code"),
				"warehouse": args[i].get("warehouse"),
				"posting_date": args[i].get("posting_date"),
				"posting_time": args[i].get("posting_time"),
				"creation": args[i].get("creation"),
				"distinct_item_warehouses": distinct_item_warehouses,
				"items_to_be_repost": args,
				"current_index": i,
			},
			allow_negative_stock=allow_negative_stock,
			via_landed_cost_voucher=via_landed_cost_voucher,
		)
		affected_transactions.update(obj.affected_transactions)

		key = (args[i].get("item_code"), args[i].get("warehouse"))
		if distinct_item_warehouses.get(key):
			distinct_item_warehouses[key].reposting_status = True

		if obj.new_items_found:
			for _item_wh, data in distinct_item_warehouses.items():
				if ("args_idx" not in data and not data.reposting_status) or (
					data.sle_changed and data.reposting_status
				):
					data.args_idx = len(args)
					args.append(data.sle)
				elif data.sle_changed and not data.reposting_status:
					args[data.args_idx] = data.sle

				data.sle_changed = False
		i += 1

		if doc:
			update_args_in_repost_item_valuation(
				doc, i, args, distinct_item_warehouses, affected_transactions
			)


class update_entries_after:
	"""
	update valuation rate and qty after transaction
	from the current time-bucket onwards using cost zone-based moving average

	:param args: args as dict

	        args = {
	                "item_code": "ABC",
	                "warehouse": "XYZ",
	                "posting_date": "2012-12-12",
	                "posting_time": "12:00"
	        }
	"""

	def __init__(
		self,
		args,
		allow_zero_rate=False,
		allow_negative_stock=None,
		via_landed_cost_voucher=False,
		verbose=1,
	):
		self.exceptions = {}
		self.verbose = verbose
		self.allow_zero_rate = allow_zero_rate
		self.via_landed_cost_voucher = via_landed_cost_voucher
		self.item_code = args.get("item_code")

		self.allow_negative_stock = allow_negative_stock or is_negative_stock_allowed(
			item_code=self.item_code
		)

		self.args = frappe._dict(args)
		if self.args.sle_id:
			self.args["name"] = self.args.sle_id

		self.company = frappe.get_cached_value("Warehouse", self.args.warehouse, "company")
		
		# Get cost zone for the warehouse
		self.cost_zone = get_warehouse_cost_zone(self.args.warehouse)
		
		self.set_precision()
		self.valuation_method = get_valuation_method(self.item_code)

		self.new_items_found = False
		self.distinct_item_warehouses = args.get("distinct_item_warehouses", frappe._dict())
		self.affected_transactions: set[tuple[str, str]] = set()
		self.reserved_stock = self.get_reserved_stock()

		self.data = frappe._dict()
		self.initialize_previous_data(self.args)
		self.build()

	def get_reserved_stock(self):
		sre = frappe.qb.DocType("Stock Reservation Entry")
		posting_datetime = get_combine_datetime(self.args.posting_date, self.args.posting_time)
		query = (
			frappe.qb.from_(sre)
			.select(Sum(sre.reserved_qty) - Sum(sre.delivered_qty))
			.where(
				(sre.item_code == self.item_code)
				& (sre.warehouse == self.args.warehouse)
				& (sre.docstatus == 1)
				& (sre.creation <= posting_datetime)
			)
		).run()

		return flt(query[0][0]) if query else 0.0

	def set_precision(self):
		self.flt_precision = cint(frappe.db.get_default("float_precision")) or 2
		self.currency_precision = get_field_precision(
			frappe.get_meta("Stock Ledger Entry").get_field("stock_value")
		)

	def initialize_previous_data(self, args):
		"""
		Get previous sl entries for current item for each related warehouse
		and assigns into self.data dict

		:Data Structure:

		self.data = {
		        warehouse1: {
		                'previus_sle': {},
		                'qty_after_transaction': 10,
		                'valuation_rate': 100,
		                'stock_value': 1000,
		                'prev_stock_value': 1000,
		                'stock_queue': '[[10, 100]]',
		                'stock_value_difference': 1000
		        }
		}

		"""
		self.data.setdefault(args.warehouse, frappe._dict())
		warehouse_dict = self.data[args.warehouse]
		previous_sle = get_previous_sle_of_current_voucher(args)
		warehouse_dict.previous_sle = previous_sle

		for key in ("qty_after_transaction", "valuation_rate", "stock_value"):
			setattr(warehouse_dict, key, flt(previous_sle.get(key)))

		warehouse_dict.update(
			{
				"prev_stock_value": previous_sle.stock_value or 0.0,
				"stock_queue": json.loads(previous_sle.stock_queue or "[]"),
				"stock_value_difference": 0.0,
			}
		)

	def build(self):
		from erpnext.controllers.stock_controller import future_sle_exists

		if self.args.get("sle_id"):
			self.process_sle_against_current_timestamp()
			if not future_sle_exists(self.args):
				self.update_bin()
		else:
			entries_to_fix = self.get_future_entries_to_fix()

			i = 0
			while i < len(entries_to_fix):
				sle = entries_to_fix[i]
				i += 1

				self.process_sle(sle)
				self.update_bin_data(sle)

				if sle.dependant_sle_voucher_detail_no:
					entries_to_fix = self.get_dependent_entries_to_fix(entries_to_fix, sle)

		if self.exceptions:
			self.raise_exceptions()

	def has_stock_reco_with_serial_batch(self, sle):
		if (
			sle.voucher_type == "Stock Reconciliation"
			and frappe.db.get_value(sle.voucher_type, sle.voucher_no, "set_posting_time") == 1
		):
			return not (sle.batch_no or sle.serial_no or sle.serial_and_batch_bundle)

		return False

	def process_sle_against_current_timestamp(self):
		sl_entries = self.get_sle_against_current_voucher()
		for sle in sl_entries:
			self.process_sle(sle)

	def get_sle_against_current_voucher(self):
		self.args["posting_datetime"] = get_combine_datetime(self.args.posting_date, self.args.posting_time)

		return frappe.db.sql(
			"""
			select
				*, posting_datetime as "timestamp"
			from
				`tabStock Ledger Entry`
			where
				item_code = %(item_code)s
				and warehouse = %(warehouse)s
				and is_cancelled = 0
				and (
					posting_datetime = %(posting_datetime)s
				)
				and creation = %(creation)s
			order by
				creation ASC
			for update
		""",
			self.args,
			as_dict=1,
		)

	def get_future_entries_to_fix(self):
		# includes current entry!
		args = self.data[self.args.warehouse].previous_sle or frappe._dict(
			{"item_code": self.item_code, "warehouse": self.args.warehouse}
		)

		return list(self.get_sle_after_datetime(args))

	def get_dependent_entries_to_fix(self, entries_to_fix, sle):
		dependant_sle = get_sle_by_voucher_detail_no(
			sle.dependant_sle_voucher_detail_no, excluded_sle=sle.name
		)

		if not dependant_sle:
			return entries_to_fix
		elif dependant_sle.item_code == self.item_code and dependant_sle.warehouse == self.args.warehouse:
			return entries_to_fix
		elif dependant_sle.item_code != self.item_code:
			self.update_distinct_item_warehouses(dependant_sle)
			return entries_to_fix
		elif dependant_sle.item_code == self.item_code and dependant_sle.warehouse in self.data:
			return entries_to_fix
		else:
			self.initialize_previous_data(dependant_sle)
			self.update_distinct_item_warehouses(dependant_sle)
			return entries_to_fix

	def update_distinct_item_warehouses(self, dependant_sle):
		key = (dependant_sle.item_code, dependant_sle.warehouse)
		val = frappe._dict({"sle": dependant_sle})

		if key not in self.distinct_item_warehouses:
			self.distinct_item_warehouses[key] = val
			self.new_items_found = True
		else:
			# Check if the dependent voucher is reposted
			# If not, then do not add it to the list
			if not self.is_dependent_voucher_reposted(dependant_sle):
				return

			existing_sle_posting_date = self.distinct_item_warehouses[key].get("sle", {}).get("posting_date")

			dependent_voucher_detail_nos = self.get_dependent_voucher_detail_nos(key)
			if getdate(dependant_sle.posting_date) < getdate(existing_sle_posting_date):
				if dependent_voucher_detail_nos and dependant_sle.voucher_detail_no in set(
					dependent_voucher_detail_nos
				):
					return

				val.sle_changed = True
				dependent_voucher_detail_nos.append(dependant_sle.voucher_detail_no)
				val.dependent_voucher_detail_nos = dependent_voucher_detail_nos
				self.distinct_item_warehouses[key] = val
				self.new_items_found = True
			elif dependant_sle.voucher_detail_no not in set(dependent_voucher_detail_nos):
				# Future dependent voucher needs to be repost to get the correct stock value
				# If dependent voucher has not reposted, then add it to the list
				dependent_voucher_detail_nos.append(dependant_sle.voucher_detail_no)
				self.new_items_found = True
				val.dependent_voucher_detail_nos = dependent_voucher_detail_nos
				self.distinct_item_warehouses[key] = val

	def is_dependent_voucher_reposted(self, dependant_sle) -> bool:
		# Return False if the dependent voucher is not reposted

		if self.args.items_to_be_repost and self.args.current_index:
			index = self.args.current_index
			while index < len(self.args.items_to_be_repost):
				if (
					self.args.items_to_be_repost[index].get("item_code") == dependant_sle.item_code
					and self.args.items_to_be_repost[index].get("warehouse") == dependant_sle.warehouse
				):
					if getdate(self.args.items_to_be_repost[index].get("posting_date")) > getdate(
						dependant_sle.posting_date
					):
						self.args.items_to_be_repost[index]["posting_date"] = dependant_sle.posting_date

					return False

				index += 1

		return True

	def get_dependent_voucher_detail_nos(self, key):
		if "dependent_voucher_detail_nos" not in self.distinct_item_warehouses[key]:
			self.distinct_item_warehouses[key].dependent_voucher_detail_nos = []

		return self.distinct_item_warehouses[key].dependent_voucher_detail_nos

	def validate_previous_sle_qty(self, sle):
		previous_sle = self.data[sle.warehouse].previous_sle
		if previous_sle and previous_sle.get("qty_after_transaction") < 0 and sle.get("actual_qty") > 0:
			frappe.msgprint(
				_(
					"The stock for the item {0} in the {1} warehouse was negative on the {2}. You should create a positive entry {3} before the date {4} and time {5} to post the correct valuation rate. For more details, please read the <a href='https://docs.erpnext.com/docs/user/manual/en/stock-adjustment-cogs-with-negative-stock'>documentation<a>."
				).format(
					bold(sle.item_code),
					bold(sle.warehouse),
					bold(format_date(previous_sle.posting_date)),
					sle.voucher_no,
					bold(format_date(previous_sle.posting_date)),
					bold(previous_sle.posting_time),
				),
				title=_("Warning on Negative Stock"),
				indicator="blue",
			)

	def process_sle(self, sle):
		# previous sle data for this warehouse
		self.wh_data = self.data[sle.warehouse]

		self.validate_previous_sle_qty(sle)
		self.affected_transactions.add((sle.voucher_type, sle.voucher_no))

		if (sle.serial_no and not self.via_landed_cost_voucher) or not cint(self.allow_negative_stock):
			# validate negative stock for serialized items, fifo valuation
			# or when negative stock is not allowed for moving average
			if not self.validate_negative_stock(sle):
				self.wh_data.qty_after_transaction += flt(sle.actual_qty)
				return

		# Get dynamic incoming/outgoing rate
		if not self.args.get("sle_id"):
			self.get_dynamic_incoming_outgoing_rate(sle)

		if (
			sle.voucher_type == "Stock Reconciliation"
			and (sle.serial_and_batch_bundle)
			and sle.voucher_detail_no
			and not self.args.get("sle_id")
			and sle.is_cancelled == 0
		):
			self.reset_actual_qty_for_stock_reco(sle)

		if (
			sle.voucher_type in ["Purchase Receipt", "Purchase Invoice"]
			and sle.voucher_detail_no
			and sle.actual_qty < 0
			and is_internal_transfer(sle)
		):
			sle.outgoing_rate = get_incoming_rate_for_inter_company_transfer(sle)

		dimensions = get_inventory_dimensions()
		has_dimensions = False
		if dimensions:
			for dimension in dimensions:
				if sle.get(dimension.get("fieldname")):
					has_dimensions = True

		if sle.serial_and_batch_bundle:
			self.calculate_valuation_for_serial_batch_bundle(sle)
		elif sle.serial_no and not self.args.get("sle_id"):
			# Only run in reposting
			self.get_serialized_values(sle)
			self.wh_data.qty_after_transaction += flt(sle.actual_qty)
			if sle.voucher_type == "Stock Reconciliation" and not sle.batch_no:
				self.wh_data.qty_after_transaction = sle.qty_after_transaction

			self.wh_data.stock_value = flt(self.wh_data.qty_after_transaction) * flt(
				self.wh_data.valuation_rate
			)
		elif (
			sle.batch_no
			and frappe.db.get_value("Batch", sle.batch_no, "use_batchwise_valuation", cache=True)
			and not self.args.get("sle_id")
		):
			# Only run in reposting
			self.update_batched_values(sle)
		else:
			if (
				sle.voucher_type == "Stock Reconciliation"
				and not sle.batch_no
				and not sle.has_batch_no
				and not has_dimensions
			):
				# assert
				self.wh_data.valuation_rate = sle.valuation_rate
				self.wh_data.qty_after_transaction = sle.qty_after_transaction
				self.wh_data.stock_value = flt(self.wh_data.qty_after_transaction) * flt(
					self.wh_data.valuation_rate
				)
				if self.valuation_method != "Moving Average":
					self.wh_data.stock_queue = [
						[self.wh_data.qty_after_transaction, self.wh_data.valuation_rate]
					]
			else:
				if self.valuation_method == "Moving Average":
					# *** MODIFIED: Use cost zone-based moving average ***
					self.get_cost_zone_moving_average_values(sle)
					self.wh_data.qty_after_transaction += flt(sle.actual_qty)
					self.wh_data.stock_value = flt(self.wh_data.qty_after_transaction) * flt(
						self.wh_data.valuation_rate
					)

					if flt(self.wh_data.qty_after_transaction, self.flt_precision) != 0:
						self.wh_data.valuation_rate = flt(
							self.wh_data.stock_value, self.currency_precision
						) / flt(self.wh_data.qty_after_transaction, self.flt_precision)

				else:
					self.update_queue_values(sle)

		# rounding as per precision
		self.wh_data.stock_value = flt(self.wh_data.stock_value, self.currency_precision)
		if not self.wh_data.qty_after_transaction:
			self.wh_data.stock_value = 0.0

		stock_value_difference = self.wh_data.stock_value - self.wh_data.prev_stock_value
		self.wh_data.prev_stock_value = self.wh_data.stock_value

		# update current sle
		sle.qty_after_transaction = flt(self.wh_data.qty_after_transaction, self.flt_precision)
		sle.valuation_rate = self.wh_data.valuation_rate
		sle.stock_value = self.wh_data.stock_value
		sle.stock_queue = json.dumps(self.wh_data.stock_queue)

		if not sle.is_adjustment_entry:
			sle.stock_value_difference = stock_value_difference
		elif sle.is_adjustment_entry and not self.args.get("sle_id"):
			sle.stock_value_difference = (
				get_stock_value_difference(
					sle.item_code, sle.warehouse, sle.posting_date, sle.posting_time, sle.voucher_no
				)
				* -1
			)

		sle.doctype = "Stock Ledger Entry"
		sle.modified = now()
		frappe.get_doc(sle).db_update()

		if self.valuation_method == "Moving Average" and not self.args.get("sle_id"):
			self.update_cost_zone_bins(sle)

		if not self.args.get("sle_id") or (
			sle.serial_and_batch_bundle and sle.auto_created_serial_and_batch_bundle
		):
			self.update_outgoing_rate_on_transaction(sle)

	def get_cost_zone_moving_average_values(self, sle):
		"""Calculate moving average valuation rate based on cost zone"""
		actual_qty = flt(sle.actual_qty)
		
		# Get cost zone stock data
		zone_data = get_cost_zone_stock_data(sle.item_code, self.cost_zone, self.company)
		
		# Calculate current zone totals (before this transaction)
		zone_total_qty = zone_data.get("total_qty", 0)
		zone_total_value = zone_data.get("total_stock_value", 0)
		
		# Calculate new zone totals after this transaction
		new_zone_qty = zone_total_qty + actual_qty
		
		if new_zone_qty >= 0:
			if actual_qty > 0:
				# Incoming stock
				if zone_total_qty <= 0:
					# First stock in the zone
					new_valuation_rate = sle.incoming_rate
				else:
					# Calculate weighted average
					incoming_value = actual_qty * sle.incoming_rate
					new_zone_value = zone_total_value + incoming_value
					new_valuation_rate = new_zone_value / new_zone_qty if new_zone_qty else 0
			elif sle.outgoing_rate:
				# Outgoing stock with specified rate
				if new_zone_qty > 0:
					outgoing_value = actual_qty * sle.outgoing_rate
					new_zone_value = zone_total_value + outgoing_value  # actual_qty is negative
					new_valuation_rate = new_zone_value / new_zone_qty
				else:
					new_valuation_rate = sle.outgoing_rate
			else:
				# Use current zone average rate
				new_valuation_rate = zone_data.get("avg_valuation_rate", 0)
		else:
			# Negative stock scenario
			if zone_total_qty >= 0 and sle.outgoing_rate:
				new_valuation_rate = sle.outgoing_rate
			elif actual_qty > 0:
				new_valuation_rate = sle.incoming_rate
			else:
				new_valuation_rate = zone_data.get("avg_valuation_rate", 0)

			# Get fallback rate if needed
			if not new_valuation_rate and sle.voucher_detail_no:
				allow_zero_valuation_rate = self.check_if_allow_zero_valuation_rate(
					sle.voucher_type, sle.voucher_detail_no
				)
				if not allow_zero_valuation_rate:
					new_valuation_rate = self.get_fallback_rate(sle)

		# Update warehouse data with new valuation rate
		self.wh_data.valuation_rate = new_valuation_rate

	def update_cost_zone_bins(self, sle):
		"""Update valuation rate for all bins in the same cost zone"""
		try:
			update_cost_zone_valuation_rate(
				sle.item_code, 
				self.cost_zone, 
				self.wh_data.valuation_rate, 
				self.company
			)
		except Exception as e:
			frappe.log_error(f"Error updating cost zone bins: {str(e)}")

	def get_serialized_values(self, sle):
		from erpnext.stock.serial_batch_bundle import SerialNoValuation

		incoming_rate = flt(sle.incoming_rate)
		actual_qty = flt(sle.actual_qty)

		if incoming_rate < 0:
			# wrong incoming rate
			incoming_rate = self.wh_data.valuation_rate

		stock_value_change = 0
		if actual_qty > 0:
			stock_value_change = actual_qty * incoming_rate
		else:
			# In case of delivery/stock issue, get average purchase rate
			# of serial nos of current entry
			if not sle.is_cancelled:
				new_sle = copy.deepcopy(sle)
				new_sle.qty = new_sle.actual_qty
				new_sle.serial_nos = get_serial_nos_data(new_sle.get("serial_no"))

				sn_obj = SerialNoValuation(
					sle=new_sle, warehouse=new_sle.get("warehouse"), item_code=new_sle.get("item_code")
				)

				outgoing_value = sn_obj.get_incoming_rate()
				stock_value_change = actual_qty * outgoing_value
			else:
				stock_value_change = actual_qty * sle.outgoing_rate

		new_stock_qty = self.wh_data.qty_after_transaction + actual_qty

		if new_stock_qty > 0:
			new_stock_value = (
				self.wh_data.qty_after_transaction * self.wh_data.valuation_rate
			) + stock_value_change
			if new_stock_value >= 0:
				# calculate new valuation rate only if stock value is positive
				# else it remains the same as that of previous entry
				self.wh_data.valuation_rate = new_stock_value / new_stock_qty

		if self.wh_data.valuation_rate is None and sle.voucher_detail_no:
			allow_zero_rate = self.check_if_allow_zero_valuation_rate(sle.voucher_type, sle.voucher_detail_no)
			if not allow_zero_rate:
				self.wh_data.valuation_rate = self.get_fallback_rate(sle)

	def reset_actual_qty_for_stock_reco(self, sle):
		doc = frappe.get_doc("Stock Reconciliation", sle.voucher_no)
		doc.recalculate_current_qty(sle.voucher_detail_no, sle.creation, sle.actual_qty > 0)

		if sle.actual_qty < 0:
			doc.reload()

			sle.actual_qty = (
				flt(frappe.db.get_value("Stock Reconciliation Item", sle.voucher_detail_no, "current_qty"))
				* -1
			)

			if abs(sle.actual_qty) == 0.0:
				sle.is_cancelled = 1

				if sle.serial_and_batch_bundle:
					for row in doc.items:
						if row.name == sle.voucher_detail_no:
							row.db_set("current_serial_and_batch_bundle", "")

					sabb_doc = frappe.get_doc("Serial and Batch Bundle", sle.serial_and_batch_bundle)
					sabb_doc.voucher_detail_no = None
					sabb_doc.voucher_no = None
					sabb_doc.cancel()

		if sle.serial_and_batch_bundle and frappe.get_cached_value("Item", sle.item_code, "has_serial_no"):
			self.update_serial_no_status(sle)

	def update_serial_no_status(self, sle):
		from erpnext.stock.serial_batch_bundle import get_serial_nos

		serial_nos = get_serial_nos(sle.serial_and_batch_bundle)
		if not serial_nos:
			return

		warehouse = None
		status = "Inactive"

		if sle.actual_qty > 0:
			warehouse = sle.warehouse
			status = "Active"

		sn_table = frappe.qb.DocType("Serial No")

		query = (
			frappe.qb.update(sn_table)
			.set(sn_table.warehouse, warehouse)
			.set(sn_table.status, status)
			.where(sn_table.name.isin(serial_nos))
		)

		query.run()

	def calculate_valuation_for_serial_batch_bundle(self, sle):
		if not frappe.db.exists("Serial and Batch Bundle", sle.serial_and_batch_bundle):
			return

		if self.args.get("sle_id") and sle.actual_qty < 0:
			doc = frappe.db.get_value(
				"Serial and Batch Bundle",
				sle.serial_and_batch_bundle,
				["total_amount", "total_qty"],
				as_dict=1,
			)
		else:
			doc = frappe.get_doc("Serial and Batch Bundle", sle.serial_and_batch_bundle)
			doc.set_incoming_rate(save=True, allow_negative_stock=self.allow_negative_stock)
			doc.calculate_qty_and_amount(save=True)

		if stock_queue := frappe.get_all(
			"Serial and Batch Entry",
			filters={"parent": sle.serial_and_batch_bundle, "stock_queue": ("is", "set")},
			pluck="stock_queue",
			order_by="idx desc",
			limit=1,
		):
			self.wh_data.stock_queue = json.loads(stock_queue[0]) if stock_queue else []

		self.wh_data.stock_value = round_off_if_near_zero(self.wh_data.stock_value + doc.total_amount)
		self.wh_data.qty_after_transaction += flt(doc.total_qty, self.flt_precision)
		if flt(self.wh_data.qty_after_transaction, self.flt_precision):
			self.wh_data.valuation_rate = flt(self.wh_data.stock_value, self.flt_precision) / flt(
				self.wh_data.qty_after_transaction, self.flt_precision
			)

	def get_outgoing_rate_for_batched_item(self, sle):
		if self.wh_data.qty_after_transaction == 0:
			return 0

		return flt(self.wh_data.stock_value) / flt(self.wh_data.qty_after_transaction)

	def validate_negative_stock(self, sle):
		"""
		validate negative stock for entries current datetime onwards
		will not consider cancelled entries
		"""
		diff = self.wh_data.qty_after_transaction + flt(sle.actual_qty) - flt(self.reserved_stock)
		diff = flt(diff, self.flt_precision)  # respect system precision

		if diff < 0 and abs(diff) > 0.0001:
			# negative stock!
			exc = sle.copy().update({"diff": diff})
			self.exceptions.setdefault(sle.warehouse, []).append(exc)
			return False
		else:
			return True

	def get_dynamic_incoming_outgoing_rate(self, sle):
		# Get updated incoming/outgoing rate from transaction
		if sle.recalculate_rate or self.has_landed_cost_based_on_pi(sle):
			rate = self.get_incoming_outgoing_rate_from_transaction(sle)

			if flt(sle.actual_qty) >= 0:
				sle.incoming_rate = rate
			else:
				sle.outgoing_rate = rate

	def has_landed_cost_based_on_pi(self, sle):
		if sle.voucher_type == "Purchase Receipt" and frappe.db.get_single_value(
			"Buying Settings", "set_landed_cost_based_on_purchase_invoice_rate"
		):
			return True

		return False

	def get_incoming_outgoing_rate_from_transaction(self, sle):
		rate = 0
		# Material Transfer, Repack, Manufacturing
		if sle.voucher_type == "Stock Entry":
			self.recalculate_amounts_in_stock_entry(sle.voucher_no, sle.voucher_detail_no)
			rate = frappe.db.get_value("Stock Entry Detail", sle.voucher_detail_no, "valuation_rate")
		# Sales and Purchase Return
		elif sle.voucher_type in (
			"Purchase Receipt",
			"Purchase Invoice",
			"Delivery Note",
			"Sales Invoice",
			"Subcontracting Receipt",
		):
			if frappe.get_cached_value(sle.voucher_type, sle.voucher_no, "is_return"):
				from erpnext.controllers.sales_and_purchase_return import (
					get_rate_for_return,  # don't move this import to top
				)

				if (
					self.valuation_method == "Moving Average"
					and not sle.get("serial_no")
					and not sle.get("batch_no")
					and not sle.get("serial_and_batch_bundle")
				):
					rate = get_incoming_rate(
						{
							"item_code": sle.item_code,
							"warehouse": sle.warehouse,
							"posting_date": sle.posting_date,
							"posting_time": sle.posting_time,
							"qty": sle.actual_qty,
							"serial_no": sle.get("serial_no"),
							"batch_no": sle.get("batch_no"),
							"serial_and_batch_bundle": sle.get("serial_and_batch_bundle"),
							"company": sle.company,
							"voucher_type": sle.voucher_type,
							"voucher_no": sle.voucher_no,
							"allow_zero_valuation": self.allow_zero_rate,
							"sle": sle.name,
						}
					)

					if not rate and sle.voucher_type in ["Delivery Note", "Sales Invoice"]:
						rate = get_rate_for_return(
							sle.voucher_type,
							sle.voucher_no,
							sle.item_code,
							voucher_detail_no=sle.voucher_detail_no,
							sle=sle,
						)

				else:
					rate = get_rate_for_return(
						sle.voucher_type,
						sle.voucher_no,
						sle.item_code,
						voucher_detail_no=sle.voucher_detail_no,
						sle=sle,
					)

				if (
					sle.get("serial_and_batch_bundle")
					and rate > 0
					and sle.voucher_type in ["Delivery Note", "Sales Invoice"]
				):
					frappe.db.set_value(
						sle.voucher_type + " Item",
						sle.voucher_detail_no,
						"incoming_rate",
						rate,
					)
			elif (
				sle.voucher_type in ["Purchase Receipt", "Purchase Invoice"]
				and sle.voucher_detail_no
				and is_internal_transfer(sle)
			):
				rate = get_incoming_rate_for_inter_company_transfer(sle)
			else:
				if sle.voucher_type in ("Purchase Receipt", "Purchase Invoice"):
					rate_field = "valuation_rate"
				elif sle.voucher_type == "Subcontracting Receipt":
					rate_field = "rate"
				else:
					rate_field = "incoming_rate"

				# check in item table
				item_code, incoming_rate = frappe.db.get_value(
					sle.voucher_type + " Item", sle.voucher_detail_no, ["item_code", rate_field]
				)

				if item_code == sle.item_code:
					rate = incoming_rate
				else:
					if sle.voucher_type in ("Delivery Note", "Sales Invoice"):
						ref_doctype = "Packed Item"
					elif sle == "Subcontracting Receipt":
						ref_doctype = "Subcontracting Receipt Supplied Item"
					else:
						ref_doctype = "Purchase Receipt Item Supplied"

					rate = frappe.db.get_value(
						ref_doctype,
						{"parent_detail_docname": sle.voucher_detail_no, "item_code": sle.item_code},
						rate_field,
					)

		return rate

	def update_outgoing_rate_on_transaction(self, sle):
		"""
		Update outgoing rate in Stock Entry, Delivery Note, Sales Invoice and Sales Return
		In case of Stock Entry, also calculate FG Item rate and total incoming/outgoing amount
		"""
		if sle.actual_qty and sle.voucher_detail_no:
			outgoing_rate = abs(flt(sle.stock_value_difference)) / abs(sle.actual_qty)

			if flt(sle.actual_qty) < 0 and sle.voucher_type == "Stock Entry":
				self.update_rate_on_stock_entry(sle, outgoing_rate)
			elif sle.voucher_type in ("Delivery Note", "Sales Invoice"):
				self.update_rate_on_delivery_and_sales_return(sle, outgoing_rate)
			elif flt(sle.actual_qty) < 0 and sle.voucher_type in ("Purchase Receipt", "Purchase Invoice"):
				self.update_rate_on_purchase_receipt(sle, outgoing_rate)
			elif flt(sle.actual_qty) < 0 and sle.voucher_type == "Subcontracting Receipt":
				self.update_rate_on_subcontracting_receipt(sle, outgoing_rate)
		elif sle.voucher_type == "Stock Reconciliation":
			self.update_rate_on_stock_reconciliation(sle)

	def update_rate_on_stock_entry(self, sle, outgoing_rate):
		frappe.db.set_value("Stock Entry Detail", sle.voucher_detail_no, "basic_rate", outgoing_rate)

		# Update outgoing item's rate, recalculate FG Item's rate and total incoming/outgoing amount
		if not sle.dependant_sle_voucher_detail_no or self.is_manufacture_entry_with_sabb(sle):
			self.recalculate_amounts_in_stock_entry(sle.voucher_no, sle.voucher_detail_no)

	def is_manufacture_entry_with_sabb(self, sle):
		if (
			self.args.get("sle_id")
			and sle.serial_and_batch_bundle
			and sle.auto_created_serial_and_batch_bundle
		):
			purpose = frappe.get_cached_value("Stock Entry", sle.voucher_no, "purpose")
			if purpose in ["Manufacture", "Repack"]:
				return True

		return False

	def recalculate_amounts_in_stock_entry(self, voucher_no, voucher_detail_no):
		stock_entry = frappe.get_doc("Stock Entry", voucher_no, for_update=True)
		stock_entry.calculate_rate_and_amount(reset_outgoing_rate=False, raise_error_if_no_rate=False)
		stock_entry.db_update()
		for d in stock_entry.items:
			# Update only the row that matches the voucher_detail_no or the row containing the FG/Scrap Item.
			if (
				d.name == voucher_detail_no
				or (not d.s_warehouse and d.t_warehouse)
				or stock_entry.purpose in ["Manufacture", "Repack"]
			):
				d.db_update()

	def update_rate_on_delivery_and_sales_return(self, sle, outgoing_rate):
		# Update item's incoming rate on transaction
		item_code = frappe.db.get_value(sle.voucher_type + " Item", sle.voucher_detail_no, "item_code")
		if item_code == sle.item_code:
			frappe.db.set_value(
				sle.voucher_type + " Item", sle.voucher_detail_no, "incoming_rate", outgoing_rate
			)
		else:
			# packed item
			frappe.db.set_value(
				"Packed Item",
				{"parent_detail_docname": sle.voucher_detail_no, "item_code": sle.item_code},
				"incoming_rate",
				outgoing_rate,
			)

	def update_rate_on_purchase_receipt(self, sle, outgoing_rate):
		if frappe.db.exists(sle.voucher_type + " Item", sle.voucher_detail_no):
			if sle.voucher_type in ["Purchase Receipt", "Purchase Invoice"]:
				details = frappe.get_cached_value(
					sle.voucher_type,
					sle.voucher_no,
					["is_internal_supplier", "is_return", "return_against"],
					as_dict=True,
				)
				if details.is_internal_supplier or (details.is_return and not details.return_against):
					rate = outgoing_rate if details.is_return else sle.outgoing_rate

					frappe.db.set_value(
						f"{sle.voucher_type} Item", sle.voucher_detail_no, "valuation_rate", rate
					)
		else:
			frappe.db.set_value(
				"Purchase Receipt Item Supplied", sle.voucher_detail_no, "rate", outgoing_rate
			)

		# Recalculate subcontracted item's rate in case of subcontracted purchase receipt/invoice
		if frappe.get_cached_value(sle.voucher_type, sle.voucher_no, "is_subcontracted"):
			doc = frappe.get_doc(sle.voucher_type, sle.voucher_no)
			doc.update_valuation_rate(reset_outgoing_rate=False)
			for d in doc.items + doc.supplied_items:
				d.db_update()

	def update_rate_on_subcontracting_receipt(self, sle, outgoing_rate):
		if frappe.db.exists("Subcontracting Receipt Item", sle.voucher_detail_no):
			frappe.db.set_value("Subcontracting Receipt Item", sle.voucher_detail_no, "rate", outgoing_rate)
		else:
			frappe.db.set_value(
				"Subcontracting Receipt Supplied Item",
				sle.voucher_detail_no,
				{"rate": outgoing_rate, "amount": abs(sle.actual_qty) * outgoing_rate},
			)

		scr = frappe.get_doc("Subcontracting Receipt", sle.voucher_no, for_update=True)
		scr.calculate_items_qty_and_amount()
		scr.db_update()
		for d in scr.items:
			d.db_update()

	def update_rate_on_stock_reconciliation(self, sle):
		if not sle.serial_no and not sle.batch_no:
			sr = frappe.get_doc("Stock Reconciliation", sle.voucher_no, for_update=True)

			for item in sr.items:
				# Skip for Serial and Batch Items
				if item.name != sle.voucher_detail_no or item.serial_no or item.batch_no:
					continue

				previous_sle = get_previous_sle(
					{
						"item_code": item.item_code,
						"warehouse": item.warehouse,
						"posting_date": sr.posting_date,
						"posting_time": sr.posting_time,
						"sle": sle.name,
					}
				)

				item.current_qty = previous_sle.get("qty_after_transaction") or 0.0
				item.current_valuation_rate = previous_sle.get("valuation_rate") or 0.0
				item.current_amount = flt(item.current_qty) * flt(item.current_valuation_rate)

				item.amount = flt(item.qty) * flt(item.valuation_rate)
				item.quantity_difference = item.qty - item.current_qty
				item.amount_difference = item.amount - item.current_amount
			else:
				sr.difference_amount = sum([item.amount_difference for item in sr.items])
			sr.db_update()

			for item in sr.items:
				item.db_update()

	def get_incoming_value_for_serial_nos(self, sle, serial_nos):
		# get rate from serial nos within same company
		all_serial_nos = frappe.get_all(
			"Serial No", fields=["purchase_rate", "name", "company"], filters={"name": ("in", serial_nos)}
		)

		incoming_values = sum(flt(d.purchase_rate) for d in all_serial_nos if d.company == sle.company)

		# Get rate for serial nos which has been transferred to other company
		invalid_serial_nos = [d.name for d in all_serial_nos if d.company != sle.company]
		for serial_no in invalid_serial_nos:
			incoming_rate = frappe.db.sql(
				"""
				select incoming_rate
				from `tabStock Ledger Entry`
				where
					company = %s
					and actual_qty > 0
					and is_cancelled = 0
					and (serial_no = %s
						or serial_no like %s
						or serial_no like %s
						or serial_no like %s
					)
				order by posting_date desc
				limit 1
			""",
				(sle.company, serial_no, serial_no + "\n%", "%\n" + serial_no, "%\n" + serial_no + "\n%"),
			)

			incoming_values += flt(incoming_rate[0][0]) if incoming_rate else 0

		return incoming_values

	def get_moving_average_values(self, sle):
		actual_qty = flt(sle.actual_qty)
		new_stock_qty = flt(self.wh_data.qty_after_transaction) + actual_qty
		if new_stock_qty >= 0:
			if actual_qty > 0:
				if flt(self.wh_data.qty_after_transaction) <= 0:
					self.wh_data.valuation_rate = sle.incoming_rate
				else:
					new_stock_value = (self.wh_data.qty_after_transaction * self.wh_data.valuation_rate) + (
						actual_qty * sle.incoming_rate
					)

					self.wh_data.valuation_rate = new_stock_value / new_stock_qty

			elif sle.outgoing_rate:
				if new_stock_qty:
					new_stock_value = (self.wh_data.qty_after_transaction * self.wh_data.valuation_rate) + (
						actual_qty * sle.outgoing_rate
					)

					self.wh_data.valuation_rate = new_stock_value / new_stock_qty
				else:
					self.wh_data.valuation_rate = sle.outgoing_rate
		else:
			if flt(self.wh_data.qty_after_transaction) >= 0 and sle.outgoing_rate:
				self.wh_data.valuation_rate = sle.outgoing_rate

			if not self.wh_data.valuation_rate and actual_qty > 0:
				self.wh_data.valuation_rate = sle.incoming_rate

			# Get valuation rate from previous SLE or Item master, if item does not have the
			# allow zero valuration rate flag set
			if not self.wh_data.valuation_rate and sle.voucher_detail_no:
				allow_zero_valuation_rate = self.check_if_allow_zero_valuation_rate(
					sle.voucher_type, sle.voucher_detail_no
				)
				if not allow_zero_valuation_rate:
					self.wh_data.valuation_rate = self.get_fallback_rate(sle)

	def update_queue_values(self, sle):
		incoming_rate = flt(sle.incoming_rate)
		actual_qty = flt(sle.actual_qty)
		outgoing_rate = flt(sle.outgoing_rate)

		self.wh_data.qty_after_transaction = round_off_if_near_zero(
			self.wh_data.qty_after_transaction + actual_qty
		)

		if self.valuation_method == "LIFO":
			stock_queue = LIFOValuation(self.wh_data.stock_queue)
		else:
			stock_queue = FIFOValuation(self.wh_data.stock_queue)

		_prev_qty, prev_stock_value = stock_queue.get_total_stock_and_value()

		if actual_qty > 0:
			stock_queue.add_stock(qty=actual_qty, rate=incoming_rate)
		else:

			def rate_generator() -> float:
				allow_zero_valuation_rate = self.check_if_allow_zero_valuation_rate(
					sle.voucher_type, sle.voucher_detail_no
				)
				if not allow_zero_valuation_rate:
					return self.get_fallback_rate(sle)
				else:
					return 0.0

			stock_queue.remove_stock(
				qty=abs(actual_qty), outgoing_rate=outgoing_rate, rate_generator=rate_generator
			)

		_qty, stock_value = stock_queue.get_total_stock_and_value()

		stock_value_difference = stock_value - prev_stock_value

		self.wh_data.stock_queue = stock_queue.state
		self.wh_data.stock_value = round_off_if_near_zero(self.wh_data.stock_value + stock_value_difference)

		if not self.wh_data.stock_queue:
			self.wh_data.stock_queue.append(
				[0, sle.incoming_rate or sle.outgoing_rate or self.wh_data.valuation_rate]
			)

		if self.wh_data.qty_after_transaction:
			self.wh_data.valuation_rate = self.wh_data.stock_value / self.wh_data.qty_after_transaction

	def update_batched_values(self, sle):
		from erpnext.stock.serial_batch_bundle import BatchNoValuation

		incoming_rate = flt(sle.incoming_rate)
		actual_qty = flt(sle.actual_qty)

		self.wh_data.qty_after_transaction = round_off_if_near_zero(
			self.wh_data.qty_after_transaction + actual_qty
		)

		if actual_qty > 0:
			stock_value_difference = incoming_rate * actual_qty
		else:
			new_sle = copy.deepcopy(sle)

			new_sle.qty = new_sle.actual_qty
			new_sle.batch_nos = frappe._dict({new_sle.batch_no: new_sle})
			batch_obj = BatchNoValuation(
				sle=new_sle,
				warehouse=new_sle.get("warehouse"),
				item_code=new_sle.get("item_code"),
			)

			outgoing_rate = batch_obj.get_incoming_rate()

			if outgoing_rate is None:
				# This can *only* happen if qty available for the batch is zero.
				# in such case fall back various other rates.
				# future entries will correct the overall accounting as each
				# batch individually uses moving average rates.
				outgoing_rate = self.get_fallback_rate(sle)

			stock_value_difference = outgoing_rate * actual_qty

		self.wh_data.stock_value = round_off_if_near_zero(self.wh_data.stock_value + stock_value_difference)
		if self.wh_data.qty_after_transaction:
			self.wh_data.valuation_rate = self.wh_data.stock_value / self.wh_data.qty_after_transaction

	def check_if_allow_zero_valuation_rate(self, voucher_type, voucher_detail_no):
		ref_item_dt = ""

		if voucher_type == "Stock Entry":
			ref_item_dt = voucher_type + " Detail"
		elif voucher_type in ["Purchase Invoice", "Sales Invoice", "Delivery Note", "Purchase Receipt"]:
			ref_item_dt = voucher_type + " Item"

		if ref_item_dt:
			return frappe.db.get_value(ref_item_dt, voucher_detail_no, "allow_zero_valuation_rate")
		else:
			return 0

	def get_fallback_rate(self, sle) -> float:
		"""When exact incoming rate isn't available use any of other "average" rates as fallback.
		This should only get used for negative stock."""
		return get_valuation_rate(
			sle.item_code,
			sle.warehouse,
			sle.voucher_type,
			sle.voucher_no,
			self.allow_zero_rate,
			currency=erpnext.get_company_currency(sle.company),
			company=sle.company,
		)

	def get_sle_before_datetime(self, args):
		"""get previous stock ledger entry before current time-bucket"""
		sle = get_stock_ledger_entries(args, "<", "desc", "limit 1", for_update=False)
		sle = sle[0] if sle else frappe._dict()
		return sle

	def get_sle_after_datetime(self, args):
		"""get Stock Ledger Entries after a particular datetime, for reposting"""
		return get_stock_ledger_entries(args, ">", "asc", for_update=True, check_serial_no=False)

	def raise_exceptions(self):
		msg_list = []
		for warehouse, exceptions in self.exceptions.items():
			deficiency = min(e["diff"] for e in exceptions)

			if (
				exceptions[0]["voucher_type"],
				exceptions[0]["voucher_no"],
			) in frappe.local.flags.currently_saving:
				msg = _("{0} units of {1} needed in {2} to complete this transaction.").format(
					frappe.bold(abs(deficiency)),
					frappe.get_desk_link("Item", exceptions[0]["item_code"], show_title_with_name=True),
					frappe.get_desk_link("Warehouse", warehouse),
				)
			else:
				msg = _(
					"{0} units of {1} needed in {2} on {3} {4} for {5} to complete this transaction."
				).format(
					frappe.bold(abs(deficiency)),
					frappe.get_desk_link("Item", exceptions[0]["item_code"], show_title_with_name=True),
					frappe.get_desk_link("Warehouse", warehouse),
					exceptions[0]["posting_date"],
					exceptions[0]["posting_time"],
					frappe.get_desk_link(exceptions[0]["voucher_type"], exceptions[0]["voucher_no"]),
				)

			if msg:
				if self.reserved_stock:
					allowed_qty = abs(exceptions[0]["actual_qty"]) - abs(exceptions[0]["diff"])

					if allowed_qty > 0:
						msg = "{} As {} units are reserved for other sales orders, you are allowed to consume only {} units.".format(
							msg, frappe.bold(self.reserved_stock), frappe.bold(allowed_qty)
						)
					else:
						msg = f"{msg} As the full stock is reserved for other sales orders, you're not allowed to consume the stock."

				msg_list.append(msg)
		if msg_list:
			message = "\n\n".join(msg_list)
			if self.verbose:
				
				frappe.throw(message, NegativeStockError, title=_("Insufficient Stock"))
			else:
				raise NegativeStockError(message)
	def update_bin(self):
		cost_zones = {}
		for warehouse, data in self.data.items():
			cost_zone = frappe.db.get_value("Warehouse", warehouse, "custom_cost_zone")
			if not cost_zone:
				frappe.throw(f"Cost Zone not set for Warehouse {warehouse}")

			if cost_zone not in cost_zones:
				cost_zones[cost_zone] = {
					"warehouses_with_transactions": {},
					"valuation_rate": None,
				}

			cost_zones[cost_zone]["warehouses_with_transactions"][warehouse] = data

			if data.valuation_rate is not None:
				cost_zones[cost_zone]["valuation_rate"] = data.valuation_rate

		for cost_zone, zone_data in cost_zones.items():
			all_warehouses_in_zone = frappe.db.get_all(
				"Warehouse",
				filters={"custom_cost_zone": cost_zone},
				fields=["name"],
			)

			valuation_rate = zone_data["valuation_rate"]
			warehouses_with_transactions = zone_data["warehouses_with_transactions"]

			for warehouse_data in all_warehouses_in_zone:
				warehouse = warehouse_data.name
				bin_name = get_or_make_bin(self.item_code, warehouse)

				updated_values = {}

				# If this warehouse had a transaction, update quantity and stock_value
				if warehouse in warehouses_with_transactions:
					transaction_data = warehouses_with_transactions[warehouse]
					updated_values.update({
						"actual_qty": transaction_data.qty_after_transaction,
						"stock_value": transaction_data.stock_value,
					})
				else:
					if valuation_rate is not None:
						current_bin_data = frappe.db.get_value(
							"Bin",
							bin_name,
							["actual_qty"],
							as_dict=True,
						)

						if current_bin_data and current_bin_data.actual_qty:
							new_stock_value = current_bin_data.actual_qty * valuation_rate
							updated_values["stock_value"] = new_stock_value

				if valuation_rate is not None:
					updated_values["valuation_rate"] = valuation_rate

				if updated_values:
					frappe.db.set_value("Bin", bin_name, updated_values, update_modified=True)

	def update_bin_data(self, sle):
		cost_zone = frappe.db.get_value("Warehouse", sle.warehouse, "custom_cost_zone")

		if not cost_zone:
			frappe.throw(f"Cost Zone not set for Warehouse {sle.warehouse}")

		warehouses_in_zone = frappe.db.get_all(
			"Warehouse",
			filters={"custom_cost_zone": cost_zone},
			fields=["name"],
		)

		for warehouse_data in warehouses_in_zone:
			warehouse = warehouse_data.name
			bin_name = get_or_make_bin(sle.item_code, warehouse)

			values_to_update = {}

			if warehouse == sle.warehouse:
				values_to_update.update({
					"actual_qty": sle.qty_after_transaction,
					"stock_value": sle.stock_value,
				})
			else:
				if sle.valuation_rate is not None:
					current_bin_data = frappe.db.get_value(
						"Bin",
						bin_name,
						["actual_qty"],
						as_dict=True,
					)

					if current_bin_data and current_bin_data.actual_qty:
						new_stock_value = current_bin_data.actual_qty * sle.valuation_rate
						values_to_update["stock_value"] = new_stock_value

			if sle.valuation_rate is not None:
				values_to_update["valuation_rate"] = sle.valuation_rate

			if values_to_update:
				frappe.db.set_value("Bin", bin_name, values_to_update)




# The rest of the code remains the same as in the original file...
# Including all the utility functions like get_previous_sle_of_current_voucher, 
# get_previous_sle, get_stock_ledger_entries, etc.


def get_valuation_rate(
	item_code,
	warehouse,
	voucher_type,
	voucher_no,
	allow_zero_rate=False,
	currency=None,
	company=None,
	raise_error_if_no_rate=True,
	batch_no=None,
	serial_and_batch_bundle=None,
):
	from erpnext.stock.serial_batch_bundle import BatchNoValuation

	if not company:
		company = frappe.get_cached_value("Warehouse", warehouse, "company")

	if warehouse and batch_no and frappe.db.get_value("Batch", batch_no, "use_batchwise_valuation"):
		table = frappe.qb.DocType("Stock Ledger Entry")
		query = (
			frappe.qb.from_(table)
			.select(Sum(table.stock_value_difference) / Sum(table.actual_qty))
			.where(
				(table.item_code == item_code)
				& (table.warehouse == warehouse)
				& (table.batch_no == batch_no)
				& (table.is_cancelled == 0)
				& (table.voucher_no != voucher_no)
				& (table.voucher_type != voucher_type)
			)
		)

		last_valuation_rate = query.run()
		if last_valuation_rate:
			return flt(last_valuation_rate[0][0])

	if warehouse and serial_and_batch_bundle:
		sabb = frappe.db.get_value(
			"Serial and Batch Bundle", serial_and_batch_bundle, ["posting_date", "posting_time"], as_dict=True
		)
		batch_obj = BatchNoValuation(
			sle=frappe._dict(
				{
					"item_code": item_code,
					"warehouse": warehouse,
					"actual_qty": -1,
					"serial_and_batch_bundle": serial_and_batch_bundle,
					"posting_date": sabb.posting_date,
					"posting_time": sabb.posting_time,
				}
			)
		)

		return batch_obj.get_incoming_rate()

	try:
		cost_zone = frappe.db.get_value("Warehouse", warehouse, "custom_cost_zone")
		
		if cost_zone:
			zone_data = get_cost_zone_stock_data(item_code, cost_zone, company)
			
			if zone_data and zone_data.get("avg_valuation_rate"):
				return flt(zone_data.get("avg_valuation_rate"))
			
			warehouses_in_zone = get_warehouses_in_cost_zone(cost_zone, company)
			
			if warehouses_in_zone:
				bin_data = frappe.db.sql("""
					SELECT 
						SUM(actual_qty) as total_qty,
						SUM(stock_value) as total_stock_value
					FROM `tabBin`
					WHERE item_code = %s 
					AND warehouse IN ({})
					AND actual_qty > 0
				""".format(','.join(['%s'] * len(warehouses_in_zone))), 
				[item_code] + warehouses_in_zone, as_dict=1)
				
				if bin_data and bin_data[0] and bin_data[0].total_qty:
					total_qty = flt(bin_data[0].total_qty)
					total_stock_value = flt(bin_data[0].total_stock_value)
					if total_qty > 0:
						return flt(total_stock_value / total_qty)
		
		last_valuation_rate = frappe.db.sql("""
			select valuation_rate
			from `tabStock Ledger Entry`
			where
				item_code = %s
				AND warehouse = %s
				AND valuation_rate >= 0
				AND is_cancelled = 0
				AND NOT (voucher_no = %s AND voucher_type = %s)
			order by posting_datetime desc, creation desc limit 1""",
			(item_code, warehouse, voucher_no, voucher_type),
		)
		
		if last_valuation_rate:
			return flt(last_valuation_rate[0][0])
			
	except Exception as e:
		frappe.log_error(f"Error calculating cost zone valuation rate: {str(e)}")
		
		last_valuation_rate = frappe.db.sql("""
			select valuation_rate
			from `tabStock Ledger Entry`
			where
				item_code = %s
				AND warehouse = %s
				AND valuation_rate >= 0
				AND is_cancelled = 0
				AND NOT (voucher_no = %s AND voucher_type = %s)
			order by posting_datetime desc, creation desc limit 1""",
			(item_code, warehouse, voucher_no, voucher_type),
		)
		
		if last_valuation_rate:
			return flt(last_valuation_rate[0][0])

	valuation_rate = frappe.db.get_value("Item", item_code, "valuation_rate")

	if not valuation_rate:
		valuation_rate = frappe.db.get_value("Item", item_code, "standard_rate")

		if not valuation_rate:
			valuation_rate = frappe.db.get_value(
				"Item Price", dict(item_code=item_code, buying=1, currency=currency), "price_list_rate"
			)

	if (
		not allow_zero_rate
		and not valuation_rate
		and raise_error_if_no_rate
		and cint(erpnext.is_perpetual_inventory_enabled(company))
	):
		form_link = get_link_to_form("Item", item_code)

		message = _(
			"Valuation Rate for the Item {0}, is required to do accounting entries for {1} {2}."
		).format(form_link, voucher_type, voucher_no)
		message += "<br><br>" + _("Here are the options to proceed:")
		solutions = (
			"<li>"
			+ _(
				"If the item is transacting as a Zero Valuation Rate item in this entry, please enable 'Allow Zero Valuation Rate' in the {0} Item table."
			).format(voucher_type)
			+ "</li>"
		)
		solutions += (
			"<li>"
			+ _("If not, you can Cancel / Submit this entry")
			+ " {} ".format(frappe.bold("after"))
			+ _("performing either one below:")
			+ "</li>"
		)
		sub_solutions = "<ul><li>" + _("Create an incoming stock transaction for the Item.") + "</li>"
		sub_solutions += "<li>" + _("Mention Valuation Rate in the Item master.") + "</li></ul>"
		msg = message + solutions + sub_solutions + "</li>"

		frappe.throw(msg=msg, title=_("Valuation Rate Missing"))

	return valuation_rate


def get_cost_zone_valuation_rate_from_bins(item_code, cost_zone, company=None):
	"""Calculate valuation rate from current bin data across the cost zone"""
	warehouses = get_warehouses_in_cost_zone(cost_zone, company)
	
	if not warehouses:
		return 0
	bin_data = frappe.db.sql("""
		SELECT 
			SUM(actual_qty) as total_qty,
			SUM(stock_value) as total_stock_value
		FROM `tabBin`
		WHERE item_code = %s 
		AND warehouse IN ({})
		AND actual_qty > 0
	""".format(','.join(['%s'] * len(warehouses))), 
	[item_code] + warehouses, as_dict=1)
	
	if bin_data and bin_data[0] and bin_data[0].total_qty:
		total_qty = flt(bin_data[0].total_qty)
		total_stock_value = flt(bin_data[0].total_stock_value)
		if total_qty > 0:
			return flt(total_stock_value / total_qty)
	
	return 0


















