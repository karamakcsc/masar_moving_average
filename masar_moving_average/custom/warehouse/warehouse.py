import frappe 

def validate(self , method): 
    # remove_warehouse_from_other_cost_zones(self)
    if not self.is_new():
        remove_warehouse_from_other_cost_zones(self)
    add_warehouse_to_cost_zone(self)
    
def on_trash(self , method):
    remove_warehouse_from_other_cost_zones_on_delete(self)
    
def after_insert(self, method):
    add_warehouse_to_cost_zone(self)
    
def add_warehouse_to_cost_zone(self):
    if not self.custom_cost_zone:
        return
    
    cost_zone = frappe.get_doc("Cost Zone", self.custom_cost_zone)
    if not any(w.warehouse == self.name for w in cost_zone.warehouses):
        cost_zone.append("warehouses", {
            "warehouse": self.name
        })
        cost_zone.save()
        
def remove_warehouse_from_other_cost_zones(self):
    wh_exist = frappe.db.sql("""
        SELECT parent FROM `tabCost Zone Details`
        WHERE warehouse = %s
    """, (self.name,), as_dict=1)
    
    for wh in wh_exist:
        cost_zone = frappe.get_doc("Cost Zone", wh.parent)
        cost_zone.warehouses = [w for w in cost_zone.warehouses if w.warehouse != self.name]
        cost_zone.save()
        
def remove_warehouse_from_other_cost_zones_on_delete(self):
    wh_exist = frappe.db.sql("""
        SELECT parent FROM `tabCost Zone Details`
        WHERE warehouse = %s
    """, (self.name,), as_dict=1)

    for wh in wh_exist:
        cost_zone = frappe.get_doc("Cost Zone", wh.parent)
        cost_zone.warehouses = [w for w in cost_zone.warehouses if w.warehouse != self.name]
        cost_zone.save()
    