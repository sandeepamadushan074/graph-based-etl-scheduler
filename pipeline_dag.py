import time
from extractor import extract_from_csv
from transformer import transform_data
from quality_checks import run_quality_checks
from loader import load_to_database

def build_retail_etl_dag(scheduler, state):
    def run_step(delay, fn):
        time.sleep(delay)
        return fn()

    scheduler.add_task("Ext_Cust", lambda: run_step(0.15, lambda: state.update({"cust_raw": extract_from_csv("data/customers.csv")})), 0.15, 0)
    scheduler.add_task("Ext_Prod", lambda: run_step(0.12, lambda: state.update({"prod_raw": extract_from_csv("data/products.csv")})), 0.12, 0)
    scheduler.add_task("Ext_Store", lambda: run_step(0.08, lambda: state.update({"store_raw": extract_from_csv("data/stores.csv")})), 0.08, 0)
    scheduler.add_task("Ext_Sales", lambda: run_step(0.30, lambda: state.update({"sales_raw": extract_from_csv("data/sales.csv")})), 0.30, 0)

    scheduler.add_task("Trf_Cust", lambda: run_step(0.18, lambda: state.update({"cust_clean": transform_data(state["cust_raw"], "Customers")})), 0.18, 1, ["Ext_Cust"])
    scheduler.add_task("Trf_Prod", lambda: run_step(0.14, lambda: state.update({"prod_clean": transform_data(state["prod_raw"], "Products")})), 0.14, 1, ["Ext_Prod"])
    scheduler.add_task("Trf_Store", lambda: run_step(0.10, lambda: state.update({"store_clean": transform_data(state["store_raw"], "Stores")})), 0.10, 1, ["Ext_Store"])
    scheduler.add_task("Trf_Sales", lambda: run_step(0.35, lambda: state.update({"sales_clean": transform_data(state["sales_raw"], "Sales")})), 0.35, 1, ["Ext_Sales"])

    def make_validator(key, name):
        def _val():
            time.sleep(0.10)
            if not run_quality_checks(state[key], name):
                raise RuntimeError(f"Quality Gate failed on {name}")
        return _val

    scheduler.add_task("Val_Cust", make_validator("cust_clean", "Customers"), 0.10, 2, ["Trf_Cust"])
    scheduler.add_task("Val_Prod", make_validator("prod_clean", "Products"), 0.10, 2, ["Trf_Prod"])
    scheduler.add_task("Val_Store", make_validator("store_clean", "Stores"), 0.10, 2, ["Trf_Store"])
    scheduler.add_task("Val_Sales", make_validator("sales_clean", "Sales"), 0.15, 2, ["Trf_Sales"])

    def join_warehouse_tables():
        time.sleep(0.25)
        sales = state["sales_clean"]
        cust = state["cust_clean"].rename(columns={"id": "customer_id"})
        prod = state["prod_clean"].rename(columns={"id": "product_id"})
        store = state["store_clean"].rename(columns={"id": "store_id"})

        merged = sales.merge(cust, on="customer_id", how="left")
        merged = merged.merge(prod, on="product_id", how="left")
        merged = merged.merge(store, on="store_id", how="left")
        merged["net_amount"] = merged["quantity"] * merged["unit_price"] * (1.0 - merged["discount"])
        state["dw_fact"] = merged
        print(f"    [+] Joined Star-Schema Fact Table: {len(merged)} enriched records ready.")

    scheduler.add_task(
        "Join_DW", join_warehouse_tables, 0.25, 3,
        ["Val_Cust", "Val_Prod", "Val_Store", "Val_Sales"]
    )

    scheduler.add_task(
        "Load_DW",
        lambda: run_step(0.20, lambda: load_to_database(state["dw_fact"], "fact_retail_sales")),
        0.20, 4, ["Join_DW"]
    )