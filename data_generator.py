import os
import numpy as np
import pandas as pd

def generate_retail_datasets(scale_rows, inject_fatal_error=False):
    os.makedirs("data", exist_ok=True)
    np.random.seed(42)

    n_cust = max(500, scale_rows // 10)
    n_prod = max(200, scale_rows // 25)
    n_store = 50
    n_sales = scale_rows

    # 1. Customers Dataset
    cust_df = pd.DataFrame({
        "ID": np.random.randint(1, n_cust + 1, size=n_cust),
        "Customer Name": [f"  Customer_{i} " if i % 15 != 0 else None for i in range(n_cust)],
        "Loyalty Score": [np.random.uniform(10, 100) if i % 20 != 0 else np.nan for i in range(n_cust)]
    }).drop_duplicates(subset=["ID"])
    cust_df.to_csv("data/customers.csv", index=False)

    # 2. Products Dataset
    prod_df = pd.DataFrame({
        "ID": np.arange(1, n_prod + 1),
        "Product Category": np.random.choice(["Electronics ", " Apparel", "Home", None], size=n_prod),
        "Unit Price": np.random.uniform(5.0, 500.0, size=n_prod)
    })
    if inject_fatal_error:
        # Inject duplicate primary keys to trigger Data Quality Gate failure
        prod_df.loc[0:10, "ID"] = 999999
    prod_df.to_csv("data/products.csv", index=False)

    # 3. Stores Dataset
    store_df = pd.DataFrame({
        "ID": np.arange(1, n_store + 1),
        "Store Region": np.random.choice(["Colombo", "Kandy", "Galle", "Jaffna"], size=n_store),
        "Floor Space": np.random.randint(1000, 5000, size=n_store)
    })
    store_df.to_csv("data/stores.csv", index=False)

    # 4. Sales Fact Dataset
    sales_df = pd.DataFrame({
        "ID": np.arange(1, n_sales + 1),
        "Customer ID": np.random.choice(cust_df["ID"], size=n_sales),
        "Product ID": np.random.choice(prod_df["ID"], size=n_sales),
        "Store ID": np.random.choice(store_df["ID"], size=n_sales),
        "Quantity": np.random.randint(1, 10, size=n_sales),
        "Discount": [np.random.uniform(0, 0.3) if i % 10 != 0 else np.nan for i in range(n_sales)]
    })
    sales_df.to_csv("data/sales.csv", index=False)