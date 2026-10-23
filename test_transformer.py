import pandas as pd
import numpy as np

# Import the core transformation function
from transformer import transform_data

def test_full_transform_pipeline():
    """
    Test the data transformation logic to ensure it normalizes columns,
    removes duplicates, fills null values, and strips strings correctly.
    """
    
    # 1. Setup messy dummy data
    dummy_data = {
        "  Customer ID  ": [1, 2, 2, 3],
        " Product Name": [" Laptop ", "Mouse", "Mouse", np.nan],
        "Price ": [1000.0, 25.0, 25.0, np.nan]
    }
    df = pd.DataFrame(dummy_data)

    # 2. Run the transformation function
    clean_df = transform_data(df, "Test Dataset")

    # 3. Assertions (Validation Checks)

    # Check column normalization
    assert list(clean_df.columns) == ["customer_id", "product_name", "price"], \
        "Column names were not standardized correctly."

    # Check duplicate removal (Initial 4 rows should become 3 rows)
    assert len(clean_df) == 3, "Duplicate rows were not removed."

    # Check null value handling for numerics (Price median should replace NaN)
    assert not clean_df["price"].isnull().any(), "Null values remain in numeric columns."
    
    # Check string stripping and fillna
    assert clean_df["product_name"].iloc[0] == "Laptop", "String spaces were not stripped properly."
    assert "Unknown" in clean_df["product_name"].values, "Null strings were not replaced with 'Unknown'."

if __name__ == "__main__":
    try:
        test_full_transform_pipeline()
        print("\n[+] All transformation unit tests passed successfully!")
    except AssertionError as e:
        print(f"\n[ERROR] Test Failed: {e}")