import pandas as pd
import numpy as np

def transform_data(df, dataset_name):
    print(f"\n[*] --- Transforming Dataset: {dataset_name} ---")
    
    # Standardize column names (lowercase and replace spaces with underscores)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Remove exact duplicate records
    initial_rows = len(df)
    df = df.drop_duplicates()
    print(f"[*] Dropped {initial_rows - len(df)} duplicate rows.")

    # Fill missing values for numerical columns using the median
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)

    # Fill missing values for string/object columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].fillna("Unknown")
        # Remove leading/trailing spaces
        df[col] = df[col].str.strip()

    print(f"[+] Transformation complete. Current shape: {df.shape}")
    return df