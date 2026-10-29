import pandas as pd
import numpy as np

def transform_data(df, dataset_name):
    # Create an explicit copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Remove exact duplicate records
    initial_rows = len(df)
    df = df.drop_duplicates().copy()
    dropped = initial_rows - len(df)

    # Fill missing values for numerical columns using median
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df.loc[:, col] = df[col].fillna(median_val)

    # Fill missing values for string/object columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df.loc[:, col] = df[col].fillna("Unknown")
        df.loc[:, col] = df[col].astype(str).str.strip()

    print(f"    [+] Transformed {dataset_name}: {len(df)} valid rows ({dropped} duplicates removed).")
    return df