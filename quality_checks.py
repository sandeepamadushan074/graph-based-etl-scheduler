import pandas as pd

def run_quality_checks(df, dataset_name):
    print(f"\n[*] --- Running Quality Checks for: {dataset_name} ---")
    is_valid = True

    # 1. Check if the dataframe is empty
    if df.empty:
        print(f"[ERROR] Validation Failed: {dataset_name} contains no data.")
        return False

    # 2. Check for duplicate IDs (if 'id' column exists)
    if 'id' in df.columns:
        duplicate_ids = df['id'].duplicated().sum()
        if duplicate_ids > 0:
            print(f"[ERROR] Validation Failed: Found {duplicate_ids} duplicate IDs.")
            is_valid = False

    # 3. Check for any remaining null values
    total_nulls = df.isnull().sum().sum()
    if total_nulls > 0:
        print(f"[WARNING] Found {total_nulls} missing values across the dataset.")
        is_valid = False

    # Final validation verdict
    if is_valid:
        print(f"[+] {dataset_name} passed all data quality checks.")
    else:
        print(f"[-] {dataset_name} failed one or more quality checks.")

    return is_valid