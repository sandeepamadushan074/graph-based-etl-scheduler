import pandas as pd
import os

def extract_from_csv(file_path):
    # Check if the file exists before attempting to read
    if not os.path.exists(file_path):
        print(f"[ERROR] The data file {file_path} was not found.")
        raise FileNotFoundError(f"Missing file: {file_path}")

    print(f"[*] Starting extraction from {file_path}...")
    
    try:
        df = pd.read_csv(file_path)
        print(f"[+] Successfully extracted {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read the CSV file. Reason: {e}")
        raise