import pandas as pd
import sqlite3

def load_to_database(df, table_name, db_name="graph_etl_warehouse.db"):
    print(f"\n[*] --- Loading data into table: {table_name} ---")
    
    try:
        # Establish connection to a local SQLite database
        conn = sqlite3.connect(db_name)

        # Load DataFrame into the database (replace table if it already exists)
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        print(f"[+] Successfully loaded {len(df)} records into '{table_name}'.")
        
        # Close the connection
        conn.close()
    except Exception as e:
        print(f"[ERROR] Database load failed. Reason: {e}")
        raise