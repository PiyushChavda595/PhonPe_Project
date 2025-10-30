# etl_script.py
import os
import git
import json
import pandas as pd
import mysql.connector

# --- Database Credentials ---
DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "admin" # Using "admin" as requested
DB_NAME = "phonepe_pulse"

# --- GitHub Repository ---
REPO_URL = "https://github.com/PhonePe/pulse.git"
REPO_DIR = "pulse"

def clone_data_repo():
    """Clones the PhonePe Pulse GitHub repository if it doesn't already exist."""
    if not os.path.exists(REPO_DIR):
        print(f"Cloning repository: {REPO_URL}...")
        try:
            git.Repo.clone_from(REPO_URL, REPO_DIR)
            print("Repository cloned successfully.")
        except git.GitCommandError as e:
            print(f"Error cloning repository: {e}")
            # Optionally, exit or raise the error if cloning is critical
    else:
        print(f"Repository '{REPO_DIR}' already exists. Skipping clone.")

def create_database_and_tables():
    """Creates the database and tables if they don't exist."""
    conn = None # Initialize conn to None
    cursor = None # Initialize cursor to None
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        print("MySQL connection established.")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.database = DB_NAME
        print(f"Database '{DB_NAME}' selected/created.")
        print("Creating tables (if they don't exist)...")
        # Aggregated Tables
        cursor.execute("CREATE TABLE IF NOT EXISTS aggregated_transaction (State VARCHAR(255), Year INT, Quarter INT, Transaction_type VARCHAR(255), Transaction_count BIGINT, Transaction_amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, Transaction_type))")
        cursor.execute("CREATE TABLE IF NOT EXISTS aggregated_user (State VARCHAR(255), Year INT, Quarter INT, Brand VARCHAR(255), Transaction_count BIGINT, Percentage DECIMAL(10, 5), PRIMARY KEY (State, Year, Quarter, Brand))")
        cursor.execute("CREATE TABLE IF NOT EXISTS aggregated_insurance (State VARCHAR(255), Year INT, Quarter INT, Name VARCHAR(255), Count BIGINT, Amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, Name))")
        # Map Tables
        cursor.execute("CREATE TABLE IF NOT EXISTS map_transaction (State VARCHAR(255), Year INT, Quarter INT, District VARCHAR(255), Transaction_count BIGINT, Transaction_amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, District))")
        cursor.execute("CREATE TABLE IF NOT EXISTS map_user (State VARCHAR(255), Year INT, Quarter INT, District VARCHAR(255), RegisteredUsers BIGINT, AppOpens BIGINT, PRIMARY KEY (State, Year, Quarter, District))")
        cursor.execute("CREATE TABLE IF NOT EXISTS map_insurance (State VARCHAR(255), Year INT, Quarter INT, District VARCHAR(255), Count BIGINT, Amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, District))")
        # Top Tables
        cursor.execute("CREATE TABLE IF NOT EXISTS top_transaction (State VARCHAR(255), Year INT, Quarter INT, Pincode VARCHAR(20), Transaction_count BIGINT, Transaction_amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, Pincode))") # Changed Pincode to VARCHAR
        cursor.execute("CREATE TABLE IF NOT EXISTS top_user (State VARCHAR(255), Year INT, Quarter INT, Pincode VARCHAR(20), RegisteredUsers BIGINT, PRIMARY KEY (State, Year, Quarter, Pincode))") # Changed Pincode to VARCHAR
        cursor.execute("CREATE TABLE IF NOT EXISTS top_insurance (State VARCHAR(255), Year INT, Quarter INT, Pincode VARCHAR(20), Count BIGINT, Amount DECIMAL(30, 2), PRIMARY KEY (State, Year, Quarter, Pincode))") # Changed Pincode to VARCHAR
        conn.commit()
        print("Tables checked/created successfully.")
    except mysql.connector.Error as err:
        print(f"Database Error during setup: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")

def insert_data_into_db(df, table_name):
    """Inserts DataFrame into MySQL table using bulk insert."""
    # Ensure numeric types and handle potential NaNs
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.fillna(0) # Replace NaN with 0, suitable for counts/amounts

    # Convert Pincode to string if it exists and is not already string
    if 'Pincode' in df.columns and not pd.api.types.is_string_dtype(df['Pincode']):
        df['Pincode'] = df['Pincode'].astype(str)

    conn = None # Initialize conn to None
    cursor = None # Initialize cursor to None
    try:
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = conn.cursor()

        # Check if table has data (simple check)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        if cursor.fetchone()[0] > 0:
            print(f"Table '{table_name}' already contains data. Skipping insertion.")
            return

        tuples = [tuple(x) for x in df.to_numpy()]
        cols = '`, `'.join(list(df.columns)) # Properly quote column names
        cols = f"`{cols}`"
        placeholders = ','.join(['%s'] * len(df.columns))
        query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"

        cursor.executemany(query, tuples)
        conn.commit()
        print(f"Data insertion complete for {table_name}.")
    except mysql.connector.Error as err:
        print(f"Error inserting data into {table_name}: {err}")
        if conn:
            conn.rollback() # Rollback on error
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# --- Data Processing Functions ---

def process_aggregated_transaction():
    path = os.path.join(REPO_DIR, "data/aggregated/transaction/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list) # Return empty if path missing
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('transactionData', []):
                                        payment_instrument = item.get('paymentInstruments', [{}])[0]
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'Transaction_type': item.get('name'),
                                            'Transaction_count': payment_instrument.get('count', 0),
                                            'Transaction_amount': payment_instrument.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_aggregated_user():
    path = os.path.join(REPO_DIR, "data/aggregated/user/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    users_by_device = data.get('data', {}).get('usersByDevice') # Safely get data
                                    if users_by_device: # Check if the list exists and is not None
                                        for item in users_by_device:
                                            record = {
                                                'State': state.replace('-', ' ').title(),
                                                'Year': int(year),
                                                'Quarter': int(file.strip('.json')),
                                                'Brand': item.get('brand'),
                                                'Transaction_count': item.get('count', 0),
                                                'Percentage': item.get('percentage', 0.0)
                                            }
                                            data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_aggregated_insurance():
    path = os.path.join(REPO_DIR, "data/aggregated/insurance/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('transactionData', []):
                                        payment_instrument = item.get('paymentInstruments', [{}])[0]
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'Name': item.get('name'),
                                            'Count': payment_instrument.get('count', 0),
                                            'Amount': payment_instrument.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_map_transaction():
    path = os.path.join(REPO_DIR, "data/map/transaction/hover/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('hoverDataList', []):
                                        metric = item.get('metric', [{}])[0]
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'District': item.get('name', '').replace(' district', '').title(),
                                            'Transaction_count': metric.get('count', 0),
                                            'Transaction_amount': metric.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_map_user():
    path = os.path.join(REPO_DIR, "data/map/user/hover/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    hover_data = data.get('data', {}).get('hoverData', {})
                                    for district, values in hover_data.items():
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'District': district.replace(' district', '').title(),
                                            'RegisteredUsers': values.get('registeredUsers', 0),
                                            'AppOpens': values.get('appOpens', 0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_map_insurance():
    path = os.path.join(REPO_DIR, "data/map/insurance/hover/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('hoverDataList', []):
                                        metric = item.get('metric', [{}])[0]
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'District': item.get('name', '').replace(' district', '').title(),
                                            'Count': metric.get('count', 0),
                                            'Amount': metric.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_top_transaction():
    path = os.path.join(REPO_DIR, "data/top/transaction/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('pincodes', []):
                                        metric = item.get('metric', {})
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'Pincode': str(item.get('entityName')), # Ensure pincode is string
                                            'Transaction_count': metric.get('count', 0),
                                            'Transaction_amount': metric.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_top_user():
    path = os.path.join(REPO_DIR, "data/top/user/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('pincodes', []):
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'Pincode': str(item.get('name')), # Ensure pincode is string
                                            'RegisteredUsers': item.get('registeredUsers', 0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)

def process_top_insurance():
    path = os.path.join(REPO_DIR, "data/top/insurance/country/india/state/")
    data_list = []
    if not os.path.exists(path): 
        return pd.DataFrame(data_list)
    states = os.listdir(path)
    for state in states:
        state_path = os.path.join(path, state)
        if os.path.isdir(state_path):
            years = os.listdir(state_path)
            for year in years:
                year_path = os.path.join(state_path, year)
                if os.path.isdir(year_path):
                    files = os.listdir(year_path)
                    for file in files:
                        if file.endswith('.json'):
                            file_path = os.path.join(year_path, file)
                            try:
                                with open(file_path, 'r') as f:
                                    data = json.load(f)
                                    for item in data.get('data', {}).get('pincodes', []):
                                        metric = item.get('metric', {})
                                        record = {
                                            'State': state.replace('-', ' ').title(),
                                            'Year': int(year),
                                            'Quarter': int(file.strip('.json')),
                                            'Pincode': str(item.get('entityName')), # Ensure pincode is string
                                            'Count': metric.get('count', 0),
                                            'Amount': metric.get('amount', 0.0)
                                        }
                                        data_list.append(record)
                            except Exception as e:
                                print(f"Error processing {file_path}: {e}")
    return pd.DataFrame(data_list)


if __name__ == "__main__":
    clone_data_repo()
    create_database_and_tables()

    # --- Define all processing functions and target tables ---
    processing_map = {
        process_aggregated_transaction: "aggregated_transaction",
        process_aggregated_user: "aggregated_user",
        process_aggregated_insurance: "aggregated_insurance",
        process_map_transaction: "map_transaction",
        process_map_user: "map_user",
        process_map_insurance: "map_insurance",
        process_top_transaction: "top_transaction",
        process_top_user: "top_user",
        process_top_insurance: "top_insurance"
    }

    # Process and insert data if repo exists
    if os.path.exists(REPO_DIR):
        for process_func, table_name in processing_map.items():
            print(f"Processing data for {table_name}...")
            try:
                df = process_func()
                if not df.empty:
                    insert_data_into_db(df, table_name)
                else:
                    print(f"No data generated for {table_name}.")
            except Exception as e:
                print(f"Error during processing/insertion for {table_name}: {e}")
    else:
        print(f"Error: Data repository '{REPO_DIR}' not found. Cannot process data.")

    print("\nETL process finished.")