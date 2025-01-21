
# import os
# import pandas as pd
# import psycopg2
# import re
# from psycopg2 import sql

# def sanitize_column_name(col_name):
#     if isinstance(col_name, str):
#         return re.sub(r'\W+', '_', col_name).lower()
#     else:
#         return col_name 

# # def clean_data(df):
# #     df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
# #     return df
# def clean_data(df):
#     df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
#     return df


# def identify_primary_key(df):
#     for col in df.columns:
#         if df[col].is_unique:
#             return col
#     return None

# UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'csv')

# def upload_csv_to_postgresql(db_name, username, password, csv_file_name, host='localhost', port='5432'):
#     current_dir = os.getcwd()
#     csv_file_path = os.path.join(current_dir, csv_file_name)
#     conn = psycopg2.connect(dbname=db_name, user=username, password=password, host=host, port=port)
    
#     if not csv_file_path.lower().endswith('.csv'):
#         print("Error: Only CSV files with .csv extension are supported.")
#         return

#     df = pd.read_csv(csv_file_path)
#     directory_name = os.path.splitext(os.path.basename(csv_file_name))[0]
#     directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
#     os.makedirs(directory_path, exist_ok=True)

#     table_name = sanitize_column_name(os.path.splitext(os.path.basename(csv_file_name))[0])
#     cur = conn.cursor()

#     # Clean the data
#     df = clean_data(df)

#     # Sanitize column names
#     df.columns = [sanitize_column_name(col) for col in df.columns]

#     # Identify primary key
#     primary_key_column = identify_primary_key(df)
#     if primary_key_column is None:
#         print("No unique column found to be used as primary key. Adding a new 'id' column.")
#         df.insert(0, 'id', range(1, len(df) + 1))
#         primary_key_column = 'id'

#     primary_key_column = sanitize_column_name(primary_key_column)

#     # Define table columns with data types
#     columns = ', '.join('"{}" VARCHAR'.format(col) for col in df.columns)
#     create_table_query = 'CREATE TABLE IF NOT EXISTS "{}" ({}, PRIMARY KEY ({}));'.format(table_name, columns, primary_key_column)
#     cur.execute(create_table_query)
    
    
#     for _, row in df.iterrows():
#         primary_key_value = str(row[primary_key_column])
#         print(f"Deleting row with {primary_key_column} = {primary_key_value}")
#         cur.execute('DELETE FROM "{}" WHERE "{}"=%s'.format(table_name, primary_key_column), (primary_key_value,))

#     # Print how many rows were deleted
#     print(f"Deleted {len(df)} rows with matching primary key values in table '{table_name}'.")

#     # Insert new rows
#     for _, row in df.iterrows():
#         cur.execute('INSERT INTO "{}" ({}) VALUES ({});'.format(table_name, ', '.join(['"{}"'.format(col) for col in df.columns]), ', '.join(["%s" for _ in row])), tuple(row))

#     print("Successfully inserted new data into table '{}'.".format(table_name))


#     # Check if the 'datasource' table exists and create it if it does not
#     cur.execute("""
#         SELECT EXISTS (
#             SELECT FROM information_schema.tables
#             WHERE table_name = 'datasource'
#         );
#     """)
#     table_exists = cur.fetchone()[0]
    
#     if not table_exists:
#         cur.execute("""
#             CREATE TABLE datasource (
#                 id SERIAL PRIMARY KEY,
#                 data_source_name VARCHAR(255),
#                 data_source_path VARCHAR(255)
#             );
#         """)
#         print("Created 'datasource' table.")

#     # Insert into 'datasource'
#     insert_query = sql.SQL("""
#         INSERT INTO datasource (data_source_name, data_source_path)
#         VALUES (%s, %s)
#     """)
#     cur.execute(insert_query, (directory_name, directory_path))

#     conn.commit()
#     cur.close()

#     # Save the DataFrame to the directory
#     file_name = "{}.csv".format(table_name)
#     file_path = os.path.join(directory_path, file_name)
#     df.to_csv(file_path, index=False)

#     conn.close()

import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql

from upload import is_table_used_in_charts 

# Sanitize column names by replacing non-alphanumeric characters with underscores and converting to lowercase
def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name 


# Clean DataFrame by stripping whitespace from string columns
def clean_data(df):
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df


# Identify a unique column to use as a primary key
def identify_primary_key(df):
    for col in df.columns:
        if df[col].is_unique:
            return col
    return None
def check_table_usage(cur,table_name):
    
    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    # Remove any surrounding quotes from the table name
    table_name = table_name.strip('"').strip("'")

    # Debugging: Print the received table name
    print(f"Received table name: {table_name}")

    # Check if the table is used for chart creation
    is_in_use = is_table_used_in_charts(table_name)
    print("is_in_use",is_in_use)
    return is_in_use

# Function to validate if the table exists
def validate_table_structure(cur, table_name):
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    table_exists = cur.fetchone()[0]
    return table_exists


# # Function to map pandas dtypes to PostgreSQL types
# def determine_sql_data_type(value):
#     if pd.api.types.is_string_dtype(value):
#         return 'VARCHAR'
#     elif pd.api.types.is_integer_dtype(value):
#         return 'INTEGER'
#     elif pd.api.types.is_float_dtype(value):
#         return 'DOUBLE PRECISION'
#     elif pd.api.types.is_bool_dtype(value):
#         return 'BOOLEAN'
#     elif pd.api.types.is_datetime64_any_dtype(value):
#         return 'DATE'
#     else:
#         return 'VARCHAR'

def preprocess_dates(df):
    # Identify columns that are of string type
    date_columns = [
        col for col in df.columns if df[col].dtype == 'object' and df[col].str.match(r"^\d{1,2}-\d{1,2}-\d{4}$").any()
    ]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    return df

def determine_sql_data_type(value):
    # Check for string values resembling dates
    date_pattern = r"^\d{1,2}[-/]\d{1,2}[-/]\d{2,4}$"
    if pd.api.types.is_string_dtype(value):
        if value.str.match(date_pattern).any():
            return 'DATE'
        return 'VARCHAR'
    elif pd.api.types.is_integer_dtype(value):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(value):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(value):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(value):
        return 'DATE'
    else:
        return 'VARCHAR'

# Function to check for duplicate columns
def check_repeating_columns(df):
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    return duplicate_columns


UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'csv')


def upload_csv_to_postgresql(db_name, username, password, csv_file_name, host='localhost', port='5432'):
    current_dir = os.getcwd()
    csv_file_path = os.path.join(current_dir, csv_file_name)
    conn = psycopg2.connect(dbname=db_name, user=username, password=password, host=host, port=port)
    
    if not csv_file_path.lower().endswith('.csv'):
        print("Error: Only CSV files with .csv extension are supported.")
        return

    df = pd.read_csv(csv_file_path)
    df = preprocess_dates(df)
    
    duplicate_columns = check_repeating_columns(df)
    if duplicate_columns:
        print(f"Error: Duplicate columns found in the CSV: {duplicate_columns}")
        return

    directory_name = os.path.splitext(os.path.basename(csv_file_name))[0]
    directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
    os.makedirs(directory_path, exist_ok=True)

    table_name = sanitize_column_name(os.path.splitext(os.path.basename(csv_file_name))[0])
    cur = conn.cursor()

    # Clean the data
    df = clean_data(df)

    # Sanitize column names
    df.columns = [sanitize_column_name(col) for col in df.columns]

    # Identify primary key
    primary_key_column = identify_primary_key(df)
    if primary_key_column is None:
        print("No unique column found to be used as primary key. Adding a new 'id' column.")
        df.insert(0, 'id', range(1, len(df) + 1))
        primary_key_column = 'id'

    primary_key_column = sanitize_column_name(primary_key_column)

    # Validate table structure
    table_exists = validate_table_structure(cur, table_name)

    
    # if not table_exists:
    #     # Define table columns with data types
    #     columns = ', '.join('"{}" {}'.format(col, determine_sql_data_type(df[col])) for col in df.columns)
    #     create_table_query = 'CREATE TABLE IF NOT EXISTS "{}" ({}, PRIMARY KEY ({}));'.format(table_name, columns, primary_key_column)
    #     cur.execute(create_table_query)
    # else:
    #     print(f"Table '{table_name}' already exists.")
    if table_exists:
        is_table_in_use = check_table_usage(cur, table_name)
        print(f"Table '{table_name}' is in use: {is_table_in_use}")

        
        if table_exists and is_table_in_use == False:
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
            existing_columns = [row[0] for row in cur.fetchall()]

            # Detect and delete extra columns
            columns_to_delete = [col for col in existing_columns if col not in df.columns]
            for col in columns_to_delete:
                alter_query = sql.SQL('ALTER TABLE {} DROP COLUMN {}').format(
                    sql.Identifier(table_name),
                    sql.Identifier(col)
                )
                try:
                    cur.execute(alter_query)
                    print(f"Deleted column '{col}' from table '{table_name}'.")
                except Exception as e:
                    print(f"Error deleting column '{col}': {e}")
            
            # Detect and add missing columns
            missing_columns = [col for col in df.columns if col not in existing_columns]
            for col in missing_columns:
                col_type = determine_sql_data_type(df[col])
                alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                    sql.Identifier(table_name),
                    sql.Identifier(col),
                    sql.SQL(col_type)
                )
                try:
                    cur.execute(alter_query)
                    print(f"Added column '{col}' to table '{table_name}'.")
                except Exception as e:
                    print(f"Error adding column '{col}': {e}")

    else:
        # Create table if it does not exist
        columns = ', '.join('"{}" {}'.format(col, determine_sql_data_type(df[col])) for col in df.columns)
        create_table_query = 'CREATE TABLE IF NOT EXISTS "{}" ({}, PRIMARY KEY ({}));'.format(table_name, columns, primary_key_column)
        try:
            cur.execute(create_table_query)
            print(f"Created table '{table_name}'.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            return

    # Delete existing rows matching primary key
    for _, row in df.iterrows():
        primary_key_value = str(row[primary_key_column])
        cur.execute('DELETE FROM "{}" WHERE "{}"=%s'.format(table_name, primary_key_column), (primary_key_value,))

    print(f"Deleted {len(df)} rows with matching primary key values in table '{table_name}'.")

    # Insert new rows
    for _, row in df.iterrows():
        cur.execute('INSERT INTO "{}" ({}) VALUES ({});'.format(
            table_name,
            ', '.join(['"{}"'.format(col) for col in df.columns]),
            ', '.join(["%s" for _ in row])
        ), tuple(row))

    print(f"Successfully inserted new data into table '{table_name}'.")

    # Check if the 'datasource' table exists and create it if it does not
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'datasource'
        );
    """)
    table_exists = cur.fetchone()[0]
    
    if not table_exists:
        cur.execute("""
            CREATE TABLE datasource (
                id SERIAL PRIMARY KEY,
                data_source_name VARCHAR(255),
                data_source_path VARCHAR(255)
            );
        """)
        print("Created 'datasource' table.")

    # Insert into 'datasource'
    insert_query = sql.SQL("""
        INSERT INTO datasource (data_source_name, data_source_path)
        VALUES (%s, %s)
    """)
    cur.execute(insert_query, (directory_name, directory_path))

    conn.commit()
    cur.close()

    # Save the DataFrame to the directory
    file_name = "{}.csv".format(table_name)
    file_path = os.path.join(directory_path, file_name)
    df.to_csv(file_path, index=False)

    conn.close()
    print("File successfully uploaded and saved.")
