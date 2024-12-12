
import psycopg2
import json
import os
import pandas as pd
import traceback
from psycopg2 import sql

def sanitize_column_name(name):
    # Function to sanitize column names (e.g., remove spaces, special characters)
    return name.strip().replace(" ", "_").lower()

def determine_sql_data_type(series):
    # Function to determine the SQL data type based on pandas dtype
    if pd.api.types.is_integer_dtype(series):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(series):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    else:
        return 'VARCHAR'

def convert_to_correct_type(value, expected_type):
    # Function to convert values to the correct SQL type based on expected type
    if expected_type == 'INTEGER':
        return int(value)
    elif expected_type == 'FLOAT':
        return float(value)
    elif expected_type == 'BOOLEAN':
        return bool(value)
    elif expected_type == 'TIMESTAMP':
        return pd.to_datetime(value)
    else:
        return str(value)

def validate_table_structure(cur, table_name):
    # Check if table exists in the database
    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')")
    return cur.fetchone()[0]

def update_table_structure(cur, table_name, df):
    # Check if columns need to be added or modified in the existing table
    for col in df.columns:
        # Check if column exists
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' AND column_name = '{col}'")
        result = cur.fetchone()

        if result is None:  # Column doesn't exist, so add it
            print(f"Adding missing column '{col}' to table '{table_name}'.")
            column_type = determine_sql_data_type(df[col])
            alter_table_query = f"ALTER TABLE {table_name} ADD COLUMN {col} {column_type}"
            cur.execute(alter_table_query)
        else:
            print(f"Column '{col}' already exists in table '{table_name}'.")

def flatten_json_data(data):
    """
    Flattens a JSON array of objects with nested structures. 
    Converts nested arrays into rows while preserving parent fields.
    """
    def flatten_record(record, parent_key='', parent_row={}):
        """
        Flattens a single JSON object into a row. Handles nested objects and lists.
        """
        rows = []
        for key, value in record.items():
            new_key = f"{parent_key}_{key}" if parent_key else key

            if isinstance(value, dict):
                # Recursively flatten dictionaries
                rows.extend(flatten_record(value, new_key, parent_row))
            elif isinstance(value, list):
                # For lists, create separate rows for each element
                for item in value:
                    if isinstance(item, dict):
                        # Combine parent_row with list element (flattened)
                        rows.extend(flatten_record(item, new_key, parent_row))
                    else:
                        # Handle simple lists (non-dict)
                        row = parent_row.copy()
                        row[new_key] = item
                        rows.append(row)
            else:
                # For scalar values, add to the current row
                parent_row[new_key] = value

        # Append the final row for non-list elements
        if not rows:
            rows.append(parent_row.copy())

        return rows

    # Flatten each record in the JSON array
    flattened_rows = []
    for record in data:
        flattened_rows.extend(flatten_record(record))

    # Convert to pandas DataFrame
    return pd.DataFrame(flattened_rows)

def upload_json_to_postgresql(database_name, username, password, json_file_path, primary_key_column, host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(dbname=database_name, user=username, password=password, host=host, port=port)
        cur = conn.cursor()

        with open(json_file_path, 'r') as f:
            data = json.load(f)

        if isinstance(data, list):
            json_array = data
        elif isinstance(data, dict):
            # If it's a dictionary, get all values (we assume the first key contains the array)
            if len(data) == 1:  # Ensure there is only one key in the dictionary
                json_array = list(data.values())[0]  # Get the first value
                if isinstance(json_array, dict):
                    json_array = [json_array]  # Convert dictionary to list if it's not already a list
                elif not isinstance(json_array, list):
                    raise ValueError(f"The array '{list(data.keys())[0]}' is not in the correct format. It should be an array.")
            else:
                raise ValueError("The dictionary should have exactly one key pointing to the array data.")
        else:
            raise ValueError("Invalid JSON format. The root element must be an array or an object containing an array.")

        # Continue with flattening and uploading logic...

        # Flatten nested JSON data
        df = flatten_json_data(json_array)

        # Sanitize column names
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Sanitize primary key column
        primary_key_column = sanitize_column_name(primary_key_column)

        # Check if primary key column exists
        if primary_key_column not in df.columns:
            df['id'] = range(1, len(df) + 1)  # Create a new 'id' column if primary key is missing
            primary_key_column = 'id'  # Set the new column as primary key

        # Sanitize table name
        table_name = os.path.splitext(os.path.basename(json_file_path))[0]
        table_name = sanitize_column_name(table_name)

        # Validate or update the table structure
        table_exists = validate_table_structure(cur, table_name)
        if not table_exists:
            print(f"Creating table '{table_name}'.")
            column_types = [
                (col, determine_sql_data_type(df[col])) for col in df.columns
            ]
            columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
            create_table_query = sql.SQL('CREATE TABLE {} ({})').format(
                sql.Identifier(table_name),
                sql.SQL(columns)
            )
            cur.execute(create_table_query)

            if primary_key_column in df.columns:
                alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                    sql.Identifier(table_name), sql.Identifier(primary_key_column)
                )
                cur.execute(alter_table_query)
        else:
            print(f"Table '{table_name}' exists. Updating the table structure if necessary.")
            update_table_structure(cur, table_name, df)

        # Insert or update data into the table
        for _, row in df.iterrows():
            if primary_key_column not in row:
                print(f"Row is missing the primary key column '{primary_key_column}'. Skipping.")
                continue
            
            # Prepare the row data, converting it to the correct types
            converted_row = []
            for col in df.columns:
                expected_type = determine_sql_data_type(df[col])
                converted_row.append(convert_to_correct_type(row[col], expected_type))
            
            # Check if the row already exists based on the primary key
            cur.execute(
                sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(
                    sql.Identifier(table_name),
                    sql.Identifier(primary_key_column)
                ),
                (row[primary_key_column],)
            )
            exists = cur.fetchone()[0]
            if exists:
                print(f"Row with primary key {row[primary_key_column]} already exists. Updating.")
                update_query = sql.SQL('UPDATE {} SET ({}) = ({}) WHERE {} = %s').format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                    sql.SQL(', ').join(sql.Placeholder() for _ in df.columns),
                    sql.Identifier(primary_key_column)
                )
                cur.execute(update_query, tuple(converted_row) + (row[primary_key_column],))
            else:
                print(f"Inserting new row with primary key {row[primary_key_column]}.")
                insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                    sql.SQL(', ').join(sql.Placeholder() for _ in df.columns)
                )
                cur.execute(insert_query, tuple(converted_row))

        # Save details in the datasource table
        csv_save_path = os.path.join('uploads', f"{table_name}.json")
        os.makedirs(os.path.dirname(csv_save_path), exist_ok=True)
        with open(csv_save_path, 'w') as f:
            df.to_json(f, orient='records')

        # Create a datasource table if it doesn't exist
        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS datasource (
                id SERIAL PRIMARY KEY,
                data_source_name VARCHAR(255),
                data_source_path VARCHAR(255)
            );
        """)
        cur.execute(""" 
            INSERT INTO datasource (data_source_name, data_source_path)
            VALUES (%s, %s)
        """, (table_name, csv_save_path))

        conn.commit()
        cur.close()
        conn.close()

        return "Upload successful"
    except Exception as e:
        traceback.print_exc()
        return f"Error: {str(e)}"
