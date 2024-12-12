import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
import traceback

# Function to sanitize column names (replace spaces and special characters with underscores)
def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name

# Function to validate if the table exists
def validate_table_structure(cur, table_name,df):
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    table_exists = cur.fetchone()[0]
    return table_exists

# Function to map pandas dtypes to PostgreSQL types
def determine_sql_data_type(value):
    if pd.api.types.is_string_dtype(value):
        return 'VARCHAR'
    elif pd.api.types.is_integer_dtype(value):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(value):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(value):
        return 'BOOLEAN'
    elif pd.api.types.is_numeric_dtype(value):
        return 'DOUBLE PRECISION'  
    elif pd.api.types.is_datetime64_any_dtype(value):
        return 'DATE'
    else:
        return 'VARCHAR'

# Function to check for repeating columns
def check_repeating_columns(df):
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    return duplicate_columns


UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')


def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host='localhost', port='5432',selected_sheets=None):
    try:
        current_dir = os.getcwd()
        excel_file_path = os.path.join(current_dir, excel_file_name)

        conn = psycopg2.connect(dbname=database_name, user=username, password=password, host=host, port=port)
        if not excel_file_path.lower().endswith('.xlsx'):
            return "Error: Only Excel files with .xlsx extension are supported."

        xls = pd.ExcelFile(excel_file_path)

        directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
        directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
        os.makedirs(directory_path, exist_ok=True)

        cur = conn.cursor()

        # for sheet_name in xls.sheet_names:
        #     df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        #     table_name = sanitize_column_name(sheet_name)
        #     df.columns = [sanitize_column_name(col) for col in df.columns]
        for sheet_name in selected_sheets:  # Loop through user-selected sheets
            sheet_name_cleaned = sheet_name.strip('"').strip()
            if sheet_name_cleaned not in xls.sheet_names:
                
                print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
                continue
            df = pd.read_excel(excel_file_name, sheet_name=sheet_name_cleaned)
            table_name = sanitize_column_name(sheet_name_cleaned)
            df.columns = [sanitize_column_name(col) for col in df.columns]
            print(f"Columns in {sheet_name}: {df.columns}")
    
            table_exists = validate_table_structure(cur, table_name, df)
            print(f"Table exists for {table_name}: {table_exists}")

            if table_exists:
                print(f"Table '{table_name}' already exists.")
            else:
                print(f"Creating table '{table_name}'.")
                
                # Determine data types based on column values
                column_types = []
                for col in df.columns:
                    if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
                        col_type = 'VARCHAR'
                    elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
                        col_type = 'INTEGER'
                    elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
                        col_type = 'NUMERIC'
                    else:
                        col_type = 'VARCHAR'  # Default to VARCHAR for mixed or empty columns
                    
                    column_types.append((col, col_type))

                columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
                create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
                                                                              sql.SQL(columns))
                # cur.execute(create_table_query)
                try:
                    print(f"Create Table Query: {create_table_query.as_string(cur)}")
                    cur.execute(create_table_query)
                except Exception as e:
                    print(f"Error creating table {table_name}: {str(e)}")
                    continue  # Skip to next sheet if table creation fails



                if primary_key_column in df.columns:
                    alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                        sql.Identifier(table_name), sql.Identifier(primary_key_column))
                    try:
                        cur.execute(alter_table_query)
                    except Exception as e:
                        print(f"Error adding primary key to {table_name}: {str(e)}")
                        continue
            duplicate_primary_keys = df[df.duplicated(subset=[primary_key_column], keep=False)][primary_key_column].tolist()
            if duplicate_primary_keys:
                return f"Error: Duplicate primary key values found: {', '.join(map(str, duplicate_primary_keys))}"

            for _, row in df.iterrows():
                if primary_key_column not in row:
                    return f"Error: Primary key column '{primary_key_column}' not found in the Excel sheet."

                cur.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(sql.Identifier(table_name),
                                                                                sql.Identifier(primary_key_column)),
                    (str(row[primary_key_column]),))
                exists = cur.fetchone()[0]

                if exists:
                    cur.execute(sql.SQL("SELECT * FROM {} WHERE {} = %s").format(sql.Identifier(table_name),
                                                                                sql.Identifier(primary_key_column)),
                                (str(row[primary_key_column]),))
                    db_row = cur.fetchone()
                    db_row_values = db_row[1:]  # Exclude the primary key column
                    df_row_values = [row[col] for col in df.columns if col != primary_key_column]
                    
                    if db_row_values == df_row_values:
                        print(f"Row with {primary_key_column} = {row[primary_key_column]} in table '{table_name}' is not updated as the values are the same.")
                        continue  
                    
                    update_values = [(sql.Identifier(col), row[col]) for col in df.columns if col != primary_key_column]
                    update_values = [(col, None if value == 'NaN' else value) for col, value in update_values]  # Convert 'NaN' to None
                    update_query = sql.SQL('UPDATE {} SET {} WHERE {} = %s').format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(col + sql.SQL(' = %s') for col, _ in update_values),
                        sql.Identifier(primary_key_column)
                    )
                    cur.execute(update_query, [value for _, value in update_values] + [str(row[primary_key_column])])
                    print(f"Updated row with {primary_key_column} = {row[primary_key_column]} in table '{table_name}'")

                else:
                    insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                        sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                    )
                    cur.execute(insert_query, tuple(row))

            file_name = f"{table_name}.xlsx"
            file_path = os.path.join(directory_path, file_name)
            df.to_excel(file_path, index=False)

        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename = 'datasource'
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

        insert_query = sql.SQL("""
                    INSERT INTO datasource (data_source_name, data_source_path)
                    VALUES (%s, %s)
                """)
        cur.execute(insert_query, (directory_name, directory_path))
        conn.commit()

        cur.close()
        conn.close()

        return "Upload successful"
    except Exception as e:
        print("An error occurred:", e)
        return f"Error: {str(e)}"