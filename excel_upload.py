# import os
# import pandas as pd
# import psycopg2
# import re
# from psycopg2 import sql
# import traceback
# from tqdm import tqdm

# # Function to sanitize column names (replace spaces and special characters with underscores)
# def sanitize_column_name(col_name):
#     if isinstance(col_name, str):
#         return re.sub(r'\W+', '_', col_name).lower()
#     else:
#         return col_name

# # Function to validate if the table exists
# def validate_table_structure(cur, table_name,df):
#     cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
#     table_exists = cur.fetchone()[0]
#     return table_exists

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
#     elif pd.api.types.is_numeric_dtype(value):
#         return 'DOUBLE PRECISION'  
#     elif pd.api.types.is_datetime64_any_dtype(value):
#         return 'DATE'
#     else:
#         return 'VARCHAR'

# # Function to check for repeating columns
# def check_repeating_columns(df):
#     duplicate_columns = df.columns[df.columns.duplicated()].tolist()
#     return duplicate_columns


# UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')



# def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host, port='5432', selected_sheets=None):
#     try:
#         current_dir = os.getcwd()
#         excel_file_path = os.path.join(current_dir, excel_file_name)

#         conn = psycopg2.connect(dbname=database_name, user=username, password=password, host=host, port=port)
#         if not excel_file_path.lower().endswith('.xlsx'):
#             return "Error: Only Excel files with .xlsx extension are supported."

#         xls = pd.ExcelFile(excel_file_path)

#         directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
#         directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
#         os.makedirs(directory_path, exist_ok=True)

#         cur = conn.cursor()

#         for sheet_name in selected_sheets:  # Loop through user-selected sheets
#             sheet_name_cleaned = sheet_name.strip('"').strip()
#             if sheet_name_cleaned not in xls.sheet_names:
#                 print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
#                 continue
#             df = pd.read_excel(excel_file_name, sheet_name=sheet_name_cleaned)
#             table_name = sanitize_column_name(sheet_name_cleaned)
#             df.columns = [sanitize_column_name(col) for col in df.columns]
#             print(f"Columns in {sheet_name}: {df.columns}")
    
#             table_exists = validate_table_structure(cur, table_name, df)
#             print(f"Table exists for {table_name}: {table_exists}")

#             if table_exists:
#                 print(f"Validating and adding missing columns to table '{table_name}'.")
                
#                 # Detect and add missing columns
#                 cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
#                 existing_columns = [row[0] for row in cur.fetchall()]

#                 missing_columns = [col for col in df.columns if col not in existing_columns]

#                 for col in missing_columns:
#                     # Infer column type based on data in DataFrame
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     else:
#                         col_type = 'VARCHAR'  # Default to VARCHAR for mixed or empty columns

#                     alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
#                         sql.Identifier(table_name),
#                         sql.Identifier(col),
#                         sql.SQL(col_type)
#                     )

#                     try:
#                         cur.execute(alter_query)
#                         print(f"Added column '{col}' with type '{col_type}' to table '{table_name}'.")
#                     except Exception as e:
#                         print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
#                         continue

#             else:
#                 print(f"Creating table '{table_name}'.")
                
#                 # Determine data types based on column values
#                 column_types = []
#                 for col in df.columns:
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     else:
#                         col_type = 'VARCHAR'  # Default to VARCHAR for mixed or empty columns
                    
#                     column_types.append((col, col_type))

#                 columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
#                 create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
#                                                                               sql.SQL(columns))
#                 try:
#                     print(f"Create Table Query: {create_table_query.as_string(cur)}")
#                     cur.execute(create_table_query)
#                 except Exception as e:
#                     print(f"Error creating table {table_name}: {str(e)}")
#                     continue  # Skip to next sheet if table creation fails

#                 if primary_key_column in df.columns:
#                     alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
#                         sql.Identifier(table_name), sql.Identifier(primary_key_column))
#                     try:
#                         cur.execute(alter_table_query)
#                     except Exception as e:
#                         print(f"Error adding primary key to {table_name}: {str(e)}")
#                         continue

#             duplicate_primary_keys = df[df.duplicated(subset=[primary_key_column], keep=False)][primary_key_column].tolist()
#             if duplicate_primary_keys:
#                 return f"Error: Duplicate primary key values found: {', '.join(map(str, duplicate_primary_keys))}"

#             # # Delete rows with matching primary key values in bulk
#             # primary_key_values = df[primary_key_column].tolist()
#             # # delete_query = sql.SQL("DELETE FROM {} WHERE {} IN (%s)").format(
#             # #     sql.Identifier(table_name),
#             # #     sql.Identifier(primary_key_column)
#             # # )
#             # # cur.execute(delete_query, (primary_key_values,))
#             # # Convert the list of primary key values to a tuple
#             # primary_key_values_tuple = tuple(primary_key_values)

#             # # Delete rows with matching primary key values in bulk
#             # delete_query = sql.SQL("DELETE FROM {} WHERE {} IN ({})").format(
#             #     sql.Identifier(table_name),
#             #     sql.Identifier(primary_key_column),
#             #     sql.SQL(', ').join([sql.Placeholder()] * len(primary_key_values_tuple))  # Placeholder for each primary key value
#             # )

#             # # Execute the DELETE query with primary key values as arguments
#             # cur.execute(delete_query, primary_key_values_tuple)

#             # print(f"Deleted {len(primary_key_values)} rows with matching primary key values in table '{table_name}'.")
#             # Convert the list of primary key values to a tuple
#             primary_key_values_tuple = tuple(df[primary_key_column].tolist())

#             # Delete rows with matching primary key values in bulk
#             delete_query = sql.SQL("DELETE FROM {} WHERE {} IN ({})").format(
#                 sql.Identifier(table_name),
#                 sql.Identifier(primary_key_column),
#                 sql.SQL(', ').join([sql.Placeholder()] * len(primary_key_values_tuple))  # Placeholder for each primary key value
#             )

#             # Execute the DELETE query with primary key values as arguments
#             cur.execute(delete_query, primary_key_values_tuple)

#             # Check if rows were deleted
#             if cur.rowcount > 0:
#                 print(f"Deleted {cur.rowcount} rows with matching primary key values in table '{table_name}'.")
#             else:
#                 print("No rows were deleted.")



#             # # Iterate over rows in the DataFrame and insert new or updated rows
#             # for _, row in df.iterrows():
#             #     for col in df.select_dtypes(include=['datetime']).columns:
#             #         if pd.isna(row[col]):
#             #             row[col] = None  # Replace invalid dates with None (NULL)

#             #     insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#             #         sql.Identifier(table_name),
#             #         sql.SQL(', ').join(map(sql.Identifier, df.columns)),
#             #         sql.SQL(', ').join(sql.Placeholder() for _ in df.columns)
#             #     )
#             #     cur.execute(insert_query, tuple(row))
#             #     print(f"Inserted row with {primary_key_column} = {row[primary_key_column]} in table '{table_name}'.")
            
            
            
#             # Prepare rows for batch insertion
#             rows_to_insert = []
#             for _, row in df.iterrows():
#                 for col in df.select_dtypes(include=['datetime']).columns:
#                     if pd.isna(row[col]):
#                         row[col] = None  # Replace invalid dates with None (NULL)
#                 rows_to_insert.append(tuple(row))

#             # Batch insert into the database with progress tracking
#             placeholders = ', '.join(['%s'] * len(df.columns))
#             batch_insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#                 sql.Identifier(table_name),
#                 sql.SQL(', ').join(map(sql.Identifier, df.columns)),
#                 sql.SQL(placeholders)
#             )

#             try:
#                 # Define batch size for inserts
#                 batch_size = 1000
#                 total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size  # Ceiling division

#                 with tqdm(total=len(rows_to_insert), desc="Inserting rows", unit="row") as pbar:
#                     for i in range(0, len(rows_to_insert), batch_size):
#                         batch = rows_to_insert[i:i + batch_size]
#                         cur.executemany(batch_insert_query.as_string(conn), batch)
#                         conn.commit()  # Commit after each batch
#                         pbar.update(len(batch))  # Update the progress bar

#                 print(f"Batch inserted {len(rows_to_insert)} rows into table '{table_name}'.")
#             except Exception as e:
#                 print(f"Error during batch insert: {str(e)}")




#             # rows_to_insert = []
#             # for _, row in df.iterrows():
#             #     for col in df.select_dtypes(include=['datetime']).columns:
#             #         if pd.isna(row[col]):
#             #             row[col] = None  # Replace invalid dates with None (NULL)
#             #     rows_to_insert.append(tuple(row))

#             # # Batch insert into the database
#             # placeholders = ', '.join(['%s'] * len(df.columns))
#             # batch_insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#             #     sql.Identifier(table_name),
#             #     sql.SQL(', ').join(map(sql.Identifier, df.columns)),
#             #     sql.SQL(placeholders)
#             # )

#             # try:
#             #     # Execute the batch insert
#             #     cur.executemany(batch_insert_query.as_string(conn), rows_to_insert)
#             #     print(f"Batch inserted {len(rows_to_insert)} rows into table '{table_name}'.")
#             # except Exception as e:
#             #     print(f"Error during batch insert: {str(e)}")


#             file_name = f"{table_name}.xlsx"
#             file_path = os.path.join(directory_path, file_name)
#             df.to_excel(file_path, index=False)

#         cur.execute("""
#             SELECT EXISTS (
#                 SELECT FROM pg_tables 
#                 WHERE schemaname = 'public' 
#                 AND tablename = 'datasource'
#             );
#         """)
#         table_exists = cur.fetchone()[0]

#         if not table_exists:
#             cur.execute("""
#                 CREATE TABLE datasource (
#                     id SERIAL PRIMARY KEY,
#                     data_source_name VARCHAR(255),
#                     data_source_path VARCHAR(255)
#                 );
#             """)
#             print("Created 'datasource' table.")

#         insert_query = sql.SQL("""
#                     INSERT INTO datasource (data_source_name, data_source_path)
#                     VALUES (%s, %s)
#                 """)
#         cur.execute(insert_query, (directory_name, directory_path))
#         conn.commit()

#         cur.close()
#         conn.close()

#         return "Upload successful"
#     except Exception as e:
#         print("An error occurred:", e)
#         return f"Error: {str(e)}"








# cleaned code below

import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql
import traceback
from tqdm import tqdm
from user_upload import  get_db_connection
from flask import Flask, request, jsonify



def is_table_used_in_charts( table_name):
    conn = get_db_connection(dbname="datasource")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM new_dashboard_details_new WHERE selected_table = %s
        )
        """,
        (table_name,)
    )
    return cur.fetchone()[0]

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
    # elif pd.api.types.is_datetime64_any_dtype(value):
    #     return 'DATE'
    else:
        return 'VARCHAR'

# Function to check for repeating columns
def check_repeating_columns(df):
    duplicate_columns = df.columns[df.columns.duplicated()].tolist()
    return duplicate_columns


UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')



def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host, port='5432', selected_sheets=None):
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

        for sheet_name in selected_sheets:  # Loop through user-selected sheets
            sheet_name_cleaned = sheet_name.strip('"').strip()
            if sheet_name_cleaned not in xls.sheet_names:
                print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
                continue
            df = pd.read_excel(excel_file_name, sheet_name=sheet_name_cleaned)
            table_name = sanitize_column_name(sheet_name_cleaned)
            df.columns = [sanitize_column_name(col) for col in df.columns]
            print(f"Columns in {sheet_name}: {df.columns}")
    
            # table_exists = validate_table_structure(cur, table_name, df)
            # print(f"Table exists for {table_name}: {table_exists}")
            table_exists = validate_table_structure(cur, table_name, df)
            print(f"Table exists for {table_name}: {table_exists}")
            is_table_in_use = check_table_usage(cur, table_name)  # Add your implementation for this function
            print(f"Table '{table_name}' is in use: {is_table_in_use}")

            if table_exists and is_table_in_use == False:

                # If table exists and is NOT in use, handle missing columns
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
                existing_columns = [row[0] for row in cur.fetchall()]
                print(f"Existing columns in table '{table_name}': {existing_columns}")

                # Find columns to delete (existing in table but missing in uploaded file)
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
                        print(f"Error deleting column '{col}' from table '{table_name}': {str(e)}")
                        continue

            if table_exists:
                print(f"Validating and adding missing columns to table '{table_name}'.")
                
                # Detect and add missing columns
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
                existing_columns = [row[0] for row in cur.fetchall()]

                missing_columns = [col for col in df.columns if col not in existing_columns]

                for col in missing_columns:
                    # Infer column type based on data in DataFrame
                    if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
                        col_type = 'VARCHAR'
                    elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
                        col_type = 'INTEGER'
                    elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
                        col_type = 'NUMERIC'
                    # elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
                    #     col_type = 'DATE'
                    elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                        col_type = 'DATE'
                    else:
                        col_type = 'VARCHAR'  # Default to VARCHAR for mixed or empty columns

                    alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
                        sql.Identifier(table_name),
                        sql.Identifier(col),
                        sql.SQL(col_type)
                    )

                    try:
                        cur.execute(alter_query)
                        print(f"Added column '{col}' with type '{col_type}' to table '{table_name}'.")
                    except Exception as e:
                        print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
                        continue

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
                    # elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
                    #     col_type = 'DATE'
                    elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
                        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                        col_type = 'DATE'
                    else:
                        col_type = 'VARCHAR'  # Default to VARCHAR for mixed or empty columns
                    
                    column_types.append((col, col_type))

                columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
                create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
                                                                              sql.SQL(columns))
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
            primary_key_values_tuple = tuple(df[primary_key_column].tolist())

            # Delete rows with matching primary key values in bulk
            delete_query = sql.SQL("DELETE FROM {} WHERE {} IN ({})").format(
                sql.Identifier(table_name),
                sql.Identifier(primary_key_column),
                sql.SQL(', ').join([sql.Placeholder()] * len(primary_key_values_tuple))  # Placeholder for each primary key value
            )

            # Execute the DELETE query with primary key values as arguments
            cur.execute(delete_query, primary_key_values_tuple)

            # Check if rows were deleted
            if cur.rowcount > 0:
                print(f"Deleted {cur.rowcount} rows with matching primary key values in table '{table_name}'.")
            else:
                print("No rows were deleted.")

            # Prepare rows for batch insertion
            # rows_to_insert = []
            # for _, row in df.iterrows():
            #     for col in df.select_dtypes(include=['datetime']).columns:
            #         if pd.isna(row[col]):
            #             row[col] = None  # Replace invalid dates with None (NULL)
            #     rows_to_insert.append(tuple(row))
            
            # Prepare rows with NaN/NaT replaced by None
            rows_to_insert = []
            for _, row in df.iterrows():
                new_row = [None if pd.isna(value) else value for value in row]
                rows_to_insert.append(tuple(new_row))

            # Batch insert into the database with progress tracking
            placeholders = ', '.join(['%s'] * len(df.columns))
            batch_insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                sql.SQL(placeholders)
            )

            try:
                # Define batch size for inserts
                batch_size = 1000
                total_batches = (len(rows_to_insert) + batch_size - 1) // batch_size  # Ceiling division
                # df = df.where(pd.notna(df), None)
                with tqdm(total=len(rows_to_insert), desc="Inserting rows", unit="row") as pbar:
                    for i in range(0, len(rows_to_insert), batch_size):
                        batch = rows_to_insert[i:i + batch_size]
                        cur.executemany(batch_insert_query.as_string(conn), batch)
                        conn.commit()  # Commit after each batch
                        pbar.update(len(batch))  # Update the progress bar

                print(f"Batch inserted {len(rows_to_insert)} rows into table '{table_name}'.")
            except Exception as e:
                print(f"Error during batch insert: {str(e)}")


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



# def upload_excel_to_postgresql(database_name, username, password, excel_file_name, primary_key_column, host, port='5432', selected_sheets=None):
#     try:
#         current_dir = os.getcwd()
#         excel_file_path = os.path.join(current_dir, excel_file_name)

#         conn = psycopg2.connect(dbname=database_name, user=username, password=password, host=host, port=port)
#         if not excel_file_path.lower().endswith('.xlsx'):
#             return "Error: Only Excel files with .xlsx extension are supported."

#         xls = pd.ExcelFile(excel_file_path)

#         directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
#         directory_path = os.path.join(UPLOAD_FOLDER, directory_name)
#         os.makedirs(directory_path, exist_ok=True)

#         cur = conn.cursor()

#         for sheet_name in selected_sheets:  # Loop through user-selected sheets
#             sheet_name_cleaned = sheet_name.strip('"').strip()
#             if sheet_name_cleaned not in xls.sheet_names:
#                 print(f"Sheet '{sheet_name_cleaned}' not found in the Excel file. Skipping...")
#                 continue
#             df = pd.read_excel(excel_file_name, sheet_name=sheet_name_cleaned)
#             table_name = sanitize_column_name(sheet_name_cleaned)
#             df.columns = [sanitize_column_name(col) for col in df.columns]
#             print(f"Columns in {sheet_name}: {df.columns}")
    
#             table_exists = validate_table_structure(cur, table_name, df)
#             print(f"Table exists for {table_name}: {table_exists}")
#             is_table_in_use = check_table_usage(cur, table_name)  # Add your implementation for this function
#             print(f"Table '{table_name}' is in use: {is_table_in_use}")

#             if table_exists and not is_table_in_use:
#                 # Handle missing columns (existing in table but not in DataFrame)
#                 cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
#                 existing_columns = [row[0] for row in cur.fetchall()]
#                 columns_to_delete = [col for col in existing_columns if col not in df.columns]

#                 for col in columns_to_delete:
#                     alter_query = sql.SQL('ALTER TABLE {} DROP COLUMN {}').format(
#                         sql.Identifier(table_name),
#                         sql.Identifier(col)
#                     )
#                     try:
#                         cur.execute(alter_query)
#                         print(f"Deleted column '{col}' from table '{table_name}'.")
#                     except Exception as e:
#                         print(f"Error deleting column '{col}' from table '{table_name}': {str(e)}")
#                         conn.rollback()

#                 # Detect and add missing columns (existing in DataFrame but not in table)
#                 existing_columns = [row[0] for row in cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';").fetchall()]
#                 missing_columns = [col for col in df.columns if col not in existing_columns]

#                 for col in missing_columns:
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
#                         df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
#                         col_type = 'DATE'
#                     else:
#                         col_type = 'VARCHAR'

#                     alter_query = sql.SQL('ALTER TABLE {} ADD COLUMN {} {}').format(
#                         sql.Identifier(table_name),
#                         sql.Identifier(col),
#                         sql.SQL(col_type)
#                     )
#                     try:
#                         cur.execute(alter_query)
#                         print(f"Added column '{col}' with type '{col_type}' to table '{table_name}'.")
#                     except Exception as e:
#                         print(f"Error adding column '{col}' to table '{table_name}': {str(e)}")
#                         conn.rollback()

#             elif not table_exists:
#                 # Create new table with inferred column types
#                 column_types = []
#                 for col in df.columns:
#                     if df[col].dropna().apply(lambda x: isinstance(x, str)).all():
#                         col_type = 'VARCHAR'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, int)).all():
#                         col_type = 'INTEGER'
#                     elif df[col].dropna().apply(lambda x: isinstance(x, float)).all():
#                         col_type = 'NUMERIC'
#                     elif pd.to_datetime(df[col].dropna(), errors='coerce').notna().all():
#                         df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
#                         col_type = 'DATE'
#                     else:
#                         col_type = 'VARCHAR'
#                     column_types.append((col, col_type))

#                 columns = ', '.join(f'"{col}" {col_type}' for col, col_type in column_types)
#                 create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name), sql.SQL(columns))
#                 try:
#                     cur.execute(create_table_query)
#                     if primary_key_column in df.columns:
#                         cur.execute(sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
#                             sql.Identifier(table_name), sql.Identifier(primary_key_column)))
#                 except Exception as e:
#                     print(f"Error creating table {table_name}: {str(e)}")
#                     conn.rollback()
#                     continue

#             # Check for duplicate primary keys
#             if primary_key_column in df.columns:
#                 duplicates = df[df.duplicated(subset=[primary_key_column], keep=False)]
#                 if not duplicates.empty:
#                     return f"Error: Duplicate primary keys found in {table_name}: {duplicates[primary_key_column].tolist()}"

#             # Prepare rows with NaN/NaT replaced by None
#             rows_to_insert = []
#             for _, row in df.iterrows():
#                 new_row = [None if pd.isna(value) else value for value in row]
#                 rows_to_insert.append(tuple(new_row))

#             # Batch insert data
#             placeholders = ', '.join(['%s'] * len(df.columns))
#             batch_insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
#                 sql.Identifier(table_name),
#                 sql.SQL(', ').join(map(sql.Identifier, df.columns)),
#                 sql.SQL(placeholders)
#             )

#             try:
#                 batch_size = 1000
#                 with tqdm(total=len(rows_to_insert), desc="Inserting rows", unit="row") as pbar:
#                     for i in range(0, len(rows_to_insert), batch_size):
#                         batch = rows_to_insert[i:i + batch_size]
#                         cur.executemany(batch_insert_query, batch)
#                         conn.commit()
#                         pbar.update(len(batch))
#                 print(f"Inserted {len(rows_to_insert)} rows into {table_name}")
#             except Exception as e:
#                 print(f"Error inserting into {table_name}: {str(e)}")
#                 conn.rollback()
#                 continue

#             # Save uploaded file
#             file_path = os.path.join(directory_path, f"{table_name}.xlsx")
#             df.to_excel(file_path, index=False)

#         # Update datasource table
#         try:
#             cur.execute("""
#                 INSERT INTO datasource (data_source_name, data_source_path)
#                 VALUES (%s, %s)
#             """, (directory_name, directory_path))
#             conn.commit()
#         except Exception as e:
#             print(f"Error updating datasource: {str(e)}")
#             conn.rollback()

#         cur.close()
#         conn.close()

#         return "Upload successful"
#     except Exception as e:
#         print("An error occurred:", e)
#         if 'conn' in locals():
#             conn.rollback()
#         return f"Error: {str(e)}"






















