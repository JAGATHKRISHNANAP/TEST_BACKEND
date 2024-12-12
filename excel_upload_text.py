import os
import pandas as pd
import psycopg2
import re
from psycopg2 import sql

def sanitize_column_name(col_name):
    if isinstance(col_name, str):
        return re.sub(r'\W+', '_', col_name).lower()
    else:
        return col_name

def validate_table_structure(cur, table_name, df):
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)", (table_name,))
    table_exists = cur.fetchone()[0]
    return table_exists
def upload_excel_to_postgresql(db_name, username, password, excel_file_name, primary_key_column, host='localhost', port='5432'):
    try:
        current_dir = os.getcwd()
        excel_file_path = os.path.join(current_dir, excel_file_name)
        conn = psycopg2.connect(dbname=db_name, user=username, password=password, host=host, port=port)
        if not excel_file_path.lower().endswith('.xlsx'):
            return "Error: Only Excel files with .xlsx extension are supported."

        xls = pd.ExcelFile(excel_file_path)
        directory_name = os.path.splitext(os.path.basename(excel_file_name))[0]
        dir_path = "C:/Users/hp/Desktop/COMIENZO/DATA ANALYTICS/sample/datas1"
        directory_path = os.path.join(dir_path, directory_name)
        os.makedirs(directory_path, exist_ok=True)

        cur = conn.cursor()

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
            table_name = sanitize_column_name(sheet_name)
            df.columns = [sanitize_column_name(col) for col in df.columns]

            # Check if table exists
            if validate_table_structure(cur, table_name, df):
                print(f"Table '{table_name}' already exists.")
            else:
                # Create table if it doesn't exist
                print("Creating table '{}'.".format(table_name))
                columns = ', '.join('"{}" VARCHAR'.format(col) for col in df.columns)
                create_table_query = sql.SQL('CREATE TABLE {} ({})').format(sql.Identifier(table_name),
                                                                             sql.SQL(columns))
                cur.execute(create_table_query)

                # Add primary key constraint if specified
                if primary_key_column in df.columns:
                    alter_table_query = sql.SQL('ALTER TABLE {} ADD PRIMARY KEY ({})').format(
                        sql.Identifier(table_name), sql.Identifier(primary_key_column))
                    cur.execute(alter_table_query)

                print("Table '{}' created successfully.".format(table_name))

            # Check for duplicate primary keys
            duplicate_primary_keys = df[df.duplicated(subset=[primary_key_column], keep=False)][primary_key_column].tolist()
            if duplicate_primary_keys:
                return f"Error: Duplicate primary key values found: {', '.join(map(str, duplicate_primary_keys))}"

            # Validate each row data
            for _, row in df.iterrows():
                # Check if primary key column exists
                if primary_key_column not in row:
                    return f"Error: Primary key column '{primary_key_column}' not found in the Excel sheet."

                # Check if the primary key exists in the table
                cur.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} = %s)").format(sql.Identifier(table_name),
                                                                                   sql.Identifier(primary_key_column)),
                    (str(row[primary_key_column]),))
                exists = cur.fetchone()[0]

                if exists:
                    # Update existing row
                    update_values = [(sql.Identifier(col), row[col]) for col in df.columns if
                                     col != primary_key_column]
                    update_values = [(col, None if value == 'NaN' else value) for col, value in
                                     update_values]  # Convert 'NaN' to None
                    update_query = sql.SQL('UPDATE {} SET {} WHERE {} = %s').format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(col + sql.SQL(' = %s') for col, _ in update_values),
                        sql.Identifier(primary_key_column)
                    )
                    cur.execute(update_query, [value for _, value in update_values] + [str(row[primary_key_column])])
                    print("Updated row with {} = {} in table '{}'".format(primary_key_column, row[primary_key_column],
                                                                          table_name))
                else:
                    # Insert new row
                    insert_query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(map(sql.Identifier, df.columns)),
                        sql.SQL(', ').join(sql.Placeholder() * len(df.columns))
                    )
                    cur.execute(insert_query, tuple(row))

            # Save DataFrame to Excel
            file_name = "{}.xlsx".format(table_name)
            file_path = os.path.join(directory_path, file_name)
            df.to_excel(file_path, index=False)

        # Update datasource table
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
        print("An error occurred:", e)  # Print the error for debugging
        return "Error: {}".format(str(e))