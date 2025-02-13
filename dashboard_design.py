import psycopg2
from flask import jsonify
import psycopg2
import pandas as pd
import re

def get_database_table_names(db_name, username, password, host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name != 'datasource'")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            AND table_name NOT IN ('employee_list', 'datasource')
        """)
                           

        table_names = cursor.fetchall()
        print("table names",table_names)
        for table in table_names:
            print(table[0])
        cursor.close()
        conn.close()
        table_names = [table[0] for table in table_names]
        return jsonify(table_names)
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return jsonify([])
    
def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def remove_symbols(value):
    if isinstance(value, str):
        return ''.join(e for e in value if e.isalnum())
    return value

def get_column_names(db_name, username, password, table_name, host='localhost', port='5432'):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
        column_names = [desc[0] for desc in cursor.description]

        cursor.execute(f"SELECT * FROM {table_name}")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=column_names)

        # Print all column names
        print("All column names in the dataframe:")
        print(df.columns.tolist())

        # Try to convert columns to numeric where possible
        for column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='ignore')

        numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
        text_columns = df.select_dtypes(include=[object]).columns.tolist()

        print("Numeric columns are:", numeric_columns)
        print("=====================================")
        print("Text columns are:", text_columns)

        numeric_columns_cleaned = {}
        text_columns_cleaned = {}

        for column_name in numeric_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            print("numeric column=============", list(numeric_columns_cleaned.keys()))
            num_columns=list(numeric_columns_cleaned.keys())

        for column_name in text_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            print("text column>>>>>>>>>>>>>>>>", list(text_columns_cleaned.keys()))
            txt_columns=list(text_columns_cleaned.keys())

        cursor.close()
        conn.close()

        return {
            'numeric_columns': num_columns,
            'text_columns':txt_columns
        }
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return {'numeric_columns': [], 'text_columns': []}
