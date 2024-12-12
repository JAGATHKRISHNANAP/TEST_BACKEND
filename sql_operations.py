from psycopg2 import sql

def insert_into_datasource(cur, directory_name, directory_path):
    insert_query = sql.SQL("""
                INSERT INTO datasource (data_source_name, data_source_path)
                VALUES (%s, %s)
            """)
    cur.execute(insert_query, (directory_name, directory_path))
