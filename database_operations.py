import psycopg2

def connect_to_db(db_name, username, password, host='localhost', port='5432'):
    return psycopg2.connect(dbname=db_name, user=username, password=password, host=host, port=port)


