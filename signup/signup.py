from flask import Flask, request, jsonify
import psycopg2
import bcrypt
import logging
from config import PASSWORD, USER_NAME, HOST, PORT

def get_db_connection(dbname="datasource"):
    conn = psycopg2.connect(
        dbname=dbname,
        # user="postgres",
        # password="jaTHU@12",
        # host="localhost",
        # port="5432"
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
        
    )
    return conn


def connect_db(company):
    try:
        conn = psycopg2.connect(
            dbname=company,
            # user="postgres",
            # password="jaTHU@12",
            # host="localhost",
            # port="5432" 
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None



def create_database(organizationName):
    try:
        conn = get_db_connection(dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        logging.info(f"Creating database: {organizationName}")
        cursor.execute(f"CREATE DATABASE {organizationName}")
    except Exception as e:
        logging.error(f"Error creating database {organizationName}: {str(e)}")
        raise e
    finally:
        cursor.close()
        conn.close()


def create_table_if_not_exists(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS organizationdatatest (
        id SERIAL PRIMARY KEY,
        organizationName VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        userName VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL
    );
    """)

def insert_user_data(organizationName, email, userName, password):
    try:
        create_database(organizationName)  # Assuming this creates the database if needed
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create table if it does not exist
        create_table_if_not_exists(cursor)

        # Insert data into table
        cursor.execute(
            """
            INSERT INTO organizationdatatest (organizationName, email, userName, password)
            VALUES (%s, %s, %s, %s)
            """,
            (organizationName, email, userName, password)
        )
        conn.commit()
        logging.info(f"User data inserted for {organizationName}")

    except Exception as e:
        logging.error(f"Error inserting user data: {str(e)}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()



def fetch_usersdata():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """

             SELECT * FROM organizationdatatest
            """
        )
                    # SELECT userName FROM organizationdatatest
        data = cursor.fetchall()
        return data
    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()


def fetch_login_data(email, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_if_not_exists(cursor)

    # SQL query to check if email and password match
    cursor.execute("SELECT * FROM organizationdatatest WHERE email = %s AND password = %s", (email, password))
    user = cursor.fetchone()

    cursor.close()
    conn.close()    
    return { "user": user}



import binascii
import bcrypt
# def fetch_company_login_data(email, password, company):
#     conn = connect_db(company)
#     cursor = conn.cursor()

#     # First query to fetch the user details (except password)
#     cursor.execute("SELECT employee_id, employee_name, role_id, email FROM employee_list WHERE email = %s", (email,))
#     user = cursor.fetchone()

#     if user:
#         # Second query to fetch the hashed password separately
#         cursor.execute("SELECT password FROM employee_list WHERE email = %s", (email,))
#         hashed_password_row = cursor.fetchone()

#         if hashed_password_row:
#             stored_hash_with_hex = hashed_password_row[0]  # Get the password from the result
#             stored_hash_bytes = binascii.unhexlify(stored_hash_with_hex.replace('\\x', ''))

#             # Check if the password matches the hashed password
#             if bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes):
#                 print("Password match!")
#                 cursor.close()
#                 conn.close()
#                 return user  # Return the user details (without the password)
#             else:
#                 print("Password does not match!")
#         else:
#             print("Password not found!")

#     else:
#         print("User not found!")

#     cursor.close()
#     conn.close()
#     return None

import binascii
import bcrypt

def fetch_company_login_data(email, password, company):
    conn = connect_db(company)
    cursor = conn.cursor()

    # First query to fetch the user details (except password)
    cursor.execute("SELECT employee_id, employee_name, role_id, email FROM employee_list WHERE email = %s", (email,))
    user = cursor.fetchone()

    if user:
        # Second query to fetch the hashed password separately
        cursor.execute("SELECT password FROM employee_list WHERE email = %s", (email,))
        hashed_password_row = cursor.fetchone()
        print(hashed_password_row)
        if hashed_password_row:
            stored_hash_with_hex = hashed_password_row[0]  # Get the password from the result
            stored_hash_bytes = binascii.unhexlify(stored_hash_with_hex.replace('\\x', ''))
            print("------------------------------------",stored_hash_bytes)    
            # Check if the password matches the hashed password
            if bcrypt.checkpw(password.encode('utf-8'), stored_hash_bytes):
                print("Password match!")

                # New query to fetch all table names in the database
                # cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name NOT IN ('employee_list', 'datasource')
                """)
                tables = cursor.fetchall()

                # Print or log the table names
                print("Tables in the database:")
                for table in tables:
                    print(table[0])

                cursor.close()
                conn.close()
                # return user,tables  # Return the user details (without the password)
                return {
                    "user": user,  # User details
                    "tables": tables  # List of table names
                }
            else:
                print("Password does not match!")
        else:
            print("Password not found!")

    else:
        print("User not found!")

    cursor.close()
    conn.close()
    return None




def fetch_company_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL query to check if email and password match
    cursor.execute("SELECT organizationName from organizationdatatest")
    user = cursor.fetchone()

    cursor.close()
    conn.close()    
    return user

def fetch_role_id_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # SQL query to check if email and password match
    cursor.execute("SELECT role_id from role")
    user = cursor.fetchone()

    cursor.close()
    conn.close()    
    return user




# def create_user_table(conn):
#     try:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS employee_list (
#                     employee_id SERIAL PRIMARY KEY,
#                     employee_name VARCHAR(255),
#                     role_id VARCHAR(255),
#                     e_mail VARCHAR(255),
#                     password VARCHAR(255)
#                 );
#             """)
#         conn.commit()
#     except Exception as e:
#         print(f"Error creating table: {e}")

def create_user_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_list (
                    employee_id SERIAL PRIMARY KEY,
                    employee_name VARCHAR(255),
                    role_id VARCHAR(255),
                    username varchar(255),     
                    email VARCHAR(255),
                    password VARCHAR(255),
                    category varchar(255),
                    action_type varchar(255), 
                    action_by varchar(255)
                    
                );
            """)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")





def encrypt_password(plain_password):
    """
    Encrypts a plain text password using bcrypt and returns the hashed password.
    """
    hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
    return hashed_password
def create_category_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL,
            company_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id) ON DELETE CASCADE
        );
    """)
def create_user_table(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_list (
                    employee_id SERIAL PRIMARY KEY,
                    employee_name VARCHAR(255),
                    role_id VARCHAR(255),
                    username VARCHAR(255),
                    email VARCHAR(255),
                    password VARCHAR(255),
                    category VARCHAR(255),
                    action_type VARCHAR(255),
                    action_by VARCHAR(255),
                    reporting_id INTEGER
                );
            """)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
def create_user_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS "user" (
            id SERIAL PRIMARY KEY, 
            company_id INT NOT NULL,  -- Changed the column order to include company_id first
            user_id INT NOT NULL, 
            role_id INT NOT NULL, 
            category_id INT NOT NULL, 
            FOREIGN KEY (role_id) REFERENCES role(role_id),
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id),
            FOREIGN KEY (category_id) REFERENCES category(category_id)
        );
    """)

