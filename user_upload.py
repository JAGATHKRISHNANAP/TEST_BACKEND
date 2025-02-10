# # from flask import jsonify
# # import psycopg2
# # import hashlib
# # import bcrypt
# # from signup.signup import create_user_table,encrypt_password,create_user_table_if_not_exists,create_category_table_if_not_exists

# # from config import PASSWORD, USER_NAME, HOST, PORT
# # def get_db_connection(dbname="datasource"):
# #     return psycopg2.connect(
# #         dbname=dbname,
# #         user="postgres",
# #         password="Gayu@123",
# #         host="localhost",
# #         port="5432"
# #     )

# # def handle_manual_registration(user_details):
# #     conn_datasource = get_db_connection()
# #     if not conn_datasource:
# #         return jsonify({'message': 'Failed to connect to datasource database'}), 500

# #     try:
# #         # Extracting details
# #         employee_name = user_details.get("employeeName")
# #         role_name = user_details.get("roleId")
# #         organization_name = user_details.get("company")
# #         username = user_details.get("userName")
# #         email = user_details.get("email")
# #         password = user_details.get("password")
# #         retype_password = user_details.get("retypePassword")
# #         categories = user_details.get("categories", [])
# #         reporting_id = user_details.get("reportingId")

# #         if password != retype_password:
# #             return jsonify({'message': 'Passwords do not match'}), 400

# #         role_id = fetch_role_id(conn_datasource, role_name)

# #         conn = get_company_db_connection(organization_name)
# #         if not conn:
# #             return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500
        
# #         with conn_datasource.cursor() as cursor:
# #             # Start a transaction block in datasource database
# #             cursor.execute("BEGIN;")
# #             create_category_table_if_not_exists(cursor)
# #             create_user_table_if_not_exists(cursor)
# #             cursor.execute("COMMIT;")
# #         with conn.cursor() as cursor:
# #             if check_username_exists(cursor, username):
# #                 return jsonify({'message': 'Username already exists in employee_list'}), 400

# #             hashed_password = encrypt_password(password)
# #             action_type, action_by = "add", "admin"
# #             employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

# #             handle_categories(conn_datasource, employee_id, role_id, organization_name, categories)

# #         conn.commit()
# #         conn_datasource.commit()
# #         return jsonify({'message': 'User and categories created successfully'}), 200

# #     except Exception as e:
# #         print(f"Error during manual registration: {e}")
# #         return jsonify({'message': 'Error creating user'}), 500

# #     finally:
# #         # conn.close()
# #         conn_datasource.close()

# # def handle_file_upload_registration(user_details):
# #     conn_datasource = get_db_connection()
# #     if not conn_datasource:
# #         return jsonify({'message': 'Failed to connect to datasource database'}), 500

# #     action_type, action_by = "add", "admin"

# #     try:
# #         with conn_datasource.cursor() as cursor:
# #             # Start a transaction block in datasource database
# #             cursor.execute("BEGIN;")

# #             for user in user_details:
# #                 employee_name = user.get("Employee Name")
# #                 role_name = user.get("Role Name")
# #                 organization_name = user.get("Organization Name")
# #                 username = user.get("Username")
# #                 password = user.get("Password")
# #                 email = user.get("Email")
# #                 categories = user.get("Categories")
# #                 reporting_id = user.get("Reporting ID")

# #                 category_list = [category.strip() for category in (categories.split(",") if categories else [])]

# #                 if not all([employee_name, role_name, username, categories]):
# #                     return jsonify({'message': f'Missing required details for user: {username}'}), 400

# #                 role_id = fetch_role_id(conn_datasource, role_name)
# #                 conn = get_company_db_connection(organization_name)
# #                 print("conn",conn)
# #                 if not conn:
# #                     return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500

# #                 # Correct usage of cursor here
# #                 create_user_table(conn)
# #                 create_category_table_if_not_exists(cursor)
# #                 create_user_table_if_not_exists(cursor)

# #                 # Begin company transaction
# #                 with conn.cursor() as company_cursor:
# #                     if check_username_exists(company_cursor, username):
# #                         return jsonify({'message': f'Username already exists for user: {username}'}), 400

# #                     hashed_password = encrypt_password(password)
# #                     employee_id = insert_user(company_cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

# #                     # Handle categories and user permissions
# #                     handle_categories(conn_datasource, employee_id, role_id, organization_name, category_list)

# #                 conn.commit()  # Commit the company database transaction

# #             cursor.execute("COMMIT;")  # Commit the datasource transaction
# #         return jsonify({'message': 'File upload processed successfully'}), 200

# #     except Exception as e:
# #         print(f"Error during file upload registration: {e}")
# #         conn_datasource.rollback()  # Rollback the transaction in case of error
# #         return jsonify({'message': 'Error during file upload registration'}), 500

# #     finally:
# #         conn_datasource.close()


# # def fetch_role_id(conn, role_name):
# #     with conn.cursor() as cursor:
# #         cursor.execute("SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)", (role_name,))
# #         role_data = cursor.fetchone()
# #         if not role_data:
# #             raise ValueError(f"Role not found for role name: {role_name}")
# #         return role_data[0]

# # def check_username_exists(cursor, username):
# #     cursor.execute("SELECT COUNT(*) FROM employee_list WHERE username = %s", (username,))
# #     result = cursor.fetchone()
# #     return result and result[0] > 0

# # def insert_user(cursor, employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id=None):
# #     cursor.execute("""
# #         INSERT INTO employee_list (employee_name, role_id, username, email, password, category, action_type, action_by, reporting_id)
# #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
# #     """, (employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id))
# #     cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
# #     return cursor.fetchone()[0]

# # def handle_categories(conn_datasource, employee_id, role_id, organization_name, categories):
# #     for category in categories:
# #         with conn_datasource.cursor() as cursor:
# #             cursor.execute("""
# #                 SELECT category_id FROM category 
# #                 WHERE LOWER(category_name) = LOWER(%s) 
# #                 AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
# #             """, (category, organization_name))
# #             result = cursor.fetchone()
# #             if result:
# #                 raise ValueError(f"Category '{category}' already exists for company '{organization_name}'")
# #             cursor.execute("""
# #                 INSERT INTO category (category_name, company_id)
# #                 VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
# #                 RETURNING category_id;
# #             """, (category, organization_name))
# #             category_id = cursor.fetchone()[0]
# #             cursor.execute("""
# #                 INSERT INTO "user" (user_id, role_id, company_id, category_id)
# #                 VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
# #             """, (employee_id, role_id, organization_name, category_id))


# # def encrypt_password(plain_password):
# #     """
# #     Encrypts a plain text password using bcrypt and returns the hashed password.
# #     """
# #     hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
# #     return hashed_password

# # def get_company_db_connection(company_name):
# #     # Define connection string to connect to a company's database
# #     conn = psycopg2.connect(
# #         dbname=company_name,  # Ensure this is the correct company database
# #          user=USER_NAME,
# #         password=PASSWORD,
# #         host=HOST,
# #         port=PORT
# #     )
# #     return conn
# # from flask import jsonify
# # import psycopg2
# # import hashlib
# # import bcrypt
# # from signup.signup import create_user_table, encrypt_password, create_user_table_if_not_exists, create_category_table_if_not_exists

# # from config import PASSWORD, USER_NAME, HOST, PORT

# # # Database connection to the datasource (common database)
# # def get_db_connection(dbname="datasource"):
# #     return psycopg2.connect(
# #         dbname=dbname,
# #         user="postgres",
# #         password="Gayu@123",
# #         host="localhost",
# #         port="5432"
# #     )

# # # Handle manual user registration
# # def handle_manual_registration(user_details):
# #     conn_datasource = get_db_connection()
# #     if not conn_datasource:
# #         return jsonify({'message': 'Failed to connect to datasource database'}), 500

# #     try:
# #         # Extract user details
# #         employee_name = user_details.get("employeeName")
# #         role_name = user_details.get("roleId")
# #         organization_name = user_details.get("company")
# #         username = user_details.get("userName")
# #         email = user_details.get("email")
# #         password = user_details.get("password")
# #         retype_password = user_details.get("retypePassword")
# #         categories = user_details.get("categories", [])
# #         reporting_id = user_details.get("reportingId")

# #         # Password validation
# #         if password != retype_password:
# #             return jsonify({'message': 'Passwords do not match'}), 400

# #         # Fetch role_id
# #         role_id = fetch_role_id(conn_datasource, role_name)

# #         # Get company DB connection
# #         conn = get_company_db_connection(organization_name)
# #         if not conn:
# #             return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500
        
# #         # Create necessary tables in datasource database
# #         with conn_datasource.cursor() as cursor:
# #             cursor.execute("BEGIN;")
# #             create_category_table_if_not_exists(cursor)
# #             create_user_table_if_not_exists(cursor)
# #             cursor.execute("COMMIT;")
        
# #         with conn.cursor() as cursor:
# #             # Check if username already exists
# #             if check_username_exists(cursor, username):
# #                 return jsonify({'message': 'Username already exists in employee_list'}), 400

# #             # Encrypt password
# #             hashed_password = encrypt_password(password)
# #             action_type, action_by = "add", "admin"
# #             employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

# #             # Handle categories and user permissions
# #             handle_categories(conn_datasource, employee_id, role_id, organization_name, categories)

# #         # Commit transactions
# #         conn.commit()
# #         conn_datasource.commit()
# #         return jsonify({'message': 'User and categories created successfully'}), 200

# #     except Exception as e:
# #         print(f"Error during manual registration: {e}")
# #         return jsonify({'message': 'Error creating user'}), 500

# #     finally:
# #         conn_datasource.close()

# # # Handle file upload for user registration
# # def handle_file_upload_registration(user_details):
# #     conn_datasource = get_db_connection()
# #     if not conn_datasource:
# #         return jsonify({'message': 'Failed to connect to datasource database'}), 500

# #     action_type, action_by = "add", "admin"

# #     try:
# #         with conn_datasource.cursor() as cursor:
# #             cursor.execute("BEGIN;")

# #             for user in user_details:
# #                 employee_name = user.get("Employee Name")
# #                 role_name = user.get("Role Name")
# #                 organization_name = user.get("Organization Name")
# #                 username = user.get("Username")
# #                 password = user.get("Password")
# #                 email = user.get("Email")
# #                 categories = user.get("Categories")
# #                 reporting_id = user.get("Reporting ID")

# #                 category_list = [category.strip() for category in (categories.split(",") if categories else [])]

# #                 if not all([employee_name, role_name, username, categories]):
# #                     return jsonify({'message': f'Missing required details for user: {username}'}), 400

# #                 role_id = fetch_role_id(conn_datasource, role_name)
# #                 conn = get_company_db_connection(organization_name)
# #                 if not conn:
# #                     return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500

# #                 # Create necessary tables in company database
# #                 create_user_table(conn)
# #                 create_category_table_if_not_exists(cursor)
# #                 create_user_table_if_not_exists(cursor)

# #                 # Begin company transaction
# #                 with conn.cursor() as company_cursor:
# #                     if check_username_exists(company_cursor, username):
# #                         return jsonify({'message': f'Username already exists for user: {username}'}), 400

# #                     hashed_password = encrypt_password(password)
# #                     employee_id = insert_user(company_cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

# #                     # Handle categories and user permissions
# #                     handle_categories(conn_datasource, employee_id, role_id, organization_name, category_list)

# #                 conn.commit()  # Commit company transaction

# #             cursor.execute("COMMIT;")  # Commit datasource transaction
# #         return jsonify({'message': 'File upload processed successfully'}), 200

# #     except Exception as e:
# #         print(f"Error during file upload registration: {e}")
# #         conn_datasource.rollback()  # Rollback the transaction in case of error
# #         return jsonify({'message': 'Error during file upload registration'}), 500

# #     finally:
# #         conn_datasource.close()

# # # Fetch role_id for given role name
# # def fetch_role_id(conn, role_name):
# #     with conn.cursor() as cursor:
# #         cursor.execute("SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)", (role_name,))
# #         role_data = cursor.fetchone()
# #         if not role_data:
# #             raise ValueError(f"Role not found for role name: {role_name}")
# #         return role_data[0]

# # # Check if username exists in the employee_list table
# # def check_username_exists(cursor, username):
# #     cursor.execute("SELECT COUNT(*) FROM employee_list WHERE username = %s", (username,))
# #     result = cursor.fetchone()
# #     return result and result[0] > 0

# # # Insert a new user into employee_list table
# # def insert_user(cursor, employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id=None):
# #     cursor.execute("""
# #         INSERT INTO employee_list (employee_name, role_id, username, email, password, category, action_type, action_by, reporting_id)
# #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
# #     """, (employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id))
# #     cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
# #     return cursor.fetchone()[0]

# # # Handle categories and user permissions
# # def handle_categories(conn_datasource, employee_id, role_id, organization_name, categories):
# #     for category in categories:
# #         with conn_datasource.cursor() as cursor:
# #             cursor.execute("""
# #                 SELECT category_id FROM category 
# #                 WHERE LOWER(category_name) = LOWER(%s) 
# #                 AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
# #             """, (category, organization_name))
# #             result = cursor.fetchone()
# #             if result:
# #                 raise ValueError(f"Category '{category}' already exists for company '{organization_name}'")
# #             cursor.execute("""
# #                 INSERT INTO category (category_name, company_id)
# #                 VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
# #                 RETURNING category_id;
# #             """, (category, organization_name))
# #             category_id = cursor.fetchone()[0]
# #             cursor.execute("""
# #                 INSERT INTO "user" (user_id, role_id, company_id, category_id)
# #                 VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
# #             """, (employee_id, role_id, organization_name, category_id))

# # # Encrypt password using bcrypt
# # def encrypt_password(plain_password):
# #     hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
# #     return hashed_password

# # # Get a connection to the company's database
# # def get_company_db_connection(company_name):
# #     conn = psycopg2.connect(
# #         dbname=company_name,
# #         user=USER_NAME,
# #         password=PASSWORD,
# #         host=HOST,
# #         port=PORT
# #     )
# #     return conn
# from flask import Flask, jsonify, request
# import psycopg2
# import bcrypt
# from config import PASSWORD, USER_NAME, HOST, PORT
# from signup.signup import  encrypt_password

# app = Flask(__name__)

# def get_db_connection(dbname="datasource"):
#     return psycopg2.connect(
#         dbname=dbname,
#         user="postgres",
#         password="Gayu@123",
#         host="localhost",
#         port="5432"
#     )

# def handle_manual_registration(user_details):
#     conn_datasource = get_db_connection()
#     if not conn_datasource:
#         return jsonify({'message': 'Failed to connect to datasource database'}), 500

#     try:
#         # Extracting details
#         employee_name = user_details.get("employeeName")
#         role_name = user_details.get("roleId")
#         organization_name = user_details.get("company")
#         username = user_details.get("userName")
#         email = user_details.get("email")
#         password = user_details.get("password")
#         retype_password = user_details.get("retypePassword")
#         categories = user_details.get("categories", [])
#         reporting_id = user_details.get("reportingId")

#         # Check if passwords match
#         if password != retype_password:
#             return jsonify({'message': 'Passwords do not match'}), 400

#         # Fetch role_id
#         role_id = fetch_role_id(conn_datasource, role_name)

#         # Connect to company database
#         conn = get_company_db_connection(organization_name)
#         if not conn:
#             return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500
        
#         with conn_datasource.cursor() as cursor:
#             # Start transaction block in datasource database
#             cursor.execute("BEGIN;")
            
#             create_category_table_if_not_exists(cursor)
            
#             create_user_table_if_not_exists(cursor)
#             cursor.execute("COMMIT;")

#         with conn.cursor() as cursor:
#             if check_username_exists(cursor, username):
#                 return jsonify({'message': 'Username already exists in employee_list'}), 400

#             # Encrypt password
#             hashed_password = encrypt_password(password)
#             action_type, action_by = "add", "admin"
#             employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

#             # Handle categories
#             handle_categories(conn_datasource, employee_id, role_id, organization_name, categories)

#         conn.commit()
#         conn_datasource.commit()
#         return jsonify({'message': 'User and categories created successfully'}), 200

#     except Exception as e:
#         print(f"Error during manual registration: {e}")
#         return jsonify({'message': 'Error creating user'}), 500

#     finally:
#         conn_datasource.close()


# def handle_file_upload_registration(user_details):
#     conn_datasource = get_db_connection()
#     if not conn_datasource:
#         return jsonify({'message': 'Failed to connect to datasource database'}), 500

#     action_type, action_by = "add", "admin"

#     try:
#         with conn_datasource.cursor() as cursor:
#             # Start transaction block in datasource database
#             cursor.execute("BEGIN;")

#             for user in user_details:
                
#                 employee_name = user.get("Employee Name")
#                 role_name = user.get("Role Name")
#                 organization_name = user.get("Organization Name")
#                 username = user.get("Username")
#                 password = user.get("Password")
#                 email = user.get("Email")
#                 categories = user.get("Categories")
#                 reporting_id = user.get("Reporting ID")

#                 category_list = [category.strip() for category in (categories.split(",") if categories else [])]

#                 if not all([employee_name, role_name, username, categories]):
#                     return jsonify({'message': f'Missing required details for user: {username}'}), 400

#                 # Fetch role_id
#                 role_id = fetch_role_id(conn_datasource, role_name)

#                 # Connect to company database
#                 conn = get_company_db_connection(organization_name)
#                 if not conn:
#                     return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500

#                 # Create necessary tables in company database
#                 create_user_table(conn)
#                 create_category_table_if_not_exists(cursor)
#                 create_user_table_if_not_exists(cursor)

#                 with conn.cursor() as company_cursor:
#                     if check_username_exists(company_cursor, username):
#                         return jsonify({'message': f'Username already exists for user: {username}'}), 400

#                     # Encrypt password
#                     hashed_password = encrypt_password(password)
#                     employee_id = insert_user(company_cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

#                     # Handle categories and user permissions
#                     handle_categories(conn_datasource, employee_id, role_id, organization_name, category_list)

#                 conn.commit()

#             cursor.execute("COMMIT;")
#         return jsonify({'message': 'File upload processed successfully'}), 200

#     except Exception as e:
#         print(f"Error during file upload registration: {e}")
#         conn_datasource.rollback()
#         return jsonify({'message': 'Error during file upload registration'}), 500

#     finally:
#         conn_datasource.close()


# def fetch_role_id(conn, role_name):
#     with conn.cursor() as cursor:
#         cursor.execute("SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)", (role_name,))
#         role_data = cursor.fetchone()
#         if not role_data:
#             raise ValueError(f"Role not found for role name: {role_name}")
#         return role_data[0]


# def check_username_exists(cursor, username):
#     cursor.execute("SELECT COUNT(*) FROM employee_list WHERE username = %s", (username,))
#     result = cursor.fetchone()
#     return result and result[0] > 0


# def insert_user(cursor, employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id=None):
#     cursor.execute("""
#         INSERT INTO employee_list (employee_name, role_id, username, email, password, category, action_type, action_by, reporting_id)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """, (employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id))
#     cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
#     return cursor.fetchone()[0]


# def handle_categories(conn_datasource, employee_id, role_id, organization_name, categories):
#     for category in categories:
#         with conn_datasource.cursor() as cursor:
#             cursor.execute("""
#                 SELECT category_id FROM category 
#                 WHERE LOWER(category_name) = LOWER(%s) 
#                 AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
#             """, (category, organization_name))
#             result = cursor.fetchone()
#             if result:
#                 raise ValueError(f"Category '{category}' already exists for company '{organization_name}'")
#             cursor.execute("""
#                 INSERT INTO category (category_name, company_id)
#                 VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
#                 RETURNING category_id;
#             """, (category, organization_name))
#             category_id = cursor.fetchone()[0]
#             cursor.execute("""
#                 INSERT INTO "user" (user_id, role_id, company_id, category_id)
#                 VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
#             """, (employee_id, role_id, organization_name, category_id))


# def encrypt_password(plain_password):
#     """
#     Encrypts a plain text password using bcrypt and returns the hashed password.
#     """
#     hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
#     return hashed_password


# def get_company_db_connection(company_name):
#     conn = psycopg2.connect(
#         dbname=company_name,
#         user=USER_NAME,
#         password=PASSWORD,
#         host=HOST,
#         port=PORT
#     )
#     return conn


# # def create_user_table_if_not_exists(cursor):
# #     cursor.execute("""
# #         CREATE TABLE IF NOT EXISTS "user" (
# #             user_id SERIAL PRIMARY KEY,
# #             role_id INT NOT NULL,
# #             company_id INT NOT NULL,
# #             category_id INT NOT NULL,
# #             FOREIGN KEY (role_id) REFERENCES role(role_id),
# #             FOREIGN KEY (company_id) REFERENCES organizationdatatest(id),
# #             FOREIGN KEY (category_id) REFERENCES category(category_id)
# #         );
# #     """)


# # Create user table in company database
# def create_user_table(conn):
#     try:
#         with conn.cursor() as cursor:
#             cursor.execute("""
#                 CREATE TABLE IF NOT EXISTS employee_list (
#                     employee_id SERIAL PRIMARY KEY,
#                     employee_name VARCHAR(255),
#                     role_id VARCHAR(255),
#                     username VARCHAR(255),
#                     email VARCHAR(255),
#                     password VARCHAR(255),
#                     category VARCHAR(255),
#                     action_type VARCHAR(255),
#                     action_by VARCHAR(255),
#                     reporting_id INTEGER
#                 );
#             """)
#         conn.commit()
#     except Exception as e:
#         print(f"Error creating table: {e}")
# def create_user_table_if_not_exists(cursor):
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS "user" (
#             id SERIAL PRIMARY KEY, 
#             company_id INT NOT NULL,  -- Changed the column order to include company_id first
#             user_id INT NOT NULL, 
#             role_id INT NOT NULL, 
#             category_id INT NOT NULL, 
#             FOREIGN KEY (role_id) REFERENCES role(role_id),
#             FOREIGN KEY (company_id) REFERENCES organizationdatatest(id),
#             FOREIGN KEY (category_id) REFERENCES category(category_id)
#         );
#     """)
# def create_category_table_if_not_exists(cursor):
#     cursor.execute("""
#         CREATE TABLE IF NOT EXISTS category (
#             category_id SERIAL PRIMARY KEY,
#             category_name VARCHAR(255) NOT NULL,
#             company_id INT NOT NULL,
#             FOREIGN KEY (company_id) REFERENCES organizationdatatest(id)
#         );
#     """)

from flask import jsonify
import psycopg2
import hashlib
import bcrypt
from signup.signup import  encrypt_password

from config import PASSWORD, USER_NAME, HOST, PORT

# Database connection to the datasource (common database)
def get_db_connection(dbname="datasource"):
    return psycopg2.connect(
        dbname=dbname,
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

# Handle manual user registration
def handle_manual_registration(user_details):
    conn_datasource = get_db_connection()
    if not conn_datasource:
        return jsonify({'message': 'Failed to connect to datasource database'}), 500

    try:
        # Extract user details
        employee_name = user_details.get("employeeName")
        role_name = user_details.get("roleId")
        organization_name = user_details.get("company")
        username = user_details.get("userName")
        email = user_details.get("email")
        password = user_details.get("password")
        retype_password = user_details.get("retypePassword")
        categories = user_details.get("categories", [])
        reporting_id = user_details.get("reportingId")
        if reporting_id == "":
            reporting_id = None
        # Password validation
        if password != retype_password:
            return jsonify({'message': 'Passwords do not match'}), 400

        # Fetch role_id
        role_id = fetch_role_id(conn_datasource, role_name)

        # Get company DB connection
        conn = get_company_db_connection(organization_name)
        if not conn:
            return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500
        
        # Create necessary tables in datasource database
        with conn_datasource.cursor() as cursor:
            cursor.execute("BEGIN;")
            # Create necessary tables in company database
            create_user_table(conn)
            create_category_table_if_not_exists(cursor)
            create_user_table_if_not_exists(cursor)
            cursor.execute("COMMIT;")
        
        with conn.cursor() as cursor:
            # Check if username already exists
            if check_username_exists(cursor, username):
                return jsonify({'message': 'Username already exists in employee_list'}), 400

            # Encrypt password
            hashed_password = encrypt_password(password)
            action_type, action_by = "add", "admin"
            employee_id = insert_user(cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

            # Handle categories and user permissions
            handle_categories(conn_datasource, employee_id, role_id, organization_name, categories)

        # Commit transactions
        conn.commit()
        conn_datasource.commit()
        return jsonify({'message': 'User and categories created successfully'}), 200

    except Exception as e:
        print(f"Error during manual registration: {e}")
        return jsonify({'message': 'Error creating user'}), 500

    finally:
        conn_datasource.close()

# Handle file upload for user registration
def handle_file_upload_registration(user_details,company):
    conn_datasource = get_db_connection()
    if not conn_datasource:
        return jsonify({'message': 'Failed to connect to datasource database'}), 500

    action_type, action_by = "add", "admin"

    try:
        with conn_datasource.cursor() as cursor:
            cursor.execute("BEGIN;")
            
            for user in user_details:
                employee_name = user.get("Employee Name")
                role_name = user.get("Role Name")
                organization_name = company
                username = user.get("Username")
                password = user.get("Password")
                email = user.get("Email")
                categories = user.get("Categories")
                reporting_id = user.get("Reporting ID")
                if reporting_id == "":
                    reporting_id = None
                category_list = [category.strip() for category in (categories.split(",") if categories else [])]

                if not all([employee_name, role_name, username, categories]):
                    return jsonify({'message': f'Missing required details for user: {username}'}), 400

                role_id = fetch_role_id(conn_datasource, role_name)
                conn = get_company_db_connection(organization_name)
                if not conn:
                    return jsonify({'message': f'Failed to connect to company database for {organization_name}'}), 500

                # Create necessary tables in company database
                create_user_table(conn)
                create_category_table_if_not_exists(cursor)
                create_user_table_if_not_exists(cursor)

                # Begin company transaction
                with conn.cursor() as company_cursor:
                    if check_username_exists(company_cursor, username):
                        return jsonify({'message': f'Username already exists for user: {username}'}), 400

                    hashed_password = encrypt_password(password)
                    employee_id = insert_user(company_cursor, employee_name, role_id, username, email, hashed_password, categories, action_type, action_by, reporting_id)

                    # Handle categories and user permissions
                    handle_categories(conn_datasource, employee_id, role_id, organization_name, category_list)

                conn.commit()  # Commit company transaction

            cursor.execute("COMMIT;")  # Commit datasource transaction
        return jsonify({'message': 'File upload processed successfully'}), 200

    except Exception as e:
        print(f"Error during file upload registration: {e}")
        conn_datasource.rollback()  # Rollback the transaction in case of error
        return jsonify({'message': 'Error during file upload registration'}), 500

    finally:
        conn_datasource.close()

# Fetch role_id for given role name
def fetch_role_id(conn, role_name):
    with conn.cursor() as cursor:
        cursor.execute("SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)", (role_name,))
        role_data = cursor.fetchone()
        if not role_data:
            raise ValueError(f"Role not found for role name: {role_name}")
        return role_data[0]

# Check if username exists in the employee_list table
def check_username_exists(cursor, username):
    cursor.execute("SELECT COUNT(*) FROM employee_list WHERE username = %s", (username,))
    result = cursor.fetchone()
    return result and result[0] > 0

# Insert a new user into employee_list table
def insert_user(cursor, employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id=None):
    cursor.execute("""
        INSERT INTO employee_list (employee_name, role_id, username, email, password, category, action_type, action_by, reporting_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (employee_name, role_id, username, email, password, categories, action_type, action_by, reporting_id))
    cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
    return cursor.fetchone()[0]

# Handle categories and user ssions
def handle_categories(conn_datasource, employee_id, role_id, organization_name, categories):
    for category in categories:
        with conn_datasource.cursor() as cursor:
            cursor.execute("""
                SELECT category_id FROM category 
                WHERE LOWER(category_name) = LOWER(%s) 
                AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
            """, (category, organization_name))
            result = cursor.fetchone()
            if result:
                raise ValueError(f"Category '{category}' already exists for company '{organization_name}'")
            cursor.execute("""
                INSERT INTO category (category_name, company_id)
                VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
                RETURNING category_id;
            """, (category, organization_name))
            category_id = cursor.fetchone()[0]
            cursor.execute("""
                INSERT INTO "user" (user_id, role_id, company_id, category_id)
                VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
            """, (employee_id, role_id, organization_name, category_id))

# Encrypt password using bcrypt
def encrypt_password(plain_password):
    hashed_password = bcrypt.hashpw(plain_password.encode('utf-8'), bcrypt.gensalt())
    return hashed_password

# Get a connection to the company's database
def get_company_db_connection(company_name):
    conn = psycopg2.connect(
        dbname=company_name,
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return conn

# Create user table in company database
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

# Create user table if not exists in the datasource database
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
def create_category_table_if_not_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category (
            category_id SERIAL PRIMARY KEY,
            category_name VARCHAR(255) NOT NULL,
            company_id INT NOT NULL,
            FOREIGN KEY (company_id) REFERENCES organizationdatatest(id) ON DELETE CASCADE
        );
    """)