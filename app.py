import os
from flask_cors import CORS
import json
from flask import Flask, request, jsonify,session
from werkzeug.utils import secure_filename
from excel_upload import upload_excel_to_postgresql
from csv_upload import upload_csv_to_postgresql
from dashboard_design import get_database_table_names
from bar_chart import fetch_data ,drill_down,fetch_column_name ,calculationFetch,fetch_data_for_duel ,perform_calculation,get_column_names,fetchText_data,edit_fetch_data,fetch_hierarchical_data,Hierarchial_drill_down
import traceback
import bar_chart as bc
from dashboard_save.dashboard_save import insert_combined_chart_details, create_dashboard_table, create_connection,get_dashboard_names,get_dashboard_view_chart_data
from signup.signup import insert_user_data,fetch_usersdata,fetch_login_data,connect_db,create_user_table,encrypt_password,fetch_company_login_data
import psycopg2
from audio import allowed_file,transcribe_audio_with_timestamps,save_file_to_db
from histogram_utils import generate_histogram_details,handle_column_data_types
from json_upload import upload_json_to_postgresql
from config import  ALLOWED_EXTENSIONS,DB_NAME,USER_NAME,PASSWORD,HOST,PORT
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import logging
from flask_session import Session  # Flask-Session for server-side session handling
import uuid
from viewChart.viewChart import get_db_connection_view, fetch_chart_data,filter_chart_data
from user_upload import handle_manual_registration, handle_file_upload_registration, get_db_connection



company_name_global = None

logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)



app = Flask(__name__)
secret_key = os.urandom(24)
app.secret_key = secret_key
# CORS(app,resources={r"/*": {"origins": "http://localhost:3000"}}) 
CORS(app, resources={r"/*": {"origins": "*"}})
# CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

db_name = DB_NAME
username = USER_NAME
password = PASSWORD
host = HOST
port = PORT
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'audio')
company_name=None

app.config['SECRET_KEY'] =b'y\xd8\x9e\xa6a\xe0\x8eK\x02L\x14@\x0f\x03\xab\x8e\xae\x1d\tB\xbc\xfbL\xcc'
app.config['SESSION_TYPE'] = 'filesystem'  # Store sessions on the server

Session(app)



@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "Hello, World! tessstingsssss"})

@app.route('/test_db')
def test_db():
    try:
        # Establish connection
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to fetch all table names
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        
        # Fetch all table names
        tables = cursor.fetchall()
        table_list = [table[0] for table in tables]

        # Close the connection
        cursor.close()
        conn.close()

        # Check if table list is empty
        if not table_list:
            return {"message": "No tables found in the database."}, 200

        # Return the table names
        return {"tables": table_list}, 200

    except Exception as e:
        return {"error": f"Database connection failed: {e}"}, 500



@app.route('/uploadexcel', methods=['POST'])
def upload_file_excel():
    # database_name='excel_database'
    database_name = request.form.get('company_database') 
    # database_name = request.form.get('databaseName')
    excel_file = request.files['file']
    primary_key_column = request.form.get('primaryKeyColumnName')
    selected_sheets = request.form.getlist('selectedSheets')  # New addition

    print("database_name=============111111111111111111111111", database_name)
    print("method=============", request.method)
    print("files==============", request.files)
    print("primary_key_column====================", primary_key_column) 
    
    print("selected_sheets ", selected_sheets)
    excel_file_name = secure_filename(excel_file.filename)
    os.makedirs('tmp', exist_ok=True)
    temp_file_path = f'tmp/{excel_file_name}'
    excel_file.save(temp_file_path)
    
    result=upload_excel_to_postgresql(database_name, username, password, temp_file_path, primary_key_column, host, port,selected_sheets)
    if result == "Upload successful":
        return jsonify({'message': 'File uploaded successfully'}), 200
    else:
        return jsonify({'message': result}), 500


@app.route('/uploadcsv', methods=['POST'])
def upload_file_csv():
    excel_file = request.files['file']
    database_name = request.form.get('company_database')
    print("company_database====================",database_name)  
    excel_file_name = secure_filename(excel_file.filename)
    os.makedirs('tmp', exist_ok=True)
    temp_file_path = f'tmp/{excel_file_name}'
    excel_file.save(temp_file_path)
    # database_name='csv_database'
    upload_csv_to_postgresql(database_name, username, password, temp_file_path, host, port)
    return jsonify({'message': 'File uploaded successfully'})

@app.route('/load-data', methods=['POST'])
def load_data():
    database_name = request.json['databaseName']
    checked_paths = request.json['checkedPaths']
    print("Database name:", database_name)
    print("Checked paths:", checked_paths)
    return jsonify({'message': 'Data loaded successfully'})

@app.route('/table_names')
def get_table_names():
    database_name=request.args.get('databaseName')
    table_names_response = get_database_table_names(database_name, username, password, host, port)
    if table_names_response is None:
        return jsonify({'message': 'Error fetching table names'}), 500
    table_names = table_names_response.get_json()  
    return jsonify(table_names)

@app.route('/column_names/<table_name>',methods=['GET'] )
def get_columns(table_name):
    db_nameeee= request.args.get('databaseName')
    column_names = get_column_names(db_nameeee, username, password, table_name, host, port)
    print("column_names====================",column_names)
    return jsonify(column_names)

@app.route('/join-tables', methods=['POST'])
def join_tables():
    data = request.json
    print("data__________",data)
    tables = data.get('tables')  
    selected_columns = data.get('selectedColumns') 
    join_columns = data.get('joinColumns')  
    join_type = data.get('joinType', 'INNER JOIN') 
    database_name = data.get('databaseName')
    view_name = data.get('joinedTableName')  

    query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT {', '.join(selected_columns)} FROM {tables[0]}"

    for table in tables[1:]:
        join_column = join_columns.get(table)
        query += f" {join_type} {table} ON {tables[0]}.{join_column} = {table}.{join_column}"

    print("Executing query:", query)

    # Connect to the database
    connection = None
    try:
        # Use your database connection method
        connection = connect_db(database_name)
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()  # Commit to save the view in the database
        print("View created successfully")
    except Exception as e:
        print("Error executing query:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        if connection:
            cursor.close()
            connection.close()

    return jsonify({"message": f"View '{view_name}' created successfully"})


@app.route('/plot_chart', methods=['POST', 'GET'])
def get_bar_chart_route():
    df = bc.global_df
    df_json = df.to_json(orient='split')  # Convert the DataFrame to JSON
    data = request.json

    table_name = data['selectedTable']
    x_axis_columns = data['xAxis'].split(', ')  # Split multiple columns into a list
    y_axis_columns = data['yAxis']  # Assuming yAxis can be multiple columns as well
    aggregation = data['aggregate']
    checked_option = data['filterOptions']
    db_nameeee = data['databaseName']
    print("y_axis_columns====================", y_axis_columns)
    print("db_nameeee====================", db_nameeee)
    print("data====================", data)
    try:
        df[y_axis_columns[0]] = pd.to_datetime(df[y_axis_columns[0]], format='%H:%M:%S', errors='raise')

        # If successful, convert time format to minutes
        df[y_axis_columns[0]] = df[y_axis_columns[0]].apply(lambda x: x.hour * 60 + x.minute)
        print(f"{y_axis_columns[0]} converted to minutes.")
    except ValueError:
        # If conversion fails, it is not in time format
        print(f"{y_axis_columns[0]} is not in time format. No conversion applied.")

    if len(y_axis_columns) == 1:
        data = fetch_data(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee)
        
        if aggregation == "count":
            print("Data for count aggregation:", data)
            array1 = [item[0] for item in data]
            array2 = [item[1] for item in data]
            print("Array1:", array1)
            print("Array2:", array2)
            
            # Return the JSON response for count aggregation
            return jsonify({"categories": array1, "values": array2, "aggregation": aggregation, "dataframe": df_json})
        
        # For other aggregation types
        categories = {}
        for row in data:
            category = tuple(row[:-1])
            y_axis_value = row[-1]
            if category not in categories:
                categories[category] = initial_value(aggregation)
            update_category(categories, category, y_axis_value, aggregation)

        labels = [', '.join(category) for category in categories.keys()]
        values = list(categories.values())
        print("labels====================", labels)
        print("values====================", values)
        
        # Return the JSON response for other aggregations
        return jsonify({"categories": labels, "values": values, "aggregation": aggregation, "dataframe": df_json})


    elif len(y_axis_columns) == 2:
        datass = fetch_data_for_duel(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee)
        
        data = {
            "categories": [row[0] for row in datass],
            "series1": [row[1] for row in datass],
            "series2": [row[2] for row in datass],
            "dataframe": df_json
        }
        
        return jsonify(data)

def initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        if isinstance(categories[category_key], list):
            categories[category_key][0] += float(y_axis_value)
            categories[category_key][1] += 1
        else:
            categories[category_key] = [float(y_axis_value), 1]
    elif aggregation == 'count':
        categories[category_key] += 1

@app.route('/edit_plot_chart', methods=['POST', 'GET'])
def get_edit_chart_route():
    data = request.json
    table_name = data['selectedTable']
    x_axis_columns = data['xAxis'].split(', ')  # Split multiple columns into a list
    y_axis_columns = data['yAxis'] # Assuming yAxis can be multiple columns as well
    aggregation = data['aggregate']
    checked_option = data['filterOptions'] 
    db_nameeee = data['databaseName']
    print(".......................................",data)
    if len(y_axis_columns) == 1:
        data = edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee)
        print("data====================", data)     
        categories = {}  
        for row in data:
            category = tuple(row[:-1])
            y_axis_value = row[-1]
            if category not in categories:
                categories[category] = initial_value(aggregation)
            update_category(categories, category, y_axis_value, aggregation)      
        labels = [', '.join(category) for category in categories.keys()]  
        values = list(categories.values())
        print("labels====================", labels)
        print("values====================", values)
        return jsonify({"categories": labels, "values": values, "aggregation": aggregation})
    elif len(y_axis_columns) == 2:
        datass = fetch_data_for_duel(table_name, x_axis_columns, checked_option, y_axis_columns, aggregation, db_nameeee)
        data = {
            "categories": [row[0] for row in datass],
            "series1": [row[1] for row in datass],
            "series2": [row[2] for row in datass]
        }
        print("data====================", data)
        
        return jsonify(data)

def edit_initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def edit_update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        if isinstance(categories[category_key], list):
            categories[category_key][0] += float(y_axis_value)
            categories[category_key][1] += 1
        else:
            categories[category_key] = [float(y_axis_value), 1]
    elif aggregation == 'count':
        categories[category_key] += 1

@app.route('/your-backend-endpoint', methods=['POST','GET'])
def handle_bar_click():

    conn = connect_to_db()
    cur = conn.cursor()
    data = request.json
    clicked_category = data.get('category')
    x_axis_columns = data.get('xAxis')
    y_axis_column = data.get('yAxis')
    table_name = data.get('tableName')
    aggregation = data.get('aggregation')
    print("x_axis_columns====================",x_axis_columns)
    print("aggregate====================",aggregation)
    print("clicked_category====================",clicked_category)
    if aggregation == "sum":
        aggregation = "SUM"
    elif aggregation == "average":
        aggregation = "AVG"
    elif aggregation == "count":
        aggregation = "COUNT"
    elif aggregation == "maximum":
        aggregation = "MAX"
    elif aggregation == "minimum":
        aggregation = "MIN"

    data=drill_down(clicked_category, x_axis_columns, y_axis_column, aggregation)
    # data=drill_down(clicked_category, x_axis_columns, y_axis_column, table_name, aggregation)
    categories={}
    y_axis_values = []
    for row in data:
        category = tuple(row[:-1])
        y_axis_value = int(row[-1])
        y_axis_values.append(y_axis_value)

        if category not in categories:
            categories[category] = initial_value(aggregation)
       
    labels = [', '.join(category) for category in categories.keys()]
    values = list(categories.values())

    return jsonify({"categories": labels, "values": y_axis_values, "aggregation": aggregation})

@app.route('/plot_chart/<selectedTable>/<columnName>', methods=['POST','GET'])
def get_filter_options(selectedTable, columnName):
    table_name=selectedTable
    column_name=columnName
    db_nameeee= request.args.get('databaseName')
    print("db_nameeee====================",db_nameeee)
    column_data=fetch_column_name(table_name, column_name, db_nameeee)
    print("column_data====================",column_data)
    return jsonify(column_data)

def create_table():
    try:

        conn=connect_to_db()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS new_dashboard_details_new (
                id SERIAL PRIMARY KEY,
                User_id INTEGER, 
                company_name VARCHAR,
                chart_name VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                database_name VARCHAR,
                selected_table VARCHAR,
                x_axis VARCHAR[],
                y_axis VARCHAR[],
                aggregate VARCHAR,
                chart_type VARCHAR,
                chart_color VARCHAR,
                chart_heading VARCHAR,
                drilldown_chart_color VARCHAR,
                filter_options VARCHAR
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("Table created successfully")
    except Exception as e:
        print("Error in create-------------:", e)

@app.route('/save_data', methods=['POST'])
def save_data():
    data = request.json
    user_id=data['user_id']
    save_name= data['saveName']
    company_name=data['company_name']   
    print("company_name====================",company_name)  
    print("user_id====================",user_id)
    print("save_name====================", save_name)
    create_table()
    
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        chart_heading_json = json.dumps(data.get('chart_heading'))
        cur.execute("""
            INSERT INTO new_dashboard_details_new (
                User_id,
                company_name,                
                chart_name,
                database_name,
                selected_table,
                x_axis,
                y_axis,
                aggregate,
                chart_type,
                chart_color,
                chart_heading,
                drilldown_chart_color,
                filter_options
            ) VALUES (
                %s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """, (
            data.get('user_id'),
            data.get('company_name'),       
            data.get('saveName'),  
            data.get('databaseName'),
            data.get('selectedTable'),
            data.get('xAxis'),
            data.get('yAxis'),
            data.get('aggregate'),
            data.get('chartType'),
            data.get('chartColor'),
            chart_heading_json,
            data.get('drillDownChartColor'),
            data.get('filterOptions')
        ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'message': 'Data inserted successfully'})
    except Exception as e:
        print("Error:iiiiiiiiiiiii", e)
        return jsonify({'error': str(e)})
    
@app.route('/update_data', methods=['POST'])
def update_data():
    data = request.json
    print("Received data for update:", data)
    
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        chart_heading_json = json.dumps(data.get('chart_heading'))
        
        # Update data in the table
        cur.execute("""
            UPDATE new_dashboard_details_new
            SET

                selected_table = %s,
                x_axis = %s,
                y_axis = %s,
                aggregate = %s,
                chart_type = %s,
                chart_color = %s,
                chart_heading = %s,
                drilldown_chart_color = %s,
                filter_options = %s
            WHERE
                id = %s
        """, (

            data.get('selectedTable'),
            data.get('xAxis'),
            data.get('yAxis'),
            data.get('aggregate'),
            data.get('chartType'),
            data.get('chartColor'),
            chart_heading_json,
            data.get('drillDownChartColor'),
            data.get('filterOptions'),
            data.get('chartId')  # Assuming there's an 'id' field in the data
        ))
        

        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({'message': 'Data updated successfully'})
    except Exception as e:
        print("Error:", e)
        return jsonify({'error': str(e)})

def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=USER_NAME, password=PASSWORD, host=HOST
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to the database:", e)
        return None

# def get_chart_names(company_name_global):
#     conn = connect_to_db()
#     if conn:
#         try:
#             cursor = conn.cursor()
#             query = "SELECT chart_name FROM new_dashboard_details_new WHERE company_name = %s"
#             cursor.execute(query, (company_name_global,))
#             chart_names = [row[0] for row in cursor.fetchall()]
#             cursor.close()
#             conn.close()
#             print("chart_names", chart_names)
#             return chart_names
#         except psycopg2.Error as e:
#             print("Error fetching chart names:", e)
#             conn.close()
#             return None
#     else:
#         return None


# def get_chart_names(user_id, company_name_global):
#     # Step 1: Get employees reporting to the given user_id from the company database.
#     conn_company = get_company_db_connection(company_name_global)
#     # print("company_name_global====================",conn_company)
#     reporting_employees = []

#     if conn_company:
#         try:
#             with conn_company.cursor() as cursor:
#                 # Check if reporting_id column exists dynamically (skip errors if missing).
#                 cursor.execute("""
#                     SELECT column_name FROM information_schema.columns 
#                     WHERE table_name='employee_list' AND column_name='reporting_id'
#                 """)
#                 column_exists = cursor.fetchone()

#                 if column_exists:
#                     # Fetch employees who report to the given user_id (including NULL reporting_id if not assigned).
#                     cursor.execute("""
#                         SELECT employee_id FROM employee_list WHERE reporting_id = %s OR reporting_id IS NULL
#                     """, (user_id,))
#                     reporting_employees = [row[0] for row in cursor.fetchall()]
#                     print(f"Reporting employees: {reporting_employees}")

#         except psycopg2.Error as e:
#             print(f"Error fetching reporting employees: {e}")
#         finally:
#             conn_company.close()

#     # Include the user's own employee_id for fetching their charts.
#     # Convert all IDs to integers for consistent data type handling.
#     all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

#     # Step 2: Fetch dashboard names for these employees from the datasource database.
#     conn_datasource = get_db_connection("datasource")
#     dashboard_structure = {}

#     if conn_datasource:
#         try:
#             with conn_datasource.cursor() as cursor:
#                 # Create placeholders for the IN clause
#                 placeholders = ', '.join(['%s'] * len(all_employee_ids))
                
#                 # Updated query to use placeholders for company_name
#                 query = f"""
#                     SELECT user_id, chart_name FROM new_dashboard_details_new
#                     WHERE user_id IN ({placeholders}) AND company_name = %s
#                 """
#                 cursor.execute(query, tuple(all_employee_ids) + (company_name_global,))
#                 charts = cursor.fetchall()

#                 # Organize charts by user_id
#                 for uid, chart_name in charts:
#                     if uid not in dashboard_structure:
#                         dashboard_structure[uid] = []
#                     dashboard_structure[uid].append(chart_name)
#         except psycopg2.Error as e:
#             print(f"Error fetching dashboard details: {e}")
#         finally:
#             conn_datasource.close()

#     return dashboard_structure






def get_chart_names(user_id, company_name_global):
    # Step 1: Get employees reporting to the given user_id from the company database.
    conn_company = get_company_db_connection(company_name_global)
    # print("company_name_global====================",conn_company)
    reporting_employees = []

    if conn_company:
        try:
            with conn_company.cursor() as cursor:
                # Fetch all details from the employee_list table
                cursor.execute("SELECT employee_id, reporting_id FROM employee_list")
                table_data = cursor.fetchall()  # Fetch all rows
                print("Table data:", table_data)
                
                # Filter and print employee_id where reporting_id matches user_id
                for row in table_data:
                    employee_id, reporting_id = row
                    if reporting_id == user_id:
                        reporting_employees.append(employee_id)
                        print(f"Employee ID reporting to User ID {user_id}: {employee_id}")
                    else:
                        print(f"user id not match to reporting id")
                
                print(f"Reporting employees: {reporting_employees}")

        except psycopg2.Error as e:
            print(f"Error fetching table details: {e}")
        finally:
            conn_company.close()

    # Include the user's own employee_id for fetching their charts.
    # Convert all IDs to integers for consistent data type handling.
    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection("datasource")
    dashboard_structure = {}

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Create placeholders for the IN clause
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                
                # Updated query to use placeholders for company_name
                query = f"""
                    SELECT user_id, chart_name FROM new_dashboard_details_new
                    WHERE user_id IN ({placeholders}) AND company_name = %s
                """
                cursor.execute(query, tuple(all_employee_ids) + (company_name_global,))
                charts = cursor.fetchall()

                # Organize charts by user_id
                for uid, chart_name in charts:
                    if uid not in dashboard_structure:
                        dashboard_structure[uid] = []
                    dashboard_structure[uid].append(chart_name)
        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_structure








# @app.route('/total_rows', methods=['GET'])
# def chart_names():
#     global company_name_global
#     print("company_name====================",company_name_global)  
    
#     names = get_chart_names(company_name_global)
#     print("names====================", names)   
#     if names is not None:
#         return jsonify({'chart_names': names})
#     else:
#         return jsonify({'error': 'Failed to fetch chart names'})


@app.route('/total_rows', methods=['GET'])
def chart_names():
    global company_name_global
    user_id = request.args.get('user_id')
    print("company_name_global====================", company_name_global)
    print("user_id====================", user_id)  

    try:
        user_id = int(user_id)  # Convert to integer
    except ValueError:
        return jsonify({'error': 'Invalid user_id. Must be an integer.'})

    names = get_chart_names(user_id, company_name_global)
    print("names====================", names)   
    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})


def get_chart_data(chart_name):
    print("chart_id====================......................................................",chart_name)
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id, selected_table, x_axis, y_axis, aggregate, chart_type, chart_color, chart_heading, drilldown_chart_color, filter_options, database_name FROM new_dashboard_details_new WHERE chart_name = %s", (chart_name,))
            data = cursor.fetchone()
            
            if data is None:
                print(f"No data found for Chart ID: {chart_name}")
                return None
            
            filterdata = list(data[10])
            cursor.close()
            conn.close()
            return data
        except psycopg2.Error as e:
            print("Error fetching data for Chart", chart_name, ":", e)
            conn.close()
            return None
    else:
        return None

@app.route('/chart_data/<chart_name>', methods=['GET'])
def chart_data(chart_name):
    data = get_chart_data(chart_name)
    print("chart datas------------------------------------------------------------------------------------------------------------------",data)
    if data is not None:
        return jsonify(data)
    else:
        return jsonify({'error': 'Failed to fetch data for Chart {}'.format(chart_name)})

BASE_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'excel')
@app.route('/list-excel-files', methods=['GET'])
def list_files():
    directory_structure = {}

    for root, dirs, files in os.walk(BASE_DIR):
        dir_path = os.path.relpath(root, BASE_DIR)
        if dir_path == '.':
            dir_path = ''
        directory_structure[dir_path] = {'dirs': dirs, 'files': files}
    return jsonify(directory_structure)

BASE_CSV_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'uploads', 'csv')
@app.route('/list-csv-files', methods=['GET'])
def list_csv_files():
    directory_structure = {}

    for root, dirs, files in os.walk(BASE_CSV_DIR):
        dir_path = os.path.relpath(root, BASE_CSV_DIR)
        if dir_path == '.':
            dir_path = ''
        directory_structure[dir_path] = {'dirs': dirs, 'files': files}
    
    return jsonify(directory_structure)

@app.route('/save_all_chart_details', methods=['POST'])
def save_all_chart_details():
    data = request.get_json()
    user_id=data['user_id']
    charts = data['charts']
    print("user_id====================", user_id)
    dashboardfilterXaxis = data['dashboardfilterXaxis']
    dashboardClickedCategory = data['selectedCategory']
    file_name = data['fileName']
    company_name=data['company_name']
    print("company_name====================", company_name)
    print("file_name====================", file_name)
    conn = create_connection()
    if conn is None:
        return jsonify({'message': 'Database connection failed'}), 500
    create_dashboard_table(conn)

    # Initialize lists to combine chart details
    combined_chart_details = {
        'user_id': user_id, 
        'file_name': file_name,
        'company_name': company_name,
        'chart_ids': [],
        'positions': [],
        'chart_types': [],
        'chart_Xaxes': [],
        'chart_Yaxes': [],
        'chart_aggregates': [],
        'filterdata': dashboardfilterXaxis,  # Assuming this remains the same for all charts
        'clicked_category': dashboardClickedCategory  # Assuming this remains the same for all charts
    }
    
    for chart in charts:
        chart_id_key = list(chart.keys())[0]
        chart_type = list(chart.keys())[5]
        chart_Xaxis = list(chart.keys())[2]
        chart_Yaxis = list(chart.keys())[3]
        chart_aggregate = list(chart.keys())[4]

        combined_chart_details['chart_ids'].append(chart.get(chart_id_key))
        combined_chart_details['positions'].append(chart.get('position'))
        combined_chart_details['chart_types'].append(chart.get(chart_type))
        combined_chart_details['chart_Xaxes'].append(chart.get(chart_Xaxis))
        combined_chart_details['chart_Yaxes'].append(chart.get(chart_Yaxis))
        combined_chart_details['chart_aggregates'].append(chart.get(chart_aggregate))
    insert_combined_chart_details(conn, combined_chart_details)
    conn.close()
    
    return jsonify({
        'message': 'Chart details saved successfully',
        'chart_details': combined_chart_details
    })

@app.route('/plot_dual_axis_chart', methods=['POST','GET'])
def get_duel_chart_route():
    data = request.json
    table_name = data['selectedTable']
    x_axis_columns = data['xAxis'].split(', ')  # Split multiple columns into a list
    y_axis_column = data['yAxis']
    aggregation = data['aggregate']
    checked_option=data['filterOptions']
    db_nameeee=data['databaseName']
    print("db_nameeee====================",db_nameeee)
    print("checked_option====================",checked_option)
    print("aggregate====================",aggregation) 
    print("x_axis_columns====================",x_axis_columns)
    print("y_axis_column====================",y_axis_column) 
    categories = {}
    for row in data:
        category = tuple(row[:-1])
        y_axis_value = row[-1]
        if category not in categories:
            categories[category] = initial_value(aggregation)
        update_category(categories, category, y_axis_value, aggregation)
    labels = [', '.join(category) for category in categories.keys()]  
    values = list(categories.values())
    print("labels====================",labels)
    print("values====================",values)
    return jsonify({"categories": labels, "values": values, "aggregation": aggregation})

def initial_value(aggregation):
    if aggregation in ['sum', 'average']:
        return 0
    elif aggregation == 'minimum':
        return float('inf')
    elif aggregation == 'maximum':
        return float('-inf')
    elif aggregation == 'count':
        return 0

def update_category(categories, category_key, y_axis_value, aggregation):
    if aggregation == 'sum':
        categories[category_key] += float(y_axis_value)
    elif aggregation == 'minimum':
        categories[category_key] = min(categories[category_key], float(y_axis_value))
    elif aggregation == 'maximum':
        categories[category_key] = max(categories[category_key], float(y_axis_value))
    elif aggregation == 'average':
        categories[category_key][0] += float(y_axis_value)
        categories[category_key][1] += 1
    elif aggregation == 'count':
        categories[category_key] += 1

@app.route('/upload_audio_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if file and allowed_file(file.filename, ALLOWED_EXTENSIONS):
        filename = secure_filename(file.filename)
        file.save(os.path.join(UPLOAD_FOLDER, filename))
        transcript=transcribe_audio_with_timestamps(os.path.join(UPLOAD_FOLDER, filename))
        postgres_dp=save_file_to_db(filename, transcript)
        print("postgres_dp is=======================> ",postgres_dp)
        return jsonify({'message': 'File successfully uploaded','transcription': transcript}), 200
    else:
        return jsonify({'error': 'Invalid file type'}), 400


from bar_chart import global_df
@app.route('/api/calculation', methods=['POST'])
def handle_calculation():
    data = request.json
    columnName = data.get('columnName')
    calculation = data.get('calculation')
    
    dataframe = calculationFetch()
    print("========================================================before calculation========================================================")
    print(dataframe.head(5))
    print("========================================================")
    if dataframe is None:
        return jsonify({'error': 'Failed to fetch data from the database'}), 500
    
    try:
        new_dataframe = perform_calculation(dataframe.copy(), columnName, calculation)
        print("========================================================after calculation========================================================")
        print(new_dataframe.head(5))
        print("========================================================")

        dataframe_json = new_dataframe.to_json(orient='split')
        return jsonify({'dataframe': dataframe_json})
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    except Exception as e:
        print(f"Error processing dataframe: {e}")
        return jsonify({'error': 'Error processing dataframe'}), 500
    
@app.route('/api/signup', methods=['POST'])
def signup():
    data = request.json
    userName = data.get('userName')
    password = data.get('password')
    retypePassword = data.get('retypePassword')
    organizationName = data.get('organizationName')
    email = data.get('email')
    if password != retypePassword:
        return jsonify({'error': 'Passwords do not match'}), 400
    try:
        insert_user_data(organizationName, email, userName, password)
        return jsonify({'message': 'User created successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/signUP_username', methods=['GET'])
def get_userdata():
    usersdata = fetch_usersdata()
    return jsonify(usersdata)

@app.route('/api/login', methods=['POST'])
def login():
    global company_name_global
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')
    company = data.get('company')

    if company is not None:
        company_name_global = company

    print("company_name====================",company)
    print("email====================",email)
    print("password================",password)

    usersdata = None
    employeedata = None
    if company is None:
        usersdata = fetch_login_data(email, password)
    else:
        employeedata = fetch_company_login_data(email, password, company)
        print("employeedata====================",employeedata)

    if email == 'superadmin@gmail.com' and password == 'superAdmin':
        session_id = str(uuid.uuid4())  # Unique session ID
        session['session_id'] = session_id
        session['user_role'] = 'admin'
        return jsonify({'message': 'Login successful to admin page', 'session_id': session_id}), 200
    elif usersdata:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session['user_id'] = usersdata # Assuming usersdata contains a user ID
        return jsonify({'message': 'Login successful to user page', 'session_id': session_id, 'data': usersdata}), 200
    elif employeedata:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session['employee_id'] = employeedata # Assuming employeedata contains employee ID
        return jsonify({'message': 'Login successful to user employee page', 'session_id': session_id, 'data': employeedata}), 200
    else:
        return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/api/singlevalue_text_chart', methods=['POST'])
def receive_single_value_chart_data():
    data = request.get_json()
    print("data====================",data)
    chart_id=data.get('chart_id')
    x_axis = data.get('text_y_xis')[0]
    databaseName = data.get('text_y_database')
    table_Name = data.get('text_y_table')
    print("table_Name====================",table_Name)
    aggregate=data.get('text_y_aggregate')
    print("x_axis====================",x_axis)  
    print("databaseName====================",databaseName)  
    print("table_Name====================",table_Name)
    print("aggregate====================",aggregate)
    aggregate_py = {
                    'count': 'count',
                    'sum': 'sum',
                    'average': 'mean',
                    'minimum': 'min',
                    'maximum': 'max'
                }.get(aggregate, 'sum') 
    fetched_data = fetchText_data(databaseName, table_Name, x_axis,aggregate_py)
    print("Fetched Data:", fetched_data)
    print(f"Received x_axis: {x_axis}")
    print(f"Received databaseName: {databaseName}")
    print(f"Received table_Name: {table_Name}")
    print(f"aggregate====================",{aggregate})
    return jsonify({"data": fetched_data,
                    "chart_id": chart_id,
                     "message": "Data received successfully!"})

@app.route('/api/text_chart', methods=['POST'])
def receive_chart_data():
    data = request.get_json()
    print("data====================",data)
    chart_id=data.get('chart_id')
    x_axis = data.get('text_y_xis')[0]
    databaseName = data.get('text_y_database')
    table_Name = data.get('text_y_table')[0]
    aggregate=data.get('text_y_aggregate')
    print("x_axis====================",x_axis)  
    print("databaseName====================",databaseName)  
    print("table_Name====================",table_Name)
    print("aggregate====================",aggregate)

    fetched_data = fetchText_data(databaseName, table_Name, x_axis,aggregate)
    print("Fetched Data:", fetched_data)
    print(f"Received x_axis: {x_axis}")
    print(f"Received databaseName: {databaseName}")
    print(f"Received table_Name: {table_Name}")
    print(f"aggregate====================",{aggregate})

    return jsonify({"data": fetched_data,
                    "chart_id": chart_id,
                     "message": "Data received successfully!"})


@app.route('/api/handle-clicked-category',methods=['POST'])
def handle_clicked_category():
    data=request.json
    category = data.get('category')
    charts= data.get('charts')  
    clicked_catagory_Xaxis=data.get('x_axis')
    print("Category clicked:", category)
    print("clicked_catagory_Xaxis====================",clicked_catagory_Xaxis)
    if isinstance(clicked_catagory_Xaxis, list):
            clicked_catagory_Xaxis = ', '.join(clicked_catagory_Xaxis)
    # print("Charts:", charts)    
    chart_details = []
    chart_data_list = []
    if charts:
        charts_count = len(charts)  
        print("Charts count:", charts_count)
        for chart in charts:   
            chart_id = chart.get('chart_id')
            table_name = chart.get('tableName')
            x_axis = chart.get('x_axis')
            y_axis = chart.get('y_axis')
            aggregate = chart.get('aggregate')
            chart_type = chart.get('chart_type')
            filter_options = chart.get('filter_options')
            database_name = chart.get('databaseName')
            if isinstance(x_axis, list):
                x_axis = ', '.join(x_axis)
            
            if isinstance(y_axis, list):
                y_axis = ', '.join(y_axis)
            chart_details.append([{
                'chart_id': chart_id,
                'table_name': table_name,
                'x_axis': x_axis,
                'y_axis': y_axis,
                'aggregate': aggregate,
                'filter_options': filter_options,
                'database_name': database_name
            }])
           
            print("x_axis====================",x_axis)
            chart_data = filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate,clicked_catagory_Xaxis,category)
            chart_data_list.append({
                "chart_id": chart_id,
                "data": chart_data
            })
        print(chart_data_list)
    return jsonify({"message": "Category clicked successfully!",
                    "chart_data_list": chart_data_list})


@app.route('/api/send-chart-details', methods=['POST'])
def receive_chart_details():
    data = request.json
    chart_id = data.get('chart_id')
    tableName = data.get('tableName')
    x_axis = data.get('x_axis')  # Assuming this is a list of columns to group by
    y_axis = data.get('y_axis')  # Assuming this is a list of columns to aggregate
    aggregate = data.get('aggregate')  # Aggregation method, e.g., 'sum', 'mean', etc.
    chart_type = data.get('chart_type')
    chart_heading = data.get('chart_heading')
    filter_options = data.get('filter_options').split(', ')  # Convert filter_options string to a list
    databaseName = data.get('databaseName')

    print("chart_id====================", chart_id)
    print("tableName====================", tableName)
    print("x_axis====================", x_axis)
    print("y_axis====================", y_axis)
    print("aggregate====================", aggregate)
    print("chart_type====================", chart_type)
    print("chart_heading====================", chart_heading)
    print("filter_options====================", filter_options)
    print("databaseName====================", databaseName)

    # Define aggregate function based on request
    aggregate_py = {
        'count': 'count',
        'sum': 'sum',
        'average': 'mean',
        'minimum': 'min',
        'maximum': 'max'
    }.get(aggregate, 'sum')  # Default to 'sum' if no match

    try:
        # Get the database connection
        connection = get_db_connection_view(databaseName)
        df = fetch_chart_data(connection, tableName)
        print(df.head())

        # Logic for 'singleValueChart'
        if chart_type == 'singleValueChart':
            try:
                df[y_axis[0]] = pd.to_numeric(df[y_axis[0]], errors='coerce')
                single_value = df[y_axis[0]].agg(aggregate_py)
                print(f"Single value computed: {single_value}")

                connection.close()

                # Return the single value as part of the response
                return jsonify({
                    "message": "Single value chart details received successfully!",
                    "single_value": float(single_value),  # Convert Decimal to float
                    "chart_type": chart_type,
                    "chart_heading": chart_heading,
                }), 200

            except Exception as e:
                print(f"Error processing singleValueChart: {e}")
                connection.close()
                return jsonify({"message": "Error processing single value chart", "error": str(e)}), 500
        if chart_type == 'sampleAitestChart':
            try:
                df = fetch_chart_data(connection, tableName)
                df, numeric_columns, text_columns = handle_column_data_types(df)
                histogram_details = generate_histogram_details(df)
                connection.close()
                return jsonify({
                    "histogram_details": histogram_details,
                }), 200
            except Exception as e:
                print("Error while processing chart:", e)
                return jsonify({"error": "An error occurred while generating the chart."}), 500
        if chart_type != 'treeHierarchy':
            if aggregate == 'count':
                grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")
                print("Grouped DataFrame with all rows:", grouped_df)
                filtered_df_valid = df[df[y_axis[0]].notnull()]
                grouped_df_valid = filtered_df_valid.groupby(x_axis[0])[y_axis[0]].count().reset_index(name="count")
                print("Grouped DataFrame with valid rows:", grouped_df_valid)
                chosen_grouped_df = grouped_df_valid
                categories = chosen_grouped_df[x_axis[0]].tolist()
                values = chosen_grouped_df["count"].tolist()
                filtered_categories = []
                filtered_values = []
                for category, value in zip(categories, values):
                    if category in filter_options:
                        filtered_categories.append(category)
                        filtered_values.append(value)

                print("filtered_categories====================", filtered_categories)
                print("filtered_values====================", filtered_values)

                connection.close()

                return jsonify({
                    "message": "Chart details received successfully!",
                    "categories": filtered_categories,
                    "values": filtered_values,
                    "chart_type": chart_type,
                    "chart_heading": chart_heading,
                    "x_axis": x_axis,
                    "y_axis": y_axis,
                }), 200
            else:
                if len(y_axis) == 2:
                    for axis in y_axis:
                        try:
                            df[axis] = pd.to_datetime(df[axis], errors='raise', format='%H:%M:%S')
                            df[axis] = df[axis].apply(lambda x: x.hour * 60 + x.minute)
                        except (ValueError, TypeError):
                            df[axis] = pd.to_numeric(df[axis], errors='coerce')

                    grouped_df = df.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
                    print("Grouped DataFrame (dual y-axis): ", grouped_df.head())

                    categories = grouped_df[x_axis[0]].tolist()
                    values1 = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float
                    values2 = [float(value) for value in grouped_df[y_axis[1]]]  # Convert Decimal to float

                    # Filter categories and values based on filter_options
                    filtered_categories = []
                    filtered_values1 = []
                    filtered_values2 = []
                    for category, value1, value2 in zip(categories, values1, values2):
                        if category in filter_options:
                            filtered_categories.append(category)
                            filtered_values1.append(value1)
                            filtered_values2.append(value2)

                    print("filtered_categories====================", filtered_categories)
                    print("filtered_values1====================", filtered_values1)
                    print("filtered_values2====================", filtered_values2)

                    connection.close()

                    # Return the filtered data for both series
                    return jsonify({
                        "message": "Chart details received successfully!",
                        "categories": filtered_categories,
                        "series1": filtered_values1,
                        "series2": filtered_values2,
                        "chart_type": chart_type,
                        "chart_heading": chart_heading,
                        "x_axis": x_axis,
                    }), 200

                else:
                    # Handle single y-axis column
                    try:
                        df[y_axis[0]] = pd.to_datetime(df[y_axis[0]], errors='raise', format='%H:%M:%S')
                        df[y_axis[0]] = df[y_axis[0]].apply(lambda x: x.hour * 60 + x.minute)
                    except (ValueError, TypeError):
                        df[y_axis[0]] = pd.to_numeric(df[y_axis[0]], errors='coerce')

                    grouped_df = df.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()

                    print("Grouped DataFrame: ", grouped_df.head())

                    categories = grouped_df[x_axis[0]].tolist()
                    values = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float

                    # Filter categories and values based on filter_options
                    filtered_categories = []
                    filtered_values = []
                    for category, value in zip(categories, values):
                        if category in filter_options:
                            filtered_categories.append(category)
                            filtered_values.append(value)

                    print("filtered_categories====================", filtered_categories)
                    print("filtered_values====================", filtered_values)

                    connection.close()

                    # Return the filtered data
                    return jsonify({
                        "message": "Chart details received successfully!",
                        "categories": filtered_categories,
                        "values": filtered_values,
                        "chart_type": chart_type,
                        "chart_heading": chart_heading,
                        "x_axis": x_axis,
                    }), 200
        else:
            # Logic for 'treeHierarchy' chart type
            filtered_categories = []
            filtered_values = []
            print("No grouping or conversion for treeHierarchy chart type.")

            connection.close()
            dataframe_dict = df.to_dict(orient='records')
            print("df_json====================", dataframe_dict)

            return jsonify({
                "message": "Chart details received successfully!",
                "categories": filtered_categories,
                "values": filtered_values,
                "chart_type": chart_type,
                "chart_heading": chart_heading,
                "x_axis": x_axis,
                "data frame": dataframe_dict,
            }), 200

    except Exception as e:
        print("Error: ", e)
        return jsonify({"message": "Error processing request", "error": str(e)}), 500



def get_dashboard_data(dashboard_name):
    conn = connect_to_db()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM dashboard_details_wu_id WHERE file_name = %s", (dashboard_name,))
            data = cursor.fetchone()
            
            if data is None:
                print(f"No data found for Chart ID: {dashboard_name}")
                return None
            cursor.close()
            conn.close()
            # print("data====================",data)
            return data
        except psycopg2.Error as e:
            print("Error fetching data for Chart", dashboard_name, ":", e)
            conn.close()
            return None
    else:
        return None

@app.route('/Dashboard_data/<dashboard_name>', methods=['GET'])
def dashboard_data(dashboard_name):
    data = get_dashboard_data(dashboard_name)
    print("chart datas------------------------------------------------------------------------------------------------------------------",data) 
    if data is not None:
        chart_ids = data[4]
        print("chart_ids====================",chart_ids)    
        chart_datas=get_dashboard_view_chart_data(chart_ids)
        # print("chart_datas====================",chart_datas)
        # return jsonify(data,chart_datas)
        return jsonify({
            "data": data,
            "chart_datas": chart_datas
        })
    else:
        return jsonify({'error': 'Failed to fetch data for Chart {}'.format(dashboard_name)})

@app.route('/saved_dashboard_total_rows', methods=['GET'])
def saved_dashboard_names():
    global company_name_global
    user_id = request.args.get('user_id')
    names = get_dashboard_names(user_id,company_name_global)
    print("names====================", names)   
    if names is not None:
        return jsonify({'chart_names': names})
    else:
        return jsonify({'error': 'Failed to fetch chart names'})


# @app.route('/api/usersignup', methods=['POST'])
# def usersignup():
    data = request.json
    print("Received Data:", data)

    register_type = data.get("registerType")
    print("Register Type:", register_type)

    user_details = data.get("userDetails")
    print("User Details:", user_details)

    if register_type == "manual":
        # Manual Registration Handling
        employee_name = user_details.get("employeeName")
        role_name = user_details.get("roleId")  # This is a role name (e.g., "Developer")
        organization_name = user_details.get("company")
        username = user_details.get("userName")
        email=user_details.get("email")
        password = user_details.get("password")
        retype_password = user_details.get("retypePassword")
        categories = user_details.get("categories", [])  # Extract categories as a list

        if password != retype_password:
            return jsonify({'message': 'Passwords do not match'}), 400

        # Connect to the signup database first to get the role_id
        conn_datasource = get_db_connection("datasource")
        if not conn_datasource:
            print("Failed to connect to signup database.")
            return jsonify({'message': 'Failed to connect to signup database'}), 500

        try:
            # Fetch the role_id based on the role name from the signup database
            with conn_datasource.cursor() as cursor_datasource:
                cursor_datasource.execute("""
                    SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)
                """, (role_name,))
                role_data = cursor_datasource.fetchone()

                if not role_data:
                    print(f"Role not found for role name: {role_name}")
                    return jsonify({'message': 'Role not found for role name: ' + role_name}), 404

                role_id = role_data[0]  # Get the role_id

        except Exception as e:
            print(f"Error fetching role_id: {e}")
            return jsonify({'message': 'Error fetching role_id'}), 500

        # Connect to the respective company's database
        conn = get_company_db_connection(organization_name)
        if not conn:
            print(f"Failed to connect to company database for {organization_name}.")
            return jsonify({'message': 'Failed to connect to company database'}), 500
        create_user_table(conn)

        try:
            with conn.cursor() as cursor:
                # Check if the username already exists in employee_list
                cursor.execute("""
                    SELECT COUNT(*) FROM employee_list WHERE username = %s
                """, (username,))
                result = cursor.fetchone()

                if result and result[0] > 0:
                    return jsonify({'message': 'Username already exists in employee_list'}), 400

                # Encrypt the password
                hashed_password = encrypt_password(password)

                # Define the action_type and action_by fields
                action_type = "add"  # New user registration, action type set to 'add'
                action_by = "admin"  # Set this to the current admin's username or user ID

                # Insert into employee_list table in the company database
                cursor.execute("""
                    INSERT INTO employee_list (employee_name, role_id, username,email, password,category, action_type, action_by)
                    VALUES (%s, %s, %s,%s, %s,%s, %s, %s)
                """, (employee_name, role_id, username,email, hashed_password, categories,action_type, action_by))

                # Retrieve the generated employee_id
                cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
                employee_id = cursor.fetchone()[0]

            # Handle category insertion with check for duplicate category under same company
            if categories:
                for category in categories:
                    with conn_datasource.cursor() as cursor_datasource:
                        # Check if the category already exists for the specific company
                        cursor_datasource.execute("""
                            SELECT category_id FROM category 
                            WHERE LOWER(category_name) = LOWER(%s) 
                            AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
                        """, (category, organization_name))
                        
                        result = cursor_datasource.fetchone()
                        
                        if result:
                            # If the category exists for the same company, raise an error
                            return jsonify({
                                'message': f"Category '{category}' already exists for the company '{organization_name}'"
                            }), 400

                        # Insert the new category if it doesn't exist
                        cursor_datasource.execute("""
                            INSERT INTO category (category_name, company_id)
                            VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
                            RETURNING category_id;
                        """, (category, organization_name))
                        category_id = cursor_datasource.fetchone()[0]  # Fetch the new category_id

                        # Insert the user into the signup database with the category_id
                        cursor_datasource.execute("""
                            INSERT INTO "user" (user_id, role_id, company_id, category_id)
                            VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
                        """, (employee_id, role_id, organization_name, category_id))

            # Commit transactions only after all validation and inserts are successful
            conn.commit()  
            conn_datasource.commit()

            return jsonify({'message': 'User and categories created successfully'}), 200
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({'message': 'Error creating user'}), 500

        finally:
            # Close both database connections
            conn.close()
            conn_datasource.close()

    elif register_type == "File_Upload":
    # File upload handling (similar logic for action_type applies here)
        conn_datasource = get_db_connection("datasource")
        if not conn_datasource:
            return jsonify({'message': 'Failed to connect to datasource database'}), 500

        try:
            for user in user_details:
                employee_name = user.get("employeeName")
                role_name = user.get("roleId")  # This is a role name (e.g., "Developer")
                organization_name = user.get("company")
                username = user.get("userName")
                password = user.get("Password")
                categories = user.get("category")
                email = user.get("email")
                category_list = [category.strip() for category in categories.splitlines()]

                if not employee_name or not role_name or not username or not categories:
                    return jsonify({'message': f'Missing required details for user: {username}'}), 400

                # Fetch the role_id from the signup database
                with conn_datasource.cursor() as cursor_datasource:
                    cursor_datasource.execute("""
                        SELECT role_id FROM role WHERE LOWER(role_name) = LOWER(%s)
                    """, (role_name,))
                    role_data = cursor_datasource.fetchone()

                    if not role_data:
                        return jsonify({'message': f'Role not found for user: {username}'}), 404

                    role_id = role_data[0]

                # Connect to the company's database
                conn = connect_db(organization_name)
                if not conn:
                    return jsonify({'message': f'Failed to connect to database for company: {organization_name}'}), 500

                try:
                    with conn.cursor() as cursor:
                        # Check if the username already exists in employee_list
                        cursor.execute("""
                            SELECT COUNT(*) FROM employee_list WHERE username = %s
                        """, (username,))
                        result = cursor.fetchone()

                        if result and result[0] > 0:
                            return jsonify({'message': f'Username already exists for user: {username}'}), 400

                        # Encrypt the password
                        hashed_password = encrypt_password(password)

                        # Insert into employee_list table
                        action_type = "add"  # File upload case, action type is 'add'
                        action_by = "admin"  # Set dynamically based on the user performing the action

                        cursor.execute("""
                            INSERT INTO employee_list (employee_name, role_id, username,category, email, password, action_type, action_by)
                            VALUES (%s, %s, %s, %s,%s, %s, %s, %s)
                        """, (employee_name, role_id, username, categories,email, hashed_password, action_type, action_by))

                        # Retrieve the generated employee_id
                        cursor.execute("SELECT currval(pg_get_serial_sequence('employee_list', 'employee_id'))")
                        employee_id = cursor.fetchone()[0]

                    # Handle category insertion for file upload with duplicate check
                    for category in category_list:
                        # Open a new cursor for each category
                        with conn_datasource.cursor() as cursor_datasource:
                            cursor_datasource.execute("""
                                SELECT category_id FROM category 
                                WHERE LOWER(category_name) = LOWER(%s) 
                                AND company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
                            """, (category, organization_name))
                            
                            result = cursor_datasource.fetchone()

                            if result:
                                return jsonify({
                                    'message': f"Category '{category}' already exists for the company '{organization_name}'"
                                }), 400

                            # Insert new category if not found
                            cursor_datasource.execute("""
                                INSERT INTO category (category_name, company_id)
                                VALUES (%s, (SELECT id FROM organizationdatatest WHERE organizationname = %s))
                                RETURNING category_id;
                            """, (category, organization_name))
                            category_id = cursor_datasource.fetchone()[0]

                        # Insert into the user table in the signup database
                        with conn_datasource.cursor() as cursor_datasource:  # New cursor for user table insertion
                            cursor_datasource.execute("""
                                INSERT INTO "user" (user_id, role_id, company_id, category_id)
                                VALUES (%s, %s, (SELECT id FROM organizationdatatest WHERE organizationname = %s), %s)
                            """, (employee_id, role_id, organization_name, category_id))

                    # Commit transactions after all user and category insertions are complete
                    conn.commit()
                    conn_datasource.commit()

                except Exception as e:
                    print(f"Error processing file upload user {username}: {e}")
                    return jsonify({'message': f'Error processing file upload for user: {username}'}), 500

                finally:
                    if conn:
                        conn.close()

        except Exception as e:
            print(f"Error processing file upload: {e}")
            return jsonify({'message': 'Error processing file upload'}), 500

        finally:
            if conn_datasource:
                conn_datasource.close()

        return jsonify({'message': 'File upload processed successfully'}), 200


@app.route('/api/usersignup', methods=['POST'])
def usersignup():
    data = request.json
    print("Received Data:", data)

    register_type = data.get("registerType")
    print("Register Type:", register_type)

    user_details = data.get("userDetails")
    print("User Details:", user_details)

    if register_type == "manual":
        return handle_manual_registration(user_details)
    elif register_type == "File_Upload":
        return handle_file_upload_registration(user_details)

    return jsonify({'message': 'Invalid registration type'}), 400

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

@app.route('/api/companies', methods=['GET'])
def get_companies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT id,organizationName FROM organizationdatatest')
    companies = cursor.fetchall()
    cursor.close()
    conn.close()
    company_list = [{'id': company[0], 'name': company[1]} for company in companies]
    print(company_list)
    return jsonify(company_list)

@app.route('/api/roles', methods=['GET'])
def get_roles():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT role_id ,role_name FROM role')
    roles = cur.fetchall()
    cur.close()
    conn.close()
    role_list = [{'id': role[0], 'name': role[1]} for role in roles]
    print(role_list)
    return jsonify(role_list)

@app.route('/fetchglobeldataframe', methods=['GET'])
def get_hello_data():
    dataframe=bc.global_df
    print("dataframe........................",dataframe)
    dataframe_dict = dataframe.to_dict(orient='records')
    return jsonify({"data frame":dataframe_dict})

@app.route('/aichartdata', methods=['GET'])
def ai_barchart():
    dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
    histogram_details = generate_histogram_details(dataframe)
    return jsonify({"histogram_details": histogram_details})

@app.route('/boxplotchartdata', methods=['GET'])
def ai_boxPlotChart():
    dataframe = bc.global_df  # Assuming bc.global_df is your DataFrame
    # axes = dataframe.hist()
    aaa = dataframe.plot(kind='box', subplots=True, layout=(4, 2), sharex=False, sharey=False)
    details = []

    for ax in aaa:
        ax_details = {
            "Title": ax.get_title(),
            "X-axis label": ax.get_xlabel(),
            "Y-axis label": ax.get_ylabel(),
            "X-axis limits": ax.get_xlim(),
            "Y-axis limits": ax.get_ylim(),
            "Number of elements (patches)": len(ax.patches)
        }
        details.append(ax_details)

        # Now 'details' contains all the axis details in an array of dictionaries
        print(details)
        # # Print non-empty histogram details
        # for detail in filtered_histogram_details:
        #     print(detail)

    # Return JSON response
    return jsonify({
        # "data_frame": dataframe_dict,
        "histogram_details": details
    })

# ////////-----------------15-10-2024-----------gayathri------//////////

@app.route('/api/fetch_categories', methods=['GET'])
def fetch_categories():
    try:
        conn = get_db_connection('datasource')
        cursor = conn.cursor()
        cursor.execute("SELECT category_id, category_name FROM category")
        categories = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify([{'id': row[0], 'name': row[1]} for row in categories])
    except Exception as e:
        print(f"Error fetching categories: {e}")
        return jsonify({'message': 'Error fetching categories'}),500

@app.route('/api/users', methods=['GET'])
def get_all_users():
    company_name = request.args.get('companyName')
    print("company_name====================",company_name)
    page = int(request.args.get('page', 1))  # Default to page 1 if not provided
    limit = int(request.args.get('limit', 10))  # Default to 10 users per page

    if not company_name:
        return jsonify({'error': 'Company name is required'}), 400

    try:
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()

        # Calculate the offset based on the current page and limit
        offset = (page - 1) * limit

        # Fetch users for the specified company with pagination
        cursor.execute("""
            SELECT employee_name, username, role_id, category 
            FROM employee_list
            LIMIT %s OFFSET %s;
        """, (limit, offset))

        users = cursor.fetchall()

        # Fetch the total number of users for the specified company (for pagination)
        cursor.execute("SELECT COUNT(*) FROM employee_list;")
        total_users = cursor.fetchone()[0]

        # Create a list of user dictionaries
        user_list = [
            {
                'employee_name': user[0],
                'username': user[1],
                'role_id': user[2],
                'category': user[3],
            }
            for user in users
        ]

        return jsonify({
            'users': user_list,
            'total': total_users  # Total users count for pagination
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_company_db_connection(company_name):

    # This is where you define the connection string
    conn = psycopg2.connect(
        dbname=company_name,  # Ensure this is the correct company database
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

@app.route('/api/fetch_user_data', methods=['POST'])
def fetch_user_data():
    data = request.json
    print("data",data)
    username = data.get('username')
    organization_name = data.get('organization_name')  # Changed from company_id to organization_name
    print("username",username)
    print("organization_name",organization_name)


    # Check if username and organization_name are provided
    if not username or not organization_name:
        return jsonify({'message': 'Username and organization_name are required'}), 400

    # Connect to the company's database using organization_name
    conn = get_company_db_connection(organization_name)
    if not conn:
        print(f"Failed to connect to company database for {organization_name}.")
        return jsonify({'message': 'Failed to connect to company database'}), 500

    try:
        # Fetch employee details based on username
        cursor = conn.cursor()
        cursor.execute("""
            SELECT employee_id, employee_name, role_id,category, username,email FROM employee_list WHERE username = %s
        """, (username,))
        employee_data = cursor.fetchone()
        cursor.close()

        if not employee_data:
            return jsonify({'message': 'Employee not found'}), 404

        # Extract employee data
        employee_id, employee_name, role_id, category_name,username,email = employee_data
        print("employee data",employee_data)
        # Fetch the category name from the signup database
        conn_datasource = get_db_connection("datasource")
        user_details = {
            'employee_id': employee_id,
            'employee_name': employee_name,
            'role_id': role_id,
            'username': username,
            'category_name': category_name,
            'email':email
        }
        print("userdetails",user_details)
        # Check if new role or category needs to be updated
        new_role_id = data.get('new_role_id')
        new_category_name = data.get('new_category_name')

        if new_role_id or new_category_name:
            # Update role if provided
            if new_role_id:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE employee_list SET role_id = %s WHERE employee_id = %s
                """, (new_role_id, employee_id))
                conn.commit()
                cursor.close()
                user_details['role_id'] = new_role_id  # Update in response

            # Update category if provided
            if new_category_name:
                cursor_datasource = conn_datasource.cursor()
                cursor_datasource.execute("""
                    UPDATE category SET category_name = %s WHERE company_id = (SELECT id FROM organizationdatatest WHERE organizationname = %s)
                """, (new_category_name, organization_name))
                conn_datasource.commit()
                cursor_datasource.close()
                user_details['category_name'] = new_category_name  # Update in response

        # Return the employee details along with updated role/category if applicable
        return jsonify(user_details), 200

    except Exception as e:
        print(f"Error fetching or updating user data: {e}")
        return jsonify({'message': 'Error fetching or updating user data'}), 500

    finally:
        conn.close()

@app.route('/api/update_user_details', methods=['POST'])
def update_user_details():
    data = request.json
    print("data......",data)
    username = data.get('username')
    organization_name = data.get('companyName')
    new_role_id = data.get('roleId')
    print("new role id",new_role_id)
    category_name = data.get('categoryName')
    formatted_category_name = f'{{{category_name}}}'  # Format category with curly braces
    print("category---------", formatted_category_name)

    # Connect to company database
    conn = connect_db(organization_name)
    if not conn:
        return jsonify({'message': f'Failed to connect to database for company: {organization_name}'}), 500

    try:
        cursor = conn.cursor()

        # Update role
        if new_role_id:
            cursor.execute("""
                UPDATE employee_list SET role_id = %s ,category=%s WHERE username = %s
                
            """, (new_role_id,formatted_category_name, username))
            conn.commit()

        return jsonify({'message': 'User details updated successfully'}), 200
    except Exception as e:
        print(f"Error updating user details: {e}")
        return jsonify({'message': 'Error updating user details'}), 500
    finally:
        conn.close()

@app.route('/api/user/<username>', methods=['PUT'])
def update_user(username):
    data = request.get_json()
    company_name = data.get('company_name')
    role_id = data.get('role_id')
    category_name = data.get('category')

    if not company_name or not role_id or not category_name:
        return jsonify({'error': 'Company name, role, and category are required'}), 400

    try:
        conn = get_company_db_connection(company_name)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE employee_list 
            SET role_id = %s, category = %s 
            WHERE username = %s;
        """, (role_id, category_name, username))
        conn.commit()

        return jsonify({'message': 'User details updated successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            conn.close()

from predictions import load_and_predict  
@app.route('/api/predictions', methods=['GET','POST'])
def get_predictions():
    data = request.json
    x_axis = data.get('xAxis')
    y_axis = data.get('yAxis')    
    timePeriod = data.get('timePeriod')
    number_of_periods = data.get('number')   
    print("xAxis:", x_axis, "yAxis:", y_axis)
    print("timePeriod:", timePeriod, "number_of_periods:", number_of_periods)
    prediction_data = load_and_predict(x_axis, y_axis,number_of_periods,timePeriod)
    # prediction_data = load_and_predict(x_axis, y_axis)
    return jsonify(prediction_data)  # Return data as JSON

@app.route('/Hierarchial-backend-endpoint', methods=['POST', 'GET'])
def handle_hierarchical_bar_click():
    global global_df

    if request.method == 'POST':
        data = request.json
        print("Received request data:", data)

        clicked_category = data.get('category')
        x_axis_columns = data.get('xAxis')
        y_axis_column = data.get('yAxis')
        table_name = data.get('tableName')
        db_name = data.get('databaseName')
        current_depth = data.get('currentLevel', 0)

        print("Clicked Category:", clicked_category)
        print("X-axis Columns:", x_axis_columns)

        try:
            if global_df is None:
                global_df = fetch_hierarchical_data(table_name, db_name)
                print("Fetched data:", global_df.head() if global_df is not None else "No data returned")

            if global_df is None or global_df.empty:
                return jsonify({"error": "Data could not be loaded into global_df."}), 500

            if y_axis_column[0] not in global_df.columns:
                return jsonify({"error": f"Column {y_axis_column[0]} not found in global_df."}), 500
            
            global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
            drill_down_result = Hierarchial_drill_down(
                clicked_category=clicked_category, 
                x_axis_columns=x_axis_columns, 
                y_axis_column=y_axis_column, 
                depth=current_depth, 
                aggregation=data.get('aggregation')
            )
            return jsonify(drill_down_result)

        except Exception as e:
            print("An error occurred in handle_hierarchical_bar_click:", str(e))
            return jsonify({"error": "An internal error occurred.", "message": str(e)}), 500

@app.route('/nlp_upload_audio', methods=['POST'])
def nlp_upload_audio():
    # Retrieve audio file and form data
    audio = request.files.get('audio')
    table_name = request.form.get('tableName')
    database_name = request.form.get('databaseName')

    if not audio:
        return jsonify({"error": "No audio file uploaded"}), 400

    # Save audio file
    file_path = os.path.join(UPLOAD_FOLDER, audio.filename)
    try:
        audio.save(file_path)
    except Exception as e:
        return jsonify({"error": f"Failed to save audio file: {str(e)}"}), 500

    # Log for debugging
    print(f"Audio file saved to: {file_path}")
    print(f"Table name: {table_name}")
    print(f"Database name: {database_name}")

    # Return success response
    return jsonify({
        "message": "Audio uploaded successfully",
        "tableName": table_name,
        "databaseName": database_name,
        "filePath": file_path
    })

@app.route('/upload-json', methods=['POST'])
def upload_file_json():
    try:
        database_name = request.form.get('company_database')
        primary_key_column = request.form.get('primaryKeyColumnName')
        
        # Check if file is present in the request
        if 'file' not in request.files:
            return jsonify({'message': 'No file part in the request'}), 400

        json_file = request.files['file']
        
        # Check if a file is selected
        if json_file.filename == '':
            return jsonify({'message': 'No file selected for uploading'}), 400

        print("primary_key_column:", primary_key_column)
        print("json_file:", json_file.filename)
        print("database_name:", database_name)
        
        # Save the file to a temporary directory
        json_file_name = secure_filename(json_file.filename)
        os.makedirs('tmp', exist_ok=True)
        temp_file_path = f'tmp/{json_file_name}'
        json_file.save(temp_file_path)

        # Call the upload_json_to_postgresql function
        result = upload_json_to_postgresql(database_name, username, password, temp_file_path, primary_key_column, host, port)
        
        if result == "Upload successful":
            return jsonify({'message': 'File uploaded successfully'}), 200
        else:
            return jsonify({'message': result}), 500

    except Exception as e:
        traceback.print_exc()
        return jsonify({'message': f"Internal Server Error: {str(e)}"}), 500

@app.route('/api/employees', methods=['GET'])
def get_employees():
    company = request.args.get('company')  # Get company name from query parameter
    print("comapnay",company)
    if not company:
        return jsonify({"error": "Company parameter is missing"}), 400

    conn = get_company_db_connection(company)  # Assuming `get_db_connection` is a function to connect to your database
    cur = conn.cursor()
    cur.execute('SELECT employee_id, employee_name FROM employee_list ')
    employees = cur.fetchall()
    cur.close()
    conn.close()

    # Format the employee data into a list of dictionaries
    employee_list = [{'employee_id': emp[0], 'employee_name': emp[1]} for emp in employees]
    print(employee_list)  # Optionally, log the employee list for debugging purposes

    return jsonify(employee_list)






# test
from psycopg2.extras import RealDictCursor


@app.route('/delete-chart', methods=['DELETE'])
def delete_dashboard_name():
    chart_name = request.json.get('chart_name')  # Get the chart_name from JSON body
    
    if not chart_name:
        return jsonify({"error": "Chart name is required"}), 400
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("DELETE FROM dashboard_details_wu_id WHERE file_name = %s", (chart_name,))
        rows_deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()

        if rows_deleted == 0:
            return jsonify({"message": f"No chart found with the name '{chart_name}'"}), 404

        return jsonify({"message": f"Chart '{chart_name}' deleted successfully"}), 200
    
    except Exception as e:
        print("Error while deleting chart:", e)
        return jsonify({"error": "Failed to delete chart"}), 500


@app.route('/api/charts/<string:chart_name>', methods=['DELETE'])
def delete_chart(chart_name):
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500
    
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Delete the chart from the table
        cur.execute("DELETE FROM new_dashboard_details_new WHERE chart_name = %s", (chart_name,))
        rows_deleted = cur.rowcount
        
        # Commit changes and close the connection
        conn.commit()
        cur.close()
        conn.close()

        # If no rows were deleted, return a 404 error
        if rows_deleted == 0:
            return jsonify({"message": f"No chart found with the name '{chart_name}'"}), 404

        return jsonify({"message": f"Chart '{chart_name}' deleted successfully"}), 200
    
    except Exception as e:
        print("Error while deleting chart:", e)
        return jsonify({"error": "Failed to delete chart"}), 500


@app.route('/api/is-chart-in-dashboard', methods=['GET'])
def is_chart_in_dashboard():
    chart_name = request.args.get('chart_name')
    
    if not chart_name:
        return jsonify({"error": "Chart name is required"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Fetch data using subquery for chart_id
        cur.execute("""
            SELECT * 
FROM dashboard_details_wu_id 
WHERE 
    (SELECT id FROM new_dashboard_details_new WHERE chart_name = %s)::INTEGER 
    = ANY(string_to_array(trim(BOTH '{}' FROM chart_ids), ',')::INTEGER[])

        """, (chart_name,))
        
        chart_in_dashboard = cur.fetchone()
        cur.close()
        conn.close()

        if chart_in_dashboard:
            return jsonify({
                "isInDashboard": True,
                "message": f"Chart '{chart_name}' is being used in a dashboard."
            }), 200
        else:
            return jsonify({
                "isInDashboard": False,
                "message": f"Chart '{chart_name}' is not being used in a dashboard."
            }), 200

    except Exception as e:
        print("Error checking chart usage:", e)
        return jsonify({"error": "Failed to check if chart is used"}), 500
    

       
@app.route('/api/checkTableUsage', methods=['GET'])
def check_table_usage():
    table_name = request.args.get('tableName')

    if not table_name:
        return jsonify({"error": "Table name is required"}), 400

    # Remove any surrounding quotes from the table name
    table_name = table_name.strip('"').strip("'")

    # Debugging: Print the received table name
    print(f"Received table name: {table_name}")

    # Check if the table is used for chart creation
    is_in_use = is_table_used_in_charts(table_name)
    print("is_in_use",is_in_use)
    return jsonify({"isInUse": is_in_use})

# Function to check if a table is used in chart creation
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


# PUSH DATE 17-12-2024


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)
    # app.run(debug=True)



