import psycopg2
from psycopg2 import sql
from flask import jsonify, request
from config import DB_NAME,USER_NAME,PASSWORD,HOST,PORT

from histogram_utils import generate_histogram_details,handle_column_data_types

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, 
            user=USER_NAME, 
            password=PASSWORD, 
            host=HOST, 
            port=PORT
        )
        return conn
    except Exception as e:
        print(f"Error creating connection to the database: {e}")
        return None

# Function to create the table if it doesn't exist
def create_dashboard_table(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dashboard_details_WU_ID (
        id SERIAL PRIMARY KEY,
        user_id integer, 
        company_name VARCHAR(255),  
        file_name VARCHAR(255), 
        chart_ids VARCHAR(255),
        position VARCHAR(255),
        chart_type VARCHAR(255),
        chart_Xaxis VARCHAR(255),
        chart_Yaxis VARCHAR(255),
        chart_aggregate VARCHAR(255),
        filterdata VARCHAR(255),
        clicked_category VARCHAR(255)
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_combined_chart_details(conn, combined_chart_details):
    insert_query = """
    INSERT INTO dashboard_details_WU_ID 
    (user_id,company_name,file_name, chart_ids, position, chart_type, chart_Xaxis, chart_Yaxis, chart_aggregate, filterdata, clicked_category)
    VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(insert_query, (
            combined_chart_details['user_id'],
            combined_chart_details['company_name'],
            combined_chart_details['file_name'],
            combined_chart_details['chart_ids'], 
            str(combined_chart_details['positions']), 
            str(combined_chart_details['chart_types']),
            str(combined_chart_details['chart_Xaxes']), 
            str(combined_chart_details['chart_Yaxes']), 
            str(combined_chart_details['chart_aggregates']),
            str(combined_chart_details['filterdata']), 
            combined_chart_details['clicked_category']
        ))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error inserting combined chart details: {e}")




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

def get_company_db_connection(company_name):

    # This is where you define the connection string
    conn = psycopg2.connect(
        dbname=company_name,  # Ensure this is the correct company database
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return conn 

def get_dashboard_names(user_id, database_name):
    # Step 1: Get employees reporting to the given user_id from the company database.
    conn_company = get_company_db_connection(database_name)
    reporting_employees = []

    if conn_company:
        try:
            with conn_company.cursor() as cursor:
                # Check if reporting_id column exists dynamically (skip errors if missing).
                cursor.execute(""" 
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name='employee_list' AND column_name='reporting_id'
                """)
                column_exists = cursor.fetchone()

                if column_exists:
                    # Fetch employees who report to the given user_id (including NULL reporting_id if not assigned).
                    cursor.execute("""
                         SELECT employee_id FROM employee_list WHERE reporting_id = %s 
                    """, (user_id,))
                    reporting_employees = [row[0] for row in cursor.fetchall()]
        except psycopg2.Error as e:
            print(f"Error fetching reporting employees: {e}")
        finally:
            conn_company.close()

    # Include the user's own employee_id for fetching their charts.
    # Convert all IDs to integers for consistent data type handling.
    all_employee_ids = list(map(int, reporting_employees)) + [int(user_id)]

    # Step 2: Fetch dashboard names for these employees from the datasource database.
    conn_datasource = get_db_connection("datasource")
    dashboard_names = {}

    if conn_datasource:
        try:
            with conn_datasource.cursor() as cursor:
                # Create placeholders for the IN clause
                placeholders = ', '.join(['%s'] * len(all_employee_ids))
                query = f"""
                    SELECT user_id, file_name FROM dashboard_details_wu_id
                    WHERE user_id IN ({placeholders}) and company_name = %s
                """
                # cursor.execute(query, tuple(all_employee_ids))
                cursor.execute(query, tuple(map(str, all_employee_ids))+ (database_name,))
                dashboards = cursor.fetchall()

                # Organize dashboards by user_id
                for uid, file_name in dashboards:
                    if uid not in dashboard_names:
                        dashboard_names[uid] = []
                    dashboard_names[uid].append(file_name)
        except psycopg2.Error as e:
            print(f"Error fetching dashboard details: {e}")
        finally:
            conn_datasource.close()

    return dashboard_names

import psycopg2
import pandas as pd
from viewChart.viewChart import get_db_connection_view,fetch_chart_data
from bar_chart import fetchText_data


# def get_dashboard_view_chart_data(chart_ids):
#     conn = create_connection()  # Initial connection to your main database
#     if conn:
#         try:
#             if isinstance(chart_ids, str):
#                 import ast
#                 chart_ids = ast.literal_eval(chart_ids)  # Convert string representation of list to actual list

#             chart_data_list = []

#             for chart_id in chart_ids:
#                 cursor = conn.cursor()
#                 cursor.execute("SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type, filter_options, chart_heading,chart_color FROM new_dashboard_details_new WHERE id = %s", (chart_id,))
#                 chart_data = cursor.fetchone()
#                 cursor.close()

#                 if chart_data:
#                     # Extract chart data
#                     database_name = chart_data[1]  # Assuming `database_name` is the second field
#                     table_name = chart_data[2]
#                     x_axis = chart_data[3]
#                     y_axis = chart_data[4]  # Assuming y_axis is a list
#                     aggregate = chart_data[5]
#                     chart_type = chart_data[6]
#                     chart_heading = chart_data[8]
#                     filter_options = chart_data[7]
#                     chart_color = chart_data[9]  # Assuming chart_color is a list

#                     # Determine the aggregation function
#                     aggregate_py = {
#                         'count': 'count',
#                         'sum': 'sum',
#                         'average': 'mean',
#                         'minimum': 'min',
#                         'maximum': 'max'
#                     }.get(aggregate, 'sum')  # Default to 'sum' if no match

def fetch_external_db_connection(database_name,selected_user):
    try:
        print("company_name",database_name)
        # Connect to local PostgreSQL to get external database connection details
        conn = psycopg2.connect(
           dbname=database_name,  # Ensure this is the correct company database
        user=USER_NAME,password=PASSWORD,host=HOST,port=PORT
        )
        print("conn",conn)
        cursor = conn.cursor()
        query = """
            SELECT * 
            FROM external_db_connections 
            WHERE savename = %s 
            ORDER BY created_at DESC 
            LIMIT 1;
        """
        print("query",query)
        cursor.execute(query, (selected_user,))
        connection_details = cursor.fetchone()
        conn.close()
        return connection_details
    except Exception as e:
        print(f"Error fetching connection details: {e}")
        return None

def get_dashboard_view_chart_data(chart_ids,positions):
    conn = create_connection()  # Initial connection to your main database
    if conn:
        try:
            # if isinstance(chart_ids, str):
            #     import ast
            #     chart_ids = ast.literal_eval(chart_ids)  # Convert string representation of list to actual list
            # # sorted_chart_ids = sorted(chart_ids, key=lambda x: list(chart_ids).index(x))

            # chart_positions = {chart_id: position for chart_id, position in zip(chart_ids, positions)}
            
            # # Sort chart_ids based on the positions provided
            # sorted_chart_ids = sorted(chart_ids, key=lambda x: (chart_positions.get(x, {'x': 0, 'y': 0})['x'], chart_positions.get(x, {'x': 0, 'y': 0})['y']))

            if isinstance(chart_ids, str):
                import ast
                chart_ids = ast.literal_eval(chart_ids)  # Convert string representation of list to actual list
            
            # Convert chart_ids to a list if it's a set
            if isinstance(chart_ids, set):
                chart_ids = list(chart_ids)

            # Ensure positions is a list of dictionaries
            if isinstance(positions, str):
                positions = ast.literal_eval(positions)  # If positions are passed as a string, convert it to list of dicts

            # Create a dictionary for chart ids and their positions
            chart_positions = {chart_id: position for chart_id, position in zip(chart_ids, positions)}
            
            # Check if all positions are in the correct format
            for chart_id, position in chart_positions.items():
                if not isinstance(position, dict) or 'x' not in position or 'y' not in position:
                    print(f"Invalid position for chart_id {chart_id}: {position}")
                    return []

            # Sort chart_ids based on the positions provided
            sorted_chart_ids = sorted(chart_ids, key=lambda x: (chart_positions.get(x, {'x': 0, 'y': 0})['x'], chart_positions.get(x, {'x': 0, 'y': 0})['y']))

            chart_data_list = []
            print("chart_data_list",chart_data_list)
            for chart_id in sorted_chart_ids:
                cursor = conn.cursor()
                cursor.execute("SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type, filter_options, chart_heading, chart_color, selectedUser FROM new_dashboard_details_new WHERE id = %s", (chart_id,))
                chart_data = cursor.fetchone()
                cursor.close()

                if chart_data:
                    # Extract chart data
                    database_name = chart_data[1]  # Assuming `database_name` is the second field
                    table_name = chart_data[2]
                    x_axis = chart_data[3]
                    y_axis = chart_data[4]  # Assuming y_axis is a list
                    aggregate = chart_data[5]
                    chart_type = chart_data[6]
                    chart_heading = chart_data[8]
                    filter_options = chart_data[7]
                    chart_color = chart_data[9]  # Assuming chart_color is a list
                    selected_user = chart_data[10]  # Extract the selectedUser field
                
                    # Determine the aggregation function
                    aggregate_py = {
                        'count': 'count',
                        'sum': 'sum',
                        'average': 'mean',
                        'minimum': 'min',
                        'maximum': 'max'
                    }.get(aggregate, 'sum')  # Default to 'sum' if no match

                    # Check if selectedUser is NULL
                    if selected_user is None:
                        # Use the default local connection if selectedUser is NULL
                        connection = get_db_connection_view(database_name)
                        print('Using local database connection')

                    else:
                        # Use external connection if selectedUser is provided
                        connection = fetch_external_db_connection(database_name, selected_user)
                        host = connection[3]
                        dbname = connection[7]
                        user = connection[4]
                        password = connection[5]

                        # Create a new psycopg2 connection using the details from the tuple
                        connection = psycopg2.connect(
                            dbname=dbname,
                            user=user,
                            password=password,
                            host=host
                        )
                        print('External Connection established:', connection)
                    if chart_type == "wordCloud":
                        if len(y_axis) == 0:
                            x_axis_columns_str = ', '.join(x_axis)
                            print("x_axis_columns_str:", x_axis_columns_str)
                            query = f"""
                                SELECT word, COUNT(*) AS word_count
                                FROM (
                                    SELECT regexp_split_to_table({x_axis_columns_str}, '\\s+') AS word
                                    FROM {table_name}
                                ) AS words
                                GROUP BY word
                                ORDER BY word_count DESC;
                            """
                            print("WordCloud SQL Query:", query)

                            try:
                                cursor = connection.cursor()
                                cursor.execute(query)
                                data = cursor.fetchall()
                                cursor.close()
                                print("wordcloulddata",data)
                                if data:
                                    categories = [row[0] for row in data]  # Words
                                    values = [row[1] for row in data]     # Counts

                                    chart_data_list.append({
                                        "chart_id": chart_id,
                                        "categories": categories,
                                        "values": values,
                                        "chart_type": chart_type,
                                        "chart_heading": chart_heading,
                                        "positions": chart_positions.get(chart_id)
                                    })
                                    continue
                                else:
                                    print("No data returned for WordCloud query")
                            except Exception as e:
                                print("Error executing WordCloud query:", e)
                                chart_data_list.append({
                                    "error": f"WordCloud query failed: {str(e)}"
                                })
                    
                    
    # Fetch single aggregated value based on user

                    # Handle singleValueChart type separately
                    elif chart_type == "singleValueChart":
                        print("sv")
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate,selected_user)
                        
                        print("Single Value Result for Chart ID", chart_id, ":", single_value_result)

                        # Append single value chart data
                        chart_data_list.append({
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_heading": chart_heading,
                            "value": single_value_result,
                            "positions": chart_positions.get(chart_id)
                        })
                        continue  # Skip further processing for this chart ID

                    # Proceed with category and value generation for non-singleValueChart types
                    
                    dataframe = fetch_chart_data(connection, table_name)
                    
                    print("Chart ID", chart_id)
                    print("Chart Type", chart_type)

                    # Convert y_axis values if required (either in time format or as numeric)
                    for axis in y_axis:
                        try:
                            dataframe[axis] = pd.to_datetime(dataframe[axis], errors='raise', format='%H:%M:%S')
                            dataframe[axis] = dataframe[axis].dt.hour * 60 + dataframe[axis].dt.minute + dataframe[axis].dt.second / 60
                            print(f"Converted Time to Minutes for {axis}: ", dataframe[axis].head())
                        except ValueError:
                            dataframe[axis] = pd.to_numeric(dataframe[axis], errors='coerce')

                    # Check if the aggregation type is count
                    if aggregate_py == 'count':
                        print("Aggregate is count", aggregate_py)
                        print("X-Axis:", x_axis)
                        
                        df=fetch_chart_data(connection, table_name)
                        print("dataframe---------",df.head(5))

                        grouped_df = df.groupby(x_axis[0]).size().reset_index(name="count")
                        print("grouped_df---------",grouped_df)
                        print("Grouped DataFrame (count):", grouped_df.head())

                        categories = grouped_df[x_axis[0]].tolist()
                        values = grouped_df["count"].tolist()

                        # Filter categories and values based on filter_options
                        filtered_categories = []
                        filtered_values = []
                        for category, value in zip(categories, values):
                            if category in filter_options:
                                filtered_categories.append(category)
                                filtered_values.append(value)

                        print("Filtered Categories:", filtered_categories)
                        print("Filtered Values:", filtered_values)

                        chart_data_list.append({
                            "categories": filtered_categories,
                            "values": filtered_values,
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregate,
                            "positions": chart_positions.get(chart_id)
                        })
                        continue  # Skip further processing for this chart ID

                    
                    if chart_type == "treeHierarchy":
                        # No grouping or transformation needed for this chart type
                        filtered_categories = []
                        filtered_values = []
                        print("No grouping or conversion for treeHierarchy chart type.")
                        
                        connection.close()  # Ensure DB connection is closed
                        dataframe_dict = df.to_dict(orient='records')  # Convert DataFrame to JSON
                        print("df_json====================", dataframe_dict)

                        return jsonify({
                            "message": "Chart details received successfully!",
                            "categories": filtered_categories,
                            "values": filtered_values,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "chart_heading": chart_heading,
                            "x_axis": x_axis,
                            "data_frame": dataframe_dict,
                            "positions": chart_positions.get(chart_id)
                        }), 200
                    # Handle dual y_axis columns
                    elif chart_type == "duealChart":
                        grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
                        print("Grouped DataFrame (dual y-axis):", grouped_df.head())

                        categories = grouped_df[x_axis[0]].tolist()
                        values1 = [float(value) for value in grouped_df[y_axis[0]]]
                        values2 = [float(value) for value in grouped_df[y_axis[1]]]

                        # Filter categories and values based on filter_options
                        filtered_categories = []
                        filtered_values1 = []
                        filtered_values2 = []
                        for category, value1, value2 in zip(categories, values1, values2):
                            if category in filter_options:
                                filtered_categories.append(category)
                                filtered_values1.append(value1)
                                filtered_values2.append(value2)

                        print("Filtered Categories:", filtered_categories)
                        print("Filtered Values (Series 1):", filtered_values1)
                        print("Filtered Values (Series 2):", filtered_values2)

                        chart_data_list.append({
                            "categories": filtered_categories,
                            "series1": filtered_values1,
                            "series2": filtered_values2,
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregate,
                            "positions": chart_positions.get(chart_id)
                        })
                    else:
                        grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
                        print("Grouped DataFrame:", grouped_df.head())

                        categories = grouped_df[x_axis[0]].tolist()
                        values = [float(value) for value in grouped_df[y_axis[0]]]

                        # Filter categories and values based on filter_options
                        filtered_categories = []
                        filtered_values = []
                        for category, value in zip(categories, values):
                            if category in filter_options:
                                filtered_categories.append(category)
                                filtered_values.append(value)

                        print("Filtered Categories:", filtered_categories)
                        print("Filtered Values:", filtered_values)

                        chart_data_list.append({
                            "categories": filtered_categories,
                            "values": filtered_values,
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_color": chart_color,
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregate,
                            "positions": chart_positions.get(chart_id)
                        })

            conn.close()  # Close the main connection
            return chart_data_list

        except psycopg2.Error as e:
            print("Error fetching chart data:", e)
            conn.close()
            return None
    else:
        return None



def fetch_TreeHierarchy_Data(connection, tableName):
    try:
        cursor = connection.cursor()

        # Use SQL composition to safely query using dynamic table and column names
        query = sql.SQL("SELECT * FROM {table}")
        query = query.format(
            table=sql.Identifier(tableName)
        )

        cursor.execute(query)
        results = cursor.fetchall()

        # Fetch the column names from the cursor
        column_names = [desc[0] for desc in cursor.description]

        # Convert the results to a DataFrame with the column names
        df = pd.DataFrame(results, columns=column_names)
        print("df",df)

        cursor.close()

        return df

    except Exception as e:
        raise Exception(f"Error fetching data from {tableName}: {str(e)}")
    

