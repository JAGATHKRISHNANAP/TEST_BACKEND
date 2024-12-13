import psycopg2
from psycopg2 import sql
from flask import jsonify, request
from config import DB_NAME,USER_NAME,PASSWORD,HOST,PORT

from histogram_utils import generate_histogram_details,handle_column_data_types


# # Function to create the database connection
# def create_connection():
#     try:
#         conn = psycopg2.connect(
#             dbname="datasource", 
#             user="postgres", 
#             password="jaTHU@12", 
#             host="localhost", 
#             port="5432"
#         )
#         return conn
#     except Exception as e:
#         print(f"Error creating connection to the database: {e}")
#         return None
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
        user_id VARCHAR(255), 
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






def get_dashboard_names(company_name_global):
    conn = create_connection()
    if conn:
        try:
            cursor = conn.cursor()
            query = ("SELECT file_name FROM dashboard_details_wu_id WHERE company_name = %s")
            cursor.execute(query, (company_name_global,))
            chart_names = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            print("chart_names", chart_names)
            return chart_names
        except psycopg2.Error as e:
            print("Error fetching chart names:", e)
            conn.close()
            return None
    else:
        return None
    


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
#                 cursor.execute("SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type, filter_options, chart_heading FROM new_dashboard_details_new WHERE id = %s", (chart_id,))
#                 chart_data = cursor.fetchone()
#                 cursor.close()

#                 if chart_data:
#                     # Extract chart data
#                     database_name = chart_data[1]  # Assuming `database_name` is the second field
#                     table_name = chart_data[2]
#                     x_axis = chart_data[3]
#                     text_x_axis = x_axis[0]
#                     y_axis = chart_data[4]
#                     aggregate = chart_data[5]
#                     chart_type = chart_data[6]
#                     chart_heading = chart_data[8]
#                     filter_options = chart_data[7]

#                     # Determine the aggregation function
#                     if aggregate == 'count':
#                         aggregate_py = 'count'
#                     elif aggregate == 'sum':
#                         aggregate_py = 'sum'
#                     elif aggregate == 'average':
#                         aggregate_py = 'mean'
#                     elif aggregate == 'minimum':
#                         aggregate_py = 'min'
#                     elif aggregate == 'maximum':
#                         aggregate_py = 'max'

#                     # Handle singleValueChart type separately
#                     if chart_type == "singleValueChart":
#                         single_value_result = fetchText_data(database_name, table_name, text_x_axis, aggregate_py)
#                         print("Single Value Result for Chart ID", chart_id, ":", single_value_result)

#                         # Append single value chart data
#                         chart_data_list.append({
#                             "chart_id": chart_id,
#                             "chart_type": chart_type,
#                             "chart_heading": chart_heading,
#                             "value": single_value_result  # Adjust based on the actual return structure of fetchText_data
#                         })
#                         continue  # Skip further processing for this chart ID
    
#                     # Proceed with category and value generation for non-singleValueChart types
#                     connection = get_db_connection_view(database_name)
#                     dataframe = fetch_chart_data(connection, table_name)
#                     print("Chart ID", chart_id)
#                     print("Chart Type", chart_type)
                    
#                     # Convert y_axis[0] if it is in time format (HH:MM:SS) to minutes, otherwise continue
#                     try:
#                         dataframe[y_axis[0]] = pd.to_datetime(dataframe[y_axis[0]], errors='raise', format='%H:%M:%S')
#                         # Convert time to minutes
#                         dataframe[y_axis[0]] = dataframe[y_axis[0]].dt.hour * 60 + dataframe[y_axis[0]].dt.minute + dataframe[y_axis[0]].dt.second / 60
#                         print("Converted Time to Minutes for y_axis[0]:", dataframe[y_axis[0]].head())
#                     except ValueError:
#                         # If not in time format, convert to numeric (handle non-time formats)
#                         dataframe[y_axis[0]] = pd.to_numeric(dataframe[y_axis[0]], errors='coerce')

#                     # Group data by x_axis and apply aggregation
#                     grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
#                     print("Grouped DataFrame: ", grouped_df.head())
                    
#                     categories = grouped_df[x_axis[0]].tolist()  # Assuming x_axis is a single column
#                     values = [float(value) for value in grouped_df[y_axis[0]]]  # Convert Decimal to float

#                     # Filter categories and values based on filter_options
#                     filtered_categories = []
#                     filtered_values = []
#                     for category, value in zip(categories, values):
#                         if category in filter_options:
#                             filtered_categories.append(category)
#                             filtered_values.append(value)

#                     print("Filtered Categories:", filtered_categories)
#                     print("Filtered Values:", filtered_values)
#                     chart_data_list.append({
#                         "categories": filtered_categories,
#                         "values": filtered_values,
#                         "chart_id": chart_id,
#                         "chart_type": chart_type
#                     })

#             conn.close()  # Close the main connection
#             return chart_data_list

#         except psycopg2.Error as e:
#             print("Error fetching chart data:", e)
#             conn.close()
#             return None
#     else:
#         return None

























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
#                     chart_color = chart_data[9] # Assuming chart_color is a list

#                     # Determine the aggregation function
#                     aggregate_py = {
#                         'count': 'count',
#                         'sum': 'sum',
#                         'average': 'mean',
#                         'minimum': 'min',
#                         'maximum': 'max'
#                     }.get(aggregate, 'sum')  # Default to 'sum' if no match
                    

#                     # Handle singleValueChart type separately
#                     if chart_type == "singleValueChart":
#                         single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate)
#                         print("Single Value Result for Chart ID", chart_id, ":", single_value_result)

#                         # Append single value chart data
#                         chart_data_list.append({
#                             "chart_id": chart_id,
#                             "chart_type": chart_type,
#                             "chart_heading": chart_heading,
#                             "value": single_value_result
#                         })
#                         continue  # Skip further processing for this chart ID

#                     # Proceed with category and value generation for non-singleValueChart types
#                     connection = get_db_connection_view(database_name)
#                     dataframe = fetch_chart_data(connection, table_name)
#                     print("Chart ID", chart_id)
#                     print("Chart Type", chart_type)

#                     # Convert y_axis values if required (either in time format or as numeric)
#                     for axis in y_axis:
#                         try:
#                             dataframe[axis] = pd.to_datetime(dataframe[axis], errors='raise', format='%H:%M:%S')
#                             dataframe[axis] = dataframe[axis].dt.hour * 60 + dataframe[axis].dt.minute + dataframe[axis].dt.second / 60
#                             print(f"Converted Time to Minutes for {axis}: ", dataframe[axis].head())
#                         except ValueError:
#                             dataframe[axis] = pd.to_numeric(dataframe[axis], errors='coerce')

#                     # Handle dual y_axis columns
#                     if len(y_axis) == 2:
#                         grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
#                         print("Grouped DataFrame (dual y-axis):", grouped_df.head())

#                         categories = grouped_df[x_axis[0]].tolist()
#                         values1 = [float(value) for value in grouped_df[y_axis[0]]]
#                         values2 = [float(value) for value in grouped_df[y_axis[1]]]

#                         # Filter categories and values based on filter_options
#                         filtered_categories = []
#                         filtered_values1 = []
#                         filtered_values2 = []
#                         for category, value1, value2 in zip(categories, values1, values2):
#                             if category in filter_options:
#                                 filtered_categories.append(category)
#                                 filtered_values1.append(value1)
#                                 filtered_values2.append(value2)

#                         print("Filtered Categories:", filtered_categories)
#                         print("Filtered Values (Series 1):", filtered_values1)
#                         print("Filtered Values (Series 2):", filtered_values2)

#                         chart_data_list.append({
#                             "categories": filtered_categories,
#                             "series1": filtered_values1,
#                             "series2": filtered_values2,
#                             "chart_id": chart_id,
#                             "chart_type": chart_type
#                         })

#                     if chart_type == "sampleAitestChart":
#                         try:
#                             # Fetch chart data
#                             df = fetch_chart_data(connection, table_name)
#                             print("Chart ID", chart_id)
#                             print("//////////",df.head(5))
                            
#                             # Handle column data types (conversion and cleaning)
#                             df, numeric_columns, text_columns = handle_column_data_types(df)

#                             # Generate histogram details
#                             histogram_details = generate_histogram_details(df)

#                             # Verify if global_df matches the current DataFrame
#                             # print("bc.global_df dtypes:\n", bc.global_df.dtypes)
#                             # print("df dtypes:\n", df.dtypes)
#                             # print("DataFrames are equal:", bc.global_df.equals(df))

#                             connection.close()

#                             # Return the histogram details as part of the response
#                             # return jsonify({
#                             #     "histogram_details": histogram_details,
#                             # }), 200
#                             chart_data_list.append({
#                              "histogram_details": histogram_details,  
#                              "chart_type": chart_type
#                         })
#                         except Exception as e:
#                             print("Error while processing chart:", e)
#                             return jsonify({"error": "An error occurred while generating the chart."}), 500




#                     elif chart_type == "treeHierarchy":
                        
#                         connection = get_db_connection_view(database_name)
#                         dataframe = fetch_TreeHierarchy_Data(connection, table_name)
#                         dataframe_dict = dataframe.to_dict(orient='records')
#                         chart_data_list.append({"dataframe_dict":dataframe_dict,
#                                                "x_axis":x_axis,
#                                                "chart_type": chart_type,})    
#                         # return jsonify({"data frame":dataframe_dict})

#                         print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
#                         # print("dataframe",dataframe)
#                         print(x_axis)
#                         print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

#             #         else:
                        
#             #             grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
#             #             print("Grouped DataFrame:", grouped_df.head())

#             #             categories = grouped_df[x_axis[0]].tolist()
#             #             values = [float(value) for value in grouped_df[y_axis[0]]]

#             #             # Filter categories and values based on filter_options
#             #             filtered_categories = []
#             #             filtered_values = []
#             #             for category, value in zip(categories, values):
#             #                 if category in filter_options:
#             #                     filtered_categories.append(category)
#             #                     filtered_values.append(value)

#             #             print("Filtered Categories:", filtered_categories)
#             #             print("Filtered Values:", filtered_values)

#             #             chart_data_list.append({
#             #                 "categories": filtered_categories,
#             #                 "values": filtered_values,
#             #                 "chart_id": chart_id,
#             #                 "chart_type": chart_type,
#             #                 "chart_color": chart_color,
#             #                 "x_axis": x_axis,
#             #                 "y_axis": y_axis,
#             #                 "aggregate": aggregate     
#             #             })

#             # conn.close()  # Close the main connection
#             # return chart_data_list

#             else:
#                 if aggregate_py == "count":
#                     grouped_df = dataframe.groupby(x_axis).size().reset_index(name="count")
#                     print("Grouped DataFrame (Count):", grouped_df.head())

#                     categories = grouped_df[x_axis[0]].tolist()
#                     values = grouped_df["count"].tolist()  # Use the "count" column for values
#                 else:
#                     grouped_df = dataframe.groupby(x_axis)[y_axis].agg(aggregate_py).reset_index()
#                     print("Grouped DataFrame:", grouped_df.head())

#                     categories = grouped_df[x_axis[0]].tolist()
#                     values = [float(value) for value in grouped_df[y_axis[0]]]

#                 # Filter categories and values based on filter_options
#                 filtered_categories = []
#                 filtered_values = []
#                 for category, value in zip(categories, values):
#                     if category in filter_options:
#                         filtered_categories.append(category)
#                         filtered_values.append(value)

#                 print("Filtered Categories:", filtered_categories)
#                 print("Filtered Values:", filtered_values)

#                 chart_data_list.append({
#                     "categories": filtered_categories,
#                     "values": filtered_values,
#                     "chart_id": chart_id,
#                     "chart_type": chart_type,
#                     "chart_color": chart_color,
#                     "x_axis": x_axis,
#                     "y_axis": y_axis,
#                     "aggregate": aggregate     
#                 })

#             conn.close()  # Close the main connection
#             return chart_data_list


#         except psycopg2.Error as e:
#             print("Error fetching chart data:", e)
#             conn.close()
#             return None
#     else:
#         return None











def get_dashboard_view_chart_data(chart_ids):
    conn = create_connection()  # Initial connection to your main database
    if conn:
        try:
            if isinstance(chart_ids, str):
                import ast
                chart_ids = ast.literal_eval(chart_ids)  # Convert string representation of list to actual list

            chart_data_list = []

            for chart_id in chart_ids:
                cursor = conn.cursor()
                cursor.execute("SELECT id, database_name, selected_table, x_axis, y_axis, aggregate, chart_type, filter_options, chart_heading,chart_color FROM new_dashboard_details_new WHERE id = %s", (chart_id,))
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

                    # Determine the aggregation function
                    aggregate_py = {
                        'count': 'count',
                        'sum': 'sum',
                        'average': 'mean',
                        'minimum': 'min',
                        'maximum': 'max'
                    }.get(aggregate, 'sum')  # Default to 'sum' if no match

                    # Handle singleValueChart type separately
                    if chart_type == "singleValueChart":
                        single_value_result = fetchText_data(database_name, table_name, x_axis[0], aggregate)
                        print("Single Value Result for Chart ID", chart_id, ":", single_value_result)

                        # Append single value chart data
                        chart_data_list.append({
                            "chart_id": chart_id,
                            "chart_type": chart_type,
                            "chart_heading": chart_heading,
                            "value": single_value_result
                        })
                        continue  # Skip further processing for this chart ID

                    # Proceed with category and value generation for non-singleValueChart types
                    connection = get_db_connection_view(database_name)
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
                        grouped_df = dataframe.groupby(x_axis).size().reset_index(name="count")
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
                            "aggregate": aggregate
                        })
                        continue  # Skip further processing for this chart ID

                    # Handle dual y_axis columns
                    if len(y_axis) == 2:
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
                            "x_axis": x_axis,
                            "y_axis": y_axis,
                            "aggregate": aggregate
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
                            "aggregate": aggregate
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
    
