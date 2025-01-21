
import pandas as pd
from psycopg2 import sql
import psycopg2
from psycopg2.extras import RealDictCursor
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT

# def get_db_connection_view(database_name):
#     connection = psycopg2.connect(
#         dbname=database_name,  # Connect to the specified database
#         user='postgres',
#         password='jaTHU@12',
#         host='localhost',
#         port='5432'
#     )
#     return connection

def get_db_connection_view(database_name):
    connection = psycopg2.connect(
        dbname=database_name,  # Connect to the specified database
        user=USER_NAME,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )
    return connection


def fetch_chart_data(connection, tableName):
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
        cursor.close()

        return df

    except Exception as e:
        raise Exception(f"Error fetching data from {tableName}: {str(e)}")
    



# def fetch_ai_saved_chart_data(connection, tableName):
#     try:
#         cursor = connection.cursor()
#         # Use SQL composition to safely query using dynamic table and column names
#         query = sql.SQL("SELECT * FROM {table}")
#         query = query.format(
#             table=sql.Identifier(tableName)
#         )
#         cursor.execute(query)
#         results = cursor.fetchall()
#         # Fetch the column names from the cursor
#         column_names = [desc[0] for desc in cursor.description]
#         # Convert the results to a DataFrame with the column names
#         df = pd.DataFrame(results, columns=column_names)
#         # Ensure numeric columns are properly cleaned and converted
#         for column in df.select_dtypes(include=[object]).columns:
#             # Try to convert columns to numeric if possible
#             df[column] = pd.to_numeric(df[column], errors='ignore')
#         # Filter numeric and text columns
#         numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
#         text_columns = df.select_dtypes(include=[object]).columns.tolist()
#         print("Numeric columns:", numeric_columns)
#         print("Text columns:", text_columns)

#         cursor.close()
#         return df

#     except Exception as e:
#         raise Exception(f"Error fetching data from {tableName}: {str(e)}")


# from psycopg2 import sql

import json
# from psycopg2 import sql

def fetch_ai_saved_chart_data(connection, tableName, chart_id):
    try:
        cursor = connection.cursor()

        # Safely construct the query to fetch data from the dynamic table
        query = sql.SQL(
            "SELECT ai_chart_data FROM {table} WHERE id = %s"
        ).format(
            table=sql.Identifier(tableName)  # Safely handle dynamic table names
        )

        # Execute the query with the parameterized chart_id
        cursor.execute(query, (chart_id,))
        results = cursor.fetchall()

        # Process results: Deserialize JSON if stored as JSON
        chart_data = []
        for record in results:
            ai_chart_data = record[0]
            if isinstance(ai_chart_data, list):
                ai_chart_data = json.dumps(ai_chart_data)  # Convert list to JSON string
            chart_data.append(json.loads(ai_chart_data))

        return chart_data

    except Exception as e:
        # Use logging for better error tracking
        print("Error fetching AI chart data:", e)
        return None

    finally:
        # Ensure cursor is closed
        cursor.close()

# Example usage:
# df = fetch_ai_saved_chart_data(masterdatabasecon, tableName="new_dashboard_details_new", chart_id=chart_id)
# print("AI/ML chart details:", df)
# return jsonify({
#     "histogram_details": df,
# }), 200


# def fetch_ai_saved_chart_data(connection, tableName,chart_id):
#     try:
#         cursor = connection.cursor()

#         # Safely construct the query to fetch data from the dynamic table
#         query = sql.SQL("SELECT ai_chart_data FROM {table}").format(
#             table=sql.Identifier(tableName)  # Safely handle dynamic table names
#         )

#         cursor.execute(query)
#         results = cursor.fetchall()

#         # Return results or process them further if needed
#         return results

#     except Exception as e:
#         print("Error fetching AI chart data:", e)
#         return None

#     finally:
#         cursor.close()




def fetch_AI_chart_data(connection, tableName):
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
        # column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(results)
        cursor.close()
        return df
    except Exception as e:
        raise Exception(f"Error fetching data from {tableName}: {str(e)}")
    

# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate,clicked_catagory_Xaxis,category):
#     try:
#         # Connect to the PostgreSQL databases
#         connection = psycopg2.connect(
#             dbname=database_name,
#             user='postgres',
#             password='jaTHU@12',
#             host='localhost',
#             port='5432'
#         )
#         cursor = connection.cursor(cursor_factory=RealDictCursor)

#         X_Axis= x_axis
#         Y_Axis= y_axis

#         # Construct the SQL query
#         query = f"""
#         SELECT {X_Axis}, {aggregate}({Y_Axis}::numeric) AS {Y_Axis}
#         FROM {table_name}
#         WHERE {clicked_catagory_Xaxis} = '{category}'
#         GROUP BY {X_Axis};
#         """
#         print("Query:", query)
#         cursor.execute(query)
#         result = cursor.fetchall()

#         categories = [row[x_axis] for row in result]
#         values = [row[y_axis] for row in result]

#         return {"categories": categories, "values": values}

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()


# ====================================   ABOVE  IS THE REAL CODE ================================




# def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category):
#     try:
#         # Connect to the PostgreSQL database
#         connection = psycopg2.connect(
#             dbname=database_name,
#             user='postgres',
#             password='jaTHU@12',
#             host='localhost',
#             port='5432'
#         )
#         cursor = connection.cursor(cursor_factory=RealDictCursor)

#         X_Axis = x_axis
#         Y_Axis = y_axis.split(", ")  # Split the Y_Axis if it contains multiple columns
#         print("---------------------------------------------", Y_Axis)

#         aliases = ['series1', 'series2']  # Extend this list if there are more columns
#         if len(Y_Axis) > 1 and len(Y_Axis) == len(aliases):
#             # Construct the SQL query with custom aliases (series1, series2)
#             y_axis_aggregate = ", ".join([f"{aggregate}({col}::numeric) AS {alias}" for col, alias in zip(Y_Axis, aliases)])
#         else:
#             y_axis_aggregate = f"{aggregate}({Y_Axis[0]}::numeric) AS {Y_Axis[0]}"

#         query = f"""
#         SELECT {X_Axis}, {y_axis_aggregate}
#         FROM {table_name}
#         WHERE {clicked_category_Xaxis} = '{category}'
#         GROUP BY {X_Axis};
#         """
#         print("Query:", query)
#         cursor.execute(query)
#         result = cursor.fetchall()

#         categories = [row[x_axis] for row in result]

#         # Handle multiple Y-axis values in the result
#         if len(Y_Axis) > 1:
#             values = [{col: row[col] for col in Y_Axis} for row in result]
#         else:
#             values = [row[Y_Axis[0]] for row in result]

#         return {"categories": categories, "values": values}

#     except Exception as e:
#         print("Error fetching chart data:", e)
#         return None

#     finally:
#         if cursor:
#             cursor.close()
#         if connection:
#             connection.close()







def filter_chart_data(database_name, table_name, x_axis, y_axis, aggregate, clicked_category_Xaxis, category):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            dbname=database_name,
            # user='postgres',
            # password='jaTHU@12',
            # host='localhost',
            # port='5432'
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        cursor = connection.cursor(cursor_factory=RealDictCursor)

        X_Axis = x_axis
        Y_Axis = y_axis.split(", ")  # Split the Y_Axis if it contains multiple columns
        print("---------------------------------------------", Y_Axis)

        # Define aliases for each Y_Axis column
        aliases = ['series1', 'series2']  # Extend this list if there are more columns
        if len(Y_Axis) > 1 and len(Y_Axis) == len(aliases):
            # Construct the SQL query with custom aliases (series1, series2)
            y_axis_aggregate = ", ".join([f"{aggregate}({col}::numeric) AS {alias}" for col, alias in zip(Y_Axis, aliases)])
        else:
            y_axis_aggregate = f"{aggregate}({Y_Axis[0]}::numeric) AS series1"

        # Construct the SQL query
        query = f"""
        SELECT {X_Axis}, {y_axis_aggregate}
        FROM {table_name}
        WHERE {clicked_category_Xaxis} = '{category}'
        GROUP BY {X_Axis};
        """
        print("Query:", query)
        cursor.execute(query)
        result = cursor.fetchall()

        # Extract categories
        categories = [row[x_axis] for row in result]

        # Extract values for single Y-axis (series1)
        if len(Y_Axis) == 1:
            values = [row['series1'] for row in result]
            return {"categories": categories, "values": values}

        # Extract values for multiple Y-axes (series1, series2)
        series1 = [row['series1'] for row in result]
        series2 = [row['series2'] for row in result]
        
        return {"categories": categories, "series1": series1, "series2": series2}

    except Exception as e:
        print("Error fetching chart data:", e)
        return None

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
