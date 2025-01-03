
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
        print('connection',connection)
        cursor = connection.cursor()
        print('curs',cursor)
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

        # Convert the results to a DataFrame with the column names
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
