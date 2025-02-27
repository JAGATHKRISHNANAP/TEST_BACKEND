import re
import psycopg2
import pandas as pd
from config import USER_NAME, DB_NAME, PASSWORD, HOST, PORT

global_df = None  # Ensure global_df is initialized to None

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



# def get_column_names(db_name, username, password, table_name, host='localhost', port='5432'):
#     global global_df
#     oldtablename = getattr(get_column_names, 'oldtablename', None)
    
#     if oldtablename == table_name and global_df is not None:
#         print("Using cached data from global_df")
        
#         numeric_columns = global_df.select_dtypes(include=[float, int]).columns.tolist()
#         text_columns = global_df.select_dtypes(include=[object]).columns.tolist()

#         numeric_columns_cleaned = {}
#         text_columns_cleaned = {}

#         for column_name in numeric_columns:
#             cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
#             numeric_columns_cleaned[column_name] = cleaned_values
#             num_columns = list(numeric_columns_cleaned.keys())

#         for column_name in text_columns:
#             cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
#             text_columns_cleaned[column_name] = cleaned_values
#             txt_columns = list(text_columns_cleaned.keys())

#         return {
#             'numeric_columns': num_columns,
#             'text_columns': txt_columns
#         }
    
#     try:
#         conn = psycopg2.connect(
#             dbname=db_name,
#             user=username,
#             password=password,
#             host=host,
#             port=port
#         )
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
#         column_names = [desc[0] for desc in cursor.description]

#         cursor.execute(f"SELECT * FROM {table_name}")
#         data = cursor.fetchall()
#         df = pd.DataFrame(data, columns=column_names)
#         global_df = df
#         get_column_names.oldtablename = table_name  # Update the oldtablename to the current table_name
#         print("============================database data frame============================")
#         print(global_df.head(5))
#         print("========================================================")

#         print("All column names in the dataframe:")
#         print(df.columns.tolist())

#         for column in df.columns:
#             df[column] = pd.to_numeric(df[column], errors='ignore')

#         numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
#         text_columns = df.select_dtypes(include=[object]).columns.tolist()

#         numeric_columns_cleaned = {}
#         text_columns_cleaned = {}

#         for column_name in numeric_columns:
#             cleaned_values = df[column_name].apply(remove_symbols).tolist()
#             numeric_columns_cleaned[column_name] = cleaned_values
#             num_columns = list(numeric_columns_cleaned.keys())

#         for column_name in text_columns:
#             cleaned_values = df[column_name].apply(remove_symbols).tolist()
#             text_columns_cleaned[column_name] = cleaned_values
#             txt_columns = list(text_columns_cleaned.keys())

#         cursor.close()
#         conn.close()

#         return {
#             'numeric_columns': num_columns,
#             'text_columns': txt_columns
#             # 'dataframe': global_df,
#         }
#     except psycopg2.Error as e:
#         print("Error: Unable to connect to the database.")
#         print(e)
#         return {'numeric_columns': [], 'text_columns': []}
# def get_column_names(db_name, username, password, table_name, host='localhost', port='5432'):
#     global global_df
#     oldtablename = getattr(get_column_names, 'oldtablename', None)
    
#     if oldtablename == table_name and global_df is not None:
#         print("Using cached data from global_df")
        
#         numeric_columns = global_df.select_dtypes(include=[float, int]).columns.tolist()
#         text_columns = global_df.select_dtypes(include=[object]).columns.tolist()

#         numeric_columns_cleaned = {}
#         text_columns_cleaned = {}

#         num_columns = []  # Initialize with a default value
#         txt_columns = []  # Initialize with a default value

#         for column_name in numeric_columns:
#             cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
#             numeric_columns_cleaned[column_name] = cleaned_values
#             num_columns = list(numeric_columns_cleaned.keys())
#             print("numeric columns", num_columns)

#         for column_name in text_columns:
#             cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
#             text_columns_cleaned[column_name] = cleaned_values
#             txt_columns = list(text_columns_cleaned.keys())

#             print("text columns", txt_columns)

#         return {
#             'numeric_columns': num_columns,
#             'text_columns': txt_columns
#         }
    
#     try:
#         conn = psycopg2.connect(
#             dbname=db_name,
#             user=username,
#             password=password,
#             host=host,
#             port=port
#         )
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
#         column_names = [desc[0] for desc in cursor.description]

#         cursor.execute(f"SELECT * FROM {table_name}")
#         data = cursor.fetchall()
#         df = pd.DataFrame(data, columns=column_names)
#         global_df = df
#         get_column_names.oldtablename = table_name  # Update the oldtablename to the current table_name
#         print("============================database data frame============================")
#         print(global_df.head(5))
#         print("========================================================")

#         print("All column names in the dataframe:")
#         print(df.columns.tolist())

#         for column in df.columns:
#             df[column] = pd.to_numeric(df[column], errors='ignore')

#         numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
#         text_columns = df.select_dtypes(include=[object]).columns.tolist()

#         numeric_columns_cleaned = {}
#         text_columns_cleaned = {}

#         num_columns = []  # Initialize with a default value
#         txt_columns = []  # Initialize with a default value

#         for column_name in numeric_columns:
#             cleaned_values = df[column_name].apply(remove_symbols).tolist()
#             numeric_columns_cleaned[column_name] = cleaned_values
#             num_columns = list(numeric_columns_cleaned.keys())
#             print("numeric columns", num_columns)

#         for column_name in text_columns:
#             cleaned_values = df[column_name].apply(remove_symbols).tolist()
#             text_columns_cleaned[column_name] = cleaned_values
#             txt_columns = list(text_columns_cleaned.keys())
#             print("text columns", txt_columns)

#         cursor.close()
#         conn.close()

#         return {
#             'numeric_columns': num_columns,
#             'text_columns': txt_columns
#         }
#     except psycopg2.Error as e:
#         print("Error: Unable to connect to the database.")
#         print(e)
#         return {'numeric_columns': [], 'text_columns': []}
def get_column_names(db_name, username, password, table_name,selectedUser, host='localhost', port='5432', connection_type='local',force_refresh=True):
    global global_df
    oldtablename = getattr(get_column_names, 'oldtablename', None)
    print('connectontype',connection_type)
    # if oldtablename == table_name and global_df is not None:
    #     print("Using cached data from global_df")
    if force_refresh or oldtablename != table_name:
        print("Refreshing data cache...")
        global_df = None
        get_column_names.oldtablename = None

    if global_df is not None:
        
        numeric_columns = global_df.select_dtypes(include=[float, int]).columns.tolist()
        text_columns = global_df.select_dtypes(include=[object]).columns.tolist()

        numeric_columns_cleaned = {}
        text_columns_cleaned = {}

        num_columns = []  # Initialize with a default value
        txt_columns = []  # Initialize with a default value

        for column_name in numeric_columns:
            cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            num_columns = list(numeric_columns_cleaned.keys())
            print("numeric columns", num_columns)

        for column_name in text_columns:
            cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            txt_columns = list(text_columns_cleaned.keys())

            print("text columns", txt_columns)

        return {
            'numeric_columns': num_columns,
            'text_columns': txt_columns
        }
    
    try:
        # Connect to the appropriate database based on connection_type
        if connection_type == 'local':
            conn = psycopg2.connect(
                dbname=db_name,
                user=username,
                password=password,
                host=host,
                port=port
            )
        else:  # External database connection
            connection_details = fetch_external_db_connection(db_name,selectedUser)
            if connection_details:
                db_details = {
                    "host": connection_details[3],
                    "database": connection_details[7],
                    "user": connection_details[4],
                    "password": connection_details[5],
                    "port": int(connection_details[6])

                }
            if not connection_details:
                raise Exception("Unable to fetch external database connection details.")
            
            database, user, password, host,port = db_details
            print("db_details",db_details)
            conn = psycopg2.connect(
    dbname=db_details['database'],
    user=db_details['user'],
    password=db_details['password'],
    host=db_details['host'],
    port=db_details['port']  # Ensure port is an integer here
)

        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
        column_names = [desc[0] for desc in cursor.description]

        cursor.execute(f"SELECT * FROM {table_name} ")
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=column_names)
        global_df = df
        get_column_names.oldtablename = table_name  # Update the oldtablename to the current table_name
        print("============================database data frame============================")
        print(global_df.head(5))
        print("========================================================")

        print("All column names in the dataframe:")
        print(df.columns.tolist())

        for column in df.columns:
            df[column] = pd.to_numeric(df[column], errors='ignore')

        numeric_columns = df.select_dtypes(include=[float, int]).columns.tolist()
        text_columns = df.select_dtypes(include=[object]).columns.tolist()

        numeric_columns_cleaned = {}
        text_columns_cleaned = {}

        num_columns = []  # Initialize with a default value
        txt_columns = []  # Initialize with a default value

        for column_name in numeric_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            num_columns = list(numeric_columns_cleaned.keys())
            print("numeric columns", num_columns)

        for column_name in text_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            txt_columns = list(text_columns_cleaned.keys())
            print("text columns", txt_columns)

        cursor.close()
        conn.close()

        return {
            'numeric_columns': num_columns,
            'text_columns': txt_columns
        }
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return {'numeric_columns': [], 'text_columns': []}

def fetch_external_db_connection(db_name,selectedUser):
    try:
        print("company_name", db_name)
        print("selectedUser",selectedUser)
        # Connect to local PostgreSQL to get external database connection details
        conn = psycopg2.connect(
            dbname=db_name,  # Ensure this is the correct company database
            user=USER_NAME,
            password=PASSWORD,
            host=HOST,
            port=PORT
        )
        print("conn", conn)
        cursor = conn.cursor()
        query = """
            SELECT * 
            FROM external_db_connections 
            WHERE savename = %s 
            ORDER BY created_at DESC 
            LIMIT 1;
        """
        print("query",query)
        cursor.execute(query, (selectedUser,))
        connection_details = cursor.fetchone()
        print('connection',connection_details)
        conn.close()
        return connection_details
    except Exception as e:
        print(f"Error fetching connection details: {e}")
        return None



# def edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
#     global global_df

#     if global_df is None:
#         print("Fetching data from the database...")
#         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#         cur = conn.cursor()
#         query = f"SELECT * FROM {table_name}"
#         cur.execute(query)
#         data = cur.fetchall()
#         colnames = [desc[0] for desc in cur.description]
#         cur.close()
#         conn.close()

#         global_df = pd.DataFrame(data, columns=colnames)
#         print("Full DataFrame:")
#         print(global_df[y_axis_column[0]])

#         global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
#         print(f"global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')")

#     else:
#         global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

#     if aggregation == "sum":
#         aggregation_func = "sum"
#     elif aggregation == "average":
#         aggregation_func = "mean"
#     elif aggregation == "count":
#         aggregation_func = "count"
#     elif aggregation == "maximum":
#         aggregation_func = "max"
#     elif aggregation == "minimum":
#         aggregation_func = "min"
        
#     x_axis_columns_str = x_axis_columns
#     # print("The x axis columns are:", x_axis_columns_str)
#     options = [option.strip() for option in checked_option.split(',')]
#     # print("The options are:", options)
#     filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
#     grouped_df = filtered_df.groupby(x_axis_columns_str[0]).agg({y_axis_column[0]: aggregation_func}).reset_index()
    
#     result = [tuple(x) for x in grouped_df.to_numpy()]
    
#     return result
# def edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name, selectedUser):
#     global_df =None

#     if global_df is None:
#         print("Fetching data from the database...")
#         try:
#             # Establish database connection
#             if not selectedUser or selectedUser.lower() == 'null':
#                 print("Using default database connection...")
#                 connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
#                 conn = psycopg2.connect(connection_string)
#             else:
#                 print(f"Using connection for user: {selectedUser}")
#                 connection_string = fetch_external_db_connection(db_name,selectedUser)
#                 if not connection_string:
#                     raise Exception("Unable to fetch external database connection details.")

#                 db_details = {
#                     "host": connection_string[3],
#                     "database": connection_string[7],
#                     "user": connection_string[4],
#                     "password": connection_string[5],
#                     "port": int(connection_string[6])
#                 }

#                 conn = psycopg2.connect(
#                     dbname=db_details['database'],
#                     user=db_details['user'],
#                     password=db_details['password'],
#                     host=db_details['host'],
#                     port=db_details['port']
#                 )

#             cur = conn.cursor()
#             query = f"SELECT * FROM {table_name}"
#             cur.execute(query)
#             data = cur.fetchall()
#             colnames = [desc[0] for desc in cur.description]

#             # Create a pandas DataFrame from the fetched data
#             global_df = pd.DataFrame(data, columns=colnames)

#             print("Full DataFrame:")
#             print(global_df)

#             # Ensure the y-axis column is numeric
#             if y_axis_column[0] in global_df.columns:
#                 global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
#                 print(f"Converted {y_axis_column[0]} to numeric values.")
#             else:
#                 raise KeyError(f"Column '{y_axis_column[0]}' not found in the table.")

#         except Exception as e:
#             print(f"Error while fetching data from the database: {e}")
#             return None
#         finally:
#             if 'cur' in locals() and cur:
#                 cur.close()
#             if 'conn' in locals() and conn:
#                 conn.close()
#     else:
#         # Ensure the y-axis column is numeric in the existing DataFrame
#         if y_axis_column[0] in global_df.columns:
#             global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
#         else:
#             print(f"Column '{y_axis_column[0]}' not found in the global DataFrame.")
#             return None

#     try:
#         # Define the aggregation function
#         aggregation_func_map = {
#             "sum": "sum",
#             "average": "mean",
#             "count": "count",
#             "maximum": "max",
#             "minimum": "min"
#         }
#         aggregation_func = aggregation_func_map.get(aggregation.lower())
#         if not aggregation_func:
#             raise ValueError(f"Invalid aggregation type: {aggregation}")

#         # Validate x-axis columns and options
#         if not x_axis_columns or not y_axis_column:
#             raise ValueError("x_axis_columns and y_axis_column must not be empty.")

#         if x_axis_columns[0] not in global_df.columns:
#             raise KeyError(f"Column '{x_axis_columns[0]}' not found in the DataFrame.")

#         # Filter and group the DataFrame
#         options = [option.strip() for option in checked_option.split(',')]
#         filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
#         grouped_df = filtered_df.groupby(x_axis_columns[0]).agg({y_axis_column[0]: aggregation_func}).reset_index()

#         # Convert the grouped DataFrame to a list of tuples
#         result = [tuple(x) for x in grouped_df.to_numpy()]
#         return result

#     except Exception as e:
#         print(f"Error during data processing: {e}")
#         return No
def edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name, selectedUser):
    global_df = None

    if global_df is None:
        print("Fetching data from the database...")
        try:
            # Establish database connection
            if not selectedUser or str(selectedUser).lower() == 'null':
                print("Using default database connection...")
                connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
                conn = psycopg2.connect(connection_string)
            else:
                print(f"Using connection for user: {selectedUser}")
                connection_string = fetch_external_db_connection(db_name, selectedUser)
                if not connection_string:
                    raise Exception("Unable to fetch external database connection details.")

                db_details = {
                    "host": connection_string[3],
                    "database": connection_string[7],
                    "user": connection_string[4],
                    "password": connection_string[5],
                    "port": int(connection_string[6])
                }

                conn = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port']
                )

            cur = conn.cursor()
            query = f"SELECT {', '.join([x_axis_columns[0], y_axis_column[0]])} FROM {table_name}"
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

            # Create a pandas DataFrame from the fetched data
            global_df = pd.DataFrame(data, columns=colnames)

            print("Full DataFrame:")
            print(global_df)

            # Ensure the y-axis column is numeric
            y_axis = y_axis_column[0] if isinstance(y_axis_column, list) else y_axis_column
            if y_axis in global_df.columns:
                global_df[y_axis] = pd.to_numeric(global_df[y_axis], errors='coerce')
                print(f"Converted {y_axis} to numeric values.")
            else:
                raise KeyError(f"Column '{y_axis}' not found in the table.")

        except Exception as e:
            print(f"Error while fetching data from the database: {e}")
            return None
        finally:
            if 'cur' in locals() and cur:
                cur.close()
            if 'conn' in locals() and conn:
                conn.close()

    try:
        # Define the aggregation function
        aggregation_func_map = {
            "sum": "sum",
            "average": "mean",
            "count": "count",
            "maximum": "max",
            "minimum": "min"
        }

        if aggregation.lower() not in aggregation_func_map:
            raise ValueError(f"Invalid aggregation type: {aggregation}")

        aggregation_func = aggregation_func_map[aggregation.lower()]

        # Validate x-axis columns and options
        if not x_axis_columns or not y_axis_column:
            raise ValueError("x_axis_columns and y_axis_column must not be empty.")

        if x_axis_columns[0] not in global_df.columns:
            raise KeyError(f"Column '{x_axis_columns[0]}' not found in the DataFrame.")

        # Filter and group the DataFrame
        options = checked_option.get(x_axis_columns[0], [])
        filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
        grouped_df = filtered_df.groupby(x_axis_columns[0]).agg({y_axis: aggregation_func}).reset_index()

        # Convert the grouped DataFrame to a list of tuples
        result = [tuple(x) for x in grouped_df.to_numpy()]
        return result

    except Exception as e:
        print(f"Error during data processing: {e}")
        return None



import psycopg2
import pandas as pd


def count_function(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
    global global_df

    if global_df is None:
        print("Fetching data from the database...")
        conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
        cur = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        global_df = pd.DataFrame(data, columns=colnames)
        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    else:
        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    x_axis_columns_str = x_axis_columns
    options = [option.strip() for option in checked_option.split(',')]
    filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
    
    # Perform aggregation based on the selected aggregation type
    if aggregation == "sum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].sum().reset_index()
    elif aggregation == "average":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].mean().reset_index()
    elif aggregation == "count":
        # Count without considering decimal values
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].apply(lambda x: x.dropna().astype(int).count()).reset_index()
        print("grouped_df:", grouped_df)
    elif aggregation == "maximum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].max().reset_index()
    elif aggregation == "minimum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].min().reset_index()
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    # Convert the result to a list of tuples for easy output
    result = [tuple(x) for x in grouped_df.to_numpy()]
    
    return result


# def fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
#     global global_df
#     print("table_name:", table_name)
#     print("x_axis_columns:", x_axis_columns)
#     # print("checked_option:", checked_option)
#     print("y_axis_column:", y_axis_column)
#     print("aggregation:", aggregation)

#     if global_df is None:
#         print("Fetching data from the database...")
#         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#         cur = conn.cursor()
#         query = f"SELECT * FROM {table_name}"
#         cur.execute(query)
#         data = cur.fetchall()
#         colnames = [desc[0] for desc in cur.description]
#         cur.close()
#         conn.close()

#         global_df = pd.DataFrame(data, columns=colnames)
#         print("*********************************************************************************",global_df)
#         # global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

#     # else:
#     #     global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

#     x_axis_columns_str = x_axis_columns
#     options = [option.strip() for option in checked_option.split(',')]
#     filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
#     print("filtered_df:", filtered_df)  
    
#     # Perform aggregation based on the selected aggregation type
#     if aggregation == "sum":
#         grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].sum().reset_index()
#     elif aggregation == "average":
#         grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].mean().reset_index()
#     elif aggregation == "count":

#         # Check initial data
#         print("Filtered DataFrame shape:", filtered_df.shape)
#         print("Null count in y_axis_column[0]:", filtered_df[y_axis_column[0]].isnull().sum())

#         grouped_df = filtered_df.groupby(x_axis_columns_str[0]).size().reset_index(name="count")

        
#     elif aggregation == "maximum":
#         grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].max().reset_index()
#     elif aggregation == "minimum":
#         grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].min().reset_index()
#     else:
#         raise ValueError(f"Unsupported aggregation type: {aggregation}")

#     # Convert the result to a list of tuples for easy output
#     result = [tuple(x) for x in grouped_df.to_numpy()]
#     print("result:", result)
#     return result



def fetch_data(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name,selectedUser):
    global global_df
    print("table_name:", table_name)
    print("x_axis_columns:", x_axis_columns)
    print("y_axis_column:", y_axis_column)
    print("aggregation:", aggregation)
    print("aggregation:", filter_options)
    if global_df is None:
        print("Fetching data from the database...")
        if not selectedUser or selectedUser.lower() == 'null':
                print("Using default database connection...")
                connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
                connection = psycopg2.connect(connection_string)
        else:  # External connection
                connection_details = fetch_external_db_connection(db_name,selectedUser)
                if connection_details:
                    db_details = {
                        "host": connection_details[3],
                        "database": connection_details[7],
                        "user": connection_details[4],
                        "password": connection_details[5],
                        "port": int(connection_details[6])
                    }
                if not connection_details:
                    raise Exception("Unable to fetch external database connection details.")
                
                connection = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port'],
                )
        cur = connection.cursor()
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        connection.close()

        global_df = pd.DataFrame(data, columns=colnames)
        print("*********************************************************************************", global_df)

    # Create a copy of the necessary data for processing
    temp_df = global_df.copy()
    if isinstance(filter_options, str):
        filter_options = json.loads(filter_options)
    for col, filters in filter_options.items():
        if col in temp_df.columns:
            temp_df[col] = temp_df[col].astype(str)
            temp_df = temp_df[temp_df[col].isin(filters)]


    # Convert the x_axis_columns values to strings in the temporary DataFrame
    for col in x_axis_columns:
        if col in temp_df.columns:
            temp_df[col] = temp_df[col].astype(str)

    x_axis_columns_str = x_axis_columns
    # options = [option.strip() for option in filter_options.split(',')]
     # Use filter_options directly, not checked_option
    options = []
    for col in x_axis_columns: #added loop here
        if col in filter_options:
            options.extend(filter_options[col])
   

    # Convert options to strings for comparison
    options = list(map(str, options))

    # Filter the DataFrame based on the x_axis_columns values
    filtered_df = temp_df[temp_df[x_axis_columns[0]].isin(options)]
    print("filtered_df:", filtered_df)  
    
    # Perform aggregation based on the selected aggregation type
    if aggregation == "sum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].sum().reset_index()
    elif aggregation == "average":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].mean().reset_index()
    elif aggregation == "count":
        # Check initial data
        print("Filtered DataFrame shape:", filtered_df.shape)
        print("Null count in y_axis_column[0]:", filtered_df[y_axis_column[0]].isnull().sum())

        grouped_df = filtered_df.groupby(x_axis_columns_str[0]).size().reset_index(name="count")
    elif aggregation == "maximum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].max().reset_index()
    elif aggregation == "minimum":
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].min().reset_index()
    elif aggregation == "variance":
        print("Grouped DataFrame for Variance Calculation:")
        print(filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].var())
        grouped_df = filtered_df.groupby(x_axis_columns_str[0])[y_axis_column[0]].var().reset_index()
        print("grouped_df",grouped_df)
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    # Convert the result to a list of tuples for easy output
    result = [tuple(x) for x in grouped_df.to_numpy()]
    print("result:", result)
    return result
# import json
# def fetch_data_tree(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser):
#     global global_df
#     print("table_name:", table_name)
#     print("x_axis_columns:", x_axis_columns)
#     print("y_axis_column:", y_axis_column)
#     print("aggregation:", aggregation)
#     print("filter_options:", filter_options)

#     try:
#         if isinstance(filter_options, str):
#             try:
#                 filter_options = json.loads(filter_options)
#             except json.JSONDecodeError:
#                 raise ValueError("filter_options must be a valid JSON object")

#         if not isinstance(filter_options, dict):
#             raise ValueError("filter_options should be a dictionary")
#         if global_df is None:
#             print("Fetching data from the database...")
#             if not selectedUser or selectedUser.lower() == 'null':
#                 print("Using default database connection...")
#                 connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
#                 connection = psycopg2.connect(connection_string)
#             else:  # External connection
#                 connection_details = fetch_external_db_connection(db_name, selectedUser)
#                 if not connection_details:
#                     raise Exception("Unable to fetch external database connection details.")

#                 db_details = {
#                     "host": connection_details[3],
#                     "database": connection_details[7],
#                     "user": connection_details[4],
#                     "password": connection_details[5],
#                     "port": int(connection_details[6])
#                 }
#                 connection = psycopg2.connect(
#                     dbname=db_details['database'],
#                     user=db_details['user'],
#                     password=db_details['password'],
#                     host=db_details['host'],
#                     port=db_details['port'],
#                 )

#             cur = connection.cursor()
#             query = f"SELECT * FROM {table_name}"
#             cur.execute(query)
#             data = cur.fetchall()
#             colnames = [desc[0] for desc in cur.description]
#             cur.close()
#             connection.close()

#             global_df = pd.DataFrame(data, columns=colnames)
#             print("*********************************************************************************", global_df)

#         # Create a copy of the necessary data for processing
#         temp_df = global_df.copy()

#         # Apply filters
#         for col, filters in filter_options.items():
#             if col in temp_df.columns:
#                 temp_df[col] = temp_df[col].astype(str)
#                 temp_df = temp_df[temp_df[col].isin(filters)]

#         # Convert x_axis_columns values to strings
#         for col in x_axis_columns:
#             if col in temp_df.columns:
#                 temp_df[col] = temp_df[col].astype(str)

#         # Prepare options for filtering
#         options = []
#         for col in x_axis_columns:
#             if col in filter_options:
#                 options.extend(filter_options[col])
#         options = list(map(str, options))

#         # Filter DataFrame
#         filtered_df = temp_df[temp_df[x_axis_columns[0]].isin(options)]
#         print("filtered_df:", filtered_df)
        
#         # Prepare categories and values for response
#         categories = []
#         values = []

#         for index, row in filtered_df.iterrows():
#             category = {col: row[col] for col in x_axis_columns}  # Use x_axis_columns directly
#             categories.append(category)

#             if y_axis_column:  # Check if y_axis_column is not None or empty
#                 values.append(row[y_axis_column[0]])  # Use y_axis_column[0] directly
#             else:
#                 values.append(1)  # If no y-axis is given, count occurrences

#         result = {
#             "categories": categories,
#             "values": values,
#             "chartType": "treeHierarchy",
#             "dataframe": filtered_df.to_dict(orient='records')  # Send the DataFrame as JSON
#         }

#         print("result:", result)
#         return result

#     except Exception as e:
#         print("Error preparing Tree Hierarchy data:", e)
#         return {"error": str(e)}


import json
import pandas as pd
import psycopg2

def fetch_data_tree(table_name, x_axis_columns, filter_options, y_axis_column, aggregation, db_name, selectedUser):
    global global_df
    print("table_name:", table_name)
    print("x_axis_columns:", x_axis_columns)
    print("y_axis_column:", y_axis_column)
    print("aggregation:", aggregation)
    print("filter_options:", filter_options)

    try:
        if isinstance(filter_options, str):
            try:
                filter_options = json.loads(filter_options)
            except json.JSONDecodeError:
                raise ValueError("filter_options must be a valid JSON object")

        if not isinstance(filter_options, dict):
            raise ValueError("filter_options should be a dictionary")

        if global_df is None:
            print("Fetching data from the database...")
            if not selectedUser or selectedUser.lower() == 'null':
                print("Using default database connection...")
                connection_string = f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}"
                connection = psycopg2.connect(connection_string)
            else:
                connection_details = fetch_external_db_connection(db_name, selectedUser)
                if not connection_details:
                    raise Exception("Unable to fetch external database connection details.")

                db_details = {
                    "host": connection_details[3],
                    "database": connection_details[7],
                    "user": connection_details[4],
                    "password": connection_details[5],
                    "port": int(connection_details[6])
                }
                connection = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port'],
                )

            cur = connection.cursor()
            query = f"SELECT * FROM {table_name}"
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            connection.close()

            global_df = pd.DataFrame(data, columns=colnames)
            print("*********************************************************************************", global_df)

        # Create a copy of the necessary data for processing
        temp_df = global_df.copy()

        # Apply filters
        for col, filters in filter_options.items():
            if col in temp_df.columns:
                temp_df[col] = temp_df[col].astype(str)
                temp_df = temp_df[temp_df[col].isin(filters)]

        # Convert x_axis_columns values to strings
        for col in x_axis_columns:
            if col in temp_df.columns:
                temp_df[col] = temp_df[col].astype(str)

        # Prepare options for filtering
        options = []
        for col in x_axis_columns:
            if col in filter_options:
                options.extend(filter_options[col])
        options = list(map(str, options))

        # Filter DataFrame
        filtered_df = temp_df[temp_df[x_axis_columns[0]].isin(options)]

        # **Apply Aggregation**
        if y_axis_column and aggregation and y_axis_column[0] in filtered_df.columns:
            if aggregation.lower() == "sum":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].sum()
            elif aggregation.lower() == "avg":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].mean()
            elif aggregation.lower() == "min":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].min()
            elif aggregation.lower() == "max":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].max()
            elif aggregation.lower() == "count":
                filtered_df = filtered_df.groupby(x_axis_columns, as_index=False)[y_axis_column[0]].count()
            else:
                raise ValueError("Unsupported aggregation type. Use 'sum', 'avg', 'min', 'max', or 'count'.")

        print("filtered_df:", filtered_df)

        # Prepare categories and values for response
        categories = []
        values = []

        for index, row in filtered_df.iterrows():
            category = {col: row[col] for col in x_axis_columns}
            categories.append(category)

            if y_axis_column:
                values.append(row[y_axis_column[0]])
            else:
                values.append(1)

        result = {
            "categories": categories,
            "values": values,
            "chartType": "treeHierarchy",
            "dataframe": filtered_df.to_dict(orient='records')
        }

        print("result:", result)
        return result

    except Exception as e:
        print("Error preparing Tree Hierarchy data:", e)
        return {"error": str(e)}


def drill_down(clicked_category, x_axis_columns, y_axis_column, aggregation):
    global global_df   
    if global_df is None:
        return []
    print(global_df)
    # print("-----------------------------------------------------------------------------------------------------------------------------------------------------",y_axis_column[0])
    global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
    if aggregation == "SUM":
        aggregation_func = "sum"
    elif aggregation == "AVG":
        aggregation_func = "mean"
    elif aggregation == "COUNT":
        aggregation_func = "count"
    elif aggregation == "MAX":
        aggregation_func = "max"
    elif aggregation == "MIN":
        aggregation_func = "min"
    else:
        print("Invalid aggregation type.")
        return []
    filtered_df = global_df[global_df[x_axis_columns[0]] == clicked_category]
    if len(x_axis_columns) > 1:
        target_column = x_axis_columns[1]
    else:
        print("Not enough columns in x_axis_columns for drill down.")
        return []

    grouped_df = filtered_df.groupby(target_column).agg({y_axis_column[0]: aggregation_func}).reset_index()
    result = [tuple(x) for x in grouped_df.to_numpy()]
    
    return result




def fetch_data_for_duel(table_name, x_axis_columns,filter_options, y_axis_columns,aggregation,db_nameeee,selectedUser):
    # conn = psycopg2.connect(f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}")
    # cur = conn.cursor()
    # if selectedUser == 'null':
    #     conn = psycopg2.connect(f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}")
    print("data====================",table_name, x_axis_columns,filter_options, y_axis_columns,aggregation,db_nameeee,selectedUser)
    print("duelbar====================",filter_options)
    if not selectedUser or selectedUser.lower() == 'null':
                print("Using default database connection...")
                connection_string = f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}"
                conn = psycopg2.connect(connection_string)
    else:  # External connection
        connection_details = fetch_external_db_connection(db_nameeee,selectedUser)
        if connection_details:
            db_details = {
                "host": connection_details[3],
                "database": connection_details[7],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6])
            }
        if not connection_details:
            raise Exception("Unable to fetch external database connection details.")
        
        conn = psycopg2.connect(
            dbname=db_details['database'],
            user=db_details['user'],
            password=db_details['password'],
            host=db_details['host'],
            port=db_details['port'],
        )
    
    cur = conn.cursor()
    query = f"SELECT * FROM {table_name}"
    cur.execute(query)
    data = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    

    global_df = pd.DataFrame(data, columns=colnames)
    print("*********************************************************************************", global_df)

    filter_clause = ""
    if filter_options:  # Check if filter_options is not empty
        where_clauses = []
        for col, filters in filter_options.items():
            if col in global_df.columns:  # Check if column exists
                # filters_str = ', '.join([f"'{f}'" for f in filters]) #Escape string for SQL query
                filters_str = ', '.join(["'{}'".format(f.replace("'", "''")) for f in filters])
                where_clauses.append(f"{col} IN ({filters_str})")
        if where_clauses: #Check if there is any where clause to add
            filter_clause = "WHERE " + " AND ".join(where_clauses)

    if aggregation == "sum":
        agg_func = "SUM"
    elif aggregation == "average":
        agg_func = "AVG"
    elif aggregation == "count":
        agg_func = "COUNT"
    elif aggregation == "maximum":
        agg_func = "MAX"
    elif aggregation == "minimum":
        agg_func = "MIN"
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    x_axis_columns_str = ', '.join(f'"{column}"' for column in x_axis_columns)
    print("x_axis_columns_str",x_axis_columns_str)
    if len(y_axis_columns) == 1:
        # query = f"SELECT {x_axis_columns_str}, {agg_func}(\"{y_axis_columns[0]}\"::numeric) AS {y_axis_columns[0]} FROM {table_name} {filter_clause} GROUP BY {x_axis_columns_str}"
        query = f"""
        SELECT {x_axis_columns[0]}, {x_axis_columns[1]}, {agg_func}({y_axis_columns[0]})
        FROM {table_name}
        {filter_clause}
        GROUP BY {x_axis_columns[0]}, {x_axis_columns[1]}
        ORDER BY {x_axis_columns[0]}, {x_axis_columns[1]};
        """
        cur.execute(query)
        print("query",query)
        data = cur.fetchall()
   
    
    else:  # Dual y-axis
        query = f"SELECT {x_axis_columns_str}, {agg_func}(\"{y_axis_columns[0]}\"::numeric) AS {y_axis_columns[0]}, {agg_func}(\"{y_axis_columns[1]}\"::numeric) AS {y_axis_columns[1]} FROM {table_name} {filter_clause} GROUP BY {x_axis_columns_str}"

    print("Constructed Query:", cur.mogrify(query).decode('utf-8')) #No need to pass options here, it is already handled by filter clause
    cur.execute(query)
    rows = cur.fetchall()
    print("Rows from database:", rows)
    cur.close()
    conn.close()
    return rows
    # if aggregation == "sum":
    #     aggregation = "SUM"
    # elif aggregation == "average":
    #     aggregation = "AVG"
    # elif aggregation == "count":
    #     aggregation = "COUNT"
    # elif aggregation == "maximum":
    #     aggregation = "MAX"
    # elif aggregation == "minimum":
    #     aggregation = "MIN"
        
    # x_axis_columns_str = ', '.join(f'"{column}"' for column in x_axis_columns)
    # options = [option.strip() for option in checked_option.split(',')]
    # placeholders = ','.join(['%s' for _ in options])
    # # query = f"SELECT {x_axis_columns[0]}, {aggregation}(\"{y_axis_column[0]}\"::numeric) AS {y_axis_column[0]},{aggregation}(\"{y_axis_column[1]}\"::numeric) AS {y_axis_column[1]} FROM {table_name} WHERE {x_axis_columns[0]} IN ({placeholders}) GROUP BY {x_axis_columns_str}"  
    # # Ensure there are at least two columns for y_axis_columns, or handle it appropriately
    # if len(y_axis_columns) == 1:
    #     print("y=1")
    #     query = f"SELECT {x_axis_columns[0]}, {aggregation}(\"{y_axis_columns[0]}\"::numeric) AS {y_axis_columns[0]} FROM {table_name} GROUP BY {x_axis_columns_str}"
    
    # else:
    #     query = f"SELECT {x_axis_columns[0]}, {aggregation}(\"{y_axis_columns[0]}\"::numeric) AS {y_axis_columns[0]}, {aggregation}(\"{y_axis_columns[1]}\"::numeric) AS {y_axis_columns[1]} FROM {table_name} WHERE {x_axis_columns[0]} IN ({placeholders}) GROUP BY {x_axis_columns_str}"

    # print("Constructed Query:", cur.mogrify(query, options).decode('utf-8'))
    # cur.execute(query,options)
    # rows = cur.fetchall()
    # cur.close()
    # conn.close()
    # return rows

from psycopg2 import sql
# def fetch_column_name(table_name, x_axis_columns, db_name, selectedUser='null'):
#     print("selectedUser:", selectedUser)
    
#     # Connect to the appropriate database based on connection_type
#     if not selectedUser or selectedUser.lower() == 'null':
#         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#     else:  # External connection
#         connection_details = fetch_external_db_connection(db_name,selectedUser)
#         if connection_details:
#             db_details = {
#                 "host": connection_details[3],
#                 "database": connection_details[7],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6])
#             }
#         if not connection_details:
#             raise Exception("Unable to fetch external database connection details.")
        
#         conn = psycopg2.connect(
#             dbname=db_details['database'],
#             user=db_details['user'],
#             password=db_details['password'],
#             host=db_details['host'],
#             port=db_details['port'],
#         )
    
#     cur = conn.cursor()
#     type_query = sql.SQL(
#         "SELECT data_type FROM information_schema.columns WHERE table_name = {table} AND column_name = {col}"
#     )
#     type_check_query = type_query.format(
#         table=sql.Literal(table_name),
#         col=sql.Literal(x_axis_columns)
#     )
#     cur.execute(type_check_query)
#     column_type = cur.fetchone()

#     # Dynamically build the query based on the column type
#     if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
#         # Use TO_CHAR for date or timestamp columns
#         query = sql.SQL("SELECT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table} GROUP BY {col}")
#     else:
#         # Directly fetch the column value for other data types
#         query = sql.SQL("SELECT {col} FROM {table} GROUP BY {col}")
    
#     formatted_query = query.format(
#         col=sql.Identifier(x_axis_columns),
#         table=sql.Identifier(table_name)
#     )
    
#     cur.execute(formatted_query)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     return rows

# def fetch_column_name(table_name, x_axis_columns, db_name, selectedUser='null'):
#     """
#     Fetch distinct values for one or more columns. If multiple columns are provided
#     as a comma-separated string, the function returns a dictionary with each column's
#     distinct values.
#     """
#     print("selectedUser:", selectedUser)
    
#     # Connect to the appropriate database based on connection type
#     if not selectedUser or selectedUser.lower() == 'null':
#         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#     else:  # External connection
#         connection_details = fetch_external_db_connection(db_name, selectedUser)
#         if connection_details:
#             db_details = {
#                 "host": connection_details[3],
#                 "database": connection_details[7],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6])
#             }
#         if not connection_details:
#             raise Exception("Unable to fetch external database connection details.")
        
#         conn = psycopg2.connect(
#             dbname=db_details['database'],
#             user=db_details['user'],
#             password=db_details['password'],
#             host=db_details['host'],
#             port=db_details['port'],
#         )
    
#     cur = conn.cursor()
    
#     # Determine if we have multiple columns (comma-separated) or just one.
#     if ',' in x_axis_columns:
#         columns = [col.strip() for col in x_axis_columns.split(',')]
#     else:
#         columns = [x_axis_columns.strip()]
    
#     results = {}
    
#     for col in columns:
#         # Check the data type of the column to format date/timestamp types appropriately.
#         type_query = sql.SQL(
#             "SELECT data_type FROM information_schema.columns WHERE table_name = {table} AND column_name = {col}"
#         )
#         type_check_query = type_query.format(
#             table=sql.Literal(table_name),
#             col=sql.Literal(col)
#         )
#         cur.execute(type_check_query)
#         column_type = cur.fetchone()
    
#         # Build the SQL query based on the column type.
#         if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
#             query = sql.SQL("SELECT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table} GROUP BY {col}")
#         else:
#             query = sql.SQL("SELECT {col} FROM {table} GROUP BY {col}")
    
#         formatted_query = query.format(
#             col=sql.Identifier(col),
#             table=sql.Identifier(table_name)
#         )
    
#         cur.execute(formatted_query)
#         rows = cur.fetchall()
#         # Convert the returned rows (tuples) to a flat list.
#         results[col] = [row[0] for row in rows]
    
#     cur.close()
#     conn.close()
    
#     # Return a dictionary mapping each column to its distinct values.
#     return results

def fetch_column_name(table_name, x_axis_columns, db_name, selectedUser='null'):
    """
    Fetch distinct values for one or more columns.
    If multiple columns are provided as a comma-separated string,
    returns a dictionary with each column's distinct values.
    """
    print("selectedUser:", selectedUser)

    # Establish database connection
    try:
        if not selectedUser or selectedUser.lower() == 'null':
            conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
        else:
            connection_details = fetch_external_db_connection(db_name, selectedUser)
            if not connection_details:
                raise Exception("Unable to fetch external database connection details.")
            
            db_details = {
                "host": connection_details[3],
                "database": connection_details[7],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6])
            }
            conn = psycopg2.connect(
                dbname=db_details['database'],
                user=db_details['user'],
                password=db_details['password'],
                host=db_details['host'],
                port=db_details['port'],
            )

        results = {}

        with conn.cursor() as cur:  # ✅ Use a regular cursor instead of a named cursor
            # Split multiple columns if needed
            columns = [col.strip() for col in x_axis_columns.split(',')] if ',' in x_axis_columns else [x_axis_columns.strip()]
            
            for col in columns:
                # Check the data type of the column
                type_query = """
                    SELECT data_type FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """
                cur.execute(type_query, (table_name, col))
                column_type = cur.fetchone()

                # Build the SQL query dynamically based on data type
                if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
                    query = sql.SQL("SELECT DISTINCT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table}")
                else:
                    query = sql.SQL("SELECT DISTINCT {col} FROM {table}")

                formatted_query = query.format(
                    col=sql.Identifier(col),
                    table=sql.Identifier(table_name)
                )

                cur.execute(formatted_query)
                rows = cur.fetchall()

                # Convert results to a flat list
                results[col] = [row[0] for row in rows]

        conn.close()
        return results

    except Exception as e:
        raise Exception(f"Error fetching distinct column values from {table_name}: {str(e)}")
# from psycopg2 import sql

# def fetch_column_name(table_name, x_axis_columns, db_name, selectedUser='null'):
#     print("selectedUser:", selectedUser)

#     # Connect to the appropriate database based on connection_type
#     if not selectedUser or selectedUser.lower() == 'null':
#         conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
#     else:  # External connection
#         connection_details = fetch_external_db_connection(db_name, selectedUser)
#         if connection_details:
#             db_details = {
#                 "host": connection_details[3],
#                 "database": connection_details[7],
#                 "user": connection_details[4],
#                 "password": connection_details[5],
#                 "port": int(connection_details[6])
#             }
#         if not connection_details:
#             raise Exception("Unable to fetch external database connection details.")

#         conn = psycopg2.connect(
#             dbname=db_details['database'],
#             user=db_details['user'],
#             password=db_details['password'],
#             host=db_details['host'],
#             port=db_details['port'],
#         )

#     cur = conn.cursor()

#     # Split x_axis_columns into a list of column names
#     column_list = x_axis_columns.split(',')
#     result = {}

#     for column_name in column_list:
#         column_name = column_name.strip()

#         # Get the column type
#         type_query = sql.SQL(
#             "SELECT data_type FROM information_schema.columns WHERE table_name = {table} AND column_name = {col}"
#         )
#         type_check_query = type_query.format(
#             table=sql.Literal(table_name),
#             col=sql.Literal(column_name)
#         )
#         cur.execute(type_check_query)
#         column_type = cur.fetchone()

#         # Dynamically build the query based on the column type
#         if column_type and column_type[0] in ('date', 'timestamp', 'timestamp with time zone'):
#             # Use TO_CHAR for date or timestamp columns
#             query = sql.SQL("SELECT TO_CHAR({col}, 'YYYY-MM-DD') FROM {table} GROUP BY {col}")
#         else:
#             # Directly fetch the column value for other data types
#             query = sql.SQL("SELECT {col} FROM {table} GROUP BY {col}")
        
#         formatted_query = query.format(
#             col=sql.Identifier(column_name),
#             table=sql.Identifier(table_name)
#         )
        
#         cur.execute(formatted_query)
#         rows = [row[0] for row in cur.fetchall()]  # Fetch and flatten the result
#         result[column_name] = rows  # Add the column data to the result dictionary

#     cur.close()
#     conn.close()
#     return result

    # query = f"SELECT {x_axis_columns} FROM {table_name} GROUP BY {x_axis_columns}"
    # print("querty",query)
    # cur.execute(query)
    # rows = cur.fetchall()
    # cur.close()
    # conn.close()
    # return rows

# def fetch_column_name(table_name, x_axis_columns,db_nameeee):
#     conn = psycopg2.connect(f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}")
#     cur = conn.cursor()
#     # for i in range(len(x_axis_columns)):
#     query = f"SELECT {x_axis_columns} FROM {table_name} GROUP BY {x_axis_columns}" 
#     cur.execute(query)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     return rows

from sqlalchemy import create_engine


def calculationFetch():
    global global_df 
    try:
        return global_df
    except Exception as e:
        print(f"Error connecting to the database or reading data: {e}")
        return None
    
def segregate_string_pattern(calculation):
    words = re.findall(r'\[([^\]]+)\]', calculation)
    symbols = re.findall(r'[+\-*/]', calculation)
    print("words", words)
    print("symbols", symbols)
    return words, symbols

def perform_calculation(dataframe, columnName, calculation):
    global global_df
    words, symbols = segregate_string_pattern(calculation)
    if len(words) != len(symbols) + 1:
        raise ValueError('Invalid calculation format')
    
    expression = dataframe[words[0]].copy()  # Use copy to prevent modifying the original column
    for i in range(len(symbols)):
        symbol = symbols[i]
        word = words[i + 1]
        if symbol == '+':
            expression += dataframe[word]
        elif symbol == '-':
            expression -= dataframe[word]
        elif symbol == '*':
            expression *= dataframe[word]
        elif symbol == '/':
            expression /= dataframe[word]
    
    dataframe[columnName] = expression.round(2)
    global_df = dataframe   
    return global_df

# def fetchText_data(databaseName, table_Name, x_axis, aggregate):
#     print("aggregate===========================>>>>", aggregate)   
#     print(table_Name)

#     aggregate_sql = {
#         'count': 'COUNT',
#         'sum': 'SUM',
#         'average': 'AVG',
#         'minimum': 'MIN',
#         'maximum': 'MAX',
#         'variance': 'VARIANCE',
#     }.get(aggregate, 'SUM') 

#     # Connect to the database
#     conn = psycopg2.connect(f"dbname={databaseName} user={USER_NAME} password={PASSWORD} host={HOST}")
#     cur = conn.cursor()

#     # Check the data type of the x_axis column
#     cur.execute(f"""
#         SELECT data_type 
#         FROM information_schema.columns 
#         WHERE table_name = %s AND column_name = %s
#     """, (table_Name, x_axis))
    
#     column_type = cur.fetchone()[0]
    
#     # Use DISTINCT only if the column type is character varying
#     if column_type == 'character varying':
#         query = f"""
#         SELECT {aggregate_sql}(DISTINCT {x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         """
#     else:
#         query = f"""
#         SELECT {aggregate_sql}({x_axis}) AS total_{x_axis}
#         FROM {table_Name}
#         """

#     print("Query:", query)
    
#     cur.execute(query)
#     result = cur.fetchone()  # Fetch only one row since the query returns a single value
    
#     # Close the cursor and connection
#     cur.close()
#     conn.close()

#     # Process the result into a dictionary
#     data = {"total_x_axis": result[0]}  # result[0] contains the aggregated value

#     return data




def fetchText_data(databaseName, table_Name, x_axis, aggregate_py,selectedUser):
    print("aggregate===========================>>>>", aggregate_py)   
    print(table_Name)
    

    # # Connect to the database
    # conn = psycopg2.connect(f"dbname={databaseName} user={USER_NAME} password={PASSWORD} host={HOST}")
    # cur = conn.cursor()
    # if selectedUser == None:
    if not selectedUser or selectedUser.lower() == 'null':
    # Handle local database connection

        conn = psycopg2.connect(f"dbname={databaseName} user={USER_NAME} password={PASSWORD} host={HOST}")

    else:  # External connection
        connection_details = fetch_external_db_connection(databaseName,selectedUser)
        if connection_details:
            db_details = {
                "host": connection_details[3],
                "database": connection_details[7],
                "user": connection_details[4],
                "password": connection_details[5],
                "port": int(connection_details[6])
            }
        if not connection_details:
            raise Exception("Unable to fetch external database connection details.")
        
        conn = psycopg2.connect(
            dbname=db_details['database'],
            user=db_details['user'],
            password=db_details['password'],
            host=db_details['host'],
            port=db_details['port'],
        )
    
    cur = conn.cursor()

    # Check the data type of the x_axis column
    cur.execute(f"""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (table_Name, x_axis))
    
    column_type = cur.fetchone()[0]
    print("column_type",column_type)
    # Use DISTINCT only if the column type is character varying
    if column_type == 'character varying':
        query = f"""
        SELECT COUNT(DISTINCT {x_axis}) AS total_{x_axis}
        FROM {table_Name}
        """
        print("character varying")  
    else:
        query = f"""
        SELECT {aggregate_py}({x_axis}) AS total_{x_axis}
        FROM {table_Name}
        """

    print("Query:", query)
    
    cur.execute(query)
    result = cur.fetchone()  # Fetch only one row since the query returns a single value
    
    # Close the cursor and connection
    cur.close()
    conn.close()

    # Process the result into a dictionary
    data = {"total_x_axis": result[0]}  # result[0] contains the aggregated value

    return data





def Hierarchial_drill_down(clicked_category, x_axis_columns, y_axis_column, depth, aggregation):
    global global_df
    if global_df is None:
        print("DataFrame not initialized. Please call fetch_hierarchical_data first.")
        return {"error": "Data not initialized."}

    print(f"Drill-Down Start: Current Depth: {depth}, Clicked Category: {clicked_category}")

    # Get the current column for this depth level
    current_column = x_axis_columns[depth]
    print("current_column",current_column)
    # Filter the DataFrame based on the clicked category at the current depth level
    filtered_df = global_df[global_df[current_column] == clicked_category]

    # Handle case where the filtered DataFrame is empty
    if filtered_df.empty:
        print(f"No data found for category '{clicked_category}' at depth {depth}.")
        return {"error": f"No data found for '{clicked_category}' at depth {depth}."}

    # If we are at the last level of the hierarchy
    if depth == len(x_axis_columns) - 1:
        print(f"At the last level: {current_column}")
        return {
            "categories": filtered_df[current_column].tolist(),
            "values": filtered_df[y_axis_column[0]].tolist()
        }

    # Move to the next depth level if not at the last level
    next_level_column = x_axis_columns[depth + 1]

    # Aggregate the data for the next level based on the aggregation method from frontend
    if aggregation == 'count':
        aggregated_df = filtered_df.groupby(next_level_column).size().reset_index(name='count')
    elif aggregation == 'sum':
        aggregated_df = filtered_df.groupby(next_level_column)[y_axis_column[0]].sum().reset_index()
    elif aggregation == 'mean':
        aggregated_df = filtered_df.groupby(next_level_column)[y_axis_column[0]].mean().reset_index()
    else:
        return {"error": "Unsupported aggregation method."}

    print(f"Next level DataFrame at depth {depth + 1} for column {next_level_column}:")
    print(aggregated_df.head())  # Log the aggregated data

    # Handle case where aggregated DataFrame is empty
    if aggregated_df.empty:
        print(f"No data available at depth {depth + 1} for column {next_level_column}. Returning current level data.")
        return {
            "categories": filtered_df[current_column].tolist(),
            "values": filtered_df[y_axis_column[0]].tolist()
        }

    # Prepare the result with the next level's categories and values
    result = {
        "categories": aggregated_df[next_level_column].tolist(),
        "values": aggregated_df['count'].tolist() if aggregation == 'count' else aggregated_df[y_axis_column[0]].tolist(),
        "next_level_column": next_level_column
    }
    return result



def fetch_hierarchical_data(table_name, db_name,selectedUser):
    global global_df

    if global_df is None:
        print("Fetching data from the database...")
        try:
            # conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
            # cur = conn.cursor()
            if not selectedUser or selectedUser.lower() == 'null':
                conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
            else:  # External connection
                connection_details = fetch_external_db_connection(db_name,selectedUser)
                if connection_details:
                    db_details = {
                        "host": connection_details[3],
                        "database": connection_details[7],
                        "user": connection_details[4],
                        "password": connection_details[5],
                        "port": int(connection_details[6])
                    }
                if not connection_details:
                    raise Exception("Unable to fetch external database connection details.")
                
                conn = psycopg2.connect(
                    dbname=db_details['database'],
                    user=db_details['user'],
                    password=db_details['password'],
                    host=db_details['host'],
                    port=db_details['port'],
                )
            
            cur = conn.cursor()

            print("conn",conn)
            query = f"SELECT * FROM {table_name}"
            print("query",query)
            cur.execute(query)
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            
            if not data:
                print("No data returned from the query.")
                return None  # or handle this case as needed

            global_df = pd.DataFrame(data, columns=colnames)
            print("Full DataFrame loaded with rows:", len(global_df))
            print(global_df.head())
            
            # Convert the y-axis column to numeric if necessary
            y_axis_column = 'Specify your y_axis_column here'  # Update with the actual y-axis column name if necessary
            if y_axis_column in global_df.columns:
                global_df[y_axis_column] = pd.to_numeric(global_df[y_axis_column], errors='coerce')
            else:
                print(f"Warning: Column {y_axis_column} not found in DataFrame columns.")

        except psycopg2.Error as db_err:
            print("Database error:", db_err)
            return None  # Handle database connection issues
        
        except Exception as e:
            print("An unexpected error occurred:", str(e))
            return None
        
        finally:
            cur.close()
            conn.close()

    return global_df
