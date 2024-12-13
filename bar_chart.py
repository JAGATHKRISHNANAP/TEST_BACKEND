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



def get_column_names(db_name, username, password, table_name, host='localhost', port='5432'):
    global global_df
    oldtablename = getattr(get_column_names, 'oldtablename', None)
    
    if oldtablename == table_name and global_df is not None:
        print("Using cached data from global_df")
        
        numeric_columns = global_df.select_dtypes(include=[float, int]).columns.tolist()
        text_columns = global_df.select_dtypes(include=[object]).columns.tolist()

        numeric_columns_cleaned = {}
        text_columns_cleaned = {}

        for column_name in numeric_columns:
            cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            num_columns = list(numeric_columns_cleaned.keys())

        for column_name in text_columns:
            cleaned_values = global_df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            txt_columns = list(text_columns_cleaned.keys())

        return {
            'numeric_columns': num_columns,
            'text_columns': txt_columns
        }
    
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=username,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")  # Get the column names without fetching data
        column_names = [desc[0] for desc in cursor.description]

        cursor.execute(f"SELECT * FROM {table_name}")
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

        for column_name in numeric_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            numeric_columns_cleaned[column_name] = cleaned_values
            num_columns = list(numeric_columns_cleaned.keys())

        for column_name in text_columns:
            cleaned_values = df[column_name].apply(remove_symbols).tolist()
            text_columns_cleaned[column_name] = cleaned_values
            txt_columns = list(text_columns_cleaned.keys())

        cursor.close()
        conn.close()

        return {
            'numeric_columns': num_columns,
            'text_columns': txt_columns
            # 'dataframe': global_df,
        }
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database.")
        print(e)
        return {'numeric_columns': [], 'text_columns': []}



def edit_fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
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
        print("Full DataFrame:")
        print(global_df[y_axis_column[0]])

        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')
        print(f"global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')")

    else:
        global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    if aggregation == "sum":
        aggregation_func = "sum"
    elif aggregation == "average":
        aggregation_func = "mean"
    elif aggregation == "count":
        aggregation_func = "count"
    elif aggregation == "maximum":
        aggregation_func = "max"
    elif aggregation == "minimum":
        aggregation_func = "min"
        
    x_axis_columns_str = x_axis_columns
    # print("The x axis columns are:", x_axis_columns_str)
    options = [option.strip() for option in checked_option.split(',')]
    # print("The options are:", options)
    filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
    grouped_df = filtered_df.groupby(x_axis_columns_str[0]).agg({y_axis_column[0]: aggregation_func}).reset_index()
    
    result = [tuple(x) for x in grouped_df.to_numpy()]
    
    return result


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


def fetch_data(table_name, x_axis_columns, checked_option, y_axis_column, aggregation, db_name):
    global global_df
    print("table_name:", table_name)
    print("x_axis_columns:", x_axis_columns)
    # print("checked_option:", checked_option)
    print("y_axis_column:", y_axis_column)
    print("aggregation:", aggregation)

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
        print("*********************************************************************************",global_df)
        # global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    # else:
    #     global_df[y_axis_column[0]] = pd.to_numeric(global_df[y_axis_column[0]], errors='coerce')

    x_axis_columns_str = x_axis_columns
    options = [option.strip() for option in checked_option.split(',')]
    filtered_df = global_df[global_df[x_axis_columns[0]].isin(options)]
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
    else:
        raise ValueError(f"Unsupported aggregation type: {aggregation}")

    # Convert the result to a list of tuples for easy output
    result = [tuple(x) for x in grouped_df.to_numpy()]
    print("result:", result)
    return result



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




def fetch_data_for_duel(table_name, x_axis_columns,checked_option, y_axis_column,aggregation,db_nameeee):
    conn = psycopg2.connect(f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}")
    cur = conn.cursor()
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
        
    x_axis_columns_str = ', '.join(f'"{column}"' for column in x_axis_columns)
    options = [option.strip() for option in checked_option.split(',')]
    placeholders = ','.join(['%s' for _ in options])
    query = f"SELECT {x_axis_columns[0]}, {aggregation}(\"{y_axis_column[0]}\"::numeric) AS {y_axis_column[0]},{aggregation}(\"{y_axis_column[1]}\"::numeric) AS {y_axis_column[1]} FROM {table_name} WHERE {x_axis_columns[0]} IN ({placeholders}) GROUP BY {x_axis_columns_str}"  

    print("Constructed Query:", cur.mogrify(query, options).decode('utf-8'))
    cur.execute(query,options)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_column_name(table_name, x_axis_columns,db_nameeee):
    conn = psycopg2.connect(f"dbname={db_nameeee} user={USER_NAME} password={PASSWORD} host={HOST}")
    cur = conn.cursor()
    # for i in range(len(x_axis_columns)):
    query = f"SELECT {x_axis_columns} FROM {table_name} GROUP BY {x_axis_columns}" 
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows



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

def fetchText_data(databaseName, table_Name, x_axis, aggregate):
    print("aggregate===========================>>>>", aggregate)   
    print(table_Name)

    aggregate_sql = {
        'count': 'COUNT',
        'sum': 'SUM',
        'average': 'AVG',
        'minimum': 'MIN',
        'maximum': 'MAX',
        'variance': 'VARIANCE',
    }.get(aggregate, 'SUM') 

    # Connect to the database
    conn = psycopg2.connect(f"dbname={databaseName} user={USER_NAME} password={PASSWORD} host={HOST}")
    cur = conn.cursor()

    # Check the data type of the x_axis column
    cur.execute(f"""
        SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (table_Name, x_axis))
    
    column_type = cur.fetchone()[0]
    
    # Use DISTINCT only if the column type is character varying
    if column_type == 'character varying':
        query = f"""
        SELECT {aggregate_sql}(DISTINCT {x_axis}) AS total_{x_axis}
        FROM {table_Name}
        """
    else:
        query = f"""
        SELECT {aggregate_sql}({x_axis}) AS total_{x_axis}
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



def fetch_hierarchical_data(table_name, db_name):
    global global_df

    if global_df is None:
        print("Fetching data from the database...")
        try:
            conn = psycopg2.connect(f"dbname={db_name} user={USER_NAME} password={PASSWORD} host={HOST}")
            cur = conn.cursor()
            query = f"SELECT * FROM {table_name}"
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