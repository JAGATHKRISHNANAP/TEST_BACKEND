import pandas as pd



def generate_histogram_details(dataframe):
    axes = dataframe.hist()
    histogram_details = []
    
    for row in axes:
        for ax in row:
            bin_data = []
            for patch in ax.patches:
                # Each patch corresponds to a bin
                bin_info = {
                    "x_start": patch.get_x(),
                    "width": patch.get_width(),
                    "height": patch.get_height()
                }
                bin_data.append(bin_info)

            # Collect axis and bin details
            ax_info = {
                "title": ax.get_title(),
                "x_axis_label": ax.get_xlabel(),
                "y_axis_label": ax.get_ylabel(),
                "x_axis_limits": ax.get_xlim(),
                "y_axis_limits": ax.get_ylim(),
                "num_bins": len(ax.patches),
                "bins": bin_data  # Each bin's data is stored here
            }
            histogram_details.append(ax_info)

    # Filter out invalid or empty histogram details
    filtered_histogram_details = [
        ax_info for ax_info in histogram_details
        if ax_info["title"] or ax_info["num_bins"] > 0
    ]
    
    return filtered_histogram_details






def handle_column_data_types(df):

    numeric_columns_cleaned = {}
    text_columns_cleaned = {}

    # Convert object columns to numeric where applicable
    for col in df.columns:
        if df[col].dtype == 'object':  # If the column is of object type
            df[col] = pd.to_numeric(df[col], errors='ignore')

    # Separate numeric and text columns
    numeric_columns = df.select_dtypes(include=['float', 'int']).columns.tolist()
    text_columns = df.select_dtypes(include=['object']).columns.tolist()

    # Example: Apply cleaning logic (optional, replace/remove symbols, etc.)
    for column_name in numeric_columns:
        cleaned_values = df[column_name].apply(lambda x: x if pd.notnull(x) else 0).tolist()  # Replace NaN with 0
        numeric_columns_cleaned[column_name] = cleaned_values

    for column_name in text_columns:
        cleaned_values = df[column_name].apply(lambda x: x.strip() if isinstance(x, str) else x).tolist()  # Strip spaces
        text_columns_cleaned[column_name] = cleaned_values

    return df, numeric_columns, text_columns