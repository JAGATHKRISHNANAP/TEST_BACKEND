
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def analyze_data(data):
    # Step 1: Remove unique identifier columns
    unique_columns = [col for col in data.columns if data[col].nunique() == len(data)]
    data = data.drop(columns=unique_columns)

    # Step 2: Prepare features for each column after dropping unique columns
    column_features = []
    for col in data.columns:
        col_data = data[col]
        col_type = "numeric" if pd.api.types.is_numeric_dtype(col_data) else "categorical"

        features = {
            "column_name": col,
            "data_type": 1 if col_type == "numeric" else 0,
            "unique_values": col_data.nunique(),
            "mean": col_data.mean() if col_type == "numeric" else 0,
            "std_dev": col_data.std() if col_type == "numeric" else 0,
            "variance": col_data.var() if col_type == "numeric" else 0,
            "missing_values": col_data.isnull().sum()
        }

        column_features.append(features)

    # Convert features to a DataFrame
    features_df = pd.DataFrame(column_features).fillna(0)
    features_df.set_index("column_name", inplace=True)

    # Standardize the data
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features_df)

    # Step 3: Apply KMeans Clustering
    kmeans = KMeans(n_clusters=2, random_state=42)
    kmeans.fit(scaled_features)
    clusters = kmeans.labels_

    # Step 4: Identify clusters with higher variance as dependent variables
    features_df['cluster'] = clusters
    cluster_variances = features_df.groupby('cluster')['variance'].mean()
    likely_dependent_cluster = cluster_variances.idxmax()

    # Classify columns based on cluster
    features_df['predicted_type'] = features_df['cluster'].apply(
        lambda x: 'dependent' if x == likely_dependent_cluster else 'independent'
    )

    # Visualization data array
    arrayData = []

    # Extract dependent and independent variables
    result = features_df[['predicted_type']].rename(columns={'predicted_type': 'Predicted Variable Type'})
    dependent_columns = result[result['Predicted Variable Type'] == 'dependent'].index
    independent_columns = result[result['Predicted Variable Type'] == 'independent'].index

    # # Prepare scatter and bar chart data with aggregations
    # if not independent_columns.empty and not dependent_columns.empty:
    #     for ind_col in independent_columns:
    #         for dep_col in dependent_columns:
    #             if pd.api.types.is_numeric_dtype(data[ind_col]) and pd.api.types.is_numeric_dtype(data[dep_col]):
    #                 # Compute correlation
    #                 correlation = data[[ind_col, dep_col]].corr().iloc[0, 1]
    #                 if abs(correlation) >= 0.3:  # Add scatter plots for meaningful correlations
    #                     scatter_chart_data = {
    #                         "chart_type": "scatter",
    #                         "x_axis": ind_col,
    #                         "y_axis": dep_col,
    #                         "categories": data[ind_col].tolist(),
    #                         "values": data[dep_col].tolist(),
    #                         "aggregation": "none"
    #                     }
    #                     arrayData.append(scatter_chart_data)
    #             else:
    #                 # Bar chart data with meaningful aggregations
    #                 if pd.api.types.is_numeric_dtype(data[ind_col]):
    #                     data[f"{ind_col}_binned"] = pd.cut(data[ind_col], bins=10)
    #                     grouped_data = data.groupby(f"{ind_col}_binned")[dep_col].agg(['sum', 'count']).reset_index()
    #                     x = f"{ind_col}_binned"
    #                 else:
    #                     grouped_data = data.groupby(ind_col)[dep_col].agg(['sum', 'count']).reset_index()
    #                     x = ind_col
    #                 if not grouped_data.empty:
    #                     # Include only meaningful aggregations
    #                     for agg in ['sum', 'count']:
    #                         categories = grouped_data[x].tolist()
    #                         values = grouped_data[agg].tolist()
    #                         if agg == 'sum' and values and max(values) > 0:  # Exclude empty or zero data
    #                             bar_chart_data = {
    #                                 "chart_type": "bar",
    #                                 "x_axis": ind_col,
    #                                 "y_axis": dep_col,
    #                                 "categories": categories,
    #                                 "values": values,
    #                                 "aggregation": agg
    #                             }
    #                             arrayData.append(bar_chart_data)
    if not independent_columns.empty and not dependent_columns.empty:
        for ind_col in independent_columns:
            for dep_col in dependent_columns:
                try:
                    if pd.api.types.is_numeric_dtype(data[ind_col]) and pd.api.types.is_numeric_dtype(data[dep_col]):
                        # Compute correlation
                        correlation = data[[ind_col, dep_col]].corr().iloc[0, 1]
                        if abs(correlation) >= 0.3:
                            scatter_chart_data = {
                                "chart_type": "scatter",
                                "x_axis": ind_col,
                                "y_axis": dep_col,
                                "categories": data[ind_col].tolist(),
                                "values": data[dep_col].tolist(),
                                "aggregation": "none"
                            }
                            arrayData.append(scatter_chart_data)
                    else:
                        if pd.api.types.is_numeric_dtype(data[ind_col]):
                            data[f"{ind_col}_binned"] = pd.cut(data[ind_col].dropna(), bins=10)
                            grouped_data = data.groupby(f"{ind_col}_binned")[dep_col].agg(['sum', 'count']).reset_index()
                            x = f"{ind_col}_binned"
                        else:
                            grouped_data = data.groupby(ind_col)[dep_col].agg(['sum', 'count']).reset_index()
                            x = ind_col
                        if not grouped_data.empty:
                            for agg in ['sum', 'count']:
                                categories = grouped_data[x].tolist()
                                values = grouped_data[agg].tolist()
                                if agg == 'sum' and values and max(values) > 0:
                                    bar_chart_data = {
                                        "chart_type": "bar",
                                        "x_axis": ind_col,
                                        "y_axis": dep_col,
                                        "categories": categories,
                                        "values": values,
                                        "aggregation": agg
                                    }
                                    arrayData.append(bar_chart_data)
                except Exception as e:
                    print(f"Error processing {ind_col}, {dep_col}: {e}")


    return arrayData

# def analyze_data(data):
#     # Step 1: Remove unique identifier columns
#     unique_columns = [col for col in data.columns if data[col].nunique() == len(data)]
#     data = data.drop(columns=unique_columns)

#     # Step 2: Prepare features for each column after dropping unique columns
#     column_features = []
#     for col in data.columns:
#         col_data = data[col]
#         col_type = "numeric" if pd.api.types.is_numeric_dtype(col_data) else "categorical"

#         features = {
#             "column_name": col,
#             "data_type": 1 if col_type == "numeric" else 0,
#             "unique_values": col_data.nunique(),
#             "mean": col_data.mean() if col_type == "numeric" else 0,
#             "std_dev": col_data.std() if col_type == "numeric" else 0,
#             "variance": col_data.var() if col_type == "numeric" else 0,
#             "missing_values": col_data.isnull().sum()
#         }

#         column_features.append(features)

#     # Convert features to a DataFrame
#     features_df = pd.DataFrame(column_features).fillna(0)
#     features_df.set_index("column_name", inplace=True)

#     # Standardize the data
#     scaler = StandardScaler()
#     scaled_features = scaler.fit_transform(features_df)

#     # Step 3: Apply KMeans Clustering
#     kmeans = KMeans(n_clusters=2, random_state=42)
#     kmeans.fit(scaled_features)
#     clusters = kmeans.labels_

#     # Step 4: Identify clusters with higher variance as dependent variables
#     features_df['cluster'] = clusters
#     cluster_variances = features_df.groupby('cluster')['variance'].mean()
#     likely_dependent_cluster = cluster_variances.idxmax()

#     # Classify columns based on cluster
#     features_df['predicted_type'] = features_df['cluster'].apply(
#         lambda x: 'dependent' if x == likely_dependent_cluster else 'independent'
#     )

#     # Visualization data array
#     arrayData = []

#     # Extract dependent and independent variables
#     result = features_df[['predicted_type']].rename(columns={'predicted_type': 'Predicted Variable Type'})
#     dependent_columns = result[result['Predicted Variable Type'] == 'dependent'].index
#     independent_columns = result[result['Predicted Variable Type'] == 'independent'].index

#     # Prepare chart data for each chart type
#     if not independent_columns.empty and not dependent_columns.empty:
#         for ind_col in independent_columns:
#             for dep_col in dependent_columns:
#                 # For scatter plots
#                 if pd.api.types.is_numeric_dtype(data[ind_col]) and pd.api.types.is_numeric_dtype(data[dep_col]):
#                     correlation = data[[ind_col, dep_col]].corr().iloc[0, 1]
#                     if abs(correlation) >= 0.3:
#                         scatter_chart_data = {
#                             "chart_type": "scatter",
#                             "x_axis": ind_col,
#                             "y_axis": dep_col,
#                             "categories": data[ind_col].tolist(),
#                             "values": data[dep_col].tolist(),
#                             "aggregation": "none"
#                         }
#                         arrayData.append(scatter_chart_data)

#                 # For bar charts
#                 if pd.api.types.is_numeric_dtype(data[ind_col]):
#                     data[f"{ind_col}_binned"] = pd.cut(data[ind_col], bins=10)
#                     grouped_data = data.groupby(f"{ind_col}_binned")[dep_col].agg(['sum', 'count']).reset_index()
#                     x = f"{ind_col}_binned"
#                 else:
#                     grouped_data = data.groupby(ind_col)[dep_col].agg(['sum', 'count']).reset_index()
#                     x = ind_col
#                 if not grouped_data.empty:
#                     for agg in ['sum', 'count']:
#                         categories = grouped_data[x].tolist()
#                         values = grouped_data[agg].tolist()
#                         if agg == 'sum' and values and max(values) > 0:
#                             bar_chart_data = {
#                                 "chart_type": "bar",
#                                 "x_axis": ind_col,
#                                 "y_axis": dep_col,
#                                 "categories": categories,
#                                 "values": values,
#                                 "aggregation": agg
#                             }
#                             arrayData.append(bar_chart_data)

#                 # For pie charts
#                 # # For pie charts
#                 # if not pd.api.types.is_numeric_dtype(data[ind_col]):
#                 #     # Check for valid data
#                 #     valid_data = data[ind_col].dropna()  # Drop NaN values
#                 #     if not valid_data.empty:
#                 #         pie_data = valid_data.value_counts(normalize=True).reset_index()
#                 #         pie_data.columns = ['category', 'proportion']  # Rename columns
                        
#                 #         # Only add pie chart data if categories exist
#                 #         if not pie_data.empty:
#                 #             pie_chart_data = {
#                 #                 "chart_type": "pie",
#                 #                 "labels": pie_data['category'].tolist(),  # Actual category names
#                 #                 "values": pie_data['proportion'].tolist(),  # Proportion of each category
#                 #                 "aggregation": "proportion"
#                 #             }
#                 #             arrayData.append(pie_chart_data)



#                 # For line charts (if sequential or numeric data is present)
#                 if pd.api.types.is_numeric_dtype(data[ind_col]):
#                     line_chart_data = {
#                         "chart_type": "line",
#                         "x_axis": ind_col,
#                         "y_axis": dep_col,
#                         "categories": data[ind_col].tolist(),
#                         "values": data[dep_col].tolist(),
#                         "aggregation": "none"
#                     }
#                     arrayData.append(line_chart_data)

#     return arrayData



# def analyze_data(data):
#     # Step 1: Ensure all numeric columns contain only numeric data
#     for col in data.columns:
#         if not pd.api.types.is_numeric_dtype(data[col]):
#             data[col] = pd.to_numeric(data[col], errors="coerce")

#     # Step 2: Remove columns with all NaN values
#     data = data.dropna(axis=1, how="all")
    
#     # Step 3: Drop rows with any NaN values (optional, based on your use case)
#     data = data.dropna(axis=0, how="any")
    
#     # Step 4: Remove unique identifier columns
#     unique_columns = [col for col in data.columns if data[col].nunique() == len(data)]
#     data = data.drop(columns=unique_columns, errors="ignore")

#     # Step 5: Analyze the remaining data (same as before)
#     column_features = []
#     for col in data.columns:
#         col_data = data[col]
#         col_type = "numeric" if pd.api.types.is_numeric_dtype(col_data) else "categorical"

#         features = {
#             "column_name": col,
#             "data_type": 1 if col_type == "numeric" else 0,
#             "unique_values": col_data.nunique(),
#             "mean": col_data.mean() if col_type == "numeric" else 0,
#             "std_dev": col_data.std() if col_type == "numeric" else 0,
#             "variance": col_data.var() if col_type == "numeric" else 0,
#             "missing_values": col_data.isnull().sum()
#         }

#         column_features.append(features)

#     features_df = pd.DataFrame(column_features).fillna(0)
#     features_df.set_index("column_name", inplace=True)

#     # Standardize and cluster
#     scaler = StandardScaler()
#     scaled_features = scaler.fit_transform(features_df)

#     kmeans = KMeans(n_clusters=2, random_state=42)
#     kmeans.fit(scaled_features)
#     clusters = kmeans.labels_

#     features_df['cluster'] = clusters
#     cluster_variances = features_df.groupby('cluster')['variance'].mean()
#     likely_dependent_cluster = cluster_variances.idxmax()

#     features_df['predicted_type'] = features_df['cluster'].apply(
#         lambda x: 'dependent' if x == likely_dependent_cluster else 'independent'
#     )

#     arrayData = []
#     result = features_df[['predicted_type']].rename(columns={'predicted_type': 'Predicted Variable Type'})
#     dependent_columns = result[result['Predicted Variable Type'] == 'dependent'].index
#     independent_columns = result[result['Predicted Variable Type'] == 'independent'].index

#     if not independent_columns.empty and not dependent_columns.empty:
#         for ind_col in independent_columns:
#             for dep_col in dependent_columns:
#                 if pd.api.types.is_numeric_dtype(data[ind_col]) and pd.api.types.is_numeric_dtype(data[dep_col]):
#                     correlation = data[[ind_col, dep_col]].corr().iloc[0, 1]
#                     if abs(correlation) >= 0.3:
#                         scatter_chart_data = {
#                             "chart_type": "scatter",
#                             "x_axis": ind_col,
#                             "y_axis": dep_col,
#                             "categories": data[ind_col].tolist(),
#                             "values": data[dep_col].tolist(),
#                             "aggregation": "none"
#                         }
#                         arrayData.append(scatter_chart_data)
#                 else:
#                     if pd.api.types.is_numeric_dtype(data[ind_col]):
#                         data[f"{ind_col}_binned"] = pd.cut(data[ind_col], bins=10)
#                         grouped_data = data.groupby(f"{ind_col}_binned")[dep_col].agg(['sum', 'count']).reset_index()
#                         x = f"{ind_col}_binned"
#                     else:
#                         grouped_data = data.groupby(ind_col)[dep_col].agg(['sum', 'count']).reset_index()
#                         x = ind_col
#                     if not grouped_data.empty:
#                         for agg in ['sum', 'count']:
#                             categories = grouped_data[x].tolist()
#                             values = grouped_data[agg].tolist()
#                             if agg == 'sum' and values and max(values) > 0:
#                                 bar_chart_data = {
#                                     "chart_type": "bar",
#                                     "x_axis": ind_col,
#                                     "y_axis": dep_col,
#                                     "categories": categories,
#                                     "values": values,
#                                     "aggregation": agg
#                                 }
#                                 arrayData.append(bar_chart_data)

#     return arrayData


def analyze_data_for_save(data):
    # Step 1: Remove unique identifier columns
    unique_columns = [col for col in data.columns if data[col].nunique() == len(data)]
    data = data.drop(columns=unique_columns)

    # Step 2: Ensure numeric columns have consistent data types
    for col in data.select_dtypes(include=['object']).columns:
        data[col] = pd.to_numeric(data[col], errors='coerce')

    # Step 3: Prepare features for each column
    column_features = []
    for col in data.columns:
        col_data = data[col]
        col_type = "numeric" if pd.api.types.is_numeric_dtype(col_data) else "categorical"

        features = {
            "column_name": col,
            "data_type": 1 if col_type == "numeric" else 0,
            "unique_values": col_data.nunique(),
            "mean": col_data.mean() if col_type == "numeric" else 0,
            "std_dev": col_data.std() if col_type == "numeric" else 0,
            "variance": col_data.var() if col_type == "numeric" else 0,
            "missing_values": col_data.isnull().sum()
        }

        column_features.append(features)

    # Convert features to a DataFrame
    features_df = pd.DataFrame(column_features).fillna(0)
    features_df.set_index("column_name", inplace=True)

    # Standardize the data
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features_df)

    # Step 4: Apply KMeans Clustering
    kmeans = KMeans(n_clusters=2, random_state=42)
    kmeans.fit(scaled_features)
    clusters = kmeans.labels_

    features_df['cluster'] = clusters
    cluster_variances = features_df.groupby('cluster')['variance'].mean()
    likely_dependent_cluster = cluster_variances.idxmax()

    features_df['predicted_type'] = features_df['cluster'].apply(
        lambda x: 'dependent' if x == likely_dependent_cluster else 'independent'
    )

    # Visualization data array
    arrayData = []

    # Step 5: Extract and process columns
    result = features_df[['predicted_type']].rename(columns={'predicted_type': 'Predicted Variable Type'})
    dependent_columns = result[result['Predicted Variable Type'] == 'dependent'].index
    independent_columns = result[result['Predicted Variable Type'] == 'independent'].index

    for ind_col in independent_columns:
        for dep_col in dependent_columns:
            if pd.api.types.is_numeric_dtype(data[ind_col]) and pd.api.types.is_numeric_dtype(data[dep_col]):
                correlation = data[[ind_col, dep_col]].corr().iloc[0, 1]
                if not pd.isna(correlation) and abs(correlation) >= 0.3:
                    arrayData.append({
                        "chart_type": "scatter",
                        "x_axis": ind_col,
                        "y_axis": dep_col,
                        "categories": data[ind_col].tolist(),
                        "values": data[dep_col].tolist(),
                        "aggregation": "none"
                    })
            else:
                if pd.api.types.is_numeric_dtype(data[ind_col]):
                    try:
                        data[f"{ind_col}_binned"] = pd.cut(data[ind_col], bins=10)
                    except Exception as e:
                        print(f"Error during binning for {ind_col}: {e}")
                        continue
                grouped_data = data.groupby(ind_col)[dep_col].agg(['sum', 'count']).reset_index()
                for agg in ['sum', 'count']:
                    categories = grouped_data[ind_col].tolist()
                    values = grouped_data[agg].tolist()
                    if values and max(values) > 0:
                        arrayData.append({
                            "chart_type": "bar",
                            "x_axis": ind_col,
                            "y_axis": dep_col,
                            "categories": categories,
                            "values": values,
                            "aggregation": agg
                        })

    return arrayData
