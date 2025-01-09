# from flask import Flask, jsonify
# import pandas as pd
# from sklearn.linear_model import LinearRegression
# import numpy as np
# import bar_chart as bc

# def load_and_predict(xAxis, yAxis, number_of_periods, timePeriod):
#     # Load the global dataframe from bar_chart
#     data = bc.global_df
#     x_axis = xAxis[0]
#     y_axis = yAxis[0]

#     print("x_axis:", x_axis)
#     print("y_axis:", y_axis)
#     print("timePeriod:", timePeriod)
#     print("number_of_periods:", number_of_periods)

#     # Convert the x-axis to datetime and drop any invalid dates
#     data[x_axis] = pd.to_datetime(data[x_axis], errors='coerce')
#     data = data.dropna(subset=[x_axis])  # Drop rows with invalid dates

#     # Handle the time period and calculate the number of periods
#     if timePeriod == "years":
#         freq = 'YE'
#         data.set_index(x_axis, inplace=True)
#         aggregated_data = data.resample('Y').sum()[y_axis].reset_index()
#         number_of_periods_function = int(number_of_periods)
#     elif timePeriod == "months":
#         freq = 'M'
#         data.set_index(x_axis, inplace=True)
#         aggregated_data = data.resample('M').sum()[y_axis].reset_index()
#         number_of_periods_function = int(number_of_periods)
#     elif timePeriod == "days":
#         freq = 'D'
#         data.set_index(x_axis, inplace=True)
#         aggregated_data = data.resample('D').sum()[y_axis].reset_index()
#         number_of_periods_function = int(number_of_periods)
#     else:
#         raise ValueError(f"Invalid time period provided: {timePeriod}. Use 'years', 'months', or 'days'.")

#     # Prepare the data for linear regression model
#     aggregated_data['ds'] = pd.to_datetime(aggregated_data[x_axis])
#     aggregated_data['ds_numeric'] = aggregated_data['ds'].map(pd.Timestamp.toordinal)  # Convert datetime to ordinal (numeric)

#     X = np.array(aggregated_data['ds_numeric']).reshape(-1, 1)  # Independent variable (dates in numeric form)
#     y = np.array(aggregated_data[y_axis]).reshape(-1, 1)        # Dependent variable (target values)

#     # Train the Linear Regression model
#     model = LinearRegression()
#     model.fit(X, y)

#     # Create future dates for prediction
#     last_date = aggregated_data['ds'].max()
#     future_dates = [last_date + pd.DateOffset(**{timePeriod: i}) for i in range(1, number_of_periods_function + 1)]
#     future_dates_numeric = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)

#     # Predict future values
#     future_predictions = model.predict(future_dates_numeric)

#     # Prepare the data for JSON response
#     prediction_data = []
#     for date, prediction in zip(future_dates, future_predictions):
#         prediction_data.append({
#             'category': date.date().isoformat(),  # Convert date to ISO format
#             'value': round(prediction[0], 2)      # Predicted value (rounded)
#         })

#     return prediction_data



import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import bar_chart as bc

def load_and_predict(xAxis, yAxis, number_of_periods, timePeriod):
    # Load the global dataframe from bar_chart
    data = bc.global_df.copy()  # Create a local copy of the global dataframe
    x_axis = xAxis[0]
    y_axis = yAxis[0]

    print("x_axis:", x_axis)
    print("y_axis:", y_axis)
    print("timePeriod:", timePeriod)
    print("number_of_periods:", number_of_periods)

    # Convert the x-axis to datetime and drop any invalid dates
    data[x_axis] = pd.to_datetime(data[x_axis], errors='coerce')
    data = data.dropna(subset=[x_axis])  # Drop rows with invalid dates

    # Ensure y_axis is numeric
    data[y_axis] = pd.to_numeric(data[y_axis], errors='coerce')
    data = data.dropna(subset=[y_axis])  # Drop rows with non-numeric values in y_axis

    # Handle the time period and calculate the frequency for resampling
    if timePeriod == "years":
        freq = 'Y'
    elif timePeriod == "months":
        freq = 'M'
    elif timePeriod == "days":
        freq = 'D'
    else:
        raise ValueError(f"Invalid time period provided: {timePeriod}. Use 'years', 'months', or 'days'.")

    # Set the datetime column as the index for resampling
    data.set_index(x_axis, inplace=True)

    # Resample and aggregate only numeric columns
    aggregated_data = data.resample(freq).sum(numeric_only=True).reset_index()

    # Check if the aggregation preserved the y_axis column
    if y_axis not in aggregated_data.columns:
        raise ValueError(f"The column '{y_axis}' is not numeric or missing after resampling.")

    # Prepare the data for linear regression model
    aggregated_data['ds'] = pd.to_datetime(aggregated_data[x_axis])
    aggregated_data['ds_numeric'] = aggregated_data['ds'].map(pd.Timestamp.toordinal)  # Convert datetime to numeric

    X = np.array(aggregated_data['ds_numeric']).reshape(-1, 1)  # Independent variable (dates in numeric form)
    y = np.array(aggregated_data[y_axis]).reshape(-1, 1)        # Dependent variable (target values)

    # Train the Linear Regression model
    model = LinearRegression()
    model.fit(X, y)

    # Create future dates for prediction
    last_date = aggregated_data['ds'].max()
    future_dates = [last_date + pd.DateOffset(**{timePeriod: i}) for i in range(1, int(number_of_periods) + 1)]
    future_dates_numeric = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)

    # Predict future values
    future_predictions = model.predict(future_dates_numeric)

    # Prepare the data for JSON response
    prediction_data = []
    for date, prediction in zip(future_dates, future_predictions):
        prediction_data.append({
            'category': date.date().isoformat(),  # Convert date to ISO format
            'value': round(prediction[0], 2)      # Predicted value (rounded)
        })

    return prediction_data