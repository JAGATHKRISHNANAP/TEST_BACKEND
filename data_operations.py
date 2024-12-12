import os
import pandas as pd

def read_excel(excel_file_path, sheet_name):
    return pd.read_excel(excel_file_path, sheet_name=sheet_name)

def write_to_excel(df, file_path):
    df.to_excel(file_path, index=False)
