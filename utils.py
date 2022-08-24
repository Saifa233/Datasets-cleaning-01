import re
import hashlib
import math
import validators
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import random
import string

def get_data(filepath, filetype, rows_to_skip):
    if filetype == "excel":
        df =  pd.read_excel(filepath, index_col=False, dtype=str, skiprows = rows_to_skip)
        return df
    elif filetype == "html":
        # handle NJ source
        data =  pd.read_html(filepath)
        df = data[0]
        df.columns = df.iloc[0]
        df = df[1:]
        return df

def dup_cols_renamer(df):
    df_columns = df.columns
    new_columns = [i.replace(".", "") for i in df_columns]
    df.columns = new_columns
    return df

def get_cleaned_df_ma_acdbe(df):
    df["Agency"] = "MA"
    df["Certification"] = "DBE"
    df["About"] = df["Description of Services"]
    return df

def get_cleaned_df_ma_dbe(df):
    df["Agency"] = "MA"
    df["Certification"] = "DBE"
    df["About"] = df["Description of Services"]
    return df

def get_cleaned_df_ma_mwpbe(df):
    df["Agency"] = "MA"
    df['Certification'] = df.apply(lambda row: categorise(row), axis=1)
    df["About"] = df["Description of Services"]
    return df

def categorise(row):  
    if row["MBE - Y/N"] == "Y":
        return "MBE"
    elif row["WBE - Y/N"] == "Y":
        return "WBE"
    elif row["PBE - Y/N"] == "Y":
        return "PBE"
    return ""

def clean_nan(x):
    if str(x) == "nan":
        return ""
    return str(x)
