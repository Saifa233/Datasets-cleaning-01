import pandas as pd
import numpy as np
import json
from utils import *

if __name__ == "__main__":
    with open("./config.json", "r") as f:
        config = json.load(f)

    df_ma_acdbe = get_data("./Datasets/MA_ACDBE.xlsx", config["ma_acdbe"]["file_type"], rows_to_skip=3)
    df_ma_dbe = get_data("./Datasets/MA_DBE.xlsx", config["ma_dbe"]["file_type"], rows_to_skip=3)
    df_ma_mwpbe = get_data("./Datasets/MA_MWPBE.xlsx", config["ma_mwpbe"]["file_type"], rows_to_skip=3)

    #MA_ACDBE: handling duplicate columns and mapping schema
    df_ma_acdbe = dup_cols_renamer(df_ma_acdbe)
    df_ma_acdbe = get_cleaned_df_ma_acdbe(df_ma_acdbe)
    df_ma_acdbe.rename(columns = config["ma_acdbe"]["schema_mapping"], inplace=True)


    #MA_DBE: handling duplicate columns and mapping schema
    df_ma_dbe = dup_cols_renamer(df_ma_dbe)
    df_ma_dbe = get_cleaned_df_ma_dbe(df_ma_dbe)
    df_ma_dbe.rename(columns = config["ma_dbe"]["schema_mapping"], inplace=True)


    # MA_MWPBE: handling duplicate columns and mapping schema
    df_ma_mwpbe = dup_cols_renamer(df_ma_mwpbe)
    df_ma_mwpbe = get_cleaned_df_ma_mwpbe(df_ma_mwpbe)
    df_ma_mwpbe.rename(columns = config["ma_mwpbe"]["schema_mapping"], inplace=True)
 
    #Columns
    db_cols = set(list(df_ma_acdbe.columns)+list(df_ma_dbe.columns)+list(df_ma_mwpbe.columns))
    ma_acdbe_req_cols = list(db_cols - set(df_ma_acdbe.columns))
    ma_dbe_req_cols = list(db_cols - set(df_ma_dbe.columns))
    ma_mwpbe_req_cols = list(db_cols - set(df_ma_mwpbe.columns))

    df_ma_acdbe[ma_acdbe_req_cols] = np.nan
    df_ma_dbe[ma_dbe_req_cols] = np.nan
    df_ma_mwpbe[ma_mwpbe_req_cols] = np.nan
    df = pd.concat([df_ma_acdbe, df_ma_dbe, df_ma_mwpbe], ignore_index=True, axis=0)
    wb = pd.ExcelWriter('Output.xlsx', engine='xlsxwriter')
    df.to_excel(wb, sheet_name='Sheet1',index=False)
    wb.save()

    
