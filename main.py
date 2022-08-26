import pandas as pd
import numpy as np
import json

from utils import *

if __name__ == "__main__":
    with open("./config.json", "r") as f:
        config = json.load(f)

    df_nyc = get_data("./data-sources/NYC MWBESearchResults.xls.xlsm", config["nyc"]["file_type"])
    df_sdvob = get_data("./data-sources/SDVOB_Listing_02_05_2022.xlsm", config["sdvob"]["file_type"])
    df_nys = get_data("./data-sources/NYS MWBE Directory_2022-02-05.xlsm", config["nys"]["file_type"])
    df_dbe = get_data("./data-sources/DBE_ Directory 2-5 Edited.xls.xlsm", config["dbe"]["file_type"])
    df_nj = get_data("./data-sources/NJ Datasource.xls", config["nj"]["file_type"])
    df_ma_acdbe = get_data("./data-sources/MA_ACDBE.xlsx", config["ma_acdbe"]["file_type"])
    df_ma_dbe = get_data("./data-sources/MA_DBE.xlsx", config["ma_dbe"]["file_type"])
    df_ma_mwpbe = get_data("./data-sources/MA_MWPBE.xlsx", config["ma_mwpbe"]["file_type"])

    # NYC: handling duplicate columns and mapping schema
    df_nyc = dup_cols_renamer(df_nyc)
    df_nyc = get_cleaned_df_nyc(df_nyc)
    df_nyc.rename(columns = config["nyc"]["schema_mapping"], inplace=True)

    # SDVOB: handling duplicate columns and mapping schema
    df_sdvob = dup_cols_renamer(df_sdvob)
    df_sdvob = get_cleaned_df_sdvob(df_sdvob)
    df_sdvob.rename(columns = config["sdvob"]["schema_mapping"], inplace=True)

    # NYS: handling duplicate columns and mapping schema
    df_nys = dup_cols_renamer(df_nys)
    df_nys = get_cleaned_df_nys(df_nys)
    df_nys.rename(columns = config["nys"]["schema_mapping"], inplace=True)

    # DBE: handling duplicate columns and mapping schema
    df_dbe = dup_cols_renamer(df_dbe)
    df_dbe = get_cleaned_df_dbe(df_dbe)
    df_dbe.rename(columns = config["dbe"]["schema_mapping"], inplace=True)

    # NJ: handling duplicate columns and mapping schema
    df_nj = dup_cols_renamer(df_nj)
    df_nj = get_cleaned_df_nj(df_nj)
    df_nj.rename(columns = config["nj"]["schema_mapping"], inplace=True)

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

    db_cols = set(list(df_sdvob.columns)+list(df_nyc.columns)+list(df_nys.columns)+list(df_dbe.columns)+list(df_nj.columns))
    sdvob_req_cols = list(db_cols - set(df_sdvob.columns))
    nyc_req_cols = list(db_cols - set(df_nyc.columns))
    nys_req_cols = list(db_cols - set(df_nys.columns))
    dbe_req_cols = list(db_cols - set(df_dbe.columns))
    nj_req_cols = list(db_cols - set(df_nj.columns))
    ma_acdbe_req_cols = list(db_cols - set(df_ma_acdbe.columns))
    ma_dbe_req_cols = list(db_cols - set(df_ma_dbe.columns))
    ma_mwpbe_req_cols = list(db_cols - set(df_ma_mwpbe.columns))

    df_sdvob[sdvob_req_cols] = np.nan
    df_nyc[nyc_req_cols] = np.nan
    df_nys[nys_req_cols] = np.nan
    df_dbe[dbe_req_cols] = np.nan
    df_nj[nj_req_cols] = np.nan
    df_ma_acdbe[ma_acdbe_req_cols] = np.nan
    df_ma_dbe[ma_dbe_req_cols] = np.nan
    df_ma_mwpbe[ma_mwpbe_req_cols] = np.nan



    df_union = pd.concat([df_nyc, df_sdvob, df_nys, df_dbe, df_nj,df_ma_acdbe, df_ma_dbe, df_ma_mwpbe])
    
    df_union["Phone"] = df_union["Phone"].apply(lambda x: parse_phone_numbers(x)) # cleaning phone numbers for more accurate de-duping
    df_union["Fax"] = df_union["Fax"].apply(lambda x: parse_phone_numbers(x)) # cleaning fax numbers for more accurate de-duping
    df_union["CompanyName"] = df_union["CompanyName"].apply(lambda x: clean_company_name(x)) # uppercase company names for more accurate de-duping
    df_union["Email"] = df_union["Email"].apply(lambda x: get_valid_email_address(x)) # validate correct email address syntax
    df_union["Website"] = df_union["Website"].apply(lambda x: get_cleaned_websites(x)) # validate correct websites syntax
    df_union["Zip"] = df_union["Zip"].apply(lambda x: get_clean_zips(x)) # get clean zip codes
    df_union["Zip1"] = df_union["Zip1"].apply(lambda x: get_clean_zips(x)) # get clean zip codes
    df_union["DateOfEstablishment"] = df_union["DateOfEstablishment"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["CertificationExpiry"] = df_union["CertificationExpiry"].apply(lambda x: get_date_from_timestamp(x))
    df_union["Date1"] = df_union["Date1"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["Date2"] = df_union["Date2"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["Date3"] = df_union["Date3"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["Date4"] = df_union["Date4"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["CertifiedDate"] = df_union["CertifiedDate"].apply(lambda x: get_date_from_timestamp(x)) 
    df_union["State"] = df_union["State"].apply(lambda x: get_states_maped(x, config["states_map"]))
    df_union["State1"] = df_union["State1"].apply(lambda x: get_states_maped(x, config["states_map"]))
    df_union["City"] = df_union["City"].apply(lambda x: clean_cities(x)) # remove numeric and unexpected characters from cities
    df_union["City1"] = df_union["City1"].apply(lambda x: clean_cities(x))
    df_union["EmailDomain"] = df_union["Email"].apply(lambda x: get_email_domain(x)) 
    df_union["WebsiteDomain"] = df_union["Website"].apply(lambda x: get_website_domain(x)) 
    df_union["DBANameTemp"] = df_union["DBAName"].apply(lambda x: get_clean_dba(x, "temp")) 
    df_union["DBAName"] = df_union["DBAName"].apply(lambda x: get_clean_dba(x)) 

    for attr in config["deduping_columns"]:
         df_union.update(df_union.groupby(attr).bfill())
         df_union.update(df_union.groupby(attr).ffill())

    # aggregate CertificationType field
    df_union_sub_p = get_certification_grouped("Phone", df_union)
    df_union_sub_c = get_certification_grouped("CompanyName", df_union)
    df_union_sub_e = get_certification_grouped("Email", df_union)
    df_union_sub_w = get_certification_grouped("Website", df_union)

    for attr in config["deduping_columns"]:
        df_union = df_union[(~df_union[attr].duplicated()) | df_union[attr].isna()]

    df_union = df_union.fillna("NULL") # replacing nulls

    # create Key col for generating a unique Id
    df_union["Key"] = df_union["CompanyName"] + df_union["Email"] + df_union["Phone"] + df_union["Website"]
    df_union["Id"] = df_union["Key"].apply(lambda x: create_id(x))

    df_union = df_union[df_union["CompanyName"] != ""] 

    # Merging certification types
    for attr, df, ctcol in zip(config["deduping_columns"], 
                        [df_union_sub_p, df_union_sub_c, df_union_sub_e, df_union_sub_w],
                        ["CertificationType_p", "CertificationType_c", "CertificationType_e", "CertificationType_w"]):
        df_union = pd.merge(df_union, df,  how="left", left_on=[attr], right_on = [attr])
        df_union[ctcol] = df_union[ctcol].fillna("").apply(list) # converting NaNs to empty list to avoid NULLs in CertificationType

    df_union["CertificationType"] = df_union["CertificationType_w"] + df_union["CertificationType_p"] + df_union["CertificationType_e"] + df_union["CertificationType_c"] 
    df_union["CertificationType"] = df_union["CertificationType"].apply(lambda x: get_unique_certification(x)) # get unique set of certification types
    df_union["CertificationType"] = df_union["CertificationType"].apply(lambda x: clean_certification_type(x)) 
    df_union["Website"] = df_union["Website"].apply(lambda x: hanlde_null_dupls(x)) # remove temporary 128 bit random chars to NULL
    df_union["Phone"] = df_union["Phone"].apply(lambda x: hanlde_null_dupls(x)) # remove temporary 128 bit random chars to NULL
    df_union["City"] = df_union["City"].apply(lambda x: hanlde_null_dupls(x)) # remove numeric and unexpected characters from cities
    df_union["City1"] = df_union["City1"].apply(lambda x: hanlde_null_dupls(x)) 
    df_union["DBAName"] = df_union["DBAName"].apply(lambda x: hanlde_null_dupls(x)) 
    

    for col in ["WebsiteDomain", "DBANameTemp"]:
        df_union = df_union.drop_duplicates(subset=[col])

    # remove temporary columns
    df_union.drop(["Key", 
                    "CertificationType_w", 
                    "CertificationType_p", 
                    "CertificationType_e", 
                    "CertificationType_c",
                    "EmailDomain",
                    "WebsiteDomain",
                    "DBANameTemp"], axis=1, inplace=True)

    df_union.to_csv("companies-new.tsv", sep="\t", index=False)
