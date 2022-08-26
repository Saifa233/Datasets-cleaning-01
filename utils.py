import re
import hashlib
import math
import validators
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import random
import string


def get_data(filepath, filetype):
    '''
        Read excel, csv or other file formats and return as Dataframes
    '''
    if filetype == "excel":
        df =  pd.read_excel(filepath, dtype=str)
        return df
    elif filetype == "html":
        # handle NJ source
        data =  pd.read_html(filepath)
        df = data[0]
        df.columns = df.iloc[0]
        df = df[1:]
        return df


def clean_nan(x):
    '''
        Clean NaN string to empty string
    '''
    if str(x) == "nan":
        return ""
    return str(x)


def get_clean_dba(x, col_type=None):
    '''
        return cleaned DBA names if exists
    '''
    dba = get_lower_case(x)
    if not dba:
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))
    is_alpa = any(c.isalpha() for c in dba)
    if is_alpa and col_type == "temp":
        return dba
    elif is_alpa and not col_type:
        return get_title_case(dba)

    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))


def get_lower_case(x):
    '''
        return string in lower case else prepare for NULL
    '''
    if str(x) == "nan":
        return None
    return str(x).lower()


def get_title_case(x):
    '''
        return string in title case else prepare for NULL
    '''
    if str(x) == "nan":
        return None
    return str(x).title()

def clean_cities(x):
    '''
        Clean NaN string to empty string
    '''
    if str(x) == "nan":
        return ""
    
    if bool(re.search(r"[0-9]+", str(x))):
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))

    return str("".join(e for e in x if e.isalnum())).title() # remove special chars 


def create_id(x):
    '''
        Create a 12 digit ID from combining the following fields
        ["CompanyName", "Email", "Phone_cleaned", "Website"]
    '''
    return hashlib.sha256(x.encode("utf-8")).hexdigest()[:12] 

def dup_cols_renamer(df):
    '''
        Rename colums in case of duplicate columns. 
    '''
    df_columns = df.columns
    new_columns = [i.replace(".", "") for i in df_columns]
    df.columns = new_columns
    return df
    

def parse_phone_numbers(x):
    '''
        Parse only digit from the phone numbers.
        Used only for cleaning.
    '''
    try:
        number = re.findall(r"\d+", x)
        if len(number) == 0:
            return None
        
        number = "".join(number)
        return number[:3] + "-" + number[3:6] + "-" + number[6:10] 
    except TypeError:
        return x


def clean_company_name(x):
    '''
        Clean company names for standard 
        string matching
    '''
    x = str(x).upper()
    if " DBA " in x:
        index = x.index(" DBA ")
        x = x[:index]    
    return x


def get_clean_zips(x):
    '''
        returns clean zip codes
    '''
    try:
        x = x.strip()
        if ("-" in x) and (len(x) > 6):
            return x 
        elif len(x) < 5:
            return None
        return x 
    except:
        return x


def get_valid_email_address(x):
    '''
        Validates email address so we don't have crap 
        in the database
    '''
    try:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", x):
            return
        return x
    except TypeError:
        return x


def get_states_maped(x, mapping):
    '''
        returns states with 2 character 
        for e.g. NY, LA
    '''
    if str(x) == "nan":
        return x
    if len(x) == 2:
        return x
    else:
        if x in mapping:
            return mapping[x]
        return None


def get_cleaned_websites(x):
    '''
        returns cleaned websites which startswith http.
        If not a valid website then return temporary 
        random string with 128 bits to avoid bad backfilling
    '''
    if str(x) == "nan" or x == None or x == "":
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))

    if x == "https://" or x == "http://":
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))

    x = x.strip()
    # handle empty spaces
    if x == "": 
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))

    if x.startswith("https://") or x.startswith("http://"):
        x = get_valid_url(x)
    else:
        x = "http://" + x
    if x is not None:
        return str(x).lower()
    else:
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))


def get_email_domain(x):
    '''
        Extract email domain excluding
    '''
    if str(x) == "nan" or x == None or x == "":
        return None
    
    domain = x.split("@")[-1]
    return domain


def get_website_domain(x):
    '''
        Extract website real domain excluding www.
    '''
    if str(x) == "nan" or x == None or x == "":
        return None
    try:
        domain = re.findall(r"https?://(www\.)?([A-Za-z_0-9.-]+).*", x)
        return domain[0][1]
    except:
        return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(128))


def hanlde_null_dupls(x):
    '''
        replace temporary 128 chars with NULL
    '''
    if len(x) == 128:
        return "NULL"
    return x


def get_valid_url(x):
    '''
        validate url
    '''
    if not validators.url(x):
        return None
    return x


def get_unique_certification(x):
    '''
        remove duplicate certifications from different
        sources.
    '''
    return list(set(x))


def replace_regions_sdvob(x):
    if x == "Central New York":
        return "Central NY"
    elif x == "New York City":
        return "NYC"
    elif x == "Out-Of-State":
        return "Out of State"
    elif x == "Western New York":
        return "Western NY"
    
    return x


def replace_classifications_sdvob(x):
    if x in [
        "Commodities -- Construction",
        "Commodities -- Construction -- Consulting & Other Services",
        "Commodities -- Construction Professional Services",
        "Commodities -- Construction Professional Services -- Consulting & Other Services",
        "Commodities -- Consulting & Other Services",
    ]:
        return "Commodities"
    elif x in [
        "Construction -- Construction Professional Services",
        "Construction -- Construction Professional Services -- Consulting & Other Services",
        "Construction -- Consulting & Other Services",
    ]:
        return "Construction"
    elif x in [
        "Construction Professional Services",
        "Construction Professional Services -- Consulting & Other Services",
    ]:
        return "Construction Consultants"
    elif x == "Consulting & Other Services":
        return "Services Consultants"
    
    return x


def rename_centification_type(x, old, new):
    '''
        return new alias for certificationtype
        based on specs
    '''
    try:
        return x.replace(old, new)
    except:
        return x


def replace_certificationtype_nj(x):
    '''
        return certification type for New Jersey
    '''
    if x == "DVOB": return "SDVOB-NJ"
    elif x == "MBE": return "MBE-NJ"
    elif x == "MWBE": return "MBE-NJ, WBE-NJ"
    elif x == "SBE": return "SBE-NJ"
    elif x == "VOB": return "VOB-NJ"
    elif x == "WBE": return "WBE-NJ"
    else: return x


def clean_certification_type(x):
    '''
        clean certification type by removing duplicates
        and removing special chars if any
    '''
    if x is None or x == "" or x == []:
        return x
    certs = []
    for el in x:
        els = el.split(",")
        for s_el in els:
            cert = str(re.sub("[^A-Z0-]+", " ", s_el)).strip()
            certs.append(cert)
    return list(set(certs))


def get_industries_nj(x):
    '''
        return industry based on annual gross sales revenue
    '''
    if x == "Cat 4" or x == "Cat 5" or x == "Cat 6":
        return "Construction"
        
    return None


def replace_businesssize_nj(x):
    '''
        return business size for New Jersery
    '''
    if x == "Cat1 (under $500,000)": return "$100,000 - $499,000"
    elif x == "Cat2 (from $500,000+ to $5M)": return " $1,000,000 - $4,999,999"
    elif x == "Cat3 (Under $12M or Fed. Std.)": return "$1,000,000 - $4,999,999"
    elif x == "Cat4 (under $3M)": return "Over $5,000,000"
    elif x == "Cat5(Under 50% of Anl Fed Std)": return "Over $5,000,000"
    elif x == "Cat6 (Under Ann Fed Rev Std)": return "Over $5,000,000"
    elif x == "Cat1 & Cat4 (G & S/Const Comb)": return "$1,000,000 - $4,999,999"
    elif x == "Cat2 & Cat4 (G & S/Const Comb)": return "$1,000,000 - $4,999,999"
    elif x == "Cat2 & Cat5 (G & S/Const Comb)": return "Over $5,000,000"
    elif x == "Cat3 & Cat5 (G & S/Const Comb)": return "Over $5,000,000"
    elif x == "Cat3 & Cat6 (G&S/Const Comb)": return "Over $5,000,000"
    
    else: return x


def get_date_from_timestamp(x):
    '''
        return date from timestamp
    '''
    try:
        date = parse(x, fuzzy=True)
        return date.date()
    except:
        if ";" in str(x):
            date = parse(x.split(";")[0], fuzzy=True)
            return date.date()
        return x


def remove_parathesis(x):
    '''
        return string without paranthesis
    '''
    try:
        return re.sub(r'[()]', '', x)
    except:
        return x


def get_cleaned_df_sdvob(df):
    '''
        Does cleaning and creates new columns for SDVOB
        dataset based on provided specifications
    '''
    df["Agency"] = "NYS"
    df["CertificationType"] = "SDVOB"
    df.drop(["ControlNumber",
            "NAICS Code(s)",
            "NYS Vendor ID Number"], axis=1, inplace=True)
    
    # split the name columns
    df[["OwnerFirst", "OwnerLast"]] = df["Primary SDV Name"].str.split(",", 1, expand=True)
    # replacing old regions to new ones
    df["Home Region"] = df["Home Region"].apply(lambda x: replace_regions_sdvob(x))
    # replacing classifications to new ones
    df["Classification"] = df["Classification"].apply(lambda x: replace_classifications_sdvob(x))

    df ["Capability"] = df["Categories"] + "|" + df["Specific Function"] + "|" + df["Key Words"]

    df.drop(["Primary SDV Name",
            "Specific Function",
            "Key Words"], axis=1, inplace=True)
    return df


def get_cleaned_df_nyc(df):
    '''
        Does cleaning and creates new columns for NYC
        dataset based on provided specifications
    '''
    df["Agency"] = "NYC"

    # split the name columns
    df[["OwnerFirst", "OwnerLast"]] = df["Contact Name"].str.split(n=1, expand=True)
    # combine street address
    df["PhysicalAddress"] = df["Address Line 1"].apply(lambda x: clean_nan(x)) + ", " + df["Address Line 2"].apply(lambda x: clean_nan(x)) 
    df["PhysicalAddress"] = df["PhysicalAddress"].apply(lambda x: clean_comma_suffix(x))
    df["MailingAddress"] = df["MailingAddress1"].apply(lambda x: clean_nan(x)) + ", " + df["MailingAddress2"].apply(lambda x: clean_nan(x))
    df["MailingAddress"] = df["MailingAddress"].apply(lambda x: clean_comma_suffix(x))

    df["Vendor DBA"] = df["Vendor DBA"].apply(lambda x: remove_parathesis(x))
    df["Certification"] = df["Certification"].apply(lambda x: rename_centification_type(x, "WBE", "WBE - NYC"))
    df["Certification"] = df["Certification"].apply(lambda x: rename_centification_type(x, "MBE", "MBE - NYC")) 

    df["About"] = df["Business Description"]
    df["Capability"] = df["Business Description"].astype(str) + "|" + df["Types of Construction Projects Performed"].astype(str)

    df.drop(["Address Line 1",	
            "Address Line 2",	
            "Contact Name",
            "Business Description",
            "Goods/Materials Supplier with No Installation", 
            "Types of Construction Projects Performed"], axis=1, inplace=True)

    return df


def get_cleaned_df_nys(df):
    '''
        Does cleaning and creates new columns for NYS
        dataset based on provided specifications
    '''
    df["Certification Type"] = df["Certification Type"].apply(lambda x: rename_centification_type(x, "WBE", "WBE - NYS"))
    df["Certification Type"] = df["Certification Type"].apply(lambda x: rename_centification_type(x, "MBE", "MBE - NYS")) 

    return df


def get_cleaned_df_dbe(df):
    '''
        Does cleaning and creates new columns for DBE
        dataset based on provided specifications
    '''
    df ["Capability"] = df["Capability"].astype(str) + "|" + df["Commodity Codes"].astype(str)
    df["Certification Type"] = df["Certification Type"].astype(str) + " - " + df["Agency"].astype(str)

    df.drop(["Commodity Codes"], axis=1, inplace=True)
    return df


def get_cleaned_df_nj(df):
    '''
        Does cleaning and creates new columns for New Jersey
        dataset based on provided specifications
    '''
    df["Agency"] = "NJ"
    df["Designation"] = df["Designation"].apply(lambda x: replace_certificationtype_nj(x))
    df["BusinessSize"] = df["Gross Sale Revnue"].apply(lambda x: replace_businesssize_nj(x))
    df["PhysicalAddress"] = df["Business Address 1"].apply(lambda x: clean_nan(x)) + ', ' + df["Business Address 2"].apply(lambda x: clean_nan(x)) 
    df["PhysicalAddress"] = df["PhysicalAddress"].apply(lambda x: clean_comma_suffix(x))
    df["Industry"] = df["Gross Sale Revnue"].apply(lambda x: get_industries_nj(x))

    df.drop(["Status", 
            "Commodity/Construction Craft Codes", 
            "Gross Sale Revnue", 
            "Business Address 1", 
            "Business Address 2"], axis=1, inplace=True)
    return df


def clean_comma_suffix(x):
    '''
        remove comma suffix from string
    '''
    if str(x).endswith(", "): 
        return x[:-2]
    else: 
        return x



def get_certification_grouped(attr, df):
    '''
        return df with certification grouped and 
        separated by a comma.
    '''
    df = df[[attr, "CertificationType"]]
    df = df.groupby(attr)["CertificationType"].apply(list).to_frame()

    if attr == "Phone":
        df.rename(columns = {"CertificationType": "CertificationType_p"}, inplace=True)
    elif attr == "CompanyName":
        df.rename(columns = {"CertificationType": "CertificationType_c"}, inplace=True)
    elif attr == "Email":
        df.rename(columns = {"CertificationType": "CertificationType_e"}, inplace=True)
    elif attr == "Website":
        df.rename(columns = {"CertificationType": "CertificationType_w"}, inplace=True)

    return df

def get_cleaned_df_ma_acdbe(df):
    df["Agency"] = "MA"
    df["CertificationType"] = "DBE"
    df["About"] = df["Description of Services"]
    df[["NAICS Description","Description of Services"]] = df[["NAICS Description","Description of Services"]].fillna('')
    df["Capability"] = df["NAICS Description"] + ' ' + df["Description of Services"] 
    df.drop("NAICS Description",inplace = True, axis = 1)
    df.drop("Description of Services",inplace = True, axis = 1)
    return df

def get_cleaned_df_ma_dbe(df):
    df["Agency"] = "MA"
    df["CertificationType"] = "DBE"
    df["About"] = df["Description of Services"]
    df[["NAICS Description","Description of Services"]] = df[["NAICS Description","Description of Services"]].fillna('')
    df["Capability"] = df["NAICS Description"] + ' ' + df["Description of Services"] 
    df.drop("NAICS Description",inplace = True, axis = 1)
    df.drop("Description of Services",inplace = True, axis = 1)
    return df
    return df

def get_cleaned_df_ma_mwpbe(df):
    df["Agency"] = "MA"
    df['CertificationType'] = df.apply(lambda row: categorise(row), axis=1)
    df["About"] = df["Description of Services"]
    df[["Product Code","Description of Services"]] = df[["Product Code","Description of Services"]].fillna('')
    df["Capability"] = df["Product Code"] + ' ' + df["Description of Services"] 
    df.drop("Product Code",inplace = True, axis = 1)
    df.drop("Description of Services",inplace = True, axis = 1)
    df.drop("MBE - Y/N",inplace = True, axis = 1)
    df.drop("WBE - Y/N",inplace = True, axis = 1)
    df.drop("PBE - Y/N",inplace = True, axis = 1)
    df.drop("Non Profit - Y/N",inplace = True, axis = 1)
    return df

def categorise(row):  
    if row["MBE - Y/N"] == "Y":
        return "MBE"
    elif row["WBE - Y/N"] == "Y":
        return "WBE"
    elif row["PBE - Y/N"] == "Y":
        return "PBE"
    return ""