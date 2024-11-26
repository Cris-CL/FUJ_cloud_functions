from columns_names import *
import pandas as pd
from datetime import datetime


def format_rakuten(df):

    for col in df.columns:
       df[col] = df[col].astype(
           {f"{col}":f"{dict_col.get(col,'str')}"}
           )
       df[col] = df[col].apply(
           lambda x: None if x in ["nan","null","none"] else x
               )
    return df


def load_rak_orders_df(uri,file_name):
    try:
        df = pd.read_csv(uri,encoding="cp932")
    except:
        df = pd.read_csv(uri)
   ######## columns verification
    df.columns = [columns_names_translation.get(col) for col in df.columns] ### rename columns based on the dictionary
    df = df[columns_names_translation.values()] #### Only use columns that are in the dictionary

    df["UPDATED_AT_UTC"] = datetime.now()
    df["FILE_NAME"] = file_name
    df = format_rakuten(df)
    return df
