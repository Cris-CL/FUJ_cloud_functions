import pandas as pd
from google.cloud import bigquery





def clean_txt(file):
    """function that takes a path to the txt file as an argument and returns
    a cleaned version of it as a pandas Dataframe"""
    pay_df = pd.read_table(file)
    new_pay = pay_df.iloc[4:,:].reset_index(drop=True)
    new_pay = new_pay[new_pay.columns[0]].str.split(',"', expand=True)
    for col in new_pay.columns:
        new_pay[col] = new_pay[col].apply(
            lambda x: x.replace('"',"")).apply(
                lambda x: None if x == "" else x)

    new_pay.columns =  new_pay.iloc[0]

    new_pay = new_pay.iloc[1:,:].reset_index(drop=True)
    new_pay.columns = [x.replace(",","").lower() for x in list(new_pay.columns)]
    cols = [
    "transactionamount",
    "transactionpercentagefee",
    "transactionfixedfee",
    "totaltransactionfee",
    "nettransactionamount"
        ]
    for num_col in cols:
        new_pay[num_col] = new_pay[num_col].apply(lambda x: x.replace(",","").replace(".00",""))


    new_pay = new_pay.astype({
        "transactionposteddate":"datetime64[ns]",

        "settlementid":"string",
        "amazontransactionid":"string",
        "sellerreferenceid":"string",
        "transactiontype":"string",
        "amazonorderreferenceid":"string",
        "sellerorderid":"string",
        "storename":"string",
        "currencycode":"string",
        "transactiondescription":"string",

        "transactionamount":"float64",
        "transactionpercentagefee":"float64",
        "transactionfixedfee":"float64",
        "totaltransactionfee":"float64",
        "nettransactionamount":"float64",
    })

    return new_pay


def upload_ama(df,table_id):
    dtypes_dict = {
        "object": "STRING",
        "string": "STRING",
        "int64": "INTEGER",
        "float64": "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        }

    job_config_ama = bigquery.LoadJobConfig(
        #### creates the schema with a list comprehension based on the data types of the df
        schema=[
            eval(
                f"bigquery.SchemaField('{col}', bigquery.enums.SqlTypeNames.{dtypes_dict[str(df[col].dtypes)]})"
            ) for col in df.columns
        ]
        ,write_disposition="WRITE_APPEND",)
    client_ama = bigquery.Client()
    job_ama = client_ama.load_table_from_dataframe(
    df, table_id, job_config=job_config_ama)  # Make an API request.
    job_ama.result()
    return
