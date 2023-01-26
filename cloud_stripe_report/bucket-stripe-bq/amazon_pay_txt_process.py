import pandas as pd

dtypes_dict = {
        "object": "STRING",
        "int64": "INTEGER",
        "float64": "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        }


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

    new_pay = new_pay.iloc[4:,:].reset_index(drop=True)
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
