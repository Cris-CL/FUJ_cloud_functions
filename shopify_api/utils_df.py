import pandas as pd
from orders_parameters import dict_types,deprecated

def clean_row(row):
    """
    Cleans a row from a dataframe, removing unwanted characters

    Parameters:
    - row (str): A string that contains unwanted characters
    Returns:
    - row (str): The same string, but without unwanted characters
    """
    signs = "{}[]'"
    row = str(row)
    for sign in signs:
        row = row.replace(sign, "").replace("\n", "")
    return row


def discount_allocations(item):
    """
    Iterates through a list of dictionaries, and returns the sum of the amount field

    Parameters:
    - item (list): A list of dictionaries

    Returns:
    - amount_count (int): The sum of the amount field in the list of dictionaries
    """
    amount_count = 0
    for value in item:
        if isinstance(value, dict):
            amount_count += int(value.get("amount", 0))
        else:
            continue
    return amount_count


def line_map(df):
    """
    Expands the line_items column into multiple columns, and cleans the shipping amounts

    Takes a dataframe with the column line_items that came from the shopify orders api
    and expands the dataframe to create new rows depending on the amount of items on each
    order, after that process the relevant columns inside the line_items columnsba

    Parameters:
    - df (pd.DataFrame): A dataframe with a column called line_items

    Returns:
    - df (pd.DataFrame): The same dataframe, but with the line_items column expanded into multiple columns

    """
    ## expand lines for each product
    df = df.explode("line_items").reset_index(drop=True)

    ## Split each part of the line_items col
    df["lineitem_quantity"] = df["line_items"].apply(
        lambda x: x["quantity"] if len(x) > 0 else None
    )
    df["lineitem_name"] = df["line_items"].apply(
        lambda x: x["name"] if len(x) > 0 else None
    )
    df["lineitem_price"] = df["line_items"].apply(
        lambda x: x["price"] if len(x) > 0 else None
    )
    df["lineitem_sku"] = df["line_items"].apply(
        lambda x: x["sku"] if len(x) > 0 else None
    )
    #### discount allocations for each individual product
    df["lineitem_discount"] = df["line_items"].apply(
        lambda x: discount_allocations(x["discount_allocations"])
    )
    ## Cleaning the shipping amounts
    df.columns = [
        col.replace("total_shipping_price_set.", "shipping_") for col in df.columns
    ]
    df.drop(
        columns=[
            "line_items",
            "shipping_shop_money.currency_code",
            "shipping_presentment_money.currency_code",
        ],
        inplace=True,
    )
    df.columns = [col.replace("money.", "") for col in df.columns]
    df.columns = [col.replace("shipping_address.", "ship_") for col in df.columns]
    return df


def discount_process(df):
    """
    Extracts information from the discount_codes column, creating new columns

    Parameters:
    - df (pd.DataFrame): A dataframe with a column called discount_codes

    Returns:
    - df (pd.DataFrame): The same dataframe, but with new columns extracted from the discount_codes column

    """

    df["discount_code"] = df["discount_codes"].apply(
        lambda x: x[0]["code"] if len(x) > 0 else None
    )
    df["discount_amount"] = df["discount_codes"].apply(
        lambda x: x[0]["amount"] if len(x) > 0 else None
    )
    df["discount_type"] = df["discount_codes"].apply(
        lambda x: x[0]["type"] if len(x) > 0 else None
    )

    # clean note_attributes and gateway_names row
    df["note_attributes"] = df["note_attributes"].apply(
        lambda x: clean_row(x) if len(x) > 0 else None
    )
    df["note"] = df["note"].apply(lambda x: str(x))
    df["payment_gateway_names"] = df["payment_gateway_names"].apply(
        lambda x: clean_row(x) if len(x) > 0 else None
    )
    df.drop(columns="discount_codes", inplace=True)
    return df


def refund_clean(val):
    """
    Iterates through a list of dictionaries, and returns the sum of the subtotal field

    Parameters:
    val (list): A list of dictionaries

    Returns:
    tmp (int): The sum of the subtotal field in the list of dictionaries
    """

    tmp = 0
    for x in val:
        if isinstance(x, list):
            continue
        elif isinstance(x, dict):
            if len(x.get("refund_line_items", 0)) < 1:
                continue
            tmp += sum(
                [
                    float(part.get("subtotal", 0))
                    for part in x.get("refund_line_items", {})
                ]
            )
    return tmp


def refunds(df):
    """
    Extracts information from the refunds column, creating a new column and dropping the original one

    Parameters:
    - df (pd.DataFrame): A dataframe with a column called refunds

    Returns:
    - df (pd.DataFrame): The same dataframe, but with a new column extracted from the refunds column
    """

    df["refund"] = df["refunds"].map(lambda x: refund_clean(x) if len(x) > 0 else 0)
    df.drop(columns="refunds", inplace=True)
    return df


def type_change(df):
    """ "
    Changes the type of the columns in the dataframe, and returns the processed dataframe

    This function changes the type of the columns in the dataframe to the
    correct type to allow the data to be uploaded to BigQuery

    Parameters:
    - df (pd.DataFrame): A dataframe with all the orders from the shopify store
    Returns:
    - df (pd.DataFrame): The same dataframe, but with the columns changed to the correct type

    """

    for cols_db in dict_types.keys():
        if cols_db not in df.columns:
            if cols_db in deprecated:
                df[cols_db] = "DEPRECATED"
            else:
                df[cols_db] = None

    df = df.astype(dict_types)
    ## Delete nan strings or empty values
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None
            if isinstance(x, str)
            and x.lower() in ["nan", "none", "null", "na", "<na>", "<nan>", ""]
            else x
        )
        if col not in dict_types.keys():
            ##### dropping columns that shouldnt appear
            df.drop(
                columns=[col],
                inplace=True,
            )
    df["checkout_id"] = df["checkout_id"].apply(
        lambda x: str(int(float(x))) if isinstance(x, str) else x
    )
    return df.copy()
