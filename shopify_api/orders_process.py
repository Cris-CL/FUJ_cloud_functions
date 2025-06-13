import pandas as pd
import requests
from orders_parameters import *


def get_all_orders(last_order_id):
    """
    Gets all the orders from the shopify store, and returns a dataframe

    Using the shopify api, loops throught all the orders starting from
    last_order_id, and returns a pandas dataframe with all the orders parameters
    contained in the fields variable

    Parameters:
    - last_order_id (int): The last order id that was registered in the database

    Returns:
    - orders (pd.DataFrame): A dataframe with all the orders from the shopify store
    """

    last = last_order_id  ##first order_id 2270011949127

    limit = 250
    status = "any"
    orders = pd.DataFrame()
    fields = ",".join(fields_api)

    print(f"First order_id: {last}")
    while True:
        url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json?limit={limit}&fields={fields}&status={status}&since_id={last}"
        response_in = requests.get(url)
        try:
            df = pd.json_normalize(response_in.json()["orders"])
        except Exception as e:
            print("get_all_orders failed with error:")
            print(e, type(e))
            return pd.DataFrame()

        # Function finishes with no new info
        if len(df) < 1:
            print("Api didnt provide new data")
            return last

        orders = pd.concat([orders, df], ignore_index=True)
        last = df["id"].iloc[-1]

        if len(df) < limit:
            print(f"Last order_id: {last}")
            print(len(orders))
            break
    return orders


def get_transactions(id_list,type_id):
    """
    Gets all the transactions from the shopify store, and returns a dataframe

    Using the shopify api, loops throught all the transactions with the order_id
    in the id_list that correspond to the orders processed in stripe/komoju,
    and returns a pandas dataframe with all the transactions parameters

    Parameters:
    - id_list (set): A list with all the order ids that were processed in stripe/komoju
    - type_id (string): Selector for stripe or komoju.

    Returns:
    - trans_filter (pd.DataFrame): A dataframe with all the stripe/komoju transactions
    """

    transactions = pd.DataFrame()
    for order_id in set(id_list):

        url = f"https://{SHOPIFY_KEY}:{SHOPIFY_PASS}@{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/orders/{order_id}/transactions.json"
        response_in = requests.get(url)
        df = pd.json_normalize(response_in.json()["transactions"][0])
        for row in df.iterrows():
            if type_id == "stripe":
                for key in row[1].keys():
                    if isinstance(row[1][key], str) and "ch_" in row[1][key]:
                        df.loc[row[0], "CHARGE_ID_CORRECT"] = row[1][key]
                        continue
            elif type_id == "komoju":
                df[ "CHARGE_ID_CORRECT"] = row[1]["payment_id"]
        transactions = pd.concat([transactions, df], ignore_index=True)
    trans_filter = transactions[["order_id", "CHARGE_ID_CORRECT"]]
    return trans_filter


def join_orders_transactions(orders_df, transactions_df):
    """
    Join the orders dataframe with the transactions dataframe, and returns a new dataframe

    After getting the orders and transactions dataframes, this function merges them
    so the orders df has the charge_id from the transactions df for the stripe orders

    Parameters:
    - orders_df (pd.DataFrame): A dataframe with all the orders from the shopify store
    - transactions_df (pd.DataFrame): A dataframe with all the stripe transactions

    Returns:
    - new_df (pd.DataFrame): A dataframe with all the orders from the shopify store, with the charge_id for the stripe orders

    """

    transactions_df = transactions_df.astype({"order_id": "string"})
    try:
        new_df = pd.merge(
            orders_df,
            transactions_df,
            left_on="id",
            right_on="order_id",
            how="left",
            validate="many_to_one",
        )
        new_df["reference"] = new_df["reference"].fillna(new_df["CHARGE_ID_CORRECT"])
        new_df = new_df.drop(columns=["order_id", "CHARGE_ID_CORRECT"])

        assert new_df.shape == orders_df.shape

    except Exception as err:
        print("Error join_orders_transactions function")
        print(f"Unexpected {err=}, {type(err)=}")
        print(new_df.shape, orders_df.shape)
        new_df = orders_df

    return new_df


def other_payments_process(df):
    """
    Process the stripe orders, and returns the processed dataframe

    Parameters:
    - df (pd.DataFrame): A dataframe with all the orders from the shopify store

    Returns:
    - df (pd.DataFrame): The same dataframe, but with the stripe/komoju orders processed
    """

    try:
        stripe_list = set(df[df["payment_gateway_names"] == "stripe"]["id"])
        komoju_list = set(df[df["payment_gateway_names"] == 'KOMOJU - スマホ決済 (Smartphone Payments)']["id"])
        if len(stripe_list) < 1 and len(komoju_list) < 1:
            print("No stripe or komoju orders")
            return df
        if len(stripe_list) > 1:
            transactions_stripe = get_transactions(stripe_list,"stripe")
            df = join_orders_transactions(df, transactions_stripe)
        if len(komoju_list) > 1:
            transactions_komoju = get_transactions(komoju_list,"komoju")
            df = join_orders_transactions(df, transactions_komoju)
    except Exception as e:
        print(e, type(e))
        print("Stripe process failed")
    return df.copy()
