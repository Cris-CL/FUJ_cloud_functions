
def get_fulfilment(row):
    try:
        return row["fulfillment_status"]
    except:
        return None
    return None

def get_name(row):
    try:
        return row["name"]
    except:
        return None
    return None

def get_sku(row):
    try:
        return row["sku"]
    except:
        return None
    return None

def get_price(row):
    try:
        return row["price"]
    except:
        return None
    return None

def get_discount(row):
    try:
        return row["total_discount"]
    except:
        return None
    return None

def get_shipp(col):
    try:
        return col["shop_money"]["amount"]
    except:
        return 0
