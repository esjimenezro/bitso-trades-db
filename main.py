import pandas as pd
import requests
import sqlite3
import time


def get_bitso_trades():
    """
    This function gets the publicly available bitso trades and returns them in a DataFrame.

    :return: pd.DataFrame with the bitso trades.
    """
    # Get request
    response = requests.get("https://api.bitso.com/v3/trades/?book=btc_mxn")
    data = response.json().get("payload")

    # To data frame
    df = pd.DataFrame(data)
    return df


def update_db(db_conn, table, data_df):
    # 1. Create table if it doesn't exist
    c = db_conn.cursor()
    c.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {table} (
                book VARCHAR(20) NOT NULL,
                created_at VARCHAR(50) NOT NULL,
                amount FLOAT NOT NULL,
                maker_side VARCHAR(5),
                price FLOAT NOT NULL,
                tid INTEGER PRIMARY KEY NOT NULL);
        """)

    # 2. Insert data into the bitso_trades table. Update values that were already present.
    # Insert
    insert_query = f"""
        INSERT INTO {table}
            (book,created_at,amount,maker_side,price,tid)
            VALUES 
    """
    # Values
    values_query = ",".join([
        f"""
            ('{row.book}','{row.created_at}','{row.amount}','{row.maker_side}','{row.price}','{row.tid}')
        """
        for _, row in data_df.iterrows()
    ])
    # Not duplicates
    not_duplicates_query = """ 
        ON CONFLICT(tid) DO UPDATE SET
            book = excluded.book,
            created_at = excluded.created_at,
            amount = excluded.amount,
            maker_side = excluded.maker_side,
            price = excluded.price,
            tid = excluded.tid;
    """
    # Final query
    query = insert_query + values_query + not_duplicates_query
    # Execute query
    c.execute(query)
    conn.commit()


if __name__ == "__main__":
    # Set-up
    database_name = "bitso_api.db"
    table_name = "bitso_trades"
    period = 10
    with sqlite3.connect(database_name) as conn:
        while True:
            # Get bitso trades as data frame
            bitso_trades_df = get_bitso_trades()
            # Insert data into DB
            update_db(db_conn=conn, table=table_name, data_df=bitso_trades_df)
            time.sleep(period)






