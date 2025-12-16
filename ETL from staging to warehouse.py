# -*- coding: utf-8 -*-
"""
Created on Fri Nov 28 17:55:25 2025

@author: Ziad
"""
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import traceback

# ============================================================
# CONFIG
# ============================================================

db_name = 'EcommmerceDW'
engine = create_engine(
    f'mssql+pyodbc://sa:zezo450@localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&timeout=0', 
    fast_executemany=True
)

# ============================================================
# HELPERS
# ============================================================

def date_key(dt):
    if dt is None:
        return None
    return int(dt.strftime("%Y%m%d"))


def fetch_df(query):
    return pd.read_sql(text(query), engine)


def execute(query, params=None):
    with engine.begin() as conn:
        conn.execute(text(query), params or {})


def insert_df(df, table_name, schema="wh"):
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="append",
        index=False
    )


# ============================================================
# DIMENSION LOADERS
# ============================================================

def load_dim_customer():
    df = fetch_df("""
        SELECT DISTINCT
            src_customer_id,
            customer_name,
            phone,
            city,
            age,
            gender
        FROM staging.stg_customers
    """)
    insert_df(df, "dim_customer")
    print("Loaded dim_customer")


def load_dim_product():
    df = fetch_df("""
        SELECT DISTINCT
            src_product_id,
            product_name,
            category,
            company,
            product_page,
            price,
            rate,
            discount
        FROM staging.stg_products
    """)
    # FIX COLUMN NAMES TO MATCH WH TABLE
    df = df.rename(columns={
        "price": "current_price",
        "rate": "current_rate",
        "discount": "current_discount"
    })
    insert_df(df, "dim_product")
    print("Loaded dim_product")


def load_dim_payment():
    df = fetch_df("""
        SELECT DISTINCT
            payment_method
        FROM staging.stg_orders
    """)
    insert_df(df, "dim_payment")
    print("Loaded dim_payment")


def load_dim_courier():
    df = fetch_df("""
        SELECT DISTINCT
            courier
        FROM staging.stg_shipping
    """)
    df = df.rename(columns={
        "courier": "courier_name"
    })
    insert_df(df, "dim_courier")
    print("Loaded dim_courier")


def load_dim_shipping_status():
    df = fetch_df("""
        SELECT DISTINCT
            shipping_status
        FROM staging.stg_shipping
    """)
    df = df.rename(columns={
        "shipping_status": "status_name"
    })
    insert_df(df, "dim_shipping_status")
    print("Loaded dim_shipping_status")

def load_dim_date():
    print("Loading dim_date...")

    start_date = pd.to_datetime("2018-01-01")
    end_date = pd.to_datetime(datetime.today() + pd.DateOffset(years=1))

    # Generate date range
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    df = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "date": dates,
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "day": dates.day,
        "day_of_week": dates.dayofweek + 1,  # SQL Server weekday: 1=Monday
        "weekday_name": dates.strftime("%A"),
        "is_weekend": dates.dayofweek.isin([4, 5]).astype(int)  # Sat=5, Fri=4
    })

    # Insert into dim_date
    df.to_sql(
        "dim_date",
        engine,
        schema="wh",
        if_exists="append",
        index=False
    )

    print("Loaded dim_date")


# ============================================================
# FACT: ORDER ITEMS
# ============================================================

def load_fact_order_items():
    print("Loading fact_order_items...")

    df_items = fetch_df("SELECT * FROM staging.stg_order_items")
    df_orders = fetch_df("SELECT * FROM staging.stg_orders")
    df_ship = fetch_df("SELECT * FROM staging.stg_shipping")

    # Dimensions
    dim_customer = fetch_df("SELECT customer_sk, src_customer_id FROM wh.dim_customer")
    dim_product = fetch_df("SELECT product_sk, src_product_id FROM wh.dim_product")
    dim_payment = fetch_df("SELECT payment_sk, payment_method FROM wh.dim_payment")
    dim_courier = fetch_df("SELECT courier_sk, courier_name FROM wh.dim_courier")
    dim_status = fetch_df("SELECT status_sk, status_name FROM wh.dim_shipping_status")

    # Merge (Lookups)
    df = df_items.merge(df_orders, on="src_order_id", how="left")
    df = df.merge(df_ship, on="src_order_id", how="left")

    df = df.merge(dim_customer, left_on="src_customer_id_x", right_on="src_customer_id", how="left")
    df = df.merge(dim_product, on="src_product_id", how="left")
    df = df.merge(dim_payment, on="payment_method", how="left")
    df = df.merge(dim_courier, left_on="courier", right_on="courier_name", how="left")
    df = df.merge(dim_status, left_on="shipping_status", right_on="status_name", how="left")

    # Convert dates
    df["order_date_key"] = df["order_date"].apply(date_key)
    df["shipped_date_key"] = df["shipped_date"].apply(date_key)
    df["delivery_date_key"] = df["delivery_date"].apply(date_key)

    # Final DataFrame
    fact = pd.DataFrame({
        "src_order_item_id": df["src_order_item_id"],
        "order_id": df["src_order_id"],
        "order_date_key": df["order_date_key"],
        "shipped_date_key": df["shipped_date_key"],
        "delivery_date_key": df["delivery_date_key"],
        "customer_sk": df["customer_sk"],
        "product_sk": df["product_sk"],
        "payment_sk": df["payment_sk"],
        "courier_sk": df["courier_sk"],
        "shipping_status_sk": df["status_sk"],
        "quantity": df["quantity"],
        "price": df["price"],
        "total_price": df["quantity"] * df["price"]
    })

    insert_df(fact, "fact_order_items")
    print("Loaded fact_order_items")


# ============================================================
# FACT: RETURNS
# ============================================================

def load_fact_returns():
    print("Loading fact_returns...")

    df_ret = fetch_df("SELECT * FROM staging.stg_returns")

    # 🔥 Add missing src_product_id from order_items
    df_items = fetch_df("SELECT src_order_item_id, src_product_id, src_order_id FROM staging.stg_order_items")
    df_ret = df_ret.merge(df_items, on="src_order_item_id", how="left")

    # Load dims
    dim_customer = fetch_df("SELECT customer_sk, src_customer_id FROM wh.dim_customer")
    dim_product = fetch_df("SELECT product_sk, src_product_id FROM wh.dim_product")
    fact_items = fetch_df("SELECT order_item_sk, order_id, product_sk FROM wh.fact_order_items")

    # Lookups
    df = df_ret.merge(dim_customer, on="src_customer_id", how="left")
    print(df.columns)
    df = df.merge(dim_product, on="src_product_id", how="left")

    # Lookup order_item_sk
    df = df.merge(
        fact_items,
        left_on=["src_order_id", "product_sk"],
        right_on=["order_id", "product_sk"],
        how="left"
    )

    df["return_date_key"] = df["return_date"].apply(date_key)

    fact = pd.DataFrame({
        "src_return_id": df["src_return_id"],
        "src_order_item_id ": df["src_order_item_id"],
        "return_date_key": df["return_date_key"],
        "customer_sk": df["customer_sk"],
        "product_sk": df["product_sk"],
        "refund_amount": df["refund_amount"],
        "reason": df["reason"]
    })

    insert_df(fact, "fact_returns")
    print("Loaded fact_returns")


# ============================================================
# RUN ETL
# ============================================================

def run_etl():
    try:
        print("Starting ETL...")

        load_dim_customer()
        load_dim_product()
        load_dim_payment()
        load_dim_courier()
        load_dim_shipping_status()
        load_dim_date()
        
        load_fact_order_items()
        load_fact_returns()

        print("ETL completed successfully.")

    except Exception as e:
        print("ETL FAILED")
        print(str(e))
        traceback.print_exc()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    run_etl()
