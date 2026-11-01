import psycopg2
import os
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

def get_connection():

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "kafka_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )

conn = get_connection()
cursor = conn.cursor()

try:
    print("==================================================\n"
    "COMMODITY MARKET REPORT\n"
    "==================================================")

    query_gold = """
    SELECT
    COUNT(*) as count,
    AVG(price) as avg,
    MIN(price) as min,
    MAX(price) as max
    FROM commodity_prices
    WHERE commodity = 'gold';
    """

    df_gold = pd.read_sql(query_gold, conn)

    print("\nGOLD\n"
    "----------------------------------------")

    gold_events = df_gold["count"][0]
    gold_avg = df_gold["avg"][0]
    gold_min = df_gold["min"][0]
    gold_max = df_gold["max"][0]

    print(f"Events: {gold_events}")
    print(f"Average Price: {gold_avg:.2f}")
    print(f"Highest Price: {gold_max}")
    print(f"Lowest Price: {gold_min}")


    query_silver = """
    SELECT
    COUNT(*) as count,
    AVG(price) as avg,
    MIN(price) as min,
    MAX(price) as max
    FROM commodity_prices
    WHERE commodity = 'silver';
    """

    df_silver = pd.read_sql(query_silver, conn)

    print("\nSILVER\n"
    "----------------------------------------")

    silver_events = df_silver["count"][0]
    silver_avg = df_silver["avg"][0]
    silver_min = df_silver["min"][0]
    silver_max = df_silver["max"][0]

    print(f"Events: {silver_events}")
    print(f"Average Price: {silver_avg:.2f}")
    print(f"Highest Price: {silver_max}")
    print(f"Lowest Price: {silver_min}")

    query_oil = """
    SELECT
    COUNT(*) as count,
    AVG(price) as avg,
    MIN(price) as min,
    MAX(price) as max
    FROM commodity_prices
    WHERE commodity = 'oil';
    """

    df_oil = pd.read_sql(query_oil, conn)

    print("\nOIL\n"
    "----------------------------------------")

    oil_events = df_oil["count"][0]
    oil_avg = df_oil["avg"][0]
    oil_min = df_oil["min"][0]
    oil_max = df_oil["max"][0]

    print(f"Events: {oil_events}")
    print(f"Average Price: {oil_avg:.2f}")
    print(f"Highest Price: {oil_max}")
    print(f"Lowest Price: {oil_min}")


finally:
    cursor.close()
    conn.close()
