from airflow.sdk import dag, task # type: ignore
from datetime import datetime
import psycopg2
import pandas as pd
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo



import warnings
warnings.filterwarnings("ignore")

def get_connection():

    return psycopg2.connect(
        host=os.getenv("KAFKA_HOST", "host.docker.internal"),
        database=os.getenv("KAFKA_DB", "kafka_db"),
        user=os.getenv("KAFKA_USER", "postgres"),
        password=os.getenv("KAFKA_PASSWORD", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5434"))
    )



@dag(
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False
)

def commodity_report_dag():

    @task
    def generate_report():

        conn = get_connection()
        # cursor = conn.cursor()
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

            return {
                "gold_events": gold_events,
                "gold_avg": float(gold_avg),
                "gold_max": float(gold_max),
                "gold_min": float(gold_min),

                "silver_events": silver_events,
                "silver_avg": float(silver_avg),
                "silver_max": float(silver_max),
                "silver_min": float(silver_min),

                "oil_events": oil_events,
                "oil_avg": float(oil_avg),
                "oil_max": float(oil_max),
                "oil_min": float(oil_min)
            }

        finally:
            # cursor.close()
            conn.close()

    @task
    def backup_csv(data):

        print("CSV backup started")

        df = pd.DataFrame([data])

        os.makedirs(
            "/opt/airflow/reports",
            exist_ok=True
        )

        file_name = (
            f"/opt/airflow/reports/"
            f"market_data_"
            f"{datetime.now(ZoneInfo('Asia/Kolkata')).strftime('%Y_%m_%d_%H_%M_%S')}.csv"
        )

        df.to_csv(file_name, index=False)

        print(f"Backup saved: {file_name}")

    report_data = generate_report()
    backup_csv(report_data)

commodity_report_dag()