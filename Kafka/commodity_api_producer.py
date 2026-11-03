
from kafka import KafkaProducer
import json
import requests
import yfinance as yf

dlq_producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

url_gold = "https://api.gold-api.com/price/XAU"
url_silver = "https://api.gold-api.com/price/XAG"


def send_api_error(source, error):
    event = {
        "error": str(error),
        "source": source
    }

    try:
        dlq_producer.send(
            'dead-letter-prices',
            value=event
        ).get(timeout=10)

        print("API error sent to DLQ:", event, flush=True)

    except Exception as dlq_error:
        print("Failed to send API error to DLQ:", dlq_error, flush=True)


def get_api_data():

    # GOLD AND SILVER
    try:
        print("Fetching Gold price...", flush=True)

        gold_response = requests.get(url_gold, timeout=10)
        gold_response.raise_for_status()
        gold_data = gold_response.json()

        print("Gold API response received.", flush=True)

        print("Fetching Silver price...", flush=True)

        silver_response = requests.get(url_silver, timeout=10)
        silver_response.raise_for_status()
        silver_data = silver_response.json()

        print("Silver API response received.", flush=True)

    except Exception as e:
        send_api_error("gold_silver_api", e)
        raise RuntimeError("Gold/Silver API fetching failed") from e

    # CRUDE OIL
    try:
        print("Fetching Crude Oil price from Yahoo Finance...", flush=True)

        oil = yf.Ticker("CL=F")

        oil_history = oil.history(
            period="5d",
            timeout=15
        )

        if oil_history.empty:
            raise ValueError("No oil data returned from Yahoo Finance")

        close_prices = oil_history["Close"].dropna()

        if close_prices.empty:
            raise ValueError("Oil closing price is unavailable")

        oil_price = float(close_prices.iloc[-1])

        print("Oil API response received.", flush=True)

    except Exception as e:
        print("Oil API error:", e, flush=True)
        send_api_error("yahoo_finance_oil", e)
        raise RuntimeError("Oil API fetching failed") from e

    print("All commodity API requests completed successfully.", flush=True)

    return {
        "gold_price": gold_data["price"],
        "silver_price": silver_data["price"],
        "oil_price": oil_price
    }
