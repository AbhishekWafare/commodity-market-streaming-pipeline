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

print("api execution started")

def get_api_data():
    try:
        gold_response = requests.get(url_gold, timeout=10)
        gold_response.raise_for_status()
        gold_data = gold_response.json()

        silver_response = requests.get(url_silver, timeout=10)
        silver_response.raise_for_status()
        silver_data = silver_response.json()

    except Exception as e:
        dlq_producer.send(
            'dead-letter-prices',
            value={
                "error": str(e),
                "source": "api"
            }
        )

    # try:
    #     oil = yf.Ticker("CL=F")
    #     print(oil)
    #     oil_history = oil.history(period="1d")
    #     print(oil_history)
    #     oil_price = float(
    #         oil_history["Close"].iloc[-1]
    #     )

    # except Exception as e:
    #     print(f"fetch request failed for oil/sp500: {e}")
    #     raise

    try:
        oil = yf.Ticker("CL=F")

        oil_history = oil.history(period="5d")

        if oil_history.empty:
            raise Exception("No oil data returned from Yahoo Finance")

        oil_price = float(oil_history["Close"].iloc[-1])

    except Exception as e:
        print(f"fetch request failed for oil/sp500: {e}")
        raise

    # print(gold_data)
    return {
        "gold_price": gold_data["price"],
        "silver_price": silver_data["price"],
        "oil_price": oil_price
    }