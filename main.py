import requests
import json
from datetime import datetime
from flask import Flask, request
from google.cloud import pubsub_v1

app = Flask(__name__)
publisher = pubsub_v1.PublisherClient()

PROJECT_ID = "lexical-sol-466019-e3"
TOPIC_ID = "crypto-prices-topic"
TOPIC_PATH = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@app.route("/", methods=["POST"])
def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    response = requests.get(url)

    if response.status_code != 200:
        return f"Error fetching data: {response.status_code}", 500

    prices = response.json()
    for coin, data in prices.items():
        message = {
            "coin": coin,
            "usd_price": data["usd"],
            "timestamp": datetime.utcnow().isoformat()
        }
        publisher.publish(TOPIC_PATH, json.dumps(message).encode("utf-8"))

    return "Data published", 200
