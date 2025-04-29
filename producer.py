#!/usr/bin/env python3
import sys
import time
import json
import requests
from kafka import KafkaProducer

if __name__ == "__main__":
  if len(sys.argv) != 6:
    print("Usage: python producer.py <ticker> <start_date> <end_date> <rate> <polygon_api_key>")
    sys.exit(1)

  ticker, start_date, end_date, rate, api_key = sys.argv[1:]
  rate = float(rate)
  topic = "ohlcv"

  # Initialize the Kafka producer,
  producer = KafkaProducer(
      bootstrap_servers="localhost:9092",
      value_serializer=lambda v: json.dumps(v).encode("utf-8")
  )

  # Fetch df from Polygon.
  url = (
      f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
      f"{start_date}/{end_date}"
  )
  params = {
      "adjusted": "true",
      "sort": "asc",
      "limit": 50000,
      "apiKey": api_key
  }
  resp = requests.get(url, params=params)
  resp.raise_for_status()
  data = resp.json().get("results", [])

  # Publish each bar at the rate.
  for bar in data:
    payload = {
        "timestamp": bar["t"],
        "open": bar["o"],
        "high": bar["h"],
        "low": bar["l"],
        "close": bar["c"],
        "volume": bar["v"]
    }
    producer.send(topic, payload)
    print(f"sent {payload}")
    time.sleep(rate/1000)

  producer.flush()
  print("All done.")
