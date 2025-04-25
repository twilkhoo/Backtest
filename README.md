# Backtesting Engine

We'll be creating a Backtest Replay Engine.
There will be two modes:
  1. Backtest- we just run a strategy against stock data we already have.
  2. Replay- create a "stream" of data with specified throttling, and have this stream be a realtime simulation of market data, implemented with Kafka and Spark Structured Streaming.

This will all be displayed with a Streamlit UI.
