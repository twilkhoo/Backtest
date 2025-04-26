import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

st.title("Backtesting Engine- Replay")
st.code("Use the left sidebar to select a ticker, daterange, and strategy.", language=None)

st.sidebar.header("Parameters")

# User inputs
with st.sidebar:
  ticker = st.text_input("Ticker Symbol", value="AAPL")
  start_date = st.date_input("Start Date", value=datetime(2024, 1, 1))
  end_date = st.date_input("End Date", value=datetime(2025, 1, 1))
  api_key = st.text_input("Polygon.io API Key", type="password",
                          help="Be aware of your Polygon.io subscription limits, the free plan supports only 2 years of historical data.")


def fetch_ohlcv(ticker: str, start: datetime, end: datetime, api_key: str) -> pd.DataFrame:
  url = (
      f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
      f"{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
  )
  response = requests.get(url)
  response.raise_for_status()
  data = response.json().get("results", [])
  if not data:
    return pd.DataFrame()
  df = pd.DataFrame(data)
  df["t"] = pd.to_datetime(df["t"], unit='ms')
  df.rename(
      columns={"t": "Date", "o": "Open", "h": "High",
               "l": "Low", "c": "Close", "v": "Volume"},
      inplace=True
  )
  return df


# Runner.
if st.sidebar.button("Fetch Data"):
  if not ticker:
    st.error("Please enter a ticker symbol.")
  elif start_date >= end_date:
    st.error("Start date must be before end date.")
  elif not api_key:
    st.error("Please enter your Polygon.io API key.")
  else:
    with st.spinner("Fetching data..."):
      df = fetch_ohlcv(ticker, start_date, end_date, api_key)
    if df.empty:
      st.warning("No data returned for the specified ticker and date range.")
    else:
      # Plot candlestick chart.
      fig = go.Figure()
      fig.add_trace(
              go.Candlestick(
                  x=df["Date"],
                  open=df["Open"],
                  high=df["High"],
                  low=df["Low"],
                  close=df["Close"],
                  name=ticker
              )
      )
      
      
      every_10 = list(range(0, len(df), 10))
      fig.add_trace(go.Scatter(
          x=df.index[every_10],
          y=df["Close"].iloc[every_10],
          mode="markers",
          marker=dict(symbol="star", size=12, color="red"),
          name="Sticker"
      ))

      fig.update_layout(
          title=f"{ticker.upper()} Candlestick Chart",
          xaxis_title="Date",
          yaxis_title="Price",
      )
      st.plotly_chart(fig)
