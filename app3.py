import sys
import subprocess
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, to_timestamp
import dash
from dash import dcc, html, callback_context
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from simple_ma_crossover import *
from exponential_ma_crossover import *

spark = (
    SparkSession.builder
    .appName("BacktestReplay")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    )
    .getOrCreate()
)

schema = (
    StructType()
    .add("timestamp", LongType())
    .add("open", DoubleType())
    .add("high", DoubleType())
    .add("low", DoubleType())
    .add("close", DoubleType())
    .add("volume", LongType())
)

raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "ohlcv")
         .option("startingOffsets", "latest")
         .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("data"))
    .select(
        to_timestamp((col("data.timestamp")/1000).cast("long")).alias("ts"),
        "data.open", "data.high", "data.low", "data.close"
    )
)

data_store = []


def handle_batch(df, epoch_id):
  """
  handle_batch appends the new data from Kafka to an in-memory dict.
  """
  global data_store
  pdf = df.toPandas()
  data_store.extend(pdf.to_dict("records"))
  print(
      f"Received batch {epoch_id} with {len(pdf)} rows, we now have total {len(data_store)}")


query = None


def start_streaming(interval: int):
  """
  Doing it this way so that we can run multiple queries/backtests without shutting everything off.
  """
  global query
  if query:
    try:
      query.stop()
    except Exception:  # May raise exception if aborting off while consuming another batch.
      pass
  query = (
      parsed.writeStream
            .foreachBatch(handle_batch)
            .trigger(processingTime=f"{interval} milliseconds")
            .start()
  )


# Start the dash app.
app = dash.Dash(
    __name__,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Open+Sans:wght@300;400;600&display=swap"
    ]
)
server = app.server

# Flask frontend, I am NOT a frontend engineer...
app.layout = html.Div([
    html.Div(
        [
            html.H1("Backtest Replay"),
            html.H2("Choose a strategy, and simulate a real market feed!"),
        ],
        style={
            "display": "flex",
            "flexDirection": "column",
            "alignItems": "center",
        }
    ),


    html.Div([
        html.Div([
            html.Label("Ticker:"), dcc.Input(id="ticker", type="text",
                                             value="AAPL", style={"width": "20%"}),
            html.Br(),
            html.Label("Start Date:"), dcc.Input(id="start-date",
                                                 type="text", value="2023-06-01", style={"width": "20%"}),
            html.Br(),
            html.Label("End Date:"), dcc.Input(id="end-date", type="text",
                                               value="2023-08-17", style={"width": "20%"}),
            html.Br(),
            html.Label("Polygon.io API Key:"), dcc.Input(
                id="api-key", type="password", style={"width": "20%"}),
        ], style={"flex": "1", "display": "flex", "flexDirection": "column", "gap": "10px"}),

        html.Div([

            html.Label("Producer Interval (ms):"), dcc.Input(
                id="producer-interval", type="number", value=1000, style={"width": "20%"}),
            html.Br(),
            html.Label("Consumer Interval (ms):"), dcc.Input(
                id="consumer-interval", type="number", value=1000, style={"width": "20%"}),
            html.Br(),
            html.Label("Starting Cash (USD):"), dcc.Input(
                id="starting-cash", type="number", value=1000, style={"width": "20%"}),
            html.Br(),
            html.Label("Strategy:"), dcc.Dropdown(
                id="strategy-dropdown",
                options=[{"label": "Simple Moving Average Crossover", "value": "sma"}, 
                         {"label": "Exponential Moving Average Crossover", "value": "ema"}],
                value="sma",
                style={"width": "80%"}
            ),
            html.Div(
                id="strategy-params",
                children=[
                    dcc.Input(id="short-period", type="number",
                              style={"display": "none"}),
                    dcc.Input(id="long-period",  type="number",
                              style={"display": "none"})
                ],
                style={"display": "flex", "gap": "10px"}
            ),
        ], style={"flex": "1", "display": "flex", "flexDirection": "column", "gap": "10px"}),

    ], style={"display": "flex", "gap": "20px", "padding": "20px"}),

    html.Div(
        html.Button(
            "Start Backtest",
            id="start-btn",
            n_clicks=0,
            style={
                "width": "180px",
                "height": "48px",
                "fontSize": "16px",
                "cursor": "pointer"
            }
        ),
        style={
            "display": "flex",
            "justifyContent": "center",
            "marginTop": "20px"
        }
    ),

    html.Hr(),
    dcc.Graph(id="candlestick-chart"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0, disabled=False),
    dcc.Store(id="data-count-store", data={"count": 0})
])

# Callback to select other params depending on what strategy we decide to pick.


@app.callback(
    Output("strategy-params", "children"),
    Input("strategy-dropdown", "value")
)
def pick_strategy(strategy):
  if strategy in ("sma", "ema"):
    label = "Short EMA Period:" if strategy == "ema" else "Short SMA Period:"
    label2 = "Long EMA Period:" if strategy == "ema" else "Long SMA Period:"
    return [  
        html.Div([html.Label(label),  dcc.Input(
            id="short-period", type="number", min=1, value=5)]),
        html.Div([html.Label(label2), dcc.Input(
            id="long-period",  type="number", min=1, value=30)])
    ]
  return []

# Callback to actually start the backtesting.


@app.callback(
    Output("candlestick-chart", "figure"),
    # If disabled == false, that means we're still reading at the interval rate.
    Output("interval", "disabled"),
    # Data stores the count of the previous df rows, so if this doesn't change between intervals, we stop.
    Output("data-count-store", "data"),
    Input("start-btn", "n_clicks"),
    Input("interval", "n_intervals"),
    State("ticker", "value"),
    State("start-date", "value"),
    State("end-date", "value"),
    State("producer-interval", "value"),
    State("consumer-interval", "value"),
    State("api-key", "value"),
    State("starting-cash", "value"),
    State("strategy-dropdown", "value"),
    State("short-period", "value"),
    State("long-period", "value"),
    State("data-count-store", "data")
)
def handle_events(n_clicks, n_intervals,
                  ticker, s_date, e_date, prod_int, cons_int, api_key,
                  starting_cash, strategy, short_period, long_period,
                  store_data):

  # Let's us find what callback was initiated.
  ctx = callback_context
  triggered = ctx.triggered[0]['prop_id'].split('.')[0]
  prev_count = store_data.get('count', 0)

  # Callback for clicking the button.
  if triggered == 'start-btn':
    global data_store
    data_store.clear()
    start_streaming(int(cons_int))
    subprocess.Popen([
        sys.executable, "producer.py",
        # 1000ms seems to be the most performant rate since display takes way too long to re-render.
        ticker, s_date, e_date, str(prod_int), api_key
    ])
    return go.Figure(), False, {'count': 0}

  # Callback for updating the spark streaming data.
  if triggered == 'interval':
    curr_count = len(data_store)

    # No new data, stopping criteria.
    if curr_count == 0:
      return go.Figure(), False, {'count': 0}

    if curr_count > prev_count:
      df = pd.DataFrame(data_store).sort_values('ts')
      fig = go.Figure(data=[go.Candlestick(
          x=df['ts'], open=df['open'], high=df['high'],
          low=df['low'], close=df['close']
      )])

      # Backtest: Simple Moving Average Crossover.
      if strategy == 'sma':
        fig = simple_ma_crossover(
            df,
            starting_cash=float(starting_cash),
            short_period=int(short_period),
            long_period=int(long_period)
        )
      # Backtest: Exponential Moving Average Crossover.
      elif strategy == 'ema':
        fig = exponential_ma_crossover(
            df,
            starting_cash=float(starting_cash),
            short_period=int(short_period),
            long_period=int(long_period)
        )

      fig.update_layout(
          title='Simulated Realtime OHLCV Candlestick and Backtesting')
      return fig, False, {'count': curr_count}

    return dash.no_update, True, {'count': curr_count}

  return dash.no_update, False, store_data


# Runner.
if __name__ == "__main__":
  app.run(debug=True)
