import sys
import subprocess
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, to_timestamp
import dash
from dash import dcc, html, callback_context
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

spark = (
    SparkSession.builder
    .appName("KafkaCandlestickApp")
    .master("local[*]")
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
query = None

def handle_batch(df, epoch_id):
    """
    Called for each micro-batch; df contains only that batch's rows.
    Append them to the global data_store.
    """
    global data_store
    pdf = df.toPandas()
    data_store.extend(pdf.to_dict("records"))
    print(f"Batch {epoch_id}: received {len(pdf)} rows; total stored: {len(data_store)}")


def start_streaming(consumer_interval_ms: int):
    """
    (Re)start the Spark Structured Streaming query with the given polling interval.
    """
    global query
    if query:
        try:
            query.stop()
        except Exception:
            pass
    query = (
        parsed.writeStream
              .foreachBatch(handle_batch)
              .trigger(processingTime=f"{consumer_interval_ms} milliseconds")
              .start()
    )

# Start the dash app.
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    html.H1("OHLCV Streamer & Candlestick Viewer"),

    html.Div([
        html.Label("Ticker:"), dcc.Input(id="ticker", type="text", value="AAPL"), html.Br(),
        html.Label("Start Date:"), dcc.Input(id="start-date", type="text", placeholder="YYYY-MM-DD", value="2021-01-01"), html.Br(),
        html.Label("End Date:"), dcc.Input(id="end-date", type="text", placeholder="YYYY-MM-DD", value="2021-01-10"), html.Br(),
        html.Label("Producer Interval (ms):"), dcc.Input(id="producer-interval", type="number", value=1000), html.Br(),
        html.Label("Consumer Interval (ms):"), dcc.Input(id="consumer-interval", type="number", value=1000), html.Br(),
        html.Label("Polygon.io API Key:"), dcc.Input(id="api-key", type="text", placeholder="API Key"), html.Br(), html.Br(),
        html.Button("Start Streaming", id="start-btn", n_clicks=0)
    ], style={"columnCount": 2, "gap": "10px"}),

    html.Hr(),
    dcc.Graph(id="candlestick-chart"),
    dcc.Interval(id="interval", interval=1000, n_intervals=0, disabled=False),
    dcc.Store(id="data-count-store", data={"count": 0})
])

# Callback for actually displaying the candlestick chart.
@app.callback(
    Output("candlestick-chart", "figure"),
    Output("interval", "disabled"),
    Output("data-count-store", "data"),
    Input("start-btn", "n_clicks"),
    Input("interval", "n_intervals"),
    State("ticker", "value"),
    State("start-date", "value"),
    State("end-date", "value"),
    State("producer-interval", "value"),
    State("consumer-interval", "value"),
    State("api-key", "value"),
    State("data-count-store", "data")
)
def handle_events(n_clicks, n_intervals,
                  ticker, s_date, e_date, prod_int, cons_int, api_key,
                  store_data):
    ctx = callback_context
    triggered = ctx.triggered[0]['prop_id'].split('.')[0]
    prev_count = store_data.get('count', 0)

    # Start.
    if triggered == 'start-btn' and n_clicks:
        # reset global data store and count
        global data_store
        data_store.clear()
        # restart streaming
        start_streaming(int(cons_int))
        # fire producer
        subprocess.Popen([
            sys.executable, "producer.py",
            ticker, s_date, e_date, str(prod_int), api_key
        ])
        return go.Figure(), False, {'count': 0}

    # New tick.
    if triggered == 'interval':
        curr_count = len(data_store)
        # no data yet
        if curr_count == 0:
            return go.Figure(), False, {'count': 0}
        if curr_count > prev_count:
            df = pd.DataFrame(data_store).sort_values('ts')
            fig = go.Figure(data=[go.Candlestick(
                x=df['ts'], open=df['open'], high=df['high'],
                low=df['low'], close=df['close']
            )])
            fig.update_layout(title='Live OHLCV Candlestick')
            return fig, False, {'count': curr_count}
        return dash.no_update, True, {'count': curr_count}

    return dash.no_update, False, store_data


# Runner.
if __name__ == "__main__":
    app.run(debug=True)
