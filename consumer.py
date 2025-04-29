import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, to_timestamp
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: python consumer.py <poll_rate_in_ms>")
    sys.exit(1)

  # Starts tweaking if it's too low (refresh rate too quick), 1-2s is decent.
  poll_rate = max(1000, int(sys.argv[1]))

  # Start spark structured streaming.
  spark = (
      SparkSession.builder
      .appName("KafkaCandlestickConsumer")
      .master("local[*]")   # if you want to run locally
      .config(
          "spark.jars.packages",
          "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
      )
      .getOrCreate()
  )

  # This schema directly maps what's in the producer.py.
  schema = (
      StructType()
      .add("timestamp", LongType())
      .add("open", DoubleType())
      .add("high", DoubleType())
      .add("low", DoubleType())
      .add("close", DoubleType())
      .add("volume", LongType())
  )

  # This defines where to read from, and in what manner (e.g. rate).
  raw = (
      spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "ohlcv")
      .option("startingOffsets", "latest")
      .load()
  )

  # Parsing stream info.
  parsed = (
      raw.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), schema).alias("data"))
      .select(
          # Convert timestamp col.
          to_timestamp((col("data.timestamp")/1000).cast("long")).alias('ts'),
          "data.open", "data.high", "data.low", "data.close"
      )
  )

  # Writing into an in-memory table.
  query = (
      parsed.writeStream
            .format("memory")
            .queryName("ohlcv_table")
            .outputMode("append")
            .trigger(processingTime=f"{poll_rate} milliseconds")
            .start()
  )

  # Create a dash app (plotly) for the candlesticks.
  app = dash.Dash(__name__)
  app.layout = html.Div([
      dcc.Graph(id="candlestick-chart"),
      dcc.Interval(id="interval", interval=poll_rate, n_intervals=0)
  ])

  # A callback that fires at the interval to update the output (the graph).
  @app.callback(
      Output("candlestick-chart", "figure"),
      Input("interval", "n_intervals")
  )
  def update(n):
    # Read all data so far.
    df = spark.sql("SELECT * FROM ohlcv_table ORDER BY ts").toPandas()
    if df.empty:
      return go.Figure()
    fig = go.Figure(
        data=[go.Candlestick(
            x=df["ts"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"]
        )]
    )
    fig.update_layout(title="Live OHLCV Candlestick")
    return fig

  app.run(debug=True)
  query.awaitTermination()
