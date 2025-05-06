import pandas as pd
import plotly.graph_objs as go


def rsi_strategy(
    df: pd.DataFrame,
    starting_cash: float,
    rsi_period: int
) -> go.Figure:

  # Prepare the data.
  df = df.sort_values("ts").copy()
  fig = go.Figure(data=[go.Candlestick(
      x=df["ts"],
      open=df["open"], high=df["high"],
      low=df["low"],   close=df["close"]
  )])

  # Compute gain and loss for the entire current df.
  # A series of differences of close values each day.
  delta = df["close"].diff()
  gains = delta.clip(lower=0)
  losses = -delta.clip(upper=0)

  # Initial average for the lookback period.
  avg_gain = gains.rolling(window=rsi_period, min_periods=rsi_period).mean()
  avg_loss = losses.rolling(window=rsi_period, min_periods=rsi_period).mean()

  # For every next tick, compute the new gain and loss.
  for i in range(rsi_period, len(df)):
    prev_gain = avg_gain.iat[i-1]
    prev_loss = avg_loss.iat[i-1]
    avg_gain.iat[i] = (prev_gain*(rsi_period-1) + gains.iat[i]) / rsi_period
    avg_loss.iat[i] = (prev_loss*(rsi_period-1) + losses.iat[i]) / rsi_period

  # Compute the RSI.
  rs = avg_gain / avg_loss
  df["rsi"] = 100 - (100 / (1 + rs))

  # See if we have any overbought/oversold signals.
  cash = starting_cash
  pos = 0
  for i in range(1, len(df)):
    prev_rsi = df["rsi"].iat[i-1]
    curr_rsi = df["rsi"].iat[i]
    price = df["close"].iat[i]

    if pd.isna(prev_rsi) or pd.isna(curr_rsi):
      continue

    # We buy when RSI is > 30 (oversold).
    if prev_rsi <= 30 and curr_rsi > 30:
      if pos < 0:
        cash += pos * price
        pos = 0
      if pos == 0:
        n = int(cash / price)
        pos = n
        cash -= n * price

    # We short when RSI < 70 (overbought).
    elif prev_rsi >= 70 and curr_rsi < 70:
      if pos > 0:
        cash += pos * price
        pos = 0
      if pos == 0:
        n = int(cash / price)
        pos = -n
        cash += n * price

  # Compute the final value.
  final_value = cash + pos * df["close"].iat[-1]

  # Overlay RSI values on the graph.
  fig.add_trace(go.Scatter(
      x=df["ts"], y=df["rsi"], name="RSI", yaxis="y2"
  ))
  fig.update_layout(
      yaxis2=dict(
          title="RSI",
          overlaying="y",
          side="right",
          range=[0, 100]
      ),
      annotations=[dict(
          xref="paper", yref="paper", x=0.5, y=-0.2,
          text=f"Ending cash: ${final_value:.2f}",
          showarrow=False
      )]
  )

  return fig
