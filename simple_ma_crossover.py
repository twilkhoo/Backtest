import pandas as pd
import plotly.graph_objs as go

def simple_ma_crossover(
    df: pd.DataFrame,
    starting_cash: float,
    short_period: int,
    long_period: int
) -> go.Figure:
  # Make a copy and sort by timestamp.
  df = df.sort_values('ts').copy()

  # Base candlestick figure.
  fig = go.Figure(data=[go.Candlestick(
      x=df['ts'],
      open=df['open'], high=df['high'],
      low=df['low'],   close=df['close']
  )])

  # Compute moving averages.
  df['short_ma'] = df['close'].rolling(window=short_period).mean()
  df['long_ma'] = df['close'].rolling(window=long_period).mean()

  cash = starting_cash
  pos = 0 # The current position (how much money we're up/down in the current long/short stretch).

  # The actual backtesting.
  for i in range(1, len(df)):
    prev_s, prev_l = df['short_ma'].iat[i-1], df['long_ma'].iat[i-1]
    curr_s, curr_l = df['short_ma'].iat[i],   df['long_ma'].iat[i]
    price = df['close'].iat[i]

    if pd.isna(prev_s) or pd.isna(prev_l) or pd.isna(curr_s) or pd.isna(curr_l):
      continue

    # Short crossing over long, buy!
    if prev_s <= prev_l and curr_s > curr_l:
      if pos < 0:
        cash += pos * price  # Close the short.
        pos = 0
      if pos == 0:
        n = int(cash / price)
        pos = n
        cash -= n * price

    # Short crossing under long, sell!
    elif prev_s >= prev_l and curr_s < curr_l:
      if pos > 0:
        cash += pos * price  # Close the long.
        pos = 0
      if pos == 0:
        n = int(cash / price)
        pos = -n
        cash += n * price

  # Final portfolio value.
  final_value = cash + pos * df['close'].iat[-1]

  # Overlay the MAs.
  fig.add_trace(go.Scatter(x=df['ts'], y=df['short_ma'], name='Short MA'))
  fig.add_trace(go.Scatter(x=df['ts'], y=df['long_ma'],  name='Long MA'))

  # Label how much money we ended up with.
  fig.update_layout(annotations=[dict(
      xref='paper', yref='paper', x=0.5, y=-0.2,
      text=f"Ending cash: ${final_value:.2f}",
      showarrow=False
  )])

  return fig
