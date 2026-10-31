import ccxt
import pandas as pd
import requests


def check_liquidity_24h(exchange, symbol, min_24h_quote_volume=10000000):
  """Checks if 24-hour trading volume meets the minimum threshold.

  min_24h_quote_volume: Threshold in USDT (e.g., 10,000,000 USDT for high
  liquidity).
  """
  try:
    ticker = exchange.fetch_ticker(symbol)
    # 'quoteVolume' is the total volume in USDT over the last 24h
    vol_```python
import ccxt
import pandas as pd
import requests

def check_liquidity_24h(exchange, symbol, min_24h_quote_volume=10_000_000):
  """Checks if 24-hour trading volume meets the minimum threshold.
  min_24h_quote_volume: Threshold in USDT (e.g., 10,000,000 USDT for high liquidity).
  """
  try:
    ticker = exchange.fetch_ticker(symbol)
    vol_24h = ticker.get('quoteVolume', 0)
    is_liquid = vol_24h >= min_24h_quote_volume
    return is_liquid, vol_24h
  except Exception as e:
    print(f"Error fetching 24h ticker for {symbol}: {e}")
    return False, 0

def analyze_market_with_24h_volume(exchange, symbol, timeframe='5m', min_24h_volume=10_000_000):
  is_liquid, vol_24h = check_liquidity_24h(exchange, symbol, min_24h_volume)
  if not is_liquid:
    print(f"[{symbol}] Skipped: 24h volume {vol_24h:,.0f} USDT is below threshold {min_24h_volume:,.0f} USDT")
    return None

  ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
  df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
  return df
