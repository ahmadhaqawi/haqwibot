import os
import threading
import time
import ccxt
from flask import Flask
import pandas as pd
import pandas_ta as ta
import requests

app = Flask(__name__)


@app.route("/")
def home():
  return "Bot is running"


# Telegram Configuration
TELEGRAM_TOKEN = os.environ.get(
    "TELEGRAM_TOKEN", "9185362616:AAE8zy7PFG_yoUjgurptodXU1C19"
)
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "7586556190")

# Binance API Setup
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_SECRET_KEY = os.environ.get("BINANCE_SECRET_KEY", "")

exchange = ccxt.binance({
    "apiKey": BINANCE_API_KEY,
    "secret": BINANCE_SECRET_KEY,
    "enableRateLimit": True,
})


def send_telegram_message(message: str):
  url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
  payload = {
      "chat_id": TELEGRAM_CHAT_ID,
      "text": message,
      "parse_mode": "Markdown",
  }
  try:
    requests.post(url, json=payload, timeout=10)
  except Exception as e:
    print(f"Error sending Telegram message: {e}")


def fetch_data(symbol: str, timeframe: str = "1h", limit: int = 100):
  try:
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(
        ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    return df
  except Exception as e:
    print(f"Error fetching data for {symbol} ({timeframe}): {e}")
    return None


def check_liquidity_depth(symbol: str):
  """Evaluates order book liquidity (bid/ask depth within top 20 levels)."""
  try:
    order_book = exchange.fetch_order_book(symbol, limit=20)
    bid_depth = sum([bid[1] for bid in order_book.get("bids", [])])
    ask_depth = sum([bid[1] for bid in order_book.get("asks", [])])
    return bid_depth, ask_depth
  except Exception as e:
    print(f"Error fetching order book liquidity for {symbol}: {e}")
    return 0, 0


def check_signals(df: pd.DataFrame):
  """Computes RSI (14) and MACD (12, 26, 9) signals on candle data."""
  if df is None or len(df) < 35:
    return None, None

  df["RSI"] = ta.rsi(df["close"], length=14)
  macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
  df = pd.concat([df, macd], axis=1)

  last_row = df.iloc[-1]
  prev_row = df.iloc[-2]

  rsi_curr = last_row["RSI"]
  rsi_prev = prev_row["RSI"]

  # Identify MACD columns dynamically
  macd_cols = [c for c in df.columns if c.startswith("MACD_")]
  sig_cols = [c for c in df.columns if c.startswith("MACDs_")]

  macd_bull = False
  macd_bear = False
  if macd_cols and sig_cols:
    m_col, s_col = macd_cols[0], sig_cols[0]
    macd_bull = (
        last_row[m_col] > last_row[s_col] and prev_row[m_col] <= prev_row[s_col]
    )
    macd_bear = (
        last_row[m_col] < last_row[s_col] and prev_row[m_col] >= prev_row[s_col]
    )

  signal = None
  # BUY: RSI recovers above 30 or Bullish MACD crossover from oversold
  if (rsi_curr > 30 and rsi_prev <= 30) or (rsi_curr < 45 and macd_bull):
    signal = "BUY"
  # SELL: RSI drops below 70 or Bearish MACD crossover from overbought
  elif (rsi_curr < 70 and rsi_prev >= 70) or (rsi_curr > 55 and macd_bear):
    signal = "SELL"

  return signal, last_row


def scan_multitimeframe_symbol(
    symbol: str, timeframes: list = ["1h", "2h", "4h"]
):
  """Checks confluence across 1h, 2h, and 4h intervals alongside liquidity."""
  tf_results = {}
  active_signals = []

  for tf in timeframes:
    df = fetch_data(symbol, timeframe=tf)
    signal, row_data = check_signals(df)
    tf_results[tf] = {"signal": signal, "data": row_data}
    if signal:
      active_signals.append((tf, signal))

  # If any timeframe has an active trigger, report confluence & liquidity
  if active_signals:
    bid_depth, ask_depth = check_liquidity_depth(symbol)
    base_data = tf_results["1h"]["data"]
    curr_price = (
        base_data["close"]
        if base_data is not None
        else tf_results[active_signals[0][0]]["data"]["close"]
    )

    msg_lines = [
        f"🚨 *إشارة جديدة: {symbol}*",
        f"💵 *السعر الحالي:* `{curr_price}`",
        f"📊 *السيولة (عمق الطلب/العرض):* `{bid_depth:.2f}` / `{ask_depth:.2f}`",
        "🕒 *تحليل الفريمات:*",
    ]

    for tf in timeframes:
      sig = tf_results[tf]["signal"] or "لا توجد إشارة"
      rsi_val = (
          f"{tf_results[tf]['data']['RSI']:.2f}"
          if tf_results[tf]["data"] is not None
          else "N/A"
      )
      msg_lines.append(f"• *فريم {tf}:* {sig} (RSI: `{rsi_val}`)")

    send_telegram_message("\n".join(msg_lines))


def run_bot():
  symbols = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
  min_volume_threshold = 1000000

  while True:
    try:
      for symbol in symbols:
        try:
          # Verify 24h/4h base volume threshold
          ticker = exchange.fetch_ticker(symbol)
          quote_vol = ticker.get("quoteVolume", 0)

          if quote_vol >= min_volume_threshold:
            scan_multitimeframe_symbol(symbol, timeframes=["1h", "2h", "4h"])
          time.sleep(2)  # Avoid endpoint rate-limiting between pairs
        except Exception as e:
          print(f"خطأ في معالجة {symbol}: {e}")

    except Exception as e:
      print(f"خطأ في تحميل الأسواق: {e}")

    time.sleep(10)  # Polling cycle interval


if __name__ == "__main__":
  bot_thread = threading.Thread(target=run_bot, daemon=True)
  bot_thread.start()
  port = int(os.environ.get("PORT", 10000))
  app.run(host="0.0.0.0", port=port)
