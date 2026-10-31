import os
import threading
import time
import ccxt
import pandas as pd
import requests
from flask import Flask

# 1. Initialize Flask app for Render health checks / port binding
app = Flask(__name__)


@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "message": "Bot is active and running"}, 200


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    # Host must be 0.0.0.0 to bind externally inside the container
    app.run(host="0.0.0.0", port=port)


# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")


def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[Alert] {message}")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"Error sending Telegram alert: {e}")


def check_liquidity_24h(exchange, symbol, min_24h_quote_volume=10000000):
    try:
        ticker = exchange.fetch_ticker(symbol)
        vol_24h = float(ticker.get("quoteVolume", 0.0) or 0.0)
        is_liquid = vol_24h >= min_24h_quote_volume
        return is_liquid, vol_24h
    except Exception as e:
        print(f"Error fetching 24h ticker for {symbol}: {e}")
        return False, 0.0


def fetch_market_data(exchange, symbol, timeframe="5m", limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(
            ohlcv,
            columns=["timestamp", "open", "high", "low", "close", "volume"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        print(f"Error fetching candles for {symbol}: {e}")
        return None


def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def scan_symbol(exchange, symbol, timeframe="5m", min_24h_vol=10000000):
    is_liquid, vol_24h = check_liquidity_24h(exchange, symbol, min_24h_vol)
    if not is_liquid:
        return

    df = fetch_market_data(exchange, symbol, timeframe=timeframe, limit=50)
    if df is None or len(df) < 20:
        return

    df["rsi"] = calculate_rsi(df["close"], period=14)
    last_close = df["close"].iloc[-1]
    last_rsi = df["rsi"].iloc[-1]

    if last_rsi >= 52:
        msg = f"🟢 *LONG Signal*\nSymbol: `{symbol}`\nTF: `{timeframe}`\nClose: `{last_close}`\nRSI: `{last_rsi:.2f}`\n24h Vol: `${vol_24h:,.0f}`"
        send_telegram_alert(msg)
    elif last_rsi <= 48:
        msg = f"🔴 *SHORT Signal*\nSymbol: `{symbol}`\nTF: `{timeframe}`\nClose: `{last_close}`\nRSI: `{last_rsi:.2f}`\n24h Vol: `${vol_24h:,.0f}`"
        send_telegram_alert(msg)


if __name__ == "__main__":
    server_thread = threading.Thread(target=run_flask, daemon=True)
    server_thread.start()

    exchange = ccxt.binance({"enableRateLimit": True})
    symbols_to_monitor = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]

    print("Bot initialized. Running scan loop...")
    send_telegram_alert("🚀 *Bot started successfully on Render.*")

    while True:
        for sym in symbols_to_monitor:
            scan_symbol(exchange, sym, timeframe="5m", min_24h_vol=10000000)
            time.sleep(1)
        time.sleep(60)
