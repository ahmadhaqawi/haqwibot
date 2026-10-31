import threading
import os
import time
import requests
import pandas as pd
import pandas_ta as ta
import ccxt
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running"

TELEGRAM_TOKEN = '9185362616:AAE8zy7pFG_yOuJGurptodXU1CJ19YFQQMQ'
TELEGRAM_CHAT_ID = '7586556190'
BINANCE_API_KEY = 'qPofjSlD7PuRnvABVK1HwrA7SbtTyUBc6I5UnLakPmWky8FSvSkNh5NEF8v2HWZu'
BINANCE_SECRET_KEY = 'G7Fs62dMe2YmRay0qbLIkGCzQpFxpwgJobaoZeIkiVZuoJU7GTDuLvMoXqrwP5zB'

exchange = ccxt.binance({
    'apiKey': BINANCE_API_KEY,
    'secret': BINANCE_SECRET_KEY,
    'enableRateLimit': True
})

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    requests.post(url, json=payload)

def fetch_data(symbol, timeframe='5m', limit=100):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None

def check_liquidity_4h(symbol):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, '4h', limit=1)
        volume_4h = ohlcv[0][5]
        return volume_4h
    except Exception as e:
        print(f"Error fetching 4h volume for {symbol}: {e}")
        return 0

def analyze_liquidity(symbol):
    try:
        order_book = exchange.fetch_order_book(symbol, limit=20)
        bid_depth = sum([bid[1] for bid in order_book['bids']])
        ask_depth = sum([ask[1] for ask in order_book['asks']])
        return bid_depth, ask_depth
    except Exception as e:
        print(f"Error fetching liquidity for {symbol}: {e}")
        return 0, 0

def check_signals(df):
    if df is None or len(df) < 30:
        return None, None
    df['RSI'] = ta.rsi(df['close'], length=14)
    macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
    df = pd.concat([df, macd], axis=1)
    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]
    signal = None
    if last_row['RSI'] < 30 and prev_row['RSI'] >= 30 and last_row['MACDh_12_26_9'] > 0:
        signal = 'BUY'
    elif last_row['RSI'] > 70 and prev_row['RSI'] <= 70 and last_row['MACDh_12_26_9'] < 0:
        signal = 'SELL'
    return signal, last_row

def run_bot():
    send_telegram_message("🤖 البوت بدأ بمسح العملات ذات السيولة العالية (4h)")
    min_volume_threshold = 1000000
    while True:
        try:
            markets = exchange.load_markets()
            usdt_symbols = [symbol for symbol in markets if symbol.endswith('/USDT')]
            for symbol in usdt_symbols:
                try:
                    volume_4h = check_liquidity_4h(symbol)
                    if volume_4h < min_volume_threshold:
                        continue
                    df = fetch_data(symbol, timeframe='5m')
                    if df is None:
                        continue
                    bid_depth, ask_depth = analyze_liquidity(symbol)
                    signal, data = check_signals(df)
                    if signal:
                        message = (
                            f"🔔 إشارة جديدة لـ {symbol}\n"
                            f"الاتجاه: {signal}\n"
                            f"السعر الحالي: {data['close']}\n"
                            f"RSI: {data['RSI']:.2f}\n"
                            f"حجم التداول (4h): {volume_4h:.2f}\n"
                            f"عمق السيولة - الشراء: {bid_depth:.2f}, البيع: {ask_depth:.2f}\n"
                            f"💡 يُنصح باتخاذ القرار بناءً على التحليل الفني والسيولة."
                        )
                        send_telegram_message(message)
                    time.sleep(2)
                except Exception as e:
                    print(f"خطأ في معالجة {symbol}: {e}")
        except Exception as e:
            print(f"خطأ في تحميل الأسواق: {e}")
        time.sleep(10)

if __name__ == '__main__':
    threading.Thread(target=run_bot).start()
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
