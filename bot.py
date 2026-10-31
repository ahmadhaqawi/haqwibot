import ccxt import time import requests import pandas as pd import pandas_ta as ta
TELEGRAM_TOKEN = '9185362616:AAE8zy7pFG_yOuJGurptodXU1CJ19YFQQMQ' TELEGRAM_CHAT_ID = '7586556190'
BINANCE_API_KEY = 'qPofjSlD7PuRnvABVK1HwrA7SbtTyUBc6I5UnLakPmWky8FSvSkNh5NEF8v2HWZu' BINANCE_SECRET_KEY = 'G7Fs62dMe2YmRay0qbLIkGCzQpFxpwgJobaoZeIkiVZuoJU7GTDuLvMoXqrwP5zB'
exchange = ccxt.binance({ 'apiKey': BINANCE_API_KEY, 'secret': BINANCE_SECRET_KEY, 'enableRateLimit': True })
def send_telegram_message(message): url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage" payload = { 'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown' } requests.post(url, json=payload)
def fetch_data(symbol, timeframe='5m', limit=100): ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=limit) df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']) df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms') return df
def analyze_liquidity(symbol): order_book = exchange.fetch_order_book(symbol, limit=20) bid_depth = sum([bid[1] for bid in order_book['bids']]) ask_depth = sum([ask[1] for ask in order_book['asks']]) return bid_depth, ask_depth
def check_signals(df): df['RSI'] = ta.rsi(df['close'], length=14) macd = ta.macd(df['close'], fast=12, slow=26, signal=9) df = pd.concat([df, macd], axis=1)
last_row = df.iloc[-1]
prev_row = df.iloc[-2]

signal = None
if last_row['RSI'] < 30 and prev_row['RSI'] >= 30 and last_row['MACDh_12_26_9'] > 0:
    signal = 'BUY'
elif last_row['RSI'] > 70 and prev_row['RSI'] <= 70 and last_row['MACDh_12_26_9'] < 0:
    signal = 'SELL'
    
return signal, last_row
def run_bot(symbol): send_telegram_message(f"🤖 البوت بدأ بمراقبة زوج {symbol}") while True: try: df = fetch_data(symbol) bid_depth, ask_depth = analyze_liquidity(symbol) signal, data = check_signals(df)
        if signal:
            message = (
                f"🔔 إشارة جديدة لـ {symbol}\n"
                f"الاتجاه: {signal}\n"
                f"السعر الحالي: {data['close']}\n"
                f"RSI: {data['RSI']:.2f}\n"
                f"عمق السيولة - الشراء: {bid_depth:.2f}, البيع: {ask_depth:.2f}\n"
                f"💡 يُنصح باتخاذ القرار بناءً على التحليل الفني والسيولة."
            )
            send_telegram_message(message)
        
        time.sleep(300)
    except Exception as e:
        print(f"خطأ: {e}")
        time.sleep(60)
if name == 'main': run_bot('BTC/USDT')