import os
import sys
import threading
import time
from datetime import datetime, timezone
import asyncio
import websockets
import json

import ccxt
import pandas as pd
import requests
from flask import Flask

app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_active"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# 1. الإعدادات والمتغيرات (Config)
# ==========================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# --- إعدادات مراقبة الصفقات متعددة المنصات ---
WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "100000")) 

# --- إعدادات استراتيجية التداول (OKX) ---
FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", 
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]
TIMEFRAME = "5m" 
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
LEVERAGE = 5 

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(timezone.utc).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10)
    except Exception as e:
        print(f"[TG ERROR] {e}")

def fmt(p): 
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def clean_symbol(sym):
    return sym.split(':')[0]

# ==========================================
# 3. محرك مراقبة الصفقات متعدد المنصات (WebSocket)
# ==========================================

async def listen_to_binance():
    """مراقبة صفقات منصة Binance Futures"""
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    print("[WS] Connecting to Binance...", flush=True)
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print("[WS] Binance Connected.", flush=True)
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data'].get('e') == 'aggTrade':
                        trade = data['data']
                        price, quantity = float(trade['p']), float(trade['q'])
                        cost = price * quantity
                        if cost >= WHALE_THRESHOLD_WS:
                            side_ar = "شراء (لونق)" if trade['m'] == False else "بيع (شورت)"
                            emoji = "🟢" if trade['m'] == False else "🔴"
                            symbol_display = trade['s'].replace('USDT', '/USDT')
                            
                            # الترتيب الجديد: الزوج أولاً ثم النوع
                            msg = (f"🐋 <b>صفقة ضخمة</b>\n"
                                   f"➖➖➖➖➖➖➖➖\n"
                                   f"🪙 الزوج: <code>{symbol_display}</code>\n"
                                   f"{emoji} النوع: <b>{side_ar}</b>\n"
                                   f"💰 القيمة: <code>${cost:,.0f}</code>\n"
                                   f"💵 السعر: <code>{fmt(price)}</code>\n"
                                   f"➖➖➖➖➖➖➖➖\n"
                                   f"🏢 المنصة: <b>Binance</b>")
                            send(msg)
        except Exception as e:
            print(f"[WS BINANCE ERROR] {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

async def listen_to_okx():
    """مراقبة صفقات منصة OKX"""
    args = [{"channel": "trades", "instId": sym.replace("USDT", "-USDT-SWAP")} for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    print("[WS] Connecting to OKX...", flush=True)
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_msg = {"op": "subscribe", "args": args}
                await websocket.send(json.dumps(subscribe_msg))
                print("[WS] OKX Subscribed.", flush=True)
                
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['px'])
                            size = float(trade['sz'])
                            cost = price * size
                            side = trade.get('side', 'buy')
                            if cost >= WHALE_THRESHOLD_WS:
                                side_ar = "شراء (لونق)" if side == 'buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'buy' else "🔴"
                                symbol_display = trade['instId'].replace("-SWAP", "").replace("-", "/")
                                
                                # الترتيب الجديد: الزوج أولاً ثم النوع
                                msg = (f"🐋 <b>صفقة ضخمة</b>\n"
                                       f"➖➖➖➖➖➖➖➖\n"
                                       f"🪙 الزوج: <code>{symbol_display}</code>\n"
                                       f"{emoji} النوع: <b>{side_ar}</b>\n"
                                       f"💰 القيمة: <code>${cost:,.0f}</code>\n"
                                       f"💵 السعر: <code>{fmt(price)}</code>\n"
                                       f"➖➖➖➖➖➖➖➖\n"
                                       f"🏢 المنصة: <b>OKX</b>")
                                send(msg)
        except Exception as e:
            print(f"[WS OKX ERROR] {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

async def listen_to_bybit():
    """مراقبة صفقات منصة Bybit"""
    args = [f"publicTrade.{sym}" for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    print("[WS] Connecting to Bybit...", flush=True)
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                subscribe_msg = {"op": "subscribe", "args": args}
                await websocket.send(json.dumps(subscribe_msg))
                print("[WS] Bybit Subscribed.", flush=True)
                
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['p'])
                            size = float(trade['v'])
                            cost = price * size
                            side = trade.get('S', 'Buy')
                            if cost >= WHALE_THRESHOLD_WS:
                                side_ar = "شراء (لونق)" if side == 'Buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'Buy' else "🔴"
                                symbol_display = trade['s'].replace('USDT', '/USDT')
                                
                                # الترتيب الجديد: الزوج أولاً ثم النوع
                                msg = (f"🐋 <b>صفقة ضخمة</b>\n"
                                       f"➖➖➖➖➖➖➖➖\n"
                                       f"🪙 الزوج: <code>{symbol_display}</code>\n"
                                       f"{emoji} النوع: <b>{side_ar}</b>\n"
                                       f"💰 القيمة: <code>${cost:,.0f}</code>\n"
                                       f"💵 السعر: <code>{fmt(price)}</code>\n"
                                       f"➖➖➖➖➖➖➖➖\n"
                                       f"🏢 المنصة: <b>Bybit</b>")
                                send(msg)
        except Exception as e:
            print(f"[WS BYBIT ERROR] {e}. Reconnecting...", flush=True)
            await asyncio.sleep(5)

async def run_websockets():
    """تشغيل جميع اتصالات WebSocket في نفس الوقت"""
    await asyncio.gather(
        listen_to_binance(),
        listen_to_okx(),
        listen_to_bybit()
    )

def run_websocket_loop():
    """تشغيل حلقة WebSocket في مسار منفصل"""
    asyncio.run(run_websockets())

# ==========================================
# 4. دوال استراتيجية التداول (OKX)
# ==========================================
def ema(series, period): return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss  = -delta.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
    rs    = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    line     = ema_fast - ema_slow
    hist     = line - ema(line, signal)
    return line, hist

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def add_indicators(df):
    df["ema9"]   = ema(df["close"], 9)
    df["ema21"]  = ema(df["close"], 21)
    df["ema200"] = ema(df["close"], 200)
    df["rsi"]    = rsi(df["close"], 14)
    _, df["macd_hist"] = macd(df["close"])
    df["atr"]    = atr(df, 14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    return df

def check_signal(df, position, entry_price=None, sl=None, tp=None):
    if len(df) < 210: return None
    last  = df.iloc[-2]      
    prev  = df.iloc[-3]
    price = last["close"]

    if position == "LONG":
        if price <= sl: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
        if price <= last["ema21"]: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "EMA Cross"}
    if position == "SHORT":
        if price >= sl: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}
        if price >= last["ema21"]: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "EMA Cross"}

    if position is not None: return None

    cross_up   = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    vol_ok     = last["volume"] > 1.2 * last["vol_ma"] 
    macd_up    = last["macd_hist"] > 0 and last["macd_hist"] > prev["macd_hist"]
    macd_down  = last["macd_hist"] < 0 and last["macd_hist"] < prev["macd_hist"]

    trend_up   = price > last["ema200"]
    trend_down = price < last["ema200"]

    if (cross_up and trend_up and 40 < last["rsi"] < 70 and vol_ok and macd_up):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 2.0 * last["atr"], "tp": price + 4.0 * last["atr"], "rsi": last["rsi"]}

    if (cross_down and trend_down and 30 < last["rsi"] < 60 and vol_ok and macd_down):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 2.0 * last["atr"], "tp": price - 4.0 * last["atr"], "rsi": last["rsi"]}

    return None

def scan_symbol(exchange, symbol):
    try:
        if symbol not in exchange.markets:
            return

        if symbol not in state:
            state[symbol] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None}
            
        ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=300)
        if len(ohlcv) < 210: return

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df = add_indicators(df)

        current_candle_ts = int(df.iloc[-2]["timestamp"])
        if state[symbol]["last_candle_ts"] == current_candle_ts:
            return
        state[symbol]["last_candle_ts"] = current_candle_ts

        pos = state[symbol]["position"]
        entry = state[symbol]["entry"]
        sl = state[symbol]["sl"]
        tp = state[symbol]["tp"]

        signal = check_signal(df, pos, entry, sl, tp)

        if not signal: return

        today = datetime.now(timezone.utc).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return 

        if signal["action"] == "ENTRY":
            state[symbol].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"]})
            send(msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"]))

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT": pnl = -pnl
            state[symbol].update({"position": None, "entry": None, "sl": None, "tp": None})
            
            daily_pnl["value"] += (pnl * LEVERAGE) * RISK_PCT / 100
            
            send(msg_exit(symbol, pos, signal["price"], entry, signal["reason"], pnl))
            
            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nلقد وصلت للحد الأقصى للخسارة اليومية (-{MAX_DAILY_LOSS}%).")

    except Exception as e:
        print(f"[ERROR] Scan {symbol}: {e}", flush=True)

# ==========================================
# 5. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting Multi-Exchange Bot...", flush=True)
    
    # تشغيل مراقب الصفقات متعدد المنصات في الخلفية
    ws_thread = threading.Thread(target=run_websocket_loop, daemon=True)
    ws_thread.start()

    # إعداد منصة OKX لاستراتيجية التداول
    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'} 
    })
    exchange.load_markets()

    send(f"🚀 <b>تم تشغيل البوت (استراتيجية + مراقب متعدد المنصات)</b>\n"
         f"➖➖➖➖➖➖➖➖\n"
         f"🪙 استراتيجية التداول: <b>{len(FUTURES_SYMBOLS)} عملة (OKX)</b>\n"
         f"⏱️ الفريم الزمني: <b>5 دقائق</b>\n"
         f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
         f"👁️ مراقب الصفقات (Binance + OKX + Bybit)\n"
         f"💰 حد تنبيه الصفقات: <b>${WHALE_THRESHOLD_WS:,.0f}</b>")

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                scan_symbol(exchange, sym)
                time.sleep(1) 
        except Exception as e:
            print(f"[FATAL] Loop error: {e}", flush=True)
        
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
