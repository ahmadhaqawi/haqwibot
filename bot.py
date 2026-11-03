import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
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
KSA_TZ = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "500000")) 
WHALE_COOLDOWN = int(os.getenv("WHALE_COOLDOWN", "600")) 
last_whale_alert = {}

WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", 
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]

TIMEFRAMES = ["5m", "15m"] 
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
LEVERAGE = 5 

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML",
                      "disable_web_page_preview": True},
                timeout=10)
        except Exception as e:
            print(f"[TG ERROR] Failed to send to {cid}: {e}")

def fmt(p): 
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def msg_entry(sym, side, price, sl, tp, rsi, adx, signal_type):
    if side == "LONG":
        title = f"🟢 <b>إشارة لونق ({signal_type})</b>"
    else:
        title = f"🔴 <b>إشارة شورت ({signal_type})</b>"
        
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    
    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"⏰ وقت الدخول: <code>{now_str}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 RSI: <code>{rsi:.1f}</code> | ADX: <code>{adx:.1f}</code>\n"
            f"💡 <i>بمجرد ربح 1%، قم بتعديل وقف الخسارة إلى سعر الدخول.</i>")

def msg_exit(sym, side, timeframe, price, entry, reason, pnl_pct, duration, entry_str, exit_str):
    title = f"⚪ <b>إغلاق صفقة لونق ({timeframe})</b>" if side == "LONG" else f"⚪ <b>إغلاق صفقة شورت ({timeframe})</b>"
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    reasons_ar = {"Stop Loss": "ضرب وقف الخسارة 🛑", "Take Profit": "تحقق هدف الربح 🎯"}
    ar_reason = reasons_ar.get(reason, reason)

    leveraged_pnl = pnl_pct * LEVERAGE

    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"📥 الدخول: <code>{fmt(entry)}</code> (⏰ {entry_str})\n"
            f"📤 الخروج: <code>{fmt(price)}</code> (⏰ {exit_str})\n"
            f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code>\n"
            f"🔥 <b>بالرافعة ({LEVERAGE}x):</b> <code>{leveraged_pnl:+.2f}%</code>\n"
            f"⏱️ <b>مدة الصفقة:</b> <code>{duration}</code>\n"
            f"📝 السبب: {ar_reason}")

# ==========================================
# 3. محرك مراقبة الصفقات (الحيتان)
# ==========================================
async def process_whale_alert(platform, symbol_display, side_ar, emoji, cost, price):
    global last_whale_alert
    key = f"{platform}_{symbol_display}"
    now = time.time()
    if now - last_whale_alert.get(key, 0) < WHALE_COOLDOWN:
        return 
    last_whale_alert[key] = now
    
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    
    msg = (f"🐋 <b>صفقة ضخمة</b>\n"
           f"🪙 الزوج: <code>{symbol_display}</code>\n"
           f"{emoji} النوع: <b>{side_ar}</b>\n"
           f"💰 القيمة: <code>${cost:,.0f}</code>\n"
           f"💵 السعر: <code>{fmt(price)}</code>\n"
           f"⏰ الوقت: <code>{now_str}</code>")
    send(msg)

async def listen_to_binance():
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
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
                            await process_whale_alert("Binance", symbol_display, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(5)

async def listen_to_okx():
    args = [{"channel": "trades", "instId": sym.replace("USDT", "-USDT-SWAP")} for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
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
                                await process_whale_alert("OKX", symbol_display, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(5)

async def listen_to_bybit():
    args = [f"publicTrade.{sym}" for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
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
                                await process_whale_alert("Bybit", symbol_display, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(5)

async def run_websockets():
    await asyncio.gather(listen_to_binance(), listen_to_okx(), listen_to_bybit())

def run_websocket_loop():
    asyncio.run(run_websockets())

# ==========================================
# 4. المؤشرات الفنية
# ==========================================
def ema(series, period): return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss  = -delta.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
    rs    = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def adx(high, low, close, period=14):
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    return dx.ewm(alpha=1/period, adjust=False).mean()

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["