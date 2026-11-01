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

WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "100000")) 

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

def msg_entry(sym, side, price, sl, tp, rsi, adx):
    if side == "LONG":
        title = "🟢 <b>إشارة لونق (LONG) - فيوتشر</b>"
        liq = price * (1 - (1/LEVERAGE) + 0.005) 
    else:
        title = "🔴 <b>إشارة شورت (SHORT) - فيوتشر</b>"
        liq = price * (1 + (1/LEVERAGE) - 0.005)
        
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    
    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 RSI: <code>{rsi:.1f}</code> | ADX: <code>{adx:.1f}</code>\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"💡 <i>بمجرد ربح 1%، انقل وقف الخسارة للدخول.</i>")

def msg_exit(sym, side, price, entry, reason, pnl_pct):
    title = "⚪ <b>إغلاق صفقة لونق</b>" if side == "LONG" else "⚪ <b>إغلاق صفقة شورت</b>"
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    reasons_ar = {"Stop Loss": "ضرب وقف الخسارة 🛑", "Take Profit": "تحقق هدف الربح 🎯", "EMA Cross": "كسر المتوسط 📉"}
    ar_reason = reasons_ar.get(reason, reason)

    leveraged_pnl = pnl_pct * LEVERAGE

    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"📥 الدخول: <code>{fmt(entry)}</code>\n"
            f"📤 الخروج: <code>{fmt(price)}</code>\n"
            f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code>\n"
            f"🔥 <b>بالرافعة ({LEVERAGE}x):</b> <code>{leveraged_pnl:+.2f}%</code>\n"
            f"📝 السبب: {ar_reason}")

# ==========================================
# 3. المؤشرات الفنية المتقدمة (ADX + ATR)
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
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    return dx.ewm(alpha=1/period, adjust=False).mean()

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def add_indicators(df):
    df["ema9"]   = ema(df["close"], 9)
    df["ema21"]  = ema(df["close"], 21)
    df["rsi"]    = rsi(df["close"], 14)
    df["adx"]    = adx(df["high"], df["low"], df["close"], 14)
    df["atr"]    = atr(df, 14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    return df

def check_signal(df_5m, df_1h, position, entry_price=None, sl=None, tp=None):
    if len(df_5m) < 50 or len(df_1h) < 50: return None
    
    last_5m = df_5m.iloc[-2]      
    prev_5m = df_5m.iloc[-3]
    price = last_5m["close"]

    # إدارة الخروج
    if position == "LONG":
        if price <= sl: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
        if price <= last_5m["ema21"]: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "EMA Cross"}
    if position == "SHORT":
        if price >= sl: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}
        if price >= last_5m["ema21"]: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "EMA Cross"}

    if position is not None: return None

    # فلاتر الدخول
    trend_1h_up   = df_1h.iloc[-2]["close"] > df_1h.iloc[-2]["ema21"] # فلتر اتجاه الساعة
    trend_1h_down = df_1h.iloc[-2]["close"] < df_1h.iloc[-2]["ema21"]
    
    cross_up   = prev_5m["ema9"] <= prev_5m["ema21"] and last_5m["ema9"] > last_5m["ema21"]
    cross_down = prev_5m["ema9"] >= prev_5m["ema21"] and last_5m["ema9"] < last_5m["ema21"]
    
    # شروط القوة (ADX > 25 يعني اتجاه قوي)
    adx_strong = last_5m["adx"] > 25
    vol_ok     = last_5m["volume"] > 1.2 * last_5m["vol_ma"] 

    # LONGS
    if (cross_up and trend_1h_up and adx_strong and vol_ok and 40 < last_5m["rsi"] < 70):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 1.5 * last_5m["atr"], "tp": price + 3.0 * last_5m["atr"], 
                "rsi": last_5m["rsi"], "adx": last_5m["adx"]}

    # SHORTS
    if (cross_down and trend_1h_down and adx_strong and vol_ok and 30 < last_5m["rsi"] < 60):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 1.5 * last_5m["atr"], "tp": price - 3.0 * last_5m["atr"], 
                "rsi": last_5m["rsi"], "adx": last_5m["adx"]}

    return None

# ==========================================
# 4. الفحص الرئيسي
# ==========================================
def scan_symbol(exchange, symbol):
    try:
        if symbol not in exchange.markets:
            return

        if symbol not in state:
            state[symbol] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None}
            
        # جلب بيانات 5 دقائق و 1 ساعة
        ohlcv_5m = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=100)
        ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=50)
        
        if len(ohlcv_5m) < 50 or len(ohlcv_1h) < 50: return

        df_5m = pd.DataFrame(ohlcv_5m, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        df_5m = add_indicators(df_5m)
        df_1h = add_indicators(df_1h) # نستخدم EMA21 على الساعة كفلتر

        current_candle_ts = int(df_5m.iloc[-2]["timestamp"])
        if state[symbol]["last_candle_ts"] == current_candle_ts:
            return
        state[symbol]["last_candle_ts"] = current_candle_ts

        pos = state[symbol]["position"]
        entry = state[symbol]["entry"]
        sl = state[symbol]["sl"]
        tp = state[symbol]["tp"]

        signal = check_signal(df_5m, df_1h, pos, entry, sl, tp)

        if not signal: return

        today = datetime.now(timezone.utc).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return 

        if signal["action"] == "ENTRY":
            state[symbol].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"]})
            send(msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"], signal["adx"]))

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT": pnl = -pnl
            state[symbol].update({"position": None, "entry": None, "sl": None, "tp": None})
            
            daily_pnl["value"] += (pnl * LEVERAGE) * RISK_PCT / 100
            send(msg_exit(symbol, pos, signal["price"], entry, signal["reason"], pnl))
            
            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nوصلت للحد الأقصى للخسارة اليومية.")

    except Exception as e:
        print(f"[ERROR] Scan {symbol}: {e}", flush=True)

# ==========================================
# 5. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting Advanced Futures Bot...", flush=True)
    
    # تشغيل مراقب الصفقات
    ws_thread = threading.Thread(target=run_websocket_loop, daemon=True)
    ws_thread.start()

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'} 
    })
    exchange.load_markets()

    send(f"🚀 <b>تم تشغيل بوت التداول الاحترافي (V2)</b>\n"
         f"➖➖➖➖➖➖➖➖\n"
         f"🛡️ <b>فلتر ADX:</b> مفعل (تجنب السوق العرضي)\n"
         f"📈 <b>فلتر الاتجاه:</b> فريم الساعة (1H)\n"
         f"⏱️ <b>فريم الدخول:</b> 5 دقائق\n"
         f"⚙️ الرافعة: <b>{LEVERAGE}x</b>")

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
