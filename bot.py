import os
import sys
import threading
import time
from datetime import datetime, timezone

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

# أزواج العقود الآجلة في OKX (Swap)
FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", 
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]

TIMEFRAME = "15m" # فريم 15 دقيقة أفضل للرافعة المالية
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))

# نسبة الرافعة المالية المقترحة (للتنبيه فقط)
LEVERAGE = 5 

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(timezone.utc).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        print("[TG ERROR] Token or Chat ID missing.")
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

def msg_entry(sym, side, price, sl, tp, rsi):
    if side == "LONG":
        title = "🟢 <b>إشارة لونق (LONG) - فيوتشر</b>"
        liq = price * (1 - (1/LEVERAGE) + 0.005) # تقدير سعر التصفية
    else:
        title = "🔴 <b>إشارة شورت (SHORT) - فيوتشر</b>"
        liq = price * (1 + (1/LEVERAGE) - 0.005)
        
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    
    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"⚙️ الرافعة المقترحة: <b>{LEVERAGE}x</b>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"🛑 وقف الخسارة (SL): <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح (TP): <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"⚠️ <b>سعر التصفية التقريبي:</b> <code>{fmt(liq)}</code>\n"
            f"📊 RSI: <code>{rsi:.1f}</code>\n"
            f"⚖️ الربح للخسارة: <code>1:{reward/risk:.1f}</code>\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"💡 <i>نصيحة: بمجرد تحقق ربح 1%، انقل وقف الخسارة لنقطة الدخول.</i>")

def msg_exit(sym, side, price, entry, reason, pnl_pct):
    title = "⚪ <b>إغلاق صفقة لونق</b>" if side == "LONG" else "⚪ <b>إغلاق صفقة شورت</b>"
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    reasons_ar = {"Stop Loss": "ضرب وقف الخسارة 🛑", "Take Profit": "تحقق هدف الربح 🎯", "EMA Cross": "كسر المتوسط المتحرك 📉"}
    ar_reason = reasons_ar.get(reason, reason)

    # حساب الربح مع الرافعة (تقريبي)
    leveraged_pnl = pnl_pct * LEVERAGE

    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"📥 سعر الدخول: <code>{fmt(entry)}</code>\n"
            f"📤 سعر الخروج: <code>{fmt(price)}</code>\n"
            f"📊 النتيجة (بدون رافعة): <code>{pnl_pct:+.2f}%</code>\n"
            f"🔥 <b>النتيجة بالرافعة ({LEVERAGE}x):</b> <code>{leveraged_pnl:+.2f}%</code>\n"
            f"📝 السبب: {ar_reason}")

# ==========================================
# 3. المؤشرات الفنية
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

    # إدارة الخروج
    if position == "LONG":
        if price <= sl: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
        if price <= last["ema21"]: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "EMA Cross"}
    if position == "SHORT":
        if price >= sl: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}
        if price >= last["ema21"]: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "EMA Cross"}

    if position is not None: return None

    # شروط الدخول
    cross_up   = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    vol_ok     = last["volume"] > 1.2 * last["vol_ma"]
    macd_up    = last["macd_hist"] > 0 and last["macd_hist"] > prev["macd_hist"]
    macd_down  = last["macd_hist"] < 0 and last["macd_hist"] < prev["macd_hist"]

    # فلتر الاتجاه: السعر يجب أن يكون فوق المتوسط 200 للونق، وتحته للشورت
    trend_up   = price > last["ema200"]
    trend_down = price < last["ema200"]

    # LONGS
    if (cross_up and trend_up and 40 < last["rsi"] < 70 and vol_ok and macd_up):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 2.0 * last["atr"], "tp": price + 4.0 * last["atr"], "rsi": last["rsi"]}

    # SHORTS
    if (cross_down and trend_down and 30 < last["rsi"] < 60 and vol_ok and macd_down):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 2.0 * last["atr"], "tp": price - 4.0 * last["atr"], "rsi": last["rsi"]}

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
            
            # حساب الخسارة اليومية بناءً على الرافعة
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
    print("[INIT] Starting Futures Bot with OKX...", flush=True)
    
    # إعداد المنصة لتعمل على العقود الآجلة (Swap) وليس الفوري
    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'} 
    })
    
    exchange.load_markets()

    send(f"🚀 <b>تم تشغيل بوت العقود الآجلة (Futures)</b>\n"
         f"➖➖➖➖➖➖➖➖\n"
         f"🪙 العملات المراقبة: <b>{len(FUTURES_SYMBOLS)} عملة</b>\n"
         f"⏱️ الفريم الزمني: <b>15 دقيقة</b>\n"
         f"⚙️ الرافعة المالية: <b>{LEVERAGE}x</b>\n"
         f"📈 الاستراتيجية: EMA + RSI + MACD + ATR\n"
         f"🛑 حد الخسارة اليومي: -{MAX_DAILY_LOSS}%\n"
         f"⚠️ <i>تأكد من إعداد الرافعة المالية في حسابك لتطابق البوت.</i>")

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
