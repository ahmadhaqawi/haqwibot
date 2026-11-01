import os
import time
import threading
import traceback
from datetime import datetime, timezone
import pandas as pd
import requests
from flask import Flask
from binance.client import Client
from binance import ThreadedWebsocketManager

# ==========================================
# 1. الإعدادات والمتغيرات (Config)
# ==========================================
TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(",")]
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))

# حالة البوت لكل زوج
state = {
    sym: {"position": None, "entry": None, "sl": None, "tp": None,
          "htf_trend": None, "candles_5m": [], "candles_1h": []}
    for sym in SYMBOLS
}
daily_pnl = {"value": 0.0, "date": datetime.now(timezone.utc).date()}
lock = threading.Lock()

# ==========================================
# 2. سيرفر Flask (لإبقاء الخدمة مستيقظة على Render)
# ==========================================
app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health():
    return {"status": "ok", "service": "bot_active"}, 200

def run_flask():
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)), use_reloader=False)

# ==========================================
# 3. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
    if not TOKEN or not CHAT_ID: 
        print("[TG ERROR] Token or Chat ID missing.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10)
    except Exception as e:
        print(f"[TG ERROR] {e}")

def fmt(p): 
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def msg_entry(sym, side, price, sl, tp, rsi):
    ar_side = "شراء" if side == "LONG" else "بيع"
    emoji = "🟢" if side == "LONG" else "🔴"
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    
    return (f"{emoji} <b>إشارة دخول {ar_side}</b>\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 مؤشر RSI: <code>{rsi:.1f}</code>\n"
            f"⚖️ الربح للخسارة: <code>1:{reward/risk:.1f}</code>")

def msg_exit(sym, side, price, entry, reason, pnl_pct):
    ar_side = "شراء" if side == "LONG" else "بيع"
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    
    reasons_ar = {
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Take Profit": "تحقق هدف الربح 🎯",
        "EMA Cross": "كسر المتوسط المتحرك 📉",
    }
    ar_reason = reasons_ar.get(reason, reason)

    return (f"⚪ <b>إغلاق صفقة {ar_side}</b>\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"📥 سعر الدخول: <code>{fmt(entry)}</code>\n"
            f"📤 سعر الخروج: <code>{fmt(price)}</code>\n"
            f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code> ({status})\n"
            f"📝 السبب: {ar_reason}")

# ==========================================
# 4. المؤشرات الفنية والاستراتيجية
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

def check_signal(df, htf_trend, position, entry_price=None, sl=None, tp=None):
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
    vol_ok    = last["volume"] > 1.2 * last["vol_ma"]
    macd_up   = last["macd_hist"] > 0 and last["macd_hist"] > prev["macd_hist"]
    macd_down = last["macd_hist"] < 0 and last["macd_hist"] < prev["macd_hist"]

    if (cross_up and htf_trend == "UP" and 40 < last["rsi"] < 70 and vol_ok and macd_up):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 1.5 * last["atr"], "tp": price + 3.0 * last["atr"], "rsi": last["rsi"]}

    if (cross_down and htf_trend == "DOWN" and 30 < last["rsi"] < 60 and vol_ok and macd_down):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 1.5 * last["atr"], "tp": price - 3.0 * last["atr"], "rsi": last["rsi"]}

    return None

# ==========================================
# 5. جلب البيانات ومعالجة الشموع اللحظية (مع تخطي الحظر الجغرافي)
# ==========================================
# استخدام data-api.binance.vision يتجاوز حظر Render الأمريكي
client = Client()
client.API_URL = 'https://data-api.binance.vision/api'

def fetch_initial(sym):
    k5  = client.get_klines(symbol=sym, interval="5m",  limit=300)
    k1h = client.get_klines(symbol=sym, interval="1h",  limit=300)
    def to_df(ks):
        return pd.DataFrame(ks, columns=["t","open","high","low","close","volume","ct","qv","n","tb","tq","i"]).astype({"open":float,"high":float,"low":float,"close":float,"volume":float})
    with lock:
        state[sym]["candles_5m"] = to_df(k5).to_dict("records")
        state[sym]["candles_1h"] = to_df(k1h).to_dict("records")

def update_trend(sym):
    with lock:
        df = pd.DataFrame(state[sym]["candles_1h"])
    if len(df) < 200: return
    df = add_indicators(df)
    last = df.iloc[-2]
    trend = "UP" if last["close"] > last["ema200"] else "DOWN"
    with lock:
        state[sym]["htf_trend"] = trend

def on_kline(msg):
    try:
        if msg["k"]["x"] is not True: return
        sym = msg["s"]
        k = msg["k"]
        candle = {"t": k["t"], "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"])}
        with lock:
            state[sym]["candles_5m"].append(candle)
            if len(state[sym]["candles_5m"]) > 500:
                state[sym]["candles_5m"] = state[sym]["candles_5m"][-500:]
            df = pd.DataFrame(state[sym]["candles_5m"])
            pos, entry, sl, tp, trend = state[sym]["position"], state[sym]["entry"], state[sym]["sl"], state[sym]["tp"], state[sym]["htf_trend"]

        df = add_indicators(df)
        signal = check_signal(df, trend, pos, entry, sl, tp)

        if not signal: return

        today = datetime.now(timezone.utc).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return 

        if signal["action"] == "ENTRY":
            with lock:
                state[sym].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"]})
            send(msg_entry(sym, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"]))

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT": pnl = -pnl
            with lock:
                state[sym].update({"position": None, "entry": None, "sl": None, "tp": None})
                daily_pnl["value"] += pnl * RISK_PCT / 100
            
            send(msg_exit(sym, pos, signal["price"], entry, signal["reason"], pnl))
            
            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nلقد وصلت للحد الأقصى للخسارة اليومية (-{MAX_DAILY_LOSS}%).\nلن يتم إرسال إشارات جديدة حتى الغد.")

    except Exception:
        print("[KLINE ERROR]", traceback.format_exc())

def on_1h(msg):
    try:
        if msg["k"]["x"] is not True: return
        sym = msg["s"]
        k = msg["k"]
        with lock:
            state[sym]["candles_1h"].append({"t": k["t"], "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"])})
            if len(state[sym]["candles_1h"]) > 500:
                state[sym]["candles_1h"] = state[sym]["candles_1h"][-500:]
        update_trend(sym)
    except Exception:
        print("[1H ERROR]", traceback.format_exc())

# ==========================================
# 6. التشغيل الرئيسي
# ==========================================
def main():
    print("[INIT] Loading initial data...")
    for s in SYMBOLS:
        fetch_initial(s)
        update_trend(s)

    # استخدام مسار البث العام لـ Binance لتجاوز الحظر الجغرافي
    twm = ThreadedWebsocketManager(client=client)
    twm.stream_url = 'wss://data-stream.binance.vision'
    twm.start()

    for s in SYMBOLS:
        twm.start_kline_socket(callback=on_kline, symbol=s, interval="5m")
        twm.start_kline_socket(callback=on_1h,   symbol=s, interval="1h")

    send("🚀 <b>تم تشغيل بوت التداول بنجاح</b>\n"
         "➖➖➖➖➖➖➖➖\n"
         f"🪙 الأزواج المراقبة: <code>{', '.join(SYMBOLS)}</code>\n"
         f"🧠 الاستراتيجية: تقاطع EMA + RSI + MACD\n"
         f"📈 فلتر الاتجاه: EMA 200 (فريم الساعة)\n"
         f"💰 إدارة المخاطر: {RISK_PCT}% لكل صفقة\n"
         f"🛑 حد الخسارة اليومي: -{MAX_DAILY_LOSS}%")

    while True:
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    main()