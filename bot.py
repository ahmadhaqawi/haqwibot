import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
# 1. الإعدادات (نسخة التجربة السريعة)
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# تقليل العملات مؤقتاً لتسريع التجربة
FUTURES_SYMBOLS = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "DOGE/USDT:USDT", "XRP/USDT:USDT"]

TIMEFRAMES = ["5m", "15m"]
SCAN_INTERVAL = 60
LEVERAGE = 5 
state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال الرسائل
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e: print(f"[TG ERROR] {e}")

def send_photo(photo_buf, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"photo": ("chart.png", photo_buf, "image/png")}, timeout=15)
        except Exception as e: print(f"[TG PHOTO ERROR] {e}")

def fmt(p): return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

# ==========================================
# 3. توليد الشارت (مبسط)
# ==========================================
def generate_chart(symbol, df, timeframe, side, resistance, support):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df['datetime'], df['close'], label='Close', color='#00BFFF', linewidth=2)
    ax.axhline(resistance, color='#32CD32', linestyle='-', label=f'Res ({fmt(resistance)})')
    ax.axhline(support, color='#FF1493', linestyle='-', label=f'Sup ({fmt(support)})')
    ax.set_title(f"{symbol} - {timeframe} | {side}", color='white')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45, color='gray')
    plt.yticks(color='gray')
    ax.legend(loc='upper left', facecolor='black', edgecolor='white')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# 4. المؤشرات
# ==========================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()
def rsi(s, p=14):
    d = s.diff(); g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g/(l+1e-9)))
def atr(df, p=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    df["ema9"] = ema(df["close"], 9); df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"]); df["atr"] = atr(df)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max().shift(1)
    df["low20"] = df["low"].rolling(20).min().shift(1)
    return df

# ==========================================
# 5. فلترة مبسطة جداً للتجربة
# ==========================================
def check_signal(df_main, timeframe, position, entry_price=None, sl=None, tp=None):
    if len(df_main) < 50: return None
    last = df_main.iloc[-2]
    prev = df_main.iloc[-3]
    price = last["close"]

    if position == "LONG":
        if price <= sl: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
    if position == "SHORT":
        if price >= sl: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}

    if position is not None: return None

    # شروط مخففة جداً (فقط تقاطع المتوسطات)
    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    support = last["low20"]
    resistance = last["high20"]

    if cross_up:
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 1.5 * last["atr"], "tp": price + 3.0 * last["atr"],
                "rsi": last["rsi"], "adx": 0, "type": f"تجربة {timeframe}",
                "support": support, "resistance": resistance}

    if cross_down:
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 1.5 * last["atr"], "tp": price - 3.0 * last["atr"],
                "rsi": last["rsi"], "adx": 0, "type": f"تجربة {timeframe}",
                "support": support, "resistance": resistance}

    return None

# ==========================================
# 6. الفحص
# ==========================================
def scan_symbol(exchange, symbol, timeframe):
    try:
        state_key = (symbol, timeframe)
        if symbol not in exchange.markets: return
        if state_key not in state:
            state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None, "entry_time": None}
            
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        if len(ohlcv) < 50: return

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = add_indicators(df)

        current_candle_ts = int(df.iloc[-2]["timestamp"])
        if state[state_key]["last_candle_ts"] == current_candle_ts: return
        state[state_key]["last_candle_ts"] = current_candle_ts

        pos = state[state_key]["position"]
        entry = state[state_key]["entry"]
        sl = state[state_key]["sl"]
        tp = state[state_key]["tp"]

        signal = check_signal(df, timeframe, pos, entry, sl, tp)
        if not signal: return

        if signal["action"] == "ENTRY":
            state[state_key].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"], "entry_time": datetime.now(timezone.utc)})
            
            chart_buf = generate_chart(symbol, df, timeframe, signal["side"], signal["resistance"], signal["support"])
            caption = (f"🟢 <b>إشارة تجريبية ({timeframe})</b>\n"
                       f"🪙 الزوج: {symbol}\n"
                       f"💵 الدخول: {fmt(signal['price'])}\n"
                       f"🛑 SL: {fmt(signal['sl'])}\n"
                       f"🎯 TP: {fmt(signal['tp'])}\n"
                       f"🟢 مقاومة: {fmt(signal['resistance'])}\n"
                       f"🔴 دعم: {fmt(signal['support'])}")
            send_photo(chart_buf, caption)

        elif signal["action"] == "EXIT":
            state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None, "entry_time": None})
            send(f"⚪ <b>إغلاق صفقة تجريبية</b>\nالسبب: {signal['reason']}")

    except Exception as e:
        print(f"[ERROR] {symbol} ({timeframe}): {e}", flush=True)

# ==========================================
# 7. التشغيل
# ==========================================
def trading_loop():
    print("[INIT] Starting TEST Bot...", flush=True)
    exchange = ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
    exchange.load_markets()

    # رسالة اختبار فورية
    send("🚨 <b>اختبار الاتصال:</b> البوت يعمل الآن وسيبدأ فحص السوق (نسخة تجريبية)!")

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                for tf in TIMEFRAMES:
                    scan_symbol(exchange, sym, tf)
                    time.sleep(1) 
        except Exception as e:
            print(f"[FATAL] {e}", flush=True)
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
