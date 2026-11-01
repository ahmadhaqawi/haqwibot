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

# قائمة أفضل 50 عملة من حيث السيولة (ثابتة لتجنب أخطاء الاتصال)
TOP_50_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "ADA/USDT", "AVAX/USDT", "DOGE/USDT", "TRX/USDT", "DOT/USDT",
    "LINK/USDT", "MATIC/USDT", "TON/USDT", "SHIB/USDT", "LTC/USDT",
    "BCH/USDT", "NEAR/USDT", "UNI/USDT", "ICP/USDT", "APT/USDT",
    "SUI/USDT", "FIL/USDT", "ARB/USDT", "OP/USDT", "IMX/USDT",
    "INJ/USDT", "RNDR/USDT", "HBAR/USDT", "ATOM/USDT", "STX/USDT",
    "VET/USDT", "MNT/USDT", "GRT/USDT", "FTM/USDT", "THETA/USDT",
    "LDO/USDT", "RUNE/USDT", "ALGO/USDT", "SEI/USDT", "QNT/USDT",
    "EGLD/USDT", "AAVE/USDT", "FLOW/USDT", "CHZ/USDT", "SAND/USDT",
    "MANA/USDT", "AXS/USDT", "SNX/USDT", "KAVA/USDT", "FET/USDT"
]

TIMEFRAME = os.getenv("TIMEFRAME", "5m")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))

# حالة البوت لكل زوج
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

# دالة تحليل تدفق السيولة والصفقات العالية
def analyze_market_flow(exchange, symbol):
    try:
        trades = exchange.fetch_trades(symbol, limit=500)
        if not trades: return "غير متاح"
        
        buy_vol = sum(t['amount'] for t in trades if t['side'] == 'buy')
        sell_vol = sum(t['amount'] for t in trades if t['side'] == 'sell')
        total_vol = buy_vol + sell_vol
        
        buy_ratio = (buy_vol / total_vol) * 100 if total_vol > 0 else 50
        
        if buy_ratio >= 60:
            flow_text = f"🟢 تدفق سيولة شراء قوي ({buy_ratio:.0f}%)"
        elif buy_ratio <= 40:
            flow_text = f"🔴 تدفق سيولة بيع قوي ({100-buy_ratio:.0f}%)"
        else:
            flow_text = f"⚖️ تدفق سيولة متوازن (شراء {buy_ratio:.0f}% | بيع {100-buy_ratio:.0f}%)"

        max_trade = max(trades, key=lambda t: t['cost'])
        max_trade_usdt = max_trade['cost']
        
        whale_text = ""
        if max_trade_usdt > 50000:
            whale_text = f"\n🐋 <b>صفقة حيتان:</b> <code>${max_trade_usdt:,.0f}</code>"
            
        return flow_text + whale_text
    except Exception as e:
        print(f"[FLOW ERROR] {symbol}: {e}")
        return "تعذر جلب البيانات"

def msg_entry(sym, side, price, sl, tp, rsi, flow_data):
    if side == "LONG":
        title = "🟢 <b>إشارة صفقة شراء (LONG)</b>"
    else:
        title = "🔴 <b>إشارة صفقة بيع (SHORT)</b>"
        
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    
    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"🛑 وقف الخسارة (SL): <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح (TP): <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 مؤشر RSI: <code>{rsi:.1f}</code>\n"
            f"⚖️ الربح للخسارة: <code>1:{reward/risk:.1f}</code>\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"{flow_data}")

def msg_exit(sym, side, price, entry, reason, pnl_pct):
    if side == "LONG":
        title = "⚪ <b>إغلاق صفقة شراء (LONG)</b>"
    else:
        title = "⚪ <b>إغلاق صفقة بيع (SHORT)</b>"
        
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    
    reasons_ar = {
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Take Profit": "تحقق هدف الربح 🎯",
        "EMA Cross": "كسر المتوسط المتحرك 📉",
    }
    ar_reason = reasons_ar.get(reason, reason)

    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"📥 سعر الدخول: <code>{fmt(entry)}</code>\n"
            f"📤 سعر الخروج: <code>{fmt(price)}</code>\n"
            f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code> ({status})\n"
            f"📝 السبب: {ar_reason}")

# ==========================================
# 3. المؤشرات الفنية والاستراتيجية
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
# 4. دالة الفحص الرئيسية
# ==========================================
def scan_symbol(exchange, symbol):
    try:
        if symbol not in state:
            state[symbol] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None}
            
        ohlcv_5m = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=300)
        ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=300)
        
        if len(ohlcv_5m) < 210 or len(ohlcv_1h) < 200:
            return

        df_5m = pd.DataFrame(ohlcv_5m, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df_5m = add_indicators(df_5m)
        df_1h = add_indicators(df_1h)

        last_1h = df_1h.iloc[-2]
        htf_trend = "UP" if last_1h["close"] > last_1h["ema200"] else "DOWN"

        current_candle_ts = int(df_5m.iloc[-2]["timestamp"])
        if state[symbol]["last_candle_ts"] == current_candle_ts:
            return
        state[symbol]["last_candle_ts"] = current_candle_ts

        pos = state[symbol]["position"]
        entry = state[symbol]["entry"]
        sl = state[symbol]["sl"]
        tp = state[symbol]["tp"]

        signal = check_signal(df_5m, htf_trend, pos, entry, sl, tp)

        if not signal: return

        today = datetime.now(timezone.utc).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return 

        if signal["action"] == "ENTRY":
            state[symbol].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"]})
            flow_data = analyze_market_flow(exchange, symbol)
            send(msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"], flow_data))

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT": pnl = -pnl
            state[symbol].update({"position": None, "entry": None, "sl": None, "tp": None})
            daily_pnl["value"] += pnl * RISK_PCT / 100
            
            send(msg_exit(symbol, pos, signal["price"], entry, signal["reason"], pnl))
            
            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nلقد وصلت للحد الأقصى للخسارة اليومية (-{MAX_DAILY_LOSS}%).")

    except Exception as e:
        print(f"[ERROR] Scan {symbol}: {e}", flush=True)

# ==========================================
# 5. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting bot with ccxt...", flush=True)
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })

    send("🚀 <b>تم تشغيل بوت التداول بنجاح</b>\n"
         "➖➖➖➖➖➖➖➖\n"
         f"🪙 عدد العملات المراقبة: <b>{len(TOP_50_SYMBOLS)} عملة</b>\n"
         f"🧠 الاستراتيجية: تقاطع EMA + RSI + MACD\n"
         f"📈 فلتر الاتجاه: EMA 200 (فريم الساعة)\n"
         f"🐋 مراقبة الحيتان: <b>مفعلة</b>\n"
         f"💰 إدارة المخاطر: {RISK_PCT}% لكل صفقة\n"
         f"🛑 حد الخسارة اليومي: -{MAX_DAILY_LOSS}%")

    while True:
        try:
            for sym in TOP_50_SYMBOLS:
                scan_symbol(exchange, sym)
                time.sleep(1) # تأخير ثانية بين كل عملة
                
        except Exception as e:
            print(f"[FATAL] Loop error: {e}", flush=True)
        
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()