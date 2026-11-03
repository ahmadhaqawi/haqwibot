import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
import io
import matplotlib
matplotlib.use('Agg') # مهم جداً للعمل على السيرفر بدون شاشة
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
# 1. الإعدادات والمتغيرات (Config)
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", 
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]

TIMEFRAMES = ["5m", "15m", "1h", "4h"]
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3"))
LEVERAGE = 5 

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام (نص + صورة)
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

def send_photo(photo_buf, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"photo": ("chart.png", photo_buf, "image/png")},
                timeout=15)
        except Exception as e:
            print(f"[TG ERROR] Failed to send photo to {cid}: {e}")

def fmt(p): 
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def is_good_trading_time():
    now = datetime.now(KSA_TZ)
    hour = now.hour
    if 2 <= hour < 7: return False
    return True

# دالة توليد الشارت
def generate_chart(symbol, df, timeframe, side, resistance, support):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(10, 5))
    
    # رسم السعر
    ax.plot(df['datetime'], df['close'], label='Close Price', color='#00BFFF', linewidth=2)
    ax.plot(df['datetime'], df['ema9'], label='EMA 9', color='#FFD700', linestyle='--', alpha=0.8)
    ax.plot(df['datetime'], df['ema21'], label='EMA 21', color='#FF4500', linestyle='--', alpha=0.8)
    
    # رسم المقاومة والدعم
    ax.axhline(resistance, color='#32CD32', linestyle='-', linewidth=1.5, label=f'Resistance ({fmt(resistance)})')
    ax.axhline(support, color='#FF1493', linestyle='-', linewidth=1.5, label=f'Support ({fmt(support)})')
    
    # تظليل منطقة الاختراق
    if side == "LONG":
        ax.axhline(resistance, xmin=0.8, xmax=1.0, color='#32CD32', linewidth=4, alpha=0.5) # خط الاختراق
    else:
        ax.axhline(support, xmin=0.8, xmax=1.0, color='#FF1493', linewidth=4, alpha=0.5)

    ax.set_title(f"{symbol} - {timeframe} | {side}", color='white', fontsize=14, fontweight='bold')
    ax.set_ylabel("Price (USDT)", color='white')
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.xticks(rotation=45, color='gray')
    plt.yticks(color='gray')
    ax.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.3)
    ax.legend(loc='upper left', facecolor='black', edgecolor='white')
    
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf

def msg_entry(sym, side, price, sl, tp, rsi, adx, signal_type, support, resistance):
    if side == "LONG":
        title = f"🟢 <b>توصية شراء (LONG) - {signal_type}</b>"
    else:
        title = f"🔴 <b>توصية بيع (SHORT) - {signal_type}</b>"
        
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    
    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"⏰ وقت الدخول: <code>{now_str}</code>\n"
            f"🟢 المقاومة: <code>{fmt(resistance)}</code>\n"
            f"🔴 الدعم: <code>{fmt(support)}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح (TP): <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 RSI: <code>{rsi:.1f}</code> | ADX: <code>{adx:.1f}</code>\n"
            f"💡 <i>بمجرد ربح 1%، قم بتعديل وقف الخسارة إلى سعر الدخول.</i>")

def msg_exit(sym, side, timeframe, price, entry, reason, pnl_pct, duration, entry_str, exit_str):
    title = f"⚪ <b>إغلاق صفقة لونق ({timeframe})</b>" if side == "LONG" else f"⚪ <b>إغلاق صفقة شورت ({timeframe})</b>"
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
# 3. المؤشرات الفنية
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
    df["highest_high_20"] = df["high"].rolling(20).max().shift(1)
    df["lowest_low_20"]   = df["low"].rolling(20).min().shift(1)
    return df

def check_signal(df_main, df_1h, timeframe, position, entry_price=None, sl=None, tp=None):
    if len(df_main) < 50 or len(df_1h) < 50: return None
    
    last_main = df_main.iloc[-2]      
    prev_main = df_main.iloc[-3]
    last_1h   = df_1h.iloc[-2]
    price = last_main["close"]

    if position == "LONG":
        if price <= sl: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp: return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
    if position == "SHORT":
        if price >= sl: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp: return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}

    if position is not None: return None
    if not is_good_trading_time(): return None

    trend_1h_up   = last_1h["close"] > last_1h["ema21"] and last_1h["close"] > last_1h["ema9"]
    trend_1h_down = last_1h["close"] < last_1h["ema21"] and last_1h["close"] < last_1h["ema9"]
    vol_ok = last_main["volume"] > 1.8 * last_main["vol_ma"]
    
    if timeframe in ["1h", "4h"]:
        adx_strong = last_main["adx"] > 32
    else:
        adx_strong = last_main["adx"] > 38

    ema_spread = abs(last_main["ema9"] - last_main["ema21"]) / price * 100 > 0.15
    atr_current = last_main["atr"]
    atr_avg = df_main["atr"].iloc[-20:].mean()
    volatility_ok = (0.75 * atr_avg) < atr_current < (1.9 * atr_avg)

    cross_up   = prev_main["ema9"] <= prev_main["ema21"] and last_main["ema9"] > last_main["ema21"]
    cross_down = prev_main["ema9"] >= prev_main["ema21"] and last_main["ema9"] < last_main["ema21"]

    if timeframe == "5m": atr_mult = 2.1
    elif timeframe == "15m": atr_mult = 2.4
    elif timeframe == "1h": atr_mult = 2.7
    else: atr_mult = 3.0

    # دعم ومقاومة
    support = last_main["lowest_low_20"]
    resistance = last_main["highest_high_20"]

    if (cross_up and trend_1h_up and adx_strong and vol_ok and ema_spread and volatility_ok and 46 < last_main["rsi"] < 63):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - atr_mult * last_main["atr"], "tp": price + (atr_mult * 2.3) * last_main["atr"],
                "rsi": last_main["rsi"], "adx": last_main["adx"], "type": f"ترند قوي {timeframe}",
                "support": support, "resistance": resistance}

    if (cross_down and trend_1h_down and adx_strong and vol_ok and ema_spread and volatility_ok and 37 < last_main["rsi"] < 54):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + atr_mult * last_main["atr"], "tp": price - (atr_mult * 2.3) * last_main["atr"],
                "rsi": last_main["rsi"], "adx": last_main["adx"], "type": f"ترند قوي {timeframe}",
                "support": support, "resistance": resistance}

    breakout_vol = last_main["volume"] > 3.2 * last_main["vol_ma"]
    if (price > resistance and breakout_vol and adx_strong and volatility_ok and 63 < last_main["rsi"] < 76 and trend_1h_up):
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - (atr_mult - 0.3) * last_main["atr"], "tp": price + (atr_mult * 2.1) * last_main["atr"],
                "rsi": last_main["rsi"], "adx": last_main["adx"], "type": f"اختراق قوي {timeframe} ⚡",
                "support": support, "resistance": resistance}

    if (price < support and breakout_vol and adx_strong and volatility_ok and 24 < last_main["rsi"] < 37 and trend_1h_down):
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + (atr_mult - 0.3) * last_main["atr"], "tp": price - (atr_mult * 2.1) * last_main["atr"],
                "rsi": last_main["rsi"], "adx": last_main["adx"], "type": f"اختراق قوي {timeframe} ⚡",
                "support": support, "resistance": resistance}

    return None

# ==========================================
# 4. الفحص الرئيسي
# ==========================================
def scan_symbol(exchange, symbol, timeframe):
    try:
        state_key = (symbol, timeframe)
        if symbol not in exchange.markets: return

        if state_key not in state:
            state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None, "entry_time": None}
            
        ohlcv_main = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=50)
        
        if len(ohlcv_main) < 50 or len(ohlcv_1h) < 50: return

        df_main = pd.DataFrame(ohlcv_main, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
        
        # تحويل الوقت للرسم
        df_main['datetime'] = pd.to_datetime(df_main['timestamp'], unit='ms')

        df_main = add_indicators(df_main)
        df_1h = add_indicators(df_1h)

        current_candle_ts = int(df_main.iloc[-2]["timestamp"])
        if state[state_key]["last_candle_ts"] == current_candle_ts: return
        state[state_key]["last_candle_ts"] = current_candle_ts

        pos = state[state_key]["position"]
        entry = state[state_key]["entry"]
        sl = state[state_key]["sl"]
        tp = state[state_key]["tp"]

        signal = check_signal(df_main, df_1h, timeframe, pos, entry, sl, tp)
        if not signal: return

        today = datetime.now(KSA_TZ).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS: return 

        if signal["action"] == "ENTRY":
            state[state_key].update({
                "position": signal["side"], "entry": signal["price"], 
                "sl": signal["sl"], "tp": signal["tp"],
                "entry_time": datetime.now(timezone.utc)
            })
            
            # توليد الشارت وإرسال الصورة
            chart_buf = generate_chart(symbol, df_main, timeframe, signal["side"], signal["resistance"], signal["support"])
            caption = msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"], signal["adx"], signal["type"], signal["support"], signal["resistance"])
            send_photo(chart_buf, caption)

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT": pnl = -pnl
            
            exit_time_utc = datetime.now(timezone.utc)
            entry_time_utc = state[state_key]["entry_time"]
            
            duration_str = "غير محدد"
            entry_str = "-"
            exit_str = "-"
            
            if entry_time_utc:
                time_diff = exit_time_utc - entry_time_utc
                total_seconds = int(time_diff.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                duration_str = f"{hours} ساعة و {minutes} دقيقة" if hours > 0 else f"{minutes} دقيقة"
                entry_str = entry_time_utc.astimezone(KSA_TZ).strftime("%I:%M %p")
                exit_str = exit_time_utc.astimezone(KSA_TZ).strftime("%I:%M %p")

            state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None, "entry_time": None})
            
            daily_pnl["value"] += (pnl * LEVERAGE) * RISK_PCT / 100
            send(msg_exit(symbol, pos, timeframe, signal["price"], entry, signal["reason"], pnl, duration_str, entry_str, exit_str))
            
            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nوصلت للحد الأقصى للخسارة اليومية.")

    except Exception as e:
        print(f"[ERROR] Scan {symbol} ({timeframe}): {e}", flush=True)

# ==========================================
# 5. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting V12 Bot (Charts & No Whales)...", flush=True)

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'} 
    })
    exchange.load_markets()

    send(f"🛡️ <b>تم تشغيل البوت (V12) - الشارت والدعم والمقاومة</b>\n"
         f"📈 <b>الفريمات:</b> 5m | 15m | 1h | 4h\n"
         f"📊 <b>الشارت:</b> يتم إرفاق صورة مع كل إشارة\n"
         f"🛑 <b>وقف الخسارة:</b> ديناميكي (ATR)\n"
         f"🧠 <b>فلاتر:</b> ADX + Volume + تقلب + وقت التداول\n"
         f"⚙️ الرافعة: <b>{LEVERAGE}x</b>")

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                for tf in TIMEFRAMES:
                    scan_symbol(exchange, sym, tf)
                    time.sleep(1) 
        except Exception as e:
            print(f"[FATAL] Loop error: {e}", flush=True)
        
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
