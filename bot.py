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
import matplotlib.patches as mpatches
import numpy as np
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
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT",
    "PEPE/USDT:USDT", "WIF/USDT:USDT", "TIA/USDT:USDT", "ORDI/USDT:USDT",
    "SEI/USDT:USDT", "RUNE/USDT:USDT", "FET/USDT:USDT", "RNDR/USDT:USDT",
    "IMX/USDT:USDT", "STX/USDT:USDT", "THETA/USDT:USDT", "ALGO/USDT:USDT",
    "EGLD/USDT:USDT", "FLOW/USDT:USDT", "SAND/USDT:USDT"
]

TIMEFRAMES = ["5m", "15m", "1h", "4h"]
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
LEVERAGE = 5 

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
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

def is_good_trading_time():
    now = datetime.now(KSA_TZ)
    if 2 <= now.hour < 7: return False
    return True

# ==========================================
# 3. توليد الشارت (شموع + خط ترند + أهداف)
# ==========================================
def generate_chart(symbol, df, timeframe, side, resistance, support, entry, sl, tp):
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(12, 6))
    
    df_plot = df.tail(40).reset_index(drop=True)
    
    # رسم الشموع
    for i in range(len(df_plot)):
        row = df_plot.iloc[i]
        color = '#26a69a' if row['close'] >= row['open'] else '#ef5350'
        ax.plot([i, i], [row['low'], row['high']], color=color, linewidth=1)
        height = abs(row['close'] - row['open'])
        bottom = min(row['close'], row['open'])
        rect = mpatches.Rectangle((i - 0.3, bottom), 0.6, height, color=color)
        ax.add_patch(rect)
    
    # --- حساب ورسم خط الترند ---
    # نقسم الشارت إلى نصفين للبحث عن القيعان والقمم
    half_idx = len(df_plot) // 2
    first_half = df_plot.iloc[:half_idx]
    second_half = df_plot.iloc[half_idx:]
    
    # خط الترند الصاعد (يربط أدنى قاع في النصف الأول بأدنى قاع في النصف الثاني)
    if side == "LONG":
        p1_idx = first_half['low'].idxmin()
        p1_val = first_half['low'].min()
        p2_idx = second_half['low'].idxmin()
        p2_val = second_half['low'].min()
        
        # حساب ميل الخط وتمديده لآخر الشارت
        slope = (p2_val - p1_val) / (p2_idx - p1_idx) if p2_idx != p1_idx else 0
        end_idx = len(df_plot) - 1
        end_val = p1_val + slope * (end_idx - p1_idx)
        
        ax.plot([p1_idx, end_idx], [p1_val, end_val], color='#FFD700', linewidth=2, linestyle='-', label='Uptrend Line')
    
    # خط الترند الهابط (يربط أعلى قمة في النصف الأول بأعلى قمة في النصف الثاني)
    elif side == "SHORT":
        p1_idx = first_half['high'].idxmax()
        p1_val = first_half['high'].max()
        p2_idx = second_half['high'].idxmax()
        p2_val = second_half['high'].max()
        
        slope = (p2_val - p1_val) / (p2_idx - p1_idx) if p2_idx != p1_idx else 0
        end_idx = len(df_plot) - 1
        end_val = p1_val + slope * (end_idx - p1_idx)
        
        ax.plot([p1_idx, end_idx], [p1_val, end_val], color='#FFD700', linewidth=2, linestyle='-', label='Downtrend Line')

    # حساب الحد الأعلى والأدنى للشارت ليشمل كل الخطوط
    max_price = max(df_plot['high'].max(), resistance, tp, sl, entry)
    min_price = min(df_plot['low'].min(), support, tp, sl, entry)
    price_range = max_price - min_price
    ax.set_ylim(min_price - (price_range * 0.1), max_price + (price_range * 0.1))
    
    # رسم الخطوط الأفقية
    ax.axhline(resistance, color='#32CD32', linestyle='--', linewidth=1.5)
    ax.axhline(support, color='#FF1493', linestyle='--', linewidth=1.5)
    ax.axhline(entry, color='#00BFFF', linestyle='-', linewidth=2, alpha=0.8)
    ax.axhline(tp, color='#00FF00', linestyle=':', linewidth=2)
    ax.axhline(sl, color='#FF0000', linestyle=':', linewidth=2)
    
    # كتابة الأرقام على الشارت
    x_pos = len(df_plot) - 1
    ha_align = 'right'
    
    ax.text(x_pos, resistance, f" Resistance: {fmt(resistance)} ", color='#32CD32', va='bottom', ha=ha_align, fontweight='bold', fontsize=9)
    ax.text(x_pos, support, f" Support: {fmt(support)} ", color='#FF1493', va='top', ha=ha_align, fontweight='bold', fontsize=9)
    ax.text(x_pos, entry, f" ENTRY: {fmt(entry)} ", color='#00BFFF', va='center', ha=ha_align, fontweight='bold', fontsize=10)
    ax.text(x_pos, tp, f" TP: {fmt(tp)} ", color='#00FF00', va='bottom', ha=ha_align, fontweight='bold', fontsize=9)
    ax.text(x_pos, sl, f" SL: {fmt(sl)} ", color='#FF0000', va='top', ha=ha_align, fontweight='bold', fontsize=9)
    
    # تنسيق الشارت
    ax.set_title(f"{symbol} - {timeframe} | {side}", color='white', fontsize=14, fontweight='bold')
    ax.set_ylabel("Price (USDT)", color='white')
    
    step = max(1, len(df_plot) // 8)
    ticks = range(0, len(df_plot), step)
    tick_labels = [df_plot.iloc[i]['datetime'].strftime('%H:%M') for i in ticks]
    ax.set_xticks(ticks)
    ax.set_xticklabels(tick_labels, rotation=45, color='gray')
    
    plt.yticks(color='gray')
    ax.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.3)
    
    # إضافة وسيلة إيضاح صغيرة لخط الترند
    ax.legend(loc='upper left', facecolor='black', edgecolor='white')
    
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# 4. صياغة الرسائل والمؤشرات
# ==========================================
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

def msg_exit(sym, side, timeframe, price, entry, reason):
    title = f"⚪ <b>إغلاق صفقة ({timeframe})</b>"
    reasons_ar = {"Stop Loss": "ضرب وقف الخسارة 🛑", "Take Profit": "تحقق هدف الربح 🎯"}
    ar_reason = reasons_ar.get(reason, reason)
    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"📥 الدخول: <code>{fmt(entry)}</code>\n"
            f"📤 الخروج: <code>{fmt(price)}</code>\n"
            f"📝 السبب: {ar_reason}")

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
    if not is_good_trading_time(): return None

    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    support = last["low20"]
    resistance = last["high20"]

    if cross_up:
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price - 1.5 * last["atr"], "tp": price + 3.0 * last["atr"],
                "rsi": last["rsi"], "adx": 0, "type": f"شموع {timeframe}",
                "support": support, "resistance": resistance}

    if cross_down:
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price + 1.5 * last["atr"], "tp": price - 3.0 * last["atr"],
                "rsi": last["rsi"], "adx": 0, "type": f"شموع {timeframe}",
                "support": support, "resistance": resistance}

    return None

# ==========================================
# 5. الفحص الرئيسي
# ==========================================
def scan_symbol(exchange, symbol, timeframe):
    try:
        state_key = (symbol, timeframe)
        if symbol not in exchange.markets: return
        if state_key not in state:
            state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None, "last_candle_ts": None}
            
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
            state[state_key].update({"position": signal["side"], "entry": signal["price"], "sl": signal["sl"], "tp": signal["tp"]})
            
            chart_buf = generate_chart(symbol, df, timeframe, signal["side"], signal["resistance"], signal["support"], signal["price"], signal["sl"], signal["tp"])
            caption = msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"], signal["adx"], signal["type"], signal["support"], signal["resistance"])
            send_photo(chart_buf, caption)

        elif signal["action"] == "EXIT":
            state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None})
            send(msg_exit(symbol, pos, timeframe, signal["price"], entry, signal["reason"]))

    except Exception as e:
        print(f"[ERROR] {symbol} ({timeframe}): {e}", flush=True)

# ==========================================
# 6. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting V18 Bot (Trendline Added)...", flush=True)
    exchange = ccxt.okx({'enableRateLimit': True, 'options': {'defaultType': 'swap'}})
    exchange.load_markets()

    send("🛡️ <b>تم تشغيل البوت (V18)</b>\n"
         "📊 <b>جديد:</b> تم إضافة خط الترند الأصفر (Trendline) لجميع الشارتات.")

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
