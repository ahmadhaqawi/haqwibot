import os
import threading
import time
import json
from datetime import datetime, timezone, timedelta
import io
import gc
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import ccxt
import pandas as pd
import requests
from flask import Flask

app = Flask(__name__)

# ==========================================
# الإعدادات
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_SECRET = os.getenv("OKX_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", os.getenv("OKX_PASSWORD", ""))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
    "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT", "DOT/USDT:USDT",
    "NEAR/USDT:USDT", "APT/USDT:USDT", "SUI/USDT:USDT", "ARB/USDT:USDT",
    "OP/USDT:USDT", "INJ/USDT:USDT", "ATOM/USDT:USDT", "LTC/USDT:USDT",
    "BCH/USDT:USDT", "FIL/USDT:USDT", "SEI/USDT:USDT"
]

TIMEFRAMES = ["15m", "1h", "4h"]
SCAN_INTERVAL = 60
LEVERAGE = 5
POSITION_SIZE_USDT = 50
MAX_OPEN_POSITIONS = 5  # ✅ تم إرجاعها إلى 5 صفقات لمراقبة التحليل
MARGIN_MODE = "isolated"
REENTRY_COOLDOWN_SEC = 1800

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")

plt.style.use('dark_background')

state_lock = threading.Lock()
state = {}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}
active_symbols_cache = []

# ==========================================
# Flask
# ==========================================
@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_v27_safe"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# إدارة الحالة
# ==========================================
def load_state():
    global state, daily_pnl
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                loaded = {}
                for k, v in data.get("state", {}).items():
                    parts = k.split("|")
                    if len(parts) == 2:
                        loaded[(parts[0], parts[1])] = v
                with state_lock:
                    state = loaded
                    daily_pnl = data.get("daily_pnl", daily_pnl)
        except Exception as e:
            print(f"[STATE ERROR] {e}")

def save_state():
    try:
        with state_lock:
            data = {"state": {f"{k[0]}|{k[1]}": v for k, v in state.items()}, "daily_pnl": daily_pnl}
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE ERROR] {e}")

# ==========================================
# تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    for cid in [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception: pass

def send_photo(photo_buf, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    for cid in [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]:
        try:
            photo_buf.seek(0)
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"photo": ("chart.png", photo_buf, "image/png")}, timeout=15)
        except Exception: pass

def fmt(p):
    if p is None or (isinstance(p, float) and np.isnan(p)): return "0.00"
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

# ==========================================
# الشارت
# ==========================================
def generate_chart(symbol, df, timeframe, side, resistance, support, entry, sl, tp):
    try:
        fig, ax = plt.subplots(figsize=(12, 6))
        df_plot = df.tail(40).reset_index(drop=True)
        for i in range(len(df_plot)):
            row = df_plot.iloc[i]
            color = '#26a69a' if row['close'] >= row['open'] else '#ef5350'
            ax.plot([i, i], [row['low'], row['high']], color=color, linewidth=1)
            ax.add_patch(mpatches.Rectangle((i - 0.3, min(row['close'], row['open'])), 0.6, abs(row['close'] - row['open']), color=color))
        if resistance and not np.isnan(resistance): ax.axhline(resistance, color='#32CD32', linestyle='--', linewidth=1.5)
        if support and not np.isnan(support): ax.axhline(support, color='#FF1493', linestyle='--', linewidth=1.5)
        ax.axhline(entry, color='#00BFFF', linestyle='-', linewidth=2)
        ax.axhline(tp, color='#00FF00', linestyle=':', linewidth=2)
        ax.axhline(sl, color='#FF0000', linestyle=':', linewidth=2)
        ax.set_title(f"{symbol.split(':')[0]} - {timeframe} | {side}", color='white')
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=110)
        buf.seek(0)
        plt.clf(); plt.close('all'); gc.collect()
        return buf
    except Exception: return None

# ==========================================
# الرسائل
# ==========================================
def msg_entry(sym, side, price, sl, tp, rsi, adx, real_order):
    title = f"🟢 <b>شراء (LONG)</b>" if side == "LONG" else f"🔴 <b>بيع (SHORT)</b>"
    status = "✅ تم فتح الصفقة آلياً" if real_order else "إشارة فقط"
    return (f"{title} | {status}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"💵 الدخول: <code>{fmt(price)}</code>\n"
            f"🛑 SL: <code>{fmt(sl)}</code>\n"
            f"🎯 TP: <code>{fmt(tp)}</code>\n"
            f"📊 RSI: {rsi:.1f} | ADX: {adx:.1f}")

def msg_exit(sym, side, timeframe, exit_price, entry_price, reason):
    raw = ((exit_price - entry_price) / entry_price * 100) if side == "LONG" else ((entry_price - exit_price) / entry_price * 100)
    lev = raw * LEVERAGE
    icon = "🟢" if lev > 0 else "🔴"
    reasons = {"Take Profit": "تحقق الهدف 🎯", "Stop Loss": "ضرب وقف الخسارة 🛑", "Breakeven Hit": "خروج آمن 🛡️"}
    return (f"{icon} <b>إغلاق صفقة ({timeframe})</b>\n"
            f"🪙 {sym.split(':')[0]} ({side})\n"
            f"📥 الدخول: <code>{fmt(entry_price)}</code> | 📤 الخروج: <code>{fmt(exit_price)}</code>\n"
            f"📈 النتيجة: <b>{lev:+.2f}%</b>\n"
            f"📝 السبب: {reasons.get(reason, reason)}")

# ==========================================
# المؤشرات
# ==========================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()
def rsi(s, p=14):
    d = s.diff(); g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean(); l = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g / (l + 1e-9)))
def adx(high, low, close, p=14):
    up, down = high.diff(), -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0); minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/p, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    return (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9) * 100).ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    if len(df) < 30: return df
    df = df.copy()
    df["ema9"] = ema(df["close"], 9); df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"]); df["adx"] = adx(df["high"], df["low"], df["close"])
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max().shift(1); df["low20"] = df["low"].rolling(20).min().shift(1)
    return df

# ==========================================
# OKX Trading
# ==========================================
def create_exchange():
    ex = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_SECRET, 'password': OKX_PASSPHRASE,
                   'enableRateLimit': True, 'options': {'defaultType': 'swap'}, 'timeout': 20000})
    ex.set_sandbox_mode(True)
    return ex

def get_current_price(ex, sym):
    try: return float(ex.fetch_ticker(sym)['last'])
    except Exception: return None

def calculate_amount(ex, sym, usdt, price, lev):
    try:
        market = ex.market(sym)
        raw = (usdt * lev) / (price * float(market.get('contractSize', 1)))
        amt = float(ex.amount_to_precision(sym, raw))
        if amt < float(market.get('limits', {}).get('amount', {}).get('min', 1.0)): return 0.0
        return amt
    except Exception: return 0.0

def cancel_pending(ex, sym):
    try:
        market = ex.market(sym)
        algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
        for o in (algo.get('data') or []):
            try: ex.privatePostTradeCancelAlgos([{'instId': market['id'], 'algoId': o['algoId']}])
            except Exception: pass
        for o in ex.fetch_open_orders(sym):
            try: ex.cancel_order(o['id'], sym)
            except Exception: pass
    except Exception: pass

def place_sl_tp(ex, sym, side, amt, trigger, is_sl):
    trig_str = str(ex.price_to_precision(sym, trigger))
    amt_str = str(ex.amount_to_precision(sym, amt))
    market = ex.market(sym)
    req = {'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': side, 'posSide': 'net',
           'ordType': 'conditional', 'sz': amt_str, 'reduceOnly': 'true'}
    if is_sl: req.update({'slTriggerPx': trig_str, 'slOrdPx': '-1'})
    else: req.update({'tpTriggerPx': trig_str, 'tpOrdPx': '-1'})
    return ex.privatePostTradeOrderAlgo(req)

def open_position(ex, sym, side, price, sl, tp):
    try:
        amount = calculate_amount(ex, sym, POSITION_SIZE_USDT, price, LEVERAGE)
        if amount <= 0: return False, "كمية غير صالحة"
        
        try: ex.set_leverage(LEVERAGE, sym, params={'mgnMode': MARGIN_MODE})
        except Exception: pass
        cancel_pending(ex, sym)
        
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'
        
        order = ex.create_order(sym, 'market', order_side, amount, None, {'tdMode': MARGIN_MODE, 'posSide': 'net'})
        time.sleep(0.5)
        
        try: place_sl_tp(ex, sym, close_side, amount, sl, True)
        except Exception as e: print(f"[SL ERR] {e}")
        try: place_sl_tp(ex, sym, close_side, amount, tp, False)
        except Exception as e: print(f"[TP ERR] {e}")
        
        return True, order.get('id')
    except Exception as e:
        return False, str(e)

def close_position(ex, sym):
    try:
        cancel_pending(ex, sym)
        for p in ex.fetch_positions([sym]):
            if float(p.get('contracts', 0)) > 0:
                side = 'sell' if p['side'] == 'long' else 'buy'
                ex.create_order(sym, 'market', side, abs(float(p['contracts'])), None, {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        return True
    except Exception: return False

# ==========================================
# منطق الإشارة
# ==========================================
def check_signal(df_15m, df_1h, df_4h, timeframe, position, entry=None, sl=None, tp=None, be_hit=False):
    if len(df_15m) < 50 or len(df_1h) < 50 or len(df_4h) < 50: return None
    last_15m, current_15m = df_15m.iloc[-2], df_15m.iloc[-1]
    last_1h, last_4h = df_1h.iloc[-2], df_4h.iloc[-2]

    if position == "LONG":
        if current_15m["high"] >= tp: return {"action": "EXIT", "side": "LONG", "price": tp, "reason": "Take Profit"}
        if current_15m["low"] <= sl: return {"action": "EXIT", "side": "LONG", "price": sl, "reason": "Breakeven Hit" if be_hit else "Stop Loss"}
        if not be_hit and current_15m["high"] >= entry + (tp - entry) * 0.5: return {"action": "UPDATE_SL", "new_sl": entry}
    elif position == "SHORT":
        if current_15m["low"] <= tp: return {"action": "EXIT", "side": "SHORT", "price": tp, "reason": "Take Profit"}
        if current_15m["high"] >= sl: return {"action": "EXIT", "side": "SHORT", "price": sl, "reason": "Breakeven Hit" if be_hit else "Stop Loss"}
        if not be_hit and current_15m["low"] <= entry - (entry - tp) * 0.5: return {"action": "UPDATE_SL", "new_sl": entry}

    if position is not None: return None

    price = last_15m["close"]
    trend_up_15m = last_15m["ema9"] > last_15m["ema21"]
    trend_up_1h = last_1h["close"] > last_1h["ema21"] and last_1h["ema9"] > last_1h["ema21"]
    trend_up_4h = last_4h["close"] > last_4h["ema21"]
    
    trend_down_15m = last_15m["ema9"] < last_15m["ema21"]
    trend_down_1h = last_1h["close"] < last_1h["ema21"] and last_1h["ema9"] < last_1h["ema21"]
    trend_down_4h = last_4h["close"] < last_4h["ema21"]

    adx_strong = last_15m["adx"] > 35
    vol_ok = last_15m["volume"] > 1.5 * last_15m["vol_ma"]

    sl_pct = 0.02
    tp_pct = 0.04
    
    support, resistance = last_15m["low20"], last_15m["high20"]

    if trend_up_15m and trend_up_1h and trend_up_4h and adx_strong and vol_ok and 45 < last_15m["rsi"] < 65:
        return {"action": "ENTRY", "side": "LONG", "price": price,
                "sl": price * (1 - sl_pct), "tp": price * (1 + tp_pct),
                "rsi": last_15m["rsi"], "adx": last_15m["adx"], "type": f"Safe Trend {timeframe}",
                "support": support, "resistance": resistance}

    if trend_down_15m and trend_down_1h and trend_down_4h and adx_strong and vol_ok and 35 < last_15m["rsi"] < 55:
        return {"action": "ENTRY", "side": "SHORT", "price": price,
                "sl": price * (1 + sl_pct), "tp": price * (1 - tp_pct),
                "rsi": last_15m["rsi"], "adx": last_15m["adx"], "type": f"Safe Trend {timeframe}",
                "support": support, "resistance": resistance}

    return None

def fetch_data(ex, sym, tf, limit=100):
    for _ in range(3):
        try: return ex.fetch_ohlcv(sym, tf, limit=limit)
        except Exception: time.sleep(1.5)
    return []

def process_symbol(ex, sym):
    try:
        with state_lock:
            if state.get((sym, "15m"), {}).get("position") is None:
                if any(v.get("position") is not None for k, v in state.items() if k[0] == sym): return

        df_1h_data = fetch_data(ex, sym, "1h")
        df_4h_data = fetch_data(ex, sym, "4h")
        df_15m_data = fetch_data(ex, sym, "15m")
        if len(df_1h_data) < 50 or len(df_4h_data) < 50 or len(df_15m_data) < 50: return

        df_15m = add_indicators(pd.DataFrame(df_15m_data, columns=["timestamp", "open", "high", "low", "close", "volume"]))
        df_1h = add_indicators(pd.DataFrame(df_1h_data, columns=["timestamp", "open", "high", "low", "close", "volume"]))
        df_4h = add_indicators(pd.DataFrame(df_4h_data, columns=["timestamp", "open", "high", "low", "close", "volume"]))

        state_key = (sym, "15m")
        with state_lock:
            if state_key not in state:
                state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None, "last_exit_ts": 0, "breakeven_hit": False}
            pos_data = state[state_key].copy()

        if time.time() - pos_data.get("last_exit_ts", 0) < REENTRY_COOLDOWN_SEC: return

        signal = check_signal(df_15m, df_1h, df_4h, "15m", pos_data["position"], pos_data["entry"], pos_data["sl"], pos_data["tp"], pos_data["breakeven_hit"])
        if not signal: return

        if signal["action"] == "ENTRY":
            with state_lock:
                open_count = sum(1 for v in state.values() if v.get("position") is not None)
            if open_count >= MAX_OPEN_POSITIONS: return

            actual_price = get_current_price(ex, sym) or signal["price"]
            success, info = open_position(ex, sym, signal["side"], actual_price, signal["sl"], signal["tp"])

            with state_lock:
                if success:
                    state[state_key].update({"position": signal["side"], "entry": actual_price, "sl": signal["sl"], "tp": signal["tp"], "breakeven_hit": False})
                else:
                    state[state_key]["last_exit_ts"] = time.time()
            save_state()

            chart = generate_chart(sym, df_15m, "15m", signal["side"], signal["resistance"], signal["support"], actual_price, signal["sl"], signal["tp"])
            caption = msg_entry(sym, signal["side"], actual_price, signal["sl"], signal["tp"], signal["rsi"], signal["adx"], success)
            if chart: send_photo(chart, caption)
            else: send(caption)

        elif signal["action"] == "EXIT":
            if not close_position(ex, sym): return
            with state_lock:
                state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None, "last_exit_ts": time.time()})
            save_state()
            send(msg_exit(sym, pos_data["position"], "15m", signal["price"], pos_data["entry"], signal["reason"]))

    except Exception as e:
        print(f"[ERROR] {sym}: {e}")

# ==========================================
# التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting V27 Bot (5 Max Positions)...", flush=True)
    if not OKX_API_KEY: return send("❌ مفاتيح API غير موجودة")

    ex = create_exchange()
    try:
        ex.load_markets()
        load_state()
        print(f"[OKX] Connected. Balance: {ex.fetch_balance().get('USDT', {}).get('free', 0)}")
    except Exception as e:
        return send(f"❌ فشل الاتصال: {e}")

    send("🛡️ <b>تم تشغيل البوت V27</b>\n"
         "✅ الحد الأقصى للصفقات: 5 صفقات.\n"
         "✅ إصلاح خطأ الأهداف والـ SL بنسبة ثابتة.\n"
         "✅ شروط دخول صارمة جداً (ADX > 35 + 3 فريمات متوافقة).")

    active_symbols = [s for s in FUTURES_SYMBOLS if s in ex.markets]
    
    while True:
        start = time.time()
        for sym in active_symbols:
            process_symbol(ex, sym)
            time.sleep(1)
        time.sleep(max(10, SCAN_INTERVAL - (time.time() - start)))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
