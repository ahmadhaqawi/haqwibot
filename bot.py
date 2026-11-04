import os
import threading
import time
import json
from datetime import datetime, timezone, timedelta
import io
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

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_v24_okx_demo"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# الإعدادات
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_SECRET = os.getenv("OKX_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", "")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT",
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT",
]

TIMEFRAMES = ["15m", "1h", "4h"]
SCAN_INTERVAL = 45
LEVERAGE = 5
POSITION_SIZE_USDT = 50
MAX_OPEN_POSITIONS = 5
MARGIN_MODE = "isolated"
STATE_FILE = "bot_state.json"

plt.style.use('dark_background')

state_lock = threading.Lock()
state = {}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}

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
                print(f"[STATE] Loaded {len(state)} positions.")
        except Exception as e:
            print(f"[STATE ERROR] {e}")

def save_state():
    try:
        with state_lock:
            data = {
                "state": {f"{k[0]}|{k[1]}": v for k, v in state.items()},
                "daily_pnl": daily_pnl
            }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE ERROR] {e}")

# ==========================================
# تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    for cid in [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
        except Exception as e:
            print(f"[TG ERROR] {e}")

def send_photo(photo_buf, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    for cid in [c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()]:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"photo": ("chart.png", photo_buf, "image/png")},
                timeout=15
            )
        except Exception as e:
            print(f"[TG PHOTO ERROR] {e}")

def fmt(p):
    if p is None or (isinstance(p, float) and np.isnan(p)):
        return "0.00"
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
            height = abs(row['close'] - row['open'])
            bottom = min(row['close'], row['open'])
            ax.add_patch(mpatches.Rectangle((i - 0.3, bottom), 0.6, height, color=color))

        if resistance and not np.isnan(resistance):
            ax.axhline(resistance, color='#32CD32', linestyle='--', linewidth=1.5)
        if support and not np.isnan(support):
            ax.axhline(support, color='#FF1493', linestyle='--', linewidth=1.5)

        ax.axhline(entry, color='#00BFFF', linestyle='-', linewidth=2)
        ax.axhline(tp, color='#00FF00', linestyle=':', linewidth=2)
        ax.axhline(sl, color='#FF0000', linestyle=':', linewidth=2)

        ax.set_title(f"{symbol.split(':')[0]} - {timeframe} | {side}", color='white', fontsize=14)
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=110)
        buf.seek(0)
        plt.close(fig)
        return buf
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        plt.close('all')
        return None

# ==========================================
# الرسائل (شراء + بيع + المكسب)
# ==========================================
def msg_entry(sym, side, price, sl, tp, rsi_val, adx_val, signal_type, support, resistance, real_order=False):
    title = f"🟢 <b>توصية شراء (LONG)</b>" if side == "LONG" else f"🔴 <b>توصية بيع (SHORT)</b>"
    status = "✅ تم فتح الصفقة فعلياً على OKX Demo" if real_order else "إشارة فقط"
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")

    return (f"{title}\n"
            f"📌 {status}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"⚙️ الرافعة: <b>{LEVERAGE}x</b> | الهامش: <b>{POSITION_SIZE_USDT} USDT</b>\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"⏰ الوقت: <code>{now_str}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
            f"🎯 هدف الربح: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
            f"📊 RSI: {rsi_val:.1f} | ADX: {adx_val:.1f}\n"
            f"🛡️ Breakeven مفعل تلقائياً")

def msg_exit(sym, side, timeframe, exit_price, entry_price, reason):
    raw = ((exit_price - entry_price) / entry_price * 100) if side == "LONG" else ((entry_price - exit_price) / entry_price * 100)
    lev = raw * LEVERAGE
    icon = "🟢" if lev > 0 else "🔴" if lev < 0 else "⚪"
    reasons = {
        "Take Profit": "تحقق هدف الربح 🎯",
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Breakeven Hit": "خروج عند نقطة الدخول 🛡️"
    }
    return (f"{icon} <b>إغلاق صفقة ({timeframe})</b>\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code> ({side})\n"
            f"📥 سعر الدخول: <code>{fmt(entry_price)}</code>\n"
            f"📤 سعر الخروج: <code>{fmt(exit_price)}</code>\n"
            f"📈 العائد الصافي: <b>{lev:+.2f}%</b> (مع الرافعة {LEVERAGE}x)\n"
            f"📝 السبب: {reasons.get(reason, reason)}")

# ==========================================
# المؤشرات
# ==========================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()
def rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g / (l + 1e-9)))
def adx(high, low, close, p=14):
    up, down = high.diff(), -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/p, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    return dx.ewm(alpha=1/p, adjust=False).mean()
def atr(df, p=14):
    tr = pd.concat([df.high-df.low, (df.high-df.close.shift()).abs(), (df.low-df.close.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    if len(df) < 30: return df
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df["high"], df["low"], df["close"])
    df["atr"] = atr(df)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max().shift(1)
    df["low20"] = df["low"].rolling(20).min().shift(1)
    return df

# ==========================================
# دوال التداول على OKX Demo
# ==========================================
def create_exchange():
    exchange = ccxt.okx({
        'apiKey': OKX_API_KEY,
        'secret': OKX_SECRET,
        'password': OKX_PASSPHRASE,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 20000
    })
    exchange.set_sandbox_mode(True)
    return exchange

def set_leverage_safe(exchange, symbol, leverage):
    try:
        exchange.set_leverage(leverage, symbol, params={'mgnMode': MARGIN_MODE})
        return True
    except Exception as e:
        print(f"[LEVERAGE ERROR] {symbol}: {e}")
        return False

def calculate_amount(exchange, symbol, usdt_margin, price, leverage):
    try:
        market = exchange.market(symbol)
        contract_size = float(market.get('contractSize', 1))
        amount = (usdt_margin * leverage) / (price * contract_size)
        amount = float(exchange.amount_to_precision(symbol, amount))
        return max(amount, float(market.get('limits', {}).get('amount', {}).get('min', 0.01)))
    except Exception as e:
        print(f"[AMOUNT CALC ERROR] {e}")
        return 0.01

def open_real_position(exchange, symbol, side, entry_price, sl, tp):
    try:
        set_leverage_safe(exchange, symbol, LEVERAGE)
        amount = calculate_amount(exchange, symbol, POSITION_SIZE_USDT, entry_price, LEVERAGE)
        if amount <= 0:
            return False, "كمية غير صالحة"

        order_side = 'buy' if side == "LONG" else 'sell'
        params = {'tdMode': MARGIN_MODE, 'reduceOnly': False}

        order = exchange.create_order(symbol, 'market', order_side, amount, None, params)
        print(f"[ORDER OPENED] {symbol} {side} amount={amount}")

        try:
            close_side = 'sell' if side == "LONG" else 'buy'
            exchange.create_order(symbol, 'market', close_side, amount, None, {
                'tdMode': MARGIN_MODE, 'reduceOnly': True,
                'stopLoss': {'triggerPrice': sl, 'type': 'market'}
            })
            exchange.create_order(symbol, 'market', close_side, amount, None, {
                'tdMode': MARGIN_MODE, 'reduceOnly': True,
                'takeProfit': {'triggerPrice': tp, 'type': 'market'}
            })
        except Exception as e:
            print(f"[TP/SL WARNING] {e}")

        return True, order.get('id')
    except Exception as e:
        print(f"[OPEN ERROR] {symbol}: {e}")
        return False, str(e)

def close_real_position(exchange, symbol, side):
    try:
        positions = exchange.fetch_positions([symbol])
        for pos in positions:
            if float(pos.get('contracts', 0)) > 0:
                close_side = 'sell' if pos['side'] == 'long' else 'buy'
                amount = abs(float(pos['contracts']))
                exchange.create_order(symbol, 'market', close_side, amount, None, {
                    'tdMode': MARGIN_MODE, 'reduceOnly': True
                })
                print(f"[POSITION CLOSED] {symbol}")
                return True
        return False
    except Exception as e:
        print(f"[CLOSE ERROR] {e}")
        return False

# ==========================================
# منطق الإشارة
# ==========================================
def check_signal(df_main, df_1h, timeframe, position, entry_price=None, sl=None, tp=None, breakeven_hit=False):
    if len(df_main) < 50 or len(df_1h) < 50:
        return None

    last = df_main.iloc[-2]
    prev = df_main.iloc[-3]
    current = df_main.iloc[-1]
    last_1h = df_1h.iloc[-2]

    if position == "LONG":
        if current["high"] >= tp:
            return {"action": "EXIT", "side": "LONG", "price": tp, "reason": "Take Profit"}
        if current["low"] <= sl:
            reason = "Breakeven Hit" if breakeven_hit else "Stop Loss"
            return {"action": "EXIT", "side": "LONG", "price": sl, "reason": reason}
        be_trigger = entry_price + (tp - entry_price) * 0.5
        if not breakeven_hit and current["high"] >= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price}

    elif position == "SHORT":
        if current["low"] <= tp:
            return {"action": "EXIT", "side": "SHORT", "price": tp, "reason": "Take Profit"}
        if current["high"] >= sl:
            reason = "Breakeven Hit" if breakeven_hit else "Stop Loss"
            return {"action": "EXIT", "side": "SHORT", "price": sl, "reason": reason}
        be_trigger = entry_price - (entry_price - tp) * 0.5
        if not breakeven_hit and current["low"] <= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price}

    if position is not None:
        return None

    price = last["close"]
    trend_up = last_1h["close"] > last_1h["ema21"] and last_1h["close"] > last_1h["ema9"]
    trend_down = last_1h["close"] < last_1h["ema21"] and last_1h["close"] < last_1h["ema9"]
    vol_ok = last["volume"] > 1.3 * last["vol_ma"] if not pd.isna(last["vol_ma"]) else False
    adx_ok = last["adx"] > (25 if timeframe in ["1h", "4h"] else 28)
    atr_mean = df_main["atr"].iloc[-20:].mean()
    vola_ok = (0.6 * atr_mean) < last["atr"] < (2.3 * atr_mean) if not pd.isna(atr_mean) else False

    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
    spread_ok = abs(last["ema9"] - last["ema21"]) / price * 100 >= 0.02

    atr_mult = 2.2 if timeframe == "15m" else 2.5 if timeframe == "1h" else 2.8
    support, resistance = last["low20"], last["high20"]

    if cross_up and trend_up and adx_ok and vol_ok and spread_ok and vola_ok and 45 < last["rsi"] < 65:
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": price - atr_mult * last["atr"],
            "tp": price + atr_mult * 2.2 * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند قوي {timeframe}",
            "support": support, "resistance": resistance
        }

    if cross_down and trend_down and adx_ok and vol_ok and spread_ok and vola_ok and 35 < last["rsi"] < 55:
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": price + atr_mult * last["atr"],
            "tp": price - atr_mult * 2.2 * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند قوي {timeframe}",
            "support": support, "resistance": resistance
        }
    return None

def fetch_data_safe(exchange, symbol, timeframe, limit=100):
    for _ in range(3):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception:
            time.sleep(1.2)
    return []

def scan_symbol_timeframe(exchange, symbol, timeframe, df_1h):
    try:
        state_key = (symbol, timeframe)
        with state_lock:
            if state_key not in state:
                state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None,
                                    "last_candle_ts": None, "breakeven_hit": False, "order_id": None}
            pos_data = state[state_key].copy()

        if timeframe == "1h":
            df = df_1h.copy()
        else:
            ohlcv = fetch_data_safe(exchange, symbol, timeframe)
            if len(ohlcv) < 50: return
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = add_indicators(df)

        if len(df) < 50: return

        pos = pos_data["position"]
        current_ts = int(df.iloc[-2]["timestamp"])
        if pos is None and pos_data["last_candle_ts"] == current_ts:
            return

        signal = check_signal(df, df_1h, timeframe, pos, pos_data["entry"], pos_data["sl"], pos_data["tp"], pos_data["breakeven_hit"])
        if not signal: return

        if signal["action"] == "ENTRY":
            with state_lock:
                open_count = sum(1 for v in state.values() if v.get("position") is not None)
            if open_count >= MAX_OPEN_POSITIONS:
                print(f"[SKIP] Max open positions ({MAX_OPEN_POSITIONS}) reached")
                return

            success, info = open_real_position(exchange, symbol, signal["side"], signal["price"], signal["sl"], signal["tp"])
            
            with state_lock:
                state[state_key].update({
                    "position": signal["side"], "entry": signal["price"],
                    "sl": signal["sl"], "tp": signal["tp"],
                    "breakeven_hit": False, "last_candle_ts": current_ts,
                    "order_id": info if success else None
                })
            save_state()

            chart = generate_chart(symbol, df, timeframe, signal["side"], signal["resistance"],
                                   signal["support"], signal["price"], signal["sl"], signal["tp"])
            caption = msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"],
                                signal["rsi"], signal["adx"], signal["type"],
                                signal["support"], signal["resistance"], real_order=success)
            if chart:
                send_photo(chart, caption)
            else:
                send(caption)

            if not success:
                send(f"⚠️ فشل فتح الصفقة على {symbol.split(':')[0]}: {info}")

        elif signal["action"] == "UPDATE_SL":
            with state_lock:
                state[state_key]["sl"] = signal["new_sl"]
                state[state_key]["breakeven_hit"] = True
            save_state()
            send(f"🛡️ <b>Breakeven</b>\n🪙 {symbol.split(':')[0]} | تم نقل الوقف إلى سعر الدخول")

        elif signal["action"] == "EXIT":
            close_real_position(exchange, symbol, pos)
            raw = ((signal["price"] - pos_data["entry"]) / pos_data["entry"] * 100) if pos == "LONG" else \
                  ((pos_data["entry"] - signal["price"]) / pos_data["entry"] * 100)
            lev_pnl = raw * LEVERAGE
            with state_lock:
                daily_pnl["trades"] += 1
                daily_pnl["value"] += lev_pnl
                if lev_pnl > 0: daily_pnl["wins"] += 1
                state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None, "breakeven_hit": False})
            save_state()
            send(msg_exit(symbol, pos, timeframe, signal["price"], pos_data["entry"], signal["reason"]))

    except Exception as e:
        print(f"[SCAN ERROR] {symbol} {timeframe}: {e}")

# ==========================================
# التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting V24 OKX Demo Bot...", flush=True)
    load_state()

    if not OKX_API_KEY or not OKX_SECRET or not OKX_PASSPHRASE:
        print("[ERROR] OKX API credentials missing in Environment Variables")
        send("❌ مفاتيح OKX غير موجودة في Environment Variables")
        return

    exchange = create_exchange()
    try:
        exchange.load_markets()
        balance = exchange.fetch_balance()
        usdt_balance = balance.get('USDT', {}).get('free', 0)
        print(f"[OKX DEMO] Connected. USDT balance: {usdt_balance}")
        active_symbols = [s for s in FUTURES_SYMBOLS if s in exchange.markets]
        print(f"[OKX] {len(active_symbols)} symbols ready")
    except Exception as e:
        print(f"[CONNECTION ERROR] {e}")
        send(f"❌ فشل الاتصال بـ OKX Demo:\n<code>{e}</code>")
        return

    send("🛡️ <b>تم تشغيل البوت V24 على OKX Demo</b>\n"
         f"• الرافعة: {LEVERAGE}x\n"
         f"• حجم الصفقة: {POSITION_SIZE_USDT} USDT\n"
         f"• أقصى صفقات مفتوحة: {MAX_OPEN_POSITIONS}\n"
         f"• الوضع: {MARGIN_MODE}\n"
         "• مراقبة الحيتان: <b>موقوفة</b>")

    while True:
        cycle_start = time.time()
        try:
            today = str(datetime.now(KSA_TZ).date())
            with state_lock:
                if daily_pnl["date"] != today:
                    daily_pnl.update({"date": today, "trades": 0, "wins": 0, "value": 0.0})
                    save_state()

            for sym in active_symbols:
                ohlcv_1h = fetch_data_safe(exchange, sym, "1h", 60)
                if len(ohlcv_1h) < 50: continue
                df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df_1h['datetime'] = pd.to_datetime(df_1h['timestamp'], unit='ms')
                df_1h = add_indicators(df_1h)

                for tf in TIMEFRAMES:
                    scan_symbol_timeframe(exchange, sym, tf, df_1h)

        except Exception as e:
            print(f"[LOOP ERROR] {e}")

        time.sleep(max(8, SCAN_INTERVAL - (time.time() - cycle_start)))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
