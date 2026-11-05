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
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", os.getenv("OKX_PASSWORD", "")) # دعم لكلا الاسمين

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "XRP/USDT:USDT",
    "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT",
    "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT", "SUI/USDT:USDT",
    "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT", "ATOM/USDT:USDT",
    "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT", "SEI/USDT:USDT",
]

FALLBACK_SYMBOLS = [
    "TRX/USDT:USDT", "WLD/USDT:USDT", "TIA/USDT:USDT",
    "JUP/USDT:USDT", "PEPE/USDT:USDT", "ORDI/USDT:USDT",
]

TIMEFRAMES = ["15m", "1h", "4h"]
SCAN_INTERVAL = 45
LEVERAGE = 5
POSITION_SIZE_USDT = 50
MAX_OPEN_POSITIONS = 5
MARGIN_MODE = "isolated"
REENTRY_COOLDOWN_SEC = 1200  # 20 دقيقة بعد الإغلاق

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")

plt.style.use('dark_background')

state_lock = threading.Lock()
state = {}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}
active_symbols_cache = []
_signal_log_throttle = {}

# ==========================================
# Flask
# ==========================================
@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_v25_okx_demo"}, 200

@app.route("/debug")
def debug_info():
    with state_lock:
        active = {f"{k[0]}|{k[1]}": v for k, v in state.items() if v.get("position")}
    return {
        "open_positions": active,
        "open_count": len(active),
        "daily_pnl": daily_pnl,
        "symbols_count": len(active_symbols_cache),
        "time_ksa": datetime.now(KSA_TZ).isoformat(),
    }, 200

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
                print(f"[STATE] Loaded {len(state)} positions from {STATE_FILE}")
        except Exception as e:
            print(f"[STATE ERROR] {e}")
    else:
        print(f"[STATE] No state file at {STATE_FILE}, starting fresh")

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
            photo_buf.seek(0)
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
        plt.clf()
        plt.close('all')
        gc.collect()
        return buf
    except Exception as e:
        print(f"[CHART ERROR] {e}")
        plt.clf()
        plt.close('all')
        gc.collect()
        return None

# ==========================================
# الرسائل
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
            f"🛡️ Breakeven عند 50% من المسافة للهدف")

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
# OKX Trading (مع تعديل posSide)
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

def get_current_price(exchange, symbol):
    try:
        ticker = exchange.fetch_ticker(symbol)
        price = ticker.get('last') or ticker.get('close')
        return float(price) if price else None
    except Exception as e:
        print(f"[TICKER ERROR] {symbol}: {e}")
        return None

def calculate_amount(exchange, symbol, usdt_margin, price, leverage):
    try:
        market = exchange.market(symbol)
        contract_size = float(market.get('contractSize', 1))
        raw_contracts = (usdt_margin * leverage) / (price * contract_size)
        if raw_contracts <= 0: return 0.0

        amount = float(exchange.amount_to_precision(symbol, raw_contracts))
        min_amt = float(market.get('limits', {}).get('amount', {}).get('min', 1.0))

        if amount < min_amt:
            required_margin = (min_amt * contract_size * price) / leverage
            try:
                free_usdt = exchange.fetch_balance().get('USDT', {}).get('free', 0)
            except Exception:
                free_usdt = 0

            if required_margin <= free_usdt * 0.9:
                return min_amt
            return 0.0
        return amount
    except Exception as e:
        print(f"[AMOUNT CALC ERROR] {symbol}: {e}")
        return 0.0

def _cancel_pending_algo_orders(exchange, symbol):
    try:
        market = exchange.market(symbol)
        inst_id = market['id']
        n = 0
        try:
            algo_resp = exchange.privateGetTradeOrdersAlgoPending({
                'instId': inst_id,
                'ordType': 'conditional',
            })
            for o in (algo_resp.get('data') or []):
                try:
                    exchange.privatePostTradeCancelAlgos([{'instId': inst_id, 'algoId': o['algoId']}])
                    n += 1
                except Exception: pass
        except Exception: pass

        try:
            orders = exchange.fetch_open_orders(symbol)
            for o in orders:
                try:
                    exchange.cancel_order(o['id'], symbol)
                    n += 1
                except Exception: pass
        except Exception: pass
        return n
    except Exception:
        return 0

def _place_sl(exchange, symbol, close_side, amount, trigger):
    trigger_str = str(exchange.price_to_precision(symbol, trigger))
    amount_str = str(exchange.amount_to_precision(symbol, amount))
    market = exchange.market(symbol)

    try:
        return exchange.create_order(symbol, 'market', close_side, amount, None, {
            'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True,
            'triggerPrice': float(trigger_str), 'orderPrice': -1, 'stop': True
        })
    except Exception as e1:
        print(f"[SL UNIFIED FAILED] {symbol}: {e1} → raw")
    
    req = {
        'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': close_side,
        'posSide': 'net', 'ordType': 'conditional', 'sz': amount_str,
        'reduceOnly': 'true', 'slTriggerPx': trigger_str, 'slOrdPx': '-1',
    }
    return exchange.privatePostTradeOrderAlgo(req)

def _place_tp(exchange, symbol, close_side, amount, trigger):
    trigger_str = str(exchange.price_to_precision(symbol, trigger))
    amount_str = str(exchange.amount_to_precision(symbol, amount))
    market = exchange.market(symbol)

    try:
        return exchange.create_order(symbol, 'market', close_side, amount, None, {
            'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True,
            'triggerPrice': float(trigger_str), 'orderPrice': -1, 'stop': True
        })
    except Exception as e1:
        print(f"[TP UNIFIED FAILED] {symbol}: {e1} → raw")

    req = {
        'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': close_side,
        'posSide': 'net', 'ordType': 'conditional', 'sz': amount_str,
        'reduceOnly': 'true', 'tpTriggerPx': trigger_str, 'tpOrdPx': '-1',
    }
    return exchange.privatePostTradeOrderAlgo(req)

def _verify_sl_tp_exists(exchange, symbol):
    try:
        market = exchange.market(symbol)
        for attempt in range(3):
            time.sleep(0.5)
            try:
                algo = exchange.privateGetTradeOrdersAlgoPending({
                    'instId': market['id'], 'ordType': 'conditional',
                })
                pending = algo.get('data') or []
                has_sl = any(o.get('slTriggerPx') for o in pending)
                has_tp = any(o.get('tpTriggerPx') for o in pending)
                if has_sl:
                    return has_sl, has_tp
            except Exception: pass
        return False, False
    except Exception:
        return False, False

def open_real_position(exchange, symbol, side, entry_price, sl, tp):
    try:
        set_leverage_safe(exchange, symbol, LEVERAGE)
        amount = calculate_amount(exchange, symbol, POSITION_SIZE_USDT, entry_price, LEVERAGE)
        if amount <= 0: return False, "كمية غير صالحة"

        _cancel_pending_algo_orders(exchange, symbol)
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'

        sl_formatted = float(exchange.price_to_precision(symbol, sl))
        tp_formatted = float(exchange.price_to_precision(symbol, tp))

        order = None
        attached_ok = False
        try:
            params = {
                'tdMode': MARGIN_MODE, 'posSide': 'net', # ✅ إضافة posSide
                'slTriggerPx': str(sl_formatted), 'slOrdPx': '-1',
                'tpTriggerPx': str(tp_formatted), 'tpOrdPx': '-1',
            }
            order = exchange.create_order(symbol, 'market', order_side, amount, None, params)
            attached_ok = True
        except Exception as e:
            print(f"[ATTACHED FAILED] {symbol}: {e} → fallback")

        if not attached_ok:
            # ✅ إضافة posSide في الأمر الأساسي
            order = exchange.create_order(symbol, 'market', order_side, amount, None, {
                'tdMode': MARGIN_MODE, 'posSide': 'net'
            })
            sl_ok = False
            try:
                _place_sl(exchange, symbol, close_side, amount, sl_formatted)
                sl_ok = True
            except Exception: pass

            try:
                _place_tp(exchange, symbol, close_side, amount, tp_formatted)
            except Exception: pass

            if not sl_ok:
                try:
                    exchange.create_order(symbol, 'market', close_side, amount, None, {
                        'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True
                    })
                except Exception: pass
                return False, f"فشل SL → إغلاق طارئ"

        pos_verified = False
        for _ in range(3):
            time.sleep(0.4)
            try:
                positions = exchange.fetch_positions([symbol])
                if any(float(p.get('contracts', 0)) > 0 for p in positions):
                    pos_verified = True
                    break
            except Exception: pass

        if not pos_verified:
            try:
                exchange.create_order(symbol, 'market', close_side, amount, None, {
                    'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True
                })
            except Exception: pass
            return False, "لم يتم التحقق من المركز"

        has_sl, has_tp = _verify_sl_tp_exists(exchange, symbol)
        if not has_sl:
            try:
                exchange.create_order(symbol, 'market', close_side, amount, None, {
                    'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True
                })
            except Exception: pass
            return False, "الصفقة بلا حماية → إغلاق طارئ"

        return True, order.get('id')

    except Exception as e:
        print(f"[OPEN ERROR] {symbol}: {e}")
        return False, str(e)

def close_real_position(exchange, symbol, side):
    try:
        _cancel_pending_algo_orders(exchange, symbol)
        positions = exchange.fetch_positions([symbol])
        for pos in positions:
            if float(pos.get('contracts', 0)) > 0:
                close_side = 'sell' if pos['side'] == 'long' else 'buy'
                amount = abs(float(pos['contracts']))
                exchange.create_order(symbol, 'market', close_side, amount, None, {
                    'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True
                })

        time.sleep(0.6)
        positions_after = exchange.fetch_positions([symbol])
        if any(float(p.get('contracts', 0)) > 0 for p in positions_after):
            return False
        return True
    except Exception as e:
        print(f"[CLOSE ERROR] {e}")
        return False

def update_breakeven(exchange, symbol, side, amount, entry_price, tp, fallback_sl=None):
    try:
        _cancel_pending_algo_orders(exchange, symbol)
        close_side = 'sell' if side == "LONG" else 'buy'
        entry_formatted = float(exchange.price_to_precision(symbol, entry_price))
        tp_formatted = float(exchange.price_to_precision(symbol, tp))

        sl_ok = False
        try:
            _place_sl(exchange, symbol, close_side, amount, entry_formatted)
            sl_ok = True
        except Exception: pass

        if sl_ok:
            try: _place_tp(exchange, symbol, close_side, amount, tp_formatted)
            except Exception: pass
            return True

        if fallback_sl is not None:
            try:
                fb = float(exchange.price_to_precision(symbol, fallback_sl))
                _place_sl(exchange, symbol, close_side, amount, fb)
                try: _place_tp(exchange, symbol, close_side, amount, tp_formatted)
                except Exception: pass
                return False
            except Exception: pass

        try:
            exchange.create_order(symbol, 'market', close_side, amount, None, {
                'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True
            })
        except Exception: pass
        send(f"🚨 <b>إغلاق طارئ</b>\n🪙 {symbol.split(':')[0]}\nفشل Breakeven → أغلقنا الصفقة لحماية الرصيد")
        return False
    except Exception as e:
        print(f"[BREAKEVEN ERROR] {e}")
        return False

def reconcile_state_with_exchange(exchange):
    try:
        all_positions = exchange.fetch_positions()
        live = {}
        for p in all_positions:
            if float(p.get('contracts', 0)) > 0:
                live[p['symbol']] = p['side']

        cleaned = 0
        orphans = []
        with state_lock:
            for key, data in state.items():
                if data.get('position') is None: continue
                sym = key[0]
                expected = 'long' if data['position'] == 'LONG' else 'short'
                if sym not in live or live[sym] != expected:
                    data.update({'position': None, 'entry': None, 'sl': None, 'tp': None, 'breakeven_hit': False})
                    cleaned += 1

            known = {k[0] for k, v in state.items() if v.get('position')}
            for sym, s in live.items():
                if sym not in known:
                    orphans.append((sym, s))
        save_state()

        if orphans:
            lines = [f"⚠️ <b>صفقات يتيمة على المنصة</b>\n"]
            for sym, s in orphans:
                lines.append(f"🪙 {sym.split(':')[0]} ({s.upper()})")
            lines.append("\n📌 موجودة على OKX لكن مو مسجلة عند البوت")
            send("\n".join(lines))
    except Exception as e:
        print(f"[RECONCILE ERROR] {e}")

# ==========================================
# منطق الإشارة
# ==========================================
def check_signal(df_main, df_trend, timeframe, position, entry_price=None, sl=None, tp=None, breakeven_hit=False):
    if len(df_main) < 50 or len(df_trend) < 50: return None

    last = df_main.iloc[-2]
    current = df_main.iloc[-1]
    trend_last = df_trend.iloc[-2]

    if position == "LONG":
        if current["high"] >= tp: return {"action": "EXIT", "side": "LONG", "price": tp, "reason": "Take Profit"}
        if current["low"] <= sl:
            reason = "Breakeven Hit" if breakeven_hit else "Stop Loss"
            return {"action": "EXIT", "side": "LONG", "price": sl, "reason": reason}
        be_trigger = entry_price + (tp - entry_price) * 0.5
        if not breakeven_hit and current["high"] >= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price}

    elif position == "SHORT":
        if current["low"] <= tp: return {"action": "EXIT", "side": "SHORT", "price": tp, "reason": "Take Profit"}
        if current["high"] >= sl:
            reason = "Breakeven Hit" if breakeven_hit else "Stop Loss"
            return {"action": "EXIT", "side": "SHORT", "price": sl, "reason": reason}
        be_trigger = entry_price - (entry_price - tp) * 0.5
        if not breakeven_hit and current["low"] <= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price}

    if position is not None: return None

    price = last["close"]
    trend_up = trend_last["close"] > trend_last["ema21"] and trend_last["close"] > trend_last["ema9"]
    trend_down = trend_last["close"] < trend_last["ema21"] and trend_last["close"] < trend_last["ema9"]
    vol_ok = last["volume"] > 1.3 * last["vol_ma"] if not pd.isna(last["vol_ma"]) else False
    adx_ok = last["adx"] > (25 if timeframe in ["1h", "4h"] else 28)
    atr_mean = df_main["atr"].iloc[-20:].mean()
    vola_ok = (0.6 * atr_mean) < last["atr"] < (2.3 * atr_mean) if not pd.isna(atr_mean) else False

    spread_ok = abs(last["ema9"] - last["ema21"]) / price * 100 >= 0.02
    trend_align_up = spread_ok and last["ema9"] > last["ema21"]
    trend_align_down = spread_ok and last["ema9"] < last["ema21"]

    cross_up_recent = any(
        df_main.iloc[i-1]["ema9"] <= df_main.iloc[i-1]["ema21"] and
        df_main.iloc[i]["ema9"] > df_main.iloc[i]["ema21"]
        for i in range(-4, -1)
    )
    cross_down_recent = any(
        df_main.iloc[i-1]["ema9"] >= df_main.iloc[i-1]["ema21"] and
        df_main.iloc[i]["ema9"] < df_main.iloc[i]["ema21"]
        for i in range(-4, -1)
    )

    atr_mult = 2.2 if timeframe == "15m" else 2.5 if timeframe == "1h" else 2.8
    support, resistance = last["low20"], last["high20"]

    if (cross_up_recent or trend_align_up) and trend_up and adx_ok and vol_ok and vola_ok and 45 < last["rsi"] < 65:
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": price - atr_mult * last["atr"], "tp": price + atr_mult * 2.2 * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند قوي {timeframe}",
            "support": support, "resistance": resistance
        }

    if (cross_down_recent or trend_align_down) and trend_down and adx_ok and vol_ok and vola_ok and 35 < last["rsi"] < 55:
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": price + atr_mult * last["atr"], "tp": price - atr_mult * 2.2 * last["atr"],
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

def scan_symbol_timeframe(exchange, symbol, timeframe, df_trend, df_main=None):
    try:
        state_key = (symbol, timeframe)
        with state_lock:
            if state_key not in state:
                state[state_key] = {"position": None, "entry": None, "sl": None, "tp": None,
                                    "last_candle_ts": None, "breakeven_hit": False,
                                    "order_id": None, "last_exit_ts": 0}
            pos_data = state[state_key].copy()

            if pos_data["position"] is None:
                has_active = any(v.get("position") is not None for k, v in state.items() if k[0] == symbol)
                if has_active: return

        if df_main is not None:
            df = df_main.copy()
        else:
            ohlcv = fetch_data_safe(exchange, symbol, timeframe)
            if len(ohlcv) < 50: return
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = add_indicators(df)

        if len(df) < 50: return

        pos = pos_data["position"]
        current_ts = int(df.iloc[-2]["timestamp"])
        signal = check_signal(df, df_trend, timeframe, pos, pos_data["entry"], pos_data["sl"], pos_data["tp"], pos_data["breakeven_hit"])

        if not signal and pos is None:
            last = df.iloc[-2]; prev = df.iloc[-3]
            c_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
            c_dn = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]
            if c_up or c_dn:
                log_key = (symbol, timeframe)
                last_log = _signal_log_throttle.get(log_key, 0)
                if time.time() - last_log > 300:
                    vol_ratio = last["volume"] / max(last["vol_ma"], 1e-9) if not pd.isna(last["vol_ma"]) else 0
                    spread = abs(last["ema9"] - last["ema21"]) / last["close"] * 100
                    print(f"[SIGNAL FILTERED] {symbol} {timeframe} | cross={'UP' if c_up else 'DN'} "
                          f"adx={last['adx']:.1f} rsi={last['rsi']:.1f} volx={vol_ratio:.2f} spread={spread:.3f}%")
                    _signal_log_throttle[log_key] = time.time()
            return

        if not signal: return

        if signal["action"] == "ENTRY":
            if pos_data.get("last_candle_ts") == current_ts: return
            if time.time() - pos_data.get("last_exit_ts", 0) < REENTRY_COOLDOWN_SEC: return

            with state_lock:
                open_count = sum(1 for v in state.values() if v.get("position") is not None)
            if open_count >= MAX_OPEN_POSITIONS: return

            actual_price = get_current_price(exchange, symbol) or signal["price"]
            sl_dist = abs(signal["price"] - signal["sl"])
            tp_dist = abs(signal["tp"] - signal["price"])
            if signal["side"] == "LONG":
                actual_sl = actual_price - sl_dist
                actual_tp = actual_price + tp_dist
            else:
                actual_sl = actual_price + sl_dist
                actual_tp = actual_price - tp_dist

            success, info = open_real_position(exchange, symbol, signal["side"], actual_price, actual_sl, actual_tp)

            with state_lock:
                if success:
                    state[state_key].update({
                        "position": signal["side"], "entry": actual_price,
                        "sl": actual_sl, "tp": actual_tp,
                        "breakeven_hit": False, "last_candle_ts": current_ts, "order_id": info
                    })
                else:
                    state[state_key]["last_candle_ts"] = current_ts
            save_state()

            chart = generate_chart(symbol, df, timeframe, signal["side"], signal["resistance"], signal["support"], actual_price, actual_sl, actual_tp)
            caption = msg_entry(symbol, signal["side"], actual_price, actual_sl, actual_tp, signal["rsi"], signal["adx"], signal["type"], signal["support"], signal["resistance"], real_order=success)
            
            if chart: send_photo(chart, caption)
            else: send(caption)

            if not success:
                send(f"⚠️ فشل فتح الصفقة على {symbol.split(':')[0]}: {info}")

        elif signal["action"] == "UPDATE_SL":
            real_amt = 0
            try:
                for p in exchange.fetch_positions([symbol]):
                    if float(p.get('contracts', 0)) > 0:
                        real_amt = abs(float(p['contracts']))
                        break
            except Exception: pass

            ok = False
            if real_amt > 0:
                ok = update_breakeven(exchange, symbol, pos, real_amt, signal["new_sl"], pos_data["tp"], pos_data["sl"])

            if ok:
                with state_lock:
                    state[state_key]["sl"] = signal["new_sl"]
                    state[state_key]["breakeven_hit"] = True
                save_state()
                send(f"🛡️ <b>Breakeven</b>\n🪙 {symbol.split(':')[0]} | تم نقل الوقف على المنصة إلى الدخول")
            else:
                send(f"⚠️ فشل نقل الوقف على {symbol.split(':')[0]} — راجع اللوجز")

        elif signal["action"] == "EXIT":
            closed_ok = close_real_position(exchange, symbol, pos)
            if not closed_ok: return

            raw = ((signal["price"] - pos_data["entry"]) / pos_data["entry"] * 100) if pos == "LONG" else ((pos_data["entry"] - signal["price"]) / pos_data["entry"] * 100)
            lev_pnl = raw * LEVERAGE
            with state_lock:
                daily_pnl["trades"] += 1
                daily_pnl["value"] += lev_pnl
                if lev_pnl > 0: daily_pnl["wins"] += 1
                state[state_key].update({
                    "position": None, "entry": None, "sl": None, "tp": None,
                    "breakeven_hit": False, "last_candle_ts": current_ts, "last_exit_ts": time.time(),
                })
            save_state()
            send(msg_exit(symbol, pos, timeframe, signal["price"], pos_data["entry"], signal["reason"]))

    except Exception as e:
        print(f"[SCAN ERROR] {symbol} {timeframe}: {e}")

# ==========================================
# التشغيل الرئيسي
# ==========================================
def trading_loop():
    global active_symbols_cache
    print("[INIT] Starting V25 OKX Demo Bot (posSide fixed)...", flush=True)

    if not OKX_API_KEY or not OKX_SECRET or not OKX_PASSPHRASE:
        send("❌ مفاتيح OKX غير موجودة في Environment Variables")
        return

    exchange = create_exchange()
    try:
        exchange.load_markets()
        balance = exchange.fetch_balance()
        usdt_balance = balance.get('USDT', {}).get('free', 0)
        print(f"[OKX DEMO] Connected. USDT balance: {usdt_balance}")
    except Exception as e:
        send(f"❌ فشل الاتصال بـ OKX Demo:\n<code>{e}</code>")
        return

    try:
        acct = exchange.privateGetAccountConfig()
        data = (acct.get('data') or [{}])[0]
        acct_lv = data.get('acctLv', '?')
        pos_mode = data.get('posMode', '?')
        print(f"[ACCOUNT] acctLv={acct_lv} | posMode={pos_mode}")
        if pos_mode == 'long_short_mode':
            send(f"⛔ <b>الحساب في long_short_mode</b>\nالبوت يحتاج <b>net_mode</b>.\nغيّره من: OKX → Account → Position Mode")
            return
    except Exception as e:
        print(f"[ACCT CONFIG WARN] {e} — سنكمل على افتراض net_mode")

    active_symbols = [s for s in FUTURES_SYMBOLS if s in exchange.markets]
    missing = [s for s in FUTURES_SYMBOLS if s not in exchange.markets]
    if missing:
        for cand in FALLBACK_SYMBOLS:
            if len(active_symbols) >= 20: break
            if cand in exchange.markets and cand not in active_symbols:
                active_symbols.append(cand)

    active_symbols_cache = active_symbols[:]
    print(f"[OKX] {len(active_symbols)} symbols ready")

    load_state()
    reconcile_state_with_exchange(exchange)

    send("🛡️ <b>تم تشغيل البوت V25 على OKX Demo</b>\n"
         f"• الرافعة: {LEVERAGE}x\n"
         f"• حجم الصفقة: {POSITION_SIZE_USDT} USDT\n"
         f"• وضع المركز: Net Mode ✅\n"
         f"• حماية SL/TP: مرفقة على OKX ✅")

    last_heartbeat = 0
    while True:
        cycle_start = time.time()
        try:
            today = str(datetime.now(KSA_TZ).date())
            with state_lock:
                if daily_pnl["date"] != today:
                    daily_pnl.update({"date": today, "trades": 0, "wins": 0, "value": 0.0})
                    save_state()

            for sym in active_symbols:
                ohlcv_4h = fetch_data_safe(exchange, sym, "4h", 60)
                df_4h = None
                if len(ohlcv_4h) >= 50:
                    df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    df_4h = add_indicators(df_4h)

                ohlcv_1h = fetch_data_safe(exchange, sym, "1h", 60)
                df_1h = None
                if len(ohlcv_1h) >= 50:
                    df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                    df_1h = add_indicators(df_1h)

                if df_1h is not None:
                    scan_symbol_timeframe(exchange, sym, "15m", df_1h)
                    time.sleep(0.04)

                if df_4h is not None and df_1h is not None:
                    scan_symbol_timeframe(exchange, sym, "1h", df_4h, df_main=df_1h)
                    time.sleep(0.04)

                if df_4h is not None:
                    scan_symbol_timeframe(exchange, sym, "4h", df_4h, df_main=df_4h)
                    time.sleep(0.04)

            if time.time() - last_heartbeat > 300:
                with state_lock:
                    open_ct = sum(1 for v in state.values() if v.get("position"))
                    pnl_v = daily_pnl["value"]
                    wins = daily_pnl["wins"]
                    trades = daily_pnl["trades"]
                print(f"[HEARTBEAT] {datetime.now(KSA_TZ).strftime('%H:%M:%S')} | open={open_ct} | trades={trades} | pnl={pnl_v:+.2f}%", flush=True)
                last_heartbeat = time.time()

        except Exception as e:
            print(f"[LOOP ERROR] {e}")

        time.sleep(max(8, SCAN_INTERVAL - (time.time() - cycle_start)))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()