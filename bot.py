import os
import threading
import time
import json
import logging
from datetime import datetime, timezone, timedelta
import numpy as np
import ccxt
import pandas as pd
import requests
from flask import Flask

app = Flask(__name__)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

# ==========================================
# الإعدادات - V39
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_SECRET = os.getenv("OKX_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", os.getenv("OKX_PASSWORD", ""))
USE_SANDBOX = os.getenv("USE_SANDBOX", "true").lower() in ("true", "1", "yes")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
AUTHORIZED_CHATS = {c.strip() for c in TELEGRAM_CHAT_ID.split(",") if c.strip()}

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT", "XRP/USDT:USDT",
    "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT", "LINK/USDT:USDT", "DOT/USDT:USDT",
    "NEAR/USDT:USDT", "APT/USDT:USDT", "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT",
    "INJ/USDT:USDT", "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT",
    "SEI/USDT:USDT", "TIA/USDT:USDT", "STX/USDT:USDT", "IMX/USDT:USDT", "RUNE/USDT:USDT",
    "FET/USDT:USDT", "WLD/USDT:USDT", "PEPE/USDT:USDT", "WIF/USDT:USDT", "ORDI/USDT:USDT"
]

SCAN_INTERVAL = 55
LEVERAGE = 5
MARGIN_MODE = "isolated"
MAX_OPEN_POSITIONS = 5
REENTRY_COOLDOWN_SEC = 1500

# --- حجم الصفقة ---
USE_RISK_SIZING = True
RISK_PCT_PER_TRADE = 0.75
POSITION_SIZE_USDT = 50
MIN_MARGIN_USDT = 12
MAX_MARGIN_USDT = 130

# --- SL/TP ---
ATR_SL_MULT = 1.55
RR_RATIO = 2.3
MIN_SL_PCT = 0.0065
MAX_SL_PCT = 0.032
TRAIL_ATR_MULT = 1.05
BE_FEE_BUFFER = 0.0008

# --- حماية المحفظة ---
DAILY_LOSS_LIMIT_PCT = 4.0
DAILY_PROFIT_TARGET_PCT = 6.0          
MAX_SAME_DIRECTION = 3

# --- فلاتر السوق ---
MAX_SPREAD_PCT = 0.0011
MAX_FUNDING_RATE = 0.00045

CACHE_TTL = {"15m": 0, "1h": 280, "4h": 850}

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")

state_lock = threading.Lock()
trade_lock = threading.RLock()
state = {}
meta = {"paused": False, "last_report": "", "loss_alert": "", "last_heartbeat": "", "profit_alert": ""}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}

_data_cache = {}
_equity_cache = {"ts": 0, "value": 0.0}

# ==========================================
# Flask
# ==========================================
@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_v39_floating_pnl"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# إدارة الحالة
# ==========================================
def new_sym_state():
    return {
        "position": None, "entry": None, "sl": None, "tp": None,
        "contracts": 0.0, "initial_contracts": 0.0, "margin": 0.0,
        "opened_ts": 0, "last_exit_ts": 0,
        "breakeven_hit": False,
        "partial_1_taken": False,
        "partial_2_taken": False,
        "regime": "unknown"
    }

def load_state():
    global state, daily_pnl, meta
    if not os.path.exists(STATE_FILE): return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        loaded = {}
        for k, v in data.get("state", {}).items():
            sym = k.split("|")[0]
            base = new_sym_state()
            base.update(v)
            loaded[sym] = base
        with state_lock:
            state = loaded
            daily_pnl = data.get("daily_pnl", daily_pnl)
            meta.update(data.get("meta", {}))
    except Exception as e:
        log.error(f"[STATE LOAD ERROR] {e}")

def save_state():
    try:
        with state_lock:
            data = {"state": dict(state), "daily_pnl": daily_pnl, "meta": meta}
            txt = json.dumps(data, ensure_ascii=False, indent=2)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(txt)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.error(f"[STATE SAVE ERROR] {e}")

def roll_daily():
    global daily_pnl
    today = str(datetime.now(KSA_TZ).date())
    with state_lock:
        if daily_pnl.get("date") != today:
            daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": today}

def update_daily_stats(pnl_usdt):
    roll_daily()
    with state_lock:
        daily_pnl["value"] += pnl_usdt
        daily_pnl["trades"] += 1
        if pnl_usdt > 0: daily_pnl["wins"] += 1
    save_state()

def reset_sym_state(sym, cooldown=True):
    with state_lock:
        s = new_sym_state()
        s["last_exit_ts"] = time.time() if cooldown else 0
        state[sym] = s
    save_state()

# ==========================================
# تيليجرام
# ==========================================
def send(msg, chat_id=None):
    if not TELEGRAM_BOT_TOKEN: return
    targets = [chat_id] if chat_id else list(AUTHORIZED_CHATS)
    for cid in targets:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            log.warning(f"[TG SEND] {e}")

def fmt(p):
    if p is None or (isinstance(p, float) and np.isnan(p)): return "0.00"
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

REASONS = {
    "Take Profit": "تحقق الهدف 🎯",
    "Stop Loss": "ضرب وقف الخسارة 🛑",
    "Breakeven Hit": "خروج آمن (Breakeven) 🛡️",
    "Trailing Stop": "خروج بوقف متحرك 📈",
    "Partial TP": "إغلاق جزئي 💰",
    "Manual": "إغلاق يدوي 🖐️",
    "Emergency": "إغلاق طارئ 🚨",
    "Manual/External": "إغلاق من المنصة ⚡",
}

def msg_entry(sym, side, price, sl, tp, margin, real_order, regime=""):
    title = "🟢 <b>توصية شراء (LONG)</b>" if side == "LONG" else "🔴 <b>توصية بيع (SHORT)</b>"
    status = "✅ <b>تم فتح الصفقة آلياً</b>" if real_order else "⚠️ <b>إشارة فقط</b>"
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * LEVERAGE
    reward_usd = margin * (tp_pct / 100) * LEVERAGE
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    regime_txt = f"\n📡 <b>نظام السوق:</b> {regime}" if regime else ""

    return (f"{title}\n{status}{regime_txt}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <b>الزوج:</b> <code>{sym.split(':')[0]}</code>\n"
            f"💵 <b>سعر الدخول:</b> <code>{fmt(price)}</code>\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x | <b>الهامش:</b> {margin:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 <b>وقف الخسارة:</b> <code>{fmt(sl)}</code> ({sl_pct:.2f}%)\n"
            f"📉 <i>المخاطرة: {risk_usd:.2f} USDT</i>\n"
            f"🎯 <b>الهدف:</b> <code>{fmt(tp)}</code> ({tp_pct:.2f}%)\n"
            f"📈 <i>العائد المتوقع: +{reward_usd:.2f} USDT</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ {now_str}")

def msg_exit(sym, side, exit_price, entry_price, pnl_usd, margin, reason):
    pct = (pnl_usd / margin * 100) if margin else 0.0
    if pnl_usd > 0: title = "🟢 <b>إغلاق بربح</b>"
    elif pnl_usd < 0: title = "🔴 <b>إغلاق بخسارة</b>"
    else: title = "⚪ <b>إغلاق متعادل</b>"

    return (f"{title}\n━━━━━━━━━━━━━━━\n"
            f"🪙 <code>{sym.split(':')[0]}</code> ({side})\n"
            f"📥 الدخول: <code>{fmt(entry_price)}</code>\n"
            f"📤 الخروج: <code>{fmt(exit_price)}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 على الهامش: {pct:+.2f}%\n"
            f"💰 النتيجة: {pnl_usd:+.2f} USDT\n"
            f"📝 السبب: {REASONS.get(reason, reason)}\n"
            f"━━━━━━━━━━━━━━━")

# ==========================================
# المؤشرات
# ==========================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g / (l + 1e-9)))

def true_range(high, low, close):
    return pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)

def atr(high, low, close, p=14):
    return true_range(high, low, close).ewm(alpha=1/p, adjust=False).mean()

def adx(high, low, close, p=14):
    up, down = high.diff(), -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr_val = atr(high, low, close, p)
    plus_di = 100 * (plus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    return (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9) * 100).ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    if len(df) < 60: return df
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200) if len(df) >= 200 else ema(df["close"], 50)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df["high"], df["low"], df["close"])
    df["atr"] = atr(df["high"], df["low"], df["close"])
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max().shift(1)
    df["low20"] = df["low"].rolling(20).min().shift(1)
    return df

# ==========================================
# المنصة (OKX)
# ==========================================
def create_exchange():
    ex = ccxt.okx({
        'apiKey': OKX_API_KEY, 'secret': OKX_SECRET, 'password': OKX_PASSPHRASE,
        'enableRateLimit': True, 'options': {'defaultType': 'swap'}, 'timeout': 20000
    })
    if USE_SANDBOX: ex.set_sandbox_mode(True)
    return ex

def get_current_price(ex, sym):
    try: return float(ex.fetch_ticker(sym)['last'])
    except Exception as e:
        log.warning(f"[PRICE] {sym}: {e}")
        return None

def get_equity(ex, force=False):
    if not force and time.time() - _equity_cache["ts"] < 60 and _equity_cache["value"] > 0:
        return _equity_cache["value"]
    try:
        bal = ex.fetch_balance().get('USDT', {})
        val = float(bal.get('total') or bal.get('free') or 0)
        if val > 0: _equity_cache.update({"ts": time.time(), "value": val})
        return val or _equity_cache["value"]
    except Exception as e:
        log.warning(f"[EQUITY] {e}")
        return _equity_cache["value"]

def contract_size(ex, sym):
    try: return float(ex.market(sym).get('contractSize') or 1)
    except Exception: return 1.0

def calculate_contracts(ex, sym, margin, price, lev):
    try:
        market = ex.market(sym)
        raw = (margin * lev) / (price * float(market.get('contractSize') or 1))
        amt = float(ex.amount_to_precision(sym, raw))
        min_amount = float(market.get('limits', {}).get('amount', {}).get('min') or 1.0)
        return amt if amt >= min_amount else 0.0
    except Exception as e:
        log.warning(f"[AMOUNT] {sym}: {e}")
        return 0.0

def compute_margin(equity, sl_pct):
    if not USE_RISK_SIZING or equity <= 0: return float(POSITION_SIZE_USDT)
    risk_usd = equity * RISK_PCT_PER_TRADE / 100.0
    margin = risk_usd / (sl_pct * LEVERAGE)
    return max(MIN_MARGIN_USDT, min(MAX_MARGIN_USDT, margin))

def get_open_position(ex, sym):
    for p in ex.fetch_positions([sym]):
        contracts = abs(float(p.get('contracts') or 0))
        if contracts > 0:
            return {"side": "LONG" if p.get('side') == 'long' else "SHORT",
                    "contracts": contracts,
                    "entry": float(p.get('entryPrice') or 0),
                    "margin": float(p.get('initialMargin') or 0)}
    return None

def get_protection(ex, sym):
    has_sl = has_tp = False
    try:
        market = ex.market(sym)
        algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
        for o in (algo.get('data') or []):
            if o.get('slTriggerPx'): has_sl = True
            if o.get('tpTriggerPx'): has_tp = True
    except Exception: pass
    return has_sl, has_tp

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
    except Exception as e:
        log.warning(f"[CANCEL PENDING] {sym}: {e}")

def place_sl_tp(ex, sym, side, contracts, trigger, is_sl):
    trig_str = str(ex.price_to_precision(sym, trigger))
    amt_str = str(ex.amount_to_precision(sym, contracts))
    market = ex.market(sym)
    req = {
        'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': side,
        'posSide': 'net', 'ordType': 'conditional', 'sz': amt_str, 'reduceOnly': 'true'
    }
    if is_sl: req.update({'slTriggerPx': trig_str, 'slOrdPx': '-1'})
    else: req.update({'tpTriggerPx': trig_str, 'tpOrdPx': '-1'})
    res = ex.privatePostTradeOrderAlgo(req)
    data = (res.get('data') or [{}])[0]
    if str(data.get('sCode', '0')) != '0':
        raise Exception(f"algo rejected: {data.get('sMsg')}")
    return res

def protect_position(ex, sym, close_side, contracts, sl, tp, retries=3):
    sl_ok = tp_ok = False
    for i in range(retries):
        try:
            place_sl_tp(ex, sym, close_side, contracts, sl, True)
            sl_ok = True
            break
        except Exception as e:
            log.error(f"[SL ERR] {sym} try {i+1}: {e}")
            time.sleep(0.8)
    if tp is not None and contracts > 0:
        for i in range(retries):
            try:
                place_sl_tp(ex, sym, close_side, contracts, tp, False)
                tp_ok = True
                break
            except Exception as e:
                log.error(f"[TP ERR] {sym} try {i+1}: {e}")
                time.sleep(0.8)
    return sl_ok, tp_ok

def close_position_safe(ex, sym, pos_data, amount=None):
    try:
        side = pos_data["position"]
        close_side = 'sell' if side == "LONG" else 'buy'
        contracts = pos_data["contracts"]
        close_amt = amount if amount and amount < contracts else contracts

        ex.create_order(sym, 'market', close_side, close_amt, None,
                        {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        time.sleep(0.8)
        cancel_pending(ex, sym)

        if close_amt >= contracts: return True

        remaining = contracts - close_amt
        be_sl = pos_data["entry"] * (1 + BE_FEE_BUFFER) if side == "LONG" else pos_data["entry"] * (1 - BE_FEE_BUFFER)
        sl_ok, _ = protect_position(ex, sym, close_side, remaining, be_sl, pos_data.get("tp"))
        if not sl_ok:
            log.error(f"[PARTIAL PROTECT FAIL] {sym} — إغلاق طارئ للمتبقي")
            ex.create_order(sym, 'market', close_side, remaining, None,
                            {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
            return False
        return True
    except Exception as e:
        log.error(f"[CLOSE SAFE ERR] {sym}: {e}")
        return False

def open_position(ex, sym, side, price, sl, tp, margin):
    try:
        amount = calculate_contracts(ex, sym, margin, price, LEVERAGE)
        if amount <= 0: return False, "كمية غير صالحة", price, 0.0

        try: ex.set_leverage(LEVERAGE, sym, params={'mgnMode': MARGIN_MODE})
        except Exception as e: log.warning(f"[LEVERAGE] {sym}: {e}")
        cancel_pending(ex, sym)

        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'

        order = ex.create_order(sym, 'market', order_side, amount, None, {'tdMode': MARGIN_MODE, 'posSide': 'net'})
        time.sleep(1.1)

        entry, contracts = price, amount
        try:
            pos = get_open_position(ex, sym)
            if pos:
                entry = pos["entry"] or price
                contracts = pos["contracts"]
        except Exception as e:
            log.warning(f"[POS FETCH] {sym}: {e}")

        sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, sl, tp)
        if not sl_ok:
            send(f"🚨 فشل وضع وقف الخسارة لـ <code>{sym.split(':')[0]}</code> — جاري الإغلاق.")
            ex.create_order(sym, 'market', close_side, contracts, None, {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
            return False, "فشل وضع SL", entry, 0.0
        if not tp_ok:
            send(f"⚠️ تم وضع SL لكن فشل TP لـ <code>{sym.split(':')[0]}</code>")

        return True, order.get('id'), entry, contracts
    except Exception as e:
        log.error(f"[OPEN ERR] {sym}: {e}")
        return False, str(e), price, 0.0

# ==========================================
# إنهاء الصفقة
# ==========================================
def get_exit_price(ex, sym, pos_data):
    try:
        close_side = 'sell' if pos_data["position"] == "LONG" else 'buy'
        since = int(pos_data.get("opened_ts", 0) * 1000) - 8000
        trades = ex.fetch_my_trades(sym, since=since, limit=80)
        fills = [t for t in trades if t.get('side') == close_side and t.get('price') and t.get('amount')]
        total = sum(float(t['amount']) for t in fills)
        if total > 0:
            return sum(float(t['price']) * float(t['amount']) for t in fills) / total
    except Exception as e:
        log.warning(f"[EXIT PRICE] {sym}: {e}")
    return None

def detect_reason(pos_data, exit_price):
    side, entry, sl, tp = pos_data["position"], pos_data["entry"], pos_data["sl"], pos_data["tp"]
    if tp and abs(exit_price - tp) / max(tp, 1e-9) < 0.0035: return "Take Profit"
    if sl and abs(exit_price - sl) / max(sl, 1e-9) < 0.0035:
        if pos_data.get("breakeven_hit"):
            profitable = (sl > entry * 1.0015) if side == "LONG" else (sl < entry * 0.9985)
            return "Trailing Stop" if profitable else "Breakeven Hit"
        return "Stop Loss"
    return "Manual/External"

def finalize_trade(ex, sym, reason_hint=None):
    with state_lock:
        pos_data = dict(state.get(sym, {}))
    if not pos_data.get("position"): return
    side, entry = pos_data["position"], pos_data["entry"]
    exit_price = get_exit_price(ex, sym, pos_data) or get_current_price(ex, sym) or entry

    cs = contract_size(ex, sym)
    direction = 1 if side == "LONG" else -1
    pnl = (exit_price - entry) * direction * pos_data.get("contracts", 0.0) * cs
    margin = pos_data.get("margin") or POSITION_SIZE_USDT
    reason = reason_hint or detect_reason(pos_data, exit_price)

    update_daily_stats(pnl)
    reset_sym_state(sym, cooldown=True)
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, pnl, margin, reason))

def emergency_close(ex, sym, why):
    send(f"🚨 <b>إغلاق طارئ</b> <code>{sym.split(':')[0]}</code>\n{why}")
    with state_lock:
        pos_data = dict(state.get(sym, {}))
    if pos_data.get("position"):
        if close_position_safe(ex, sym, pos_data):
            finalize_trade(ex, sym, reason_hint="Emergency")
        else:
            send(f"🆘 فشل الإغلاق الطارئ لـ <code>{sym.split(':')[0]}</code>")

# ==========================================
# كشف نظام السوق + الاستراتيجية
# ==========================================
def detect_regime(df_15m, df_1h, df_4h):
    if len(df_4h) < 50 or len(df_1h) < 50: return "unknown"
    last_4h = df_4h.iloc[-2]
    last_1h = df_1h.iloc[-2]
    
    adx4 = last_4h.get("adx", 0)
    adx1 = last_1h.get("adx", 0)
    ema50_4 = last_4h.get("ema50", last_4h["close"])
    
    up_4h = last_4h["close"] > ema50_4 and last_4h["ema9"] > last_4h["ema21"]
    down_4h = last_4h["close"] < ema50_4 and last_4h["ema9"] < last_4h["ema21"]

    if adx4 > 28 and adx1 > 24:
        if up_4h: return "strong_trend_up"
        if down_4h: return "strong_trend_down"

    if adx4 > 20 and (up_4h or down_4h): return "weak_trend"

    return "range"

def check_entry_signal(df_15m, df_1h, df_4h):
    if len(df_15m) < 70 or len(df_1h) < 60 or len(df_4h) < 60: return None

    last = df_15m.iloc[-2]
    prev = df_15m.iloc[-3]
    last_1h = df_1h.iloc[-2]
    last_4h = df_4h.iloc[-2]

    needed = ["ema9", "ema21", "ema50", "rsi", "adx", "atr", "vol_ma"]
    if any(last.get(k) is None or pd.isna(last.get(k)) for k in needed): return None

    price = float(last["close"])
    atr_val = float(last["atr"])
    ema21 = float(last["ema21"])
    ema50 = float(last["ema50"])
    rsi_val = float(last["rsi"])
    adx_val = float(last["adx"])

    regime = detect_regime(df_15m, df_1h, df_4h)
    if regime == "range": return None

    atr_pct = atr_val / price
    if atr_pct < 0.004 or atr_pct > 0.045: return None

    vol_ok = last["volume"] > 1.2 * last["vol_ma"]
    if not vol_ok or adx_val < 23: return None

    dist_ema21 = abs(price - ema21) / price
    dist_ema50 = abs(price - ema50) / price
    near_ema = dist_ema21 < 0.011 or dist_ema50 < 0.016

    bullish_reject = last["close"] > last["open"] and (last["low"] < ema21 or prev["low"] < ema21)
    bearish_reject = last["close"] < last["open"] and (last["high"] > ema21 or prev["high"] > ema21)

    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * atr_val / price))
    tp_pct = sl_pct * RR_RATIO

    if regime in ("strong_trend_up", "weak_trend") and last_4h["close"] > last_4h["ema50"]:
        if (last["ema9"] > last["ema21"] and last_1h["ema9"] > last_1h["ema21"]) and near_ema and 40 < rsi_val < 61 and bullish_reject:
            return {"side": "LONG", "price": price, "sl_pct": sl_pct, "sl": price * (1 - sl_pct), "tp": price * (1 + tp_pct), "regime": regime}

    if regime in ("strong_trend_down", "weak_trend") and last_4h["close"] < last_4h["ema50"]:
        if (last["ema9"] < last["ema21"] and last_1h["ema9"] < last_1h["ema21"]) and near_ema and 39 < rsi_val < 60 and bearish_reject:
            return {"side": "SHORT", "price": price, "sl_pct": sl_pct, "sl": price * (1 + sl_pct), "tp": price * (1 - tp_pct), "regime": regime}

    return None

def market_filters_ok(ex, sym, side):
    try:
        t = ex.fetch_ticker(sym)
        bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
        if bid and ask and last and (ask - bid) / last > MAX_SPREAD_PCT: return False, "سبريد عالي"
    except Exception: pass
    try:
        fr = ex.fetch_funding_rate(sym).get('fundingRate')
        if fr is not None:
            if side == "LONG" and fr > MAX_FUNDING_RATE: return False, "فندنغ مرتفع"
            if side == "SHORT" and fr < -MAX_FUNDING_RATE: return False, "فندنغ سلبي"
    except Exception: pass
    return True, ""

def fetch_data(ex, sym, tf, limit=130):
    key = (sym, tf)
    ttl = CACHE_TTL.get(tf, 0)
    cached = _data_cache.get(key)
    if cached and ttl and time.time() - cached[0] < ttl: return cached[1]
    for _ in range(3):
        try:
            data = ex.fetch_ohlcv(sym, tf, limit=limit)
            _data_cache[key] = (time.time(), data)
            return data
        except Exception as e:
            log.warning(f"[OHLCV] {sym} {tf}: {e}")
            time.sleep(1.3)
    return []

def to_df(raw):
    return add_indicators(pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"]))

# ==========================================
# إدارة الصفقة المتقدمة
# ==========================================
def move_sl(ex, sym, pos_data, new_sl, title):
    side = pos_data["position"]
    close_side = 'sell' if side == "LONG" else 'buy'
    contracts = pos_data["contracts"]

    cancel_pending(ex, sym)
    sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, new_sl, pos_data.get("tp"))
    if not sl_ok:
        protect_position(ex, sym, close_side, contracts, pos_data["sl"], None)
        return False

    with state_lock:
        state[sym]["sl"] = new_sl
        state[sym]["breakeven_hit"] = True
    save_state()
    send(f"{title}\n━━━━━━━━━━━━━━━\n🪙 <code>{sym.split(':')[0]}</code>\n🛑 الوقف الجديد: <code>{fmt(new_sl)}</code>")
    return True

def manage_partials(ex, sym, pos_data, price):
    side = pos_data["position"]
    entry = pos_data["entry"]
    risk = abs(entry - pos_data["sl"])
    if risk <= 0: return False

    contracts = pos_data["contracts"]

    if not pos_data.get("partial_1_taken"):
        target1 = entry + 1.3 * risk if side == "LONG" else entry - 1.3 * risk
        reached = price >= target1 if side == "LONG" else price <= target1
        if reached and contracts > 0:
            close_amt = contracts * 0.40
            if close_position_safe(ex, sym, pos_data, amount=close_amt):
                new_contracts = contracts - close_amt
                be_sl = entry * (1 + BE_FEE_BUFFER) if side == "LONG" else entry * (1 - BE_FEE_BUFFER)
                with state_lock:
                    state[sym]["contracts"] = new_contracts
                    state[sym]["partial_1_taken"] = True
                    state[sym]["sl"] = be_sl
                    state[sym]["breakeven_hit"] = True
                save_state()
                send(f"💰 <b>إغلاق جزئي أول (40%)</b> — <code>{sym.split(':')[0]}</code>\nتم تأمين الأرباح + نقل الوقف للتعادل")
                return True

    if pos_data.get("partial_1_taken") and not pos_data.get("partial_2_taken"):
        target2 = entry + 2.1 * risk if side == "LONG" else entry - 2.1 * risk
        reached = price >= target2 if side == "LONG" else price <= target2
        if reached and contracts > 0:
            close_amt = contracts * 0.50
            if close_position_safe(ex, sym, pos_data, amount=close_amt):
                new_contracts = contracts - close_amt
                with state_lock:
                    state[sym]["contracts"] = new_contracts
                    state[sym]["partial_2_taken"] = True
                save_state()
                send(f"💰 <b>إغلاق جزئي ثاني</b> — <code>{sym.split(':')[0]}</code>\nتم قفل المزيد من الأرباح")
                return True

    return False

def manage_position(ex, sym, pos_data):
    side = pos_data["position"]
    entry = pos_data["entry"]
    sl = pos_data["sl"]
    price = get_current_price(ex, sym)
    if not price: return

    try:
        has_sl, _ = get_protection(ex, sym)
        if not has_sl:
            close_side = 'sell' if side == "LONG" else 'buy'
            sl_ok, _ = protect_position(ex, sym, close_side, pos_data["contracts"], sl, pos_data.get("tp"))
            if not sl_ok:
                emergency_close(ex, sym, "الصفقة بدون وقف خسارة")
                return
    except Exception: pass

    if manage_partials(ex, sym, pos_data, price): return

    if not pos_data.get("breakeven_hit"):
        risk = abs(entry - sl)
        trigger = entry + risk * 0.9 if side == "LONG" else entry - risk * 0.9
        if (price >= trigger if side == "LONG" else price <= trigger):
            be_sl = entry * (1 + BE_FEE_BUFFER) if side == "LONG" else entry * (1 - BE_FEE_BUFFER)
            move_sl(ex, sym, pos_data, be_sl, "🛡️ تأمين الصفقة (Breakeven)")
        return

    raw = fetch_data(ex, sym, "15m")
    if len(raw) < 50: return
    df = to_df(raw)
    atr_val = df.iloc[-2].get("atr")
    if atr_val is None or pd.isna(atr_val): return

    mult = TRAIL_ATR_MULT
    if pos_data.get("partial_2_taken"): mult *= 0.82
    elif pos_data.get("partial_1_taken"): mult *= 0.90

    if side == "LONG":
        new_sl = price - mult * float(atr_val)
        if new_sl > sl * 1.001 and new_sl < price:
            move_sl(ex, sym, pos_data, new_sl, "📈 تحريك الوقف المتحرك")
    else:
        new_sl = price + mult * float(atr_val)
        if new_sl < sl * 0.999 and new_sl > price:
            move_sl(ex, sym, pos_data, new_sl, "📈 تحريك الوقف المتحرك")

# ==========================================
# معالجة الرموز
# ==========================================
def entries_allowed(ex):
    if meta.get("paused"): return False
    roll_daily()
    equity = get_equity(ex)
    if equity <= 0: return True

    if daily_pnl["value"] <= -equity * DAILY_LOSS_LIMIT_PCT / 100.0:
        today = daily_pnl["date"]
        if meta.get("loss_alert") != today:
            meta["loss_alert"] = today
            save_state()
            send(f"⛔ <b>وصلنا حد الخسارة اليومي</b>\nتم إيقاف الصفقات الجديدة")
        return False

    if daily_pnl["value"] >= equity * DAILY_PROFIT_TARGET_PCT / 100.0:
        today = daily_pnl["date"]
        if meta.get("profit_alert") != today:
            meta["profit_alert"] = today
            save_state()
            send(f"✅ <b>تحقق الهدف اليومي للربح</b>\nتم إيقاف الدخول لحماية الأرباح")
        return False

    return True

def process_symbol(ex, sym):
    try:
        with trade_lock:
            with state_lock:
                if sym not in state: state[sym] = new_sym_state()
                pos_data = dict(state[sym])

            if pos_data.get("position") is not None:
                exch_pos = get_open_position(ex, sym)
                if exch_pos is None:
                    finalize_trade(ex, sym)
                    return
                with state_lock:
                    state[sym]["contracts"] = exch_pos["contracts"]
                manage_position(ex, sym, dict(state[sym]))
                return

            if time.time() - pos_data.get("last_exit_ts", 0) < REENTRY_COOLDOWN_SEC: return
            if not entries_allowed(ex): return

            with state_lock:
                open_count = sum(1 for v in state.values() if v.get("position"))
            if open_count >= MAX_OPEN_POSITIONS: return

            raw_15m = fetch_data(ex, sym, "15m")
            raw_1h = fetch_data(ex, sym, "1h")
            raw_4h = fetch_data(ex, sym, "4h")
            if len(raw_15m) < 70 or len(raw_1h) < 60 or len(raw_4h) < 60: return

            df_15m, df_1h, df_4h = to_df(raw_15m), to_df(raw_1h), to_df(raw_4h)
            signal = check_entry_signal(df_15m, df_1h, df_4h)
            if not signal: return

            same_dir = sum(1 for v in state.values() if v.get("position") == signal["side"])
            if same_dir >= MAX_SAME_DIRECTION: return

            ok, why = market_filters_ok(ex, sym, signal["side"])
            if not ok: return

            actual_price = get_current_price(ex, sym) or signal["price"]
            sl_pct = signal["sl_pct"]
            sl = actual_price * (1 - sl_pct) if signal["side"] == "LONG" else actual_price * (1 + sl_pct)
            tp = actual_price * (1 + sl_pct * RR_RATIO) if signal["side"] == "LONG" else actual_price * (1 - sl_pct * RR_RATIO)

            margin = compute_margin(get_equity(ex), sl_pct)
            success, info, entry, contracts = open_position(ex, sym, signal["side"], actual_price, sl, tp, margin)

            if success:
                with state_lock:
                    state[sym].update({
                        "position": signal["side"], "entry": entry, "sl": sl, "tp": tp,
                        "contracts": contracts, "initial_contracts": contracts,
                        "margin": margin, "opened_ts": time.time(),
                        "breakeven_hit": False, "partial_1_taken": False, "partial_2_taken": False,
                        "regime": signal.get("regime", "")
                    })
            else:
                with state_lock:
                    state[sym]["last_exit_ts"] = time.time()
            save_state()

            caption = msg_entry(sym, signal["side"], entry, sl, tp, margin, success, signal.get("regime", ""))
            if not success: caption += f"\n❌ {info}"
            send(caption)

    except Exception as e:
        log.error(f"[ERROR] {sym}: {e}")

# ==========================================
# مزامنة + الحلقة الرئيسية
# ==========================================
def sync_positions(ex, active_symbols):
    try:
        exch = {}
        for p in ex.fetch_positions():
            c = abs(float(p.get('contracts') or 0))
            if c > 0 and p.get('symbol') in active_symbols: exch[p['symbol']] = p
    except Exception as e:
        log.error(f"[SYNC] {e}"); return

    with trade_lock:
        for sym in active_symbols:
            with state_lock:
                st = dict(state.get(sym, new_sym_state()))

            if st.get("position") and sym not in exch:
                finalize_trade(ex, sym)
            elif st.get("position") and sym in exch:
                p = exch[sym]
                contracts = abs(float(p.get('contracts')))
                with state_lock:
                    state[sym]["contracts"] = contracts
                try:
                    has_sl, _ = get_protection(ex, sym)
                    if not has_sl:
                        close_side = 'sell' if st["position"] == "LONG" else 'buy'
                        protect_position(ex, sym, close_side, contracts, st["sl"], st.get("tp"))
                except Exception: pass
            elif not st.get("position") and sym in exch:
                p = exch[sym]
                side = "LONG" if p.get('side') == 'long' else "SHORT"
                entry = float(p.get('entryPrice') or 0)
                contracts = abs(float(p.get('contracts')))
                sl_pct = 0.017
                sl = entry * (1 - sl_pct) if side == "LONG" else entry * (1 + sl_pct)
                tp = entry * (1 + sl_pct * RR_RATIO) if side == "LONG" else entry * (1 - sl_pct * RR_RATIO)
                close_side = 'sell' if side == "LONG" else 'buy'
                cancel_pending(ex, sym)
                if protect_position(ex, sym, close_side, contracts, sl, tp)[0]:
                    with state_lock:
                        state[sym] = new_sym_state()
                        state[sym].update({
                            "position": side, "entry": entry, "sl": sl, "tp": tp,
                            "contracts": contracts, "initial_contracts": contracts,
                            "margin": float(p.get('initialMargin') or POSITION_SIZE_USDT),
                            "opened_ts": time.time()
                        })
                    send(f"♻️ تم تبني مركز <code>{sym.split(':')[0]}</code> ({side})")
        save_state()

def status_text(ex):
    roll_daily()
    equity = get_equity(ex, force=True)
    
    # حساب إجمالي الربح العائم (لجميع الصفقات المفتوحة)
    total_floating_pnl = 0.0
    with state_lock:
        opens = {k: dict(v) for k, v in state.items() if v.get("position")}
    
    pos_lines = []
    for sym, v in opens.items():
        price = get_current_price(ex, sym) or v["entry"]
        d = 1 if v["position"] == "LONG" else -1
        upnl = (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
        total_floating_pnl += upnl
        
        flags = ""
        if v.get("partial_2_taken"): flags += " 💰💰"
        elif v.get("partial_1_taken"): flags += " 💰"
        if v.get("breakeven_hit"): flags += " 🛡️"
        pos_lines.append(f"• <code>{sym.split(':')[0]}</code> {v['position']} | {fmt(v['entry'])} → {fmt(price)} | {upnl:+.2f}{flags}")

    lines = [
        f"📊 <b>حالة البوت V39</b> {'⏸️' if meta.get('paused') else '▶️'}",
        "━━━━━━━━━━━━━━━",
        f"💼 الرصيد: {equity:,.2f} USDT",
        f"📅 المغلق اليوم: {daily_pnl['value']:+.2f} USDT | صفقات: {daily_pnl['trades']} | رابحة: {daily_pnl['wins']}",
        f"📈 <b>الربح العائم (المفتوح): {total_floating_pnl:+.2f} USDT</b>",
        "━━━━━━━━━━━━━━━"
    ]
    
    if not opens:
        lines.append("لا توجد صفقات مفتوحة.")
    else:
        lines.extend(pos_lines)
        
    return "\n".join(lines)

def find_symbol(arg, active_symbols):
    arg = arg.upper().replace("/USDT", "").replace("USDT", "")
    for s in active_symbols:
        if s.startswith(arg + "/"): return s
    return None

# ==========================================
# أوامر تيليجرام
# ==========================================
def handle_command(ex, text, chat_id, active_symbols):
    parts = text.strip().split()
    cmd = parts[0].lower().split("@")[0]

    if cmd in ("/status", "/start"):
        send(status_text(ex), chat_id)
    elif cmd == "/pause":
        meta["paused"] = True; save_state()
        send("⏸️ تم إيقاف فتح صفقات جديدة (الصفقات المفتوحة تبقى مُدارة).", chat_id)
    elif cmd == "/resume":
        meta["paused"] = False; save_state()
        send("▶️ تم استئناف البحث عن صفقات.", chat_id)
    elif cmd == "/close":
        if len(parts) < 2:
            send("استخدم: /close BTC", chat_id); return
        sym = find_symbol(parts[1], active_symbols)
        if not sym:
            send("عملة غير معروفة.", chat_id); return
        with trade_lock:
            with state_lock:
                pos_data = dict(state.get(sym, {}))
            if not pos_data.get("position"):
                send("لا توجد صفقة مفتوحة على هذه العملة.", chat_id); return
            if close_position_safe(ex, sym, pos_data):
                finalize_trade(ex, sym, reason_hint="Manual")
            else:
                send("❌ فشل الإغلاق، تحقق من المنصة.", chat_id)
    elif cmd == "/closeall":
        with trade_lock:
            with state_lock:
                syms = [k for k, v in state.items() if v.get("position")]
            if not syms:
                send("لا توجد صفقات مفتوحة.", chat_id); return
            for sym in syms:
                with state_lock:
                    pos_data = dict(state.get(sym, {}))
                if close_position_safe(ex, sym, pos_data):
                    finalize_trade(ex, sym, reason_hint="Manual")
    elif cmd == "/help":
        send("<b>الأوامر المتاحة:</b>\n"
             "/status - عرض حالة البوت والرصيد\n"
             "/pause - إيقاف الدخول مؤقتاً\n"
             "/resume - استئناف الدخول\n"
             "/close BTC - إغلاق صفقة عملة محددة\n"
             "/closeall - إغلاق جميع الصفقات", chat_id)

def telegram_listener(active_symbols):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS: return
    try:
        ex2 = create_exchange(); ex2.load_markets()
    except Exception as e:
        log.error(f"[TG INIT] {e}"); return
    offset = None
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                             params={"timeout": 25, "offset": offset}, timeout=35).json()
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text, chat_id = msg.get("text", ""), str(msg.get("chat", {}).get("id", ""))
                if chat_id not in AUTHORIZED_CHATS or not text.startswith("/"): continue
                try:
                    handle_command(ex2, text, chat_id, active_symbols)
                except Exception as e:
                    send(f"خطأ: {e}", chat_id)
        except Exception as e:
            log.warning(f"[TG POLL] {e}"); time.sleep(5)

def maybe_send_daily_report(ex):
    now = datetime.now(KSA_TZ)
    today = str(now.date())
    if ((now.hour == 23 and now.minute >= 55) or (now.hour == 0 and now.minute < 12)) and meta.get("last_report") != today:
        roll_daily()
        wr = (daily_pnl["wins"] / daily_pnl["trades"] * 100) if daily_pnl["trades"] else 0
        send(f"📋 <b>التقرير اليومي V39</b> — {today}\n━━━━━━━━━━━━━━━\n"
             f"💰 النتيجة: {daily_pnl['value']:+.2f} USDT\n"
             f"🔢 الصفقات: {daily_pnl['trades']} | الرابحة: {daily_pnl['wins']} ({wr:.0f}%)\n"
             f"💼 الرصيد: {get_equity(ex, force=True):,.2f} USDT")
        meta["last_report"] = today
        save_state()

def maybe_send_heartbeat(ex):
    now = datetime.now(KSA_TZ)
    current_hour = now.strftime("%Y-%m-%d %H")
    if meta.get("last_heartbeat") != current_hour and now.minute < 4:
        meta["last_heartbeat"] = current_hour
        save_state()
        with state_lock:
            open_ct = sum(1 for v in state.values() if v.get("position"))
        log.info(f"[HEARTBEAT] Open: {open_ct} | Equity: {get_equity(ex)}")

def init_exchange():
    ex = create_exchange(); ex.load_markets()
    return ex

def trading_loop():
    log.info("[INIT] Starting V39 Advanced Bot...")
    if not OKX_API_KEY: return send("❌ مفاتيح API غير موجودة")

    try:
        ex = init_exchange()
        load_state()
        balance = get_equity(ex, force=True)
        mode_str = "تجريبي (Sandbox)" if USE_SANDBOX else "حقيقي"
        log.info(f"[OKX] Connected ({mode_str}). Equity: {balance}")
    except Exception as e:
        return send(f"❌ فشل الاتصال: {e}")

    active_symbols = [s for s in FUTURES_SYMBOLS if s in ex.markets]
    sync_positions(ex, active_symbols)
    threading.Thread(target=telegram_listener, args=(active_symbols,), daemon=True).start()

    send(f"🚀 <b>تم تشغيل البوت V39 (الآمن)</b>\n"
         f"━━━━━━━━━━━━━━━\n"
         f"⚙️ الوضع: {mode_str}\n"
         f"🪙 العملات: {len(active_symbols)}\n"
         f"📊 أقصى صفقات: {MAX_OPEN_POSITIONS}\n"
         f"💰 المخاطرة: {RISK_PCT_PER_TRADE}% من الرصيد\n"
         f"🎯 Partial: 40% @ 1.3R → 30% @ 2.1R + Trailing\n"
         f"━━━━━━━━━━━━━━━")

    fail_streak = 0
    while True:
        start = time.time()
        cycle_errors = 0
        try:
            for sym in active_symbols:
                try:
                    process_symbol(ex, sym)
                except Exception as e:
                    cycle_errors += 1
                    log.error(f"[LOOP] {sym}: {e}")
                time.sleep(0.4)

            maybe_send_daily_report(ex)
            maybe_send_heartbeat(ex)
        except Exception as e:
            cycle_errors += 1
            log.error(f"[LOOP FATAL] {e}")

        fail_streak = fail_streak + 1 if cycle_errors >= 5 else 0
        if fail_streak >= 3:
            try:
                ex = init_exchange()
                fail_streak = 0
                log.warning("[RECONNECT] تم إعادة الاتصال")
            except Exception as e:
                log.error(f"[RECONNECT FAIL] {e}")

        elapsed = time.time() - start
        time.sleep(max(8, SCAN_INTERVAL - elapsed))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
