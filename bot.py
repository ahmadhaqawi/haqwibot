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
# الإعدادات
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

SCAN_INTERVAL = 60
LEVERAGE = 5
MARGIN_MODE = "isolated"
MAX_OPEN_POSITIONS = 5
REENTRY_COOLDOWN_SEC = 1800

# --- حجم الصفقة ---
USE_RISK_SIZING = False       
RISK_PCT_PER_TRADE = 1.0      
POSITION_SIZE_USDT = 50       
MIN_MARGIN_USDT = 10
MAX_MARGIN_USDT = 100

# --- SL/TP حسب ATR ---
ATR_SL_MULT = 1.5
RR_RATIO = 2.0                
MIN_SL_PCT = 0.008            
MAX_SL_PCT = 0.04             
TRAIL_ATR_MULT = 1.0          
BE_FEE_BUFFER = 0.0007        

# --- حماية المحفظة ---
DAILY_LOSS_LIMIT_PCT = 5.0    
MAX_SAME_DIRECTION = 3        

# --- فلاتر السوق ---
MAX_SPREAD_PCT = 0.001        
MAX_FUNDING_RATE = 0.0005     

CACHE_TTL = {"15m": 0, "1h": 300, "4h": 900}

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")

state_lock = threading.Lock()
trade_lock = threading.RLock()   
state = {}
meta = {"paused": False, "last_report": "", "loss_alert": "", "last_heartbeat": ""}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}

_data_cache = {}
_equity_cache = {"ts": 0, "value": 0.0}

# ==========================================
# Flask Health Check
# ==========================================
@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_v35_robust"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# إدارة الحالة
# ==========================================
def new_sym_state():
    return {"position": None, "entry": None, "sl": None, "tp": None, "contracts": 0.0,
            "margin": 0.0, "opened_ts": 0, "last_exit_ts": 0, "breakeven_hit": False}

def load_state():
    global state, daily_pnl, meta
    if not os.path.exists(STATE_FILE):
        return
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
        if pnl_usdt > 0:
            daily_pnl["wins"] += 1
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
    if not TELEGRAM_BOT_TOKEN:
        return
    targets = [chat_id] if chat_id else list(AUTHORIZED_CHATS)
    for cid in targets:
        try:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                          json={"chat_id": cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
        except Exception as e:
            log.warning(f"[TG SEND] {e}")

def fmt(p):
    if p is None or (isinstance(p, float) and np.isnan(p)):
        return "0.00"
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

# ==========================================
# الرسائل
# ==========================================
REASONS = {
    "Take Profit": "تحقق الهدف 🎯",
    "Stop Loss": "ضرب وقف الخسارة 🛑",
    "Breakeven Hit": "خروج آمن (Breakeven) 🛡️",
    "Trailing Stop": "خروج بوقف متحرك (ربح مؤمَّن) 📈",
    "Manual": "إغلاق يدوي من تيليجرام 🖐️",
    "Emergency": "إغلاق طارئ (فشل حماية الصفقة) 🚨",
    "Manual/External": "إغلاق من المنصة ⚡",
}

def msg_entry(sym, side, price, sl, tp, margin, real_order):
    title = "🟢 <b>توصية شراء (LONG)</b>" if side == "LONG" else "🔴 <b>توصية بيع (SHORT)</b>"
    status = "✅ <b>تم فتح الصفقة آلياً</b>" if real_order else "⚠️ <b>إشارة فقط (لم تفتح)</b>"

    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * LEVERAGE
    reward_usd = margin * (tp_pct / 100) * LEVERAGE
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")

    return (f"{title}\n{status}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <b>الزوج:</b> <code>{sym.split(':')[0]}</code>\n"
            f"💵 <b>سعر الدخول:</b> <code>{fmt(price)}</code>\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x | <b>الهامش:</b> {margin:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 <b>وقف الخسارة (SL):</b> <code>{fmt(sl)}</code> ({sl_pct:.2f}%)\n"
            f"📉 <i>الخسارة المتوقعة: {risk_usd:.2f} USDT</i>\n"
            f"🎯 <b>هدف الربح (TP):</b> <code>{fmt(tp)}</code> ({tp_pct:.2f}%)\n"
            f"📈 <i>الربح المتوقع: +{reward_usd:.2f} USDT</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ <b>وقت الدخول:</b> {now_str}")

def msg_exit(sym, side, exit_price, entry_price, pnl_usd, margin, reason):
    pct = (pnl_usd / margin * 100) if margin else 0.0
    if pnl_usd > 0: title = "🟢 <b>إغلاق صفقة بربح</b>"
    elif pnl_usd < 0: title = "🔴 <b>إغلاق صفقة بخسارة</b>"
    else: title = "⚪ <b>إغلاق صفقة (تعادل)</b>"

    return (f"{title}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <b>الزوج:</b> <code>{sym.split(':')[0]}</code> ({side})\n"
            f"📥 <b>الدخول:</b> <code>{fmt(entry_price)}</code>\n"
            f"📤 <b>الخروج:</b> <code>{fmt(exit_price)}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 <b>النسبة على الهامش:</b> {pct:+.2f}%\n"
            f"💰 <b>الربح/الخسارة:</b> {pnl_usd:+.2f} USDT\n"
            f"📝 <b>السبب:</b> {REASONS.get(reason, reason)}\n"
            f"━━━━━━━━━━━━━━━")

# ==========================================
# المؤشرات الفنية
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
    if len(df) < 30: return df
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
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
        'apiKey': OKX_API_KEY,
        'secret': OKX_SECRET,
        'password': OKX_PASSPHRASE,
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 20000
    })
    if USE_SANDBOX:
        ex.set_sandbox_mode(True)
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
        if val > 0:
            _equity_cache.update({"ts": time.time(), "value": val})
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
    if not USE_RISK_SIZING or equity <= 0:
        return float(POSITION_SIZE_USDT)
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
    market = ex.market(sym)
    algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
    for o in (algo.get('data') or []):
        if o.get('slTriggerPx'): has_sl = True
        if o.get('tpTriggerPx'): has_tp = True
    return has_sl, has_tp

def cancel_pending(ex, sym):
    try:
        market = ex.market(sym)
        algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
        for o in (algo.get('data') or []):
            try: ex.privatePostTradeCancelAlgos([{'instId': market['id'], 'algoId': o['algoId']}])
            except Exception as e: log.warning(f"[CANCEL ALGO] {sym}: {e}")
        for o in ex.fetch_open_orders(sym):
            try: ex.cancel_order(o['id'], sym)
            except Exception as e: log.warning(f"[CANCEL ORDER] {sym}: {e}")
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
            time.sleep(1)
    if tp is not None:
        for i in range(retries):
            try:
                place_sl_tp(ex, sym, close_side, contracts, tp, False)
                tp_ok = True
                break
            except Exception as e:
                log.error(f"[TP ERR] {sym} try {i+1}: {e}")
                time.sleep(1)
    return sl_ok, tp_ok

def close_position(ex, sym):
    try:
        cancel_pending(ex, sym)
        for p in ex.fetch_positions([sym]):
            contracts = abs(float(p.get('contracts') or 0))
            if contracts > 0:
                side = 'sell' if p['side'] == 'long' else 'buy'
                ex.create_order(sym, 'market', side, contracts, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        time.sleep(0.5)
        return get_open_position(ex, sym) is None
    except Exception as e:
        log.error(f"[CLOSE ERR] {sym}: {e}")
        return False

def open_position(ex, sym, side, price, sl, tp, margin):
    try:
        amount = calculate_contracts(ex, sym, margin, price, LEVERAGE)
        if amount <= 0:
            return False, "كمية غير صالحة", price, 0.0

        try: ex.set_leverage(LEVERAGE, sym, params={'mgnMode': MARGIN_MODE})
        except Exception as e: log.warning(f"[LEVERAGE] {sym}: {e}")
        cancel_pending(ex, sym)

        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'

        order = ex.create_order(sym, 'market', order_side, amount, None, {'tdMode': MARGIN_MODE, 'posSide': 'net'})
        time.sleep(1.0)

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
            send(f"🚨 <b>فشل وضع وقف الخسارة</b> لـ <code>{sym.split(':')[0]}</code>\nجاري إغلاق الصفقة فوراً.")
            closed = close_position(ex, sym)
            if not closed:
                send(f"🆘 <b>تنبيه عاجل:</b> فشل إغلاق <code>{sym.split(':')[0]}</code> وهي بدون حماية! أغلقها يدوياً الآن.")
            return False, "فشل وضع SL", entry, 0.0
        if not tp_ok:
            send(f"⚠️ تم وضع SL لكن فشل TP لـ <code>{sym.split(':')[0]}</code>. راقب الصفقة.")

        return True, order.get('id'), entry, contracts
    except Exception as e:
        log.error(f"[OPEN ERR] {sym}: {e}")
        return False, str(e), price, 0.0

# ==========================================
# إنهاء الصفقة (حساب الربح الفعلي)
# ==========================================
def get_exit_price(ex, sym, pos_data):
    try:
        close_side = 'sell' if pos_data["position"] == "LONG" else 'buy'
        since = int(pos_data.get("opened_ts", 0) * 1000) - 5000
        trades = ex.fetch_my_trades(sym, since=since, limit=100)
        fills = [t for t in trades if t.get('side') == close_side and t.get('price') and t.get('amount')]
        total = sum(float(t['amount']) for t in fills)
        if total > 0:
            return sum(float(t['price']) * float(t['amount']) for t in fills) / total
    except Exception as e:
        log.warning(f"[EXIT PRICE] {sym}: {e}")
    return None

def detect_reason(pos_data, exit_price):
    side, entry, sl, tp = pos_data["position"], pos_data["entry"], pos_data["sl"], pos_data["tp"]
    if tp and abs(exit_price - tp) / tp < 0.003:
        return "Take Profit"
    if sl and abs(exit_price - sl) / sl < 0.003:
        if pos_data.get("breakeven_hit"):
            profitable = (sl > entry * 1.002) if side == "LONG" else (sl < entry * 0.998)
            return "Trailing Stop" if profitable else "Breakeven Hit"
        return "Stop Loss"
    return "Manual/External"

def finalize_trade(ex, sym, reason_hint=None):
    with state_lock:
        pos_data = dict(state.get(sym, {}))
    if not pos_data.get("position"):
        return
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
    if close_position(ex, sym):
        finalize_trade(ex, sym, reason_hint="Emergency")
    else:
        send(f"🆘 <b>فشل الإغلاق الطارئ</b> لـ <code>{sym.split(':')[0]}</code> — تدخل يدوي مطلوب!")

# ==========================================
# الاستراتيجية
# ==========================================
def check_entry_signal(df_15m, df_1h, df_4h):
    if len(df_15m) < 50 or len(df_1h) < 50 or len(df_4h) < 50: return None
    last_15m, last_1h, last_4h = df_15m.iloc[-2], df_1h.iloc[-2], df_4h.iloc[-2]

    needed = [last_15m.get(k) for k in ("ema9", "ema21", "rsi", "adx", "atr", "vol_ma")]
    if any(v is None or pd.isna(v) for v in needed): return None

    price = float(last_15m["close"])
    trend_up = (last_15m["ema9"] > last_15m["ema21"]
                and last_1h["close"] > last_1h["ema21"] and last_1h["ema9"] > last_1h["ema21"]
                and last_4h["close"] > last_4h["ema21"])
    trend_down = (last_15m["ema9"] < last_15m["ema21"]
                  and last_1h["close"] < last_1h["ema21"] and last_1h["ema9"] < last_1h["ema21"]
                  and last_4h["close"] < last_4h["ema21"])

    adx_strong = last_15m["adx"] > 30
    vol_ok = last_15m["volume"] > 1.3 * last_15m["vol_ma"]
    if not (adx_strong and vol_ok): return None

    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * float(last_15m["atr"]) / price))
    tp_pct = sl_pct * RR_RATIO
    support, resistance = last_15m["low20"], last_15m["high20"]

    if trend_up and 45 < last_15m["rsi"] < 65:
        return {"side": "LONG", "price": price, "sl_pct": sl_pct,
                "sl": price * (1 - sl_pct), "tp": price * (1 + tp_pct),
                "support": support, "resistance": resistance}
    if trend_down and 35 < last_15m["rsi"] < 55:
        return {"side": "SHORT", "price": price, "sl_pct": sl_pct,
                "sl": price * (1 + sl_pct), "tp": price * (1 - tp_pct),
                "support": support, "resistance": resistance}
    return None

def market_filters_ok(ex, sym, side):
    try:
        t = ex.fetch_ticker(sym)
        bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
        if bid and ask and last and (ask - bid) / last > MAX_SPREAD_PCT:
            return False, f"سبريد عالي ({(ask - bid) / last * 100:.3f}%)"
    except Exception as e:
        log.warning(f"[SPREAD] {sym}: {e}")
    try:
        fr = ex.fetch_funding_rate(sym).get('fundingRate')
        if fr is not None:
            if side == "LONG" and fr > MAX_FUNDING_RATE: return False, f"فندنغ مرتفع ({fr*100:.3f}%)"
            if side == "SHORT" and fr < -MAX_FUNDING_RATE: return False, f"فندنغ سلبي ({fr*100:.3f}%)"
    except Exception as e:
        log.warning(f"[FUNDING] {sym}: {e}")
    return True, ""

def fetch_data(ex, sym, tf, limit=100):
    key = (sym, tf)
    ttl = CACHE_TTL.get(tf, 0)
    cached = _data_cache.get(key)
    if cached and ttl and time.time() - cached[0] < ttl:
        return cached[1]
    for _ in range(3):
        try:
            data = ex.fetch_ohlcv(sym, tf, limit=limit)
            _data_cache[key] = (time.time(), data)
            return data
        except Exception as e:
            log.warning(f"[OHLCV] {sym} {tf}: {e}")
            time.sleep(1.5)
    return []

def to_df(raw):
    return add_indicators(pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"]))

# ==========================================
# إدارة الصفقة المفتوحة
# ==========================================
def move_sl(ex, sym, pos_data, new_sl, title):
    side = pos_data["position"]
    close_side = 'sell' if side == "LONG" else 'buy'
    contracts = pos_data["contracts"]

    cancel_pending(ex, sym)
    sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, new_sl, pos_data["tp"])
    if not sl_ok:
        old_ok, _ = protect_position(ex, sym, close_side, contracts, pos_data["sl"], None)
        if not old_ok:
            emergency_close(ex, sym, "فشل تحديث وقف الخسارة وفشل استرجاع القديم.")
        else:
            log.warning(f"[MOVE SL] {sym}: فشل الجديد، تم استرجاع القديم")
        return
    if not tp_ok:
        send(f"⚠️ فشل إعادة وضع TP لـ <code>{sym.split(':')[0]}</code>. راقب الصفقة.")

    with state_lock:
        state[sym]["sl"] = new_sl
        state[sym]["breakeven_hit"] = True
    save_state()
    send(f"{title}\n━━━━━━━━━━━━━━━\n"
         f"🪙 <b>الزوج:</b> <code>{sym.split(':')[0]}</code>\n"
         f"🛑 <b>وقف الخسارة الجديد:</b> <code>{fmt(new_sl)}</code>\n━━━━━━━━━━━━━━━")

def manage_position(ex, sym, pos_data):
    side, entry, sl, tp = pos_data["position"], pos_data["entry"], pos_data["sl"], pos_data["tp"]
    price = get_current_price(ex, sym)
    if not price: return

    try:
        has_sl, has_tp = get_protection(ex, sym)
        if not has_sl:
            close_side = 'sell' if side == "LONG" else 'buy'
            log.error(f"[NO SL] {sym} — إعادة وضع الحماية")
            sl_ok, _ = protect_position(ex, sym, close_side, pos_data["contracts"], sl, tp if not has_tp else None)
            if not sl_ok:
                emergency_close(ex, sym, "الصفقة بدون وقف خسارة وتعذر وضعه.")
                return
            send(f"🛡️ أُعيد وضع وقف الخسارة لـ <code>{sym.split(':')[0]}</code> (كان مفقوداً).")
    except Exception as e:
        log.warning(f"[PROTECTION CHECK] {sym}: {e}")

    if not pos_data.get("breakeven_hit"):
        trigger = entry + (tp - entry) * 0.5
        reached = price >= trigger if side == "LONG" else price <= trigger
        if reached:
            be_sl = entry * (1 + BE_FEE_BUFFER) if side == "LONG" else entry * (1 - BE_FEE_BUFFER)
            move_sl(ex, sym, pos_data, be_sl, "🛡️ <b>تأمين الصفقة (Breakeven)</b>")
        return

    raw = fetch_data(ex, sym, "15m")
    if len(raw) < 50: return
    df = to_df(raw)
    atr_val = df.iloc[-2].get("atr")
    if atr_val is None or pd.isna(atr_val): return

    if side == "LONG":
        new_sl = price - TRAIL_ATR_MULT * float(atr_val)
        if new_sl > sl * 1.002 and new_sl < price:
            move_sl(ex, sym, pos_data, new_sl, "📈 <b>تحريك الوقف المتحرك (Trailing)</b>")
    else:
        new_sl = price + TRAIL_ATR_MULT * float(atr_val)
        if new_sl < sl * 0.998 and new_sl > price:
            move_sl(ex, sym, pos_data, new_sl, "📈 <b>تحريك الوقف المتحرك (Trailing)</b>")

# ==========================================
# معالجة كل زوج
# ==========================================
def entries_allowed(ex):
    if meta.get("paused"): return False
    roll_daily()
    equity = get_equity(ex)
    if equity > 0 and daily_pnl["value"] <= -equity * DAILY_LOSS_LIMIT_PCT / 100.0:
        today = daily_pnl["date"]
        if meta.get("loss_alert") != today:
            meta["loss_alert"] = today
            save_state()
            send(f"⛔ <b>تم الوصول لحد الخسارة اليومي</b>\n"
                 f"الخسارة: {daily_pnl['value']:+.2f} USDT\n"
                 f"تم إيقاف الدخول بصفقات جديدة حتى نهاية اليوم.")
        return False
    return True

def process_symbol(ex, sym):
    try:
        with trade_lock:
            with state_lock:
                if sym not in state:
                    state[sym] = new_sym_state()
                pos_data = dict(state[sym])

            if pos_data.get("position") is not None:
                exch_pos = get_open_position(ex, sym)  
                if exch_pos is None:
                    finalize_trade(ex, sym)
                    return
                manage_position(ex, sym, pos_data)
                return

            if time.time() - pos_data.get("last_exit_ts", 0) < REENTRY_COOLDOWN_SEC:
                return
            if not entries_allowed(ex):
                return

            with state_lock:
                open_syms = [v for v in state.values() if v.get("position") is not None]
            if len(open_syms) >= MAX_OPEN_POSITIONS:
                return

            raw_15m = fetch_data(ex, sym, "15m")
            raw_1h = fetch_data(ex, sym, "1h")
            raw_4h = fetch_data(ex, sym, "4h")
            if len(raw_15m) < 50 or len(raw_1h) < 50 or len(raw_4h) < 50:
                return
            df_15m, df_1h, df_4h = to_df(raw_15m), to_df(raw_1h), to_df(raw_4h)

            signal = check_entry_signal(df_15m, df_1h, df_4h)
            if not signal:
                return

            same_dir = sum(1 for v in open_syms if v["position"] == signal["side"])
            if same_dir >= MAX_SAME_DIRECTION:
                log.info(f"[SKIP] {sym}: وصلنا حد {MAX_SAME_DIRECTION} صفقات {signal['side']}")
                return

            ok, why = market_filters_ok(ex, sym, signal["side"])
            if not ok:
                log.info(f"[SKIP] {sym}: {why}")
                return

            actual_price = get_current_price(ex, sym) or signal["price"]
            if signal["side"] == "LONG":
                sl = actual_price * (1 - signal["sl_pct"]); tp = actual_price * (1 + signal["sl_pct"] * RR_RATIO)
            else:
                sl = actual_price * (1 + signal["sl_pct"]); tp = actual_price * (1 - signal["sl_pct"] * RR_RATIO)

            margin = compute_margin(get_equity(ex), signal["sl_pct"])
            success, info, entry, contracts = open_position(ex, sym, signal["side"], actual_price, sl, tp, margin)

            if success:
                with state_lock:
                    state[sym].update({"position": signal["side"], "entry": entry, "sl": sl, "tp": tp,
                                       "contracts": contracts, "margin": margin,
                                       "opened_ts": time.time(), "breakeven_hit": False})
            else:
                with state_lock:
                    state[sym]["last_exit_ts"] = time.time()
            save_state()

            caption = msg_entry(sym, signal["side"], entry, sl, tp, margin, success)
            if not success:
                caption += f"\n❌ <i>السبب: {info}</i>"
            send(caption)
    except Exception as e:
        log.error(f"[ERROR] {sym}: {e}")

# ==========================================
# مزامنة عند التشغيل
# ==========================================
def sync_positions(ex, active_symbols):
    try:
        exch = {}
        for p in ex.fetch_positions():
            c = abs(float(p.get('contracts') or 0))
            if c > 0 and p.get('symbol') in active_symbols:
                exch[p['symbol']] = p
    except Exception as e:
        log.error(f"[SYNC] {e}")
        send(f"⚠️ فشلت مزامنة المراكز عند التشغيل: {e}")
        return

    with trade_lock:
        for sym in active_symbols:
            with state_lock:
                st = dict(state.get(sym, new_sym_state()))

            if st.get("position") and sym not in exch:
                send(f"ℹ️ صفقة <code>{sym.split(':')[0]}</code> أُغلقت أثناء توقف البوت.")
                finalize_trade(ex, sym)

            elif st.get("position") and sym in exch:
                p = exch[sym]
                close_side = 'sell' if st["position"] == "LONG" else 'buy'
                contracts = abs(float(p.get('contracts')))
                with state_lock:
                    state[sym]["contracts"] = contracts
                try:
                    has_sl, has_tp = get_protection(ex, sym)
                except Exception:
                    has_sl = has_tp = False
                if not has_sl or not has_tp:
                    cancel_pending(ex, sym)
                    sl_ok, _ = protect_position(ex, sym, close_side, contracts, st["sl"], st["tp"])
                    if not sl_ok:
                        emergency_close(ex, sym, "تعذر استرجاع الحماية بعد إعادة التشغيل.")
                    else:
                        send(f"🛡️ أُعيدت حماية <code>{sym.split(':')[0]}</code> بعد إعادة التشغيل.")

            elif not st.get("position") and sym in exch:
                p = exch[sym]
                side = "LONG" if p.get('side') == 'long' else "SHORT"
                entry = float(p.get('entryPrice') or 0)
                contracts = abs(float(p.get('contracts')))
                sl_pct = 0.02
                sl = entry * (1 - sl_pct) if side == "LONG" else entry * (1 + sl_pct)
                tp = entry * (1 + sl_pct * RR_RATIO) if side == "LONG" else entry * (1 - sl_pct * RR_RATIO)
                close_side = 'sell' if side == "LONG" else 'buy'
                cancel_pending(ex, sym)
                sl_ok, _ = protect_position(ex, sym, close_side, contracts, sl, tp)
                if not sl_ok:
                    emergency_close(ex, sym, "مركز غير معروف وتعذر حمايته.")
                    continue
                
                with state_lock:
                    if sym not in state:
                        state[sym] = new_sym_state()
                    state[sym].update({"position": side, "entry": entry, "sl": sl, "tp": tp,
                                       "contracts": contracts,
                                       "margin": float(p.get('initialMargin') or POSITION_SIZE_USDT),
                                       "opened_ts": time.time(), "breakeven_hit": False})
                send(f"♻️ تم تبني مركز موجود <code>{sym.split(':')[0]}</code> ({side}) مع SL/TP افتراضية.")
        save_state()

# ==========================================
# أوامر تيليجرام
# ==========================================
def status_text(ex):
    roll_daily()
    equity = get_equity(ex, force=True)
    lines = [f"📊 <b>حالة البوت</b> {'⏸️ (متوقف مؤقتاً)' if meta.get('paused') else '▶️ (يعمل)'}",
             "━━━━━━━━━━━━━━━",
             f"💼 الرصيد: {equity:,.2f} USDT",
             f"📅 اليوم: {daily_pnl['value']:+.2f} USDT | صفقات: {daily_pnl['trades']} | رابحة: {daily_pnl['wins']}",
             "━━━━━━━━━━━━━━━"]
    with state_lock:
        opens = {k: dict(v) for k, v in state.items() if v.get("position")}
    if not opens:
        lines.append("لا توجد صفقات مفتوحة.")
    for sym, v in opens.items():
        price = get_current_price(ex, sym) or v["entry"]
        d = 1 if v["position"] == "LONG" else -1
        upnl = (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
        lines.append(f"• <code>{sym.split(':')[0]}</code> {v['position']} | دخول {fmt(v['entry'])} | "
                     f"الآن {fmt(price)} | {upnl:+.2f} USDT{' 🛡️' if v.get('breakeven_hit') else ''}")
    return "\n".join(lines)

def find_symbol(arg, active_symbols):
    arg = arg.upper().replace("/USDT", "").replace("USDT", "")
    for s in active_symbols:
        if s.startswith(arg + "/"): return s
    return None

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
                has_pos = bool(state.get(sym, {}).get("position"))
            if not has_pos:
                send("لا توجد صفقة مفتوحة على هذه العملة.", chat_id); return
            if close_position(ex, sym):
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
                if close_position(ex, sym):
                    finalize_trade(ex, sym, reason_hint="Manual")
                else:
                    send(f"❌ فشل إغلاق <code>{sym.split(':')[0]}</code>", chat_id)
    elif cmd == "/help":
        send("/status — الحالة\n/pause — إيقاف الدخول\n/resume — استئناف\n"
             "/close BTC — إغلاق عملة\n/closeall — إغلاق الكل", chat_id)

def telegram_listener(active_symbols):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS:
        return
    try:
        ex2 = create_exchange()
        ex2.load_markets()
    except Exception as e:
        log.error(f"[TG LISTENER INIT] {e}")
        return
    offset = None
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                             params={"timeout": 25, "offset": offset}, timeout=35).json()
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                msg = upd.get("message") or {}
                text, chat_id = msg.get("text", ""), str(msg.get("chat", {}).get("id", ""))
                if chat_id not in AUTHORIZED_CHATS or not text.startswith("/"):
                    continue
                try:
                    handle_command(ex2, text, chat_id, active_symbols)
                except Exception as e:
                    log.error(f"[TG CMD] {e}")
                    send(f"❌ خطأ: {e}", chat_id)
        except Exception as e:
            log.warning(f"[TG POLL] {e}")
            time.sleep(5)

# ==========================================
# التقرير اليومي (تم تحسينه ليعمل في أي وقت بعد 11:55)
# ==========================================
def maybe_send_daily_report(ex):
    now = datetime.now(KSA_TZ)
    today = str(now.date())
    
    # التحقق: هل نحن في وقت التقرير (من 11:55 مساءً حتى 12:15 صباحاً كهامش أمان)؟
    is_report_time = (now.hour == 23 and now.minute >= 55) or (now.hour == 0 and now.minute < 15)
    
    if is_report_time and meta.get("last_report") != today:
        roll_daily()
        wr = (daily_pnl["wins"] / daily_pnl["trades"] * 100) if daily_pnl["trades"] else 0
        send(f"📋 <b>التقرير اليومي</b> — {today}\n━━━━━━━━━━━━━━━\n"
             f"💰 صافي الربح/الخسارة: {daily_pnl['value']:+.2f} USDT\n"
             f"🔢 عدد الصفقات: {daily_pnl['trades']}\n"
             f"✅ الرابحة: {daily_pnl['wins']} ({wr:.0f}%)\n"
             f"💼 الرصيد: {get_equity(ex, force=True):,.2f} USDT\n━━━━━━━━━━━━━━━")
        meta["last_report"] = today
        save_state()
        log.info("[DAILY REPORT] Sent successfully.")

# ==========================================
# نبضة الحياة (Heartbeat) لطمأنتك
# ==========================================
def maybe_send_heartbeat(ex):
    now = datetime.now(KSA_TZ)
    current_hour = now.strftime("%Y-%m-%d %H")
    if meta.get("last_heartbeat") != current_hour and now.minute < 5:
        meta["last_heartbeat"] = current_hour
        save_state()
        equity = get_equity(ex, force=True)
        with state_lock:
            open_ct = sum(1 for v in state.values() if v.get("position"))
        log.info(f"[HEARTBEAT] Bot is alive. Open positions: {open_ct}. Equity: {equity}")
        # إذا أردت رسالة في تيليجرام كل ساعة، أزل علامة المربع من السطر التالي
        # send(f"⏱️ <b>نبضة حياة:</b> البوت يعمل. صفقات مفتوحة: {open_ct} | رصيد: {equity:.2f} USDT")

# ==========================================
# الحلقة الرئيسية
# ==========================================
def init_exchange():
    ex = create_exchange()
    ex.load_markets()
    return ex

def trading_loop():
    log.info("[INIT] Starting V35 Bot (Robust Reports)...")
    if not OKX_API_KEY:
        return send("❌ مفاتيح API غير موجودة")

    try:
        ex = init_exchange()
        load_state()
        balance = get_equity(ex, force=True)
        mode_str = "تجريبي (Sandbox)" if USE_SANDBOX else "حقيقي (Production)"
        log.info(f"[OKX] Connected ({mode_str}). Equity: {balance}")
    except Exception as e:
        return send(f"❌ فشل الاتصال بالمنصة: {e}")

    active_symbols = [s for s in FUTURES_SYMBOLS if s in ex.markets]
    sync_positions(ex, active_symbols)

    threading.Thread(target=telegram_listener, args=(active_symbols,), daemon=True).start()

    send(f"🛡️ <b>تم تشغيل البوت (V35 - تقارير مضمونة)</b>\n"
         f"━━━━━━━━━━━━━━━\n"
         f"⚙️ <b>الوضع:</b> {mode_str}\n"
         f"🪙 <b>العملات:</b> {len(active_symbols)}\n"
         f"📊 <b>الحد الأقصى للصفقات:</b> {MAX_OPEN_POSITIONS} (حد {MAX_SAME_DIRECTION} بنفس الاتجاه)\n"
         f"💰 <b>حجم الصفقة:</b> {POSITION_SIZE_USDT} USDT (ثابت)\n"
         f"🎯 <b>SL/TP:</b> ATR (نسبة 1:{RR_RATIO:g}) + Trailing\n"
         f"⛔ <b>حد الخسارة اليومي:</b> {DAILY_LOSS_LIMIT_PCT}%\n"
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
                time.sleep(0.5)
            
            maybe_send_daily_report(ex)
            maybe_send_heartbeat(ex)
            
        except Exception as e:
            cycle_errors += 1
            log.error(f"[LOOP FATAL] {e}")

        fail_streak = fail_streak + 1 if cycle_errors >= 5 else 0
        if fail_streak >= 3:
            log.warning("[RECONNECT] إعادة إنشاء الاتصال بالمنصة")
            try:
                ex = init_exchange()
                fail_streak = 0
            except Exception as e:
                log.error(f"[RECONNECT FAIL] {e}")

        elapsed = time.time() - start
        time.sleep(max(10, SCAN_INTERVAL - elapsed))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
