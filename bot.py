# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V44 (Profiles + Tunable Filters)

الجديد عن V43:
- بروفايلات جاهزة بمتغير واحد PROFILE: conservative | balanced | active
  (الافتراضي: balanced على Sandbox، و conservative على الحساب الحقيقي).
- كل حد في الاستراتيجية قابل للتعديل من متغيرات البيئة في Render بدون تغيير الكود
  (MIN_ATR_PCT, MIN_ADX, MIN_VOL_MULT, REGIME_ADX4_STRONG, REGIME_ADX4_WEAK, REGIME_ADX1_STRONG,
   RSI_LONG_MIN/MAX, RSI_SHORT_MIN/MAX, NEAR_EMA21_PCT, NEAR_EMA50_PCT, MAX_SPREAD_PCT).
  المتغير المنفرد يتقدم على البروفايل.
- حماية: البروفايل active ممنوع على الحساب الحقيقي (يتحول تلقائياً إلى balanced مع تنبيه).
- تسجيل قيمة السبريد عند الحجب (log) وعرضها في /why BTC.
- أمر /config لعرض الإعدادات الفعلية الحالية.
"""
import os
import sys
import csv
import json
import time
import logging
import threading
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxt
import requests
from flask import Flask

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

# ==========================================
# الإعدادات
# ==========================================
VERSION = "V44"
KSA_TZ = timezone(timedelta(hours=3))


def env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return float(default)


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
    "FET/USDT:USDT", "WLD/USDT:USDT", "PEPE/USDT:USDT", "WIF/USDT:USDT", "ORDI/USDT:USDT",
]

SECTORS = {
    "major": {"BTC", "ETH", "BNB"},
    "l1": {"SOL", "AVAX", "NEAR", "APT", "SUI", "ADA", "DOT", "ATOM", "SEI", "TIA", "INJ"},
    "l2": {"ARB", "OP", "STX", "IMX"},
    "meme": {"DOGE", "PEPE", "WIF"},
    "ai": {"FET", "WLD"},
    "legacy": {"XRP", "LTC", "BCH", "FIL", "LINK", "RUNE", "ORDI"},
}
MAX_PER_SECTOR_SAME_DIR = 2

SCAN_INTERVAL = 30
LEVERAGE = int(env_float("LEVERAGE", 5))
MARGIN_MODE = "isolated"
MAX_OPEN_POSITIONS = 5
REENTRY_COOLDOWN_SEC = 1500

USE_RISK_SIZING = True
RISK_PCT_PER_TRADE = env_float("RISK_PCT_PER_TRADE", 0.75)
POSITION_SIZE_USDT = 50
MIN_MARGIN_USDT = 12
MAX_MARGIN_USDT = env_float("MAX_MARGIN_USDT", 130)
MAX_MARGIN_PCT_EQUITY = 0.30

ATR_SL_MULT = 1.55
RR_RATIO = 2.3
MIN_SL_PCT = 0.0065
MAX_SL_PCT = 0.032
TRAIL_ATR_MULT = 1.05
BE_FEE_BUFFER = 0.0008
BE_TRIGGER_R = 0.9
FALLBACK_SL_PCT = 0.017

PARTIAL1_R, PARTIAL1_PCT = 1.3, 0.40
PARTIAL2_R, PARTIAL2_PCT = 2.1, 0.50

FEE_RATE = 0.0005
DAILY_LOSS_LIMIT_PCT = env_float("DAILY_LOSS_LIMIT_PCT", 4.0)
DAILY_PROFIT_TARGET_PCT = env_float("DAILY_PROFIT_TARGET_PCT", 6.0)
INCLUDE_FLOATING_IN_LOSS_LIMIT = True
MAX_SAME_DIRECTION = 3
MAX_FUNDING_RATE = 0.00045
MAX_ENTRY_SLIPPAGE_PCT = 0.004
USE_BTC_FILTER = True

# ---------- بروفايلات الاستراتيجية ----------
PROFILES = {
    # الإعدادات الأصلية (الأكثر تشدداً)
    "conservative": {
        "min_atr_pct": 0.004, "min_adx": 23, "min_vol_mult": 1.2,
        "regime_adx4_strong": 28, "regime_adx1_strong": 24, "regime_adx4_weak": 20,
        "rsi_long_min": 40, "rsi_long_max": 61, "rsi_short_min": 39, "rsi_short_max": 60,
        "near_ema21_pct": 0.011, "near_ema50_pct": 0.016, "max_spread_pct": 0.0011,
    },
    # وسط: فرص أكثر مع بقاء منطق الاتجاه
    "balanced": {
        "min_atr_pct": 0.003, "min_adx": 20, "min_vol_mult": 1.0,
        "regime_adx4_strong": 26, "regime_adx1_strong": 22, "regime_adx4_weak": 18,
        "rsi_long_min": 38, "rsi_long_max": 64, "rsi_short_min": 36, "rsi_short_max": 62,
        "near_ema21_pct": 0.013, "near_ema50_pct": 0.019, "max_spread_pct": 0.0015,
    },
    # لاختبار آلية التنفيذ على Sandbox فقط
    "active": {
        "min_atr_pct": 0.0022, "min_adx": 18, "min_vol_mult": 0.8,
        "regime_adx4_strong": 24, "regime_adx1_strong": 20, "regime_adx4_weak": 15,
        "rsi_long_min": 35, "rsi_long_max": 67, "rsi_short_min": 33, "rsi_short_max": 65,
        "near_ema21_pct": 0.016, "near_ema50_pct": 0.024, "max_spread_pct": 0.003,
    },
}
_default_profile = "balanced" if USE_SANDBOX else "conservative"
PROFILE_NAME = os.getenv("PROFILE", _default_profile).strip().lower()
PROFILE_WARNING = ""
if PROFILE_NAME not in PROFILES:
    PROFILE_WARNING = f"بروفايل غير معروف '{PROFILE_NAME}' — استُخدم {_default_profile}"
    PROFILE_NAME = _default_profile
if not USE_SANDBOX and PROFILE_NAME == "active":
    PROFILE_WARNING = "البروفايل active ممنوع على الحساب الحقيقي — استُخدم balanced"
    PROFILE_NAME = "balanced"


def _cfg(key):
    """متغير البيئة (باسم المفتاح بحروف كبيرة) يتقدم على قيمة البروفايل."""
    return env_float(key.upper(), PROFILES[PROFILE_NAME][key])


MIN_ATR_PCT = _cfg("min_atr_pct")
MAX_ATR_PCT = 0.045
MIN_ADX = _cfg("min_adx")
MIN_VOL_MULT = _cfg("min_vol_mult")
REGIME_ADX4_STRONG = _cfg("regime_adx4_strong")
REGIME_ADX1_STRONG = _cfg("regime_adx1_strong")
REGIME_ADX4_WEAK = _cfg("regime_adx4_weak")
RSI_LONG_MIN = _cfg("rsi_long_min")
RSI_LONG_MAX = _cfg("rsi_long_max")
RSI_SHORT_MIN = _cfg("rsi_short_min")
RSI_SHORT_MAX = _cfg("rsi_short_max")
NEAR_EMA21_PCT = _cfg("near_ema21_pct")
NEAR_EMA50_PCT = _cfg("near_ema50_pct")
MAX_SPREAD_PCT = _cfg("max_spread_pct")

WATCHDOG_SEC = 300

TF_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")
JOURNAL_FILE = os.path.join(DATA_DIR, "trades_journal.csv")
LOCK_FILE = os.path.join(DATA_DIR, "bot.lock")

state_lock = threading.RLock()
trade_lock = threading.RLock()
journal_lock = threading.Lock()
diag_lock = threading.Lock()

state = {}
meta = {"paused": False, "last_report": "", "loss_alert": "", "profit_alert": "", "last_heartbeat": ""}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}

_data_cache = {}
_price_cache = {}
_equity_cache = {"ts": 0, "value": 0.0}
_btc_bias_cache = {"ts": 0, "value": "neutral"}
_last_eval = {}
_last_spread = {}      # sym -> (ts, spread_fraction)
_warned_external = set()
_lock_fh = None

# ---------- التشخيص ----------
diag = {"date": "", "evaluated": 0, "signals": 0, "opened": 0, "reasons": {}, "blocks": {}}
_last_reason = {}      # sym -> (ts, reason)
loop_info = {"last_cycle_ts": 0.0, "cycles": 0, "last_duration": 0.0, "last_error": "", "alerted": False}

REASON_LABELS = {
    "regime_range": "سوق عرضي (Range)",
    "insufficient_data": "بيانات غير كافية",
    "indicators_nan": "مؤشرات غير جاهزة",
    "atr_low": "تذبذب منخفض (ATR)",
    "atr_high": "تذبذب مرتفع جداً",
    "low_adx": "قوة اتجاه ضعيفة (ADX)",
    "low_volume": "حجم تداول ضعيف",
    "direction_mismatch": "اتجاه 4س غير متوافق",
    "ema_misaligned": "EMA غير متوافقة (15د/1س)",
    "far_from_ema": "السعر بعيد عن EMA",
    "rsi_out_of_range": "RSI خارج النطاق",
    "no_pullback_candle": "لا توجد شمعة ارتداد",
    "max_same_direction": "حد نفس الاتجاه",
    "sector_limit": "حد القطاع",
    "btc_filter": "فلتر اتجاه BTC",
    "spread_high": "سبريد عالي",
    "funding_high": "فندنغ مرتفع (LONG)",
    "funding_negative": "فندنغ سلبي (SHORT)",
    "ticker_unavailable": "تعذر قراءة السعر/السبريد",
    "slippage": "ابتعاد السعر عن الإشارة",
    "margin_too_small": "الهامش أقل من الحد الأدنى",
    "open_failed": "فشل فتح الصفقة",
    "cooldown": "فترة الانتظار بعد الإغلاق",
    "paused": "البوت موقوف (/pause)",
    "equity_unavailable": "تعذر قراءة الرصيد",
    "daily_loss_limit": "حد الخسارة اليومي",
    "daily_profit_target": "هدف الربح اليومي",
    "max_positions": "الحد الأقصى للصفقات",
    "no_data": "لا توجد بيانات شموع",
    "signal": "إشارة ✅",
}


def label(r):
    return REASON_LABELS.get(r, r)


def _diag_roll_locked():
    today = str(datetime.now(KSA_TZ).date())
    if diag["date"] != today:
        diag.update({"date": today, "evaluated": 0, "signals": 0, "opened": 0, "reasons": {}, "blocks": {}})


def note_eval(sym, reason):
    """reason=None تعني أن الإشارة وُجدت."""
    with diag_lock:
        _diag_roll_locked()
        diag["evaluated"] += 1
        if reason is None:
            diag["signals"] += 1
        else:
            diag["reasons"][reason] = diag["reasons"].get(reason, 0) + 1
        _last_reason[sym] = (time.time(), reason or "signal")


def note_block(sym, reason):
    with diag_lock:
        _diag_roll_locked()
        diag["blocks"][reason] = diag["blocks"].get(reason, 0) + 1
        _last_reason[sym] = (time.time(), reason)


def note_opened():
    with diag_lock:
        _diag_roll_locked()
        diag["opened"] += 1


def set_last_reason(sym, reason):
    with diag_lock:
        _last_reason[sym] = (time.time(), reason)


# ==========================================
# Flask
# ==========================================
@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": f"trading_bot_{VERSION.lower()}"}, 200


@app.route("/diag")
def diag_endpoint():
    with diag_lock:
        d = json.loads(json.dumps(diag))
    last = loop_info["last_cycle_ts"]
    return {
        "version": VERSION,
        "profile": PROFILE_NAME,
        "paused": bool(meta.get("paused")),
        "loop_age_sec": round(time.time() - last, 1) if last else None,
        "cycles": loop_info["cycles"],
        "last_cycle_duration": round(loop_info["last_duration"], 1),
        "last_error": loop_info["last_error"],
        "today": d,
    }, 200


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)


def acquire_single_instance():
    global _lock_fh
    try:
        import fcntl
        _lock_fh = open(LOCK_FILE, "w")
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except ImportError:
        return True
    except OSError:
        return False


# ==========================================
# إدارة الحالة
# ==========================================
def new_sym_state():
    return {
        "position": None, "entry": None, "sl": None, "tp": None,
        "initial_sl": None, "initial_risk": 0.0,
        "contracts": 0.0, "initial_contracts": 0.0, "margin": 0.0,
        "opened_ts": 0, "leg_ts": 0, "last_exit_ts": 0,
        "realized_pnl": 0.0, "breakeven_hit": False,
        "partial_1_taken": False, "partial_2_taken": False,
        "last_notified_sl": None, "regime": "unknown",
    }


def load_state():
    global state, daily_pnl
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        loaded = {}
        for k, v in data.get("state", {}).items():
            base = new_sym_state()
            base.update(v)
            loaded[k.split("|")[0]] = base
        with state_lock:
            state = loaded
            daily_pnl = data.get("daily_pnl", daily_pnl)
            meta.update(data.get("meta", {}))
    except Exception as e:
        log.error(f"[STATE LOAD ERROR] {e}")


def save_state():
    try:
        with state_lock:
            txt = json.dumps({"state": state, "daily_pnl": daily_pnl, "meta": meta},
                             ensure_ascii=False, indent=2)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(txt)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log.error(f"[STATE SAVE ERROR] {e}")


def set_meta(key, value):
    with state_lock:
        meta[key] = value
    save_state()


def roll_daily():
    global daily_pnl
    today = str(datetime.now(KSA_TZ).date())
    with state_lock:
        if daily_pnl.get("date") != today:
            daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": today}


def add_daily_pnl(pnl_usdt):
    roll_daily()
    with state_lock:
        daily_pnl["value"] += pnl_usdt
    save_state()


def register_trade_result(total_pnl):
    roll_daily()
    with state_lock:
        daily_pnl["trades"] += 1
        if total_pnl > 0:
            daily_pnl["wins"] += 1
    save_state()


def reset_sym_state(sym, cooldown=True):
    with state_lock:
        s = new_sym_state()
        s["last_exit_ts"] = time.time() if cooldown else 0
        state[sym] = s
    save_state()


def daily_snapshot():
    roll_daily()
    with state_lock:
        return dict(daily_pnl)


# ==========================================
# سجل الصفقات
# ==========================================
JOURNAL_FIELDS = ["close_time", "symbol", "side", "entry", "exit", "initial_contracts",
                  "pnl", "r_multiple", "reason", "regime", "duration_min"]


def journal_append(row):
    try:
        with journal_lock:
            new = not os.path.exists(JOURNAL_FILE)
            with open(JOURNAL_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_FIELDS)
                if new:
                    w.writeheader()
                w.writerow(row)
    except Exception as e:
        log.error(f"[JOURNAL] {e}")


def stats_text():
    try:
        with journal_lock:
            if not os.path.exists(JOURNAL_FILE):
                return "📭 لا يوجد سجل صفقات بعد."
            df = pd.read_csv(JOURNAL_FILE)
    except Exception as e:
        return f"خطأ في قراءة السجل: {e}"
    if df.empty:
        return "📭 لا يوجد سجل صفقات بعد."
    n = len(df)
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    gross_win = wins["pnl"].sum()
    gross_loss = abs(losses["pnl"].sum())
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    curve = df["pnl"].cumsum()
    max_dd = (curve.cummax() - curve).max()
    avg_r = df["r_multiple"].mean() if "r_multiple" in df else 0.0
    wr = len(wins) / n * 100
    avg_win = wins["pnl"].mean() if len(wins) else 0.0
    avg_loss = losses["pnl"].mean() if len(losses) else 0.0
    pf_txt = "∞" if pf == float("inf") else f"{pf:.2f}"
    return (f"📈 <b>إحصائيات الأداء الكلية</b>\n━━━━━━━━━━━━━━━\n"
            f"🔢 عدد الصفقات: {n}\n"
            f"✅ نسبة الربح: {wr:.1f}%\n"
            f"💰 الصافي: <b>{df['pnl'].sum():+.2f} USDT</b>\n"
            f"⚖️ Profit Factor: {pf_txt}\n"
            f"📐 متوسط R: {avg_r:+.2f}\n"
            f"🟢 متوسط الربح: {avg_win:+.2f} | 🔴 متوسط الخسارة: {avg_loss:+.2f}\n"
            f"📉 أقصى تراجع: {max_dd:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━")


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


def short(sym):
    return sym.split(":")[0]


REASONS = {
    "Take Profit": "تحقق الهدف 🎯",
    "Stop Loss": "ضرب وقف الخسارة 🛑",
    "Breakeven Hit": "خروج آمن (Breakeven) 🛡️",
    "Trailing Stop": "خروج بوقف متحرك 📈",
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
            f"🪙 <b>الزوج:</b> <code>{short(sym)}</code>\n"
            f"💵 <b>سعر الدخول:</b> <code>{fmt(price)}</code>\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x | <b>الهامش:</b> {margin:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 <b>وقف الخسارة:</b> <code>{fmt(sl)}</code> ({sl_pct:.2f}%)\n"
            f"📉 <i>المخاطرة: {risk_usd:.2f} USDT</i>\n"
            f"🎯 <b>الهدف:</b> <code>{fmt(tp)}</code> ({tp_pct:.2f}%)\n"
            f"📈 <i>العائد المتوقع: +{reward_usd:.2f} USDT</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ {now_str}")


def msg_exit(sym, side, exit_price, entry_price, total_pnl, final_leg_pnl, margin, r_mult, reason):
    pct = (total_pnl / margin * 100) if margin else 0.0
    if total_pnl > 0:
        title = "🟢 <b>إغلاق بربح</b>"
    elif total_pnl < 0:
        title = "🔴 <b>إغلاق بخسارة</b>"
    else:
        title = "⚪ <b>إغلاق متعادل</b>"
    return (f"{title}\n━━━━━━━━━━━━━━━\n"
            f"🪙 <code>{short(sym)}</code> ({side})\n"
            f"📥 الدخول: <code>{fmt(entry_price)}</code>\n"
            f"📤 الخروج: <code>{fmt(exit_price)}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 على الهامش: {pct:+.2f}% | R: {r_mult:+.2f}\n"
            f"💰 الصافي الكلي: <b>{total_pnl:+.2f} USDT</b>\n"
            f"🧩 الجزء الأخير: {final_leg_pnl:+.2f} USDT\n"
            f"📝 السبب: {REASONS.get(reason, reason)}\n"
            f"━━━━━━━━━━━━━━━")


# ==========================================
# المؤشرات
# ==========================================
def ema(s, p):
    return s.ewm(span=p, adjust=False).mean()


def rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1 / p, adjust=False).mean()
    l = -d.clip(upper=0).ewm(alpha=1 / p, adjust=False).mean()
    return 100 - (100 / (1 + g / (l + 1e-9)))


def true_range(high, low, close):
    return pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
                     axis=1).max(axis=1)


def atr(high, low, close, p=14):
    return true_range(high, low, close).ewm(alpha=1 / p, adjust=False).mean()


def adx(high, low, close, p=14):
    up, down = high.diff(), -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr_val = atr(high, low, close, p)
    plus_di = 100 * (plus_dm.ewm(alpha=1 / p, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / p, adjust=False).mean() / (atr_val + 1e-9))
    dx = abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9) * 100
    return dx.ewm(alpha=1 / p, adjust=False).mean()


def add_indicators(df):
    if len(df) < 60:
        return df
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df["high"], df["low"], df["close"])
    df["atr"] = atr(df["high"], df["low"], df["close"])
    df["vol_ma"] = df["volume"].rolling(20).mean()
    return df


# ==========================================
# المنصة (OKX)
# ==========================================
def create_exchange():
    ex = ccxt.okx({
        'apiKey': OKX_API_KEY, 'secret': OKX_SECRET, 'password': OKX_PASSPHRASE,
        'enableRateLimit': True, 'options': {'defaultType': 'swap'}, 'timeout': 20000,
    })
    if USE_SANDBOX:
        ex.set_sandbox_mode(True)
    return ex


def init_exchange():
    ex = create_exchange()
    ex.load_markets()
    return ex


def refresh_prices(ex, symbols):
    try:
        tickers = ex.fetch_tickers(symbols)
        now = time.time()
        for s, t in tickers.items():
            if t.get('last'):
                _price_cache[s] = (now, float(t['last']))
    except Exception as e:
        log.warning(f"[PRICES] {e}")


def get_current_price(ex, sym, max_age=20):
    c = _price_cache.get(sym)
    if c and time.time() - c[0] <= max_age:
        return c[1]
    try:
        p = float(ex.fetch_ticker(sym)['last'])
        _price_cache[sym] = (time.time(), p)
        return p
    except Exception as e:
        log.warning(f"[PRICE] {sym}: {e}")
        return c[1] if c and max_age > 0 and time.time() - c[0] <= max_age else None


def get_equity(ex, force=False):
    """يرجع 0.0 عند تعذر القراءة (لا نستخدم رصيداً قديماً لقرارات جديدة)."""
    if not force and time.time() - _equity_cache["ts"] < 60 and _equity_cache["value"] > 0:
        return _equity_cache["value"]
    try:
        bal = ex.fetch_balance().get('USDT', {})
        val = float(bal.get('total') or bal.get('free') or 0)
        if val > 0:
            _equity_cache.update({"ts": time.time(), "value": val})
            return val
        return 0.0
    except Exception as e:
        log.warning(f"[EQUITY] {e}")
        return 0.0


def contract_size(ex, sym):
    try:
        return float(ex.market(sym).get('contractSize') or 1)
    except Exception:
        return 1.0


def min_amount(ex, sym):
    try:
        return float(ex.market(sym).get('limits', {}).get('amount', {}).get('min') or 1.0)
    except Exception:
        return 1.0


def calculate_contracts(ex, sym, margin, price, lev):
    try:
        market = ex.market(sym)
        raw = (margin * lev) / (price * float(market.get('contractSize') or 1))
        amt = float(ex.amount_to_precision(sym, raw))
        return amt if amt >= min_amount(ex, sym) else 0.0
    except Exception as e:
        log.warning(f"[AMOUNT] {sym}: {e}")
        return 0.0


def compute_margin(equity, sl_pct):
    """يرجع 0.0 إذا تعذر تحديد حجم آمن (الرصيد مجهول أو الهامش أقل من الحد الأدنى)."""
    if equity <= 0:
        return 0.0
    if not USE_RISK_SIZING:
        return float(POSITION_SIZE_USDT)
    risk_usd = equity * RISK_PCT_PER_TRADE / 100.0
    margin = risk_usd / (sl_pct * LEVERAGE)
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    sized = min(cap, margin)
    return sized if sized >= MIN_MARGIN_USDT else 0.0


def get_open_position(ex, sym):
    """None فقط إذا لا يوجد مركز. أي خطأ API يرفع Exception."""
    for p in ex.fetch_positions([sym]):
        contracts = abs(float(p.get('contracts') or 0))
        if contracts > 0:
            return {"side": "LONG" if p.get('side') == 'long' else "SHORT",
                    "contracts": contracts,
                    "entry": float(p.get('entryPrice') or 0),
                    "margin": float(p.get('initialMargin') or 0)}
    return None


# ---------- أوامر الحماية ----------
def _valid_trigger(value):
    try:
        return float(value or 0) > 0
    except (TypeError, ValueError):
        return False


def get_algos(ex, sym):
    out = []
    market = ex.market(sym)
    algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
    for o in (algo.get('data') or []):
        out.append({"id": o.get('algoId'),
                    "is_sl": _valid_trigger(o.get('slTriggerPx')),
                    "is_tp": _valid_trigger(o.get('tpTriggerPx'))})
    return out


def has_protection(ex, sym):
    """(has_sl, has_tp) أو (None, None) إذا تعذر الاستعلام."""
    try:
        algos = get_algos(ex, sym)
        return any(a["is_sl"] for a in algos), any(a["is_tp"] for a in algos)
    except Exception as e:
        log.warning(f"[PROTECTION UNKNOWN] {sym}: {e}")
        return None, None


def cancel_algo_ids(ex, sym, ids):
    market = ex.market(sym)
    for aid in ids:
        try:
            ex.privatePostTradeCancelAlgos([{'instId': market['id'], 'algoId': aid}])
        except Exception as e:
            log.warning(f"[CANCEL ALGO] {sym} {aid}: {e}")


def cancel_pending(ex, sym):
    try:
        cancel_algo_ids(ex, sym, [a["id"] for a in get_algos(ex, sym)])
        for o in ex.fetch_open_orders(sym):
            try:
                ex.cancel_order(o['id'], sym)
            except Exception:
                pass
    except Exception as e:
        log.warning(f"[CANCEL PENDING] {sym}: {e}")


def place_sl_tp(ex, sym, side, contracts, trigger, is_sl):
    trig_str = str(ex.price_to_precision(sym, trigger))
    amt_str = str(ex.amount_to_precision(sym, contracts))
    market = ex.market(sym)
    req = {'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': side, 'posSide': 'net',
           'ordType': 'conditional', 'sz': amt_str, 'reduceOnly': 'true'}
    if is_sl:
        req.update({'slTriggerPx': trig_str, 'slOrdPx': '-1'})
    else:
        req.update({'tpTriggerPx': trig_str, 'tpOrdPx': '-1'})
    res = ex.privatePostTradeOrderAlgo(req)
    data = (res.get('data') or [{}])[0]
    if str(data.get('sCode', '0')) != '0':
        raise Exception(f"algo rejected: {data.get('sCode')} {data.get('sMsg')}")
    return res


def protect_position(ex, sym, close_side, contracts, sl, tp, retries=3):
    """
    وقف الخسارة: أي رفض = فشل (لا نعتبر الصفقة محمية). الهدف: رفض بسبب تجاوز السعر لا يُعد فشلاً حرجاً.
    """
    sl_ok = tp_ok = False
    if sl is not None:
        for i in range(retries):
            try:
                place_sl_tp(ex, sym, close_side, contracts, sl, True)
                sl_ok = True
                break
            except Exception as e:
                log.error(f"[SL ERR] {sym} try {i + 1}: {e}")
                time.sleep(0.8)
    if tp is not None and contracts > 0:
        for i in range(retries):
            try:
                place_sl_tp(ex, sym, close_side, contracts, tp, False)
                tp_ok = True
                break
            except Exception as e:
                if "51277" in str(e) or "51276" in str(e):
                    log.warning(f"[TP SKIP] {sym}: target already passed ({e})")
                    tp_ok = True
                    break
                log.error(f"[TP ERR] {sym} try {i + 1}: {e}")
                time.sleep(0.8)
    return sl_ok, tp_ok


def replace_protection(ex, sym, close_side, contracts, sl, tp=None):
    """نضع الجديد أولاً ثم نلغي القديم. فشل الوقف الجديد = تبقى القديمة."""
    try:
        old = get_algos(ex, sym)
    except Exception as e:
        log.warning(f"[REPLACE] list failed {sym}: {e}")
        return False
    sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, sl, tp)
    if not sl_ok:
        return False
    ids = [a["id"] for a in old if a["is_sl"] or (tp is not None and tp_ok and a["is_tp"])]
    cancel_algo_ids(ex, sym, ids)
    return True


def close_full(ex, sym):
    try:
        pos = get_open_position(ex, sym)
        if pos is None:
            cancel_pending(ex, sym)
            return True
        close_side = 'sell' if pos["side"] == "LONG" else 'buy'
        ex.create_order(sym, 'market', close_side, pos["contracts"], None,
                        {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        time.sleep(1.0)
        if get_open_position(ex, sym) is not None:
            return False
        cancel_pending(ex, sym)
        return True
    except Exception as e:
        log.error(f"[CLOSE FULL ERR] {sym}: {e}")
        return False


def set_leverage_checked(ex, sym):
    last_err = None
    for _ in range(2):
        try:
            ex.set_leverage(LEVERAGE, sym, params={'mgnMode': MARGIN_MODE})
            return True, ""
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    return False, str(last_err)


def open_position(ex, sym, side, price, sl, tp, margin):
    try:
        amount = calculate_contracts(ex, sym, margin, price, LEVERAGE)
        if amount <= 0:
            return False, "كمية غير صالحة", price, 0.0
        cancel_pending(ex, sym)
        ok, err = set_leverage_checked(ex, sym)
        if not ok:
            log.error(f"[LEVERAGE] {sym}: {err}")
            return False, f"تعذر ضبط الرافعة: {err[:120]}", price, 0.0
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'
        order = ex.create_order(sym, 'market', order_side, amount, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net'})
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
            send(f"🚨 فشل وضع وقف الخسارة لـ <code>{short(sym)}</code> — جاري الإغلاق.")
            if not close_full(ex, sym):
                send(f"🆘 فشل إغلاق <code>{short(sym)}</code> بعد فشل الوقف! تدخل يدوي فوراً.")
            return False, "فشل وضع SL", entry, 0.0
        if not tp_ok:
            send(f"⚠️ تم وضع SL لكن فشل TP لـ <code>{short(sym)}</code>")
        return True, order.get('id'), entry, contracts
    except Exception as e:
        log.error(f"[OPEN ERR] {sym}: {e}")
        return False, str(e)[:150], price, 0.0


# ==========================================
# الربح وإنهاء الصفقة
# ==========================================
def leg_pnl(side, entry, exit_price, contracts, cs):
    d = 1 if side == "LONG" else -1
    gross = (exit_price - entry) * d * contracts * cs
    fees = (entry + exit_price) * contracts * cs * FEE_RATE
    return gross - fees


def get_exit_price(ex, sym, pos_data):
    try:
        close_side = 'sell' if pos_data["position"] == "LONG" else 'buy'
        since = int(max(pos_data.get("leg_ts") or 0, pos_data.get("opened_ts") or 0) * 1000) - 8000
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
    if tp and abs(exit_price - tp) / max(tp, 1e-9) < 0.0035:
        return "Take Profit"
    if sl and abs(exit_price - sl) / max(sl, 1e-9) < 0.0035:
        if pos_data.get("breakeven_hit"):
            profitable = (sl > entry * 1.0015) if side == "LONG" else (sl < entry * 0.9985)
            return "Trailing Stop" if profitable else "Breakeven Hit"
        return "Stop Loss"
    return "Manual/External"


def finalize_trade(ex, sym, reason_hint=None):
    with state_lock:
        pos_data = dict(state.get(sym, {}))
    if not pos_data.get("position"):
        return
    side, entry = pos_data["position"], pos_data["entry"]
    exit_price = get_exit_price(ex, sym, pos_data) or get_current_price(ex, sym, 0) or entry
    cs = contract_size(ex, sym)
    final_leg = leg_pnl(side, entry, exit_price, pos_data.get("contracts", 0.0), cs)
    total = pos_data.get("realized_pnl", 0.0) + final_leg
    margin = pos_data.get("margin") or POSITION_SIZE_USDT
    reason = reason_hint or detect_reason(pos_data, exit_price)
    risk_usd = pos_data.get("initial_risk", 0.0) * pos_data.get("initial_contracts", 0.0) * cs
    r_mult = (total / risk_usd) if risk_usd > 0 else 0.0
    duration = int((time.time() - (pos_data.get("opened_ts") or time.time())) / 60)

    add_daily_pnl(final_leg)
    register_trade_result(total)
    journal_append({
        "close_time": datetime.now(KSA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": short(sym), "side": side, "entry": entry, "exit": exit_price,
        "initial_contracts": pos_data.get("initial_contracts", 0.0),
        "pnl": round(total, 4), "r_multiple": round(r_mult, 3), "reason": reason,
        "regime": pos_data.get("regime", ""), "duration_min": duration})
    reset_sym_state(sym, cooldown=True)
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, total, final_leg, margin, r_mult, reason))


def emergency_close(ex, sym, why):
    send(f"🚨 <b>إغلاق طارئ</b> <code>{short(sym)}</code>\n{why}")
    if close_full(ex, sym):
        finalize_trade(ex, sym, reason_hint="Emergency")
    else:
        send(f"🆘 فشل الإغلاق الطارئ لـ <code>{short(sym)}</code>")


# ==========================================
# البيانات
# ==========================================
def fetch_data(ex, sym, tf, limit=130):
    key = (sym, tf)
    cached = _data_cache.get(key)
    if cached and cached[1] and time.time() * 1000 < cached[1][-1][0] + TF_MS.get(tf, 60_000) + 1500:
        return cached[1]
    for _ in range(3):
        try:
            data = ex.fetch_ohlcv(sym, tf, limit=limit)
            _data_cache[key] = (time.time(), data)
            return data
        except Exception as e:
            log.warning(f"[OHLCV] {sym} {tf}: {e}")
            time.sleep(1.3)
    return cached[1] if cached else []


def to_df(raw):
    return add_indicators(pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"]))


def btc_bias(ex):
    if time.time() - _btc_bias_cache["ts"] < 120:
        return _btc_bias_cache["value"]
    val = "neutral"
    try:
        raw = fetch_data(ex, "BTC/USDT:USDT", "1h")
        if len(raw) >= 60:
            r = to_df(raw).iloc[-2]
            if r["close"] > r["ema50"] and r["ema21"] > r["ema50"] and r["ema9"] > r["ema21"]:
                val = "up"
            elif r["close"] < r["ema50"] and r["ema21"] < r["ema50"] and r["ema9"] < r["ema21"]:
                val = "down"
    except Exception as e:
        log.warning(f"[BTC BIAS] {e}")
    _btc_bias_cache.update({"ts": time.time(), "value": val})
    return val


# ==========================================
# نظام السوق + الاستراتيجية (مع سبب الرفض)
# ==========================================
def detect_regime(df_15m, df_1h, df_4h):
    if len(df_4h) < 50 or len(df_1h) < 50:
        return "unknown"
    last_4h = df_4h.iloc[-2]
    last_1h = df_1h.iloc[-2]
    adx4 = last_4h.get("adx", 0)
    adx1 = last_1h.get("adx", 0)
    ema50_4 = last_4h.get("ema50", last_4h["close"])
    up_4h = last_4h["close"] > ema50_4 and last_4h["ema9"] > last_4h["ema21"]
    down_4h = last_4h["close"] < ema50_4 and last_4h["ema9"] < last_4h["ema21"]
    if adx4 > REGIME_ADX4_STRONG and adx1 > REGIME_ADX1_STRONG:
        if up_4h:
            return "strong_trend_up"
        if down_4h:
            return "strong_trend_down"
    if adx4 > REGIME_ADX4_WEAK and (up_4h or down_4h):
        return "weak_trend"
    return "range"


def evaluate_signal(df_15m, df_1h, df_4h):
    """يرجع (signal, None) عند وجود إشارة أو (None, reason) مع أول شرط فشل. المنطق مطابق لـ V42."""
    if len(df_15m) < 70 or len(df_1h) < 60 or len(df_4h) < 60:
        return None, "insufficient_data"
    last = df_15m.iloc[-2]
    prev = df_15m.iloc[-3]
    last_1h = df_1h.iloc[-2]
    last_4h = df_4h.iloc[-2]
    needed = ["ema9", "ema21", "ema50", "rsi", "adx", "atr", "vol_ma"]
    if any(last.get(k) is None or pd.isna(last.get(k)) for k in needed):
        return None, "indicators_nan"
    price = float(last["close"])
    atr_val = float(last["atr"])
    ema21 = float(last["ema21"])
    ema50 = float(last["ema50"])
    rsi_val = float(last["rsi"])
    adx_val = float(last["adx"])

    regime = detect_regime(df_15m, df_1h, df_4h)
    if regime == "unknown":
        return None, "insufficient_data"
    if regime == "range":
        return None, "regime_range"
    atr_pct = atr_val / price
    if atr_pct < MIN_ATR_PCT:
        return None, "atr_low"
    if atr_pct > MAX_ATR_PCT:
        return None, "atr_high"
    if adx_val < MIN_ADX:
        return None, "low_adx"
    if not (last["volume"] > MIN_VOL_MULT * last["vol_ma"]):
        return None, "low_volume"

    long_dir = regime in ("strong_trend_up", "weak_trend") and last_4h["close"] > last_4h["ema50"]
    short_dir = regime in ("strong_trend_down", "weak_trend") and last_4h["close"] < last_4h["ema50"]
    if not long_dir and not short_dir:
        return None, "direction_mismatch"

    near_ema = (abs(price - ema21) / price < NEAR_EMA21_PCT or
                abs(price - ema50) / price < NEAR_EMA50_PCT)
    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * atr_val / price))
    tp_pct = sl_pct * RR_RATIO

    if long_dir:
        bullish_reject = last["close"] > last["open"] and (last["low"] < ema21 or prev["low"] < ema21)
        if not (last["ema9"] > last["ema21"] and last_1h["ema9"] > last_1h["ema21"]):
            return None, "ema_misaligned"
        if not near_ema:
            return None, "far_from_ema"
        if not (RSI_LONG_MIN < rsi_val < RSI_LONG_MAX):
            return None, "rsi_out_of_range"
        if not bullish_reject:
            return None, "no_pullback_candle"
        return {"side": "LONG", "price": price, "sl_pct": sl_pct, "sl": price * (1 - sl_pct),
                "tp": price * (1 + tp_pct), "regime": regime}, None

    bearish_reject = last["close"] < last["open"] and (last["high"] > ema21 or prev["high"] > ema21)
    if not (last["ema9"] < last["ema21"] and last_1h["ema9"] < last_1h["ema21"]):
        return None, "ema_misaligned"
    if not near_ema:
        return None, "far_from_ema"
    if not (RSI_SHORT_MIN < rsi_val < RSI_SHORT_MAX):
        return None, "rsi_out_of_range"
    if not bearish_reject:
        return None, "no_pullback_candle"
    return {"side": "SHORT", "price": price, "sl_pct": sl_pct, "sl": price * (1 + sl_pct),
            "tp": price * (1 - tp_pct), "regime": regime}, None


def market_filters_ok(ex, sym, side):
    """يرجع (ok, reason_code). السبريد المجهول يمنع الدخول، التمويل المجهول لا يمنع (فلتر مساعد)."""
    try:
        t = ex.fetch_ticker(sym)
        bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
        if bid and ask and last:
            spread = (ask - bid) / last
            _last_spread[sym] = (time.time(), spread)
            if spread > MAX_SPREAD_PCT:
                log.info(f"[SPREAD] {sym} {spread * 100:.3f}% > الحد {MAX_SPREAD_PCT * 100:.3f}% — حجب")
                return False, "spread_high"
        else:
            log.info(f"[SPREAD] {sym}: لا توجد أسعار bid/ask في الرد (لم يُطبَّق فلتر السبريد)")
    except Exception as e:
        log.warning(f"[FILTER TICKER] {sym}: {e}")
        return False, "ticker_unavailable"
    try:
        fr = ex.fetch_funding_rate(sym).get('fundingRate')
        if fr is not None:
            if side == "LONG" and fr > MAX_FUNDING_RATE:
                return False, "funding_high"
            if side == "SHORT" and fr < -MAX_FUNDING_RATE:
                return False, "funding_negative"
    except Exception as e:
        log.warning(f"[FILTER FUNDING] {sym}: {e} (تجاوز)")
    return True, ""


def sector_of(sym):
    base = sym.split("/")[0]
    for name, members in SECTORS.items():
        if base in members:
            return name
    return "other"


def sector_limit_reached(sym, side):
    sec = sector_of(sym)
    with state_lock:
        n = sum(1 for s, v in state.items() if v.get("position") == side and sector_of(s) == sec)
    return n >= MAX_PER_SECTOR_SAME_DIR


# ==========================================
# إدارة الصفقة
# ==========================================
def breakeven_price(side, entry):
    return entry * (1 + BE_FEE_BUFFER) if side == "LONG" else entry * (1 - BE_FEE_BUFFER)


def move_sl(ex, sym, st, new_sl, title, notify=True):
    side = st["position"]
    close_side = 'sell' if side == "LONG" else 'buy'
    if not replace_protection(ex, sym, close_side, st["contracts"], new_sl, None):
        return False
    with state_lock:
        state[sym]["sl"] = new_sl
        state[sym]["breakeven_hit"] = True
        prev_notified = state[sym].get("last_notified_sl")
        risk = state[sym].get("initial_risk") or 0
        big_move = (prev_notified is None or risk <= 0 or abs(new_sl - prev_notified) >= 0.3 * risk)
        if notify and big_move:
            state[sym]["last_notified_sl"] = new_sl
    save_state()
    if notify and big_move:
        send(f"{title}\n━━━━━━━━━━━━━━━\n🪙 <code>{short(sym)}</code>\n🛑 الوقف الجديد: <code>{fmt(new_sl)}</code>")
    return True


def do_partial(ex, sym, st, which):
    side = st["position"]
    entry = st["entry"]
    close_side = 'sell' if side == "LONG" else 'buy'
    contracts = st["contracts"]
    raw_amt = st["initial_contracts"] * PARTIAL1_PCT if which == 1 else contracts * PARTIAL2_PCT
    amt = float(ex.amount_to_precision(sym, raw_amt))
    mn = min_amount(ex, sym)
    remaining = contracts - amt
    if amt < mn or remaining < mn:
        with state_lock:
            state[sym][f"partial_{which}_taken"] = True
        save_state()
        return False
    try:
        ex.create_order(sym, 'market', close_side, amt, None,
                        {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
    except Exception as e:
        log.error(f"[PARTIAL ORDER] {sym}: {e}")
        return False
    time.sleep(0.8)
    exit_price = get_current_price(ex, sym, 0) or entry
    pnl = leg_pnl(side, entry, exit_price, amt, contract_size(ex, sym))
    new_sl = breakeven_price(side, entry) if which == 1 else st["sl"]
    protected = replace_protection(ex, sym, close_side, remaining, new_sl, st.get("tp"))
    with state_lock:
        s = state[sym]
        s["contracts"] = remaining
        s[f"partial_{which}_taken"] = True
        s["realized_pnl"] = s.get("realized_pnl", 0.0) + pnl
        s["leg_ts"] = time.time()
        if which == 1 and protected:
            s["sl"] = new_sl
            s["breakeven_hit"] = True
    add_daily_pnl(pnl)
    save_state()
    pct_txt = f"{int(PARTIAL1_PCT * 100)}% من الكمية" if which == 1 else f"{int(PARTIAL2_PCT * 100)}% من المتبقي"
    extra = "نقل الوقف للتعادل" if (which == 1 and protected) else "قفل أرباح إضافية"
    warn = "" if protected else "\n⚠️ تعذر تحديث الحماية."
    send(f"💰 <b>إغلاق جزئي {which} ({pct_txt})</b> — <code>{short(sym)}</code>\nربح الجزء: {pnl:+.2f} USDT | {extra}{warn}")
    return True


def manage_position(ex, sym, st):
    side = st["position"]
    entry = st["entry"]
    d = 1 if side == "LONG" else -1
    price = get_current_price(ex, sym)
    if not price:
        return
    close_side = 'sell' if side == "LONG" else 'buy'

    has_sl, _ = has_protection(ex, sym)
    if has_sl is None:
        log.warning(f"[MANAGE] حالة الحماية مجهولة لـ {sym} — تأجيل الإدارة")
        return
    if not has_sl:
        ok = replace_protection(ex, sym, close_side, st["contracts"], st["sl"], st.get("tp"))
        if not ok:
            emergency_close(ex, sym, "الصفقة بدون وقف خسارة")
            return

    risk = st.get("initial_risk") or abs(entry - (st.get("initial_sl") or st["sl"]))
    if risk <= 0:
        return
    r_now = (price - entry) * d / risk

    if not st.get("partial_1_taken") and r_now >= PARTIAL1_R:
        if do_partial(ex, sym, st, 1):
            return
    elif st.get("partial_1_taken") and not st.get("partial_2_taken") and r_now >= PARTIAL2_R:
        if do_partial(ex, sym, st, 2):
            return

    if not st.get("breakeven_hit"):
        if r_now >= BE_TRIGGER_R:
            move_sl(ex, sym, st, breakeven_price(side, entry), "🛡️ تأمين الصفقة (Breakeven)")
        return

    raw = fetch_data(ex, sym, "15m")
    if len(raw) < 50:
        return
    atr_val = to_df(raw).iloc[-2].get("atr")
    if atr_val is None or pd.isna(atr_val):
        return
    mult = TRAIL_ATR_MULT
    if st.get("partial_2_taken"):
        mult *= 0.82
    elif st.get("partial_1_taken"):
        mult *= 0.90
    be = breakeven_price(side, entry)
    sl = st["sl"]
    if side == "LONG":
        new_sl = max(price - mult * float(atr_val), be)
        if new_sl > sl * 1.001 and new_sl < price:
            move_sl(ex, sym, st, new_sl, "📈 تحريك الوقف المتحرك")
    else:
        new_sl = min(price + mult * float(atr_val), be)
        if new_sl < sl * 0.999 and new_sl > price:
            move_sl(ex, sym, st, new_sl, "📈 تحريك الوقف المتحرك")


# ==========================================
# السماح بالدخول
# ==========================================
def floating_pnl_cached(ex):
    """الربح/الخسارة العائمة من كاش الأسعار (بدون طلبات شبكة)."""
    total = 0.0
    for sym, v in open_positions_snapshot().items():
        c = _price_cache.get(sym)
        price = c[1] if c else v["entry"]
        d = 1 if v["position"] == "LONG" else -1
        total += (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
    return total


def entries_allowed(ex):
    """يرجع (ok, reason)."""
    if meta.get("paused"):
        return False, "paused"
    snap = daily_snapshot()
    equity = get_equity(ex)
    if equity <= 0:
        return False, "equity_unavailable"
    loss_now = snap["value"]
    if INCLUDE_FLOATING_IN_LOSS_LIMIT:
        loss_now += min(floating_pnl_cached(ex), 0.0)
    if loss_now <= -equity * DAILY_LOSS_LIMIT_PCT / 100.0:
        if meta.get("loss_alert") != snap["date"]:
            set_meta("loss_alert", snap["date"])
            send("⛔ <b>وصلنا حد الخسارة اليومي</b>\nتم إيقاف الصفقات الجديدة")
        return False, "daily_loss_limit"
    if DAILY_PROFIT_TARGET_PCT > 0 and snap["value"] >= equity * DAILY_PROFIT_TARGET_PCT / 100.0:
        if meta.get("profit_alert") != snap["date"]:
            set_meta("profit_alert", snap["date"])
            send("✅ <b>تحقق الهدف اليومي للربح</b>\nتم إيقاف الدخول لحماية الأرباح")
        return False, "daily_profit_target"
    return True, ""


# ==========================================
# معالجة الرموز
# ==========================================
def process_symbol(ex, sym):
    try:
        with trade_lock:
            with state_lock:
                st = state.setdefault(sym, new_sym_state())
                pos_data = dict(st)

            if pos_data.get("position") is not None:
                exch_pos = get_open_position(ex, sym)
                if exch_pos is None:
                    finalize_trade(ex, sym)
                    return
                with state_lock:
                    state[sym]["contracts"] = exch_pos["contracts"]
                    cur = dict(state[sym])
                manage_position(ex, sym, cur)
                return

            if time.time() - pos_data.get("last_exit_ts", 0) < REENTRY_COOLDOWN_SEC:
                set_last_reason(sym, "cooldown")
                return
            ok, gate = entries_allowed(ex)
            if not ok:
                set_last_reason(sym, gate)
                return
            with state_lock:
                open_count = sum(1 for v in state.values() if v.get("position"))
            if open_count >= MAX_OPEN_POSITIONS:
                set_last_reason(sym, "max_positions")
                return

            raw_15m = fetch_data(ex, sym, "15m")
            if len(raw_15m) < 70:
                set_last_reason(sym, "no_data")
                return
            closed_ts = raw_15m[-2][0]
            if _last_eval.get(sym) == closed_ts:
                return
            raw_1h = fetch_data(ex, sym, "1h")
            raw_4h = fetch_data(ex, sym, "4h")
            if len(raw_1h) < 60 or len(raw_4h) < 60:
                set_last_reason(sym, "no_data")
                return
            _last_eval[sym] = closed_ts

            signal, reason = evaluate_signal(to_df(raw_15m), to_df(raw_1h), to_df(raw_4h))
            note_eval(sym, reason)
            if not signal:
                return
            side = signal["side"]
            log.info(f"[SIGNAL] {sym} {side} regime={signal['regime']}")

            with state_lock:
                same_dir = sum(1 for v in state.values() if v.get("position") == side)
            if same_dir >= MAX_SAME_DIRECTION:
                note_block(sym, "max_same_direction")
                return
            if sector_limit_reached(sym, side):
                note_block(sym, "sector_limit")
                return
            if USE_BTC_FILTER and not sym.startswith("BTC/"):
                bias = btc_bias(ex)
                if (side == "LONG" and bias == "down") or (side == "SHORT" and bias == "up"):
                    note_block(sym, "btc_filter")
                    return
            ok, code = market_filters_ok(ex, sym, side)
            if not ok:
                note_block(sym, code)
                return

            actual_price = get_current_price(ex, sym, 0) or signal["price"]
            if abs(actual_price - signal["price"]) / signal["price"] > MAX_ENTRY_SLIPPAGE_PCT:
                note_block(sym, "slippage")
                return
            sl_pct = signal["sl_pct"]
            if side == "LONG":
                sl = actual_price * (1 - sl_pct)
                tp = actual_price * (1 + sl_pct * RR_RATIO)
            else:
                sl = actual_price * (1 + sl_pct)
                tp = actual_price * (1 - sl_pct * RR_RATIO)

            margin = compute_margin(get_equity(ex), sl_pct)
            if margin <= 0:
                log.info(f"[SKIP] {sym}: الهامش غير صالح (رصيد مجهول أو أقل من الحد الأدنى)")
                note_block(sym, "margin_too_small")
                return

            success, info, entry, contracts = open_position(ex, sym, side, actual_price, sl, tp, margin)
            now = time.time()
            if success:
                note_opened()
                with state_lock:
                    state[sym].update({
                        "position": side, "entry": entry, "sl": sl, "tp": tp,
                        "initial_sl": sl, "initial_risk": abs(entry - sl),
                        "contracts": contracts, "initial_contracts": contracts,
                        "margin": margin, "opened_ts": now, "leg_ts": now,
                        "realized_pnl": 0.0, "last_notified_sl": None,
                        "breakeven_hit": False, "partial_1_taken": False, "partial_2_taken": False,
                        "regime": signal.get("regime", "")})
            else:
                note_block(sym, "open_failed")
                with state_lock:
                    state[sym]["last_exit_ts"] = now
            save_state()
            caption = msg_entry(sym, side, entry, sl, tp, margin, success, signal.get("regime", ""))
            if not success:
                caption += f"\n❌ {info}"
            send(caption)
    except Exception as e:
        loop_info["last_error"] = f"{sym}: {str(e)[:150]}"
        log.error(f"[ERROR] {sym}: {e}")


# ==========================================
# مزامنة عند الإقلاع
# ==========================================
def adopt_sl_pct(ex, sym):
    try:
        raw = fetch_data(ex, sym, "15m")
        if len(raw) >= 60:
            df = to_df(raw)
            a, c = float(df.iloc[-2]["atr"]), float(df.iloc[-2]["close"])
            if c > 0 and not np.isnan(a):
                return max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * a / c))
    except Exception:
        pass
    return FALLBACK_SL_PCT


def sync_positions(ex, active_symbols):
    """تُستدعى عند الإقلاع فقط. جلب المراكز داخل trade_lock لتفادي لقطة قديمة."""
    with trade_lock:
        try:
            exch = {}
            for p in ex.fetch_positions():
                c = abs(float(p.get('contracts') or 0))
                if c > 0 and p.get('symbol') in active_symbols:
                    exch[p['symbol']] = p
        except Exception as e:
            log.error(f"[SYNC] {e}")
            return
        for sym in active_symbols:
            with state_lock:
                st = dict(state.get(sym, new_sym_state()))
            if st.get("position") and sym not in exch:
                finalize_trade(ex, sym)
            elif st.get("position") and sym in exch:
                contracts = abs(float(exch[sym].get('contracts')))
                with state_lock:
                    state[sym]["contracts"] = contracts
                has_sl, _ = has_protection(ex, sym)
                if has_sl is False:
                    close_side = 'sell' if st["position"] == "LONG" else 'buy'
                    replace_protection(ex, sym, close_side, contracts, st["sl"], st.get("tp"))
            elif not st.get("position") and sym in exch:
                p = exch[sym]
                side = "LONG" if p.get('side') == 'long' else "SHORT"
                entry = float(p.get('entryPrice') or 0)
                contracts = abs(float(p.get('contracts')))
                sl_pct = adopt_sl_pct(ex, sym)
                if side == "LONG":
                    sl, tp = entry * (1 - sl_pct), entry * (1 + sl_pct * RR_RATIO)
                else:
                    sl, tp = entry * (1 + sl_pct), entry * (1 - sl_pct * RR_RATIO)
                current_price = get_current_price(ex, sym)
                if current_price and ((side == "LONG" and current_price >= tp) or
                                      (side == "SHORT" and current_price <= tp)):
                    log.warning(f"[SYNC] {sym} تجاوز الهدف أثناء الإيقاف — إغلاق")
                    close_full(ex, sym)
                    continue
                close_side = 'sell' if side == "LONG" else 'buy'
                cancel_pending(ex, sym)
                if protect_position(ex, sym, close_side, contracts, sl, tp)[0]:
                    now = time.time()
                    with state_lock:
                        state[sym] = new_sym_state()
                        state[sym].update({
                            "position": side, "entry": entry, "sl": sl, "tp": tp,
                            "initial_sl": sl, "initial_risk": abs(entry - sl),
                            "contracts": contracts, "initial_contracts": contracts,
                            "margin": float(p.get('initialMargin') or POSITION_SIZE_USDT),
                            "opened_ts": now, "leg_ts": now, "regime": "adopted"})
                    send(f"♻️ تم تبني مركز <code>{short(sym)}</code> ({side}) — SL {sl_pct * 100:.2f}% (ATR)")
        save_state()


# ==========================================
# أوامر تيليجرام
# ==========================================
def open_positions_snapshot():
    with state_lock:
        return {k: dict(v) for k, v in state.items() if v.get("position")}


def unrealized(ex, sym, v):
    price = get_current_price(ex, sym) or v["entry"]
    d = 1 if v["position"] == "LONG" else -1
    upnl = (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
    return price, upnl


def flags_text(v):
    f = ""
    if v.get("partial_2_taken"):
        f += " 💰💰"
    elif v.get("partial_1_taken"):
        f += " 💰"
    if v.get("breakeven_hit"):
        f += " 🛡️"
    return f


def loop_age_text():
    last = loop_info["last_cycle_ts"]
    if not last:
        return "لم تبدأ بعد"
    age = int(time.time() - last)
    return f"قبل {age}ث" + (" ⚠️" if age > WATCHDOG_SEC else " ✅")


def status_text(ex):
    snap = daily_snapshot()
    equity = get_equity(ex, force=True)
    opens = open_positions_snapshot()
    total_float = 0.0
    lines_pos = []
    for sym, v in opens.items():
        price, upnl = unrealized(ex, sym, v)
        total_float += upnl
        lines_pos.append(f"• <code>{short(sym)}</code> {v['position']} | {fmt(v['entry'])} → {fmt(price)} | {upnl:+.2f}{flags_text(v)}")
    with diag_lock:
        ev, sg, op = diag["evaluated"], diag["signals"], diag["opened"]
    bias = _btc_bias_cache["value"] if _btc_bias_cache["ts"] else "لم يُحسب بعد"
    lines = [f"📊 <b>حالة البوت {VERSION}</b> {'⏸️' if meta.get('paused') else '▶️'}", "━━━━━━━━━━━━━━━",
             f"💼 الرصيد: {equity:,.2f} USDT",
             f"📅 المحقق اليوم: {snap['value']:+.2f} USDT | صفقات: {snap['trades']} | رابحة: {snap['wins']}",
             f"📈 <b>الربح العائم: {total_float:+.2f} USDT</b>",
             f"🔄 آخر دورة فحص: {loop_age_text()} | دورات: {loop_info['cycles']}",
             f"🔎 اليوم: تقييم {ev} | إشارات {sg} | فُتحت {op}  (التفاصيل: /why)",
             f"🐋 BTC Bias: {bias} | ⚙️ {PROFILE_NAME}",
             "━━━━━━━━━━━━━━━"]
    lines.extend(lines_pos if lines_pos else ["لا توجد صفقات مفتوحة."])
    return "\n".join(lines)


def why_text(arg, active_symbols):
    if arg:
        sym = find_symbol(arg, active_symbols)
        if not sym:
            return "عملة غير معروفة."
        with diag_lock:
            item = _last_reason.get(sym)
        if not item:
            return f"لا يوجد تقييم بعد لـ {short(sym)}."
        ts, r = item
        txt = f"🔍 <b>{short(sym)}</b>\nآخر نتيجة: {label(r)}\nقبل {int((time.time() - ts) / 60)} دقيقة"
        sp = _last_spread.get(sym)
        if sp:
            txt += f"\nآخر سبريد مقاس: {sp[1] * 100:.3f}% (الحد {MAX_SPREAD_PCT * 100:.2f}%)"
        return txt
    with diag_lock:
        d = json.loads(json.dumps(diag))
    ev = d["evaluated"]
    lines = [f"🔎 <b>لماذا لا توجد صفقات؟</b> — {d['date']}", "━━━━━━━━━━━━━━━",
             f"📊 تقييمات (رمز×شمعة): {ev}",
             f"✅ إشارات: {d['signals']} | 🟢 صفقات فُتحت: {d['opened']}"]
    lines.append(f"⚙️ البروفايل: <b>{PROFILE_NAME}</b> (/config)")
    if meta.get("paused"):
        lines.append("⏸️ <b>البوت موقوف (/resume)</b>")
    if ev:
        lines += ["", "<b>أسباب رفض الشموع:</b>"]
        for r, n in sorted(d["reasons"].items(), key=lambda x: -x[1])[:8]:
            lines.append(f"• {label(r)}: {n} ({n / ev * 100:.0f}%)")
    else:
        lines.append("\nلا توجد تقييمات بعد (انتظر إغلاق شمعة 15 دقيقة).")
    if d["blocks"]:
        lines += ["", "<b>إشارات حُجبت بعد ظهورها:</b>"]
        for r, n in sorted(d["blocks"].items(), key=lambda x: -x[1]):
            lines.append(f"• {label(r)}: {n}")
    lines.append("\nلتفاصيل عملة: /why BTC")
    return "\n".join(lines)


def config_text():
    return "\n".join([
        f"⚙️ <b>الإعدادات الحالية</b> — البروفايل: <b>{PROFILE_NAME}</b>", "━━━━━━━━━━━━━━━",
        f"ATR الأدنى (15د): {MIN_ATR_PCT * 100:.2f}%",
        f"ADX الأدنى (15د): {MIN_ADX:g}",
        f"ADX 4س قوي/ضعيف: {REGIME_ADX4_STRONG:g} / {REGIME_ADX4_WEAK:g} | ADX 1س قوي: {REGIME_ADX1_STRONG:g}",
        f"الحجم ≥ {MIN_VOL_MULT:g}× المتوسط",
        f"RSI LONG: {RSI_LONG_MIN:g}–{RSI_LONG_MAX:g} | SHORT: {RSI_SHORT_MIN:g}–{RSI_SHORT_MAX:g}",
        f"القرب من EMA21 / EMA50: {NEAR_EMA21_PCT * 100:.1f}% / {NEAR_EMA50_PCT * 100:.1f}%",
        f"أقصى سبريد: {MAX_SPREAD_PCT * 100:.2f}%",
        f"المخاطرة: {RISK_PCT_PER_TRADE}% | رافعة {LEVERAGE}x | سقف الهامش {MAX_MARGIN_USDT:.0f}",
        "━━━━━━━━━━━━━━━",
        "للتعديل: متغيرات البيئة في Render (PROFILE أو أي حد منفرد) ثم إعادة النشر."])


def positions_text(ex):
    opens = open_positions_snapshot()
    if not opens:
        return "📭 لا توجد صفقات مفتوحة حالياً."
    lines = ["📊 <b>تفاصيل الصفقات المفتوحة</b>", "━━━━━━━━━━━━━━━"]
    total = 0.0
    for sym, v in opens.items():
        price, upnl = unrealized(ex, sym, v)
        total += upnl
        pct = (upnl / v["margin"] * 100) if v.get("margin") else 0.0
        mins = int((time.time() - v["opened_ts"]) / 60) if v.get("opened_ts") else 0
        duration = f"{mins // 60}س {mins % 60}د" if mins >= 60 else f"{mins}د"
        risk = v.get("initial_risk") or 0
        d = 1 if v["position"] == "LONG" else -1
        r_now = ((price - v["entry"]) * d / risk) if risk else 0.0
        lines.append(
            f"🪙 <b>{short(sym)}</b> ({v['position']})\n"
            f"📥 الدخول: <code>{fmt(v['entry'])}</code>\n"
            f"📤 الحالي: <code>{fmt(price)}</code>\n"
            f"💰 العائم: <b>{upnl:+.2f} USDT</b> ({pct:+.1f}%) | {r_now:+.2f}R\n"
            f"🧾 المحقق من الجزئيات: {v.get('realized_pnl', 0.0):+.2f}\n"
            f"🛑 الوقف: <code>{fmt(v['sl']) if v.get('sl') else '—'}</code> | 🎯 <code>{fmt(v['tp']) if v.get('tp') else '—'}</code>\n"
            f"⏱ المدة: {duration}{flags_text(v)}\n"
            f"────────────────")
    lines.append(f"\n📈 <b>إجمالي العائم: {total:+.2f} USDT</b>")
    return "\n".join(lines)


def risk_text(ex):
    snap = daily_snapshot()
    equity = get_equity(ex, force=True)
    opens = open_positions_snapshot()
    longs = sum(1 for v in opens.values() if v["position"] == "LONG")
    shorts = sum(1 for v in opens.values() if v["position"] == "SHORT")
    loss_limit = equity * DAILY_LOSS_LIMIT_PCT / 100.0
    profit_target = equity * DAILY_PROFIT_TARGET_PCT / 100.0
    floating = floating_pnl_cached(ex)
    status = "⏸️ متوقف" if meta.get("paused") else "▶️ يعمل"
    return "\n".join([
        "🛡️ <b>ملخص إدارة المخاطر</b>", "━━━━━━━━━━━━━━━",
        f"💼 الرصيد: <b>{equity:,.2f} USDT</b>",
        f"📊 الحالة: {status}", "",
        f"🎯 المخاطرة لكل صفقة: <b>{RISK_PCT_PER_TRADE}%</b> (سقف الهامش {MAX_MARGIN_USDT:.0f} USDT)",
        f"📉 حد الخسارة اليومي: {DAILY_LOSS_LIMIT_PCT}% ({loss_limit:.1f} USDT) — يشمل العائم السالب",
        f"📈 هدف الربح اليومي: {DAILY_PROFIT_TARGET_PCT}% ({profit_target:.1f} USDT)", "",
        f"🔢 المفتوحة: {len(opens)} / {MAX_OPEN_POSITIONS}",
        f"🟢 LONG: {longs} | 🔴 SHORT: {shorts} (الحد {MAX_SAME_DIRECTION})", "",
        f"📅 المحقق اليوم: <b>{snap['value']:+.2f} USDT</b> | العائم: {floating:+.2f}",
        f"🔻 المتبقي للخسارة: {loss_limit + snap['value'] + min(floating, 0):+.2f} USDT",
        f"🔺 المتبقي للهدف: {profit_target - snap['value']:+.2f} USDT",
        "━━━━━━━━━━━━━━━"])


def daily_text(ex):
    snap = daily_snapshot()
    equity = get_equity(ex, force=True)
    total_upnl = sum(unrealized(ex, s, v)[1] for s, v in open_positions_snapshot().items())
    wr = (snap["wins"] / snap["trades"] * 100) if snap["trades"] else 0
    return "\n".join([
        f"📅 <b>ملخص اليوم</b> — {snap['date']}", "━━━━━━━━━━━━━━━",
        f"💰 المحقق اليوم: <b>{snap['value']:+.2f} USDT</b>",
        f"📈 العائم حالياً: <b>{total_upnl:+.2f} USDT</b>",
        f"📊 الإجمالي التقديري: <b>{snap['value'] + total_upnl:+.2f} USDT</b>", "",
        f"🔢 الصفقات المكتملة: {snap['trades']}",
        f"✅ الرابحة: {snap['wins']} ({wr:.0f}%)",
        f"💼 الرصيد: {equity:,.2f} USDT", "━━━━━━━━━━━━━━━"])


def find_symbol(arg, active_symbols):
    arg = arg.upper().replace("/USDT", "").replace("USDT", "")
    for s in active_symbols:
        if s.startswith(arg + "/"):
            return s
    return None


def handle_command(ex, text, chat_id, active_symbols):
    parts = text.strip().split()
    cmd = parts[0].lower().split("@")[0]
    if cmd in ("/status", "/start"):
        send(status_text(ex), chat_id)
    elif cmd in ("/pos", "/positions"):
        send(positions_text(ex), chat_id)
    elif cmd == "/risk":
        send(risk_text(ex), chat_id)
    elif cmd == "/daily":
        send(daily_text(ex), chat_id)
    elif cmd == "/stats":
        send(stats_text(), chat_id)
    elif cmd == "/config":
        send(config_text(), chat_id)
    elif cmd == "/why":
        send(why_text(parts[1] if len(parts) > 1 else None, active_symbols), chat_id)
    elif cmd == "/pause":
        set_meta("paused", True)
        send("⏸️ تم إيقاف فتح صفقات جديدة.", chat_id)
    elif cmd == "/resume":
        set_meta("paused", False)
        send("▶️ تم استئناف البحث.", chat_id)
    elif cmd == "/close":
        if len(parts) < 2:
            send("استخدم: /close BTC", chat_id)
            return
        sym = find_symbol(parts[1], active_symbols)
        if not sym:
            send("عملة غير معروفة.", chat_id)
            return
        with trade_lock:
            with state_lock:
                pos_data = dict(state.get(sym, {}))
            if not pos_data.get("position"):
                send("لا توجد صفقة مفتوحة.", chat_id)
                return
            if close_full(ex, sym):
                finalize_trade(ex, sym, reason_hint="Manual")
            else:
                send("❌ فشل الإغلاق.", chat_id)
    elif cmd == "/closeall":
        if len(parts) < 2 or parts[1].lower() != "confirm":
            send("⚠️ للتأكيد: /closeall confirm", chat_id)
            return
        with trade_lock:
            with state_lock:
                syms = [k for k, v in state.items() if v.get("position")]
            if not syms:
                send("لا توجد صفقات.", chat_id)
                return
            for sym in syms:
                if close_full(ex, sym):
                    finalize_trade(ex, sym, reason_hint="Manual")
                else:
                    send(f"❌ فشل إغلاق <code>{short(sym)}</code>", chat_id)
    elif cmd == "/help":
        send("<b>📋 الأوامر المتاحة:</b>\n\n"
             "/status - نظرة عامة + حالة الحلقة\n"
             "/why - لماذا لا توجد صفقات؟ (أسباب الرفض)\n"
             "/why BTC - آخر سبب لعملة محددة\n"
             "/config - الإعدادات والحدود الحالية\n"
             "/pos - تفاصيل كل صفقة مفتوحة\n"
             "/risk - ملخص إدارة المخاطر\n"
             "/daily - ملخص أداء اليوم\n"
             "/stats - إحصائيات الأداء الكلية\n"
             "/pause - إيقاف الدخول مؤقتاً\n"
             "/resume - استئناف الدخول\n"
             "/close BTC - إغلاق عملة محددة\n"
             "/closeall confirm - إغلاق جميع الصفقات\n"
             "/help - عرض هذه القائمة", chat_id)


def telegram_listener(active_symbols):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS:
        return
    try:
        ex2 = init_exchange()
    except Exception as e:
        log.error(f"[TG INIT] {e}")
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
                    send(f"خطأ: {e}", chat_id)
        except Exception as e:
            log.warning(f"[TG POLL] {e}")
            time.sleep(5)


def watchdog():
    """ينبهك إذا توقفت الحلقة الرئيسية (وهي مستقلة عن خيط تيليجرام)."""
    while True:
        time.sleep(60)
        last = loop_info["last_cycle_ts"]
        if not last:
            continue
        age = time.time() - last
        if age > WATCHDOG_SEC and not loop_info["alerted"]:
            loop_info["alerted"] = True
            send(f"🚨 <b>تحذير:</b> حلقة الفحص متوقفة منذ {int(age)} ثانية.\nآخر خطأ: {loop_info['last_error'] or '—'}")
        elif age <= WATCHDOG_SEC and loop_info["alerted"]:
            loop_info["alerted"] = False
            send("✅ عادت حلقة الفحص للعمل.")


# ==========================================
# التقارير الدورية
# ==========================================
def maybe_send_daily_report(ex):
    now = datetime.now(KSA_TZ)
    today = str(now.date())
    if now.hour == 23 and now.minute >= 55 and meta.get("last_report") != today:
        snap = daily_snapshot()
        wr = (snap["wins"] / snap["trades"] * 100) if snap["trades"] else 0
        with diag_lock:
            ev, sg, op = diag["evaluated"], diag["signals"], diag["opened"]
        send(f"📋 <b>التقرير اليومي {VERSION}</b> — {today}\n━━━━━━━━━━━━━━━\n"
             f"💰 النتيجة: {snap['value']:+.2f} USDT\n"
             f"🔢 الصفقات: {snap['trades']} | الرابحة: {snap['wins']} ({wr:.0f}%)\n"
             f"🔎 تقييمات {ev} | إشارات {sg} | فُتحت {op}\n"
             f"💼 الرصيد: {get_equity(ex, force=True):,.2f} USDT")
        set_meta("last_report", today)


def maybe_send_heartbeat(ex):
    now = datetime.now(KSA_TZ)
    current_hour = now.strftime("%Y-%m-%d %H")
    if meta.get("last_heartbeat") != current_hour and now.minute < 4:
        set_meta("last_heartbeat", current_hour)
        log.info(f"[HEARTBEAT] Open: {len(open_positions_snapshot())} | Equity: {get_equity(ex)}")


# ==========================================
# الحلقة الرئيسية
# ==========================================
def trading_loop():
    log.info(f"[INIT] Starting {VERSION} ...")
    if not OKX_API_KEY:
        send("❌ مفاتيح API غير موجودة")
        return
    try:
        ex = init_exchange()
        load_state()
        balance = get_equity(ex, force=True)
        mode_str = "تجريبي (Sandbox)" if USE_SANDBOX else "حقيقي"
        log.info(f"[OKX] Connected ({mode_str}). Equity: {balance}")
    except Exception as e:
        send(f"❌ فشل الاتصال: {e}")
        return

    active_symbols = [s for s in FUTURES_SYMBOLS if s in ex.markets]
    sync_positions(ex, active_symbols)
    threading.Thread(target=telegram_listener, args=(active_symbols,), daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    send(f"🚀 <b>تم تشغيل البوت {VERSION}</b>\n━━━━━━━━━━━━━━━\n"
         f"⚙️ الوضع: {mode_str}\n🪙 العملات: {len(active_symbols)}\n"
         f"📊 أقصى صفقات: {MAX_OPEN_POSITIONS}\n"
         f"💰 المخاطرة: {RISK_PCT_PER_TRADE}% | رافعة: {LEVERAGE}x\n"
         f"🎚️ البروفايل: {PROFILE_NAME}\n"
         f"🔎 /why أسباب عدم الدخول | ⚙️ /config الإعدادات\n📱 /help لعرض الأوامر\n━━━━━━━━━━━━━━━")
    if PROFILE_WARNING:
        send(f"⚠️ {PROFILE_WARNING}")

    loop_info["last_cycle_ts"] = time.time()
    fail_streak = 0
    while True:
        start = time.time()
        cycle_errors = 0
        try:
            refresh_prices(ex, active_symbols)
            if USE_BTC_FILTER:
                btc_bias(ex)
            for sym in active_symbols:
                try:
                    process_symbol(ex, sym)
                except Exception as e:
                    cycle_errors += 1
                    loop_info["last_error"] = f"{sym}: {str(e)[:150]}"
                    log.error(f"[LOOP] {sym}: {e}")
                time.sleep(0.25)
            maybe_send_daily_report(ex)
            maybe_send_heartbeat(ex)
        except Exception as e:
            cycle_errors += 1
            loop_info["last_error"] = str(e)[:150]
            log.error(f"[LOOP FATAL] {e}")
        loop_info["cycles"] += 1
        loop_info["last_cycle_ts"] = time.time()
        loop_info["last_duration"] = time.time() - start
        if loop_info["cycles"] % 10 == 1:
            with diag_lock:
                ev, sg, op = diag["evaluated"], diag["signals"], diag["opened"]
            log.info(f"[CYCLE] #{loop_info['cycles']} {loop_info['last_duration']:.0f}s | "
                     f"open={len(open_positions_snapshot())} | eval={ev} signals={sg} opened={op}")
        fail_streak = fail_streak + 1 if cycle_errors >= 5 else 0
        if fail_streak >= 3:
            try:
                ex = init_exchange()
                fail_streak = 0
                log.warning("[RECONNECT] تم إعادة الاتصال")
            except Exception as e:
                log.error(f"[RECONNECT FAIL] {e}")
        time.sleep(max(8, SCAN_INTERVAL - (time.time() - start)))


if __name__ == "__main__":
    if not acquire_single_instance():
        log.error("نسخة أخرى من البوت تعمل بالفعل — إيقاف.")
        sys.exit(1)
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()