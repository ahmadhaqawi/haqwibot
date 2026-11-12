# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V51 (Adaptive + AI Analysis + Paper Mode + Execution Safety)

التغييرات عن V50:
1) send() غير محجوب عبر queue + worker thread (تيليجرام لا يوقف التداول).
2) KER متكيف: median لآخر 3 شموع + عتبة أدنى في الترند القوي.
3) إصلاح score: الارتداد المثالي يحصل على 15 في setup بدل 12.
4) تقييم pullback و breakout معاً واختيار الأعلى score بدل if/elif.
5) سقف انزلاق ديناميكي: max(0.4%, 0.18*sl_pct).
6) حجم الصفقة يتناسب مع درجة الفرصة (compute_margin_scored).
7) بروفايل opportunist جديد (بين balanced و active، آمن للحقيقي).
8) Paper mode: كل إشارة تُرفض بـ score_low تُسجَّل في paper_signals.csv.
9) تسجيل عوامل score كلها + volume_ratio + ker في الـ journal (لمستقبل ML).
10) إصلاح bucket 74-79 في /stats.
11) /ai (AI Gateway + fallback محلي), /export, /restore, /paper, نسخ احتياطي تلقائي.
12) تنظيف دوري للذاكرة (_last_reason, _last_spread, _last_emergency_msg).
13) signal_score مضاف لـ new_sym_state (اتساق).
"""
import os
import sys
import csv
import json
import time
import logging
import threading
import queue
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxt
import requests
from flask import Flask

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

VERSION = "V51"
KSA_TZ = timezone(timedelta(hours=3))


def env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return float(default)


def env_flag(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")


# ==========================================
# الإعدادات الأساسية
# ==========================================
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

SCAN_INTERVAL = int(env_float("SCAN_INTERVAL", 15))
UNIVERSE_REFRESH_SEC = int(env_float("UNIVERSE_REFRESH_SEC", 1800))
MAX_UNIVERSE_SYMBOLS = int(env_float("MAX_UNIVERSE_SYMBOLS", 60))
ENTRY_MODE = os.getenv("ENTRY_MODE", "balanced").strip().lower()
if ENTRY_MODE not in ("balanced", "active", "strict"):
    ENTRY_MODE = "balanced"

_entry_defaults = {
    "strict": {"min_ker": 0.30, "min_adx": 23, "min_score": 78, "bo_vol": 1.8},
    "balanced": {"min_ker": 0.25, "min_adx": 20, "min_score": 68, "bo_vol": 1.5},
    "active": {"min_ker": 0.18, "min_adx": 18, "min_score": 60, "bo_vol": 1.20},
}[ENTRY_MODE]

MIN_KER = env_float("MIN_KER", _entry_defaults["min_ker"])
MIN_ADX_OVERRIDE = env_float("MIN_ADX", _entry_defaults["min_adx"])
MIN_SIGNAL_SCORE = int(env_float("MIN_SIGNAL_SCORE", _entry_defaults["min_score"]))
KER_PERIOD = int(env_float("KER_PERIOD", 10))
INCLUDE_NEW_MARKETS_DAYS = int(env_float("INCLUDE_NEW_MARKETS_DAYS", 0))
UNIVERSE_MODE = os.getenv("UNIVERSE_MODE", "dynamic").strip().lower()
EXCLUDE_BASES = {x.strip().upper() for x in os.getenv(
    "EXCLUDE_BASES",
    "ASML,QQQ,SPY,ANTHROPIC,OPENAI,NVDA,TSLA,AAPL,MSFT,GOOGL,AMZN,META,COIN,MSTR,CRCL,HOOD"
).split(",") if x.strip()}

ORPHAN_CHECK_SEC = int(env_float("ORPHAN_CHECK_SEC", 60))
ADOPT_ORPHANS = env_flag("ADOPT_ORPHANS", True)
GHOST_MAX_AGE_SEC = 120
MISSING_CONFIRMATIONS = 2

LEVERAGE = int(env_float("LEVERAGE", 5))
MARGIN_MODE = "isolated"
MAX_OPEN_POSITIONS = int(env_float("MAX_OPEN_POSITIONS", 6))
REENTRY_COOLDOWN_SEC = int(env_float("REENTRY_COOLDOWN_SEC", 1500))

USE_RISK_SIZING = True
RISK_PCT_PER_TRADE = env_float("RISK_PCT_PER_TRADE", 0.65)
POSITION_SIZE_USDT = 50
MIN_MARGIN_USDT = 10
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
MAX_SAME_DIRECTION = int(env_float("MAX_SAME_DIRECTION", 3))
MAX_FUNDING_RATE = 0.00045
MAX_ENTRY_SLIPPAGE_PCT = env_float("MAX_ENTRY_SLIPPAGE_PCT", 0.004)
USE_BTC_FILTER = True

# ---------- AI Gateway (تحليل فقط، لا يدخل في التنفيذ) ----------
AI_GATEWAY_URL = os.getenv("AI_GATEWAY_URL", "https://api.openai.com/v1/chat/completions")
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
BACKUP_INTERVAL_SEC = int(env_float("BACKUP_INTERVAL_SEC", 6 * 3600))

# ---------- إعدادات Paper Mode ----------
ENABLE_PAPER_LOG = env_flag("ENABLE_PAPER_LOG", True)

# ---------- بروفايلات الاستراتيجية ----------
PROFILES = {
    "conservative": {
        "min_atr_pct": 0.004, "min_adx": 23, "min_vol_mult": 1.2,
        "regime_adx4_strong": 28, "regime_adx1_strong": 24, "regime_adx4_weak": 20,
        "rsi_long_min": 40, "rsi_long_max": 61, "rsi_short_min": 39, "rsi_short_max": 60,
        "near_ema21_pct": 0.011, "near_ema50_pct": 0.016, "max_spread_pct": 0.0011,
        "rel_atr_ratio": 99, "atr_hard_floor": 0.003,
        "bo_vol_mult": 1.8, "bo_max_ext_atr": 2.0,
    },
    "balanced": {
        "min_atr_pct": 0.003, "min_adx": 20, "min_vol_mult": 1.0,
        "regime_adx4_strong": 26, "regime_adx1_strong": 22, "regime_adx4_weak": 18,
        "rsi_long_min": 38, "rsi_long_max": 64, "rsi_short_min": 36, "rsi_short_max": 62,
        "near_ema21_pct": 0.013, "near_ema50_pct": 0.019, "max_spread_pct": 0.0015,
        "rel_atr_ratio": 1.15, "atr_hard_floor": 0.0018,
        "bo_vol_mult": 1.5, "bo_max_ext_atr": 2.5,
    },
    "opportunist": {
        "min_atr_pct": 0.0026, "min_adx": 19, "min_vol_mult": 0.9,
        "regime_adx4_strong": 25, "regime_adx1_strong": 21, "regime_adx4_weak": 16,
        "rsi_long_min": 36, "rsi_long_max": 66, "rsi_short_min": 34, "rsi_short_max": 64,
        "near_ema21_pct": 0.014, "near_ema50_pct": 0.021, "max_spread_pct": 0.0018,
        "rel_atr_ratio": 1.05, "atr_hard_floor": 0.0017,
        "bo_vol_mult": 1.3, "bo_max_ext_atr": 2.8,
    },
    "active": {
        "min_atr_pct": 0.0022, "min_adx": 18, "min_vol_mult": 0.8,
        "regime_adx4_strong": 24, "regime_adx1_strong": 20, "regime_adx4_weak": 15,
        "rsi_long_min": 35, "rsi_long_max": 67, "rsi_short_min": 33, "rsi_short_max": 65,
        "near_ema21_pct": 0.016, "near_ema50_pct": 0.024, "max_spread_pct": 0.003,
        "rel_atr_ratio": 0.9, "atr_hard_floor": 0.0015,
        "bo_vol_mult": 1.2, "bo_max_ext_atr": 3.0,
    },
}
_default_profile = "active" if USE_SANDBOX else "opportunist"
PROFILE_NAME = os.getenv("PROFILE", _default_profile).strip().lower()
PROFILE_WARNING = ""
if PROFILE_NAME not in PROFILES:
    PROFILE_WARNING = f"بروفايل غير معروف '{PROFILE_NAME}' — استُخدم {_default_profile}"
    PROFILE_NAME = _default_profile
if not USE_SANDBOX and PROFILE_NAME == "active":
    PROFILE_WARNING = "البروفايل active ممنوع على الحساب الحقيقي — استُخدم opportunist"
    PROFILE_NAME = "opportunist"


def _cfg(key):
    return env_float(key.upper(), PROFILES[PROFILE_NAME][key])


MIN_ATR_PCT = _cfg("min_atr_pct")
MAX_ATR_PCT = 0.045
MIN_ADX = MIN_ADX_OVERRIDE
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
REL_ATR_RATIO = _cfg("rel_atr_ratio")
ATR_HARD_FLOOR = _cfg("atr_hard_floor")

ENABLE_BREAKOUT = env_flag("ENABLE_BREAKOUT", USE_SANDBOX)
BO_LOOKBACK = int(env_float("BO_LOOKBACK", 20))
BO_VOL_MULT = env_float("BO_VOL_MULT", _entry_defaults["bo_vol"])
BO_MAX_EXT_ATR = _cfg("bo_max_ext_atr")
BO_BODY_POS = env_float("BO_BODY_POS", 0.6)
BO_RSI_LONG_MIN = env_float("BO_RSI_LONG_MIN", 52)
BO_RSI_LONG_MAX = env_float("BO_RSI_LONG_MAX", 72)
BO_RSI_SHORT_MIN = env_float("BO_RSI_SHORT_MIN", 28)
BO_RSI_SHORT_MAX = env_float("BO_RSI_SHORT_MAX", 48)

ENABLE_KER_FILTER = env_flag("ENABLE_KER_FILTER", True)

WATCHDOG_SEC = 300
TF_MS = {"15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}

DATA_DIR = "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else "."
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")
JOURNAL_FILE = os.path.join(DATA_DIR, "trades_journal.csv")
PAPER_FILE = os.path.join(DATA_DIR, "paper_signals.csv")
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
_last_spread = {}
_warned_external = set()
_last_emergency_msg = {}
_lock_fh = None
_send_queue = queue.Queue(maxsize=1000)


# ==========================================
# التشخيص
# ==========================================
def _diag_blank(date=""):
    return {"date": date, "evaluated": 0, "signals": 0, "opened": 0, "signals_by": {},
            "reasons": {}, "reasons_pullback": {}, "reasons_breakout": {}, "blocks": {}}


diag = _diag_blank()
STRAT_LABEL = {"pullback": "الارتداد", "breakout": "الاختراق", "adopted": "متبنّاة", "legacy": "قديمة"}
_last_reason = {}
loop_info = {"last_cycle_ts": 0.0, "cycles": 0, "last_duration": 0.0, "last_error": "", "alerted": False}

REASON_LABELS = {
    "regime_range": "سوق عرضي (Range)",
    "insufficient_data": "بيانات غير كافية",
    "indicators_nan": "مؤشرات غير جاهزة",
    "atr_low": "تذبذب منخفض (ATR)",
    "atr_high": "تذبذب مرتفع جداً",
    "low_ker": "كفاءة اتجاه ضعيفة (KER)",
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
    "bo_no_breakout": "لا اختراق لقمة/قاع 20 شمعة",
    "bo_trend_mismatch": "اتجاه الساعة لا يدعم الاختراق",
    "bo_low_volume": "حجم الاختراق ضعيف",
    "bo_weak_candle": "شمعة الاختراق ضعيفة الإغلاق",
    "bo_rsi": "RSI خارج نطاق الاختراق",
    "bo_extended": "السعر بعيد عن EMA21 (مطاردة)",
    "bo_disabled": "الاختراق معطّل",
    "score_low": "درجة الفرصة أقل من الحد المطلوب",
}


def label(r):
    if r.startswith("signal:"):
        name = r.split(":", 1)[1]
        return f"إشارة ✅ ({STRAT_LABEL.get(name, name)})"
    return REASON_LABELS.get(r, r)


def _diag_roll_locked():
    today = str(datetime.now(KSA_TZ).date())
    if diag["date"] != today:
        diag.clear()
        diag.update(_diag_blank(today))


def note_eval(sym, signal, reasons):
    with diag_lock:
        _diag_roll_locked()
        diag["evaluated"] += 1
        if signal:
            diag["signals"] += 1
            name = signal.get("strategy", "?")
            diag["signals_by"][name] = diag["signals_by"].get(name, 0) + 1
            _last_reason[sym] = (time.time(), f"signal:{name}")
            return
        if "common" in reasons:
            r = reasons["common"]
            diag["reasons"][r] = diag["reasons"].get(r, 0) + 1
            text = label(r)
        else:
            parts = []
            for strat in ("pullback", "breakout"):
                if strat in reasons:
                    r = reasons[strat]
                    key = f"reasons_{strat}"
                    diag[key][r] = diag[key].get(r, 0) + 1
                    parts.append(f"{STRAT_LABEL[strat]}: {label(r)}")
            text = " | ".join(parts)
        _last_reason[sym] = (time.time(), text)


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
        "version": VERSION, "profile": PROFILE_NAME,
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
        "last_notified_sl": None, "regime": "unknown", "strategy": "",
        "planned_entry": None, "realized_fees_est": 0.0, "missing_checks": 0,
        "signal_score": 0,
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
# سجل الصفقات (مع عوامل score الكاملة)
# ==========================================
JOURNAL_FIELDS = [
    "close_time", "symbol", "side", "entry", "exit", "initial_contracts",
    "pnl", "r_multiple", "reason", "regime", "duration_min", "strategy",
    "fees", "fees_source", "stop_slip_usd", "stop_slip_r", "entry_slip_usd",
    "risk_usd", "score", "f_trend", "f_volume", "f_adx", "f_momentum",
    "f_candle", "f_setup", "f_entry_quality", "volume_ratio", "ker",
]


def migrate_journal():
    try:
        with journal_lock:
            if not os.path.exists(JOURNAL_FILE):
                return
            with open(JOURNAL_FILE, newline="", encoding="utf-8") as f:
                header = [h.strip() for h in f.readline().strip().split(",")]
            if header == JOURNAL_FIELDS:
                return
            with open(JOURNAL_FILE, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            for r in rows:
                if not r.get("strategy"):
                    r["strategy"] = "legacy"
            with open(JOURNAL_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_FIELDS, extrasaction="ignore", restval="")
                w.writeheader()
                w.writerows(rows)
            log.info(f"[JOURNAL] تم ترحيل {len(rows)} صفقة إلى صيغة V51")
    except Exception as e:
        log.error(f"[JOURNAL MIGRATE] {e}")


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


# ---------- Paper Mode (تسجيل الإشارات المرفوضة) ----------
PAPER_FIELDS = ["ts", "symbol", "side", "strategy", "score", "price", "sl", "tp", "blocked_by"]


def log_paper_signal(sym, sig, blocked_by):
    if not ENABLE_PAPER_LOG:
        return
    try:
        new = not os.path.exists(PAPER_FILE)
        with journal_lock:
            with open(PAPER_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=PAPER_FIELDS)
                if new:
                    w.writeheader()
                w.writerow({
                    "ts": datetime.now(KSA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": short(sym), "side": sig["side"],
                    "strategy": sig["strategy"], "score": sig.get("score", 0),
                    "price": round(sig["price"], 6),
                    "sl": round(sig["sl"], 6), "tp": round(sig["tp"], 6),
                    "blocked_by": blocked_by,
                })
    except Exception as e:
        log.warning(f"[PAPER] {e}")


def paper_stats_text():
    if not os.path.exists(PAPER_FILE):
        return "📭 لا توجد إشارات مرشحة بعد."
    try:
        with journal_lock:
            df = pd.read_csv(PAPER_FILE)
    except Exception as e:
        return f"خطأ: {e}"
    if df.empty:
        return "📭 لا توجد إشارات مرشحة بعد."
    n = len(df)
    by_block = df["blocked_by"].value_counts().head(5)
    by_score = df.groupby(pd.cut(df["score"], bins=[0, 60, 65, 70, 75, 80, 100])).size()
    lines = [f"📄 <b>إشارات Paper Mode</b> — {n} إشارة", "━━━━━━━━━━━━━━━",
             "<b>حسب سبب الحجب:</b>"]
    for reason, cnt in by_block.items():
        lines.append(f"• {reason}: {cnt}")
    lines += ["", "<b>توزيع الدرجات:</b>"]
    for interval, cnt in by_score.items():
        lines.append(f"• {interval}: {cnt}")
    lines.append("\n💡 راجع PAPER_FILE على /data لتحليل مفصّل")
    return "\n".join(lines)


def _group_line(name, g):
    wins = g[g["pnl"] > 0]
    wr = len(wins) / len(g) * 100
    avg_r = g["r_multiple"].mean() if "r_multiple" in g else 0.0
    return f"• {name}: {len(g)} صفقة | ربح {wr:.0f}% | صافي {g['pnl'].sum():+.2f} | R {avg_r:+.2f}"


def _num(df, col):
    if col not in df:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[col], errors="coerce")


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
    if "strategy" not in df:
        df["strategy"] = "legacy"
    df["strategy"] = df["strategy"].fillna("legacy")
    df["pnl"] = pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0)
    df["r_multiple"] = _num(df, "r_multiple")
    n = len(df)
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    gross_win = wins["pnl"].sum()
    gross_loss = abs(losses["pnl"].sum())
    pf = (gross_win / gross_loss) if gross_loss > 0 else float("inf")
    curve = df["pnl"].cumsum()
    max_dd = (curve.cummax() - curve).max()
    avg_r = df["r_multiple"].mean()
    wr = len(wins) / n * 100
    avg_win = wins["pnl"].mean() if len(wins) else 0.0
    avg_loss = losses["pnl"].mean() if len(losses) else 0.0
    pf_txt = "∞" if pf == float("inf") else f"{pf:.2f}"
    lines = [f"📈 <b>إحصائيات الأداء</b>", "━━━━━━━━━━━━━━━",
             f"🔢 الصفقات: {n}", f"✅ نسبة الربح: {wr:.1f}%",
             f"💰 الصافي: <b>{df['pnl'].sum():+.2f} USDT</b>",
             f"⚖️ PF: {pf_txt}", f"📐 متوسط R: {avg_r:+.2f}",
             f"🟢 متوسط الربح: {avg_win:+.2f} | 🔴 متوسط الخسارة: {avg_loss:+.2f}",
             f"📉 أقصى تراجع: {max_dd:.2f} USDT", "", "<b>حسب الاستراتيجية:</b>"]
    for name, g in df.groupby("strategy"):
        lines.append(_group_line(STRAT_LABEL.get(name, name), g))
    if "side" in df:
        lines += ["", "<b>حسب الاتجاه:</b>"]
        for name, g in df.groupby("side"):
            lines.append(_group_line(name, g))
    score = _num(df, "score")
    if score.notna().any():
        lines += ["", "<b>حسب درجة الفرصة:</b>"]
        # إصلاح bucket 74-79 (كان 0-79 خطأ)
        for lo, hi, name in ((60, 67, "60–67"), (68, 73, "68–73"),
                             (74, 79, "74–79"), (80, 89, "80–89"), (90, 100, "90+")):
            g = df[(score >= lo) & (score <= hi)]
            if len(g):
                lines.append(_group_line(name, g))
    fees = _num(df, "fees").dropna()
    stop_usd = _num(df, "stop_slip_usd").dropna()
    stop_r = _num(df, "stop_slip_r").dropna()
    entry_slip = _num(df, "entry_slip_usd").dropna()
    n_actual = int((df["fees_source"] == "actual").sum()) if "fees_source" in df else 0
    if len(fees) or len(stop_usd) or len(entry_slip):
        lines += ["", "<b>تكلفة التنفيذ:</b>"]
        if len(fees):
            lines.append(f"🧾 الرسوم: {fees.sum():.2f} USDT ({n_actual} فعلية)")
        if len(stop_usd):
            lines.append(f"⚡ انزلاق الوقف: متوسط {stop_usd.mean():.2f} ({stop_r.mean():.2f}R)")
        if len(entry_slip):
            lines.append(f"↘️ انزلاق الدخول: إجمالي {entry_slip.sum():.2f}")
    if n < 30:
        lines += ["", f"⚠️ العينة صغيرة ({n}) — لا تعتمد قبل 30 صفقة."]
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ==========================================
# تحليل AI (تحليل فقط — لا يتدخل في التنفيذ)
# ==========================================
def local_ai_analysis(df):
    n = len(df)
    if n == 0:
        return "📭 لا يوجد سجل صفقات."
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    wr = len(wins) / n * 100
    avg_win = wins["pnl"].mean() if len(wins) else 0
    avg_loss = losses["pnl"].mean() if len(losses) else 0
    best_pair = df.groupby("symbol")["pnl"].sum().idxmax() if n > 0 else "—"
    worst_pair = df.groupby("symbol")["pnl"].sum().idxmin() if n > 0 else "—"
    best_strat = df.groupby("strategy")["pnl"].sum().idxmax() if "strategy" in df and n > 0 else "—"
    lines = [
        "🧠 <b>تحليل محلي (بدون AI API):</b>",
        f"• نسبة الربح: {wr:.1f}%",
        f"• متوسط الربح: {avg_win:+.2f} | متوسط الخسارة: {avg_loss:+.2f}",
        f"• أفضل عملة: {best_pair} | أسوأ: {worst_pair}",
        f"• أفضل استراتيجية: {best_strat}",
    ]
    if avg_win > 0 and avg_loss < 0:
        rr = avg_win / abs(avg_loss)
        lines.append(f"• R/R: {rr:.2f}")
        if wr < 50 and rr < 1.5:
            lines.append("⚠️ نسبة ربح و R/R منخفضان — عدّل الأهداف")
        elif wr > 50 and rr < 1:
            lines.append("💡 نسبة ربح جيدة لكن R/R منخفض — ارفع الهدف")
    try:
        df["hour"] = pd.to_datetime(df["close_time"]).dt.hour
        best_hour = df.groupby("hour")["pnl"].sum().idxmax()
        lines.append(f"• أفضل ساعة: {best_hour}:00")
    except Exception:
        pass
    return "\n".join(lines)


def ai_analyze_trades(chat_id):
    if not os.path.exists(JOURNAL_FILE):
        return send("📭 لا يوجد سجل للتحليل.", chat_id)
    try:
        with journal_lock:
            df = pd.read_csv(JOURNAL_FILE)
        if df.empty:
            return send("📭 السجل فارغ.", chat_id)
    except Exception as e:
        return send(f"خطأ: {e}", chat_id)

    send("⏳ جاري تحليل السجل...", chat_id)

    if not AI_API_KEY:
        return send(local_ai_analysis(df), chat_id)

    total_pnl = df["pnl"].sum()
    wr = (df["pnl"] > 0).mean() * 100
    recent = df.tail(15)[["symbol", "side", "strategy", "pnl", "r_multiple", "reason"]].to_csv(index=False)

    prompt = f"""أنت محلل أداء تداول كمي. حلل سجل البوت:
1. 3 أنماط واضحة (هل البوت يخسر في استراتيجية؟ وقت؟ عملة؟).
2. تقييم المخاطر (R/R، سلاسل الخسائر).
3. توصيتان قابلتان للتنفيذ.
إجمالي: {len(df)} | WR: {wr:.1f}% | صافي: {total_pnl:.2f} USDT
آخر 15 صفقة:
{recent}
بالعربية، مختصر، نقاط واضحة."""

    try:
        headers = {"Authorization": f"Bearer {AI_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": AI_MODEL, "messages": [{"role": "user", "content": prompt}],
                   "temperature": 0.3}
        r = requests.post(AI_GATEWAY_URL, json=payload, headers=headers, timeout=45)
        r.raise_for_status()
        ai_text = r.json()["choices"][0]["message"]["content"]
        send(f"🧠 <b>تحليل AI:</b>\n\n{ai_text}", chat_id)
    except Exception as e:
        log.error(f"[AI] {e}")
        send(f"⚠️ فشل AI: {e}\n\n{local_ai_analysis(df)}", chat_id)


def backup_to_telegram(chat_id=None):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS:
        return
    targets = [chat_id] if chat_id else list(AUTHORIZED_CHATS)
    for cid in targets:
        for filepath in [JOURNAL_FILE, STATE_FILE]:
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'rb') as f:
                        requests.post(
                            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                            data={"chat_id": cid, "caption": f"🔒 {os.path.basename(filepath)}"},
                            files={"document": f}, timeout=20)
                except Exception as e:
                    log.error(f"[BACKUP] {filepath}: {e}")


def handle_document_restore(msg, chat_id):
    doc = msg.get("document")
    if not doc:
        return
    file_name = doc.get("file_name", "")
    if file_name not in ("trades_journal.csv", "bot_state.json"):
        return send("❌ الملف غير معروف.", chat_id)
    file_id = doc.get("file_id")
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile?file_id={file_id}").json()
        file_path = r["result"]["file_path"]
        dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        target = JOURNAL_FILE if file_name == "trades_journal.csv" else STATE_FILE
        with requests.get(dl_url, stream=True) as r_dl:
            with open(target, 'wb') as f:
                for chunk in r_dl.iter_content(chunk_size=8192):
                    f.write(chunk)
        send(f"✅ تم استعادة {file_name}.", chat_id)
        if file_name == "bot_state.json":
            load_state()
    except Exception as e:
        send(f"❌ فشل: {e}", chat_id)


# ==========================================
# تيليجرام (queue غير محجوب)
# ==========================================
def send(msg, chat_id=None):
    try:
        _send_queue.put_nowait((msg, chat_id))
    except queue.Full:
        log.warning("[TG QUEUE] full — dropped")


def _send_worker():
    while True:
        try:
            msg, chat_id = _send_queue.get()
            if not TELEGRAM_BOT_TOKEN:
                continue
            targets = [chat_id] if chat_id else list(AUTHORIZED_CHATS)
            for cid in targets:
                try:
                    requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                        timeout=10)
                except Exception as e:
                    log.warning(f"[TG SEND] {e}")
        except Exception as e:
            log.error(f"[TG WORKER] {e}")


def fmt(p):
    if p is None or (isinstance(p, float) and np.isnan(p)):
        return "0.00"
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"


def short(sym):
    return sym.split(":")[0]


REASONS = {
    "Take Profit": "تحقق الهدف 🎯",
    "Stop Loss": "ضرب وقف الخسارة 🛑",
    "Breakeven Hit": "خروج آمن 🛡️",
    "Trailing Stop": "وقف متحرك 📈",
    "Manual": "إغلاق يدوي 🖐️",
    "Emergency": "إغلاق طارئ 🚨",
    "Manual/External": "إغلاق من المنصة ⚡",
}


def effective_risk_pct(equity, sl_pct):
    if equity <= 0 or sl_pct <= 0:
        return 0.0
    risk_usd = equity * RISK_PCT_PER_TRADE / 100.0
    margin = risk_usd / (sl_pct * LEVERAGE)
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    margin = min(cap, margin)
    return margin * sl_pct * LEVERAGE / equity * 100.0


def msg_entry(sym, side, price, sl, tp, margin, real_order, regime="", strategy="", equity=None, score=0):
    title = "🟢 <b>شراء (LONG)</b>" if side == "LONG" else "🔴 <b>بيع (SHORT)</b>"
    status = "✅ <b>تم الفتح</b>" if real_order else "⚠️ <b>إشارة فقط</b>"
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * LEVERAGE
    reward_usd = margin * (tp_pct / 100) * LEVERAGE
    risk_pct_txt = ""
    if equity and equity > 0:
        pct = risk_usd / equity * 100
        risk_pct_txt = f" = {pct:.2f}% من الرصيد"
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    regime_txt = f"\n📡 <b>النظام:</b> {regime}" if regime else ""
    if strategy:
        regime_txt += f"\n🧠 <b>الاستراتيجية:</b> {STRAT_LABEL.get(strategy, strategy)}"
    score_txt = f"\n🎯 <b>الدرجة:</b> {score}/100" if score else ""
    return (f"{title}\n{status}{regime_txt}{score_txt}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <code>{short(sym)}</code> | 💵 <code>{fmt(price)}</code>\n"
            f"⚙️ {LEVERAGE}x | الهامش: {margin:.2f} USDT\n"
            f"🛑 SL: <code>{fmt(sl)}</code> ({sl_pct:.2f}%) → مخاطرة {risk_usd:.2f}{risk_pct_txt}\n"
            f"🎯 TP: <code>{fmt(tp)}</code> ({tp_pct:.2f}%) → عائد +{reward_usd:.2f}\n"
            f"⏰ {now_str}")


def msg_exit(sym, side, exit_price, entry_price, total_pnl, final_leg_pnl, margin, r_mult,
             reason, strategy="", fees=None, fees_src="", stop_slip_usd=None, stop_slip_r=None):
    pct = (total_pnl / margin * 100) if margin else 0.0
    if total_pnl > 0:
        title = "🟢 <b>ربح</b>"
    elif total_pnl < 0:
        title = "🔴 <b>خسارة</b>"
    else:
        title = "⚪ <b>متعادل</b>"
    extra = ""
    if fees is not None:
        extra += f"🧾 رسوم: {fees:.2f} ({'فعلية' if fees_src == 'actual' else 'تقديرية'})\n"
    if stop_slip_usd is not None and stop_slip_usd >= 0:
        extra += f"⚡ انزلاق وقف: {stop_slip_usd:.2f} USDT ({stop_slip_r:.2f}R)\n"
    return (f"{title}\n━━━━━━━━━━━━━━━\n"
            f"🪙 <code>{short(sym)}</code> ({side})\n"
            f"📥 {fmt(entry_price)} → 📤 {fmt(exit_price)}\n"
            f"📊 هامش: {pct:+.2f}% | R: {r_mult:+.2f}\n"
            f"💰 صافي: <b>{total_pnl:+.2f} USDT</b>\n"
            f"📝 {REASONS.get(reason, reason)} | {STRAT_LABEL.get(strategy, strategy or '—')}\n"
            f"{extra}━━━━━━━━━━━━━━━")


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
    return pd.concat([high - low, (high - close.shift()).abs(),
                      (low - close.shift()).abs()], axis=1).max(axis=1)


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


def efficiency_ratio(close_series, period=10):
    """KER: صافي الحركة ÷ مجموع الحركة خلال الفترة."""
    change = (close_series - close_series.shift(period)).abs()
    volatility = (close_series - close_series.shift(1)).abs().rolling(window=period).sum()
    return change / (volatility + 1e-9)


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
    if equity <= 0:
        return 0.0
    if not USE_RISK_SIZING:
        return float(POSITION_SIZE_USDT)
    risk_usd = equity * RISK_PCT_PER_TRADE / 100.0
    margin = risk_usd / (sl_pct * LEVERAGE)
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    sized = min(cap, margin)
    return sized if sized >= MIN_MARGIN_USDT else 0.0


def compute_margin_scored(equity, sl_pct, signal_score):
    """حجم الصفقة يتناسب مع جودة الفرصة — يسمح بصفقات أكثر بمخاطرة أقل لكل واحدة."""
    base = compute_margin(equity, sl_pct)
    if base <= 0:
        return 0.0
    if signal_score >= 86:
        mult = 1.00
    elif signal_score >= 78:
        mult = 0.85
    elif signal_score >= 70:
        mult = 0.70
    elif signal_score >= 62:
        mult = 0.55
    else:
        mult = 0.40
    return base * mult


def get_open_position(ex, sym):
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
    req = {'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': side,
           'posSide': 'net', 'ordType': 'conditional', 'sz': amt_str, 'reduceOnly': 'true'}
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
    sl_ok = tp_ok = False
    if sl is not None:
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
                if "51277" in str(e) or "51276" in str(e):
                    tp_ok = True
                    break
                log.error(f"[TP ERR] {sym} try {i+1}: {e}")
                time.sleep(0.8)
    return sl_ok, tp_ok


def replace_protection(ex, sym, close_side, contracts, sl, tp=None):
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
        for _ in range(6):
            time.sleep(0.8)
            if get_open_position(ex, sym) is None:
                cancel_pending(ex, sym)
                return True
        log.error(f"[CLOSE FULL] {sym}: المركز ما زال ظاهراً")
        return False
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
            return False, f"رافعة: {err[:120]}", price, 0.0
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'
        order = ex.create_order(sym, 'market', order_side, amount, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net'})
        # تأكيد ظهور المركز
        pos = None
        for _ in range(7):
            time.sleep(0.7)
            try:
                pos = get_open_position(ex, sym)
            except Exception:
                pos = None
            if pos:
                break
        if not pos:
            log.error(f"[OPEN UNCONFIRMED] {sym} (order={order.get('id')})")
            return False, "لم يتأكد التنفيذ — لم تُوضع حماية", price, 0.0
        entry = pos["entry"] or price
        contracts = pos["contracts"]
        sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, sl, tp)
        if not sl_ok:
            send(f"🚨 فشل SL <code>{short(sym)}</code> — إغلاق")
            if not close_full(ex, sym):
                send(f"🆘 فشل الإغلاق! تدخل يدوي.")
            return False, "فشل SL", entry, 0.0
        if not tp_ok:
            send(f"⚠️ TP فشل <code>{short(sym)}</code>")
        return True, order.get('id'), entry, contracts
    except Exception as e:
        log.error(f"[OPEN ERR] {sym}: {e}")
        return False, str(e)[:150], price, 0.0


# ==========================================
# الربح وإنهاء الصفقة
# ==========================================
def leg_fee_est(entry, exit_price, contracts, cs):
    return (entry + exit_price) * contracts * cs * FEE_RATE


def leg_pnl(side, entry, exit_price, contracts, cs):
    d = 1 if side == "LONG" else -1
    gross = (exit_price - entry) * d * contracts * cs
    return gross - leg_fee_est(entry, exit_price, contracts, cs)


def get_actual_fees(ex, sym, pos_data):
    if (pos_data.get("strategy") or "") == "adopted" or not pos_data.get("opened_ts"):
        return None
    try:
        since = int(pos_data["opened_ts"] * 1000) - 30000
        trades = ex.fetch_my_trades(sym, since=since, limit=100)
        total, n = 0.0, 0
        for t in trades:
            fee = t.get("fee") or {}
            cost = fee.get("cost")
            if cost is None:
                continue
            cur = (fee.get("currency") or "").upper()
            if cur and cur not in ("USDT", "USDC"):
                continue
            total += abs(float(cost))
            n += 1
        return total if n > 0 else None
    except Exception as e:
        log.warning(f"[FEES] {sym}: {e}")
        return None


def get_exit_price(ex, sym, pos_data):
    try:
        close_side = 'sell' if pos_data["position"] == "LONG" else 'buy'
        if pos_data.get("partial_1_taken") or pos_data.get("partial_2_taken"):
            since = int((pos_data.get("leg_ts") or 0) * 1000) + 1500
        else:
            since = int((pos_data.get("opened_ts") or 0) * 1000) - 8000
        trades = ex.fetch_my_trades(sym, since=since, limit=80)
        trades = [t for t in trades if not t.get("timestamp") or t["timestamp"] >= since]
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
    d = 1 if side == "LONG" else -1

    # كشف الصفقة الوهمية
    age = time.time() - (pos_data.get("opened_ts") or time.time())
    if age < GHOST_MAX_AGE_SEC and (pos_data.get("strategy") or "") != "adopted":
        try:
            fills = ex.fetch_my_trades(sym, since=int(pos_data["opened_ts"] * 1000) - 30000, limit=50)
            if fills is not None and len(fills) == 0:
                log.error(f"[GHOST] {sym}: لا تنفيذات ({age:.0f}ث) — صفقة وهمية")
                reset_sym_state(sym, cooldown=True)
                cancel_pending(ex, sym)
                send(f"⚠️ صفقة وهمية مُهملة <code>{short(sym)}</code>")
                return
        except Exception as e:
            log.warning(f"[GHOST CHECK] {sym}: {e}")

    exit_price = get_exit_price(ex, sym, pos_data) or get_current_price(ex, sym, 0) or entry
    cs = contract_size(ex, sym)
    contracts = pos_data.get("contracts", 0.0)
    realized = pos_data.get("realized_pnl", 0.0)
    realized_fees_est = pos_data.get("realized_fees_est", 0.0)

    gross_final = (exit_price - entry) * d * contracts * cs
    fee_est_final = leg_fee_est(entry, exit_price, contracts, cs)
    fees_est_total = realized_fees_est + fee_est_final
    fees_actual = get_actual_fees(ex, sym, pos_data)
    use_actual = (fees_actual is not None and fees_est_total > 0 and
                  0.2 * fees_est_total <= fees_actual <= 5 * fees_est_total)
    if use_actual:
        total = realized + realized_fees_est + gross_final - fees_actual
        fees_total, fees_src = fees_actual, "actual"
    else:
        total = realized + gross_final - fee_est_final
        fees_total, fees_src = fees_est_total, "est"
    final_leg = total - realized

    margin = pos_data.get("margin") or POSITION_SIZE_USDT
    reason = reason_hint or detect_reason(pos_data, exit_price)
    init_risk = pos_data.get("initial_risk", 0.0)
    risk_usd = init_risk * pos_data.get("initial_contracts", 0.0) * cs
    r_mult = (total / risk_usd) if risk_usd > 0 else 0.0
    duration = int((time.time() - (pos_data.get("opened_ts") or time.time())) / 60)

    stop_slip_usd = stop_slip_r = None
    if reason in ("Stop Loss", "Trailing Stop", "Breakeven Hit") and pos_data.get("sl"):
        adverse = (pos_data["sl"] - exit_price) if side == "LONG" else (exit_price - pos_data["sl"])
        stop_slip_usd = adverse * contracts * cs
        stop_slip_r = (adverse / init_risk) if init_risk else 0.0
    entry_slip_usd = None
    pe = pos_data.get("planned_entry")
    if pe:
        adverse_e = (entry - pe) if side == "LONG" else (pe - entry)
        entry_slip_usd = adverse_e * pos_data.get("initial_contracts", 0.0) * cs

    factors = pos_data.get("score_factors", {}) or {}

    add_daily_pnl(final_leg)
    register_trade_result(total)
    journal_append({
        "close_time": datetime.now(KSA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": short(sym), "side": side, "entry": entry, "exit": exit_price,
        "initial_contracts": pos_data.get("initial_contracts", 0.0),
        "pnl": round(total, 4), "r_multiple": round(r_mult, 3), "reason": reason,
        "regime": pos_data.get("regime", ""), "duration_min": duration,
        "strategy": pos_data.get("strategy") or "legacy",
        "fees": round(fees_total, 4), "fees_source": fees_src,
        "stop_slip_usd": "" if stop_slip_usd is None else round(stop_slip_usd, 4),
        "stop_slip_r": "" if stop_slip_r is None else round(stop_slip_r, 3),
        "entry_slip_usd": "" if entry_slip_usd is None else round(entry_slip_usd, 4),
        "risk_usd": round(risk_usd, 4),
        "score": pos_data.get("signal_score", 0),
        "f_trend": factors.get("trend", ""),
        "f_volume": factors.get("volume", ""),
        "f_adx": factors.get("adx", ""),
        "f_momentum": factors.get("momentum", ""),
        "f_candle": factors.get("candle", ""),
        "f_setup": factors.get("setup", ""),
        "f_entry_quality": factors.get("entry_quality", ""),
        "volume_ratio": pos_data.get("volume_ratio", ""),
        "ker": pos_data.get("ker", ""),
    })
    reset_sym_state(sym, cooldown=True)
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, total, final_leg, margin, r_mult, reason,
                  pos_data.get("strategy", ""), fees_total, fees_src, stop_slip_usd, stop_slip_r))


def emergency_close(ex, sym, why):
    now = time.time()
    if now - _last_emergency_msg.get((sym, "start"), 0) > 120:
        _last_emergency_msg[(sym, "start")] = now
        send(f"🚨 إغلاق طارئ <code>{short(sym)}</code>\n{why}")
    if close_full(ex, sym):
        finalize_trade(ex, sym, reason_hint="Emergency")
    else:
        if now - _last_emergency_msg.get((sym, "fail"), 0) > 120:
            _last_emergency_msg[(sym, "fail")] = now
            send(f"🆘 فشل الإغلاق <code>{short(sym)}</code> — يُعاد.")


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
# الاستراتيجية
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


def relative_atr(df):
    try:
        series = (df["atr"] / df["close"]).iloc[-102:-2].dropna()
        if len(series) < 60:
            return None
        base = float(series.median())
        cur = float(df["atr"].iloc[-2] / df["close"].iloc[-2])
        return cur / base if base > 0 else None
    except Exception:
        return None


def build_context(df_15m, df_1h, df_4h):
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
    adx_val = float(last["adx"])

    regime = detect_regime(df_15m, df_1h, df_4h)
    if regime == "unknown":
        return None, "insufficient_data"
    if regime == "range":
        return None, "regime_range"
    atr_pct = atr_val / price
    if atr_pct > MAX_ATR_PCT:
        return None, "atr_high"
    rel = relative_atr(df_15m)
    atr_ok = atr_pct >= MIN_ATR_PCT or (rel is not None and atr_pct >= ATR_HARD_FLOOR and rel >= REL_ATR_RATIO)
    if not atr_ok:
        return None, "atr_low"
    if adx_val < MIN_ADX:
        return None, "low_adx"

    # KER متكيف (median + regime-aware)
    ker_value = None
    if ENABLE_KER_FILTER:
        er_series = efficiency_ratio(df_15m["close"], KER_PERIOD)
        ker_recent = er_series.iloc[-4:-1].dropna()
        ker_smoothed = float(ker_recent.median()) if len(ker_recent) >= 2 else 0.0
        min_ker_eff = MIN_KER * 0.55 if regime in ("strong_trend_up", "strong_trend_down") else MIN_KER
        if ker_smoothed < min_ker_eff:
            return None, "low_ker"
        ker_value = ker_smoothed
    else:
        er_series = efficiency_ratio(df_15m["close"], KER_PERIOD)
        ker_value = float(er_series.iloc[-2]) if not pd.isna(er_series.iloc[-2]) else None

    long_dir = regime in ("strong_trend_up", "weak_trend") and last_4h["close"] > last_4h["ema50"]
    short_dir = regime in ("strong_trend_down", "weak_trend") and last_4h["close"] < last_4h["ema50"]
    if not long_dir and not short_dir:
        return None, "direction_mismatch"

    sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * atr_val / price))
    return {
        "last": last, "prev": prev, "last_1h": last_1h, "last_4h": last_4h,
        "price": price, "atr_val": atr_val, "ema21": float(last["ema21"]),
        "ema50": float(last["ema50"]), "rsi": float(last["rsi"]),
        "regime": regime, "long_dir": long_dir, "short_dir": short_dir,
        "ker": ker_value, "sl_pct": sl_pct, "tp_pct": sl_pct * RR_RATIO,
    }, None


def _make_signal(side, ctx, strategy):
    p, sp, tp_pct = ctx["price"], ctx["sl_pct"], ctx["tp_pct"]
    if side == "LONG":
        sl, tp = p * (1 - sp), p * (1 + tp_pct)
    else:
        sl, tp = p * (1 + sp), p * (1 - tp_pct)
    return {"side": side, "price": p, "sl_pct": sp, "sl": sl, "tp": tp,
            "regime": ctx["regime"], "strategy": strategy, "ker": ctx.get("ker")}


def pullback_signal(ctx):
    last, prev, last_1h = ctx["last"], ctx["prev"], ctx["last_1h"]
    price, ema21, ema50, rsi_val = ctx["price"], ctx["ema21"], ctx["ema50"], ctx["rsi"]
    if not (last["volume"] > MIN_VOL_MULT * last["vol_ma"]):
        return None, "low_volume"
    near_ema = (abs(price - ema21) / price < NEAR_EMA21_PCT or
                abs(price - ema50) / price < NEAR_EMA50_PCT)
    if ctx["long_dir"]:
        bullish_reject = last["close"] > last["open"] and (last["low"] < ema21 or prev["low"] < ema21)
        if not (last["ema9"] > last["ema21"] and last_1h["ema9"] > last_1h["ema21"]):
            return None, "ema_misaligned"
        if not near_ema:
            return None, "far_from_ema"
        if not (RSI_LONG_MIN < rsi_val < RSI_LONG_MAX):
            return None, "rsi_out_of_range"
        if not bullish_reject:
            return None, "no_pullback_candle"
        return _make_signal("LONG", ctx, "pullback"), None
    bearish_reject = last["close"] < last["open"] and (last["high"] > ema21 or prev["high"] > ema21)
    if not (last["ema9"] < last["ema21"] and last_1h["ema9"] < last_1h["ema21"]):
        return None, "ema_misaligned"
    if not near_ema:
        return None, "far_from_ema"
    if not (RSI_SHORT_MIN < rsi_val < RSI_SHORT_MAX):
        return None, "rsi_out_of_range"
    if not bearish_reject:
        return None, "no_pullback_candle"
    return _make_signal("SHORT", ctx, "pullback"), None


def breakout_signal(ctx, df_15m):
    last, last_1h = ctx["last"], ctx["last_1h"]
    window = df_15m.iloc[-(BO_LOOKBACK + 2):-2]
    hh, ll = float(window["high"].max()), float(window["low"].min())
    close, high, low = float(last["close"]), float(last["high"]), float(last["low"])
    rng = high - low
    vol_ok = last["volume"] > BO_VOL_MULT * last["vol_ma"]
    if ctx["long_dir"]:
        if not close > hh:
            return None, "bo_no_breakout"
        if not (last_1h["ema9"] > last_1h["ema21"] and last_1h["close"] > last_1h["ema50"]):
            return None, "bo_trend_mismatch"
        if not vol_ok:
            return None, "bo_low_volume"
        if rng <= 0 or (close - low) / rng < BO_BODY_POS:
            return None, "bo_weak_candle"
        if not (BO_RSI_LONG_MIN < ctx["rsi"] < BO_RSI_LONG_MAX):
            return None, "bo_rsi"
        if close - ctx["ema21"] > BO_MAX_EXT_ATR * ctx["atr_val"]:
            return None, "bo_extended"
        return _make_signal("LONG", ctx, "breakout"), None
    if not close < ll:
        return None, "bo_no_breakout"
    if not (last_1h["ema9"] < last_1h["ema21"] and last_1h["close"] < last_1h["ema50"]):
        return None, "bo_trend_mismatch"
    if not vol_ok:
        return None, "bo_low_volume"
    if rng <= 0 or (high - close) / rng < BO_BODY_POS:
        return None, "bo_weak_candle"
    if not (BO_RSI_SHORT_MIN < ctx["rsi"] < BO_RSI_SHORT_MAX):
        return None, "bo_rsi"
    if ctx["ema21"] - close > BO_MAX_EXT_ATR * ctx["atr_val"]:
        return None, "bo_extended"
    return _make_signal("SHORT", ctx, "breakout"), None


def score_signal(signal, ctx, df_15m):
    last, h1, h4 = ctx["last"], ctx["last_1h"], ctx["last_4h"]
    side = signal["side"]
    score, factors = 0, {}

    aligned = ((side == "LONG" and h1["ema9"] > h1["ema21"] and h4["close"] > h4["ema50"])
               or (side == "SHORT" and h1["ema9"] < h1["ema21"] and h4["close"] < h4["ema50"]))
    factors["trend"] = 20 if aligned else 6
    score += factors["trend"]

    vol_ratio = float(last["volume"] / last["vol_ma"]) if last["vol_ma"] else 0.0
    factors["volume"] = min(15, max(0, int(7.5 * vol_ratio)))
    score += factors["volume"]

    adx_value = float(last["adx"])
    factors["adx"] = min(15, max(0, int((adx_value - 12) * 0.75)))
    score += factors["adx"]

    r = float(last["rsi"])
    if side == "LONG":
        momentum = 15 if 52 <= r <= 66 else (10 if 47 <= r < 52 or 66 < r <= 70 else 4)
    else:
        momentum = 15 if 34 <= r <= 48 else (10 if 30 <= r < 34 or 48 < r <= 53 else 4)
    factors["momentum"] = momentum
    score += momentum

    rng = max(float(last["high"] - last["low"]), 1e-12)
    body = abs(float(last["close"] - last["open"])) / rng
    factors["candle"] = min(10, int(body * 10))
    score += factors["candle"]

    # إصلاح: الارتداد المثالي يحصل على 15 (كان 12)
    if signal.get("strategy") == "breakout":
        factors["setup"] = 15
    elif signal.get("strategy") == "pullback":
        ideal_rsi = (52 <= r <= 62) if side == "LONG" else (38 <= r <= 48)
        strong_candle = factors.get("candle", 0) >= 6
        factors["setup"] = 15 if (ideal_rsi and strong_candle) else 13
    else:
        factors["setup"] = 12
    score += factors["setup"]

    extension = abs(float(last["close"]) - float(last["ema21"])) / max(float(last["atr"]), 1e-12)
    factors["entry_quality"] = 10 if extension <= 0.8 else (7 if extension <= 1.4 else 3)
    score += factors["entry_quality"]

    signal["score"] = min(100, int(score))
    signal["score_factors"] = factors
    signal["volume_ratio"] = round(vol_ratio, 2)
    return signal


def evaluate_signal(df_15m, df_1h, df_4h):
    """تقييم الاستراتيجيتين معاً واختيار الأعلى — لا if/elif."""
    ctx, reason = build_context(df_15m, df_1h, df_4h)
    if ctx is None:
        return None, {"common": reason}

    candidates = []
    sig_pull, r_pull = pullback_signal(ctx)
    if sig_pull:
        candidates.append(sig_pull)

    r_bo = "bo_disabled"
    if ENABLE_BREAKOUT:
        sig_bo, r_bo = breakout_signal(ctx, df_15m)
        if sig_bo:
            candidates.append(sig_bo)

    if not candidates:
        reasons = {"pullback": r_pull}
        if ENABLE_BREAKOUT:
            reasons["breakout"] = r_bo
        return None, reasons

    for s in candidates:
        score_signal(s, ctx, df_15m)
    sig = max(candidates, key=lambda x: x.get("score", 0))
    if sig["score"] < MIN_SIGNAL_SCORE:
        return None, {"common": "score_low"}
    return sig, {}


def market_filters_ok(ex, sym, side):
    try:
        t = ex.fetch_ticker(sym)
        bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
        if bid and ask and last:
            spread = (ask - bid) / last
            _last_spread[sym] = (time.time(), spread)
            if spread > MAX_SPREAD_PCT:
                return False, "spread_high"
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
    except Exception:
        pass
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
        send(f"{title}\n🪙 <code>{short(sym)}</code>\n🛑 <code>{fmt(new_sl)}</code>")
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
        order = ex.create_order(sym, 'market', close_side, amt, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
    except Exception as e:
        log.error(f"[PARTIAL ORDER] {sym}: {e}")
        return False
    time.sleep(0.8)
    try:
        after = get_open_position(ex, sym)
        remaining = after["contracts"] if after else 0.0
        actual_closed = max(0.0, contracts - remaining)
        if actual_closed < mn or remaining < mn:
            return False
    except Exception as e:
        log.warning(f"[PARTIAL POS FETCH] {sym}: {e}")
        return False
    fill_ts = time.time()
    exit_price = order.get("average") or order.get("price")
    try:
        if not exit_price and order.get("id"):
            fetched = ex.fetch_order(order["id"], sym)
            exit_price = fetched.get("average") or fetched.get("price")
    except Exception:
        pass
    exit_price = float(exit_price or get_current_price(ex, sym, 0) or entry)
    cs_leg = contract_size(ex, sym)
    pnl = leg_pnl(side, entry, exit_price, actual_closed, cs_leg)
    fee_est_leg = leg_fee_est(entry, exit_price, actual_closed, cs_leg)
    new_sl = breakeven_price(side, entry) if which == 1 else st["sl"]
    protected = replace_protection(ex, sym, close_side, remaining, new_sl, st.get("tp"))
    with state_lock:
        s = state[sym]
        s["contracts"] = remaining
        s[f"partial_{which}_taken"] = True
        s["realized_pnl"] = s.get("realized_pnl", 0.0) + pnl
        s["realized_fees_est"] = s.get("realized_fees_est", 0.0) + fee_est_leg
        s["leg_ts"] = fill_ts
        if which == 1 and protected:
            s["sl"] = new_sl
            s["breakeven_hit"] = True
    add_daily_pnl(pnl)
    save_state()
    pct_txt = f"{int(PARTIAL1_PCT * 100)}%" if which == 1 else f"{int(PARTIAL2_PCT * 100)}% متبقٍ"
    extra = "وقف للتعادل" if (which == 1 and protected) else "قفل أرباح"
    warn = "" if protected else "\n⚠️ فشل تحديث الحماية."
    send(f"💰 جزئي {which} ({pct_txt}) <code>{short(sym)}</code>\nربح: {pnl:+.2f} | {extra}{warn}")
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
        return
    if not has_sl:
        sl_px = st.get("sl")
        breached = bool(sl_px) and ((side == "LONG" and price <= sl_px) or (side == "SHORT" and price >= sl_px))
        if breached:
            emergency_close(ex, sym, "السعر تجاوز الوقف ولم يُنفذ — إغلاق فوري")
            return
        ok = replace_protection(ex, sym, close_side, st["contracts"], st["sl"], st.get("tp"))
        if not ok:
            emergency_close(ex, sym, "بدون وقف خسارة")
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
            move_sl(ex, sym, st, breakeven_price(side, entry), "🛡️ Breakeven")
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
            move_sl(ex, sym, st, new_sl, "📈 وقف متحرك")
    else:
        new_sl = min(price + mult * float(atr_val), be)
        if new_sl < sl * 0.999 and new_sl > price:
            move_sl(ex, sym, st, new_sl, "📈 وقف متحرك")


# ==========================================
# السماح بالدخول
# ==========================================
def open_positions_snapshot():
    with state_lock:
        return {k: dict(v) for k, v in state.items() if v.get("position")}


def floating_pnl_cached(ex):
    total = 0.0
    for sym, v in open_positions_snapshot().items():
        c = _price_cache.get(sym)
        price = c[1] if c else v["entry"]
        d = 1 if v["position"] == "LONG" else -1
        total += (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
    return total


def entries_allowed(ex):
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
            send("⛔ حد الخسارة اليومي")
        return False, "daily_loss_limit"
    if DAILY_PROFIT_TARGET_PCT > 0 and snap["value"] >= equity * DAILY_PROFIT_TARGET_PCT / 100.0:
        if meta.get("profit_alert") != snap["date"]:
            set_meta("profit_alert", snap["date"])
            send("✅ الهدف اليومي تحقق — حماية الأرباح")
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
                    with state_lock:
                        state[sym]["missing_checks"] = state[sym].get("missing_checks", 0) + 1
                        missing = state[sym]["missing_checks"]
                    if missing < MISSING_CONFIRMATIONS:
                        return
                    finalize_trade(ex, sym)
                    return
                with state_lock:
                    state[sym]["missing_checks"] = 0
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

            signal, reasons = evaluate_signal(to_df(raw_15m), to_df(raw_1h), to_df(raw_4h))
            note_eval(sym, signal, reasons)
            if not signal:
                # Paper logging if rejected by score
                if reasons.get("common") == "score_low":
                    # نعيد بناء الإشارة للتسجيل
                    ctx, _ = build_context(to_df(raw_15m), to_df(raw_1h), to_df(raw_4h))
                    if ctx:
                        cand = []
                        sp, _ = pullback_signal(ctx)
                        if sp:
                            cand.append(sp)
                        if ENABLE_BREAKOUT:
                            sb, _ = breakout_signal(ctx, to_df(raw_15m))
                            if sb:
                                cand.append(sb)
                        for c in cand:
                            score_signal(c, ctx, to_df(raw_15m))
                        if cand:
                            best = max(cand, key=lambda x: x.get("score", 0))
                            log_paper_signal(sym, best, "score_low")
                return

            side = signal["side"]
            log.info(f"[SIGNAL] {sym} {side} score={signal.get('score', 0)} strategy={signal['strategy']}")

            with state_lock:
                same_dir = sum(1 for v in state.values() if v.get("position") == side)
            if same_dir >= MAX_SAME_DIRECTION:
                note_block(sym, "max_same_direction")
                log_paper_signal(sym, signal, "max_same_direction")
                return
            if sector_limit_reached(sym, side):
                note_block(sym, "sector_limit")
                log_paper_signal(sym, signal, "sector_limit")
                return
            if USE_BTC_FILTER and not sym.startswith("BTC/"):
                bias = btc_bias(ex)
                if (side == "LONG" and bias == "down") or (side == "SHORT" and bias == "up"):
                    note_block(sym, "btc_filter")
                    log_paper_signal(sym, signal, "btc_filter")
                    return
            ok, code = market_filters_ok(ex, sym, side)
            if not ok:
                note_block(sym, code)
                log_paper_signal(sym, signal, code)
                return

            actual_price = get_current_price(ex, sym, 0) or signal["price"]
            # سقف انزلاق ديناميكي
            sl_pct_signal = signal["sl_pct"]
            max_slip = max(MAX_ENTRY_SLIPPAGE_PCT, 0.18 * sl_pct_signal)
            if abs(actual_price - signal["price"]) / signal["price"] > max_slip:
                note_block(sym, "slippage")
                log_paper_signal(sym, signal, "slippage")
                return

            sl_pct = signal["sl_pct"]
            if side == "LONG":
                sl = actual_price * (1 - sl_pct)
                tp = actual_price * (1 + sl_pct * RR_RATIO)
            else:
                sl = actual_price * (1 + sl_pct)
                tp = actual_price * (1 - sl_pct * RR_RATIO)

            equity_now = get_equity(ex)
            margin = compute_margin_scored(equity_now, sl_pct, signal.get("score", 0))
            if margin <= 0:
                note_block(sym, "margin_too_small")
                log_paper_signal(sym, signal, "margin_too_small")
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
                        "realized_pnl": 0.0, "realized_fees_est": 0.0,
                        "planned_entry": actual_price,
                        "last_notified_sl": None,
                        "breakeven_hit": False, "partial_1_taken": False,
                        "partial_2_taken": False,
                        "regime": signal.get("regime", ""),
                        "strategy": signal.get("strategy", ""),
                        "signal_score": signal.get("score", 0),
                        "score_factors": signal.get("score_factors", {}),
                        "volume_ratio": signal.get("volume_ratio", 0),
                        "ker": signal.get("ker"),
                    })
            else:
                note_block(sym, "open_failed")
                with state_lock:
                    state[sym]["last_exit_ts"] = now
            save_state()
            caption = msg_entry(sym, side, entry, sl, tp, margin, success,
                                signal.get("regime", ""), signal.get("strategy", ""),
                                equity_now, signal.get("score", 0))
            if not success:
                caption += f"\n❌ {info}"
            send(caption)
    except Exception as e:
        loop_info["last_error"] = f"{sym}: {str(e)[:150]}"
        log.error(f"[ERROR] {sym}: {e}")


# ==========================================
# مزامنة ومراقبة اليتامى
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
        for sym in exch:
            if sym not in active_symbols:
                active_symbols.append(sym)
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
                            "opened_ts": now, "leg_ts": now,
                            "regime": "adopted", "strategy": "adopted"})
                    send(f"♻️ تبني مركز <code>{short(sym)}</code> ({side}) — SL {sl_pct*100:.2f}%")
        save_state()


def adopt_orphans(ex, active_symbols):
    with trade_lock:
        try:
            exch = {}
            for p in ex.fetch_positions():
                c = abs(float(p.get('contracts') or 0))
                if c > 0 and p.get('symbol') in active_symbols:
                    exch[p['symbol']] = p
        except Exception as e:
            log.warning(f"[ORPHAN] {e}")
            return
        for sym, p in exch.items():
            if sym not in active_symbols:
                active_symbols.append(sym)
            with state_lock:
                st = state.get(sym) or new_sym_state()
            if st.get("position"):
                continue
            side = "LONG" if p.get('side') == 'long' else "SHORT"
            entry = float(p.get('entryPrice') or 0)
            contracts = abs(float(p.get('contracts')))
            log.warning(f"[ORPHAN] {sym} {side} {contracts} @ {entry}")
            if not ADOPT_ORPHANS:
                if sym not in _warned_external:
                    _warned_external.add(sym)
                    send(f"⚠️ مركز غير مُدار <code>{short(sym)}</code> — ADOPT_ORPHANS=0")
                continue
            sl_pct = adopt_sl_pct(ex, sym)
            if side == "LONG":
                sl, tp = entry * (1 - sl_pct), entry * (1 + sl_pct * RR_RATIO)
            else:
                sl, tp = entry * (1 + sl_pct), entry * (1 - sl_pct * RR_RATIO)
            cur = get_current_price(ex, sym)
            if cur and ((side == "LONG" and cur <= sl) or (side == "SHORT" and cur >= sl)):
                send(f"🚨 يتيم <code>{short(sym)}</code> تجاوز الوقف — إغلاق فوري")
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
                        "opened_ts": now, "leg_ts": now,
                        "regime": "adopted", "strategy": "adopted"})
                save_state()
                send(f"♻️ تبني يتيم <code>{short(sym)}</code> ({side})
