# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V54 (Unified Sniper + Swing + Secure AI + Execution Safety)
"""
import os
import sys
import csv
import hashlib
import io
import json
import math
import re
import shutil
import tempfile
import time
import logging
import threading
import queue
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit

import numpy as np
import pandas as pd
import ccxt
import requests
from flask import Flask

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")

VERSION = "V54"

AI_IS_ACTIVE = False
ai_model = None
le_regime = None
le_strategy = None
le_side = None

KSA_TZ = timezone(timedelta(hours=3))


def env_float(name, default):
    try:
        value = float(os.getenv(name, default))
        return value if math.isfinite(value) else float(default)
    except ValueError:
        return float(default)


def env_flag(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")


# ==========================================
# الإعدادات
# ==========================================
OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_SECRET = os.getenv("OKX_SECRET", "")
OKX_PASSPHRASE = os.getenv("OKX_PASSPHRASE", os.getenv("OKX_PASSWORD", ""))
USE_SANDBOX = os.getenv("USE_SANDBOX", "true").lower() in ("true", "1", "yes")
ALLOW_LIVE_TRADING = env_flag("ALLOW_LIVE_TRADING", False)

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
UNIVERSE_REFRESH_SEC = int(env_float("UNIVERSE_REFRESH_SEC", 900))
MAX_UNIVERSE_SYMBOLS = int(env_float("MAX_UNIVERSE_SYMBOLS", 60))

ENTRY_MODE = os.getenv("ENTRY_MODE", "balanced").strip().lower()
if ENTRY_MODE not in ("balanced", "active", "strict"):
    ENTRY_MODE = "balanced"

_entry_defaults = {
    "strict":   {"min_ker": 0.30, "min_adx": 23, "min_score": 78, "bo_vol": 1.8},
    "balanced": {"min_ker": 0.25, "min_adx": 20, "min_score": 68, "bo_vol": 1.5},
    "active":   {"min_ker": 0.18, "min_adx": 18, "min_score": 60, "bo_vol": 1.2},
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
MAX_MARGIN_PCT_EQUITY = min(1.0, max(0.0, env_float("MAX_MARGIN_PCT_EQUITY", 0.30)))

ATR_SL_MULT = 1.55
RR_RATIO = 2.3
MIN_SL_PCT = 0.0065
MAX_SL_PCT = 0.032
TRAIL_ATR_MULT = 1.05
BE_TRIGGER_R = 0.9
FALLBACK_SL_PCT = 0.017

PARTIAL1_R, PARTIAL1_PCT = 1.3, 0.40
PARTIAL2_R, PARTIAL2_PCT = 2.1, 0.50

FEE_RATE = 0.0005
BE_FEE_BUFFER = min(0.05, max(2 * FEE_RATE, env_float("BE_FEE_BUFFER", 0.0012)))
FLOATING_FUNDING_BUFFER_PCT = max(0.0, env_float("FLOATING_FUNDING_BUFFER_PCT", 0.0))
DAILY_LOSS_LIMIT_PCT = env_float("DAILY_LOSS_LIMIT_PCT", 4.0)
DAILY_PROFIT_TARGET_PCT = env_float("DAILY_PROFIT_TARGET_PCT", 6.0)
INCLUDE_FLOATING_IN_LOSS_LIMIT = True
MAX_SAME_DIRECTION = int(env_float("MAX_SAME_DIRECTION", 3))
MAX_FUNDING_RATE = 0.00045
MAX_ENTRY_SLIPPAGE_PCT = env_float("MAX_ENTRY_SLIPPAGE_PCT", 0.004)
USE_BTC_FILTER = True

# ---------- AI Gateway & Local AI ----------
AI_GATEWAY_URL = os.getenv("AI_GATEWAY_URL", "https://api.openai.com/v1/chat/completions")
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
ENABLE_EXTERNAL_AI = env_flag("ENABLE_EXTERNAL_AI", False)
ENABLE_LOCAL_AI = env_flag("ENABLE_LOCAL_AI", False)
USE_AI_ENTRY_FILTER = env_flag("USE_AI_ENTRY_FILTER", False)
BACKUP_INTERVAL_SEC = int(env_float("BACKUP_INTERVAL_SEC", 6 * 3600))
AI_MIN_PROB = env_float("AI_MIN_PROB", 0.50)
AI_MIN_PROB_LONG = env_float("AI_MIN_PROB_LONG", AI_MIN_PROB)
AI_MIN_PROB_SHORT = env_float("AI_MIN_PROB_SHORT", AI_MIN_PROB)
VERBOSE_REJECT_LOG = env_flag("VERBOSE_REJECT_LOG", True)


def _verified_bytes(path, expected, size_limit):
    with open(path, "rb") as f:
        payload = f.read(size_limit + 1)
    if len(payload) > size_limit or hashlib.sha256(payload).hexdigest() != expected:
        raise ValueError(f"حجم أو بصمة غير صالحة: {os.path.basename(path)}")
    return payload


def _download_model(url, path, expected, size_limit):
    if urlsplit(url).scheme != "https":
        raise ValueError("AI_MODEL_URL must use HTTPS")
    with tempfile.NamedTemporaryFile(dir=os.path.dirname(os.path.abspath(path)), delete=False) as tmp:
        staging = tmp.name
    try:
        with requests.get(url, timeout=(10, 60), stream=True) as response, open(staging, "wb") as out:
            response.raise_for_status()
            if any(urlsplit(item.url).scheme != "https" for item in [*response.history, response]):
                raise ValueError("AI model redirect must use HTTPS")
            size = 0
            deadline = time.monotonic() + 180
            for chunk in response.iter_content(128 * 1024):
                size += len(chunk)
                if size > size_limit or time.monotonic() > deadline:
                    raise ValueError("AI model download exceeded its size or time budget")
                out.write(chunk)
        _verified_bytes(staging, expected, size_limit)
        os.replace(staging, path)
    finally:
        if os.path.exists(staging):
            os.unlink(staging)


def load_local_ai():
    """لا يُحمّل pickle إلا بعد مطابقة بصمات SHA-256 موثوقة لكل الملفات الأربعة."""
    global AI_IS_ACTIVE, ai_model, le_regime, le_strategy, le_side
    AI_IS_ACTIVE = False
    ai_model = le_regime = le_strategy = le_side = None
    if not ENABLE_LOCAL_AI:
        return
    specs = (
        ("ai_model_mega_v4.pkl", "AI_MODEL_SHA256", 128 * 1024**2),
        ("le_regime_mega_v4.pkl", "AI_REGIME_SHA256", 4 * 1024**2),
        ("le_strategy_mega_v4.pkl", "AI_STRATEGY_SHA256", 4 * 1024**2),
        ("le_side_mega_v4.pkl", "AI_SIDE_SHA256", 4 * 1024**2),
    )
    try:
        import joblib
        folder = os.getenv("AI_MODEL_DIR", ".")
        pins = [re.sub(r"\s+", "", os.getenv(env, "")).lower() for _, env, _ in specs]
        for (_, env, _), pin in zip(specs, pins):
            if not re.fullmatch(r"[0-9a-f]{64}", pin):
                raise ValueError(f"بصمة غير صالحة أو ناقصة في المتغير: {env}")
        verified = []
        for (name, _, limit), pin in zip(specs, pins):
            path = os.path.join(folder, name)
            url = os.getenv("AI_MODEL_URL", "").strip() if name == specs[0][0] else ""
            if not os.path.exists(path) and url:
                log.info(f"[AI] جاري تنزيل {name} ...")
                _download_model(url, path, pin, limit)
            verified.append(_verified_bytes(path, pin, limit))
        ai_model, le_regime, le_strategy, le_side = [joblib.load(io.BytesIO(blob)) for blob in verified]
        AI_IS_ACTIVE = True
        log.info("[AI] تم تحميل النموذج المحلي من ملفات مطابقة للبصمات")
    except Exception as e:
        log.warning(f"[AI] لم يُحمّل النموذج المحلي: {type(e).__name__} - {e}")


# ---------- Paper Mode ----------
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
    PROFILE_WARNING = "البروفايل active ممنوع على الحقيقي — استُخدم opportunist"
    PROFILE_NAME = "opportunist"

ENABLE_SNIPER = env_flag("ENABLE_SNIPER", True)
SNIPER_ONLY = env_flag("SNIPER_ONLY", False)

SNIPER = {
    "label": "قناص",
    "tfs": ("5m", "15m", "1h"),
    "leverage": int(env_float("SNIPER_LEVERAGE", 5)),
    "risk_pct": env_float("SNIPER_RISK_PCT", 0.32),
    "min_score": int(env_float("SNIPER_MIN_SCORE", 83)),
    "atr_sl_mult": 1.18,
    "rr": 1.85,
    "min_sl": 0.0048,
    "max_sl": 0.0145,
    "trail_atr": 0.68,
    "be_r": 0.48,
    "p1_r": 0.80,
    "p1_pct": 0.55,
    "p2_r": 1.40,
    "p2_pct": 0.45,
    "max_hold_minutes": int(env_float("SNIPER_MAX_HOLD_MIN", 75)),
    "cooldown": 420,
    "max_open": 2,
    "max_entry_slip": 0.0018,
    "max_signal_age_sec": 110,
    "min_ker": 0.29,
    "min_vol_mult": 1.40,
    "near_ema_pct": 0.0085,
}


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
TF_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000}

DATA_DIR = os.getenv("BOT_DATA_DIR") or (
    "/data" if os.path.isdir("/data") and os.access("/data", os.W_OK) else ".")
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
_funding_cache = {}
_balance_cache = {}
_balance_lock = threading.Lock()
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
    "signal_expired": "انتهت صلاحية الإشارة الزمنية",
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
    "ai_rejected": "رفض الذكاء الاصطناعي (احتمال منخفض)",
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
        if VERBOSE_REJECT_LOG:
            log.info(f"[REJECT] {short(sym)} → {text}")
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


def sniper_filters_ok(df_entry, current_price, signal_price, ker, vol_ratio):
    """فلاتر صارمة خاصة بوضع القناص"""
    if ker is None or ker < SNIPER["min_ker"]:
        return False, "low_ker_sniper"

    if vol_ratio < SNIPER["min_vol_mult"]:
        return False, "low_volume_sniper"

    try:
        ema21 = float(df_entry["ema21"].iloc[-2])
        dist = abs(current_price - ema21) / current_price
        if dist > SNIPER["near_ema_pct"]:
            return False, "far_from_ema_sniper"
    except Exception:
        return False, "ema_error_sniper"

    slip = abs(current_price - signal_price) / signal_price
    if slip > SNIPER["max_entry_slip"]:
        return False, "slippage_sniper"

    return True, ""


def get_trade_params(mode="swing"):
    """يرجع إعدادات الصفقة حسب الوضع"""
    if mode == "sniper":
        return {
            "partial1_r": SNIPER["p1_r"],
            "partial1_pct": SNIPER["p1_pct"],
            "partial2_r": SNIPER["p2_r"],
            "partial2_pct": SNIPER["p2_pct"],
            "trail_atr": SNIPER["trail_atr"],
            "be_r": SNIPER["be_r"],
            "max_hold_sec": SNIPER["max_hold_minutes"] * 60,
            "atr_sl_mult": SNIPER["atr_sl_mult"],
            "rr": SNIPER["rr"],
            "min_sl": SNIPER["min_sl"],
            "max_sl": SNIPER["max_sl"],
            "leverage": SNIPER["leverage"],
            "risk_pct": SNIPER["risk_pct"],
        }
    else:
        return {
            "partial1_r": PARTIAL1_R,
            "partial1_pct": PARTIAL1_PCT,
            "partial2_r": PARTIAL2_R,
            "partial2_pct": PARTIAL2_PCT,
            "trail_atr": TRAIL_ATR_MULT,
            "be_r": BE_TRIGGER_R,
            "max_hold_sec": 36 * 3600,
            "atr_sl_mult": ATR_SL_MULT,
            "rr": RR_RATIO,
            "min_sl": MIN_SL_PCT,
            "max_sl": MAX_SL_PCT,
            "leverage": LEVERAGE,
            "risk_pct": RISK_PCT_PER_TRADE,
        }


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
        "opened_ts": 0, "leg_ts": 0, "last_exit_ts": 0, "last_exit_mode": "swing",
        "realized_pnl": 0.0, "breakeven_hit": False,
        "partial_1_taken": False, "partial_2_taken": False,
        "last_notified_sl": None, "regime": "unknown", "strategy": "",
        "planned_entry": None, "realized_fees_est": 0.0, "missing_checks": 0,
        "signal_score": 0, "score_factors": {}, "volume_ratio": 0, "ker": None, "mode": "swing",
        "pending_protection_sl": None, "partial_order_pending": None,
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


def _atomic_write(path, payload):
    with tempfile.NamedTemporaryFile(dir=os.path.dirname(os.path.abspath(path)), delete=False) as f:
        tmp = f.name
    try:
        with open(tmp, "wb") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def save_state():
    try:
        with state_lock:
            compact_state = {
                k: v for k, v in state.items()
                if v.get("position") is not None or (v.get("last_exit_ts") or 0) > 0
            }
            txt = json.dumps({"state": compact_state, "daily_pnl": daily_pnl, "meta": meta},
                             ensure_ascii=False, indent=2)
            _atomic_write(STATE_FILE, txt.encode("utf-8"))
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


def reset_sym_state(sym, cooldown=True, mode="swing"):
    with state_lock:
        s = new_sym_state()
        s["last_exit_ts"] = time.time() if cooldown else 0
        s["last_exit_mode"] = mode or "swing"
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
            log.info(f"[JOURNAL] تم ترحيل {len(rows)} صفقة إلى صيغة {VERSION}")
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


# ---------- Paper Mode ----------
PAPER_FIELDS = ["ts", "symbol", "side", "strategy", "score", "price", "sl", "tp", "blocked_by"]


def log_paper_signal(sym, sig, blocked_by):
    if not ENABLE_PAPER_LOG:
        return
    try:
        with journal_lock:
            new = not os.path.exists(PAPER_FILE)
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
    by_block = df["blocked_by"].value_counts().head(8)
    lines = [f"📄 <b>إشارات Paper Mode</b> — {n} إشارة", "━━━━━━━━━━━━━━━",
             "<b>حسب سبب الحجب:</b>"]
    for reason, cnt in by_block.items():
        lines.append(f"• {reason}: {cnt}")
    try:
        bins = pd.cut(df["score"], bins=[0, 50, 60, 65, 70, 75, 80, 100])
        lines += ["", "<b>توزيع الدرجات:</b>"]
        for interval, cnt in df.groupby(bins, observed=False).size().items():
            if cnt:
                lines.append(f"• {interval}: {cnt}")
    except Exception:
        pass
    lines.append("\n💡 PAPER_FILE على /data للتحليل المفصّل")
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

    if not ENABLE_EXTERNAL_AI or not AI_API_KEY or urlsplit(AI_GATEWAY_URL).scheme != "https":
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
        r = requests.post(AI_GATEWAY_URL, json=payload, headers=headers, timeout=45,
                          allow_redirects=False)
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
        for filepath in [JOURNAL_FILE, STATE_FILE, PAPER_FILE]:
            file_name = os.path.basename(filepath)
            if os.path.exists(filepath):
                try:
                    lock = state_lock if filepath == STATE_FILE else journal_lock
                    with lock:
                        with open(filepath, 'rb') as f:
                            file_data = f.read()

                    r = requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                        data={"chat_id": cid, "caption": f"🔒 {file_name}"},
                        files={"document": (file_name, file_data)},
                        timeout=30
                    )
                    if not r.ok:
                        log.error(f"[BACKUP] TG Response: {r.text}")
                except Exception as e:
                    log.error(f"[BACKUP] فشل إرسال {filepath}: {e}")
                    send(f"❌ فشل إرسال {file_name}: {e}", cid)
            else:
                send(f"⚠️ الملف <code>{file_name}</code> غير موجود في السيرفر حالياً.", cid)


RESTORE_LIMIT = 10 * 1024**2


def _validated_restore(file_name, raw):
    text = raw.decode("utf-8-sig")
    if file_name == "bot_state.json":
        data = json.loads(text)
        if not isinstance(data, dict) or not all(
            isinstance(data.get(k), dict) for k in ("state", "daily_pnl", "meta")
        ):
            raise ValueError("صيغة حالة غير صالحة")
        flat = {}
        for sym, row in data["state"].items():
            if (not re.fullmatch(r"[A-Za-z0-9_.-]+/USDT:USDT", sym)
                    or not isinstance(row, dict) or row.get("position") is not None):
                raise ValueError("نسخة الحالة يجب ألا تحتوي على مراكز مفتوحة")
            last_exit = row.get("last_exit_ts", 0)
            if type(last_exit) not in (int, float) or not math.isfinite(last_exit) or last_exit < 0:
                raise ValueError("وقت إغلاق غير صالح")
            flat[sym] = {**new_sym_state(), "last_exit_ts": last_exit,
                         "last_exit_mode": row.get("last_exit_mode", "swing")}
        day = data["daily_pnl"]
        value, trades, wins = day["value"], day["trades"], day["wins"]
        datetime.strptime(day["date"], "%Y-%m-%d")
        if (type(value) not in (int, float) or not math.isfinite(value) or type(trades) is not int
                or type(wins) is not int or not 0 <= wins <= trades):
            raise ValueError("قيم الإحصاء اليومي غير صالحة")
        safe_day = {"value": value, "trades": trades, "wins": wins, "date": day["date"]}
        return json.dumps({"state": flat, "daily_pnl": safe_day, "meta": {"paused": True}},
                          ensure_ascii=False, allow_nan=False).encode("utf-8")

    fields = JOURNAL_FIELDS if file_name == "trades_journal.csv" else PAPER_FIELDS
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    if reader.fieldnames != fields:
        raise ValueError("رأس CSV لا يطابق بنية الإصدار الحالي")
    numeric = ("entry", "exit", "pnl", "r_multiple") if fields is JOURNAL_FIELDS else ("score", "price", "sl", "tp")
    for row in reader:
        if (None in row or None in row.values() or row["side"] not in ("LONG", "SHORT")
                or not row["symbol"] or not all(math.isfinite(float(row[k])) for k in numeric)):
            raise ValueError("صف CSV غير صالح")
    return text.encode("utf-8")


def handle_document_restore(msg, chat_id, ex):
    doc = msg.get("document")
    if not doc or chat_id not in AUTHORIZED_CHATS:
        return
    if (msg.get("chat") or {}).get("type") != "private":
        return send("الاستعادة متاحة في محادثة خاصة مصرح بها فقط.", chat_id)
    if (msg.get("caption") or "").strip().lower() != "/restore confirm":
        return send("أضف /restore confirm في تعليق الملف إذا أردت استبدال النسخة الحالية.", chat_id)
    file_name = doc.get("file_name", "")
    if file_name not in ("trades_journal.csv", "bot_state.json", "paper_signals.csv"):
        return send("❌ الملف غير معروف.", chat_id)
    try:
        if doc.get("file_size", 0) > RESTORE_LIMIT:
            raise ValueError("restore file too large")
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile",
                         params={"file_id": doc["file_id"]}, timeout=(5, 15), allow_redirects=False)
        r.raise_for_status()
        file_path = r.json()["result"]["file_path"]
        if not re.fullmatch(r"[A-Za-z0-9_/-]+(?:\.[A-Za-z0-9]+)?", file_path):
            raise ValueError("invalid remote file path")
        dl_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
        target = {"trades_journal.csv": JOURNAL_FILE,
                  "bot_state.json": STATE_FILE,
                  "paper_signals.csv": PAPER_FILE}[file_name]
        raw = bytearray()
        with requests.get(dl_url, stream=True, timeout=(5, 15), allow_redirects=False) as r_dl:
            r_dl.raise_for_status()
            deadline = time.monotonic() + 60
            for chunk in r_dl.iter_content(chunk_size=8192):
                raw.extend(chunk)
                if len(raw) > RESTORE_LIMIT or time.monotonic() > deadline:
                    raise ValueError("restore download exceeded its budget")
        payload = _validated_restore(file_name, raw)
        with trade_lock:
            if not meta.get("paused") or open_positions_snapshot():
                return send("أوقف الدخول بـ /pause وأغلق المراكز قبل الاستعادة.", chat_id)
            if any(abs(float(p.get("contracts") or 0)) > 0 for p in ex.fetch_positions()):
                return send("توجد مراكز على OKX. أوقف الاستعادة حتى تُغلق.", chat_id)
            with (state_lock if file_name == "bot_state.json" else journal_lock):
                if os.path.exists(target):
                    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                    shutil.copy2(target, f"{target}.{stamp}.bak")
                _atomic_write(target, payload)
                if file_name == "bot_state.json":
                    load_state()
        send(f"✅ تم استعادة {file_name}. لا يزال فتح الصفقات متوقفًا حتى /resume.", chat_id)
    except Exception as e:
        log.warning("[RESTORE] %s: %s", file_name, type(e).__name__)
        send("❌ تعذرت الاستعادة. تأكد من صيغة الملف وحجمه وحالة الاتصال.", chat_id)


# ==========================================
# تيليجرام (queue غير محجوب)
# ==========================================
def send(msg, chat_id=None):
    """غير محجوب: يضع الرسالة في queue ويعود فوراً."""
    try:
        _send_queue.put_nowait((msg, chat_id))
    except queue.Full:
        log.warning("[TG QUEUE] full — dropped")


def _send_worker():
    """Thread منفصل يرسل الرسائل من الـ queue."""
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
    "Time Exit": "انتهاء مدة القناص ⏱️",
    "Unconfirmed Exit": "سبب الإغلاق غير مؤكد",
    "Estimated Take Profit": "تحقق الهدف (تقديري)",
    "Estimated Stop Loss": "وقف الخسارة (تقديري)",
    "Estimated Trailing Stop": "وقف متحرك (تقديري)",
    "Estimated Breakeven Hit": "تعادل (تقديري)",
}


def effective_risk_pct(equity, sl_pct, lev=LEVERAGE, risk_pct=RISK_PCT_PER_TRADE):
    if equity <= 0 or sl_pct <= 0:
        return 0.0
    risk_usd = equity * risk_pct / 100.0
    margin = risk_usd / (sl_pct * lev)
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    margin = min(cap, margin)
    return margin * sl_pct * lev / equity * 100.0


def msg_entry(sym, side, price, sl, tp, margin, real_order, regime="", strategy="", equity=None, score=0, lev=LEVERAGE, mode="swing"):
    title = "🟢 <b>شراء (LONG)</b>" if side == "LONG" else "🔴 <b>بيع (SHORT)</b>"
    if mode == "sniper":
        title += " 🎯 [قناص]"
    status = "✅ <b>تم الفتح</b>" if real_order else "⚠️ <b>إشارة فقط</b>"
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * lev
    reward_usd = margin * (tp_pct / 100) * lev
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
            f"⚙️ {lev}x | الهامش: {margin:.2f} USDT\n"
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
# المؤشرات (مع KER)
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
    # ============================================================
# الجزء 2/3 — المنصة والتنفيذ والاستراتيجية
# ============================================================

# ==========================================
# المنصة (OKX)
# ==========================================
def create_exchange():
    if not USE_SANDBOX and not ALLOW_LIVE_TRADING:
        raise RuntimeError("Live trading requires ALLOW_LIVE_TRADING=true")
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
            last = t.get('last')
            if last:
                bid = float(t['bid']) if _valid_trigger(t.get('bid')) else None
                ask = float(t['ask']) if _valid_trigger(t.get('ask')) else None
                _price_cache[s] = (now, float(last), bid, ask)
    except Exception as e:
        log.warning(f"[PRICES] {e}")


def get_current_price(ex, sym, max_age=20):
    c = _price_cache.get(sym)
    if c and max_age > 0 and time.time() - c[0] <= max_age:
        return c[1]
    try:
        t = ex.fetch_ticker(sym)
        p = float(t['last'])
        if not _valid_trigger(p):
            return None
        bid = float(t['bid']) if _valid_trigger(t.get('bid')) else None
        ask = float(t['ask']) if _valid_trigger(t.get('ask')) else None
        _price_cache[sym] = (time.time(), p, bid, ask)
        return p
    except Exception as e:
        log.warning(f"[PRICE] {sym}: {e}")
        return c[1] if c and max_age > 0 and time.time() - c[0] <= max_age else None


def get_balances(ex, force=False):
    """رصيد الحساب الإجمالي للمخاطرة، والمتاح فعلياً كسقف للتنفيذ."""
    with _balance_lock:
        cached = _balance_cache.get(ex)
    if not force and cached and time.monotonic() - cached[0] < 60:
        return cached[1], cached[2]
    try:
        bal = ex.fetch_balance().get("USDT") or {}
        free = float(bal.get("free") or 0)
        equity = float(bal["total"] if bal.get("total") is not None else free)
        if not all(math.isfinite(v) for v in (free, equity)):
            raise ValueError("non-finite USDT balance")
        values = max(0.0, equity), max(0.0, free)
        with _balance_lock:
            if len(_balance_cache) >= 8:
                _balance_cache.clear()
            _balance_cache[ex] = (time.monotonic(), *values)
        return values
    except Exception as e:
        with _balance_lock:
            _balance_cache.pop(ex, None)
        log.warning("[EQUITY] %s", type(e).__name__)
        return 0.0, 0.0


def get_equity(ex, force=False):
    return get_balances(ex, force)[0]


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


def compute_margin(equity, sl_pct, risk_pct=None, leverage=None):
    """الهامش الأساسي حسب المخاطرة والمود (Swing أو Sniper)."""
    rp = risk_pct if risk_pct is not None else RISK_PCT_PER_TRADE
    lev = leverage if leverage is not None else LEVERAGE
    if (not math.isfinite(equity) or not math.isfinite(sl_pct)
            or equity <= 0 or sl_pct <= 0 or lev <= 0):
        return 0.0
    risk_usd = equity * rp / 100.0
    margin = risk_usd / (sl_pct * lev) if USE_RISK_SIZING else POSITION_SIZE_USDT
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    sized = min(cap, margin)
    return sized if sized >= MIN_MARGIN_USDT else 0.0


def compute_margin_scored(equity, sl_pct, signal_score, free_margin, ai_prob=None, risk_pct=None, leverage=None):
    base = compute_margin(equity, sl_pct, risk_pct, leverage)
    if base <= 0 or not math.isfinite(free_margin) or free_margin <= 0:
        return 0.0
    if AI_IS_ACTIVE and ai_prob is not None:
        if ai_prob >= 0.70:
            mult = 1.00
        elif ai_prob >= 0.60:
            mult = 0.85
        elif ai_prob >= 0.50:
            mult = 0.65
        else:
            mult = 0.40
    else:
        mult = next(m for threshold, m in ((86, 1.0), (78, 0.85), (70, 0.70), (62, 0.55), (0, 0.40))
                    if max(0, signal_score) >= threshold)
    sized = min(base * mult, free_margin)
    return sized if sized >= MIN_MARGIN_USDT else 0.0


def get_open_position(ex, sym):
    """None فقط إذا لا يوجد مركز. أي خطأ API يرفع Exception."""
    matches = []
    for p in ex.fetch_positions([sym]):
        if p.get("symbol") not in (None, sym):
            continue
        contracts = abs(float(p.get('contracts') or 0))
        if contracts > 0:
            if p.get("side") not in ("long", "short") or (p.get("info") or {}).get("posSide") in ("long", "short"):
                raise ValueError("the bot requires OKX net position mode")
            matches.append({"side": p["side"].upper(), "contracts": contracts,
                            "entry": float(p.get('entryPrice') or 0),
                            "margin": float(p.get('initialMargin') or 0)})
    if len(matches) > 1:
        raise ValueError("multiple positions for one instrument")
    return matches[0] if matches else None


# ==========================================
# أوامر الحماية
# ==========================================
def _valid_trigger(value):
    try:
        v = float(value or 0)
        return math.isfinite(v) and v > 0
    except (TypeError, ValueError):
        return False


def get_algos(ex, sym):
    out = []
    market = ex.market(sym)
    algo = ex.privateGetTradeOrdersAlgoPending({'instId': market['id'], 'ordType': 'conditional'})
    if str(algo.get("code")) != "0" or not isinstance(algo.get("data"), list):
        raise ValueError("invalid OKX pending algo response")
    for o in (algo.get('data') or []):
        out.append({"id": o.get('algoId'),
                    "side": o.get("side"), "size": o.get("sz"),
                    "reduce_only": str(o.get("reduceOnly")).lower() == "true",
                    "is_sl": _valid_trigger(o.get('slTriggerPx')),
                    "is_tp": _valid_trigger(o.get('tpTriggerPx'))})
    return out


def has_protection(ex, sym, close_side, contracts):
    """(has_sl, has_tp) أو (None, None) إذا تعذر الاستعلام."""
    try:
        algos = [a for a in get_algos(ex, sym) if a["id"] and a["side"] == close_side
                 and a["reduce_only"] and _valid_trigger(a["size"])
                 and math.isclose(float(a["size"]), contracts, rel_tol=1e-9, abs_tol=1e-12)]
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
    if str(res.get("code")) != "0" or str(data.get("sCode")) != "0" or not data.get("algoId"):
        raise Exception(f"algo rejected: {data.get('sCode')} {data.get('sMsg')}")
    return res


def protect_position(ex, sym, close_side, contracts, sl, tp, retries=3):
    """وقف الخسارة: أي رفض = فشل (لا نعتبر الصفقة محمية)."""
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
                    log.warning(f"[TP SKIP] {sym}: target already passed")
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
    ids = [a["id"] for a in old if a["id"] and a["reduce_only"] and a["side"] == close_side
           and (a["is_sl"] or a["is_tp"]) and (not a["is_tp"] or (tp is not None and tp_ok))]
    cancel_algo_ids(ex, sym, ids)
    return True


def close_full(ex, sym):
    """إغلاق كامل مع تأكيد متكرر."""
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


def set_leverage_checked(ex, sym, leverage=None):
    lev = leverage if leverage is not None else LEVERAGE
    last_err = None
    for _ in range(2):
        try:
            ex.set_leverage(lev, sym, params={'mgnMode': MARGIN_MODE})
            return True, ""
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    return False, str(last_err)


def open_position(ex, sym, side, price, sl, tp, margin, leverage=None):
    try:
        lev = leverage if leverage is not None else LEVERAGE
        amount = calculate_contracts(ex, sym, margin, price, lev)
        if amount <= 0:
            return False, "كمية غير صالحة", price, 0.0
        cancel_pending(ex, sym)
        ok, err = set_leverage_checked(ex, sym, lev)
        if not ok:
            log.error(f"[LEVERAGE] {sym}: {err}")
            return False, f"تعذر ضبط الرافعة: {err[:120]}", price, 0.0
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'
        order = ex.create_order(sym, 'market', order_side, amount, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net'})
        pos = None
        for _ in range(7):
            time.sleep(0.7)
            try:
                pos = get_open_position(ex, sym)
            except Exception as e:
                log.warning(f"[POS FETCH] {sym}: {e}")
                pos = None
            if pos:
                break
        if not pos:
            log.error(f"[OPEN UNCONFIRMED] {sym}: order={order.get('id')}")
            return False, "لم يتأكد تنفيذ الأمر (لا مركز) — لم تُوضع حماية", price, 0.0
        entry = pos["entry"] or price
        contracts = pos["contracts"]
        sl_ok, tp_ok = protect_position(ex, sym, close_side, contracts, sl, tp)
        if not sl_ok:
            send(f"🚨 فشل وضع SL لـ <code>{short(sym)}</code> — جاري الإغلاق.")
            if not close_full(ex, sym):
                send(f"🆘 فشل إغلاق <code>{short(sym)}</code>! تدخل يدوي فوراً.")
            return False, "فشل وضع SL", entry, 0.0
        if not tp_ok:
            send(f"⚠️ تم SL لكن فشل TP لـ <code>{short(sym)}</code>")
        return True, order.get('id'), entry, contracts
    except Exception as e:
        log.error(f"[OPEN ERR] {sym}: {e}")
        return False, str(e)[:150], price, 0.0


# ==========================================
# الربح وإنهاء الصفقة
# ==========================================
def leg_fee_est(entry, exit_price, contracts, cs):
    """رسوم تقديرية لجزء من الصفقة (دخول + خروج)."""
    return (entry + exit_price) * contracts * cs * FEE_RATE


def leg_pnl(side, entry, exit_price, contracts, cs):
    d = 1 if side == "LONG" else -1
    gross = (exit_price - entry) * d * contracts * cs
    return gross - leg_fee_est(entry, exit_price, contracts, cs)


def get_actual_fees(ex, sym, pos_data):
    """مجموع الرسوم الفعلية (USDT). None إذا لا تتوفر أو للمركز المتبنّى."""
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
    """قرينة سعرية فقط؛ لا تُسجّل باعتبارها سبباً مؤكداً."""
    if exit_price is None:
        return "Unconfirmed Exit"
    side, sl, tp = pos_data["position"], pos_data["sl"], pos_data["tp"]
    d = 1 if side == "LONG" else -1
    near_tp = bool(tp and -0.0005 <= d * (exit_price - tp) / tp <= 0.002)
    near_sl = bool(sl and -0.002 <= d * (exit_price - sl) / sl <= 0.0005)
    if near_tp == near_sl:
        return "Unconfirmed Exit"
    if near_tp:
        return "Estimated Take Profit"
    if pos_data.get("breakeven_hit"):
        be = breakeven_price(side, pos_data["entry"])
        return "Estimated Trailing Stop" if d * (sl - be) > 1e-8 else "Estimated Breakeven Hit"
    return "Estimated Stop Loss"


def finalize_trade(ex, sym, reason_hint=None):
    with state_lock:
        pos_data = dict(state.get(sym, {}))
    if not pos_data.get("position"):
        return
    side, entry = pos_data["position"], pos_data["entry"]
    mode = pos_data.get("mode", "swing")
    d = 1 if side == "LONG" else -1

    age = time.time() - (pos_data.get("opened_ts") or time.time())
    if age < GHOST_MAX_AGE_SEC and (pos_data.get("strategy") or "") != "adopted":
        try:
            fills = ex.fetch_my_trades(sym, since=int(pos_data["opened_ts"] * 1000) - 30000, limit=50)
            if fills is not None and len(fills) == 0:
                log.error(f"[GHOST] {sym}: لا تنفيذات ({age:.0f}ث) — صفقة وهمية، أُهملت")
                reset_sym_state(sym, cooldown=True, mode=mode)
                cancel_pending(ex, sym)
                send(f"⚠️ <b>صفقة وهمية مُهملة</b> <code>{short(sym)}</code>\n"
                     f"لا تنفيذات خلال {age:.0f}ث — لم تُحتسب.")
                return
        except Exception as e:
            log.warning(f"[GHOST CHECK] {sym}: {e}")

    fill_price = get_exit_price(ex, sym, pos_data)
    exit_price = fill_price or get_current_price(ex, sym, 0) or entry
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
    if fees_actual is not None and not use_actual:
        log.warning(f"[FEES] {sym}: فعلية {fees_actual:.4f} بعيدة عن التقدير {fees_est_total:.4f}")
    if use_actual:
        total = realized + realized_fees_est + gross_final - fees_actual
        fees_total, fees_src = fees_actual, "actual"
    else:
        total = realized + gross_final - fee_est_final
        fees_total, fees_src = fees_est_total, "est"
    final_leg = total - realized

    margin = pos_data.get("margin") or POSITION_SIZE_USDT
    reason = reason_hint or detect_reason(pos_data, fill_price)
    init_risk = pos_data.get("initial_risk", 0.0)
    risk_usd = init_risk * pos_data.get("initial_contracts", 0.0) * cs
    r_mult = (total / risk_usd) if risk_usd > 0 else 0.0
    duration = int((time.time() - (pos_data.get("opened_ts") or time.time())) / 60)

    stop_slip_usd = stop_slip_r = None
    if reason in ("Estimated Stop Loss", "Estimated Trailing Stop", "Estimated Breakeven Hit") and pos_data.get("sl"):
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
    reset_sym_state(sym, cooldown=True, mode=mode)
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, total, final_leg, margin, r_mult, reason,
                  pos_data.get("strategy", ""), fees_total, fees_src, stop_slip_usd, stop_slip_r))


def emergency_close(ex, sym, why):
    now = time.time()
    if now - _last_emergency_msg.get((sym, "start"), 0) > 120:
        _last_emergency_msg[(sym, "start")] = now
        send(f"🚨 <b>إغلاق طارئ</b> <code>{short(sym)}</code>\n{why}")
    if close_full(ex, sym):
        finalize_trade(ex, sym, reason_hint="Emergency")
    else:
        log.error(f"[EMERGENCY] {sym}: فشل الإغلاق الطارئ — سيُعاد")
        if now - _last_emergency_msg.get((sym, "fail"), 0) > 120:
            _last_emergency_msg[(sym, "fail")] = now
            send(f"🆘 فشل الإغلاق الطارئ لـ <code>{short(sym)}</code> — يُعاد. تحقق من المنصة.")


# ==========================================
# البيانات
# ==========================================
def fetch_data(ex, sym, tf, limit=130):
    """يعيد مؤشرات محسوبة مرة لكل شمعة مغلقة، مع حذف FIFO عند امتلاء الكاش."""
    key = (sym, tf, limit)
    tf_ms = TF_MS[tf]
    cached = _data_cache.get(key)
    current = int(time.time() * 1000 // tf_ms) * tf_ms
    if cached and cached[0] == current:
        return cached[1]
    try:
        data = ex.fetch_ohlcv(sym, tf, limit=limit)
        current = int(time.time() * 1000 // tf_ms) * tf_ms
        if len(data) < 2 or data[-1][0] != current or data[-2][0] != current - tf_ms:
            return pd.DataFrame()
        df = to_df(data)
        if tf in ("5m", "15m") and len(df) >= 70:
            df.attrs["range_filter"] = calculate_range_filter(df)
            df["ker_indicator"] = efficiency_ratio(df["close"], KER_PERIOD)
        if len(_data_cache) >= 1024 and key not in _data_cache:
            for _ in range(150):
                _data_cache.pop(next(iter(_data_cache)), None)
        _data_cache[key] = (current, df)
        return df
    except Exception as e:
        log.warning(f"[OHLCV] {sym} {tf}: {e}")
        return pd.DataFrame()


def to_df(raw):
    return add_indicators(pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"]))


def btc_bias(ex):
    if time.time() - _btc_bias_cache["ts"] < 120:
        return _btc_bias_cache["value"]
    val = "neutral"
    try:
        df = fetch_data(ex, "BTC/USDT:USDT", "1h")
        if len(df) >= 60:
            r = df.iloc[-2]
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
def detect_regime(df_15m, df_1h, df_4h, timeframe="15m"):
    if len(df_4h) < 50 or len(df_1h) < 50:
        return "unknown"
    last_4h = df_4h.iloc[-2]
    last_1h = df_1h.iloc[-2]
    adx4 = last_4h.get("adx", 0)
    adx1 = last_1h.get("adx", 0)
    ema50_4 = last_4h.get("ema50", last_4h["close"])
    up_4h = last_4h["close"] > ema50_4 and last_4h["ema9"] > last_4h["ema21"]
    down_4h = last_4h["close"] < ema50_4 and last_4h["ema9"] < last_4h["ema21"]

    if timeframe == "5m":
        adx4_strong_eff = REGIME_ADX4_STRONG * 0.85
        adx4_weak_eff = REGIME_ADX4_WEAK * 0.85
        adx1_strong_eff = REGIME_ADX1_STRONG * 0.90
    else:
        adx4_strong_eff = REGIME_ADX4_STRONG
        adx4_weak_eff = REGIME_ADX4_WEAK
        adx1_strong_eff = REGIME_ADX1_STRONG

    if adx4 > adx4_strong_eff and adx1 > adx1_strong_eff:
        if up_4h:
            return "strong_trend_up"
        if down_4h:
            return "strong_trend_down"
    if adx4 > adx4_weak_eff and (up_4h or down_4h):
        return "weak_trend"
    return "range"


def relative_atr(df):
    """نسبة ATR% الحالي إلى وسيط ATR% خلال ~100 شمعة سابقة."""
    try:
        series = (df["atr"] / df["close"]).iloc[-102:-2].dropna()
        if len(series) < 60:
            return None
        base = float(series.median())
        cur = float(df["atr"].iloc[-2] / df["close"].iloc[-2])
        return cur / base if base > 0 else None
    except Exception:
        return None


def calculate_range_filter(df, period=100, mult=3.0):
    if len(df) < 3:
        return 0
    src = df["close"].iloc[:-1]
    smooth = (src.diff().abs().ewm(span=period, adjust=False).mean()
              .ewm(span=2 * period - 1, adjust=False).mean() * mult)
    values, widths = src.to_numpy(), smooth.to_numpy()
    current = values[0]
    previous = current
    for price, width in zip(values[1:], widths[1:]):
        previous = current
        current = max(previous, price - width) if price > previous else min(previous, price + width)
    if current > previous and values[-1] > current:
        return 1
    if current < previous and values[-1] < current:
        return -1
    return 0


def build_context(df_15m, df_1h, df_4h, timeframe="15m"):
    """الشروط العامة المشتركة مع معايرة تلقائية لوضع القناص (5m)."""
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
    if not _valid_trigger(price) or not _valid_trigger(atr_val):
        return None, "indicators_nan"

    if timeframe == "5m":
        atr_mult_eff = SNIPER["atr_sl_mult"]
        min_atr_eff = MIN_ATR_PCT * 0.65
        min_adx_eff = MIN_ADX * 0.90
        min_sl_eff = SNIPER["min_sl"]
        max_sl_eff = SNIPER["max_sl"]
        rr_eff = SNIPER["rr"]
    else:
        atr_mult_eff = ATR_SL_MULT
        min_atr_eff = MIN_ATR_PCT
        min_adx_eff = MIN_ADX
        min_sl_eff = MIN_SL_PCT
        max_sl_eff = MAX_SL_PCT
        rr_eff = RR_RATIO

    regime = detect_regime(df_15m, df_1h, df_4h, timeframe)
    if regime == "unknown":
        return None, "insufficient_data"
    if regime == "range":
        return None, "regime_range"
    atr_pct = atr_val / price
    if atr_pct > MAX_ATR_PCT:
        return None, "atr_high"
    rel = relative_atr(df_15m)
    atr_ok = atr_pct >= min_atr_eff or (
        rel is not None and atr_pct >= ATR_HARD_FLOOR and rel >= REL_ATR_RATIO)
    if not atr_ok:
        return None, "atr_low"
    if adx_val < min_adx_eff:
        return None, "low_adx"

    ker_value = None
    er_series = (df_15m["ker_indicator"] if "ker_indicator" in df_15m
                 else efficiency_ratio(df_15m["close"], KER_PERIOD))
    if ENABLE_KER_FILTER:
        ker_recent = er_series.iloc[-4:-1].dropna()
        ker_smoothed = float(ker_recent.median()) if len(ker_recent) >= 2 else 0.0
        min_ker_eff = MIN_KER * 0.55 if regime in ("strong_trend_up", "strong_trend_down") else MIN_KER
        if timeframe == "5m":
            min_ker_eff = max(min_ker_eff, SNIPER["min_ker"])
        if ker_smoothed < min_ker_eff:
            return None, "low_ker"
        ker_value = ker_smoothed
    else:
        v = er_series.iloc[-2] if len(er_series) >= 2 else None
        ker_value = float(v) if (v is not None and not pd.isna(v)) else None

    rf_direction = (df_15m.attrs["range_filter"] if "range_filter" in df_15m.attrs
                    else calculate_range_filter(df_15m))

    long_dir = regime in ("strong_trend_up", "weak_trend") and last_4h["close"] > last_4h["ema50"]
    short_dir = regime in ("strong_trend_down", "weak_trend") and last_4h["close"] < last_4h["ema50"]

    if rf_direction == -1:
        long_dir = False
    if rf_direction == 1:
        short_dir = False

    if not long_dir and not short_dir:
        return None, "direction_mismatch"

    sl_pct = max(min_sl_eff, min(max_sl_eff, atr_mult_eff * atr_val / price))
    return {
        "last": last, "prev": prev, "last_1h": last_1h, "last_4h": last_4h,
        "price": price, "atr_val": atr_val, "ema21": float(last["ema21"]),
        "ema50": float(last["ema50"]), "rsi": float(last["rsi"]),
        "regime": regime, "long_dir": long_dir, "short_dir": short_dir,
        "ker": ker_value, "sl_pct": sl_pct, "tp_pct": sl_pct * rr_eff,
        "candle_ts": int(last["timestamp"]),
    }, None


def _make_signal(side, ctx, strategy):
    p, sp, tp_pct = ctx["price"], ctx["sl_pct"], ctx["tp_pct"]
    if side == "LONG":
        sl, tp = p * (1 - sp), p * (1 + tp_pct)
    else:
        sl, tp = p * (1 + sp), p * (1 - tp_pct)
    return {"side": side, "price": p, "sl_pct": sp, "sl": sl, "tp": tp,
            "regime": ctx["regime"], "strategy": strategy, "ker": ctx.get("ker"),
            "candle_ts": ctx.get("candle_ts", 0)}


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
    if not ctx["short_dir"]:
        return None, "direction_mismatch"
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
    if not ctx["short_dir"]:
        return None, "direction_mismatch"
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
    """تقييم جودة الفرصة من 100 نقطة."""
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

    if signal.get("strategy") == "breakout":
        factors["setup"] = 15 if vol_ratio >= 2.0 else 13
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


def evaluate_signal(df_15m, df_1h, df_4h, timeframe="15m"):
    """تقييم الاستراتيجيتين معاً واختيار الأعلى درجة."""
    ctx, reason = build_context(df_15m, df_1h, df_4h, timeframe)
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
    """فحص السبريد (من الكاش الحديث أو API) ومعدل التمويل (مع كاش 15 دقيقة)."""
    now = time.time()
    cached = _price_cache.get(sym)
    if cached and (now - cached[0]) <= 15 and cached[2] and cached[3]:
        last, bid, ask = cached[1], cached[2], cached[3]
        spread = (ask - bid) / last
        _last_spread[sym] = (now, spread)
        if spread > MAX_SPREAD_PCT:
            log.info(f"[SPREAD] {sym} {spread * 100:.3f}% > {MAX_SPREAD_PCT * 100:.3f}% — حجب")
            return False, "spread_high"
    else:
        try:
            t = ex.fetch_ticker(sym)
            bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
            if all(_valid_trigger(v) for v in (bid, ask, last)) and ask >= bid:
                _price_cache[sym] = (now, float(last), float(bid), float(ask))
                spread = (ask - bid) / last
                _last_spread[sym] = (now, spread)
                if spread > MAX_SPREAD_PCT:
                    log.info(f"[SPREAD] {sym} {spread * 100:.3f}% > {MAX_SPREAD_PCT * 100:.3f}% — حجب")
                    return False, "spread_high"
            else:
                return False, "ticker_unavailable"
        except Exception as e:
            log.warning(f"[FILTER TICKER] {sym}: {e}")
            return False, "ticker_unavailable"

    f_cached = _funding_cache.get(sym)
    if f_cached and (now - f_cached[0]) < 900:
        fr = f_cached[1]
    else:
        try:
            fr = ex.fetch_funding_rate(sym).get('fundingRate')
            _funding_cache[sym] = (now, fr)
        except Exception as e:
            log.warning(f"[FILTER FUNDING] {sym}: {e} (تجاوز)")
            fr = None

    if fr is not None:
        if side == "LONG" and fr > MAX_FUNDING_RATE:
            return False, "funding_high"
        if side == "SHORT" and fr < -MAX_FUNDING_RATE:
            return False, "funding_negative"
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
    # ============================================================
# الجزء 3/3 — إدارة المراكز، التنفيذ، Telegram، الحلقة الرئيسية
# ============================================================

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
        send(f"{title}\n🪙 <code>{short(sym)}</code>\n🛑 الوقف الجديد: <code>{fmt(new_sl)}</code>")
    return True


def do_partial(ex, sym, st, which):
    side = st["position"]
    entry = st["entry"]
    close_side = 'sell' if side == "LONG" else 'buy'
    params = get_trade_params(st.get("mode", "swing"))
    p1_pct = params["partial1_pct"]
    p2_pct = params["partial2_pct"]
    contracts = st["contracts"]
    raw_amt = st["initial_contracts"] * p1_pct if which == 1 else contracts * p2_pct
    amt = float(ex.amount_to_precision(sym, raw_amt))
    mn = min_amount(ex, sym)
    remaining = contracts - amt
    if amt < mn or remaining < mn:
        with state_lock:
            state[sym][f"partial_{which}_taken"] = True
        save_state()
        return False
    pending = {"which": which, "before": contracts, "id": None, "price": None, "ts": time.time()}
    with state_lock:
        state[sym]["partial_order_pending"] = pending
    save_state()
    try:
        order = ex.create_order(sym, 'market', close_side, amt, None,
                                {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        with state_lock:
            pending.update(id=order.get("id"), price=order.get("average") or order.get("price"))
        save_state()
    except Exception as e:
        log.error(f"[PARTIAL ORDER] {sym}: {e}")
        with state_lock:
            state[sym]["partial_order_pending"] = None
        save_state()
        send(f"⚠️ تعذر إرسال طلب الإغلاق الجزئي لـ <code>{short(sym)}</code>.")
        return False
    time.sleep(0.8)
    try:
        after = get_open_position(ex, sym)
        if after and after["side"] == side:
            reconcile_partial(ex, sym, st, pending, after["contracts"])
    except Exception as e:
        log.warning(f"[PARTIAL POS FETCH] {sym}: {e}")
    return True


def reconcile_partial(ex, sym, st, pending, remaining):
    which, contracts = pending["which"], pending["before"]
    actual_closed = contracts - remaining
    if actual_closed <= 1e-10 or remaining <= 0:
        if time.time() - pending.get("ts", 0) > 30:
            with state_lock:
                state[sym]["partial_order_pending"] = None
            save_state()
        return False
    side, entry = st["position"], st["entry"]
    fill_ts = time.time()
    exit_price = pending.get("price")
    try:
        if not exit_price and pending.get("id"):
            fetched = ex.fetch_order(pending["id"], sym)
            exit_price = fetched.get("average") or fetched.get("price")
    except Exception as e:
        log.warning(f"[PARTIAL FILL PRICE] {sym}: {e}")
    exit_price = float(exit_price or get_current_price(ex, sym, 0) or entry)
    cs_leg = contract_size(ex, sym)
    pnl = leg_pnl(side, entry, exit_price, actual_closed, cs_leg)
    fee_est_leg = leg_fee_est(entry, exit_price, actual_closed, cs_leg)
    new_sl = st["sl"]
    if which == 1:
        be = breakeven_price(side, entry)
        new_sl = max(be, new_sl) if side == "LONG" else min(be, new_sl)
    with state_lock:
        s = state[sym]
        s["contracts"] = remaining
        s[f"partial_{which}_taken"] = True
        s["realized_pnl"] = s.get("realized_pnl", 0.0) + pnl
        s["realized_fees_est"] = s.get("realized_fees_est", 0.0) + fee_est_leg
        s["leg_ts"] = fill_ts
        s["pending_protection_sl"] = new_sl
        s["partial_order_pending"] = None
    add_daily_pnl(pnl)
    send(f"💰 <b>إغلاق جزئي {which}</b> | <code>{short(sym)}</code>\n"
         f"ربح الجزء: {pnl:+.2f} USDT. جارٍ التحقق من حماية المتبقي.")
    repair_protection(ex, sym, dict(state[sym]), get_current_price(ex, sym))
    return True


_tp_retry = {}


def repair_protection(ex, sym, st, price):
    side = st["position"]
    close_side = 'sell' if side == "LONG" else 'buy'
    contracts = st["contracts"]
    has_sl, has_tp = has_protection(ex, sym, close_side, contracts)
    pending_sl = st.get("pending_protection_sl")
    if has_sl is None and pending_sl is None:
        return False
    if not has_sl or pending_sl is not None:
        sl_px = st.get("sl")
        desired = pending_sl or sl_px
        breached = price and desired and (price <= desired if side == "LONG" else price >= desired)
        if breached or not desired:
            if has_sl is not True or pending_sl is not None:
                emergency_close(ex, sym, "تجاوز الوقف أو تعذر تحديده")
            return False
        if replace_protection(ex, sym, close_side, contracts, desired, st.get("tp")):
            with state_lock:
                state[sym]["sl"] = desired
                state[sym]["pending_protection_sl"] = None
                entry = st["entry"]
                state[sym]["breakeven_hit"] = (desired >= entry if side == "LONG" else desired <= entry)
            save_state()
        else:
            has_sl, _ = has_protection(ex, sym, close_side, contracts)
            if has_sl is not True:
                emergency_close(ex, sym, "تعذر تأكيد وقف يغطي الكمية المتبقية")
        return False

    tp = st.get("tp")
    if tp and has_tp is False and price and time.monotonic() >= _tp_retry.get(sym, 0):
        _tp_retry[sym] = time.monotonic() + 30
        if (price >= tp if side == "LONG" else price <= tp):
            fresh = get_current_price(ex, sym, 0)
            if not fresh:
                return False
            if (fresh >= tp if side == "LONG" else fresh <= tp):
                if close_full(ex, sym):
                    finalize_trade(ex, sym, reason_hint="Take Profit")
                return False
        try:
            place_sl_tp(ex, sym, close_side, contracts, tp, False)
        except Exception as e:
            log.warning("[TP REPAIR] %s: %s", sym, e)
    return True


def manage_position(ex, sym, st):
    side, entry = st["position"], st["entry"]
    d = 1 if side == "LONG" else -1
    price = get_current_price(ex, sym)
    pending = st.get("partial_order_pending")
    if pending and reconcile_partial(ex, sym, st, pending, st["contracts"]):
        return
    if not repair_protection(ex, sym, st, price) or not price:
        return

    mode = st.get("mode", "swing")
    params = get_trade_params(mode)

    if mode == "sniper" and st.get("opened_ts") and (time.time() - st["opened_ts"]) >= params["max_hold_sec"]:
        log.info(f"[SNIPER TIME EXIT] {sym}: تجاوز {SNIPER['max_hold_minutes']} دقيقة")
        if close_full(ex, sym):
            finalize_trade(ex, sym, reason_hint="Time Exit")
        return

    risk = st.get("initial_risk") or abs(entry - (st.get("initial_sl") or st["sl"]))
    if risk <= 0:
        return
    r_now = (price - entry) * d / risk

    if not pending and not st.get("partial_1_taken") and r_now >= params["partial1_r"]:
        if do_partial(ex, sym, st, 1):
            return
    elif not pending and st.get("partial_1_taken") and not st.get("partial_2_taken") and r_now >= params["partial2_r"]:
        if do_partial(ex, sym, st, 2):
            return

    if not st.get("breakeven_hit"):
        if r_now >= params["be_r"]:
            move_sl(ex, sym, st, breakeven_price(side, entry), "🛡️ تأمين الصفقة (Breakeven)")
        return

    trail_tf = "5m" if mode == "sniper" else "15m"
    df = fetch_data(ex, sym, trail_tf)
    if len(df) < 60:
        return
    atr_val = df.iloc[-2].get("atr")
    if atr_val is None or pd.isna(atr_val):
        return
    mult = params["trail_atr"]
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
def open_positions_snapshot():
    with state_lock:
        return {k: dict(v) for k, v in state.items() if v.get("position")}


def floating_pnl_cached(ex):
    """الربح/الخسارة العائمة من كاش الأسعار (بدون طلبات شبكة)."""
    total = 0.0
    for sym, v in open_positions_snapshot().items():
        c = _price_cache.get(sym)
        price = c[1] if c else v["entry"]
        cs = contract_size(ex, sym)
        total += leg_pnl(v["position"], v["entry"], price, v["contracts"], cs)
        total -= price * v["contracts"] * cs * FLOATING_FUNDING_BUFFER_PCT
    return total


def entries_allowed(ex):
    """(ok, reason)."""
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
# معالجة الرموز (مع Paper logging و Sniper)
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
                        log.info(f"[MISSING] {sym}: ({missing}/{MISSING_CONFIRMATIONS}) إعادة تحقق")
                        return
                    finalize_trade(ex, sym)
                    return
                if exch_pos["side"] != pos_data["position"]:
                    log.error("[POSITION SIDE MISMATCH] %s: verify the position in OKX", sym)
                    return
                with state_lock:
                    state[sym]["missing_checks"] = 0
                    state[sym]["contracts"] = exch_pos["contracts"]
                    cur = dict(state[sym])
                manage_position(ex, sym, cur)
                return

        if pos_data.get("position") is None:
            last_exit_mode = pos_data.get("last_exit_mode") or "swing"
            effective_cooldown = SNIPER["cooldown"] if last_exit_mode == "sniper" else REENTRY_COOLDOWN_SEC
            if time.time() - pos_data.get("last_exit_ts", 0) < effective_cooldown:
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

            df_15m = fetch_data(ex, sym, "15m")
            if len(df_15m) < 70:
                set_last_reason(sym, "no_data")
                return

            swing_ts = int(df_15m["timestamp"].iloc[-2])
            want_swing = not SNIPER_ONLY and _last_eval.get((sym, "swing")) != swing_ts
            expected_5m = (int(time.time() * 1000 // TF_MS["5m"]) - 1) * TF_MS["5m"]
            want_sniper = ENABLE_SNIPER and _last_eval.get((sym, "sniper")) != expected_5m
            if not want_swing and not want_sniper:
                return

            df_1h = fetch_data(ex, sym, "1h")
            if len(df_1h) < 60:
                set_last_reason(sym, "no_data")
                return
            df_5m = fetch_data(ex, sym, "5m") if want_sniper else pd.DataFrame()

            # ===================== 1. محاولة القناص أولاً (5m) =====================
            signal = None
            reasons = {}
            mode = "swing"

            if want_sniper and len(df_5m) >= 70:
                try:
                    _last_eval[(sym, "sniper")] = int(df_5m["timestamp"].iloc[-2])
                    signal_s, reasons_s = evaluate_signal(df_5m, df_15m, df_1h, timeframe="5m")
                    reasons = reasons_s or {"common": "score_low"}

                    if signal_s and signal_s.get("score", 0) >= SNIPER["min_score"]:
                        current_price = get_current_price(ex, sym) or signal_s["price"]
                        ker = signal_s.get("ker")
                        vol_ratio = signal_s.get("volume_ratio", 0)

                        ok_sniper, reason_sniper = sniper_filters_ok(
                            df_5m, current_price, signal_s["price"], ker, vol_ratio
                        )

                        if ok_sniper:
                            with state_lock:
                                sniper_open = sum(1 for v in state.values() if v.get("position") and v.get("mode") == "sniper")
                            if sniper_open >= SNIPER["max_open"]:
                                log.info(f"[SNIPER REJECT] {sym} → max_open ({sniper_open}/{SNIPER['max_open']})")
                            else:
                                signal = signal_s
                                reasons = reasons_s
                                mode = "sniper"
                                log.info(f"[SNIPER] {sym} {signal['side']} score={signal.get('score')} ✓")
                        else:
                            reasons = {"common": reason_sniper}
                            log.info(f"[SNIPER REJECT] {sym} → {reason_sniper}")
                except Exception as e:
                    log.warning(f"[SNIPER ERROR] {sym}: {e}")

            # ===================== 2. النظام الأساسي (15m) =====================
            if signal is None and want_swing:
                df_4h = fetch_data(ex, sym, "4h")
                if len(df_4h) >= 60:
                    _last_eval[(sym, "swing")] = swing_ts
                    signal, reasons = evaluate_signal(df_15m, df_1h, df_4h, timeframe="15m")
                    mode = "swing"

            if not signal and not reasons:
                set_last_reason(sym, "no_data")
                return

            note_eval(sym, signal, reasons)
            if not signal:
                return

            # التحقق من عمر الإشارة الزمني
            tf_ms = TF_MS["5m" if mode == "sniper" else "15m"]
            max_age_sec = SNIPER["max_signal_age_sec"] if mode == "sniper" else 240
            if (time.time() * 1000 - (signal.get("candle_ts", 0) + tf_ms)) > max_age_sec * 1000:
                note_block(sym, "signal_expired")
                return

            side = signal["side"]
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

            if USE_BTC_FILTER:
                bias = btc_bias(ex)
                if (side == "LONG" and bias == "down") or (side == "SHORT" and bias == "up"):
                    note_block(sym, "btc_filter")
                    log_paper_signal(sym, signal, "btc_filter")
                    return

            m_ok, m_reason = market_filters_ok(ex, sym, side)
            if not m_ok:
                note_block(sym, m_reason)
                log_paper_signal(sym, signal, m_reason)
                return

            max_slip = SNIPER["max_entry_slip"] if mode == "sniper" else max(MAX_ENTRY_SLIPPAGE_PCT, 0.18 * signal["sl_pct"])
            t_params = get_trade_params(mode)
            sl_pct = max(t_params["min_sl"], min(t_params["max_sl"], signal["sl_pct"]))
            tp_rr = t_params["rr"]
            lev_eff = t_params["leverage"]
            risk_eff = t_params["risk_pct"]

            # --- فلتر وتقييم الـ AI المحلي ---
            ai_prob = None
            if AI_IS_ACTIVE:
                try:
                    factors = signal.get("score_factors", {})
                    features = [
                        signal.get("score", 0), factors.get("trend", 0), factors.get("volume", 0),
                        factors.get("adx", 0), factors.get("momentum", 0), factors.get("candle", 0),
                        factors.get("setup", 0), factors.get("entry_quality", 0),
                        signal.get("volume_ratio") or 0, signal.get("ker") or 0,
                        le_regime.transform([signal.get("regime", "unknown")])[0],
                        le_strategy.transform([signal.get("strategy", "pullback")])[0],
                        le_side.transform([signal.get("side", "LONG")])[0]
                    ]
                    ai_prob = float(ai_model.predict_proba([features])[0][1])
                    if not 0 <= ai_prob <= 1:
                        raise ValueError("invalid model probability")
                    signal["ai_probability"] = ai_prob
                    thr = AI_MIN_PROB_LONG if side == "LONG" else AI_MIN_PROB_SHORT
                    if signal.get("score", 0) >= 86:
                        thr = max(0.35, thr - 0.05)

                    log.info(f"[AI] {short(sym)} {side} score={signal.get('score')} "
                             f"احتمال={ai_prob*100:.1f}% (الحد {thr*100:.0f}%)")
                    if mode == "sniper":
                        log.info(f"[AI SNIPER] {short(sym)} مستثنى من حجب الـ AI")
                    elif USE_AI_ENTRY_FILTER and ai_prob < thr:
                        note_block(sym, "ai_rejected")
                        log_paper_signal(sym, signal, f"ai_rejected_{ai_prob:.2f}")
                        return
                except Exception as e:
                    log.warning(f"[AI ERROR] {e}")
                    ai_prob = None

            with trade_lock:
                with state_lock:
                    current = state.get(sym, {})
                    open_count = sum(bool(v.get("position")) for v in state.values())
                    same_dir = sum(v.get("position") == side for v in state.values())
                    sniper_count = sum(bool(v.get("position")) and v.get("mode") == "sniper"
                                       for v in state.values())
                if (current.get("position") or current.get("last_exit_ts") != pos_data.get("last_exit_ts")
                        or open_count >= MAX_OPEN_POSITIONS or same_dir >= MAX_SAME_DIRECTION
                        or (mode == "sniper" and sniper_count >= SNIPER["max_open"])
                        or sector_limit_reached(sym, side)):
                    return
                if meta.get("paused") or get_open_position(ex, sym) is not None:
                    return
                equity_now, free_margin = get_balances(ex, force=True)
                ok, gate = entries_allowed(ex)
                if not ok:
                    note_block(sym, gate)
                    return
                margin = compute_margin_scored(
                    equity_now, sl_pct, signal.get("score", 0), free_margin,
                    ai_prob=None if mode == "sniper" else ai_prob,
                    risk_pct=risk_eff, leverage=lev_eff
                )
                if margin <= 0:
                    note_block(sym, "margin_too_small")
                    log_paper_signal(sym, signal, "margin_too_small")
                    return
                actual_price = get_current_price(ex, sym, 0)
                if not actual_price or abs(actual_price - signal["price"]) / signal["price"] > max_slip:
                    reason = "slippage" if actual_price else "ticker_unavailable"
                    note_block(sym, reason)
                    log_paper_signal(sym, signal, reason)
                    return
                d = 1 if side == "LONG" else -1
                sl, tp = actual_price * (1 - d * sl_pct), actual_price * (1 + d * sl_pct * tp_rr)
                success, info, entry, contracts = open_position(ex, sym, side, actual_price, sl, tp, margin, leverage=lev_eff)
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
                            "breakeven_hit": False, "partial_1_taken": False, "partial_2_taken": False,
                            "regime": signal.get("regime", ""),
                            "strategy": signal.get("strategy", ""),
                            "mode": mode,
                            "signal_score": signal.get("score", 0),
                            "score_factors": signal.get("score_factors", {}),
                            "volume_ratio": signal.get("volume_ratio", 0),
                            "ker": signal.get("ker"),
                        })
                else:
                    note_block(sym, "open_failed")
                    with state_lock:
                        state[sym]["last_exit_ts"] = now
                        state[sym]["last_exit_mode"] = mode
                save_state()
            caption = msg_entry(sym, side, entry, sl, tp, margin, success,
                                signal.get("regime", ""), signal.get("strategy", ""),
                                equity_now, signal.get("score", 0), lev=lev_eff, mode=mode)
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
        df = fetch_data(ex, sym, "15m")
        if len(df) >= 60:
            a, c = float(df.iloc[-2]["atr"]), float(df.iloc[-2]["close"])
            if c > 0 and not np.isnan(a):
                return max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULT * a / c))
    except Exception:
        pass
    return FALLBACK_SL_PCT


def sync_positions(ex, active_symbols):
    """عند الإقلاع: مزامنة المراكز الموجودة على المنصة مع حالة البوت."""
    with trade_lock:
        try:
            exch = {}
            for p in ex.fetch_positions():
                c = abs(float(p.get('contracts') or 0))
                if c > 0 and p.get('symbol'):
                    exch[p['symbol']] = p
        except Exception as e:
            log.error(f"[SYNC] {e}")
            return
        for sym in exch:
            if sym not in active_symbols:
                active_symbols.append(sym)
        for sym in list(active_symbols):
            with state_lock:
                st = dict(state.get(sym, new_sym_state()))
            if st.get("position") and sym not in exch:
                finalize_trade(ex, sym)
            elif st.get("position") and sym in exch:
                if st["position"].lower() != exch[sym].get("side") or (
                    exch[sym].get("info") or {}
                ).get("posSide") in ("long", "short"):
                    log.error("[SYNC] %s: position side/mode mismatch", sym)
                    continue
                contracts = abs(float(exch[sym].get('contracts')))
                with state_lock:
                    state[sym]["contracts"] = contracts
                st["contracts"] = contracts
                repair_protection(ex, sym, st, get_current_price(ex, sym))
            elif not st.get("position") and sym in exch and ADOPT_ORPHANS:
                p = exch[sym]
                if (p.get("info") or {}).get("posSide") in ("long", "short") or p.get("side") not in ("long", "short"):
                    log.error("[SYNC] %s: requires net position mode", sym)
                    continue
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
                    log.warning(f"[SYNC] {sym} تجاوز الهدف — إغلاق")
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
                    send(f"♻️ تم تبني مركز <code>{short(sym)}</code> ({side}) — SL {sl_pct * 100:.2f}% (ATR)")
        save_state()


def adopt_orphans(ex, active_symbols):
    """مراكز على المنصة بلا حالة في البوت: تبنٍّ بوقف ATR أو تنبيه."""
    with trade_lock:
        try:
            exch = {}
            for p in ex.fetch_positions():
                c = abs(float(p.get('contracts') or 0))
                if c > 0 and p.get('symbol'):
                    exch[p['symbol']] = p
        except Exception as e:
            log.warning(f"[ORPHAN] {e}")
            return
        for sym, p in exch.items():
            if (p.get("info") or {}).get("posSide") in ("long", "short") or p.get("side") not in ("long", "short"):
                log.error("[ORPHAN] %s: requires net position mode", sym)
                continue
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
                    send(f"⚠️ مركز غير مُدار على <code>{short(sym)}</code> ({side}) — التبنّي معطّل")
                continue
            sl_pct = adopt_sl_pct(ex, sym)
            if side == "LONG":
                sl, tp = entry * (1 - sl_pct), entry * (1 + sl_pct * RR_RATIO)
            else:
                sl, tp = entry * (1 + sl_pct), entry * (1 - sl_pct * RR_RATIO)
            cur = get_current_price(ex, sym)
            if cur and ((side == "LONG" and cur <= sl) or (side == "SHORT" and cur >= sl)):
                send(f"🚨 مركز يتيم <code>{short(sym)}</code> تجاوز الوقف — إغلاق فوري")
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
                send(f"♻️ تم تبني مركز يتيم <code>{short(sym)}</code> ({side}) — SL {sl_pct * 100:.2f}% (ATR)")
            else:
                send(f"🚨 مركز يتيم <code>{short(sym)}</code> وتعذر وضع الحماية — جاري الإغلاق")
                close_full(ex, sym)


# ==========================================
# مساعدات العرض
# ==========================================
def unrealized(ex, sym, v):
    price = get_current_price(ex, sym) or v["entry"]
    d = 1 if v["position"] == "LONG" else -1
    upnl = (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
    return price, upnl


def flags_text(v):
    f = ""
    if v.get("mode") == "sniper":
        f += " 🎯"
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
             f"🔄 آخر دورة: {loop_age_text()} | دورات: {loop_info['cycles']}",
             f"🔎 اليوم: تقييم {ev} | إشارات {sg} | فُتحت {op}  (/why للتفاصيل)",
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
            txt += f"\nآخر سبريد: {sp[1] * 100:.3f}% (الحد {MAX_SPREAD_PCT * 100:.2f}%)"
        return txt
    with diag_lock:
        d = json.loads(json.dumps(diag))
    ev = d["evaluated"]
    sig_txt = " | ".join(f"{STRAT_LABEL.get(k, k)} {v}" for k, v in d.get("signals_by", {}).items()) or "—"
    lines = [f"🔎 <b>لماذا لا توجد صفقات؟</b> — {d['date']}", "━━━━━━━━━━━━━━━",
             f"📊 تقييمات: {ev}",
             f"✅ إشارات: {d['signals']} ({sig_txt}) | 🟢 فُتحت: {d['opened']}",
             f"⚙️ البروفايل: <b>{PROFILE_NAME}</b> | الاختراق: {'مفعّل' if ENABLE_BREAKOUT else 'معطّل'} | KER: {'مفعّل' if ENABLE_KER_FILTER else 'معطّل'}"]
    if meta.get("paused"):
        lines.append("⏸️ <b>البوت موقوف (/resume)</b>")
    if ev:
        lines += ["", "<b>الأسباب العامة:</b>"]
        for r, n in sorted(d["reasons"].items(), key=lambda x: -x[1])[:8]:
            lines.append(f"• {label(r)}: {n} ({n / ev * 100:.0f}%)")
        staged = sum(d.get("reasons_pullback", {}).values())
        if staged:
            lines.append(f"\nوصلت للاستراتيجيات: {staged + d['signals']} شمعة")
        for key, title in (("reasons_pullback", "الارتداد"), ("reasons_breakout", "الاختراق")):
            items = sorted(d.get(key, {}).items(), key=lambda x: -x[1])[:4]
            if items:
                lines += ["", f"<b>{title}:</b>"]
                for r, n in items:
                    lines.append(f"• {label(r)}: {n}")
    else:
        lines.append("\nلا توجد تقييمات بعد (انتظر إغلاق شمعة 15 دقيقة).")
    if d["blocks"]:
        lines += ["", "<b>إشارات حُجبت بعد ظهورها:</b>"]
        for r, n in sorted(d["blocks"].items(), key=lambda x: -x[1]):
            lines.append(f"• {label(r)}: {n}")
    lines.append("\nلتفاصيل عملة: /why BTC")
    return "\n".join(lines)


def universe_text(active_symbols):
    u = universe_info
    names = ", ".join(short(x).split("/")[0] for x in list(active_symbols)[:70])
    lines = [f"🪙 <b>العملات المفحوصة</b> — {u.get('mode') or '—'} | العدد: {len(active_symbols)}",
             "━━━━━━━━━━━━━━━", names or "—"]
    if u.get("excluded"):
        lines += ["", "<b>مستبعد:</b>"]
        for k, n in u["excluded"].items():
            smp = u.get("samples", {}).get(k) or []
            lines.append(f"• {k}: {n}" + (f" ({', '.join(smp)})" if smp else ""))
    if u.get("categories"):
        lines.append(f"\nتصنيفات OKX: {u['categories']}")
    lines.append("\nلتقييد: UNIVERSE_MODE=core أو EXCLUDE_BASES في Render.")
    return "\n".join(lines)


def config_text():
    return "\n".join([
        f"⚙️ <b>الإعدادات</b> — البروفايل: <b>{PROFILE_NAME}</b> | نمط الدخول: <b>{ENTRY_MODE}</b>",
        "━━━━━━━━━━━━━━━",
        f"ATR الأدنى: {MIN_ATR_PCT * 100:.2f}%",
        f"ATR النسبي: ≥ {REL_ATR_RATIO:g}× (حد أدنى {ATR_HARD_FLOOR * 100:.2f}%)"
        + (" — معطّل" if REL_ATR_RATIO >= 50 else ""),
        f"KER: {'مفعّل' if ENABLE_KER_FILTER else 'معطّل'} | الحد {MIN_KER:g} | الفترة {KER_PERIOD} | ADX: {MIN_ADX:g}",
        f"ADX 4س قوي/ضعيف: {REGIME_ADX4_STRONG:g}/{REGIME_ADX4_WEAK:g} | 1س قوي: {REGIME_ADX1_STRONG:g}",
        f"الارتداد — حجم ≥ {MIN_VOL_MULT:g}× | RSI L {RSI_LONG_MIN:g}–{RSI_LONG_MAX:g} | S {RSI_SHORT_MIN:g}–{RSI_SHORT_MAX:g}",
        f"القرب من EMA21/50: {NEAR_EMA21_PCT * 100:.1f}% / {NEAR_EMA50_PCT * 100:.1f}%",
        f"الاختراق: {'مفعّل' if ENABLE_BREAKOUT else 'معطّل'} | نافذة {BO_LOOKBACK} | حجم ≥ {BO_VOL_MULT:g}× | أقصى مطاردة {BO_MAX_EXT_ATR:g}×ATR",
        f"أقصى سبريد: {MAX_SPREAD_PCT * 100:.2f}%",
        f"المخاطرة: {RISK_PCT_PER_TRADE}% | رافعة {LEVERAGE}x | سقف الهامش {MAX_MARGIN_USDT:.0f}",
        f"حد الدرجة: {MIN_SIGNAL_SCORE}/100 | عملات: {UNIVERSE_MODE} (حتى {MAX_UNIVERSE_SYMBOLS})",
        f"أقصى صفقات: {MAX_OPEN_POSITIONS} | نفس الاتجاه: {MAX_SAME_DIRECTION}",
        f"القناص: {'مفعّل' if ENABLE_SNIPER else 'معطّل'} | حد الدرجة {SNIPER['min_score']} | رافعة {SNIPER['leverage']}x | مخاطرة {SNIPER['risk_pct']}% | مدة {SNIPER['max_hold_minutes']}د",
        f"AI المحلي: {'نشط ✅' if AI_IS_ACTIVE else 'معطّل'} | فلتر الدخول: {'مفعّل' if USE_AI_ENTRY_FILTER else 'تشخيص فقط'}",
        "━━━━━━━━━━━━━━━",
        "للتعديل: متغيرات البيئة في Render ثم Redeploy."])


def positions_text(ex):
    opens = open_positions_snapshot()
    if not opens:
        return "📭 لا توجد صفقات مفتوحة."
    lines = ["📊 <b>الصفقات المفتوحة</b>", "━━━━━━━━━━━━━━━"]
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
        score = v.get("signal_score", 0)
        lines.append(
            f"🪙 <b>{short(sym)}</b> ({v['position']}) | Score: {score}\n"
            f"📥 {fmt(v['entry'])} → 📤 {fmt(price)}\n"
            f"💰 العائم: <b>{upnl:+.2f} USDT</b> ({pct:+.1f}%) | {r_now:+.2f}R\n"
            f"🧾 جزئيات: {v.get('realized_pnl', 0.0):+.2f}\n"
            f"🛑 {fmt(v['sl']) if v.get('sl') else '—'} | 🎯 {fmt(v['tp']) if v.get('tp') else '—'}\n"
            f"⏱ {duration}{flags_text(v)}\n────────────────")
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
        "🛡️ <b>ملخص المخاطر</b>", "━━━━━━━━━━━━━━━",
        f"💼 الرصيد: <b>{equity:,.2f} USDT</b>",
        f"📊 الحالة: {status}", "",
        f"🎯 المخاطرة: <b>{RISK_PCT_PER_TRADE}%</b> | سقف الهامش {MAX_MARGIN_USDT:.0f}",
        f"📐 الفعلية عند 0.8%/1.5%/2.5%: "
        f"<b>{effective_risk_pct(equity, 0.008):.2f}% / {effective_risk_pct(equity, 0.015):.2f}% / {effective_risk_pct(equity, 0.025):.2f}%</b>",
        f"📉 حد الخسارة: {DAILY_LOSS_LIMIT_PCT}% ({loss_limit:.1f} USDT)",
        f"📈 هدف الربح: {DAILY_PROFIT_TARGET_PCT}% ({profit_target:.1f} USDT)", "",
        f"🔢 المفتوحة: {len(opens)} / {MAX_OPEN_POSITIONS}",
        f"🟢 LONG: {longs} | 🔴 SHORT: {shorts}",
        f"📅 اليوم: <b>{snap['value']:+.2f} USDT</b> | العائم: {floating:+.2f}",
        "━━━━━━━━━━━━━━━"])


def daily_text(ex):
    snap = daily_snapshot()
    equity = get_equity(ex, force=True)
    total_upnl = sum(unrealized(ex, s, v)[1] for s, v in open_positions_snapshot().items())
    wr = (snap["wins"] / snap["trades"] * 100) if snap["trades"] else 0
    return "\n".join([
        f"📅 <b>ملخص اليوم</b> — {snap['date']}", "━━━━━━━━━━━━━━━",
        f"💰 المحقق: <b>{snap['value']:+.2f} USDT</b>",
        f"📈 العائم: <b>{total_upnl:+.2f} USDT</b>",
        f"📊 الإجمالي: <b>{snap['value'] + total_upnl:+.2f} USDT</b>", "",
        f"🔢 الصفقات: {snap['trades']}",
        f"✅ الرابحة: {snap['wins']} ({wr:.0f}%)",
        f"💼 الرصيد: {equity:,.2f} USDT", "━━━━━━━━━━━━━━━"])


def find_symbol(arg, active_symbols):
    arg = arg.upper().replace("/USDT", "").replace("USDT", "")
    for s in list(active_symbols):
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
    elif cmd == "/paper":
        send(paper_stats_text(), chat_id)
    elif cmd == "/config":
        send(config_text(), chat_id)
    elif cmd == "/universe":
        send(universe_text(active_symbols), chat_id)
    elif cmd == "/why":
        send(why_text(parts[1] if len(parts) > 1 else None, active_symbols), chat_id)
    elif cmd == "/ai":
        ai_analyze_trades(chat_id)
    elif cmd == "/export":
        send("⏳ جاري تصدير السجل والحالة...", chat_id)
        backup_to_telegram(chat_id)
    elif cmd == "/restore":
        send("أوقف الدخول بـ /pause، وتأكد من عدم وجود مراكز. أرسل الملف في الخاص "
             "مع كتابة <code>/restore confirm</code> في تعليقه.", chat_id)
    elif cmd == "/pause":
        with trade_lock:
            set_meta("paused", True)
        send("⏸️ تم إيقاف فتح صفقات جديدة.", chat_id)
    elif cmd == "/resume":
        with trade_lock:
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
        send("<b>📋 الأوامر:</b>\n\n"
             "/status - نظرة عامة\n"
             "/why - أسباب الرفض\n"
             "/why BTC - عملة محددة\n"
             "/config - الإعدادات\n"
             "/universe - العملات المفحوصة\n"
             "/pos - الصفقات المفتوحة\n"
             "/risk - المخاطر\n"
             "/daily - ملخص اليوم\n"
             "/stats - إحصائيات\n"
             "/paper - إشارات Paper\n"
             "/ai - تحليل AI\n"
             "/export - تصدير السجل\n"
             "/restore - استعادة نسخة\n"
             "/pause - إيقاف\n"
             "/resume - استئناف\n"
             "/close BTC - إغلاق عملة\n"
             "/closeall confirm - إغلاق الكل\n"
             "/help - هذه القائمة", chat_id)


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
                if chat_id not in AUTHORIZED_CHATS:
                    continue
                if "document" in msg:
                    handle_document_restore(msg, chat_id, ex2)
                    continue
                if not text.startswith("/"):
                    continue
                try:
                    handle_command(ex2, text, chat_id, active_symbols)
                except Exception as e:
                    log.exception(f"[TG CMD] {text[:80]}")
                    send("حدث خطأ داخلي. راجع سجل البوت.", chat_id)
        except Exception as e:
            log.warning("[TG POLL] %s", type(e).__name__)
            time.sleep(5)


def watchdog():
    """ينبه إذا توقفت الحلقة الرئيسية."""
    while True:
        time.sleep(60)
        last = loop_info["last_cycle_ts"]
        if not last:
            continue
        age = time.time() - last
        if age > WATCHDOG_SEC and not loop_info["alerted"]:
            loop_info["alerted"] = True
            send(f"🚨 <b>تحذير:</b> حلقة الفحص متوقفة منذ {int(age)}ث.\nآخر خطأ: {loop_info['last_error'] or '—'}")
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
             f"🔢 الصفقات: {snap['trades']} | رابحة: {snap['wins']} ({wr:.0f}%)\n"
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
# اكتشاف السوق الديناميكي
# ==========================================
universe_info = {"mode": "", "size": 0, "excluded": {}, "samples": {}, "categories": {}, "ts": 0}


def _market_category(market):
    c = (market.get("info") or {}).get("instCategory")
    return None if c in (None, "") else str(c)


def discover_universe(ex):
    """قائمة محدودة من عقود USDT للكريبتو فقط. بدون تصنيف صريح لا يُقبل إلا الأساسي."""
    core = [x for x in FUTURES_SYMBOLS if x in ex.markets]
    core_set = set(core)
    if UNIVERSE_MODE == "core":
        universe_info.update({"mode": "core", "size": len(core), "excluded": {}, "samples": {},
                              "categories": {}, "ts": time.time()})
        log.info(f"[UNIVERSE] وضع core: {len(core)} رمز")
        return core

    now_ms = ex.milliseconds()
    candidates, categories = [], {}
    excluded = {"قائمة سوداء": [], "غير كريبتو": [], "تصنيف مجهول": []}
    for sym, market in ex.markets.items():
        if not market.get("active", True) or not market.get("swap") or market.get("quote") != "USDT":
            continue
        if market.get("settle") not in (None, "USDT"):
            continue
        base = (market.get("base") or sym.split("/")[0]).upper()
        if base in EXCLUDE_BASES:
            excluded["قائمة سوداء"].append(base)
            continue
        cat = _market_category(market)
        categories[cat or "?"] = categories.get(cat or "?", 0) + 1
        if cat is None:
            if sym not in core_set:
                excluded["تصنيف مجهول"].append(base)
                continue
        elif cat != "1":
            excluded["غير كريبتو"].append(base)
            continue
        candidates.append((sym, market))

    volumes = {}
    try:
        tickers = ex.fetch_tickers([x[0] for x in candidates])
        for sym, ticker in tickers.items():
            qv = ticker.get("quoteVolume")
            if qv is None and ticker.get("baseVolume") and ticker.get("last"):
                qv = float(ticker["baseVolume"]) * float(ticker["last"])
            volumes[sym] = float(qv or 0)
    except Exception as exc:
        log.warning(f"[UNIVERSE] تعذر جلب الأحجام، الأساسية فقط: {exc}")
        return core

    ranked = sorted(candidates, key=lambda item: volumes.get(item[0], 0), reverse=True)
    liquid = [sym for sym, _ in ranked if volumes.get(sym, 0) > 0][:MAX_UNIVERSE_SYMBOLS]
    fresh = []
    if INCLUDE_NEW_MARKETS_DAYS > 0:
        cutoff = now_ms - INCLUDE_NEW_MARKETS_DAYS * 86400_000
        for sym, market in candidates:
            stamp = market.get("created") or (market.get("info") or {}).get("listTime")
            try:
                stamp = int(stamp)
                if stamp < 10_000_000_000:
                    stamp *= 1000
                if stamp >= cutoff and sym not in liquid and volumes.get(sym, 0) > 0:
                    fresh.append(sym)
            except (TypeError, ValueError):
                continue
    result = list(dict.fromkeys(liquid + fresh))[:MAX_UNIVERSE_SYMBOLS]
    allowed = {x[0] for x in candidates}
    for sym in core:
        if sym in allowed and sym not in result and len(result) < MAX_UNIVERSE_SYMBOLS:
            result.append(sym)
    if not result:
        result = core
    universe_info.update({
        "mode": "dynamic", "size": len(result), "ts": time.time(), "categories": categories,
        "excluded": {k: len(v) for k, v in excluded.items()},
        "samples": {k: sorted(set(v))[:6] for k, v in excluded.items()}})
    log.info(f"[UNIVERSE] {len(result)} عقد | تصنيفات={categories} | "
             f"مستبعد: " + "، ".join(f"{k} {len(v)}" for k, v in excluded.items()))
    return result


# ==========================================
# الحلقة الرئيسية
# ==========================================
def trading_loop():
    log.info(f"[INIT] Starting {VERSION} ...")
    if not USE_SANDBOX and not ALLOW_LIVE_TRADING:
        log.error("[INIT] Live trading requires ALLOW_LIVE_TRADING=true")
        return
    missing = [name for name, val in (
        ("OKX_API_KEY", OKX_API_KEY),
        ("OKX_SECRET", OKX_SECRET),
        ("OKX_PASSPHRASE", OKX_PASSPHRASE),
    ) if not val]
    if missing:
        log.error(f"[INIT] Missing credentials: {', '.join(missing)}")
        send("❌ بيانات اعتماد OKX غير مكتملة — راجع متغيرات البيئة.")
        return
    load_local_ai()

    threading.Thread(target=_send_worker, daemon=True).start()

    try:
        ex = init_exchange()
        load_state()
        migrate_journal()
        balance = get_equity(ex, force=True)
        mode_str = "تجريبي (Sandbox)" if USE_SANDBOX else "حقيقي"
        log.info(f"[OKX] Connected ({mode_str}). Equity: {balance}")
    except Exception as e:
        send(f"❌ فشل الاتصال: {e}")
        return

    active_symbols = discover_universe(ex)
    active_symbols = list(dict.fromkeys(active_symbols + list(open_positions_snapshot())))
    universe_refresh_ts = time.time()
    sync_positions(ex, active_symbols)
    threading.Thread(target=telegram_listener, args=(active_symbols,), daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    send(f"🚀 <b>تم تشغيل البوت {VERSION}</b>\n━━━━━━━━━━━━━━━\n"
         f"⚙️ الوضع: {mode_str}\n🪙 العملات: {len(active_symbols)}\n"
         f"📊 أقصى صفقات: {MAX_OPEN_POSITIONS}\n"
         f"💰 المخاطرة: {RISK_PCT_PER_TRADE}% | رافعة: {LEVERAGE}x\n"
         f"🎚️ البروفايل: {PROFILE_NAME} | القناص: {'مفعّل' if ENABLE_SNIPER else 'معطّل'}\n"
         f"🧠 AI المحلي: {'نشط ✅' if AI_IS_ACTIVE else 'معطّل'} | حد الدرجة: {MIN_SIGNAL_SCORE}/100\n"
         f"🔎 /why أسباب | ⚙️ /config إعدادات | 📱 /help\n━━━━━━━━━━━━━━━")
    if PROFILE_WARNING:
        send(f"⚠️ {PROFILE_WARNING}")

    loop_info["last_cycle_ts"] = time.time()
    last_orphan_check = time.time()
    last_backup_check = time.time()
    fail_streak = 0
    while True:
        start = time.time()
        cycle_errors = 0
        try:
            if time.time() - universe_refresh_ts >= UNIVERSE_REFRESH_SEC:
                refreshed = discover_universe(ex)
                if refreshed:
                    with state_lock:
                        held = [sym for sym, st in state.items() if st.get("position")]
                    active_symbols[:] = list(dict.fromkeys(refreshed + held))
                    universe_refresh_ts = time.time()
                    log.info(f"[UNIVERSE] تحديث: {len(active_symbols)} رمز")
            refresh_prices(ex, active_symbols)
            if USE_BTC_FILTER:
                btc_bias(ex)
            held_symbols = open_positions_snapshot()
            scan_order = sorted(active_symbols, key=lambda sym: sym not in held_symbols)
            for sym in scan_order:
                try:
                    process_symbol(ex, sym)
                except Exception as e:
                    cycle_errors += 1
                    loop_info["last_error"] = f"{sym}: {str(e)[:150]}"
                    log.error(f"[LOOP] {sym}: {e}")
            if time.time() - last_orphan_check >= ORPHAN_CHECK_SEC:
                adopt_orphans(ex, active_symbols)
                last_orphan_check = time.time()
            if time.time() - last_backup_check >= BACKUP_INTERVAL_SEC:
                backup_to_telegram()
                last_backup_check = time.time()
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


# ==========================================
# نقطة الدخول
# ==========================================
if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    if not acquire_single_instance():
        log.error("نسخة أخرى من البوت تعمل بالفعل — إيقاف.")
        sys.exit(1)
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
