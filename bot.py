# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V49 (Liquidity-Aware Multi-Horizon)

الجديد عن V48:
1) محرك السيولة (Liquidity Engine):
   - كشف كنس السيولة (Sweep) في آخر شمعة مغلقة.
   - كشف برك السيولة (Equal Highs/Lows).
   - كشف فجوات FVG غير المملوءة (Fair Value Gap).
   - قياس اختلال دفتر الأوامر (Order Book Imbalance).
2) استراتيجية ثالثة "liquidity_sweep": دخول بعد كنس + رفض + توافق اتجاه.
3) مكافأة Score (حتى +15) لكل الاستراتيجيات عند توفر سياق سيولة (Sweep/FVG/OB).
4) أمر /liq SYMBOL لتقرير سيولة تفصيلي.
5) Swap اختياري (AUTO_SWAP_ON_BETTER): عند امتلاء الأماكن، يُغلق أضعف صفقة
   خاسرة (غير محمية) لصالح إشارة أقوى بفارق SWAP_SCORE_MARGIN درجة.

تنبيهات:
- AUTO_SWAP_ON_BETTER معطّل افتراضياً — فعّله فقط بعد اختبار Sandbox مطوّل.
- على الحقيقي: swing فقط افتراضياً. فعّل الآخرين عبر HORIZONS=...
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
# الإعدادات العامة
# ==========================================
VERSION = "V49"
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
MIN_SIGNAL_SCORE = int(env_float("MIN_SIGNAL_SCORE", 74))
INCLUDE_NEW_MARKETS_DAYS = int(env_float("INCLUDE_NEW_MARKETS_DAYS", 45))
LEVERAGE = int(env_float("LEVERAGE", 5))
MARGIN_MODE = "isolated"
MAX_OPEN_POSITIONS = int(env_float("MAX_OPEN_POSITIONS", 5))
REENTRY_COOLDOWN_SEC = 1500

USE_RISK_SIZING = True
RISK_PCT_PER_TRADE = env_float("RISK_PCT_PER_TRADE", 0.75)
POSITION_SIZE_USDT = 50
MIN_MARGIN_USDT = 12
MAX_MARGIN_USDT = env_float("MAX_MARGIN_USDT", 130)
MAX_MARGIN_PCT_EQUITY = 0.30
MAX_PORTFOLIO_RISK_PCT = env_float("MAX_PORTFOLIO_RISK_PCT", 3.0)

ATR_SL_MULT = 1.55
RR_RATIO = 2.3
MIN_SL_PCT = 0.0065
MAX_SL_PCT = 0.032
TRAIL_ATR_MULT = 1.05
BE_TRIGGER_R = 0.9
FALLBACK_SL_PCT = 0.017

FEE_RATE = 0.0005
EST_SLIPPAGE_PCT = env_float("EST_SLIPPAGE_PCT", 0.0003)
BE_FEE_BUFFER = 2 * FEE_RATE + EST_SLIPPAGE_PCT
MAX_COST_R = env_float("MAX_COST_R", 0.22)

DAILY_LOSS_LIMIT_PCT = env_float("DAILY_LOSS_LIMIT_PCT", 4.0)
DAILY_PROFIT_TARGET_PCT = env_float("DAILY_PROFIT_TARGET_PCT", 6.0)
INCLUDE_FLOATING_IN_LOSS_LIMIT = True
MAX_SAME_DIRECTION = 3
MAX_FUNDING_RATE = 0.00045
USE_BTC_FILTER = True

# ---------- محرك السيولة (V49) ----------
ENABLE_LIQUIDITY_SWEEP = env_flag("ENABLE_LIQUIDITY_SWEEP", True)
LIQ_SWEEP_LOOKBACK     = int(env_float("LIQ_SWEEP_LOOKBACK", 20))
LIQ_SWEEP_TOL_PCT      = env_float("LIQ_SWEEP_TOL_PCT", 0.0015)
LIQ_WICK_RATIO         = env_float("LIQ_WICK_RATIO", 0.5)
LIQ_POOL_TOL_PCT       = env_float("LIQ_POOL_TOL_PCT", 0.002)
LIQ_FVG_LOOKBACK       = int(env_float("LIQ_FVG_LOOKBACK", 30))
USE_ORDERBOOK          = env_flag("USE_ORDERBOOK", True)
ORDERBOOK_DEPTH        = int(env_float("ORDERBOOK_DEPTH", 20))
OB_IMBALANCE_LONG      = env_float("OB_IMBALANCE_LONG", 0.62)
OB_IMBALANCE_SHORT     = env_float("OB_IMBALANCE_SHORT", 0.38)
AUTO_SWAP_ON_BETTER    = env_flag("AUTO_SWAP_ON_BETTER", False)
SWAP_SCORE_MARGIN      = int(env_float("SWAP_SCORE_MARGIN", 8))
SWAP_MIN_HOLD_H        = env_float("SWAP_MIN_HOLD_H", 1.0)

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
    "active": {
        "min_atr_pct": 0.0022, "min_adx": 18, "min_vol_mult": 0.8,
        "regime_adx4_strong": 24, "regime_adx1_strong": 20, "regime_adx4_weak": 15,
        "rsi_long_min": 35, "rsi_long_max": 67, "rsi_short_min": 33, "rsi_short_max": 65,
        "near_ema21_pct": 0.016, "near_ema50_pct": 0.024, "max_spread_pct": 0.003,
        "rel_atr_ratio": 0.9, "atr_hard_floor": 0.0015,
        "bo_vol_mult": 1.2, "bo_max_ext_atr": 3.0,
    },
}
_default_profile = "active" if USE_SANDBOX else "conservative"
PROFILE_NAME = os.getenv("PROFILE", _default_profile).strip().lower()
PROFILE_WARNING = ""
if PROFILE_NAME not in PROFILES:
    PROFILE_WARNING = f"بروفايل غير معروف '{PROFILE_NAME}' — استُخدم {_default_profile}"
    PROFILE_NAME = _default_profile
if not USE_SANDBOX and PROFILE_NAME == "active":
    PROFILE_WARNING = "البروفايل active ممنوع على الحساب الحقيقي — استُخدم balanced"
    PROFILE_NAME = "balanced"

HORIZON_DEFS = {
    "scalp": {
        "label": "قصير ⚡", "tfs": ("5m", "15m", "1h"),
        "leverage": int(env_float("SCALP_LEVERAGE", 5)),
        "risk_pct": env_float("SCALP_RISK_PCT", 0.40),
        "atr_sl_mult": 1.35, "rr": 1.8, "min_sl": 0.0065, "max_sl": 0.0200,
        "trail_atr": 0.90, "be_r": 0.70,
        "p1_r": 1.0, "p1_pct": 0.50, "p2_r": 1.6, "p2_pct": 0.50,
        "max_hold_h": env_float("SCALP_MAX_HOLD_H", 3),
        "cooldown": 900, "min_score": int(env_float("SCALP_MIN_SCORE", 78)),
        "max_open": 2, "max_funding": 0.0006,
        "max_entry_slip": 0.003, "max_signal_age_s": 240,
        "mult": {"min_atr_pct": 0.55, "atr_hard_floor": 0.55,
                 "near_ema21_pct": 0.5, "near_ema50_pct": 0.5},
        "abs": {"max_atr_pct": 0.020},
    },
    "swing": {
        "label": "متوسط 🎯", "tfs": ("15m", "1h", "4h"),
        "leverage": LEVERAGE, "risk_pct": RISK_PCT_PER_TRADE,
        "atr_sl_mult": ATR_SL_MULT, "rr": RR_RATIO, "min_sl": MIN_SL_PCT, "max_sl": MAX_SL_PCT,
        "trail_atr": TRAIL_ATR_MULT, "be_r": BE_TRIGGER_R,
        "p1_r": 1.3, "p1_pct": 0.40, "p2_r": 2.1, "p2_pct": 0.50,
        "max_hold_h": env_float("SWING_MAX_HOLD_H", 36),
        "cooldown": REENTRY_COOLDOWN_SEC, "min_score": MIN_SIGNAL_SCORE,
        "max_open": 3, "max_funding": MAX_FUNDING_RATE,
        "max_entry_slip": 0.004, "max_signal_age_s": 600,
        "mult": {}, "abs": {"max_atr_pct": 0.045},
    },
    "position": {
        "label": "طويل 🏔️", "tfs": ("4h", "1d", "1d"),
        "leverage": int(env_float("POSITION_LEVERAGE", 3)),
        "risk_pct": env_float("POSITION_RISK_PCT", 0.60),
        "atr_sl_mult": 2.0, "rr": 4.5, "min_sl": 0.020, "max_sl": 0.090,
        "trail_atr": 2.2, "be_r": 1.2,
        "p1_r": 1.8, "p1_pct": 0.30, "p2_r": 3.2, "p2_pct": 0.40,
        "max_hold_h": env_float("POSITION_MAX_HOLD_H", 240),
        "cooldown": 21600, "min_score": int(env_float("POSITION_MIN_SCORE", 74)),
        "max_open": 2, "max_funding": 0.0003,
        "max_entry_slip": 0.008, "max_signal_age_s": 1800,
        "mult": {"min_atr_pct": 3.0, "atr_hard_floor": 3.0,
                 "near_ema21_pct": 2.5, "near_ema50_pct": 2.5},
        "abs": {"max_atr_pct": 0.080},
    },
}
HORIZON_ORDER = ["position", "swing", "scalp"]
_hz_env = os.getenv("HORIZONS", "scalp,swing,position" if USE_SANDBOX else "swing")
ENABLED_HORIZONS = [h for h in HORIZON_ORDER if h in {x.strip().lower() for x in _hz_env.split(",")}]
if not ENABLED_HORIZONS:
    ENABLED_HORIZONS = ["swing"]


def build_params(name):
    hd = HORIZON_DEFS[name]
    P = dict(PROFILES[PROFILE_NAME])
    for k, m in hd.get("mult", {}).items():
        P[k] = P[k] * m
    P.update(hd.get("abs", {}))
    for k in list(P):
        keys = [f"{name.upper()}_{k.upper()}"] + ([k.upper()] if name == "swing" else [])
        for ek in keys:
            if os.getenv(ek) is not None:
                P[k] = env_float(ek, P[k])
    return P


PARAMS = {n: build_params(n) for n in HORIZON_DEFS}

ENABLE_BREAKOUT = env_flag("ENABLE_BREAKOUT", USE_SANDBOX)
BO_LOOKBACK = int(env_float("BO_LOOKBACK", 20))
BO_BODY_POS = env_float("BO_BODY_POS", 0.6)
BO_RSI_LONG_MIN = env_float("BO_RSI_LONG_MIN", 52)
BO_RSI_LONG_MAX = env_float("BO_RSI_LONG_MAX", 72)
BO_RSI_SHORT_MIN = env_float("BO_RSI_SHORT_MIN", 28)
BO_RSI_SHORT_MAX = env_float("BO_RSI_SHORT_MAX", 48)

WATCHDOG_SEC = 300
TF_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

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
_last_spread = {}
_lock_fh = None


def hz_of(st):
    h = (st or {}).get("horizon") or "swing"
    return h if h in HORIZON_DEFS else "swing"


# ---------- التشخيص ----------
def _diag_blank(date=""):
    return {"date": date, "evaluated": 0, "signals": 0, "opened": 0, "signals_by": {}, "by_h": {},
            "reasons": {}, "reasons_pullback": {}, "reasons_breakout": {}, "reasons_liquidity": {},
            "blocks": {}}


diag = _diag_blank()
STRAT_LABEL = {"pullback": "الارتداد", "breakout": "الاختراق",
               "liquidity_sweep": "كنس السيولة", "adopted": "متبنّاة", "legacy": "قديمة"}
_last_reason = {}
loop_info = {"last_cycle_ts": 0.0, "cycles": 0, "last_duration": 0.0, "last_error": "", "alerted": False}

REASON_LABELS = {
    "regime_range": "سوق عرضي (Range)",
    "insufficient_data": "بيانات غير كافية",
    "indicators_nan": "مؤشرات غير جاهزة",
    "atr_low": "تذبذب منخفض (ATR)",
    "atr_high": "تذبذب مرتفع جداً",
    "low_adx": "قوة اتجاه ضعيفة (ADX)",
    "low_volume": "حجم تداول ضعيف",
    "direction_mismatch": "اتجاه إطار الاتجاه غير متوافق",
    "ema_misaligned": "EMA غير متوافقة (إطار الدخول/التأكيد)",
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
    "signal_stale": "الإشارة قديمة (الشمعة أُغلقت منذ مدة)",
    "cost_too_high": "تكلفة التنفيذ (رسوم+انزلاق) مرتفعة نسبة للوقف",
    "portfolio_heat": "سقف المخاطرة الكلية للمحفظة",
    "horizon_limit": "حد الصفقات لهذا الأفق",
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
    "bo_no_breakout": "لا اختراق لقمة/قاع آخر شموع",
    "bo_trend_mismatch": "إطار التأكيد لا يدعم الاختراق",
    "bo_low_volume": "حجم الاختراق ضعيف",
    "bo_weak_candle": "شمعة الاختراق ضعيفة الإغلاق",
    "bo_rsi": "RSI خارج نطاق الاختراق",
    "bo_extended": "السعر بعيد عن EMA21 (مطاردة)",
    "score_low": "درجة الفرصة أقل من الحد المطلوب",
    "liq_no_sweep": "لا كنس سيولة حديث",
    "liq_direction_mismatch": "الكنس عكس الاتجاه العام",
    "liq_low_volume": "حجم الكنس ضعيف",
    "liq_rsi_overbought": "RSI تشبع شرائي (لا نشتري)",
    "liq_rsi_oversold": "RSI تشبع بيعي (لا نبيع)",
    "disabled": "معطّل",
}


def label(r):
    if "@" in r:
        base, h = r.rsplit("@", 1)
        return f"{label(base)} [{HORIZON_DEFS.get(h, {}).get('label', h)}]"
    if r.startswith("signal:"):
        name = r.split(":", 1)[1]
        return f"إشارة ✅ ({STRAT_LABEL.get(name, name)})"
    return REASON_LABELS.get(r, r)


def _diag_roll_locked():
    today = str(datetime.now(KSA_TZ).date())
    if diag["date"] != today:
        diag.clear()
        diag.update(_diag_blank(today))


def _bh(h):
    return diag["by_h"].setdefault(h, {"evaluated": 0, "signals": 0, "opened": 0})


def note_eval(sym, signal, reasons, h="swing"):
    with diag_lock:
        _diag_roll_locked()
        diag["evaluated"] += 1
        _bh(h)["evaluated"] += 1
        if signal:
            diag["signals"] += 1
            _bh(h)["signals"] += 1
            name = signal.get("strategy", "?")
            diag["signals_by"][name] = diag["signals_by"].get(name, 0) + 1
            _last_reason[sym] = (time.time(), f"signal:{name}@{h}")
            return
        if "common" in reasons:
            r = reasons["common"]
            rk = f"{r}@{h}"
            diag["reasons"][rk] = diag["reasons"].get(rk, 0) + 1
            text = label(rk)
        else:
            parts = []
            for strat in ("pullback", "breakout", "liquidity"):
                if strat in reasons:
                    r = reasons[strat]
                    key = f"reasons_{strat}"
                    rk = f"{r}@{h}"
                    diag[key][rk] = diag[key].get(rk, 0) + 1
                    parts.append(f"{STRAT_LABEL.get(strat, strat)}: {label(r)}")
            text = f"[{HORIZON_DEFS[h]['label']}] " + " | ".join(parts)
        _last_reason[sym] = (time.time(), text)


def note_block(sym, reason):
    with diag_lock:
        _diag_roll_locked()
        diag["blocks"][reason] = diag["blocks"].get(reason, 0) + 1
        _last_reason[sym] = (time.time(), reason)


def note_opened(h="swing"):
    with diag_lock:
        _diag_roll_locked()
        diag["opened"] += 1
        _bh(h)["opened"] += 1


def set_last_reason(sym, reason):
    with diag_lock:
        _last_reason[sym] = (time.time(), reason)
