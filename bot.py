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
    # V49
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
        "version": VERSION, "profile": PROFILE_NAME, "horizons": ENABLED_HORIZONS,
        "paused": bool(meta.get("paused")),
        "loop_age_sec": round(time.time() - last, 1) if last else None,
        "cycles": loop_info["cycles"],
        "last_cycle_duration": round(loop_info["last_duration"], 1),
        "last_error": loop_info["last_error"], "today": d,
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
        "opened_ts": 0, "leg_ts": 0, "last_exit_ts": 0, "cooldown_sec": REENTRY_COOLDOWN_SEC,
        "realized_pnl": 0.0, "breakeven_hit": False,
        "partial_1_taken": False, "partial_2_taken": False,
        "last_notified_sl": None, "regime": "unknown", "strategy": "",
        "planned_entry": None, "realized_fees_est": 0.0,
        "horizon": "swing", "signal_score": 0,
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


def reset_sym_state(sym, cooldown=True, cooldown_sec=None):
    with state_lock:
        s = new_sym_state()
        s["last_exit_ts"] = time.time() if cooldown else 0
        if cooldown_sec:
            s["cooldown_sec"] = cooldown_sec
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
                  "pnl", "r_multiple", "reason", "regime", "duration_min", "strategy",
                  "fees", "fees_source", "stop_slip_usd", "stop_slip_r", "entry_slip_usd", "risk_usd",
                  "horizon", "score"]


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
                if not r.get("horizon"):
                    r["horizon"] = "swing"
            with open(JOURNAL_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=JOURNAL_FIELDS, extrasaction="ignore", restval="")
                w.writeheader()
                w.writerows(rows)
            log.info(f"[JOURNAL] تم ترحيل {len(rows)} صفقة")
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
    if "horizon" not in df:
        df["horizon"] = "swing"
    df["horizon"] = df["horizon"].fillna("swing").replace("", "swing")
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
    lines = ["📈 <b>إحصائيات الأداء الكلية</b>", "━━━━━━━━━━━━━━━",
             f"🔢 عدد الصفقات: {n}", f"✅ نسبة الربح: {wr:.1f}%",
             f"💰 الصافي: <b>{df['pnl'].sum():+.2f} USDT</b>",
             f"⚖️ Profit Factor: {pf_txt}", f"📐 متوسط R: {avg_r:+.2f}",
             f"🟢 متوسط الربح: {avg_win:+.2f} | 🔴 متوسط الخسارة: {avg_loss:+.2f}",
             f"📉 أقصى تراجع: {max_dd:.2f} USDT", "", "<b>حسب الأفق الزمني:</b>"]
    for name, g in df.groupby("horizon"):
        lines.append(_group_line(HORIZON_DEFS.get(name, {}).get("label", name), g))
    lines += ["", "<b>حسب الاستراتيجية:</b>"]
    for name, g in df.groupby("strategy"):
        lines.append(_group_line(STRAT_LABEL.get(name, name), g))
    if "side" in df:
        lines += ["", "<b>حسب الاتجاه:</b>"]
        for name, g in df.groupby("side"):
            lines.append(_group_line(name, g))
    sc = _num(df, "score")
    if sc.notna().sum() >= 10:
        bucket = pd.cut(sc, [0, 75, 80, 85, 101], right=False, labels=["<75", "75-79", "80-84", "85+"])
        lines += ["", "<b>حسب درجة الفرصة:</b>"]
        for name, g in df.groupby(bucket, observed=True):
            lines.append(_group_line(str(name), g))
    fees = _num(df, "fees").dropna()
    stop_usd = _num(df, "stop_slip_usd").dropna()
    stop_r = _num(df, "stop_slip_r").dropna()
    entry_slip = _num(df, "entry_slip_usd").dropna()
    n_actual = int((df["fees_source"] == "actual").sum()) if "fees_source" in df else 0
    if len(fees) or len(stop_usd) or len(entry_slip):
        lines += ["", "<b>تكلفة التنفيذ:</b>"]
        if len(fees):
            lines.append(f"🧾 الرسوم: {fees.sum():.2f} USDT ({n_actual} فعلية / {len(fees) - n_actual} تقديرية)")
        if len(stop_usd):
            lines.append(f"⚡ انزلاق الوقف: متوسط {stop_usd.mean():.2f} USDT ({stop_r.mean():.2f}R)")
        if len(entry_slip):
            lines.append(f"↘️ انزلاق الدخول: إجمالي {entry_slip.sum():.2f} | متوسط {entry_slip.mean():.2f} USDT")
    if n < 30:
        lines += ["", f"⚠️ العينة صغيرة ({n} صفقة) — لا تعتمد قبل 30 صفقة لكل أفق."]
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)


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
    "Time Stop": "انتهاء المدة دون تأمين ⏱️",
    "Manual": "إغلاق يدوي 🖐️",
    "Emergency": "إغلاق طارئ 🚨",
    "Manual/External": "إغلاق من المنصة ⚡",
    "Swap": "استبدال بفرصة أقوى 🔄",
}


def effective_risk_pct(equity, sl_pct, h="swing"):
    if equity <= 0 or sl_pct <= 0:
        return 0.0
    Hc = HORIZON_DEFS[h]
    risk_usd = equity * Hc["risk_pct"] / 100.0
    margin = risk_usd / (sl_pct * Hc["leverage"])
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    margin = min(cap, margin)
    return margin * sl_pct * Hc["leverage"] / equity * 100.0


def msg_entry(sym, side, price, sl, tp, margin, real_order, regime="", strategy="", equity=None,
              horizon="swing", score=None):
    Hc = HORIZON_DEFS[horizon]
    lev = Hc["leverage"]
    title = "🟢 <b>توصية شراء (LONG)</b>" if side == "LONG" else "🔴 <b>توصية بيع (SHORT)</b>"
    status = "✅ <b>تم فتح الصفقة آلياً</b>" if real_order else "⚠️ <b>إشارة فقط</b>"
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * lev
    reward_usd = margin * (tp_pct / 100) * lev
    risk_pct_txt = ""
    if equity and equity > 0:
        pct = risk_usd / equity * 100
        risk_pct_txt = f" = {pct:.2f}% من الرصيد"
        if pct < Hc["risk_pct"] * 0.5:
            risk_pct_txt += f" ⚠️ أقل من المستهدف ({Hc['risk_pct']}%)"
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    regime_txt = f"\n⏳ <b>الأفق:</b> {Hc['label']} ({'/'.join(dict.fromkeys(Hc['tfs']))})"
    if regime:
        regime_txt += f"\n📡 <b>نظام السوق:</b> {regime}"
    if strategy:
        regime_txt += f"\n🧠 <b>الاستراتيجية:</b> {STRAT_LABEL.get(strategy, strategy)}"
    if score is not None:
        regime_txt += f"\n🎚️ <b>درجة الفرصة:</b> {score}/100"
    return (f"{title}\n{status}{regime_txt}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <b>الزوج:</b> <code>{short(sym)}</code>\n"
            f"💵 <b>سعر الدخول:</b> <code>{fmt(price)}</code>\n"
            f"⚙️ <b>الرافعة:</b> {lev}x | <b>الهامش:</b> {margin:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 <b>وقف الخسارة:</b> <code>{fmt(sl)}</code> ({sl_pct:.2f}%)\n"
            f"📉 <i>المخاطرة: {risk_usd:.2f} USDT{risk_pct_txt}</i>\n"
            f"🎯 <b>الهدف:</b> <code>{fmt(tp)}</code> ({tp_pct:.2f}%)\n"
            f"📈 <i>العائد المتوقع: +{reward_usd:.2f} USDT</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ {now_str}")


def msg_exit(sym, side, exit_price, entry_price, total_pnl, final_leg_pnl, margin, r_mult, reason, strategy="",
             fees=None, fees_src="", stop_slip_usd=None, stop_slip_r=None, horizon="swing"):
    pct = (total_pnl / margin * 100) if margin else 0.0
    if total_pnl > 0:
        title = "🟢 <b>إغلاق بربح</b>"
    elif total_pnl < 0:
        title = "🔴 <b>إغلاق بخسارة</b>"
    else:
        title = "⚪ <b>إغلاق متعادل</b>"
    extra_lines = ""
    if fees is not None:
        extra_lines += f"🧾 الرسوم: {fees:.2f} USDT ({'فعلية' if fees_src == 'actual' else 'تقديرية'})\n"
    if stop_slip_usd is not None:
        if stop_slip_usd >= 0:
            extra_lines += f"⚡ انزلاق الوقف: خسارة إضافية {stop_slip_usd:.2f} USDT ({stop_slip_r:.2f}R)\n"
        else:
            extra_lines += f"⚡ انزلاق الوقف لصالحك: {abs(stop_slip_usd):.2f} USDT\n"
    return (f"{title}\n━━━━━━━━━━━━━━━\n"
            f"🪙 <code>{short(sym)}</code> ({side})\n"
            f"📥 الدخول: <code>{fmt(entry_price)}</code>\n"
            f"📤 الخروج: <code>{fmt(exit_price)}</code>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📊 على الهامش: {pct:+.2f}% | R: {r_mult:+.2f}\n"
            f"💰 الصافي الكلي: <b>{total_pnl:+.2f} USDT</b>\n"
            f"🧩 الجزء الأخير: {final_leg_pnl:+.2f} USDT\n"
            f"📝 السبب: {REASONS.get(reason, reason)}\n"
            f"⏳ الأفق: {HORIZON_DEFS.get(horizon, {}).get('label', horizon)}\n"
            f"🧠 الاستراتيجية: {STRAT_LABEL.get(strategy, strategy or '—')}\n"
            f"{extra_lines}"
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
# V49 — محرك السيولة (Liquidity Engine)
# ==========================================
def detect_liquidity_sweep(df, lookback=None, tol_pct=None, wick_ratio=None):
    """يكشف كنس السيولة في آخر شمعة مغلقة.
    bullish_sweep: الذيل السفلي كسر قاع سوينغ ثم أُغلق فوقه (فرصة LONG).
    bearish_sweep: الذيل العلوي كسر قمة سوينغ ثم أُغلق تحتها (فرصة SHORT).
    """
    lookback = lookback or LIQ_SWEEP_LOOKBACK
    tol_pct = tol_pct or LIQ_SWEEP_TOL_PCT
    wick_ratio = wick_ratio or LIQ_WICK_RATIO
    if len(df) < lookback + 5:
        return None, {}
    last = df.iloc[-2]
    window = df.iloc[-(lookback + 2):-2]

    prev_low = float(window["low"].min())
    prev_high = float(window["high"].max())
    low, high = float(last["low"]), float(last["high"])
    open_, close = float(last["open"]), float(last["close"])
    rng = high - low
    if rng <= 0:
        return None, {}
    body = abs(close - open_)
    lower_wick = min(open_, close) - low
    upper_wick = high - max(open_, close)
    tol = close * tol_pct

    if low < prev_low - tol and close > prev_low and lower_wick > body * wick_ratio:
        return "bullish_sweep", {
            "swept_level": prev_low,
            "wick_depth_pct": round((prev_low - low) / prev_low * 100, 3),
        }
    if high > prev_high + tol and close < prev_high and upper_wick > body * wick_ratio:
        return "bearish_sweep", {
            "swept_level": prev_high,
            "wick_depth_pct": round((high - prev_high) / prev_high * 100, 3),
        }
    return None, {}


def detect_liquidity_pools(df, lookback=50, tol_pct=None):
    tol_pct = tol_pct or LIQ_POOL_TOL_PCT
    if len(df) < lookback + 5:
        return {"equal_highs": [], "equal_lows": []}
    window = df.iloc[-(lookback + 2):-2]
    highs, lows = window["high"].values, window["low"].values
    ref = float(window["close"].iloc[-1])
    tol = ref * tol_pct
    eq_highs, eq_lows = [], []
    n = len(highs)
    for i in range(n - 1):
        for j in range(i + 1, min(i + 10, n)):
            if abs(highs[i] - highs[j]) < tol:
                eq_highs.append(float(highs[i]))
            if abs(lows[i] - lows[j]) < tol:
                eq_lows.append(float(lows[i]))
    return {
        "equal_highs": sorted(set(eq_highs))[-3:],
        "equal_lows": sorted(set(eq_lows))[-3:],
        "nearest_above": float(window["high"].max()),
        "nearest_below": float(window["low"].min()),
    }


def detect_fvg(df, lookback=None):
    """FVG غير مملوءة (آخر 5). bullish: بين c1.high و c3.low. bearish: بين c3.high و c1.low."""
    lookback = lookback or LIQ_FVG_LOOKBACK
    if len(df) < lookback + 3:
        return []
    gaps = []
    window = df.iloc[-(lookback + 3):]
    for i in range(2, len(window)):
        c1, c3 = window.iloc[i - 2], window.iloc[i]
        if c1["high"] < c3["low"]:
            gaps.append({"type": "bullish", "low": float(c1["high"]),
                         "high": float(c3["low"]),
                         "mid": (float(c1["high"]) + float(c3["low"])) / 2})
        elif c1["low"] > c3["high"]:
            gaps.append({"type": "bearish", "low": float(c3["high"]),
                         "high": float(c1["low"]),
                         "mid": (float(c3["high"]) + float(c1["low"])) / 2})
    current = float(df.iloc[-2]["close"])
    alive = []
    for g in gaps:
        if g["type"] == "bullish" and current < g["low"]:
            continue
        if g["type"] == "bearish" and current > g["high"]:
            continue
        alive.append(g)
    return alive[-5:]


def fetch_orderbook_imbalance(ex, sym, depth=None):
    if not USE_ORDERBOOK:
        return None
    depth = depth or ORDERBOOK_DEPTH
    try:
        ob = ex.fetch_order_book(sym, limit=depth)
        bids, asks = ob.get("bids", []), ob.get("asks", [])
        bv = sum(float(b[1]) for b in bids) if bids else 0.0
        av = sum(float(a[1]) for a in asks) if asks else 0.0
        total = bv + av
        if total <= 0:
            return None
        return {"bid_vol": bv, "ask_vol": av, "ratio": bv / total,
                "bid_ask_ratio": (bv / av) if av > 0 else float("inf")}
    except Exception as e:
        log.debug(f"[ORDERBOOK] {sym}: {e}")
        return None


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


def compute_margin(equity, sl_pct, h="swing"):
    if equity <= 0:
        return 0.0
    if not USE_RISK_SIZING:
        return float(POSITION_SIZE_USDT)
    Hc = HORIZON_DEFS[h]
    risk_usd = equity * Hc["risk_pct"] / 100.0
    margin = risk_usd / (sl_pct * Hc["leverage"])
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    sized = min(cap, margin)
    return sized if sized >= MIN_MARGIN_USDT else 0.0


def get_open_position(ex, sym):
    for p in ex.fetch_positions([sym]):
        contracts = abs(float(p.get('contracts') or 0))
        if contracts > 0:
            return {"side": "LONG" if p.get('side') == 'long' else "SHORT",
                    "contracts": contracts,
                    "entry": float(p.get('entryPrice') or 0),
                    "margin": float(p.get('initialMargin') or 0)}
    return None


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


def set_leverage_checked(ex, sym, lev):
    last_err = None
    for _ in range(2):
        try:
            ex.set_leverage(lev, sym, params={'mgnMode': MARGIN_MODE})
            return True, ""
        except Exception as e:
            last_err = e
            time.sleep(0.6)
    return False, str(last_err)


def open_position(ex, sym, side, price, sl, tp, margin, lev):
    try:
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
    h = hz_of(pos_data)
    d = 1 if side == "LONG" else -1
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
    if fees_actual is not None and not use_actual:
        log.warning(f"[FEES] {sym}: الفعلية {fees_actual:.4f} بعيدة عن التقدير {fees_est_total:.4f}")
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
        "horizon": h, "score": pos_data.get("signal_score", "")})
    reset_sym_state(sym, cooldown=True, cooldown_sec=HORIZON_DEFS[h]["cooldown"])
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, total, final_leg, margin, r_mult, reason,
                  pos_data.get("strategy", ""), fees_total, fees_src, stop_slip_usd, stop_slip_r, h))


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
# الاستراتيجيات
# ==========================================
def detect_regime(P, df_c, df_t):
    if len(df_t) < 50 or len(df_c) < 50:
        return "unknown"
    last_t = df_t.iloc[-2]
    last_c = df_c.iloc[-2]
    adx_t = last_t.get("adx", 0)
    adx_c = last_c.get("adx", 0)
    ema50_t = last_t.get("ema50", last_t["close"])
    up = last_t["close"] > ema50_t and last_t["ema9"] > last_t["ema21"]
    down = last_t["close"] < ema50_t and last_t["ema9"] < last_t["ema21"]
    if adx_t > P["regime_adx4_strong"] and adx_c > P["regime_adx1_strong"]:
        if up:
            return "strong_trend_up"
        if down:
            return "strong_trend_down"
    if adx_t > P["regime_adx4_weak"] and (up or down):
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


def build_context(h, df_e, df_c, df_t):
    P, Hc = PARAMS[h], HORIZON_DEFS[h]
    if len(df_e) < 70 or len(df_c) < 60 or len(df_t) < 60:
        return None, "insufficient_data"
    last = df_e.iloc[-2]
    prev = df_e.iloc[-3]
    last_c = df_c.iloc[-2]
    last_t = df_t.iloc[-2]
    needed = ["ema9", "ema21", "ema50", "rsi", "adx", "atr", "vol_ma"]
    if any(last.get(k) is None or pd.isna(last.get(k)) for k in needed):
        return None, "indicators_nan"
    price = float(last["close"])
    atr_val = float(last["atr"])
    adx_val = float(last["adx"])
    regime = detect_regime(P, df_c, df_t)
    if regime == "unknown":
        return None, "insufficient_data"
    if regime == "range":
        return None, "regime_range"
    atr_pct = atr_val / price
    if atr_pct > P["max_atr_pct"]:
        return None, "atr_high"
    rel = relative_atr(df_e)
    atr_ok = atr_pct >= P["min_atr_pct"] or (
        rel is not None and atr_pct >= P["atr_hard_floor"] and rel >= P["rel_atr_ratio"])
    if not atr_ok:
        return None, "atr_low"
    if adx_val < P["min_adx"]:
        return None, "low_adx"
    long_dir = regime in ("strong_trend_up", "weak_trend") and last_t["close"] > last_t["ema50"]
    short_dir = regime in ("strong_trend_down", "weak_trend") and last_t["close"] < last_t["ema50"]
    if not long_dir and not short_dir:
        return None, "direction_mismatch"
    sl_pct = max(Hc["min_sl"], min(Hc["max_sl"], Hc["atr_sl_mult"] * atr_val / price))
    return {
        "h": h, "P": P, "last": last, "prev": prev, "last_c": last_c, "last_t": last_t,
        "price": price, "atr_val": atr_val, "ema21": float(last["ema21"]),
        "ema50": float(last["ema50"]), "rsi": float(last["rsi"]),
        "regime": regime, "long_dir": long_dir, "short_dir": short_dir,
        "sl_pct": sl_pct, "tp_pct": sl_pct * Hc["rr"],
    }, None


def _make_signal(side, ctx, strategy):
    p, sp, tp_pct = ctx["price"], ctx["sl_pct"], ctx["tp_pct"]
    if side == "LONG":
        sl, tp = p * (1 - sp), p * (1 + tp_pct)
    else:
        sl, tp = p * (1 + sp), p * (1 - tp_pct)
    return {"side": side, "price": p, "sl_pct": sp, "sl": sl, "tp": tp,
            "regime": ctx["regime"], "strategy": strategy, "horizon": ctx["h"]}


def pullback_signal(ctx):
    P = ctx["P"]
    last, prev, last_c = ctx["last"], ctx["prev"], ctx["last_c"]
    price, ema21, ema50, rsi_val = ctx["price"], ctx["ema21"], ctx["ema50"], ctx["rsi"]
    if not (last["volume"] > P["min_vol_mult"] * last["vol_ma"]):
        return None, "low_volume"
    near_ema = (abs(price - ema21) / price < P["near_ema21_pct"] or
                abs(price - ema50) / price < P["near_ema50_pct"])
    if ctx["long_dir"]:
        bullish_reject = last["close"] > last["open"] and (last["low"] < ema21 or prev["low"] < ema21)
        if not (last["ema9"] > last["ema21"] and last_c["ema9"] > last_c["ema21"]):
            return None, "ema_misaligned"
        if not near_ema:
            return None, "far_from_ema"
        if not (P["rsi_long_min"] < rsi_val < P["rsi_long_max"]):
            return None, "rsi_out_of_range"
        if not bullish_reject:
            return None, "no_pullback_candle"
        return _make_signal("LONG", ctx, "pullback"), None
    bearish_reject = last["close"] < last["open"] and (last["high"] > ema21 or prev["high"] > ema21)
    if not (last["ema9"] < last["ema21"] and last_c["ema9"] < last_c["ema21"]):
        return None, "ema_misaligned"
    if not near_ema:
        return None, "far_from_ema"
    if not (P["rsi_short_min"] < rsi_val < P["rsi_short_max"]):
        return None, "rsi_out_of_range"
    if not bearish_reject:
        return None, "no_pullback_candle"
    return _make_signal("SHORT", ctx, "pullback"), None


def breakout_signal(ctx, df_e):
    P = ctx["P"]
    last, last_c = ctx["last"], ctx["last_c"]
    window = df_e.iloc[-(BO_LOOKBACK + 2):-2]
    hh, ll = float(window["high"].max()), float(window["low"].min())
    close, high, low = float(last["close"]), float(last["high"]), float(last["low"])
    rng = high - low
    vol_ok = last["volume"] > P["bo_vol_mult"] * last["vol_ma"]
    if ctx["long_dir"]:
        if not close > hh:
            return None, "bo_no_breakout"
        if not (last_c["ema9"] > last_c["ema21"] and last_c["close"] > last_c["ema50"]):
            return None, "bo_trend_mismatch"
        if not vol_ok:
            return None, "bo_low_volume"
        if rng <= 0 or (close - low) / rng < BO_BODY_POS:
            return None, "bo_weak_candle"
        if not (BO_RSI_LONG_MIN < ctx["rsi"] < BO_RSI_LONG_MAX):
            return None, "bo_rsi"
        if close - ctx["ema21"] > P["bo_max_ext_atr"] * ctx["atr_val"]:
            return None, "bo_extended"
        return _make_signal("LONG", ctx, "breakout"), None
    if not close < ll:
        return None, "bo_no_breakout"
    if not (last_c["ema9"] < last_c["ema21"] and last_c["close"] < last_c["ema50"]):
        return None, "bo_trend_mismatch"
    if not vol_ok:
        return None, "bo_low_volume"
    if rng <= 0 or (high - close) / rng < BO_BODY_POS:
        return None, "bo_weak_candle"
    if not (BO_RSI_SHORT_MIN < ctx["rsi"] < BO_RSI_SHORT_MAX):
        return None, "bo_rsi"
    if ctx["ema21"] - close > P["bo_max_ext_atr"] * ctx["atr_val"]:
        return None, "bo_extended"
    return _make_signal("SHORT", ctx, "breakout"), None


def liquidity_sweep_signal(ctx, df_e, ex=None, sym=None):
    """V49: دخول بعد كنس السيولة + رفض + توافق اتجاه."""
    P, last = ctx["P"], ctx["last"]
    sweep_type, info = detect_liquidity_sweep(df_e)
    if not sweep_type:
        return None, "liq_no_sweep"
    if sweep_type == "bullish_sweep" and not ctx["long_dir"]:
        return None, "liq_direction_mismatch"
    if sweep_type == "bearish_sweep" and not ctx["short_dir"]:
        return None, "liq_direction_mismatch"
    if not (last["volume"] > P["min_vol_mult"] * last["vol_ma"]):
        return None, "liq_low_volume"
    rsi_val = ctx["rsi"]
    if sweep_type == "bullish_sweep" and rsi_val > 75:
        return None, "liq_rsi_overbought"
    if sweep_type == "bearish_sweep" and rsi_val < 25:
        return None, "liq_rsi_oversold"
    side = "LONG" if sweep_type == "bullish_sweep" else "SHORT"
    sig = _make_signal(side, ctx, "liquidity_sweep")
    sig["sweep_info"] = info
    sig["sweep_type"] = sweep_type
    # FVG (سياق إضافي)
    for g in detect_fvg(df_e):
        if g["low"] <= ctx["price"] <= g["high"]:
            if (side == "LONG" and g["type"] == "bullish") or \
               (side == "SHORT" and g["type"] == "bearish"):
                sig["in_fvg"] = g["type"]
                break
    # دفتر الأوامر (اختياري)
    if ex and sym:
        ob = fetch_orderbook_imbalance(ex, sym)
        if ob:
            sig["ob_ratio"] = ob["ratio"]
    return sig, None


def score_signal(signal, ctx):
    last, hc, ht = ctx["last"], ctx["last_c"], ctx["last_t"]
    side = signal["side"]
    score, factors = 0, {}
    aligned = ((side == "LONG" and hc["ema9"] > hc["ema21"] and ht["close"] > ht["ema50"])
               or (side == "SHORT" and hc["ema9"] < hc["ema21"] and ht["close"] < ht["ema50"]))
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
    factors["setup"] = 15 if signal.get("strategy") in ("breakout", "liquidity_sweep") else 12
    score += factors["setup"]
    extension = abs(float(last["close"]) - float(last["ema21"])) / max(float(last["atr"]), 1e-12)
    factors["entry_quality"] = 10 if extension <= 0.8 else (7 if extension <= 1.4 else 3)
    score += factors["entry_quality"]
    # V49: مكافأة السيولة (حتى +15)
    liq_bonus = 0
    if signal.get("sweep_type"):
        liq_bonus += 5
    if signal.get("in_fvg"):
        liq_bonus += 5
    ob = signal.get("ob_ratio")
    if ob is not None:
        if (side == "LONG" and ob > OB_IMBALANCE_LONG) or \
           (side == "SHORT" and ob < OB_IMBALANCE_SHORT):
            liq_bonus += 5
    factors["liquidity"] = liq_bonus
    score += liq_bonus
    signal["score"] = min(100, int(score))
    signal["score_factors"] = factors
    signal["volume_ratio"] = round(vol_ratio, 2)
    return signal


def _finish_signal(sig, ctx, h, ex=None, sym=None, df_e=None):
    """V49: post-process — إثراء بـ FVG/OB ثم التسجيل."""
    if df_e is not None and not sig.get("in_fvg"):
        for g in detect_fvg(df_e):
            if g["low"] <= ctx["price"] <= g["high"]:
                if (sig["side"] == "LONG" and g["type"] == "bullish") or \
                   (sig["side"] == "SHORT" and g["type"] == "bearish"):
                    sig["in_fvg"] = g["type"]
                    break
    if ex and sym and "ob_ratio" not in sig and (sig.get("sweep_type") or sig.get("in_fvg")):
        ob = fetch_orderbook_imbalance(ex, sym)
        if ob:
            sig["ob_ratio"] = ob["ratio"]
    sig = score_signal(sig, ctx)
    if sig["score"] < HORIZON_DEFS[h]["min_score"]:
        return None, {"common": "score_low"}
    return sig, {}


def evaluate_signal(h, df_e, df_c, df_t, ex=None, sym=None):
    ctx, reason = build_context(h, df_e, df_c, df_t)
    if ctx is None:
        return None, {"common": reason}
    sig, r_pull = pullback_signal(ctx)
    if sig:
        return _finish_signal(sig, ctx, h, ex, sym, df_e)
    sig, r_bo = breakout_signal(ctx, df_e) if ENABLE_BREAKOUT else (None, "disabled")
    if sig:
        return _finish_signal(sig, ctx, h, ex, sym, df_e)
    sig, r_liq = (liquidity_sweep_signal(ctx, df_e, ex, sym)
                  if ENABLE_LIQUIDITY_SWEEP else (None, "disabled"))
    if sig:
        return _finish_signal(sig, ctx, h, ex, sym, df_e)
    return None, {"pullback": r_pull, "breakout": r_bo, "liquidity": r_liq}


def market_filters_ok(ex, sym, side, h="swing"):
    P, Hc = PARAMS[h], HORIZON_DEFS[h]
    try:
        t = ex.fetch_ticker(sym)
        bid, ask, last = t.get('bid'), t.get('ask'), t.get('last')
        if bid and ask and last:
            spread = (ask - bid) / last
            _last_spread[sym] = (time.time(), spread)
            if spread > P["max_spread_pct"]:
                log.info(f"[SPREAD] {sym} {spread * 100:.3f}% > الحد")
                return False, "spread_high"
    except Exception as e:
        log.warning(f"[FILTER TICKER] {sym}: {e}")
        return False, "ticker_unavailable"
    try:
        fr = ex.fetch_funding_rate(sym).get('fundingRate')
        if fr is not None:
            if side == "LONG" and fr > Hc["max_funding"]:
                return False, "funding_high"
            if side == "SHORT" and fr < -Hc["max_funding"]:
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
    Hc = HORIZON_DEFS[hz_of(st)]
    side = st["position"]
    entry = st["entry"]
    close_side = 'sell' if side == "LONG" else 'buy'
    contracts = st["contracts"]
    raw_amt = st["initial_contracts"] * Hc["p1_pct"] if which == 1 else contracts * Hc["p2_pct"]
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
    fill_ts = time.time()
    exit_price = get_current_price(ex, sym, 0) or entry
    cs_leg = contract_size(ex, sym)
    pnl = leg_pnl(side, entry, exit_price, amt, cs_leg)
    fee_est_leg = leg_fee_est(entry, exit_price, amt, cs_leg)
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
    pct_txt = f"{int(Hc['p1_pct'] * 100)}% من الكمية" if which == 1 else f"{int(Hc['p2_pct'] * 100)}% من المتبقي"
    extra = "نقل الوقف للتعادل" if (which == 1 and protected) else "قفل أرباح إضافية"
    warn = "" if protected else "\n⚠️ تعذر تحديث الحماية."
    send(f"💰 <b>إغلاق جزئي {which} ({pct_txt})</b> — <code>{short(sym)}</code>\nربح الجزء: {pnl:+.2f} USDT | {extra}{warn}")
    return True


def manage_position(ex, sym, st):
    h = hz_of(st)
    Hc = HORIZON_DEFS[h]
    side = st["position"]
    entry = st["entry"]
    d = 1 if side == "LONG" else -1
    price = get_current_price(ex, sym)
    if not price:
        return
    close_side = 'sell' if side == "LONG" else 'buy'
    has_sl, _ = has_protection(ex, sym)
    if has_sl is None:
        log.warning(f"[MANAGE] حالة الحماية مجهولة لـ {sym}")
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
    held_h = (time.time() - (st.get("opened_ts") or time.time())) / 3600
    if held_h > Hc["max_hold_h"] and not st.get("breakeven_hit"):
        send(f"⏱️ <b>انتهت مدة الصفقة ({Hc['max_hold_h']:g}س) دون تأمين</b> — <code>{short(sym)}</code> ({r_now:+.2f}R) — إغلاق.")
        if close_full(ex, sym):
            finalize_trade(ex, sym, reason_hint="Time Stop")
        else:
            send(f"❌ فشل إغلاق Time Stop لـ <code>{short(sym)}</code>")
        return
    if not st.get("partial_1_taken") and r_now >= Hc["p1_r"]:
        if do_partial(ex, sym, st, 1):
            return
    elif st.get("partial_1_taken") and not st.get("partial_2_taken") and r_now >= Hc["p2_r"]:
        if do_partial(ex, sym, st, 2):
            return
    if not st.get("breakeven_hit"):
        if r_now >= Hc["be_r"]:
            move_sl(ex, sym, st, breakeven_price(side, entry), "🛡️ تأمين الصفقة (Breakeven)")
        return
    raw = fetch_data(ex, sym, Hc["tfs"][0])
    if len(raw) < 50:
        return
    atr_val = to_df(raw).iloc[-2].get("atr")
    if atr_val is None or pd.isna(atr_val):
        return
    mult = Hc["trail_atr"]
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
# السماح بالدخول + Swap
# ==========================================
def floating_pnl_cached(ex):
    total = 0.0
    for sym, v in open_positions_snapshot().items():
        c = _price_cache.get(sym)
        price = c[1] if c else v["entry"]
        d = 1 if v["position"] == "LONG" else -1
        total += (price - v["entry"]) * d * v["contracts"] * contract_size(ex, sym)
    return total


def open_risk_usd(ex):
    total = 0.0
    for sym, v in open_positions_snapshot().items():
        if v.get("breakeven_hit"):
            continue
        total += (v.get("initial_risk") or 0.0) * (v.get("contracts") or 0.0) * contract_size(ex, sym)
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
            send("⛔ <b>وصلنا حد الخسارة اليومي</b>\nتم إيقاف الصفقات الجديدة")
        return False, "daily_loss_limit"
    if DAILY_PROFIT_TARGET_PCT > 0 and snap["value"] >= equity * DAILY_PROFIT_TARGET_PCT / 100.0:
        if meta.get("profit_alert") != snap["date"]:
            set_meta("profit_alert", snap["date"])
            send("✅ <b>تحقق الهدف اليومي للربح</b>\nتم إيقاف الدخول لحماية الأرباح")
        return False, "daily_profit_target"
    return True, ""


def maybe_swap_weaker(ex, new_cand):
    """
    V49: إذا امتلأت الأماكن ووصلت إشارة أقوى، نستبدل أضعف صفقة خاسرة غير محمية.
    شروط: خاسرة + لم تنقل للتعادل + عمرها ≥ SWAP_MIN_HOLD_H + فارق الدرجة ≥ SWAP_SCORE_MARGIN.
    """
    if not AUTO_SWAP_ON_BETTER:
        return False
    new_score = new_cand["signal"].get("score", 0)
    with state_lock:
        opens = [(sym, dict(v)) for sym, v in state.items() if v.get("position")]
    if not opens:
        return False
    weakest, weakest_r, weakest_score = None, 0.0, 0
    for sym in symbols:
