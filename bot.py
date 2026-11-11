# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V48.2 (AI + KER + Backup)
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
VERSION = "V48.2 AI"
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
INCLUDE_NEW_MARKETS_DAYS = int(env_float("INCLUDE_NEW_MARKETS_DAYS", 0))
UNIVERSE_MODE = os.getenv("UNIVERSE_MODE", "dynamic").strip().lower()
EXCLUDE_BASES = {x.strip().upper() for x in os.getenv(
    "EXCLUDE_BASES",
    "ASML,QQQ,SPY,ANTHROPIC,OPENAI,NVDA,TSLA,AAPL,MSFT,GOOGL,AMZN,META,COIN,MSTR,CRCL,HOOD").split(",") if x.strip()}
ORPHAN_CHECK_SEC = int(env_float("ORPHAN_CHECK_SEC", 60))
ADOPT_ORPHANS = env_flag("ADOPT_ORPHANS", True)
GHOST_MAX_AGE_SEC = 120
MISSING_CONFIRMATIONS = 2
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

def _cfg(key):
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
REL_ATR_RATIO = _cfg("rel_atr_ratio")
ATR_HARD_FLOOR = _cfg("atr_hard_floor")

# ---------- استراتيجية الاختراق ----------
ENABLE_BREAKOUT = env_flag("ENABLE_BREAKOUT", USE_SANDBOX)
BO_LOOKBACK = int(env_float("BO_LOOKBACK", 20))
BO_VOL_MULT = _cfg("bo_vol_mult")
BO_MAX_EXT_ATR = _cfg("bo_max_ext_atr")
BO_BODY_POS = env_float("BO_BODY_POS", 0.6)
BO_RSI_LONG_MIN = env_float("BO_RSI_LONG_MIN", 52)
BO_RSI_LONG_MAX = env_float("BO_RSI_LONG_MAX", 72)
BO_RSI_SHORT_MIN = env_float("BO_RSI_SHORT_MIN", 28)
BO_RSI_SHORT_MAX = env_float("BO_RSI_SHORT_MAX", 48)

# ---------- فلتر كفاءة الاتجاه (KER) ----------
ENABLE_KER_FILTER = env_flag("ENABLE_KER_FILTER", True)
MIN_KER = env_float("MIN_KER", 0.25)
KER_LOOKBACK = int(env_float("KER_LOOKBACK", 10))

# ---------- AI Gateway & Backup ----------
AI_GATEWAY_URL = os.getenv("AI_GATEWAY_URL", "https://api.openai.com/v1/chat/completions")
AI_API_KEY = os.getenv("AI_API_KEY", "").strip()
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
BACKUP_INTERVAL_SEC = int(env_float("BACKUP_INTERVAL_SEC", 6 * 3600))

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
_last_spread = {}
_warned_external = set()
_lock_fh = None

# ---------- التشخيص ----------
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
    "score_low": "درجة الفرصة أقل من الحد المطلوب",
    "low_efficiency": "كفاءة اتجاه ضعيفة (تذبذب عالي)",
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
        "version": VERSION, "profile": PROFILE_NAME, "paused": bool(meta.get("paused")),
        "loop_age_sec": round(time.time() - last, 1) if last else None,
        "cycles": loop_info["cycles"], "last_cycle_duration": round(loop_info["last_duration"], 1),
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
        "opened_ts": 0, "leg_ts": 0, "last_exit_ts": 0,
        "realized_pnl": 0.0, "breakeven_hit": False,
        "partial_1_taken": False, "partial_2_taken": False,
        "last_notified_sl": None, "regime": "unknown", "strategy": "",
        "planned_entry": None, "realized_fees_est": 0.0, "missing_checks": 0,
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
            txt = json.dumps({"state": state, "daily_pnl": daily_pnl, "meta": meta}, ensure_ascii=False, indent=2)
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
                  "pnl", "r_multiple", "reason", "regime", "duration_min", "strategy",
                  "fees", "fees_source", "stop_slip_usd", "stop_slip_r", "entry_slip_usd", "risk_usd", "score"]

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
            log.info(f"[JOURNAL] تم ترحيل {len(rows)} صفقة إلى الصيغة الجديدة")
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
    lines = [f"📈 <b>إحصائيات الأداء الكلية</b>", "━━━━━━━━━━━━━━━",
             f"🔢 عدد الصفقات: {n}",
             f"✅ نسبة الربح: {wr:.1f}%",
             f"💰 الصافي: <b>{df['pnl'].sum():+.2f} USDT</b>",
             f"⚖️ Profit Factor: {pf_txt}",
             f"📐 متوسط R: {avg_r:+.2f}",
             f"🟢 متوسط الربح: {avg_win:+.2f} | 🔴 متوسط الخسارة: {avg_loss:+.2f}",
             f"📉 أقصى تراجع: {max_dd:.2f} USDT",
             "", "<b>حسب الاستراتيجية:</b>"]
    for name, g in df.groupby("strategy"):
        lines.append(_group_line(STRAT_LABEL.get(name, name), g))
    if "side" in df:
        lines += ["", "<b>حسب الاتجاه:</b>"]
        for name, g in df.groupby("side"):
            lines.append(_group_line(name, g))
    score = _num(df, "score")
    if score.notna().any():
        lines += ["", "<b>حسب درجة الفرصة:</b>"]
        for lo, hi, name in ((0, 79, "74–79"), (80, 89, "80–89"), (90, 100, "90+")):
            g = df[(score >= lo) & (score <= hi)]
            if len(g):
                lines.append(_group_line(name, g))
    fees = _num(df, "fees").dropna()
    stop_usd = _num(df, "stop_slip_usd").dropna()
    stop_r = _num(df, "stop_slip_r").dropna()
    entry_slip = _num(df, "entry_slip_usd").dropna()
    n_actual = int((df["fees_source"] == "actual").sum()) if "fees_source" in df else 0
    if len(fees) or len(stop_usd) or len(entry_slip):
        lines += ["", "<b>تكلفة التنفيذ (الموجب = تكلفة عليك):</b>"]
        if len(fees):
            lines.append(f"🧾 الرسوم: {fees.sum():.2f} USDT ({n_actual} فعلية / {len(fees) - n_actual} تقديرية)")
        if len(stop_usd):
            lines.append(f"⚡ انزلاق الوقف: متوسط {stop_usd.mean():.2f} USDT ({stop_r.mean():.2f}R) على {len(stop_usd)} صفقة")
        if len(entry_slip):
            lines.append(f"↘️ انزلاق الدخول: إجمالي {entry_slip.sum():.2f} | متوسط {entry_slip.mean():.2f} USDT")
    if n < 30:
        lines += ["", f"⚠️ العينة صغيرة ({n} صفقة) — لا تعتمد على النسب قبل 30 صفقة على الأقل."]
    lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines)

def local_ai_analysis(df):
    n = len(df)
    if n == 0: return "📭 لا يوجد سجل صفقات."
    wins = df[df["pnl"] > 0]
    losses = df[df["pnl"] <= 0]
    wr = len(wins) / n * 100
    avg_win = wins["pnl"].mean() if len(wins) else 0
    avg_loss = losses["pnl"].mean() if len(losses) else 0
    best_pair = df.groupby("symbol")["pnl"].sum().idxmax() if n > 0 else "—"
    worst_pair = df.groupby("symbol")["pnl"].sum().idxmin() if n > 0 else "—"
    best_strat = df.groupby("strategy")["pnl"].sum().idxmax() if n > 0 else "—"
    lines = [
        "🧠 <b>تحليل محلي ذكي (لم يتم إعداد AI API):</b>",
        f"• نسبة الربح: {wr:.1f}%",
        f"• متوسط الربح: {avg_win:+.2f} USDT | متوسط الخسارة: {avg_loss:+.2f} USDT",
        f"• أفضل عملة: {best_pair} | أسوأ عملة: {worst_pair}",
        f"• أفضل استراتيجية: {best_strat}",
    ]
    if avg_win > 0 and avg_loss < 0:
        rr = avg_win / abs(avg_loss)
        lines.append(f"• نسبة R/R: {rr:.2f}")
        if wr < 50 and rr < 1.5:
            lines.append("⚠️ <b>تحذير:</b> نسبة الربح منخفضة و R/R منخفض. قد تحتاج لتعديل الأهداف.")
        elif wr > 50 and rr < 1:
            lines.append("💡 <b>نصيحة:</b> نسبة الربح جيدة لكن العائد قليل. جرب زيادة الهدف (RR).")
    try:
        df["hour"] = pd.to_datetime(df["close_time"]).dt.hour
        best_hour = df.groupby("hour")["pnl"].sum().idxmax()
        lines.append(f"• أفضل وقت للتداول: الساعة {best_hour}:00")
    except Exception:
        pass
    return "\n".join(lines)

def ai_analyze_trades(chat_id):
    if not os.path.exists(JOURNAL_FILE):
        return send("📭 لا يوجد سجل صفقات للتحليل.", chat_id)
    try:
        df = pd.read_csv(JOURNAL_FILE)
        if df.empty: return send("📭 السجل فارغ.", chat_id)
    except Exception as e:
        return send(f"خطأ في قراءة السجل: {e}", chat_id)

    send("⏳ جاري تحليل سجل الصفقات واستخراج الأنماط...", chat_id)
    
    if not AI_API_KEY:
        return send(local_ai_analysis(df), chat_id)

    total_pnl = df["pnl"].sum()
    wr = (df["pnl"] > 0).mean() * 100
    recent = df.tail(15)[["symbol", "side", "strategy", "pnl", "r_multiple", "reason"]].to_csv(index=False)
    
    prompt = f"""أنت محلل أداء تداول كمي خبير. حلل سجل بوت التداول التالي:
1. استخرج 3 أنماط (Patterns) واضحة (مثلاً: هل البوت يخسر في استراتيجية معينة؟ في وقت معين؟).
2. قيّم مؤشرات المخاطر (R/R، سلاسل الخسائر).
3. قدّم توصيتين قابلتين للتنفيذ لتحسين الربحية.
إجمالي الصفقات: {len(df)} | نسبة الربح: {wr:.1f}% | صافي الربح: {total_pnl:.2f} USDT
آخر 15 صفقة:
{recent}
اجعل الإجابة باللغة العربية، مختصرة، وبنقاط واضحة، ومناسبة لتيليجرام."""

    try:
        headers = {"Authorization": f"Bearer {AI_API_KEY}", "Content-Type": "application/json"}
        payload = {"model": AI_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3}
        r = requests.post(AI_GATEWAY_URL, json=payload, headers=headers, timeout=45)
        r.raise_for_status()
        ai_text = r.json()["choices"][0]["message"]["content"]
        send(f"🧠 <b>تحليل AI Gateway:</b>\n\n{ai_text}", chat_id)
    except Exception as e:
        log.error(f"[AI] {e}")
        send(f"⚠️ فشل الاتصال بـ AI Gateway: {e}\n\n{local_ai_analysis(df)}", chat_id)

def backup_to_telegram(chat_id=None):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS: return
    targets = [chat_id] if chat_id else list(AUTHORIZED_CHATS)
    for cid in targets:
        for filepath in [JOURNAL_FILE, STATE_FILE]:
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'rb') as f:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument",
                                      data={"chat_id": cid, "caption": f"🔒 نسخة احتياطية: {os.path.basename(filepath)}"},
                                      files={"document": f}, timeout=20)
                except Exception as e:
                    log.error(f"[BACKUP] فشل إرسال {filepath}: {e}")

def handle_document_restore(msg, chat_id):
    doc = msg.get("document")
    if not doc: return
    file_name = doc.get("file_name", "")
    if file_name not in ("trades_journal.csv", "bot_state.json"):
        return send("❌ الملف غير معروف. أرسل ملف السجل أو الحالة فقط.", chat_id)
    
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
        send(f"✅ تم استعادة {file_name} بنجاح.", chat_id)
        if file_name == "bot_state.json":
            load_state()
            send("✅ تم استعادة حالة البوت.", chat_id)
    except Exception as e:
        send(f"❌ فشل استعادة الملف: {e}", chat_id)

# ==========================================
# تيليجرام والرسائل
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

def effective_risk_pct(equity, sl_pct):
    if equity <= 0 or sl_pct <= 0:
        return 0.0
    risk_usd = equity * RISK_PCT_PER_TRADE / 100.0
    margin = risk_usd / (sl_pct * LEVERAGE)
    cap = min(MAX_MARGIN_USDT, equity * MAX_MARGIN_PCT_EQUITY)
    margin = min(cap, margin)
    return margin * sl_pct * LEVERAGE / equity * 100.0

def msg_entry(sym, side, price, sl, tp, margin, real_order, regime="", strategy="", equity=None):
    title = "🟢 <b>توصية شراء (LONG)</b>" if side == "LONG" else "🔴 <b>توصية بيع (SHORT)</b>"
    status = "✅ <b>تم فتح الصفقة آلياً</b>" if real_order else "⚠️ <b>إشارة فقط</b>"
    sl_pct = abs(price - sl) / price * 100
    tp_pct = abs(tp - price) / price * 100
    risk_usd = margin * (sl_pct / 100) * LEVERAGE
    reward_usd = margin * (tp_pct / 100) * LEVERAGE
    risk_pct_txt = ""
    if equity and equity > 0:
        pct = risk_usd / equity * 100
        risk_pct_txt = f" = {pct:.2f}% من الرصيد"
        if pct < RISK_PCT_PER_TRADE * 0.5:
            risk_pct_txt += f" ⚠️ أقل من المستهدف ({RISK_PCT_PER_TRADE}%) بسبب سقف الهامش"
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    regime_txt = f"\n📡 <b>نظام السوق:</b> {regime}" if regime else ""
    if strategy:
        regime_txt += f"\n🧠 <b>الاستراتيجية:</b> {STRAT_LABEL.get(strategy, strategy)}"
    return (f"{title}\n{status}{regime_txt}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🪙 <b>الزوج:</b> <code>{short(sym)}</code>\n"
            f"💵 <b>سعر الدخول:</b> <code>{fmt(price)}</code>\n"
            f"⚙️ <b>الرافعة:</b> {LEVERAGE}x | <b>الهامش:</b> {margin:.2f} USDT\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🛑 <b>وقف الخسارة:</b> <code>{fmt(sl)}</code> ({sl_pct:.2f}%)\n"
            f"📉 <i>المخاطرة: {risk_usd:.2f} USDT{risk_pct_txt}</i>\n"
            f"🎯 <b>الهدف:</b> <code>{fmt(tp)}</code> ({tp_pct:.2f}%)\n"
            f"📈 <i>العائد المتوقع: +{reward_usd:.2f} USDT</i>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏰ {now_str}")

def msg_exit(sym, side, exit_price, entry_price, total_pnl, final_leg_pnl, margin, r_mult, reason, strategy="",
             fees=None, fees_src="", stop_slip_usd=None, stop_slip_r=None):
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
    return pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)

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
    change = abs(close_series - close_series.shift(period))
    volatility = abs(close_series - close_series.diff()).rolling(window=period).sum()
    er = change / (volatility + 1e-9)
    return er

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
        out.append({"id": o.get('algoId'), "is_sl": _valid_trigger(o.get('slTriggerPx')), "is_tp": _valid_trigger(o.get('tpTriggerPx'))})
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
    req = {'instId': market['id'], 'tdMode': MARGIN_MODE, 'side': side, 'posSide': 'net', 'ordType': 'conditional', 'sz': amt_str, 'reduceOnly': 'true'}
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
        ex.create_order(sym, 'market', close_side, pos["contracts"], None, {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
        for _ in range(6):
            time.sleep(0.8)
            if get_open_position(ex, sym) is None:
                cancel_pending(ex, sym)
                return True
        log.error(f"[CLOSE FULL] {sym}: المركز ما زال ظاهراً بعد الانتظار")
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
            return False, f"تعذر ضبط الرافعة: {err[:120]}", price, 0.0
        order_side = 'buy' if side == "LONG" else 'sell'
        close_side = 'sell' if side == "LONG" else 'buy'
        order = ex.create_order(sym, 'market', order_side, amount, None, {'tdMode': MARGIN_MODE, 'posSide': 'net'})
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
            return False, "لم يتأكد تنفيذ الأمر (لا مركز على المنصة) — لم تُوضع حماية", price, 0.0
        entry = pos["entry"] or price
        contracts = pos["contracts"]
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
    d = 1 if side == "LONG" else -1
    
    age = time.time() - (pos_data.get("opened_ts") or time.time())
    if age < GHOST_MAX_AGE_SEC and (pos_data.get("strategy") or "") != "adopted":
        try:
            fills = ex.fetch_my_trades(sym, since=int(pos_data["opened_ts"] * 1000) - 30000, limit=50)
            if fills is not None and len(fills) == 0:
                log.error(f"[GHOST] {sym}: لا تنفيذات منذ الفتح ({age:.0f}ث) — صفقة وهمية، أُهملت")
                reset_sym_state(sym, cooldown=True)
                cancel_pending(ex, sym)
                send(f"⚠️ <b>صفقة وهمية مُهملة</b> <code>{short(sym)}</code>\nلم تُسجَّل أي تنفيذات خلال {age:.0f} ثانية من الإعلان عن الفتح — لم تُحتسب في الإحصائيات.")
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
    use_actual = (fees_actual is not None and fees_est_total > 0 and 0.2 * fees_est_total <= fees_actual <= 5 * fees_est_total)
    
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
        "score": pos_data.get("signal_score", "")})
    
    reset_sym_state(sym, cooldown=True)
    cancel_pending(ex, sym)
    send(msg_exit(sym, side, exit_price, entry, total, final_leg, margin, r_mult, reason,
                  pos_data.get("strategy", ""), fees_total, fees_src, stop_slip_usd, stop_slip_r))

_last_emergency_msg = {}

def emergency_close(ex, sym, why):
    now = time.time()
    if now - _last_emergency_msg.get((sym, "start"), 0) > 120:
        _last_emergency_msg[(sym, "start")] = now
        send(f"🚨 <b>إغلاق طارئ</b> <code>{short(sym)}</code>\n{why}")
    if close_full(ex, sym):
        finalize_trade(ex, sym, reason_hint="Emergency")
    else:
        log.error(f"[EMERGENCY] {sym}: فشل الإغلاق الطارئ — سيُعاد المحاولة في الدورة التالية")
        if now - _last_emergency_msg.get((sym, "fail"), 0) > 120:
            _last_emergency_msg[(sym, "fail")] = now
            send(f"🆘 فشل الإغلاق الطارئ لـ <code>{short(sym)}</code> — يُعاد المحاولة تلقائياً.")
            # ==========================================
# البيانات والاستراتيجية
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
        if up_4h: return "strong_trend_up"
        if down_4h: return "strong_trend_down"
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
        
    # --- فلتر KER (كفاءة الاتجاه) ---
    if ENABLE_KER_FILTER:
        er_series = efficiency_ratio(df_15m["close"], KER_LOOKBACK)
        current_ker = float(er_series.iloc[-2]) if not pd.isna(er_series.iloc[-2]) else 0.0
        if current_ker < MIN_KER:
            return None, "low_efficiency"
    # --------------------------------

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
        "sl_pct": sl_pct, "tp_pct": sl_pct * RR_RATIO,
    }, None

def _make_signal(side, ctx, strategy):
    p, sp, tp_pct = ctx["price"], ctx["sl_pct"], ctx["tp_pct"]
    if side == "LONG":
        sl, tp = p * (1 - sp), p * (1 + tp_pct)
    else:
        sl, tp = p * (1 + sp), p * (1 - tp_pct)
    return {"side": side, "price": p, "sl_pct": sp, "sl": sl, "tp": tp, "regime": ctx["regime"], "strategy": strategy}

def pullback_signal(ctx):
    last, prev, last_1h = ctx["last"], ctx["prev"], ctx["last_1h"]
    price, ema21, ema50, rsi_val = ctx["price"], ctx["ema21"], ctx["ema50"], ctx["rsi"]
    if not (last["volume"] > MIN_VOL_MULT * last["vol_ma"]):
        return None, "low_volume"
    near_ema = (abs(price - ema21) / price < NEAR_EMA21_PCT or abs(price - ema50) / price < NEAR_EMA50_PCT)
    if ctx["long_dir"]:
        bullish_reject = last["close"] > last["open"] and (last["low"] < ema21 or prev["low"] < ema21)
        if not (last["ema9"] > last["ema21"] and last_1h["ema9"] > last_1h["ema21"]):
            return None, "ema_misaligned"
        if not near_ema: return None, "far_from_ema"
        if not (RSI_LONG_MIN < rsi_val < RSI_LONG_MAX): return None, "rsi_out_of_range"
        if not bullish_reject: return None, "no_pullback_candle"
        return _make_signal("LONG", ctx, "pullback"), None
    bearish_reject = last["close"] < last["open"] and (last["high"] > ema21 or prev["high"] > ema21)
    if not (last["ema9"] < last["ema21"] and last_1h["ema9"] < last_1h["ema21"]):
        return None, "ema_misaligned"
    if not near_ema: return None, "far_from_ema"
    if not (RSI_SHORT_MIN < rsi_val < RSI_SHORT_MAX): return None, "rsi_out_of_range"
    if not bearish_reject: return None, "no_pullback_candle"
    return _make_signal("SHORT", ctx, "pullback"), None

def breakout_signal(ctx, df_15m):
    last, last_1h = ctx["last"], ctx["last_1h"]
    window = df_15m.iloc[-(BO_LOOKBACK + 2):-2]
    hh, ll = float(window["high"].max()), float(window["low"].min())
    close, high, low = float(last["close"]), float(last["high"]), float(last["low"])
    rng = high - low
    vol_ok = last["volume"] > BO_VOL_MULT * last["vol_ma"]
    if ctx["long_dir"]:
        if not close > hh: return None, "bo_no_breakout"
        if not (last_1h["ema9"] > last_1h["ema21"] and last_1h["close"] > last_1h["ema50"]): return None, "bo_trend_mismatch"
        if not vol_ok: return None, "bo_low_volume"
        if rng <= 0 or (close - low) / rng < BO_BODY_POS: return None, "bo_weak_candle"
        if not (BO_RSI_LONG_MIN < ctx["rsi"] < BO_RSI_LONG_MAX): return None, "bo_rsi"
        if close - ctx["ema21"] > BO_MAX_EXT_ATR * ctx["atr_val"]: return None, "bo_extended"
        return _make_signal("LONG", ctx, "breakout"), None
    if not close < ll: return None, "bo_no_breakout"
    if not (last_1h["ema9"] < last_1h["ema21"] and last_1h["close"] < last_1h["ema50"]): return None, "bo_trend_mismatch"
    if not vol_ok: return None, "bo_low_volume"
    if rng <= 0 or (high - close) / rng < BO_BODY_POS: return None, "bo_weak_candle"
    if not (BO_RSI_SHORT_MIN < ctx["rsi"] < BO_RSI_SHORT_MAX): return None, "bo_rsi"
    if ctx["ema21"] - close > BO_MAX_EXT_ATR * ctx["atr_val"]: return None, "bo_extended"
    return _make_signal("SHORT", ctx, "breakout"), None

def score_signal(signal, ctx, df_15m):
    last, h1, h4 = ctx["last"], ctx["last_1h"], ctx["last_4h"]
    side = signal["side"]
    score, factors = 0, {}
    
    aligned = ((side == "LONG" and h1["ema9"] > h1["ema21"] and h4["close"] > h4["ema50"]) or (side == "SHORT" and h1["ema9"] < h1["ema21"] and h4["close"] < h4["ema50"]))
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
    
    factors["setup"] = 15 if signal.get("strategy") == "breakout" else 12
    score += factors["setup"]
    
    extension = abs(float(last["close"]) - float(last["ema21"])) / max(float(last["atr"]), 1e-12)
    factors["entry_quality"] = 10 if extension <= 0.8 else (7 if extension <= 1.4 else 3)
    score += factors["entry_quality"]

    signal["score"] = min(100, int(score))
    signal["score_factors"] = factors
    signal["volume_ratio"] = round(vol_ratio, 2)
    return signal

def evaluate_signal(df_15m, df_1h, df_4h):
    ctx, reason = build_context(df_15m, df_1h, df_4h)
    if ctx is None:
        return None, {"common": reason}
    sig, r_pull = pullback_signal(ctx)
    if not sig and ENABLE_BREAKOUT:
        sig, r_bo = breakout_signal(ctx, df_15m)
        if not sig:
            return None, {"pullback": r_pull, "breakout": r_bo}
    elif not sig:
        return None, {"pullback": r_pull}
    sig = score_signal(sig, ctx, df_15m)
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
    except Exception:
        return False, "ticker_unavailable"
    try:
        fr = ex.fetch_funding_rate(sym).get('fundingRate')
        if fr is not None:
            if side == "LONG" and fr > MAX_FUNDING_RATE: return False, "funding_high"
            if side == "SHORT" and fr < -MAX_FUNDING_RATE: return False, "funding_negative"
    except Exception:
        pass
    return True, ""

def sector_of(sym):
    base = sym.split("/")[0]
    for name, members in SECTORS.items():
        if base in members: return name
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
        ex.create_order(sym, 'market', close_side, amt, None, {'tdMode': MARGIN_MODE, 'posSide': 'net', 'reduceOnly': True})
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
        sl_px = st.get("sl")
        breached = bool(sl_px) and ((side == "LONG" and price <= sl_px) or (side == "SHORT" and price >= sl_px))
        if breached:
            emergency_close(ex, sym, "السعر تجاوز وقف الخسارة ولم يُنفَّذ الأمر — إغلاق فوري")
            return
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
                return
            side = signal["side"]
            log.info(f"[SIGNAL] {sym} {side} score={signal.get('score', 0)}/100 strategy={signal['strategy']} regime={signal['regime']}")

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

            equity_now = get_equity(ex)
            margin = compute_margin(equity_now, sl_pct)
            if margin <= 0:
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
                        "realized_pnl": 0.0, "realized_fees_est": 0.0, "planned_entry": actual_price,
                        "last_notified_sl": None,
                        "breakeven_hit": False, "partial_1_taken": False, "partial_2_taken": False,
                        "regime": signal.get("regime", ""),
                        "strategy": signal.get("strategy", ""), "signal_score": signal.get("score", 0)})
            else:
                note_block(sym, "open_failed")
                with state_lock:
                    state[sym]["last_exit_ts"] = now
            save_state()
            caption = msg_entry(sym, side, entry, sl, tp, margin, success, signal.get("regime", ""),
                                signal.get("strategy", ""), equity_now)
            caption += f"\n🧠 تقييم الفرصة: <b>{signal.get('score', 0)}/100</b> | حجم التداول: {signal.get('volume_ratio', 0):.2f}×"
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
                if current_price and ((side == "LONG" and current_price >= tp) or (side == "SHORT" and current_price <= tp)):
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
                            "opened_ts": now, "leg_ts": now, "regime": "adopted",
                            "strategy": "adopted"})
                    send(f"♻️ تم تبني مركز <code>{short(sym)}</code> ({side}) — SL {sl_pct * 100:.2f}% (ATR)")
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
            with state_lock:
                st = state.get(sym) or new_sym_state()
            if st.get("position"):
                continue
            side = "LONG" if p.get('side') == 'long' else "SHORT"
            entry = float(p.get('entryPrice') or 0)
            contracts = abs(float(p.get('contracts')))
            log.warning(f"[ORPHAN] {sym} {side} {contracts} @ {entry} بلا حالة في البوت")
            if not ADOPT_ORPHANS:
                continue
            sl_pct = adopt_sl_pct(ex, sym)
            if side == "LONG":
                sl, tp = entry * (1 - sl_pct), entry * (1 + sl_pct * RR_RATIO)
            else:
                sl, tp = entry * (1 + sl_pct), entry * (1 - sl_pct * RR_RATIO)
            cur = get_current_price(ex, sym)
            if cur and ((side == "LONG" and cur <= sl) or (side == "SHORT" and cur >= sl)):
                send(f"🚨 مركز يتيم <code>{short(sym)}</code> تجاوز الوقف المحسوب — إغلاق فوري")
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
                        "opened_ts": now, "leg_ts": now, "regime": "adopted", "strategy": "adopted"})
                save_state()
                send(f"♻️ تم تبنّي مركز يتيم <code>{short(sym)}</code> ({side}) — SL {sl_pct * 100:.2f}% (ATR)")
            else:
                send(f"🚨 مركز يتيم <code>{short(sym)}</code> وتعذر وضع الحماية — جاري الإغلاق")
                close_full(ex, sym)

# ==========================================
# التقارير وأوامر تيليجرام
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
    if v.get("partial_2_taken"): f += " 💰💰"
    elif v.get("partial_1_taken"): f += " 💰"
    if v.get("breakeven_hit"): f += " 🛡️"
    return f

def loop_age_text():
    last = loop_info["last_cycle_ts"]
    if not last: return "لم تبدأ بعد"
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
    sig_txt = " | ".join(f"{STRAT_LABEL.get(k, k)} {v}" for k, v in d.get("signals_by", {}).items()) or "—"
    lines = [f"🔎 <b>لماذا لا توجد صفقات؟</b> — {d['date']}", "━━━━━━━━━━━━━━━",
             f"📊 تقييمات (رمز×شمعة): {ev}",
             f"✅ إشارات: {d['signals']} ({sig_txt}) | 🟢 فُتحت: {d['opened']}",
             f"⚙️ البروفايل: <b>{PROFILE_NAME}</b> | الاختراق: {'مفعّل' if ENABLE_BREAKOUT else 'معطّل'} | KER: {'مفعّل' if ENABLE_KER_FILTER else 'معطّل'} (/config)"]
    if meta.get("paused"):
        lines.append("⏸️ <b>البوت موقوف (/resume)</b>")
    if ev:
        lines += ["", "<b>الأسباب العامة (قبل الاستراتيجيات):</b>"]
        for r, n in sorted(d["reasons"].items(), key=lambda x: -x[1])[:8]:
            lines.append(f"• {label(r)}: {n} ({n / ev * 100:.0f}%)")
        staged = sum(d.get("reasons_pullback", {}).values())
        if staged:
            lines.append(f"\nوصلت لمرحلة الاستراتيجيات: {staged + d['signals']} شمعة")
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

def config_text():
    return "\n".join([
        f"⚙️ <b>الإعدادات الحالية</b> — البروفايل: <b>{PROFILE_NAME}</b>", "━━━━━━━━━━━━━━━",
        f"ATR الأدنى المطلق (15د): {MIN_ATR_PCT * 100:.2f}%",
        f"ADX الأدنى (15د): {MIN_ADX:g}",
        f"فلتر كفاءة الاتجاه (KER): {'مفعّل' if ENABLE_KER_FILTER else 'معطّل'} | الحد: {MIN_KER:g} | الفترة: {KER_LOOKBACK}",
        f"الارتداد — الحجم ≥ {MIN_VOL_MULT:g}× | RSI LONG {RSI_LONG_MIN:g}–{RSI_LONG_MAX:g} | SHORT {RSI_SHORT_MIN:g}–{RSI_SHORT_MAX:g}",
        f"الاختراق: {'مفعّل' if ENABLE_BREAKOUT else 'معطّل'} | نافذة {BO_LOOKBACK} شمعة | حجم ≥ {BO_VOL_MULT:g}×",
        f"أقصى سبريد: {MAX_SPREAD_PCT * 100:.2f}%",
        f"المخاطرة: {RISK_PCT_PER_TRADE}% | رافعة {LEVERAGE}x | سقف الهامش {MAX_MARGIN_USDT:.0f}",
        f"حد درجة الفرصة: {MIN_SIGNAL_SCORE}/100 | العملات: {UNIVERSE_MODE} (حتى {MAX_UNIVERSE_SYMBOLS})",
        "━━━━━━━━━━━━━━━",
        "للتعديل: متغيرات البيئة في Render (PROFILE, MIN_KER, ENABLE_BREAKOUT...)"])

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
    floating = floating_pnl_cached(ex)
    return "\n".join([
        "🛡️ <b>ملخص إدارة المخاطر</b>", "━━━━━━━━━━━━━━━",
        f"💼 الرصيد: <b>{equity:,.2f} USDT</b>",
        f"🎯 المخاطرة المستهدفة: <b>{RISK_PCT_PER_TRADE}%</b> | سقف الهامش {MAX_MARGIN_USDT:.0f} USDT",
        f"📉 حد الخسارة اليومي: {DAILY_LOSS_LIMIT_PCT}% ({loss_limit:.1f} USDT)",
        f"🔢 المفتوحة: {len(opens)} / {MAX_OPEN_POSITIONS}",
        f"🟢 LONG: {longs} | 🔴 SHORT: {shorts}",
        f"📅 المحقق اليوم: <b>{snap['value']:+.2f} USDT</b> | العائم: {floating:+.2f}",
        "━━━━━━━━━━━━━━━"])

def daily_text(ex):
    snap = daily_snapshot()
    total_upnl = sum(unrealized(ex, s, v)[1] for s, v in open_positions_snapshot().items())
    wr = (snap["wins"] / snap["trades"] * 100) if snap["trades"] else 0
    return "\n".join([
        f"📅 <b>ملخص اليوم</b> — {snap['date']}", "━━━━━━━━━━━━━━━",
        f"💰 المحقق اليوم: <b>{snap['value']:+.2f} USDT</b>",
        f"📈 العائم حالياً: <b>{total_upnl:+.2f} USDT</b>",
        f"📊 الإجمالي التقديري: <b>{snap['value'] + total_upnl:+.2f} USDT</b>",
        f"🔢 الصفقات المكتملة: {snap['trades']}",
        f"✅ الرابحة: {snap['wins']} ({wr:.0f}%)", "━━━━━━━━━━━━━━━"])

def find_symbol(arg, active_symbols):
    arg = arg.upper().replace("/USDT", "").replace("USDT", "")
    for s in active_symbols:
        if s.startswith(arg + "/"): return s
    return None

def handle_command(ex, text, chat_id, active_symbols):
    parts = text.strip().split()
    cmd = parts[0].lower().split("@")[0]
    if cmd in ("/status", "/start"): send(status_text(ex), chat_id)
    elif cmd in ("/pos", "/positions"): send(positions_text(ex), chat_id)
    elif cmd == "/risk": send(risk_text(ex), chat_id)
    elif cmd == "/daily": send(daily_text(ex), chat_id)
    elif cmd == "/stats": send(stats_text(), chat_id)
    elif cmd == "/config": send(config_text(), chat_id)
    elif cmd == "/ai": ai_analyze_trades(chat_id)
    elif cmd == "/export":
        send("⏳ جاري تصدير السجل والحالة...", chat_id)
        backup_to_telegram(chat_id)
    elif cmd == "/restore":
        send("📥 أرسل لي الآن ملف (trades_journal.csv) أو (bot_state.json) لاستعادته.", chat_id)
    elif cmd == "/why": send(why_text(parts[1] if len(parts) > 1 else None, active_symbols), chat_id)
    elif cmd == "/pause":
        set_meta("paused", True)
        send("⏸️ تم إيقاف فتح صفقات جديدة.", chat_id)
    elif cmd == "/resume":
        set_meta("paused", False)
        send("▶️ تم استئناف البحث.", chat_id)
    elif cmd == "/close":
        if len(parts) < 2: return send("استخدم: /close BTC", chat_id)
        sym = find_symbol(parts[1], active_symbols)
        if not sym: return send("عملة غير معروفة.", chat_id)
        with trade_lock:
            with state_lock:
                pos_data = dict(state.get(sym, {}))
            if not pos_data.get("position"): return send("لا توجد صفقة مفتوحة.", chat_id)
            if close_full(ex, sym): finalize_trade(ex, sym, reason_hint="Manual")
            else: send("❌ فشل الإغلاق.", chat_id)
    elif cmd == "/closeall":
        if len(parts) < 2 or parts[1].lower() != "confirm":
            return send("⚠️ للتأكيد: /closeall confirm", chat_id)
        with trade_lock:
            with state_lock:
                syms = [k for k, v in state.items() if v.get("position")]
            if not syms: return send("لا توجد صفقات.", chat_id)
            for sym in syms:
                if close_full(ex, sym): finalize_trade(ex, sym, reason_hint="Manual")
                else: send(f"❌ فشل إغلاق <code>{short(sym)}</code>", chat_id)
    elif cmd == "/help":
        send("<b>📋 الأوامر المتاحة:</b>\n\n/status - نظرة عامة\n/why - أسباب الرفض\n/config - الإعدادات\n/pos - تفاصيل الصفقات\n/risk - المخاطر\n/daily - ملخص اليوم\n/stats - إحصائيات\n/ai - تحليل السجل بالذكاء الاصطناعي\n/export - تصدير السجل وحالة البوت\n/restore - استعادة السجل من نسخة احتياطية\n/pause - إيقاف\n/resume - استئناف\n/close BTC - إغلاق عملة\n/closeall confirm - إغلاق الكل\n/help - هذه القائمة", chat_id)

def telegram_listener(active_symbols):
    if not TELEGRAM_BOT_TOKEN or not AUTHORIZED_CHATS: return
    try: ex2 = init_exchange()
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
                if chat_id not in AUTHORIZED_CHATS: continue
                
                # استقبال الملفات للاستعادة
                if "document" in msg:
                    handle_document_restore(msg, chat_id)
                    continue
                    
                if not text.startswith("/"): continue
                try: handle_command(ex2, text, chat_id, active_symbols)
                except Exception as e: send(f"خطأ: {e}", chat_id)
        except Exception as e:
            log.warning(f"[TG POLL] {e}")
            time.sleep(5)

def watchdog():
    while True:
        time.sleep(60)
        last = loop_info["last_cycle_ts"]
        if not last: continue
        age = time.time() - last
        if age > WATCHDOG_SEC and not loop_info["alerted"]:
            loop_info["alerted"] = True
            send(f"🚨 <b>تحذير:</b> حلقة الفحص متوقفة منذ {int(age)} ثانية.\nآخر خطأ: {loop_info['last_error'] or '—'}")
        elif age <= WATCHDOG_SEC and loop_info["alerted"]:
            loop_info["alerted"] = False
            send("✅ عادت حلقة الفحص للعمل.")

# ==========================================
# ماسح السوق والحلقة الرئيسية
# ==========================================
universe_info = {"mode": "", "size": 0, "excluded": {}, "samples": {}, "categories": {}, "ts": 0}

def _market_category(market):
    c = (market.get("info") or {}).get("instCategory")
    return None if c in (None, "") else str(c)

def discover_universe(ex):
    core = [x for x in FUTURES_SYMBOLS if x in ex.markets]
    core_set = set(core)
    if UNIVERSE_MODE == "core":
        return core
    candidates, categories = [], {}
    excluded = {"قائمة سوداء": [], "غير كريبتو": [], "تصنيف مجهول": []}
    for sym, market in ex.markets.items():
        if not market.get("active", True) or not market.get("swap") or market.get("quote") != "USDT": continue
        if market.get("settle") not in (None, "USDT"): continue
        base = (market.get("base") or sym.split("/")[0]).upper()
        if base in EXCLUDE_BASES:
            excluded["قائمة سوداء"].append(base); continue
        cat = _market_category(market)
        categories[cat or "?"] = categories.get(cat or "?", 0) + 1
        if cat is None:
            if sym not in core_set:
                excluded["تصنيف مجهول"].append(base); continue
        elif cat != "1":
            excluded["غير كريبتو"].append(base); continue
        candidates.append((sym, market))
    volumes = {}
    try:
        tickers = ex.fetch_tickers([x[0] for x in candidates])
        for sym, ticker in tickers.items():
            qv = ticker.get("quoteVolume") or 0
            if qv is None and ticker.get("baseVolume") and ticker.get("last"): qv = float(ticker["baseVolume"]) * float(ticker["last"])
            volumes[sym] = float(qv or 0)
    except Exception:
        return core
    ranked = sorted(candidates, key=lambda item: volumes.get(item[0], 0), reverse=True)
    liquid = [sym for sym, _ in ranked if volumes.get(sym, 0) > 0][:MAX_UNIVERSE_SYMBOLS]
    result = list(dict.fromkeys(liquid))[:MAX_UNIVERSE_SYMBOLS]
    allowed = {x[0] for x in candidates}
    for sym in core:
        if sym in allowed and sym not in result and len(result) < MAX_UNIVERSE_SYMBOLS:
            result.append(sym)
    if not result: result = core
    universe_info.update({"mode": "dynamic", "size": len(result), "categories": categories, "excluded": {k: len(v) for k, v in excluded.items()}})
    return result

def trading_loop():
    log.info(f"[INIT] Starting {VERSION} ...")
    if not OKX_API_KEY:
        send("❌ مفاتيح API غير موجودة")
        return
    try:
        ex = init_exchange()
        load_state()
        migrate_journal()
        balance = get_equity(ex, force=True)
    except Exception as e:
        send(f"❌ فشل الاتصال: {e}")
        return

    active_symbols = discover_universe(ex)
    universe_refresh_ts = time.time()
    sync_positions(ex, active_symbols)
    threading.Thread(target=telegram_listener, args=(active_symbols,), daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    
    send(f"🚀 <b>تم تشغيل البوت {VERSION}</b>\n━━━━━━━━━━━━━━━\n"
         f"⚙️ الوضع: {'Sandbox' if USE_SANDBOX else 'حقيقي'}\n🪙 العملات: {len(active_symbols)}\n"
         f"🎚️ البروفايل: {PROFILE_NAME} | KER: {'مفعّل' if ENABLE_KER_FILTER else 'معطّل'}\n"
         f"🧠 AI: {'مفعّل' if AI_API_KEY else 'محلي'} | 📁 النسخ الاحتياطي التلقائي: مفعّل")
    if PROFILE_WARNING: send(f"⚠️ {PROFILE_WARNING}")

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
            refresh_prices(ex, active_symbols)
            if USE_BTC_FILTER: btc_bias(ex)
            for sym in active_symbols:
                try:
                    process_symbol(ex, sym)
                except Exception as e:
                    cycle_errors += 1
                    loop_info["last_error"] = f"{sym}: {str(e)[:150]}"
                time.sleep(0.25)
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
            log.info(f"[CYCLE] #{loop_info['cycles']} {loop_info['last_duration']:.0f}s | open={len(open_positions_snapshot())} | eval={ev} signals={sg} opened={op}")
        fail_streak = fail_streak + 1 if cycle_errors >= 5 else 0
        if fail_streak >= 3:
            try:
                ex = init_exchange()
                fail_streak = 0
            except Exception: pass
        time.sleep(max(8, SCAN_INTERVAL - (time.time() - start)))

def maybe_send_daily_report(ex):
    now = datetime.now(KSA_TZ)
    today = str(now.date())
    if now.hour == 23 and now.minute >= 55 and meta.get("last_report") != today:
        snap = daily_snapshot()
        with diag_lock: ev, sg, op = diag["evaluated"], diag["signals"], diag["opened"]
        send(f"📋 <b>التقرير اليومي {VERSION}</b> — {today}\n💰 النتيجة: {snap['value']:+.2f} USDT\n🔢 الصفقات: {snap['trades']} | الرابحة: {snap['wins']}\n🔎 تقييمات {ev} | إشارات {sg} | فُتحت {op}\n💼 الرصيد: {get_equity(ex, force=True):,.2f} USDT")
        set_meta("last_report", today)

def maybe_send_heartbeat(ex):
    now = datetime.now(KSA_TZ)
    current_hour = now.strftime("%Y-%m-%d %H")
    if meta.get("last_heartbeat") != current_hour and now.minute < 4:
        set_meta("last_heartbeat", current_hour)

if __name__ == "__main__":
    if not acquire_single_instance():
        log.error("نسخة أخرى من البوت تعمل بالفعل — إيقاف.")
        sys.exit(1)
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
