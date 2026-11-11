# -*- coding: utf-8 -*-
"""
OKX Futures Trading Bot - V48.3 (AI + KER + Volume Delta + Spoofing)
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
VERSION = "V48.3 AI"
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
        "rel_atr_ratio": 99, "atr_hard_floor": 0.003, "bo_vol_mult": 1.8, "bo_max_ext_atr": 2.0,
    },
    "balanced": {
        "min_atr_pct": 0.003, "min_adx": 20, "min_vol_mult": 1.0,
        "regime_adx4_strong": 26, "regime_adx1_strong": 22, "regime_adx4_weak": 18,
        "rsi_long_min": 38, "rsi_long_max": 64, "rsi_short_min": 36, "rsi_short_max": 62,
        "near_ema21_pct": 0.013, "near_ema50_pct": 0.019, "max_spread_pct": 0.0015,
        "rel_atr_ratio": 1.15, "atr_hard_floor": 0.0018, "bo_vol_mult": 1.5, "bo_max_ext_atr": 2.5,
    },
    "active": {
        "min_atr_pct": 0.0022, "min_adx": 18, "min_vol_mult": 0.8,
        "regime_adx4_strong": 24, "regime_adx1_strong": 20, "regime_adx4_weak": 15,
        "rsi_long_min": 35, "rsi_long_max": 67, "rsi_short_min": 33, "rsi_short_max": 65,
        "near_ema21_pct": 0.016, "near_ema50_pct": 0.024, "max_spread_pct": 0.003,
        "rel_atr_ratio": 0.9, "atr_hard_floor": 0.0015, "bo_vol_mult": 1.2, "bo_max_ext_atr": 3.0,
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

# ---------- فلاتر التدفق والسيولة (جديد V48.3) ----------
ENABLE_VOLUME_DELTA = env_flag("ENABLE_VOLUME_DELTA", True)
VOLUME_DELTA_LOOKBACK = int(env_float("VOLUME_DELTA_LOOKBACK", 3))
ENABLE_SPOOFING_CHECK = env_flag("ENABLE_SPOOFING_CHECK", True)
SPOOFING_RATIO = env_float("SPOOFING_RATIO", 3.0)
SPOOFING_BOOK_LIMIT = 20

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
    "negative_delta": "تدفق بيعي (يتعارض مع LONG)",
    "positive_delta": "تدفق شرائي (يتعارض مع SHORT)",
    "spoofing_detected": "كشف سبوفينج (جدار وهمي)",
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
# Flask & State Management
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
# سجل الصفقات و AI & Backup
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
        "🧠 <b>تحليل محلي ذكي:</b>",
        f"• نسبة الربح: {wr:.1f}%",
        f"• متوسط الربح: {avg_win:+.2f} USDT | متوسط الخسارة: {avg_loss:+.2f} USDT",
        f"• أفضل عملة: {best_pair} | أسوأ عملة: {worst_pair}",
        f"• أفضل استراتيجية: {best_strat}",
    ]
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
    prompt = f"أنت محلل تداول. حلل السجل واستخرج 3 أنماط وتوصيتين:\nإجمالي: {len(df)} | ربح: {wr:.1f}% | صافي: {total_pnl:.2f}\nآخر صفقات:\n{recent}\nالرد بالعربية ومختصر."

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
    return change / (volatility + 1e-9)

def calculate_volume_delta(df, lookback=3):
    if len(df) < lookback + 2:
        return 0.0
    deltas = []
    for i in range(1, lookback + 1):
        c = df.iloc[-1 - i]
        if c["close"] > c["open"]:
            deltas.append(c["volume"])
        elif c["close"] < c["open"]:
            deltas.append(-c["volume"])
        else:
            deltas.append(0.0)
    return sum(deltas)

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
            return False, "لم يتأكد تنفيذ الأمر (لا مركز على المنصة)", price, 0.0
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
