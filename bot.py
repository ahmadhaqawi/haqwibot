import os
import sys
import threading
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
import asyncio
import websockets
import json

import ccxt
import pandas as pd
import requests
from flask import Flask

# ==========================================
# Logging
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ScalpBot")

app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "scalping_bot_active"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# الإعدادات - سكالبينج عدواني
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "400000"))
WHALE_COOLDOWN = int(os.getenv("WHALE_COOLDOWN", "300"))
last_whale_alert: Dict[str, float] = {}

WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
    "BNB/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT",
    "AVAX/USDT:USDT", "LINK/USDT:USDT", "SUI/USDT:USDT", "NEAR/USDT:USDT"
]

TIMEFRAMES = ["1m"]
HIGHER_TF = "5m"
SCAN_INTERVAL = 20
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "0.7"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "2.5"))
LEVERAGE = 5

ATR_MULT_SL = 1.3
ATR_MULT_TP = 2.2
TRAIL_ACTIVATE_PCT = 0.45
TRAIL_DISTANCE_PCT = 0.35

state: Dict[tuple, Dict[str, Any]] = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# تيليجرام
# ==========================================
def send(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True},
                timeout=8
            )
        except Exception as e:
            logger.error(f"Telegram error: {e}")

def fmt(p: float) -> str:
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def msg_entry(sym, side, price, sl, tp, rsi, adx, signal_type):
    title = f"⚡ <b>سكالب لونق</b>" if side == "LONG" else f"⚡ <b>سكالب شورت</b>"
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    now_str = datetime.now(KSA_TZ).strftime("%I:%M:%S %p")

    return (
        f"{title} | {signal_type}\n"
        f"🪙 <code>{sym.split(':')[0]}</code>\n"
        f"💵 الدخول: <code>{fmt(price)}</code>\n"
        f"🛑 الوقف: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
        f"🎯 الهدف: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
        f"📊 RSI: {rsi:.1f} | ADX: {adx:.1f}\n"
        f"⏰ {now_str}"
    )

def msg_exit(sym, side, price, entry, reason, pnl_pct, duration, entry_str, exit_str):
    emoji = "✅" if pnl_pct >= 0 else "❌"
    title = f"{emoji} إغلاق سكالب {'لونق' if side == 'LONG' else 'شورت'}"
    reasons = {
        "Stop Loss": "وقف خسارة",
        "Take Profit": "هدف ربح",
        "Trailing Stop": "تريلينج"
    }
    ar_reason = reasons.get(reason, reason)
    lev_pnl = pnl_pct * LEVERAGE

    return (
        f"{title}\n"
        f"🪙 <code>{sym.split(':')[0]}</code>\n"
        f"📥 {fmt(entry)} → 📤 {fmt(price)}\n"
        f"📊 النتيجة: <b>{pnl_pct:+.2f}%</b> | بالرافعة: <b>{lev_pnl:+.2f}%</b>\n"
        f"⏱️ {duration} | السبب: {ar_reason}"
    )

# ==========================================
# الحيتان
# ==========================================
async def process_whale_alert(platform, symbol_display, side_ar, emoji, cost, price):
    key = f"{platform}_{symbol_display}"
    now = time.time()
    if now - last_whale_alert.get(key, 0) < WHALE_COOLDOWN:
        return
    last_whale_alert[key] = now

    msg = (
        f"🐋 <b>حوت</b>\n"
        f"🪙 {symbol_display} | {emoji} {side_ar}\n"
        f"💰 ${cost:,.0f} @ {fmt(price)}"
    )
    send(msg)

async def listen_to_binance():
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=15, ping_timeout=15) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' in data and data['data'].get('e') == 'aggTrade':
                        t = data['data']
                        price, qty = float(t['p']), float(t['q'])
                        cost = price * qty
                        if cost >= WHALE_THRESHOLD_WS:
                            side_ar = "شراء" if not t['m'] else "بيع"
                            emoji = "🟢" if not t['m'] else "🔴"
                            await process_whale_alert("Binance", t['s'].replace("USDT", "/USDT"), side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(4)

async def listen_to_okx():
    args = [{"channel": "trades", "instId": s.replace("USDT", "-USDT-SWAP")} for s in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=15, ping_timeout=15) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' in data:
                        for t in data['data']:
                            price, size = float(t['px']), float(t['sz'])
                            cost = price * size
                            if cost >= WHALE_THRESHOLD_WS:
                                side = t.get('side', 'buy')
                                side_ar = "شراء" if side == "buy" else "بيع"
                                emoji = "🟢" if side == "buy" else "🔴"
                                sym = t['instId'].replace("-SWAP", "").replace("-", "/")
                                await process_whale_alert("OKX", sym, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(4)

async def listen_to_bybit():
    args = [f"publicTrade.{s}" for s in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=15, ping_timeout=15) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' in data:
                        for t in data['data']:
                            price, size = float(t['p']), float(t['v'])
                            cost = price * size
                            if cost >= WHALE_THRESHOLD_WS:
                                side = t.get('S', 'Buy')
                                side_ar = "شراء" if side == "Buy" else "بيع"
                                emoji = "🟢" if side == "Buy" else "🔴"
                                await process_whale_alert("Bybit", t['s'].replace("USDT", "/USDT"), side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(4)

async def run_websockets():
    await asyncio.gather(listen_to_binance(), listen_to_okx(), listen_to_bybit())

def run_websocket_loop():
    asyncio.run(run_websockets())

# ==========================================
# المؤشرات
# ==========================================
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=7):
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def adx(high, low, close, period=10):
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high-low, (high-close.shift()).abs(), (low-close.shift()).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    dx = abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9) * 100
    return dx.ewm(alpha=1/period, adjust=False).mean()

def atr(df, period=10):
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def add_indicators(df):
    df = df.copy()
    df["ema8"] = ema(df["close"], 8)
    df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"], 7)
    df["adx"] = adx(df["high"], df["low"], df["close"], 10)
    df["atr"] = atr(df, 10)
    df["vol_ma"] = df["volume"].rolling(12).mean()
    df["high_5"] = df["high"].rolling(5).max().shift(1)
    df["low_5"] = df["low"].rolling(5).min().shift(1)
    return df

def check_signal(df, df_htf, position, entry=None, sl=None, tp=None, highest=None, lowest=None):
    if len(df) < 30 or len(df_htf) < 30:
        return None

    last = df.iloc[-2]
    prev = df.iloc[-3]
    price = float(last["close"])
    htf_last = df_htf.iloc[-2]

    # إدارة الصفقة المفتوحة
    if position == "LONG":
        if highest is None or price > highest:
            highest = price
        if entry and highest:
            profit = (highest - entry) / entry * 100
            if profit >= TRAIL_ACTIVATE_PCT:
                new_sl = highest * (1 - TRAIL_DISTANCE_PCT / 100)
                if sl is None or new_sl > sl:
                    sl = new_sl
        if sl and price <= sl:
            reason = "Trailing Stop" if (entry and (highest - entry)/entry*100 >= TRAIL_ACTIVATE_PCT) else "Stop Loss"
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": reason, "sl": sl, "highest": highest}
        if tp and price >= tp:
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit", "sl": sl, "highest": highest}

    elif position == "SHORT":
        if lowest is None or price < lowest:
            lowest = price
        if entry and lowest:
            profit = (entry - lowest) / entry * 100
            if profit >= TRAIL_ACTIVATE_PCT:
                new_sl = lowest * (1 + TRAIL_DISTANCE_PCT / 100)
                if sl is None or new_sl < sl:
                    sl = new_sl
        if sl and price >= sl:
            reason = "Trailing Stop" if (entry and (entry - lowest)/entry*100 >= TRAIL_ACTIVATE_PCT) else "Stop Loss"
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": reason, "sl": sl, "lowest": lowest}
        if tp and price <= tp:
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit", "sl": sl, "lowest": lowest}

    if position is not None:
        return {"action": "UPDATE", "sl": sl, "highest": highest, "lowest": lowest}

    # شروط الدخول العدوانية
    vol_spike = last["volume"] > 1.8 * last["vol_ma"]
    adx_ok = last["adx"] > 22
    htf_up = htf_last["close"] > htf_last["ema21"]
    htf_down = htf_last["close"] < htf_last["ema21"]

    momentum_up = (last["ema8"] > last["ema21"] and 
                   prev["ema8"] <= prev["ema21"] and 
                   last["rsi"] > 52 and last["rsi"] < 78 and
                   price > last["high_5"] and vol_spike and adx_ok and htf_up)

    momentum_down = (last["ema8"] < last["ema21"] and 
                     prev["ema8"] >= prev["ema21"] and 
                     last["rsi"] < 48 and last["rsi"] > 22 and
                     price < last["low_5"] and vol_spike and adx_ok and htf_down)

    if momentum_up:
        sl_price = price - ATR_MULT_SL * last["atr"]
        tp_price = price + ATR_MULT_TP * last["atr"]
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": sl_price, "tp": tp_price,
            "rsi": last["rsi"], "adx": last["adx"], "type": "زخم 1م ⚡"
        }

    if momentum_down:
        sl_price = price + ATR_MULT_SL * last["atr"]
        tp_price = price - ATR_MULT_TP * last["atr"]
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": sl_price, "tp": tp_price,
            "rsi": last["rsi"], "adx": last["adx"], "type": "زخم 1م ⚡"
        }

    return None

# ==========================================
# الفحص
# ==========================================
def scan_symbol(exchange, symbol):
    try:
        state_key = (symbol, "1m")
        if symbol not in exchange.markets:
            return

        if state_key not in state:
            state[state_key] = {
                "position": None, "entry": None, "sl": None, "tp": None,
                "last_candle_ts": None, "entry_time": None,
                "highest": None, "lowest": None
            }

        ohlcv = exchange.fetch_ohlcv(symbol, "1m", limit=60)
        ohlcv_htf = exchange.fetch_ohlcv(symbol, HIGHER_TF, limit=40)

        if len(ohlcv) < 30 or len(ohlcv_htf) < 30:
            return

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_htf = pd.DataFrame(ohlcv_htf, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df = add_indicators(df)
        df_htf = add_indicators(df_htf)

        current_ts = int(df.iloc[-2]["timestamp"])
        if state[state_key]["last_candle_ts"] == current_ts:
            return
        state[state_key]["last_candle_ts"] = current_ts

        st = state[state_key]
        signal = check_signal(df, df_htf, st["position"], st["entry"], st["sl"], st["tp"], st["highest"], st["lowest"])

        if not signal:
            return

        if signal["action"] == "UPDATE":
            if signal.get("sl") is not None:
                st["sl"] = signal["sl"]
            if "highest" in signal:
                st["highest"] = signal["highest"]
            if "lowest" in signal:
                st["lowest"] = signal["lowest"]
            return

        today = datetime.now(KSA_TZ).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today
        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return

        if signal["action"] == "ENTRY":
            st.update({
                "position": signal["side"],
                "entry": signal["price"],
                "sl": signal["sl"],
                "tp": signal["tp"],
                "entry_time": datetime.now(timezone.utc),
                "highest": signal["price"] if signal["side"] == "LONG" else None,
                "lowest": signal["price"] if signal["side"] == "SHORT" else None
            })
            send(msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"],
                           signal["rsi"], signal["adx"], signal["type"]))
            logger.info(f"ENTRY {signal['side']} | {symbol} @ {signal['price']}")

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - st["entry"]) / st["entry"] * 100
            if st["position"] == "SHORT":
                pnl = -pnl

            exit_time = datetime.now(timezone.utc)
            entry_time = st["entry_time"]
            duration = "—"
            entry_str = exit_str = "—"
            if entry_time:
                diff = int((exit_time - entry_time).total_seconds())
                duration = f"{diff//60}د و {diff%60}ث" if diff >= 60 else f"{diff}ث"
                entry_str = entry_time.astimezone(KSA_TZ).strftime("%I:%M:%S")
                exit_str = exit_time.astimezone(KSA_TZ).strftime("%I:%M:%S")

            daily_pnl["value"] += pnl * (RISK_PCT / 1.5)

            send(msg_exit(symbol, st["position"], signal["price"], st["entry"],
                          signal["reason"], pnl, duration, entry_str, exit_str))
            logger.info(f"EXIT {st['position']} | {symbol} | {pnl:+.2f}% | {signal['reason']}")

            st.update({
                "position": None, "entry": None, "sl": None, "tp": None,
                "entry_time": None, "highest": None, "lowest": None
            })

            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 تم إيقاف البوت | وصلنا لحد الخسارة اليومية")

    except Exception as e:
        logger.error(f"Scan error {symbol}: {e}")

# ==========================================
# التشغيل
# ==========================================
def trading_loop():
    logger.info("تشغيل بوت السكالبينج العدواني...")

    threading.Thread(target=run_websocket_loop, daemon=True).start()

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    exchange.load_markets()

    send(
        f"⚡ <b>بوت سكالبينج عدواني اشتغل</b>\n"
        f"⏱ الفريم: 1 دقيقة\n"
        f"🎯 وقف ضيق + هدف سريع\n"
        f"🔄 Trailing بدري\n"
        f"🐋 مراقبة الحيتان مفعّلة\n"
        f"📉 حد الخسارة اليومي: {MAX_DAILY_LOSS}%"
    )

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                scan_symbol(exchange, sym)
                time.sleep(0.25)
        except Exception as e:
            logger.error(f"Loop error: {e}")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()