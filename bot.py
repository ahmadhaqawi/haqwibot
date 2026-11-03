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
# إعدادات الـ Logging
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TradingBot")

app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_active"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# 1. الإعدادات
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "500000"))
WHALE_COOLDOWN = int(os.getenv("WHALE_COOLDOWN", "600"))
last_whale_alert: Dict[str, float] = {}

WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT",
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]

TIMEFRAMES = ["5m", "15m"]
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "45"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1.0"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3.0"))
LEVERAGE = 5
TRAIL_ACTIVATE_PCT = 1.0   # يبدأ الـ Trailing بعد ربح 1%
TRAIL_DISTANCE_PCT = 0.6   # يبعد الوقف 0.6% عن أعلى سعر

# حالة البوت
state: Dict[tuple, Dict[str, Any]] = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": cid,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=10
            )
        except Exception as e:
            logger.error(f"فشل إرسال تيليجرام إلى {cid}: {e}")

def fmt(p: float) -> str:
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def msg_entry(sym: str, side: str, price: float, sl: float, tp: float,
              rsi: float, adx: float, signal_type: str) -> str:
    title = f"🟢 <b>إشارة لونق ({signal_type})</b>" if side == "LONG" else f"🔴 <b>إشارة شورت ({signal_type})</b>"
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")

    return (
        f"{title}\n"
        f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
        f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
        f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
        f"⏰ وقت الدخول: <code>{now_str}</code>\n"
        f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}%)\n"
        f"🎯 هدف الربح: <code>{fmt(tp)}</code> (+{reward:.2f}%)\n"
        f"📊 RSI: <code>{rsi:.1f}</code> | ADX: <code>{adx:.1f}</code>\n"
        f"💡 <i>سيتم تفعيل Trailing Stop تلقائياً بعد ربح {TRAIL_ACTIVATE_PCT}%</i>"
    )

def msg_exit(sym: str, side: str, timeframe: str, price: float, entry: float,
             reason: str, pnl_pct: float, duration: str, entry_str: str, exit_str: str) -> str:
    title = f"⚪ <b>إغلاق صفقة لونق ({timeframe})</b>" if side == "LONG" else f"⚪ <b>إغلاق صفقة شورت ({timeframe})</b>"
    status = "ربح ✅" if pnl_pct >= 0 else "خسارة ❌"
    reasons_ar = {
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Take Profit": "تحقق هدف الربح 🎯",
        "Trailing Stop": "ضرب الـ Trailing Stop 🔄"
    }
    ar_reason = reasons_ar.get(reason, reason)
    leveraged_pnl = pnl_pct * LEVERAGE

    return (
        f"{title}\n"
        f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
        f"📥 الدخول: <code>{fmt(entry)}</code> (⏰ {entry_str})\n"
        f"📤 الخروج: <code>{fmt(price)}</code> (⏰ {exit_str})\n"
        f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code> ({status})\n"
        f"🔥 <b>بالرافعة ({LEVERAGE}x):</b> <code>{leveraged_pnl:+.2f}%</code>\n"
        f"⏱️ <b>مدة الصفقة:</b> <code>{duration}</code>\n"
        f"📝 السبب: {ar_reason}"
    )

# ==========================================
# 3. مراقبة الحيتان (WebSocket)
# ==========================================
async def process_whale_alert(platform: str, symbol_display: str, side_ar: str,
                              emoji: str, cost: float, price: float):
    key = f"{platform}_{symbol_display}"
    now = time.time()
    if now - last_whale_alert.get(key, 0) < WHALE_COOLDOWN:
        return
    last_whale_alert[key] = now

    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    msg = (
        f"🐋 <b>صفقة ضخمة</b>\n"
        f"🪙 الزوج: <code>{symbol_display}</code>\n"
        f"{emoji} النوع: <b>{side_ar}</b>\n"
        f"💰 القيمة: <code>${cost:,.0f}</code>\n"
        f"💵 السعر: <code>{fmt(price)}</code>\n"
        f"⏰ الوقت: <code>{now_str}</code>\n"
        f"🏛 المنصة: <b>{platform}</b>"
    )
    send(msg)

async def listen_to_binance():
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                logger.info("متصل بـ Binance WebSocket")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data'].get('e') == 'aggTrade':
                        trade = data['data']
                        price = float(trade['p'])
                        quantity = float(trade['q'])
                        cost = price * quantity
                        if cost >= WHALE_THRESHOLD_WS:
                            side_ar = "شراء (لونق)" if trade['m'] is False else "بيع (شورت)"
                            emoji = "🟢" if trade['m'] is False else "🔴"
                            symbol_display = trade['s'].replace('USDT', '/USDT')
                            await process_whale_alert("Binance", symbol_display, side_ar, emoji, cost, price)
        except Exception as e:
            logger.warning(f"Binance WS انقطع: {e} | إعادة الاتصال بعد 5 ثواني")
            await asyncio.sleep(5)

async def listen_to_okx():
    args = [{"channel": "trades", "instId": sym.replace("USDT", "-USDT-SWAP")} for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
                logger.info("متصل بـ OKX WebSocket")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['px'])
                            size = float(trade['sz'])
                            # ملاحظة: في OKX قد يكون الحجم بعدد العقود
                            cost = price * size
                            if cost >= WHALE_THRESHOLD_WS:
                                side = trade.get('side', 'buy')
                                side_ar = "شراء (لونق)" if side == 'buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'buy' else "🔴"
                                symbol_display = trade['instId'].replace("-SWAP", "").replace("-", "/")
                                await process_whale_alert("OKX", symbol_display, side_ar, emoji, cost, price)
        except Exception as e:
            logger.warning(f"OKX WS انقطع: {e} | إعادة الاتصال بعد 5 ثواني")
            await asyncio.sleep(5)

async def listen_to_bybit():
    args = [f"publicTrade.{sym}" for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
                logger.info("متصل بـ Bybit WebSocket")
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['p'])
                            size = float(trade['v'])
                            cost = price * size
                            if cost >= WHALE_THRESHOLD_WS:
                                side = trade.get('S', 'Buy')
                                side_ar = "شراء (لونق)" if side == 'Buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'Buy' else "🔴"
                                symbol_display = trade['s'].replace('USDT', '/USDT')
                                await process_whale_alert("Bybit", symbol_display, side_ar, emoji, cost, price)
        except Exception as e:
            logger.warning(f"Bybit WS انقطع: {e} | إعادة الاتصال بعد 5 ثواني")
            await asyncio.sleep(5)

async def run_websockets():
    await asyncio.gather(
        listen_to_binance(),
        listen_to_okx(),
        listen_to_bybit()
    )

def run_websocket_loop():
    asyncio.run(run_websockets())

# ==========================================
# 4. المؤشرات الفنية
# ==========================================
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / (atr_val + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    return dx.ewm(alpha=1/period, adjust=False).mean()

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"], 14)
    df["adx"] = adx(df["high"], df["low"], df["close"], 14)
    df["atr"] = atr(df, 14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["highest_high_20"] = df["high"].rolling(20).max().shift(1)
    df["lowest_low_20"] = df["low"].rolling(20).min().shift(1)
    return df

def check_signal(df_main: pd.DataFrame, df_1h: pd.DataFrame, timeframe: str,
                 position: Optional[str], entry_price: Optional[float] = None,
                 sl: Optional[float] = None, tp: Optional[float] = None,
                 highest_since_entry: Optional[float] = None,
                 lowest_since_entry: Optional[float] = None) -> Optional[Dict]:
    if len(df_main) < 50 or len(df_1h) < 50:
        return None

    last = df_main.iloc[-2]   # آخر شمعة مغلقة
    prev = df_main.iloc[-3]
    price = float(last["close"])

    # ===== إدارة الصفقات المفتوحة =====
    if position == "LONG":
        # تحديث أعلى سعر منذ الدخول (للـ Trailing)
        if highest_since_entry is None or price > highest_since_entry:
            highest_since_entry = price

        # تفعيل Trailing Stop
        if entry_price and highest_since_entry:
            profit_pct = (highest_since_entry - entry_price) / entry_price * 100
            if profit_pct >= TRAIL_ACTIVATE_PCT:
                new_sl = highest_since_entry * (1 - TRAIL_DISTANCE_PCT / 100)
                if sl is None or new_sl > sl:
                    sl = new_sl

        if sl and price <= sl:
            reason = "Trailing Stop" if (entry_price and (highest_since_entry - entry_price) / entry_price * 100 >= TRAIL_ACTIVATE_PCT) else "Stop Loss"
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": reason, "sl": sl, "highest": highest_since_entry}

        if tp and price >= tp:
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit", "sl": sl, "highest": highest_since_entry}

    elif position == "SHORT":
        if lowest_since_entry is None or price < lowest_since_entry:
            lowest_since_entry = price

        if entry_price and lowest_since_entry:
            profit_pct = (entry_price - lowest_since_entry) / entry_price * 100
            if profit_pct >= TRAIL_ACTIVATE_PCT:
                new_sl = lowest_since_entry * (1 + TRAIL_DISTANCE_PCT / 100)
                if sl is None or new_sl < sl:
                    sl = new_sl

        if sl and price >= sl:
            reason = "Trailing Stop" if (entry_price and (entry_price - lowest_since_entry) / entry_price * 100 >= TRAIL_ACTIVATE_PCT) else "Stop Loss"
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": reason, "sl": sl, "lowest": lowest_since_entry}

        if tp and price <= tp:
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit", "sl": sl, "lowest": lowest_since_entry}

    if position is not None:
        # نرجع الحالة المحدثة فقط (للـ Trailing)
        return {"action": "UPDATE", "sl": sl, "highest": highest_since_entry, "lowest": lowest_since_entry}

    # ===== إشارات الدخول =====
    trend_1h_up = df_1h.iloc[-2]["close"] > df_1h.iloc[-2]["ema21"]
    trend_1h_down = df_1h.iloc[-2]["close"] < df_1h.iloc[-2]["ema21"]
    vol_ok = last["volume"] > 1.2 * last["vol_ma"]
    adx_strong = last["adx"] > 30

    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    atr_mult = 2.0 if timeframe == "5m" else 2.5

    # ترند
    if (cross_up and trend_1h_up and adx_strong and vol_ok and 40 < last["rsi"] < 70):
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": price - atr_mult * last["atr"],
            "tp": price + (atr_mult * 2) * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند {timeframe}"
        }

    if (cross_down and trend_1h_down and adx_strong and vol_ok and 30 < last["rsi"] < 60):
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": price + atr_mult * last["atr"],
            "tp": price - (atr_mult * 2) * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند {timeframe}"
        }

    # اختراق
    breakout_vol = last["volume"] > 2.5 * last["vol_ma"]
    if (price > last["highest_high_20"] and breakout_vol and 60 < last["rsi"] < 80):
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": price - (atr_mult - 0.5) * last["atr"],
            "tp": price + (atr_mult * 2 - 1) * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"اختراق {timeframe} ⚡"
        }

    if (price < last["lowest_low_20"] and breakout_vol and 20 < last["rsi"] < 40):
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": price + (atr_mult - 0.5) * last["atr"],
            "tp": price - (atr_mult * 2 - 1) * last["atr"],
            "rsi": last["rsi"], "adx": last["adx"], "type": f"اختراق {timeframe} ⚡"
        }

    return None

# ==========================================
# 5. الفحص الرئيسي
# ==========================================
def scan_symbol(exchange, symbol: str, timeframe: str):
    try:
        state_key = (symbol, timeframe)

        if symbol not in exchange.markets:
            return

        if state_key not in state:
            state[state_key] = {
                "position": None,
                "entry": None,
                "sl": None,
                "tp": None,
                "last_candle_ts": None,
                "entry_time": None,
                "highest": None,
                "lowest": None
            }

        ohlcv_main = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=50)

        if len(ohlcv_main) < 50 or len(ohlcv_1h) < 50:
            return

        df_main = pd.DataFrame(ohlcv_main, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df_main = add_indicators(df_main)
        df_1h = add_indicators(df_1h)

        current_candle_ts = int(df_main.iloc[-2]["timestamp"])
        if state[state_key]["last_candle_ts"] == current_candle_ts:
            return
        state[state_key]["last_candle_ts"] = current_candle_ts

        pos = state[state_key]["position"]
        entry = state[state_key]["entry"]
        sl = state[state_key]["sl"]
        tp = state[state_key]["tp"]
        highest = state[state_key]["highest"]
        lowest = state[state_key]["lowest"]

        signal = check_signal(
            df_main, df_1h, timeframe, pos, entry, sl, tp, highest, lowest
        )

        if not signal:
            return

        # تحديث الـ Trailing فقط
        if signal["action"] == "UPDATE":
            if "sl" in signal and signal["sl"] is not None:
                state[state_key]["sl"] = signal["sl"]
            if "highest" in signal:
                state[state_key]["highest"] = signal["highest"]
            if "lowest" in signal:
                state[state_key]["lowest"] = signal["lowest"]
            return

        # فحص الحد اليومي
        today = datetime.now(KSA_TZ).date()
        if daily_pnl["date"] != today:
            daily_pnl["value"] = 0.0
            daily_pnl["date"] = today

        if daily_pnl["value"] <= -MAX_DAILY_LOSS:
            return

        if signal["action"] == "ENTRY":
            state[state_key].update({
                "position": signal["side"],
                "entry": signal["price"],
                "sl": signal["sl"],
                "tp": signal["tp"],
                "entry_time": datetime.now(timezone.utc),
                "highest": signal["price"] if signal["side"] == "LONG" else None,
                "lowest": signal["price"] if signal["side"] == "SHORT" else None
            })
            send(msg_entry(
                symbol, signal["side"], signal["price"],
                signal["sl"], signal["tp"],
                signal["rsi"], signal["adx"], signal["type"]
            ))
            logger.info(f"إشارة دخول | {symbol} | {timeframe} | {signal['side']} @ {signal['price']}")

        elif signal["action"] == "EXIT":
            pnl = (signal["price"] - entry) / entry * 100
            if pos == "SHORT":
                pnl = -pnl

            exit_time_utc = datetime.now(timezone.utc)
            entry_time_utc = state[state_key]["entry_time"]

            duration_str = "غير محدد"
            entry_str = "-"
            exit_str = "-"

            if entry_time_utc:
                time_diff = exit_time_utc - entry_time_utc
                total_seconds = int(time_diff.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                duration_str = f"{hours} ساعة و {minutes} دقيقة" if hours > 0 else f"{minutes} دقيقة"
                entry_str = entry_time_utc.astimezone(KSA_TZ).strftime("%I:%M %p")
                exit_str = exit_time_utc.astimezone(KSA_TZ).strftime("%I:%M %p")

            # حساب الـ PnL بشكل أدق (بناءً على نسبة المخاطرة)
            risk_amount = RISK_PCT
            actual_pnl_contribution = (pnl / abs((entry - state[state_key]["sl"]) / entry * 100)) * risk_amount if state[state_key]["sl"] else (pnl * LEVERAGE * risk_amount / 100)
            daily_pnl["value"] += actual_pnl_contribution

            state[state_key].update({
                "position": None,
                "entry": None,
                "sl": None,
                "tp": None,
                "entry_time": None,
                "highest": None,
                "lowest": None
            })

            send(msg_exit(
                symbol, pos, timeframe, signal["price"], entry,
                signal["reason"], pnl, duration_str, entry_str, exit_str
            ))
            logger.info(f"إغلاق صفقة | {symbol} | {timeframe} | PnL: {pnl:+.2f}% | السبب: {signal['reason']}")

            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send(f"🛑 <b>تم إيقاف البوت مؤقتاً</b>\nوصلت للحد الأقصى للخسارة اليومية ({MAX_DAILY_LOSS}%).")

    except Exception as e:
        logger.error(f"خطأ في فحص {symbol} ({timeframe}): {e}", exc_info=False)

# ==========================================
# 6. التشغيل الرئيسي
# ==========================================
def trading_loop():
    logger.info("بدء تشغيل البوت المحسّن (V10) - Multi-Timeframe + Trailing Stop")

    ws_thread = threading.Thread(target=run_websocket_loop, daemon=True)
    ws_thread.start()

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    exchange.load_markets()
    logger.info(f"تم تحميل الأسواق من OKX | عدد الرموز: {len(FUTURES_SYMBOLS)}")

    send(
        f"🛡️ <b>تم تشغيل البوت المحسّن (V10)</b>\n"
        f"📈 <b>الفريمات:</b> 5 دقائق و 15 دقيقة\n"
        f"🛑 <b>وقف الخسارة:</b> ديناميكي (ATR) + Trailing Stop\n"
        f"🧠 <b>فلتر القوة:</b> ADX > 30\n"
        f"🐋 <b>مراقب الحيتان:</b> أكبر من ${WHALE_THRESHOLD_WS:,.0f}\n"
        f"⚙️ الرافعة: <b>{LEVERAGE}x</b>\n"
        f"📉 الحد اليومي للخسارة: <b>{MAX_DAILY_LOSS}%</b>"
    )

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                for tf in TIMEFRAMES:
                    scan_symbol(exchange, sym, tf)
                    time.sleep(0.35)  # تأخير خفيف لتجنب Rate Limit
        except Exception as e:
            logger.error(f"خطأ في اللوب الرئيسي: {e}")

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()