import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
import asyncio
import websockets
import json
import ccxt
import pandas as pd
import requests
from flask import Flask

app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_active"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# 1. الإعدادات والمتغيرات (Config)
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "500000"))
WHALE_COOLDOWN = int(os.getenv("WHALE_COOLDOWN", "600"))
last_whale_alert = {}

WEBSOCKET_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT",
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT"
]

TIMEFRAMES = ["15m", "5m"]          # 15m أساسي، 5m ثانوي وبشروط أقوى
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "45"))
RISK_PCT = float(os.getenv("RISK_PER_TRADE", "1.0"))
MAX_DAILY_LOSS = float(os.getenv("MAX_DAILY_LOSS", "3.0"))
LEVERAGE = 5

# إعدادات الاستراتيجية المحسّنة
ADX_MIN_15M = 32
ADX_MIN_5M = 35
VOL_MULT_TREND = 1.5
VOL_MULT_BREAKOUT = 3.0
ATR_MULT_15M = 2.2
ATR_MULT_5M = 1.8

state = {}
daily_pnl = {"value": 0.0, "date": datetime.now(KSA_TZ).date()}

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
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
            print(f"[TG ERROR] Failed to send to {cid}: {e}")

def fmt(p):
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def msg_entry(sym, side, price, sl, tp, rsi, adx, signal_type):
    if side == "LONG":
        title = f"🟢 <b>إشارة لونق ({signal_type})</b>"
    else:
        title = f"🔴 <b>إشارة شورت ({signal_type})</b>"

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
        f"💡 <i>بمجرد ربح 1%، قم بتعديل وقف الخسارة إلى سعر الدخول.</i>"
    )

def msg_exit(sym, side, timeframe, price, entry, reason, pnl_pct, duration, entry_str, exit_str):
    title = f"⚪ <b>إغلاق صفقة لونق ({timeframe})</b>" if side == "LONG" else f"⚪ <b>إغلاق صفقة شورت ({timeframe})</b>"
    reasons_ar = {
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Take Profit": "تحقق هدف الربح 🎯"
    }
    ar_reason = reasons_ar.get(reason, reason)
    leveraged_pnl = pnl_pct * LEVERAGE

    return (
        f"{title}\n"
        f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
        f"📥 الدخول: <code>{fmt(entry)}</code> (⏰ {entry_str})\n"
        f"📤 الخروج: <code>{fmt(price)}</code> (⏰ {exit_str})\n"
        f"📊 النتيجة: <code>{pnl_pct:+.2f}%</code>\n"
        f"🔥 <b>بالرافعة ({LEVERAGE}x):</b> <code>{leveraged_pnl:+.2f}%</code>\n"
        f"⏱️ <b>مدة الصفقة:</b> <code>{duration}</code>\n"
        f"📝 السبب: {ar_reason}"
    )

# ==========================================
# 3. محرك مراقبة الحيتان
# ==========================================
async def process_whale_alert(platform, symbol_display, side_ar, emoji, cost, price):
    global last_whale_alert
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
        f"⏰ الوقت: <code>{now_str}</code>"
    )
    send(msg)

async def listen_to_binance():
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data'].get('e') == 'aggTrade':
                        trade = data['data']
                        price, quantity = float(trade['p']), float(trade['q'])
                        cost = price * quantity
                        if cost >= WHALE_THRESHOLD_WS:
                            side_ar = "شراء (لونق)" if trade['m'] == False else "بيع (شورت)"
                            emoji = "🟢" if trade['m'] == False else "🔴"
                            symbol_display = trade['s'].replace('USDT', '/USDT')
                            await process_whale_alert("Binance", symbol_display, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(5)

async def listen_to_okx():
    args = [{"channel": "trades", "instId": sym.replace("USDT", "-USDT-SWAP")} for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['px'])
                            size = float(trade['sz'])
                            cost = price * size
                            side = trade.get('side', 'buy')
                            if cost >= WHALE_THRESHOLD_WS:
                                side_ar = "شراء (لونق)" if side == 'buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'buy' else "🔴"
                                symbol_display = trade['instId'].replace("-SWAP", "").replace("-", "/")
                                await process_whale_alert("OKX", symbol_display, side_ar, emoji, cost, price)
        except Exception:
            await asyncio.sleep(5)

async def listen_to_bybit():
    args = [f"publicTrade.{sym}" for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    if 'data' in data and data['data']:
                        for trade in data['data']:
                            price = float(trade['p'])
                            size = float(trade['v'])
                            cost = price * size
                            side = trade.get('S', 'Buy')
                            if cost >= WHALE_THRESHOLD_WS:
                                side_ar = "شراء (لونق)" if side == 'Buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'Buy' else "🔴"
                                symbol_display = trade['s'].replace('USDT', '/USDT')
                                await process_whale_alert("Bybit", symbol_display, side_ar, emoji, cost, price)
        except Exception:
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
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, adjust=False).mean()
    loss = -delta.clip(upper=0).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def adx(high, low, close, period=14):
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

def atr(df, period=14):
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/period, adjust=False).mean()

def add_indicators(df):
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["ema50"] = ema(df["close"], 50)
    df["rsi"] = rsi(df["close"], 14)
    df["adx"] = adx(df["high"], df["low"], df["close"], 14)
    df["atr"] = atr(df, 14)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["highest_high_20"] = df["high"].rolling(20).max().shift(1)
    df["lowest_low_20"] = df["low"].rolling(20).min().shift(1)
    df["ema21_slope"] = df["ema21"].diff(3)
    return df

def check_signal(df_main, df_1h, df_4h, timeframe, position, entry_price=None, sl=None, tp=None):
    if len(df_main) < 60 or len(df_1h) < 50 or len(df_4h) < 40:
        return None

    last = df_main.iloc[-2]
    prev = df_main.iloc[-3]
    price = last["close"]

    # ========== إدارة الخروج ==========
    if position == "LONG":
        if price <= sl:
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Stop Loss"}
        if price >= tp:
            return {"action": "EXIT", "side": "LONG", "price": price, "reason": "Take Profit"}
    if position == "SHORT":
        if price >= sl:
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Stop Loss"}
        if price <= tp:
            return {"action": "EXIT", "side": "SHORT", "price": price, "reason": "Take Profit"}

    if position is not None:
        return None

    # ========== فلاتر الترند العليا ==========
    trend_1h_up = df_1h.iloc[-2]["close"] > df_1h.iloc[-2]["ema21"]
    trend_1h_down = df_1h.iloc[-2]["close"] < df_1h.iloc[-2]["ema21"]

    trend_4h_up = df_4h.iloc[-2]["close"] > df_4h.iloc[-2]["ema21"]
    trend_4h_down = df_4h.iloc[-2]["close"] < df_4h.iloc[-2]["ema21"]

    slope_up = last["ema21_slope"] > 0
    slope_down = last["ema21_slope"] < 0

    if timeframe == "15m":
        adx_min = ADX_MIN_15M
        atr_mult = ATR_MULT_15M
        vol_mult = VOL_MULT_TREND
    else:
        adx_min = ADX_MIN_5M
        atr_mult = ATR_MULT_5M
        vol_mult = VOL_MULT_TREND * 1.3

    adx_ok = last["adx"] >= adx_min
    vol_ok = last["volume"] > vol_mult * last["vol_ma"]

    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    # لونق ترند
    if (cross_up
        and trend_1h_up
        and trend_4h_up
        and slope_up
        and adx_ok
        and vol_ok
        and price > last["ema21"]
        and 45 < last["rsi"] < 68):

        sl_price = price - atr_mult * last["atr"]
        tp_price = price + (atr_mult * 2.0) * last["atr"]
        return {
            "action": "ENTRY",
            "side": "LONG",
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
            "rsi": last["rsi"],
            "adx": last["adx"],
            "type": f"ترند قوي {timeframe}"
        }

    # شورت ترند
    if (cross_down
        and trend_1h_down
        and trend_4h_down
        and slope_down
        and adx_ok
        and vol_ok
        and price < last["ema21"]
        and 32 < last["rsi"] < 55):

        sl_price = price + atr_mult * last["atr"]
        tp_price = price - (atr_mult * 2.0) * last["atr"]
        return {
            "action": "ENTRY",
            "side": "SHORT",
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
            "rsi": last["rsi"],
            "adx": last["adx"],
            "type": f"ترند قوي {timeframe}"
        }

    # اختراق
    breakout_vol = last["volume"] > VOL_MULT_BREAKOUT * last["vol_ma"]

    if (price > last["highest_high_20"]
        and breakout_vol
        and trend_1h_up
        and trend_4h_up
        and last["adx"] > adx_min + 3
        and 58 < last["rsi"] < 78):

        sl_price = price - (atr_mult * 0.9) * last["atr"]
        tp_price = price + (atr_mult * 1.8) * last["atr"]
        return {
            "action": "ENTRY",
            "side": "LONG",
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
            "rsi": last["rsi"],
            "adx": last["adx"],
            "type": f"اختراق {timeframe} ⚡"
        }

    if (price < last["lowest_low_20"]
        and breakout_vol
        and trend_1h_down
        and trend_4h_down
        and last["adx"] > adx_min + 3
        and 22 < last["rsi"] < 42):

        sl_price = price + (atr_mult * 0.9) * last["atr"]
        tp_price = price - (atr_mult * 1.8) * last["atr"]
        return {
            "action": "ENTRY",
            "side": "SHORT",
            "price": price,
            "sl": sl_price,
            "tp": tp_price,
            "rsi": last["rsi"],
            "adx": last["adx"],
            "type": f"اختراق {timeframe} ⚡"
        }

    return None

# ==========================================
# 5. الفحص الرئيسي
# ==========================================
def scan_symbol(exchange, symbol, timeframe):
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
                "entry_time": None
            }

        ohlcv_main = exchange.fetch_ohlcv(symbol, timeframe, limit=100)
        ohlcv_1h = exchange.fetch_ohlcv(symbol, "1h", limit=60)
        ohlcv_4h = exchange.fetch_ohlcv(symbol, "4h", limit=50)

        if len(ohlcv_main) < 60 or len(ohlcv_1h) < 50 or len(ohlcv_4h) < 40:
            return

        df_main = pd.DataFrame(ohlcv_main, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df_main = add_indicators(df_main)
        df_1h = add_indicators(df_1h)
        df_4h = add_indicators(df_4h)

        current_candle_ts = int(df_main.iloc[-2]["timestamp"])
        if state[state_key]["last_candle_ts"] == current_candle_ts:
            return
        state[state_key]["last_candle_ts"] = current_candle_ts

        pos = state[state_key]["position"]
        entry = state[state_key]["entry"]
        sl = state[state_key]["sl"]
        tp = state[state_key]["tp"]

        # منع التضارب بين الفريمات
        has_opposite = False
        for tf in TIMEFRAMES:
            other_key = (symbol, tf)
            if other_key in state and state[other_key]["position"] is not None:
                other_pos = state[other_key]["position"]
                if pos is None or other_pos != pos:
                    has_opposite = True
                    break

        signal = check_signal(df_main, df_1h, df_4h, timeframe, pos, entry, sl, tp)
        if not signal:
            return

        if signal["action"] == "ENTRY" and has_opposite:
            return

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
                "entry_time": datetime.now(timezone.utc)
            })
            send(msg_entry(
                symbol, signal["side"], signal["price"],
                signal["sl"], signal["tp"],
                signal["rsi"], signal["adx"], signal["type"]
            ))

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

            state[state_key].update({
                "position": None,
                "entry": None,
                "sl": None,
                "tp": None,
                "entry_time": None
            })

            daily_pnl["value"] += (pnl * LEVERAGE) * (RISK_PCT / 100)
            send(msg_exit(
                symbol, pos, timeframe, signal["price"], entry,
                signal["reason"], pnl, duration_str, entry_str, exit_str
            ))

            if daily_pnl["value"] <= -MAX_DAILY_LOSS:
                send("🛑 <b>تم إيقاف البوت مؤقتاً</b>\nوصلت للحد الأقصى للخسارة اليومية.")

    except Exception as e:
        print(f"[ERROR] Scan {symbol} ({timeframe}): {e}", flush=True)

# ==========================================
# 6. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting Improved Bot V10 (Multi-Timeframe + 4H Filter)...", flush=True)

    ws_thread = threading.Thread(target=run_websocket_loop, daemon=True)
    ws_thread.start()

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    })
    exchange.load_markets()

    send(
        f"🛡️ <b>تم تشغيل البوت المحسّن (V10)</b>\n"
        f"📈 <b>الفريمات:</b> 15 دقيقة (أساسي) + 5 دقائق (ثانوي)\n"
        f"🧠 <b>فلتر الترند:</b> 1H + 4H + ميل EMA\n"
        f"🛑 <b>وقف الخسارة:</b> ديناميكي (ATR)\n"
        f"📊 <b>ADX:</b> 32+ على 15م | 35+ على 5م\n"
        f"🐋 <b>مراقب الحيتان:</b> أكبر من ${WHALE_THRESHOLD_WS:,.0f}\n"
        f"⚙️ الرافعة: <b>{LEVERAGE}x</b>"
    )

    while True:
        try:
            for sym in FUTURES_SYMBOLS:
                for tf in TIMEFRAMES:
                    scan_symbol(exchange, sym, tf)
                    time.sleep(0.8)
        except Exception as e:
            print(f"[FATAL] Loop error: {e}", flush=True)

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
