import os
import sys
import threading
import time
import json
import asyncio
import websockets
from datetime import datetime, timezone, timedelta
import io
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import numpy as np
import ccxt
import pandas as pd
import requests
from flask import Flask

app = Flask(__name__)

@app.route("/")
@app.route("/healthz")
def health_check():
    return {"status": "ok", "service": "trading_bot_active_v21_whales"}, 200

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, use_reloader=False)

# ==========================================
# 1. الإعدادات والمتغيرات (Config)
# ==========================================
KSA_TZ = timezone(timedelta(hours=3))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

FUTURES_SYMBOLS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT", "BNB/USDT:USDT",
    "XRP/USDT:USDT", "DOGE/USDT:USDT", "ADA/USDT:USDT", "AVAX/USDT:USDT",
    "LINK/USDT:USDT", "DOT/USDT:USDT", "NEAR/USDT:USDT", "APT/USDT:USDT",
    "SUI/USDT:USDT", "ARB/USDT:USDT", "OP/USDT:USDT", "INJ/USDT:USDT",
    "ATOM/USDT:USDT", "LTC/USDT:USDT", "BCH/USDT:USDT", "FIL/USDT:USDT",
    "PEPE/USDT:USDT", "WIF/USDT:USDT", "TIA/USDT:USDT", "ORDI/USDT:USDT",
    "SEI/USDT:USDT", "RUNE/USDT:USDT", "FET/USDT:USDT", "RENDER/USDT:USDT",
    "IMX/USDT:USDT", "STX/USDT:USDT", "THETA/USDT:USDT", "ALGO/USDT:USDT",
    "EGLD/USDT:USDT", "FLOW/USDT:USDT", "SAND/USDT:USDT"
]

TIMEFRAMES = ["5m", "15m", "1h", "4h"]
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "30"))
LEVERAGE = 5

STATE_FILE = "bot_state.json"
state = {}
daily_pnl = {"value": 0.0, "trades": 0, "wins": 0, "date": str(datetime.now(KSA_TZ).date())}

# إعدادات الحيتان
WHALE_THRESHOLD_WS = float(os.getenv("WHALE_THRESHOLD_WS", "1000000"))
WHALE_COOLDOWN     = int(os.getenv("WHALE_COOLDOWN", "300"))
last_whale_alert   = {}

WEBSOCKET_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT",
]

OKX_CONTRACT_SIZES = {
    "BTC-USDT-SWAP": 0.01,
    "ETH-USDT-SWAP": 0.1,
    "SOL-USDT-SWAP": 1.0,
    "BNB-USDT-SWAP": 0.01,
    "XRP-USDT-SWAP": 100.0,
    "DOGE-USDT-SWAP": 100.0,
    "ADA-USDT-SWAP": 100.0,
    "AVAX-USDT-SWAP": 1.0,
    "LINK-USDT-SWAP": 1.0,
    "SUI-USDT-SWAP": 1.0,
}

plt.style.use('dark_background')

# ==========================================
# إدارة حفظ واسترجاع الحالة
# ==========================================
def load_state():
    global state, daily_pnl
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                state = {tuple(k.split("|")): v for k, v in data.get("state", {}).items()}
                daily_pnl = data.get("daily_pnl", daily_pnl)
                print(f"[STATE] Loaded {len(state)} tracked positions.")
        except Exception as e:
            print(f"[STATE ERROR] Failed to load state: {e}")

def save_state():
    try:
        data = {
            "state": {f"{k[0]}|{k[1]}": v for k, v in state.items()},
            "daily_pnl": daily_pnl
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE ERROR] Failed to save state: {e}")

# ==========================================
# 2. إرسال رسائل تيليجرام
# ==========================================
def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            res = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                timeout=10
            )
            if not res.ok:
                print(f"[TG ERROR] HTTP {res.status_code}: {res.text}")
        except Exception as e:
            print(f"[TG ERROR] {e}")

def send_photo(photo_buf, caption):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    chat_ids = [cid.strip() for cid in TELEGRAM_CHAT_ID.split(",") if cid.strip()]
    for cid in chat_ids:
        try:
            res = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": cid, "caption": caption, "parse_mode": "HTML"},
                files={"photo": ("chart.png", photo_buf, "image/png")},
                timeout=15
            )
            if not res.ok:
                print(f"[TG PHOTO ERROR] HTTP {res.status_code}: {res.text}")
        except Exception as e:
            print(f"[TG PHOTO ERROR] {e}")

def fmt(p):
    if p is None: return "0.00"
    return f"{p:,.4f}" if p >= 1 else f"{p:.6f}"

def is_good_trading_time():
    now = datetime.now(KSA_TZ)
    if 2 <= now.hour < 7: return False
    return True

# ==========================================
# 3. توليد الشارت
# ==========================================
def generate_chart(symbol, df, timeframe, side, resistance, support, entry, sl, tp):
    fig, ax = plt.subplots(figsize=(12, 6))
    df_plot = df.tail(40).reset_index(drop=True)

    for i in range(len(df_plot)):
        row = df_plot.iloc[i]
        color = '#26a69a' if row['close'] >= row['open'] else '#ef5350'
        ax.plot([i, i], [row['low'], row['high']], color=color, linewidth=1)
        height = abs(row['close'] - row['open'])
        bottom = min(row['close'], row['open'])
        rect = mpatches.Rectangle((i - 0.3, bottom), 0.6, height, color=color)
        ax.add_patch(rect)

    half_idx = len(df_plot) // 2
    first_half, second_half = df_plot.iloc[:half_idx], df_plot.iloc[half_idx:]

    try:
        if side == "LONG":
            p1_idx = int(first_half['low'].idxmin())
            p1_val = float(first_half['low'].min())
            p2_idx = int(second_half['low'].idxmin())
            p2_val = float(second_half['low'].min())
            if p2_idx > p1_idx and p2_val >= p1_val:
                slope = (p2_val - p1_val) / (p2_idx - p1_idx)
                end_val = p1_val + slope * ((len(df_plot) - 1) - p1_idx)
                ax.plot([p1_idx, len(df_plot) - 1], [p1_val, end_val], color='#FFD700', linewidth=2, label='Uptrend')
        elif side == "SHORT":
            p1_idx = int(first_half['high'].idxmax())
            p1_val = float(first_half['high'].max())
            p2_idx = int(second_half['high'].idxmax())
            p2_val = float(second_half['high'].max())
            if p2_idx > p1_idx and p2_val <= p1_val:
                slope = (p2_val - p1_val) / (p2_idx - p1_idx)
                end_val = p1_val + slope * ((len(df_plot) - 1) - p1_idx)
                ax.plot([p1_idx, len(df_plot) - 1], [p1_val, end_val], color='#FFD700', linewidth=2, label='Downtrend')
    except Exception as e:
        print(f"[CHART TREND ERROR] {e}")

    valid_vals = [v for v in [df_plot['high'].max(), resistance, tp, sl, entry] if v is not None and not np.isnan(v)]
    min_vals = [v for v in [df_plot['low'].min(), support, tp, sl, entry] if v is not None and not np.isnan(v)]

    max_p = max(valid_vals) if valid_vals else entry * 1.05
    min_p = min(min_vals) if min_vals else entry * 0.95
    p_range = max_p - min_p if max_p > min_p else entry * 0.05
    ax.set_ylim(min_p - (p_range * 0.1), max_p + (p_range * 0.1))

    if resistance is not None and not np.isnan(resistance):
        ax.axhline(resistance, color='#32CD32', linestyle='--', linewidth=1.5)
        ax.text(len(df_plot) - 1, resistance, f" Res: {fmt(resistance)} ", color='#32CD32', va='bottom', ha='right', fontweight='bold')
    if support is not None and not np.isnan(support):
        ax.axhline(support, color='#FF1493', linestyle='--', linewidth=1.5)
        ax.text(len(df_plot) - 1, support, f" Sup: {fmt(support)} ", color='#FF1493', va='top', ha='right', fontweight='bold')

    ax.axhline(entry, color='#00BFFF', linestyle='-', linewidth=2, alpha=0.8)
    ax.axhline(tp, color='#00FF00', linestyle=':', linewidth=2)
    ax.axhline(sl, color='#FF0000', linestyle=':', linewidth=2)

    x_pos = len(df_plot) - 1
    ax.text(x_pos, entry, f" ENTRY: {fmt(entry)} ", color='#00BFFF', va='center', ha='right', fontweight='bold')
    ax.text(x_pos, tp, f" TP: {fmt(tp)} ", color='#00FF00', va='bottom', ha='right', fontweight='bold')
    ax.text(x_pos, sl, f" SL: {fmt(sl)} ", color='#FF0000', va='top', ha='right', fontweight='bold')

    ax.set_title(f"{symbol.split(':')[0]} - {timeframe} | {side}", color='white', fontsize=14, fontweight='bold')
    step = max(1, len(df_plot) // 8)
    ax.set_xticks(range(0, len(df_plot), step))
    ax.set_xticklabels([df_plot.iloc[i]['datetime'].strftime('%H:%M') for i in range(0, len(df_plot), step)], rotation=45, color='gray')
    plt.yticks(color='gray')
    ax.grid(color='gray', linestyle='--', linewidth=0.5, alpha=0.3)
    ax.legend(loc='upper left', facecolor='black', edgecolor='white')

    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=120)
    buf.seek(0)
    plt.close(fig)
    return buf

# ==========================================
# 4. صياغة الرسائل
# ==========================================
def msg_entry(sym, side, price, sl, tp, rsi_val, adx_val, signal_type, support, resistance):
    title = f"🟢 <b>توصية شراء (LONG) - {signal_type}</b>" if side == "LONG" else f"🔴 <b>توصية بيع (SHORT) - {signal_type}</b>"
    risk = abs(price - sl) / price * 100
    reward = abs(tp - price) / price * 100
    rr_ratio = reward / (risk + 1e-9)
    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")

    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code>\n"
            f"⚙️ الرافعة: <b>{LEVERAGE}x</b> (الربح المستهدف: +{reward*LEVERAGE:.1f}%)\n"
            f"💵 سعر الدخول: <code>{fmt(price)}</code>\n"
            f"⏰ وقت الدخول: <code>{now_str}</code>\n"
            f"🟢 المقاومة: <code>{fmt(resistance)}</code>\n"
            f"🔴 الدعم: <code>{fmt(support)}</code>\n"
            f"🛑 وقف الخسارة: <code>{fmt(sl)}</code> (-{risk:.2f}% | رافعة: -{risk*LEVERAGE:.1f}%)\n"
            f"🎯 هدف الربح (TP): <code>{fmt(tp)}</code> (+{reward:.2f}% | R:R 1:{rr_ratio:.1f})\n"
            f"📊 RSI: <code>{rsi_val:.1f}</code> | ADX: <code>{adx_val:.1f}</code>\n"
            f"🛡️ <i>ميزة الـ Breakeven الذكي (حماية الأرباح) مفعلة تلقائياً.</i>")

def msg_exit(sym, side, timeframe, exit_price, entry_price, reason):
    if side == "LONG":
        raw_pnl_pct = (exit_price - entry_price) / entry_price * 100
    else:
        raw_pnl_pct = (entry_price - exit_price) / entry_price * 100

    leveraged_pnl = raw_pnl_pct * LEVERAGE
    pnl_sign = "+" if leveraged_pnl > 0 else ""
    pnl_icon = "🟢" if leveraged_pnl > 0 else "🔴" if leveraged_pnl < 0 else "⚪"

    title = f"{pnl_icon} <b>إغلاق صفقة ({timeframe})</b>"
    reasons_ar = {
        "Take Profit": "تحقق هدف الربح بنجاح 🎯",
        "Stop Loss": "ضرب وقف الخسارة 🛑",
        "Breakeven Hit": "خروج عند نقطة الدخول (حماية رأس المال) 🛡️"
    }

    return (f"{title}\n"
            f"🪙 الزوج: <code>{sym.split(':')[0]}</code> ({side})\n"
            f"📥 سعر الدخول: <code>{fmt(entry_price)}</code>\n"
            f"📤 سعر الخروج: <code>{fmt(exit_price)}</code>\n"
            f"📈 العائد الصافي: <b>{pnl_sign}{leveraged_pnl:.2f}%</b> (الرافعة {LEVERAGE}x)\n"
            f"📝 السبب: {reasons_ar.get(reason, reason)}")

# ==========================================
# 5. المؤشرات الفنية
# ==========================================
def ema(s, p): return s.ewm(span=p, adjust=False).mean()

def rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).ewm(alpha=1/p, adjust=False).mean()
    l = -d.clip(upper=0).ewm(alpha=1/p, adjust=False).mean()
    return 100 - (100 / (1 + g / (l + 1e-9)))

def adx(high, low, close, p=14):
    up, down = high.diff(), -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr_val = tr.ewm(alpha=1/p, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    minus_di = 100 * (minus_dm.ewm(alpha=1/p, adjust=False).mean() / (atr_val + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    return dx.ewm(alpha=1/p, adjust=False).mean()

def atr(df, p=14):
    h, l, c = df["high"], df["low"], df["close"]
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/p, adjust=False).mean()

def add_indicators(df):
    df["ema9"] = ema(df["close"], 9)
    df["ema21"] = ema(df["close"], 21)
    df["rsi"] = rsi(df["close"])
    df["adx"] = adx(df["high"], df["low"], df["close"])
    df["atr"] = atr(df)
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["high20"] = df["high"].rolling(20).max().shift(1)
    df["low20"] = df["low"].rolling(20).min().shift(1)
    return df

# ==========================================
# 6. مراقبة الحيتان (Non-blocking & Accurate)
# ==========================================
async def process_whale_alert(platform, symbol_display, side_ar, emoji, cost, price):
    key = f"{platform}_{symbol_display}"
    now = time.time()
    if now - last_whale_alert.get(key, 0) < WHALE_COOLDOWN:
        return
    last_whale_alert[key] = now

    # تحديد عنوان الرسالة حسب نوع الصفقة: صفقة شراء / صفقة بيع
    trade_word = "صفقة شراء" if "شراء" in side_ar else "صفقة بيع"

    now_str = datetime.now(KSA_TZ).strftime("%I:%M %p")
    msg = (f"🐋 <b>{trade_word} ({platform})</b>\n"
           f"🪙 الزوج: <code>{symbol_display}</code>\n"
           f"{emoji} النوع: <b>{side_ar}</b>\n"
           f"💰 القيمة: <code>${cost:,.0f}</code>\n"
           f"💵 السعر: <code>{fmt(price)}</code>\n"
           f"⏰ الوقت: <code>{now_str}</code>")
    await asyncio.to_thread(send, msg)

async def listen_to_binance():
    streams = "/".join([f"{sym.lower()}@aggTrade" for sym in WEBSOCKET_SYMBOLS])
    uri = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                print("[WS] Binance connected", flush=True)
                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)
                    if 'data' in data and data['data'].get('e') == 'aggTrade':
                        t = data['data']
                        price = float(t['p'])
                        qty = float(t['q'])
                        cost = price * qty
                        if cost >= WHALE_THRESHOLD_WS:
                            is_buy = (t['m'] == False)
                            side_ar = "شراء (لونق)" if is_buy else "بيع (شورت)"
                            emoji = "🟢" if is_buy else "🔴"
                            sym_disp = t['s'].replace('USDT', '/USDT')
                            await process_whale_alert("Binance", sym_disp, side_ar, emoji, cost, price)
        except Exception as e:
            print(f"[WS BINANCE ERROR] {e}", flush=True)
            await asyncio.sleep(5)

async def listen_to_okx():
    args = [{"channel": "trades", "instId": sym.replace("USDT", "-USDT-SWAP")}
            for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://ws.okx.com:8443/ws/v5/public"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                print("[WS] OKX connected", flush=True)
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                while True:
                    raw = await ws.recv()
                    data = json.loads(raw)
                    if 'data' in data and data['data']:
                        for t in data['data']:
                            inst_id = t.get('instId', '')
                            price = float(t['px'])
                            contracts = float(t['sz'])
                            ct_val = OKX_CONTRACT_SIZES.get(inst_id, 1.0)
                            coin_amount = contracts * ct_val
                            cost = price * coin_amount

                            if cost >= WHALE_THRESHOLD_WS:
                                side = t.get('side', 'buy')
                                side_ar = "شراء (لونق)" if side == 'buy' else "بيع (شورت)"
                                emoji = "🟢" if side == 'buy' else "🔴"
                                sym_disp = inst_id.replace("-SWAP", "").replace("-", "/")
                                await process_whale_alert("OKX", sym_disp, side_ar, emoji, cost, price)
        except Exception as e:
            print(f"[WS OKX ERROR] {e}", flush=True)
            await asyncio.sleep(5)

async def listen_to_bybit():
    args = [f"publicTrade.{sym}" for sym in WEBSOCKET_SYMBOLS]
    uri = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                print("[WS] Bybit connected", flush=True)
                await ws.send(json.dumps({"op": "subscribe", "args": args}))

                async def bybit_ping():
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            break

                ping_task = asyncio.create_task(bybit_ping())

                try:
                    while True:
                        raw = await ws.recv()
                        data = json.loads(raw)
                        if 'data' in data and data['data']:
                            for t in data['data']:
                                price = float(t['p'])
                                size = float(t['v'])
                                cost = price * size
                                if cost >= WHALE_THRESHOLD_WS:
                                    is_buy = (t.get('S', 'Buy') == 'Buy')
                                    side_ar = "شراء (لونق)" if is_buy else "بيع (شورت)"
                                    emoji = "🟢" if is_buy else "🔴"
                                    sym_disp = t['s'].replace('USDT', '/USDT')
                                    await process_whale_alert("Bybit", sym_disp, side_ar, emoji, cost, price)
                finally:
                    ping_task.cancel()
        except Exception as e:
            print(f"[WS BYBIT ERROR] {e}", flush=True)
            await asyncio.sleep(5)

async def run_websockets():
    await asyncio.gather(
        listen_to_binance(),
        listen_to_okx(),
        listen_to_bybit(),
    )

def run_websocket_loop():
    while True:
        try:
            asyncio.run(run_websockets())
        except Exception as e:
            print(f"[FATAL WS LOOP ERROR] {e}", flush=True)
            time.sleep(5)

# ==========================================
# 7. الفحص الدقيق والتحكم بالصفقات
# ==========================================
def check_signal(df_main, df_1h, timeframe, position, entry_price=None, sl=None, tp=None, breakeven_hit=False):
    if len(df_main) < 50 or len(df_1h) < 50: return None

    last = df_main.iloc[-2]
    prev = df_main.iloc[-3]
    current = df_main.iloc[-1]
    last_1h = df_1h.iloc[-2]

    if position == "LONG":
        if current["high"] >= tp:
            return {"action": "EXIT", "side": "LONG", "price": tp, "reason": "Take Profit"}
        if current["low"] <= sl:
            reason = "Breakeven Hit" if breakeven_hit and abs(sl - entry_price) < 1e-5 else "Stop Loss"
            return {"action": "EXIT", "side": "LONG", "price": sl, "reason": reason}
        be_trigger = entry_price + (tp - entry_price) * 0.5
        if not breakeven_hit and entry_price and current["high"] >= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price, "reason": "Breakeven Activated"}

    elif position == "SHORT":
        if current["low"] <= tp:
            return {"action": "EXIT", "side": "SHORT", "price": tp, "reason": "Take Profit"}
        if current["high"] >= sl:
            reason = "Breakeven Hit" if breakeven_hit and abs(sl - entry_price) < 1e-5 else "Stop Loss"
            return {"action": "EXIT", "side": "SHORT", "price": sl, "reason": reason}
        be_trigger = entry_price - (entry_price - tp) * 0.5
        if not breakeven_hit and entry_price and current["low"] <= be_trigger:
            return {"action": "UPDATE_SL", "new_sl": entry_price, "reason": "Breakeven Activated"}

    if position is not None: return None
    if not is_good_trading_time(): return None

    price = last["close"]
    trend_1h_up = last_1h["close"] > last_1h["ema21"] and last_1h["close"] > last_1h["ema9"]
    trend_1h_down = last_1h["close"] < last_1h["ema21"] and last_1h["close"] < last_1h["ema9"]
    vol_ok = last["volume"] > 1.3 * last["vol_ma"]

    adx_strong = last["adx"] > (25 if timeframe in ["1h", "4h"] else 30)
    volatility_ok = (0.6 * df_main["atr"].iloc[-20:].mean()) < last["atr"] < (2.2 * df_main["atr"].iloc[-20:].mean())

    cross_up = prev["ema9"] <= prev["ema21"] and last["ema9"] > last["ema21"]
    cross_down = prev["ema9"] >= prev["ema21"] and last["ema9"] < last["ema21"]

    ema_spread = abs(last["ema9"] - last["ema21"]) / price * 100 >= 0.02

    atr_mult = 2.0 if timeframe == "5m" else 2.2 if timeframe == "15m" else 2.5 if timeframe == "1h" else 2.8
    support, resistance = last["low20"], last["high20"]

    if cross_up and trend_1h_up and adx_strong and vol_ok and ema_spread and volatility_ok and (45 < last["rsi"] < 65):
        sl_calc = price - atr_mult * last["atr"]
        tp_calc = price + (atr_mult * 2.2) * last["atr"]
        return {
            "action": "ENTRY", "side": "LONG", "price": price,
            "sl": sl_calc, "tp": tp_calc,
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند قوي {timeframe}",
            "support": support, "resistance": resistance
        }

    if cross_down and trend_1h_down and adx_strong and vol_ok and ema_spread and volatility_ok and (35 < last["rsi"] < 55):
        sl_calc = price + atr_mult * last["atr"]
        tp_calc = price - (atr_mult * 2.2) * last["atr"]
        return {
            "action": "ENTRY", "side": "SHORT", "price": price,
            "sl": sl_calc, "tp": tp_calc,
            "rsi": last["rsi"], "adx": last["adx"], "type": f"ترند قوي {timeframe}",
            "support": support, "resistance": resistance
        }

    return None

def fetch_data_safe(exchange, symbol, timeframe, limit=100, retries=3):
    for i in range(retries):
        try:
            return exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
        except Exception as e:
            if i == retries - 1:
                print(f"[FETCH ERROR] {symbol} {timeframe}: {e}")
                return []
            time.sleep(1)
    return []

def scan_symbol_timeframe(exchange, symbol, timeframe, df_1h):
    try:
        state_key = (symbol, timeframe)
        if state_key not in state:
            state[state_key] = {
                "position": None, "entry": None, "sl": None, "tp": None,
                "last_candle_ts": None, "breakeven_hit": False
            }

        if timeframe == "1h":
            df = df_1h.copy()
        else:
            ohlcv = fetch_data_safe(exchange, symbol, timeframe, limit=100)
            if len(ohlcv) < 50: return
            df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = add_indicators(df)

        pos = state[state_key]["position"]
        current_candle_ts = int(df.iloc[-2]["timestamp"])

        if pos is None and state[state_key]["last_candle_ts"] == current_candle_ts:
            return

        entry = state[state_key]["entry"]
        sl = state[state_key]["sl"]
        tp = state[state_key]["tp"]
        be_hit = state[state_key]["breakeven_hit"]

        signal = check_signal(df, df_1h, timeframe, pos, entry, sl, tp, be_hit)
        if not signal: return

        if signal["action"] == "ENTRY":
            state[state_key].update({
                "position": signal["side"], "entry": signal["price"],
                "sl": signal["sl"], "tp": signal["tp"], "breakeven_hit": False,
                "last_candle_ts": current_candle_ts
            })
            save_state()
            chart_buf = generate_chart(symbol, df, timeframe, signal["side"], signal["resistance"], signal["support"], signal["price"], signal["sl"], signal["tp"])
            caption = msg_entry(symbol, signal["side"], signal["price"], signal["sl"], signal["tp"], signal["rsi"], signal["adx"], signal["type"], signal["support"], signal["resistance"])
            send_photo(chart_buf, caption)

        elif signal["action"] == "UPDATE_SL":
            state[state_key]["sl"] = signal["new_sl"]
            state[state_key]["breakeven_hit"] = True
            save_state()
            send(f"🛡️ <b>تحديث وقف الخسارة (Breakeven)</b>\n"
                 f"🪙 الزوج: <code>{symbol.split(':')[0]}</code> ({timeframe})\n"
                 f"✅ تم نقل وقف الخسارة تلقائياً إلى سعر الدخول: <code>{fmt(signal['new_sl'])}</code> لحماية رأس المال بعد قطع 50% من الهدف.")

        elif signal["action"] == "EXIT":
            exit_price = signal["price"]

            raw_pnl = ((exit_price - entry) / entry * 100) if pos == "LONG" else ((entry - exit_price) / entry * 100)
            lev_pnl = raw_pnl * LEVERAGE
            daily_pnl["trades"] += 1
            daily_pnl["value"] += lev_pnl
            if lev_pnl > 0:
                daily_pnl["wins"] += 1

            state[state_key].update({"position": None, "entry": None, "sl": None, "tp": None, "breakeven_hit": False})
            save_state()
            send(msg_exit(symbol, pos, timeframe, exit_price, entry, signal["reason"]))

    except Exception as e:
        print(f"[ERROR] {symbol} ({timeframe}): {e}", flush=True)

# ==========================================
# 8. التشغيل الرئيسي
# ==========================================
def trading_loop():
    print("[INIT] Starting V21 Bot (Optimized + Whales Fixed)...", flush=True)
    load_state()

    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 15000
    })

    try:
        exchange.load_markets()
        active_symbols = [s for s in FUTURES_SYMBOLS if s in exchange.markets]
        print(f"[OKX] Loaded {len(active_symbols)} valid swap markets out of {len(FUTURES_SYMBOLS)}")
    except Exception as e:
        print(f"[MARKETS ERROR] {e}")
        active_symbols = FUTURES_SYMBOLS

    ws_thread = threading.Thread(target=run_websocket_loop, daemon=True)
    ws_thread.start()
    print("[INIT] Whale monitor started via WebSockets.", flush=True)

    send("🛡️ <b>تم تشغيل البوت المحدّث (V21) بنجاح</b>\n"
         "⚡ <b>التحسينات النشطة:</b>\n"
         "• متابعة حية وفورية للصفقات (Live Candle Exit).\n"
         "• حماية رأس المال الذكية (Dynamic Breakeven عند 50% من الهدف).\n"
         "• حفظ واسترجاع الصفقات بعد إعادة تشغيل السيرفر تلقائياً.\n"
         "• خفض استهلاك الـ API بنسبة 60% وتسريع فحص الأزواج.\n"
         f"🐋 <b>مراقب الحيتان مفعّل:</b> صفقات لحظية أكبر من <code>${WHALE_THRESHOLD_WS:,.0f}</code> عبر (Binance, OKX, Bybit).")

    while True:
        cycle_start = time.time()
        try:
            today_str = str(datetime.now(KSA_TZ).date())
            if daily_pnl["date"] != today_str:
                daily_pnl["date"] = today_str
                daily_pnl["trades"] = 0
                daily_pnl["wins"] = 0
                daily_pnl["value"] = 0.0
                save_state()

            for sym in active_symbols:
                ohlcv_1h = fetch_data_safe(exchange, sym, "1h", limit=60)
                if len(ohlcv_1h) < 50:
                    continue
                df_1h = pd.DataFrame(ohlcv_1h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df_1h['datetime'] = pd.to_datetime(df_1h['timestamp'], unit='ms')
                df_1h = add_indicators(df_1h)

                for tf in TIMEFRAMES:
                    scan_symbol_timeframe(exchange, sym, tf, df_1h)

        except Exception as e:
            print(f"[FATAL LOOP ERROR] {e}", flush=True)

        elapsed = time.time() - cycle_start
        sleep_dur = max(5, SCAN_INTERVAL - elapsed)
        time.sleep(sleep_dur)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
