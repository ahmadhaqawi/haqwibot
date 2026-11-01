import os
import sys
import threading
import time
from datetime import datetime, timezone

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


# ---------- Config from env ----------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

SYMBOLS = [s.strip() for s in os.getenv(
    "SYMBOLS", "BTC/USDT,ETH/USDT,SOL/USDT").split(",") if s.strip()]
TIMEFRAME = os.getenv("TIMEFRAME", "5m")
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))

RSI_LONG_ENTRY = float(os.getenv("RSI_LONG_ENTRY", "55"))
RSI_LONG_EXIT = float(os.getenv("RSI_LONG_EXIT", "50"))
RSI_SHORT_ENTRY = float(os.getenv("RSI_SHORT_ENTRY", "45"))
RSI_SHORT_EXIT = float(os.getenv("RSI_SHORT_EXIT", "50"))

SL_PCT = float(os.getenv("SL_PCT", "0.02"))   # 2%
TP_PCT = float(os.getenv("TP_PCT", "0.04"))   # 4%


def log(level, msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level}] {msg}", flush=True)


# ---------- Telegram ----------
def send_telegram_alert(message: str, disable_preview: bool = True) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("ERROR", "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": disable_preview,
    }
    for attempt in range(1, 4):
        try:
            res = requests.post(url, json=payload, timeout=10)
            data = res.json()
            if not data.get("ok"):
                log("ERROR", f"Telegram API: {data}")
                return False
            log("INFO", "Telegram alert delivered.")
            return True
        except Exception as e:
            log("EXCEPTION", f"Telegram send failed (try {attempt}): {e}")
            time.sleep(2)
    return False


# ---------- RSI (Wilder's) ----------
def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / (avg_loss + 1e-9)
    return 100 - (100 / (1 + rs))


# ---------- State ----------
positions = {sym: None for sym in SYMBOLS}          # None | 'LONG' | 'SHORT'
last_candle_ts = {sym: None for sym in SYMBOLS}      # last processed closed candle
entry_info = {}                                      # sym -> {entry, sl, tp}


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}"


def build_signal_message(symbol, side, action, price, rsi, tf):
    emoji = {"LONG": "🟢", "SHORT": "🔴"}
    action_ar = "دخول" if action == "ENTRY" else "خروج"
    lines = [
        f"{emoji.get(side, '⚪')} <b>{side} {action_ar}</b>",
        f"الزوج: <code>{symbol}</code>",
        f"الفريم: <code>{tf}</code>",
        f"السعر: <code>{fmt_price(price)}</code>",
        f"RSI: <code>{rsi:.2f}</code>",
    ]
    if action == "ENTRY":
        if side == "LONG":
            sl = price * (1 - SL_PCT)
            tp = price * (1 + TP_PCT)
        else:
            sl = price * (1 + SL_PCT)
            tp = price * (1 - TP_PCT)
        lines.append(f"SL: <code>{fmt_price(sl)}</code>")
        lines.append(f"TP: <code>{fmt_price(tp)}</code>")
        entry_info[symbol] = {"entry": price, "sl": sl, "tp": tp}
    else:
        e = entry_info.pop(symbol, None)
        if e:
            pnl = (price - e["entry"]) / e["entry"] * 100
            if side == "SHORT":
                pnl = -pnl
            lines.append(f"PnL: <code>{pnl:+.2f}%</code>")
    return "\n".join(lines)


def scan_symbol(exchange, symbol):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=RSI_PERIOD + 50)
        if not ohlcv or len(ohlcv) < RSI_PERIOD + 5:
            log("WARN", f"Insufficient candles for {symbol}")
            return

        df = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["rsi"] = calculate_rsi(df["close"], RSI_PERIOD)

        # استخدم آخر شمعة مغلقة (iloc[-2])
        closed = df.iloc[-2]
        closed_ts = int(closed["timestamp"])
        close_price = float(closed["close"])
        rsi_val = float(closed["rsi"])
        prev_rsi = float(df["rsi"].iloc[-3]) if len(df) >= 3 else rsi_val

        # تجنب معالجة نفس الشمعة مرتين
        if last_candle_ts[symbol] == closed_ts:
            return
        last_candle_ts[symbol] = closed_ts

        pos = positions[symbol]
        log("SCAN",
            f"{symbol} | close={close_price} | RSI={rsi_val:.2f} (prev={prev_rsi:.2f}) | pos={pos}")

        if pos is None:
            # LONG entry: تقاطع RSI صاعدًا فوق 55
            if prev_rsi < RSI_LONG_ENTRY <= rsi_val:
                positions[symbol] = "LONG"
                send_telegram_alert(build_signal_message(
                    symbol, "LONG", "ENTRY", close_price, rsi_val, TIMEFRAME))
            # SHORT entry: تقاطع RSI هابطًا تحت 45
            elif prev_rsi > RSI_SHORT_ENTRY >= rsi_val:
                positions[symbol] = "SHORT"
                send_telegram_alert(build_signal_message(
                    symbol, "SHORT", "ENTRY", close_price, rsi_val, TIMEFRAME))
        elif pos == "LONG":
            if rsi_val <= RSI_LONG_EXIT:
                positions[symbol] = None
                send_telegram_alert(build_signal_message(
                    symbol, "LONG", "EXIT", close_price, rsi_val, TIMEFRAME))
        elif pos == "SHORT":
            if rsi_val >= RSI_SHORT_EXIT:
                positions[symbol] = None
                send_telegram_alert(build_signal_message(
                    symbol, "SHORT", "EXIT", close_price, rsi_val, TIMEFRAME))

    except ccxt.ExchangeError as e:
        log("ERROR", f"Exchange error on {symbol}: {e}")
    except Exception as e:
        log("ERROR", f"Error scanning {symbol}: {e}")


def trading_loop():
    log("INIT", "Background trading loop started.")
    exchange = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })

    log("INIT", f"Symbols: {SYMBOLS} | TF: {TIMEFRAME} | Interval: {SCAN_INTERVAL}s")
    send_telegram_alert(
        "🚀 <b>Trading Bot is live</b>\n"
        f"Symbols: <code>{', '.join(SYMBOLS)}</code>\n"
        f"TF: <code>{TIMEFRAME}</code>\n"
        f"RSI long in/out: <code>{RSI_LONG_ENTRY}/{RSI_LONG_EXIT}</code>\n"
        f"RSI short in/out: <code>{RSI_SHORT_ENTRY}/{RSI_SHORT_EXIT}</code>\n"
        f"SL/TP: <code>{SL_PCT*100:.1f}% / {TP_PCT*100:.1f}%</code>"
    )

    while True:
        try:
            for sym in SYMBOLS:
                scan_symbol(exchange, sym)
                time.sleep(1)
        except Exception as e:
            log("FATAL", f"Loop error: {e}")
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()