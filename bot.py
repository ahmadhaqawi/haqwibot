import os
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

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

def send(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: 
        print("[TG ERROR] Token or Chat ID missing.")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=10)
        print("[TG SUCCESS] Message sent.", flush=True)
    except Exception as e:
        print(f"[TG ERROR] {e}")

def msg_entry(sym, side, price, sl, tp, rsi):
    title = "🟢 <b>إشارة صفقة شراء (LONG)</b>" if side == "LONG" else "🔴 <b>إشارة صفقة بيع (SHORT)</b>"
    return (f"{title}\n"
            f"➖➖➖➖➖➖➖➖\n"
            f"🪙 الزوج: <code>{sym}</code>\n"
            f"💵 سعر الدخول: <code>{price}</code>\n"
            f"🛑 وقف الخسارة: <code>{sl}</code>\n"
            f"🎯 هدف الربح: <code>{tp}</code>\n"
            f"📊 RSI: <code>{rsi}</code>\n"
            f"⚠️ <b>هذه إشارة تجريبية للتأكد من عمل البوت</b>")

def trading_loop():
    print("[INIT] Starting test bot...", flush=True)
    send("🚀 <b>البوت يعمل الآن، جاري إرسال الإشارات التجريبية...</b>")
    time.sleep(3)
    
    # إرسال 3 إشارات تجريبية فورية
    send(msg_entry("BTC/USDT", "LONG", 65000.0, 64000.0, 67000.0, 55.5))
    time.sleep(3)
    send(msg_entry("ETH/USDT", "SHORT", 3500.0, 3600.0, 3300.0, 42.1))
    
    print("[INIT] Test signals sent successfully!", flush=True)

    # حلقة مستمرة لإبقاء السيرفر حياً
    while True:
        time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    trading_loop()
