def build_signal_message(symbol, side, action, price, rsi, tf):
    # ترجمة الاتجاه
    side_ar = "شراء" if side == "LONG" else "بيع"
    emoji   = "🟢" if side == "LONG" else "🔴"

    # عنوان الرسالة حسب نوع الحركة
    if action == "ENTRY":
        title = f"{emoji} <b>إشارة دخول {side_ar}</b>"
    else:
        title = f"{emoji} <b>إشارة خروج من صفقة {side_ar}</b>"

    lines = [
        title,
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

        lines.append(f"وقف الخسارة: <code>{fmt_price(sl)}</code>")
        lines.append(f"هدف الربح: <code>{fmt_price(tp)}</code>")
        entry_info[symbol] = {"entry": price, "sl": sl, "tp": tp}
    else:
        e = entry_info.pop(symbol, None)
        if e:
            pnl = (price - e["entry"]) / e["entry"] * 100
            if side == "SHORT":
                pnl = -pnl
            status = "ربح ✅" if pnl >= 0 else "خسارة ❌"
            lines.append(f"سعر الدخول: <code>{fmt_price(e['entry'])}</code>")
            lines.append(f"النتيجة: <code>{pnl:+.2f}%</code> ({status})")

    return "\n".join(lines)