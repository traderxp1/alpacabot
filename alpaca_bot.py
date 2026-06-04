"""
Alpaca Stocks Bot - رنكو للأسهم الأمريكية
نفس منطق بوت بايننس لكن على Alpaca Paper Trading
"""

import os
import logging
import threading
import time as time_module
import json
from flask import Flask, request, jsonify
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import requests

# ====================================================================
# الإعدادات
# ====================================================================
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "PKLWAHSB3YKRPVPIYTK2K3JU6E")
ALPACA_API_SECRET = os.environ.get("ALPACA_API_SECRET", "3cNDSeDdiHCYuPYmbjGJvdG5NQmwsy1wrFWHFxsw5jnm")
ALPACA_BASE_URL   = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET", "renko2026")

# ====================================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)
state_lock = threading.Lock()

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_API_KEY,
    "APCA-API-SECRET-KEY": ALPACA_API_SECRET,
    "Content-Type": "application/json"
}

# ====================================================================
# الحالة
# ====================================================================
states = {}
trades_log = []
processed_signals = []

def fresh_state(symbol):
    return {
        "in_trade": False,
        "pending": False,
        "symbol": symbol,
        "entry_price": None,
        "backup_sl": None,
        "tp_price": None,
        "qty": 0.0,
        "order_id": None,
        "sl_order_id": None,
        "be_active": False,
        "last_action": None,
        "last_error": None,
    }

def get_state(symbol):
    if symbol not in states:
        states[symbol] = fresh_state(symbol)
    return states[symbol]

def reset_symbol(symbol):
    with state_lock:
        states[symbol] = fresh_state(symbol)

def log_action(symbol, action, details=""):
    s = get_state(symbol)
    s["last_action"] = action
    log.info(f"[{symbol}] ACTION: {action} | {details}")

def is_duplicate(data):
    sig = f"{data.get('action')}_{data.get('symbol')}_{data.get('entry','')}_{data.get('backup_sl','')}_{data.get('exit_price','')}"
    if sig in processed_signals[-20:]:
        return True
    processed_signals.append(sig)
    if len(processed_signals) > 100:
        processed_signals.pop(0)
    return False

# ====================================================================
# دوال Alpaca API
# ====================================================================
def alpaca_get(endpoint):
    r = requests.get(f"{ALPACA_BASE_URL}/{endpoint}", headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.json()

def alpaca_post(endpoint, data):
    r = requests.post(f"{ALPACA_BASE_URL}/{endpoint}", headers=HEADERS, json=data, timeout=10)
    r.raise_for_status()
    return r.json()

def alpaca_delete(endpoint):
    r = requests.delete(f"{ALPACA_BASE_URL}/{endpoint}", headers=HEADERS, timeout=10)
    if r.status_code not in (200, 204):
        r.raise_for_status()
    return True

def get_current_price(symbol):
    try:
        r = requests.get(
            f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest",
            headers=HEADERS, timeout=10
        )
        data = r.json()
        return float(data["quote"]["ap"])  # ask price
    except Exception:
        return None

def cancel_all_orders(symbol):
    try:
        orders = alpaca_get(f"orders?status=open&symbols={symbol}")
        for o in orders:
            try:
                alpaca_delete(f"orders/{o['id']}")
                log.info(f"[{symbol}] ألغى أوردر {o['id']}")
            except Exception as e:
                log.error(f"[{symbol}] فشل إلغاء {o['id']}: {e}")
    except Exception as e:
        log.error(f"[{symbol}] فشل جلب الأوردرات: {e}")

def place_stop_loss(symbol, qty, sl_price):
    try:
        order = alpaca_post("orders", {
            "symbol": symbol,
            "qty": str(round(qty, 6)),
            "side": "sell",
            "type": "stop",
            "stop_price": str(round(sl_price, 4)),
            "time_in_force": "gtc"
        })
        log.info(f"[{symbol}] SL احتياطي @ {sl_price} qty={qty}")
        return order["id"]
    except Exception as e:
        log.error(f"[{symbol}] فشل SL: {e}")
        get_state(symbol)["last_error"] = str(e)
        return None

def market_buy(symbol, qty):
    order = alpaca_post("orders", {
        "symbol": symbol,
        "qty": str(round(qty, 6)),
        "side": "buy",
        "type": "market",
        "time_in_force": "day"
    })
    return order

def market_sell(symbol, qty):
    order = alpaca_post("orders", {
        "symbol": symbol,
        "qty": str(round(qty, 6)),
        "side": "sell",
        "type": "market",
        "time_in_force": "day"
    })
    return order

def stop_buy(symbol, qty, stop_price):
    order = alpaca_post("orders", {
        "symbol": symbol,
        "qty": str(round(qty, 6)),
        "side": "buy",
        "type": "stop",
        "stop_price": str(round(stop_price, 4)),
        "time_in_force": "gtc"
    })
    return order

def is_market_open():
    try:
        clock = alpaca_get("clock")
        return clock.get("is_open", False)
    except Exception:
        return True  # افترض مفتوح لو فشل الفحص

def get_position_qty(symbol):
    try:
        pos = alpaca_get(f"positions/{symbol}")
        return float(pos.get("qty", 0))
    except Exception:
        return 0.0

# ====================================================================
# معالجات الإشارات
# ====================================================================
def handle_pending_entry(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)

    if s["in_trade"] or s["pending"]:
        return {"status": "ignored", "reason": f"{symbol} نشط"}

    if not is_market_open():
        return {"status": "ignored", "reason": "السوق مغلق"}

    try:
        current = get_current_price(symbol)
        log.info(f"[{symbol}] سعر حالي={current} إشارة={entry}")

        # لو السعر تجاوز الإشارة بأكثر من 0.5% → شراء فوري
        if current and (current - entry) / entry * 100 > 0.5:
            log.info(f"[{symbol}] تجاوز الإشارة → شراء فوري")
            order = market_buy(symbol, qty)
            sl_id = place_stop_loss(symbol, qty, sl)
            with state_lock:
                s.update({
                    "in_trade": True, "pending": False,
                    "entry_price": current, "backup_sl": sl, "tp_price": tp,
                    "qty": qty, "order_id": order["id"], "sl_order_id": sl_id
                })
            log_action(symbol, "ENTRY_MARKET", f"qty={qty}")
            return {"status": "ok", "method": "market"}

        # وضع أوردر بيستوب
        order = stop_buy(symbol, qty, entry)
        with state_lock:
            s.update({
                "pending": True,
                "entry_price": entry, "backup_sl": sl, "tp_price": tp,
                "qty": qty, "order_id": order["id"]
            })
        log_action(symbol, "PENDING_ENTRY", f"entry={entry} qty={qty}")
        return {"status": "ok", "order_id": order["id"]}

    except Exception as e:
        s["last_error"] = str(e)
        log.error(f"[{symbol}] فشل الدخول: {e}")
        return {"status": "error", "message": str(e)}


def handle_exit(data):
    symbol = data.get("symbol")
    reason = data.get("exit_reason", "?")
    exit_price = data.get("exit_price", 0)
    pnl = data.get("pnl", 0)
    s = get_state(symbol)

    if not s["in_trade"] and not s["pending"]:
        return {"status": "ignored", "reason": f"{symbol} لا توجد صفقة"}

    try:
        cancel_all_orders(symbol)
        qty = s["qty"]

        # جرّب تجيب الكمية من Alpaca مباشرة
        actual_qty = get_position_qty(symbol)
        if actual_qty > 0:
            qty = actual_qty

        if qty <= 0:
            reset_symbol(symbol)
            return {"status": "warning", "reason": "لا يوجد رصيد"}

        sell = market_sell(symbol, qty)
        log.info(f"[{symbol}] بيع: {sell['id']} السبب={reason}")

        trades_log.append({
            "time":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol":      symbol,
            "entry":       s.get("entry_price"),
            "exit":        exit_price,
            "exit_reason": reason,
            "pnl":         pnl,
        })

        reset_symbol(symbol)
        log_action(symbol, "EXIT", f"reason={reason} sold={qty}")
        return {"status": "ok", "sold": qty}
    except Exception as e:
        s["last_error"] = str(e)
        log.error(f"[{symbol}] فشل الخروج: {e}")
        return {"status": "error", "message": str(e)}


def handle_update_backup_sl(data):
    symbol = data.get("symbol")
    new_sl = float(data["backup_sl"])
    s = get_state(symbol)

    if not s["in_trade"] and not s["pending"]:
        return {"status": "ignored", "reason": f"{symbol} غير نشط"}

    if s["pending"]:
        with state_lock:
            s["backup_sl"] = new_sl
        log_action(symbol, "UPDATE_SL_PENDING", f"SL={new_sl}")
        return {"status": "ok", "note": "محفوظ للتطبيق بعد التنفيذ"}

    try:
        cancel_all_orders(symbol)
        sl_id = place_stop_loss(symbol, s["qty"], new_sl)
        with state_lock:
            s["backup_sl"] = new_sl
            s["be_active"] = True
            s["sl_order_id"] = sl_id
        log_action(symbol, "UPDATE_BACKUP_SL", f"SL={new_sl}")
        return {"status": "ok", "new_sl": new_sl}
    except Exception as e:
        s["last_error"] = str(e)
        return {"status": "error", "message": str(e)}


def handle_cancel_pending(data):
    symbol = data.get("symbol")
    if not symbol:
        return {"status": "ignored"}
    cancel_all_orders(symbol)
    reset_symbol(symbol)
    log_action(symbol, "CANCEL_PENDING")
    return {"status": "ok"}


# ====================================================================
# مراقب الأوردرات
# ====================================================================
def monitor_orders():
    while True:
        try:
            time_module.sleep(15)
            pending = [sym for sym, st in list(states.items()) if st.get("pending")]
            for symbol in pending:
                s = get_state(symbol)
                if not s.get("pending") or not s.get("order_id"):
                    continue
                try:
                    order = alpaca_get(f"orders/{s['order_id']}")
                    status = order.get("status", "")
                    if status == "filled":
                        actual_qty = float(order.get("filled_qty", s["qty"]))
                        log.info(f"[{symbol}] ✅ أوردر نُفّذ qty={actual_qty}")
                        sl_id = place_stop_loss(symbol, actual_qty, s["backup_sl"])
                        with state_lock:
                            s.update({
                                "in_trade": True, "pending": False,
                                "qty": actual_qty, "sl_order_id": sl_id
                            })
                        log_action(symbol, "PENDING_FILLED", f"qty={actual_qty}")
                    elif status in ("canceled", "expired", "rejected"):
                        log.info(f"[{symbol}] أوردر {status}")
                        reset_symbol(symbol)
                except Exception as e:
                    log.error(f"[{symbol}] مراقب: {e}")
        except Exception as e:
            log.error(f"مراقب: {e}")


# ====================================================================
# Webhook
# ====================================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    secret = request.headers.get("X-Webhook-Secret") or request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return jsonify({"error": "غير مصرح"}), 401
    try:
        raw = request.get_data(as_text=True).strip()
        data = None
        for line in reversed(raw.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    data = json.loads(line)
                    break
                except Exception:
                    continue
        if not data:
            return jsonify({"error": "JSON خاطئ"}), 400
        if not data.get("symbol"):
            return jsonify({"error": "لا يوجد رمز"}), 400

        data["symbol"] = str(data["symbol"]).upper().strip()
        if is_duplicate(data):
            return jsonify({"status": "مكرر"}), 200

        action = data.get("action", "").upper()
        log.info(f"وصل: {json.dumps(data)}")

        if action in ("PENDING_ENTRY", "ENTRY"):
            r = handle_pending_entry(data)
        elif action == "EXIT":
            r = handle_exit(data)
        elif action == "UPDATE_BACKUP_SL":
            r = handle_update_backup_sl(data)
        elif action == "CANCEL_PENDING":
            r = handle_cancel_pending(data)
        else:
            r = {"status": "إجراء غير معروف", "action": action}
        return jsonify(r), 200
    except Exception as e:
        log.error(f"Webhook خطأ: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ====================================================================
# Reset
# ====================================================================
@app.route("/reset/<symbol>", methods=["GET", "POST"])
def reset_route(symbol):
    secret = request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return jsonify({"error": "غير مصرح"}), 401
    symbol = symbol.upper()
    cancel_all_orders(symbol)
    reset_symbol(symbol)
    return jsonify({"status": "ok", "symbol": symbol})


# ====================================================================
# الداشبورد
# ====================================================================
@app.route("/", methods=["GET"])
def dashboard():
    def reason_ar(r):
        if r == "TP": return "تيك بروفت"
        if r in ("BE", "SL_MARKET"): return "بريك ايفن"
        if r == "SL": return "ستوب لوز"
        return r

    active = [sym for sym, st in states.items() if st["in_trade"] or st["pending"]]
    cards = ""
    if not active:
        cards = '<div class="card"><p style="color:#555;text-align:center">لا توجد صفقات نشطة</p></div>'
    else:
        for sym in active:
            st = states[sym]
            status_txt = "🟢 صفقة مفتوحة" if st["in_trade"] else "🟡 أمر معلّق"
            scls = "green" if st["in_trade"] else "yellow"
            cards += f"""<div class="card"><h2>{sym}</h2>
<div class="row"><span class="label">الوضع</span><span class="val {scls}">{status_txt}</span></div>
<div class="row"><span class="label">سعر الدخول</span><span class="val">{st['entry_price'] or '—'}</span></div>
<div class="row"><span class="label">🛡️ SL</span><span class="val red">{st['backup_sl'] or '—'}</span></div>
<div class="row"><span class="label">Take Profit</span><span class="val green">{st['tp_price'] or '—'}</span></div>
<div class="row"><span class="label">الكمية</span><span class="val">{st['qty'] or '—'}</span></div>
<div class="row"><span class="label">Breakeven</span><span class="val {'yellow' if st['be_active'] else 'label'}">{'✅ مفعّل' if st['be_active'] else 'غير مفعّل'}</span></div>
<div class="row"><span class="label">آخر إجراء</span><span class="val">{st['last_action'] or '—'}</span></div>
<div class="row"><span class="label">آخر خطأ</span><span class="val red">{st['last_error'] or '—'}</span></div>
</div>"""

    summary = ""
    rows = '<p style="color:#555;font-size:12px;padding:8px">لا توجد صفقات بعد</p>'
    if trades_log:
        total_pnl = sum(float(t["pnl"]) for t in trades_log)
        tp_count  = sum(1 for t in trades_log if t["exit_reason"] == "TP")
        be_count  = sum(1 for t in trades_log if t["exit_reason"] in ("BE","SL_MARKET"))
        sl_count  = sum(1 for t in trades_log if t["exit_reason"] == "SL")
        clr = "green" if total_pnl >= 0 else "red"
        summary = f'''<div class="card">
<div class="row"><span class="label">الاجمالي</span><span class="val {clr}">{total_pnl:+.2f} USD</span></div>
<div class="row"><span class="label">✅ تيك بروفت</span><span class="val green">{tp_count}</span></div>
<div class="row"><span class="label">➡️ بريك ايفن</span><span class="val yellow">{be_count}</span></div>
<div class="row"><span class="label">❌ ستوب لوز</span><span class="val red">{sl_count}</span></div>
</div>'''
        body = "".join(f'<tr><td>{t["time"]}</td><td>{t["symbol"]}</td><td>{reason_ar(t["exit_reason"])}</td><td class="{"green" if float(t["pnl"])>=0 else "red"}">{float(t["pnl"]):+.2f}</td></tr>' for t in reversed(trades_log[-30:]))
        rows = f'<table><tr><th>الوقت</th><th>رمز</th><th>النتيجة</th><th>P&L</th></tr>{body}</table>'

    html = f"""<!DOCTYPE html><html dir="rtl" lang="ar"><head><meta charset="UTF-8">
<meta http-equiv="refresh" content="10"><title>Alpaca Bot</title><style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:#0a0a0f;color:#e0e0e0;font-family:monospace;padding:24px}}
h1{{color:#00ff88;font-size:20px;margin-bottom:8px}}
.sub{{color:#555;font-size:12px;margin-bottom:20px}}
.card{{background:#12121a;border:1px solid #1e1e2e;border-radius:8px;padding:16px;margin-bottom:16px}}
.card h2{{color:#7878ff;font-size:14px;margin-bottom:12px}}
.row{{display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #1a1a2a;font-size:13px}}
.row:last-child{{border-bottom:none}}
.label{{color:#888}} .val{{color:#fff;font-weight:bold}}
.green{{color:#00ff88}} .red{{color:#ff4466}} .yellow{{color:#ffcc00}}
table{{width:100%;border-collapse:collapse;font-size:12px}}
th{{background:#1e1e2e;color:#7878ff;padding:8px;text-align:right}}
td{{padding:7px 8px;border-bottom:1px solid #1a1a2a}}
.footer{{color:#333;font-size:11px;margin-top:16px;text-align:center}}
</style></head><body>
<h1>⚡ ALPACA BOT · أسهم أمريكية</h1>
<div class="sub">🧪 Paper Trading · فلوس وهمية</div>
{summary}
<h2 style="color:#7878ff;font-size:13px;margin-bottom:12px">العملات النشطة</h2>
{cards}
<div class="card"><h2>سجل الصفقات ({len(trades_log)})</h2>{rows}</div>
<p class="footer">يتحدث كل 10 ثواني</p>
</body></html>"""
    return html


# ====================================================================
# التشغيل
# ====================================================================
@app.before_request
def startup():
    pass

if __name__ == "__main__":
    monitor_thread = threading.Thread(target=monitor_orders, daemon=True)
    monitor_thread.start()
    log.info("🚀 Alpaca Bot يبدأ")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
