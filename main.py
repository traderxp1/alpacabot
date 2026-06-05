"""
Alpaca Stocks Bot v6 ANALYTICS DASHBOARD Railway/GitHub
+ قاعدة بيانات PostgreSQL كاملة
+ داشبورد تحليلي شامل V6: أداء، إشارات، أحداث، صفقات نشطة
"""

import os
import logging
import threading
import time as time_module
import json
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import requests
import psycopg
from psycopg.rows import dict_row
from html import escape

# ====================================================================
ALPACA_API_KEY    = os.environ.get("ALPACA_API_KEY", "")
ALPACA_API_SECRET = os.environ.get("ALPACA_API_SECRET", "")
ALPACA_BASE_URL   = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets/v2")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET", "renko2026")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")

# V6 Dashboard: Active positions + performance analytics + signal/action logs.
# Compatible with latest TradingView Renko strategy alerts:
# PLACE_BUY_STOP / PENDING_ENTRY = place real waiting buy-stop order
# ENTRY = immediate market entry only when strategy is in Fast Green Close mode
# CANCEL_PENDING / UPDATE_BACKUP_SL / EXIT are supported

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
# قاعدة البيانات
# ====================================================================
def get_db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id SERIAL PRIMARY KEY,
                        open_time TIMESTAMP,
                        close_time TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        entry_price FLOAT,
                        exit_price FLOAT,
                        sl_price FLOAT,
                        tp_price FLOAT,
                        exit_reason VARCHAR(20),
                        qty FLOAT,
                        pnl FLOAT,
                        pnl_pct FLOAT,
                        duration_min INT,
                        rr_actual FLOAT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS active_states (
                        symbol VARCHAR(20) PRIMARY KEY,
                        state_json TEXT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signal_events (
                        id SERIAL PRIMARY KEY,
                        received_at TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        action VARCHAR(40),
                        status VARCHAR(40),
                        reason TEXT,
                        entry_price FLOAT,
                        sl_price FLOAT,
                        tp_price FLOAT,
                        qty FLOAT,
                        exit_price FLOAT,
                        pnl FLOAT,
                        raw_json TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS action_events (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        action VARCHAR(60),
                        details TEXT
                    )
                """)

                # --- V6 FIX: migrate old PostgreSQL tables instead of only CREATE IF NOT EXISTS ---
                # Some old Railway databases already have a trades table without close_time.
                # CREATE TABLE IF NOT EXISTS does not add missing columns, so we add them safely here.
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS open_time TIMESTAMP")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_time TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(20)")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS qty FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl_pct FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS duration_min INT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS rr_actual FLOAT")

                cur.execute("ALTER TABLE active_states ADD COLUMN IF NOT EXISTS state_json TEXT")
                cur.execute("ALTER TABLE active_states ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")

                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS received_at TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS action VARCHAR(40)")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS status VARCHAR(40)")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS reason TEXT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS entry_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS sl_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS tp_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS qty FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS exit_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS pnl FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS raw_json TEXT")

                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS action VARCHAR(60)")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS details TEXT")
            conn.commit()
        log.info("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        log.error(f"❌ فشل: {e}")

def save_trade(symbol, entry, exit_price, exit_reason, qty, pnl, sl=None, tp=None, open_time=None):
    try:
        pnl_pct = round((pnl / (entry * qty)) * 100, 4) if entry and qty and entry * qty > 0 else None
        risk = entry - sl if sl and entry else None
        reward = exit_price - entry if exit_price and entry else None
        rr_actual = round(reward / risk, 3) if risk and risk > 0 and reward is not None else None
        duration_min = None
        if open_time:
            try:
                if isinstance(open_time, str):
                    open_time = datetime.fromisoformat(open_time)
                delta = datetime.utcnow() - open_time
                duration_min = int(delta.total_seconds() / 60)
            except:
                pass
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                    (open_time, symbol, entry_price, exit_price, sl_price, tp_price,
                     exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (open_time, symbol, entry, exit_price, sl, tp,
                      exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الصفقة: {e}")

def load_trades(limit=100):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM trades ORDER BY close_time DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الصفقات: {e}")
        return []

def save_state(symbol, state):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO active_states (symbol, state_json, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (symbol) DO UPDATE
                    SET state_json = EXCLUDED.state_json, updated_at = NOW()
                """, (symbol, json.dumps(state, default=str)))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الحالة: {e}")

def load_all_states():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, state_json FROM active_states")
                rows = cur.fetchall()
                return {r["symbol"]: json.loads(r["state_json"]) for r in rows}
    except Exception as e:
        log.error(f"فشل تحميل الحالات: {e}")
        return {}

def delete_state(symbol):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM active_states WHERE symbol = %s", (symbol,))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حذف الحالة: {e}")


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def safe_dt(value):
    if not value:
        return None
    if hasattr(value, "strftime"):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def fmt_num(value, digits=4, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    return f"{v:,.{digits}f}"

def fmt_money(value, digits=4, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.{digits}f}"

def pct(value, digits=2, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    return f"{v:.{digits}f}%"

def load_signal_events(limit=80):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM signal_events ORDER BY received_at DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الإشارات: {e}")
        return []

def load_action_events(limit=80):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM action_events ORDER BY created_at DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الأحداث: {e}")
        return []

def save_action_event(symbol, action, details=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO action_events (symbol, action, details) VALUES (%s, %s, %s)", (symbol, action, details))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الحدث: {e}")

def log_signal_event(symbol, action, status="", reason="", data=None):
    try:
        data = data or {}
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signal_events
                    (symbol, action, status, reason, entry_price, sl_price, tp_price,
                     qty, exit_price, pnl, raw_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    symbol, action, status, reason,
                    safe_float(data.get("entry")), safe_float(data.get("backup_sl")),
                    safe_float(data.get("tp")), safe_float(data.get("qty")),
                    safe_float(data.get("exit_price")), safe_float(data.get("pnl")),
                    json.dumps(data, ensure_ascii=False, default=str)
                ))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الإشارة: {e}")

# ====================================================================
# الحالة
# ====================================================================
states = {}
processed_signals = []

def fresh_state(symbol):
    return {
        "in_trade": False, "pending": False, "symbol": symbol,
        "entry_price": None, "backup_sl": None, "tp_price": None,
        "qty": 0.0, "order_id": None, "sl_order_id": None,
        "be_active": False, "last_action": None, "last_error": None,
        "open_time": None,
    }

def get_state(symbol):
    if symbol not in states:
        states[symbol] = fresh_state(symbol)
    return states[symbol]

def clean_symbol(raw):
    if not raw:
        return raw
    s = str(raw).upper().strip()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace(".P", "").replace("PERP", "")
    return s

def reset_symbol(symbol):
    with state_lock:
        states[symbol] = fresh_state(symbol)
        delete_state(symbol)

def log_action(symbol, action, details=""):
    s = get_state(symbol)
    s["last_action"] = action
    save_state(symbol, s)
    save_action_event(symbol, action, details)
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
# دوال Alpaca
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
        r = requests.get(f"https://data.alpaca.markets/v2/stocks/{symbol}/quotes/latest", headers=HEADERS, timeout=10)
        return float(r.json()["quote"]["ap"])
    except:
        return None

def cancel_all_orders(symbol):
    try:
        orders = alpaca_get(f"orders?status=open&symbols={symbol}")
        for o in orders:
            try:
                alpaca_delete(f"orders/{o['id']}")
            except Exception as e:
                log.error(f"[{symbol}] فشل إلغاء: {e}")
    except Exception as e:
        log.error(f"[{symbol}] فشل جلب الأوردرات: {e}")

def place_stop_loss(symbol, qty, sl_price):
    try:
        order = alpaca_post("orders", {
            "symbol": symbol, "qty": str(round(qty, 6)), "side": "sell",
            "type": "stop", "stop_price": str(round(sl_price, 4)), "time_in_force": "gtc"
        })
        return order["id"]
    except Exception as e:
        log.error(f"[{symbol}] فشل SL: {e}")
        get_state(symbol)["last_error"] = str(e)
        return None

def market_buy(symbol, qty):
    return alpaca_post("orders", {"symbol": symbol, "qty": str(round(qty, 6)), "side": "buy", "type": "market", "time_in_force": "day"})

def market_sell(symbol, qty):
    return alpaca_post("orders", {"symbol": symbol, "qty": str(round(qty, 6)), "side": "sell", "type": "market", "time_in_force": "day"})

def stop_buy(symbol, qty, stop_price):
    return alpaca_post("orders", {"symbol": symbol, "qty": str(round(qty, 6)), "side": "buy", "type": "stop", "stop_price": str(round(stop_price, 4)), "time_in_force": "gtc"})

def is_market_open():
    try:
        return alpaca_get("clock").get("is_open", False)
    except:
        return True

def get_position_qty(symbol):
    try:
        return float(alpaca_get(f"positions/{symbol}").get("qty", 0))
    except:
        return 0.0

# ====================================================================
# معالجات الإشارات
# ====================================================================
def handle_entry(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)
    if s["in_trade"] or s["pending"]:
        return {"status": "ignored"}
    if not is_market_open():
        return {"status": "ignored", "reason": "السوق مغلق"}
    try:
        cancel_all_orders(symbol)
        order = market_buy(symbol, qty)
        sl_id = place_stop_loss(symbol, qty, sl)
        with state_lock:
            s.update({
                "in_trade": True, "pending": False,
                "entry_price": entry, "backup_sl": sl, "tp_price": tp,
                "qty": qty, "order_id": order["id"], "sl_order_id": sl_id,
                "open_time": datetime.utcnow().isoformat(),
            })
            save_state(symbol, s)
        log_action(symbol, "ENTRY_MARKET", f"qty={qty}")
        return {"status": "ok", "method": "market"}
    except Exception as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        return {"status": "error", "message": str(e)}

def handle_pending_entry(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)
    if s["in_trade"] or s["pending"]:
        return {"status": "ignored"}
    if not is_market_open():
        return {"status": "ignored", "reason": "السوق مغلق"}
    try:
        current = get_current_price(symbol)
        if current and (current - entry) / entry * 100 > 0.5:
            order = market_buy(symbol, qty)
            sl_id = place_stop_loss(symbol, qty, sl)
            with state_lock:
                s.update({
                    "in_trade": True, "pending": False,
                    "entry_price": current, "backup_sl": sl, "tp_price": tp,
                    "qty": qty, "order_id": order["id"], "sl_order_id": sl_id,
                    "open_time": datetime.utcnow().isoformat(),
                })
            log_action(symbol, "ENTRY_MARKET_FALLBACK", f"qty={qty}")
            return {"status": "ok", "method": "market"}
        order = stop_buy(symbol, qty, entry)
        with state_lock:
            s.update({
                "pending": True,
                "entry_price": entry, "backup_sl": sl, "tp_price": tp,
                "qty": qty, "order_id": order["id"],
                "open_time": datetime.utcnow().isoformat(),
            })
        log_action(symbol, "PENDING_ENTRY", f"entry={entry}")
        return {"status": "ok"}
    except Exception as e:
        s["last_error"] = str(e)
        return {"status": "error", "message": str(e)}

def handle_entry_filled(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)
    try:
        cancel_all_orders(symbol)
        order = market_buy(symbol, qty)
        sl_id = place_stop_loss(symbol, qty, sl)
        with state_lock:
            s.update({
                "in_trade": True, "pending": False,
                "entry_price": entry, "backup_sl": sl, "tp_price": tp,
                "qty": qty, "order_id": order["id"], "sl_order_id": sl_id,
                "open_time": datetime.utcnow().isoformat(),
            })
        log_action(symbol, "ENTRY_FILLED", f"entry={entry} sl_prev={sl}")
        return {"status": "ok"}
    except Exception as e:
        s["last_error"] = str(e)
        return {"status": "error", "message": str(e)}

def handle_exit(data):
    symbol     = data.get("symbol")
    reason     = data.get("exit_reason", "?")
    exit_price = float(data.get("exit_price", 0))
    pnl        = float(data.get("pnl", 0))
    s = get_state(symbol)
    if not s["in_trade"] and not s["pending"]:
        return {"status": "ignored"}
    try:
        cancel_all_orders(symbol)
        qty = s["qty"]
        actual_qty = get_position_qty(symbol)
        if actual_qty > 0:
            qty = actual_qty
        if qty <= 0:
            reset_symbol(symbol)
            return {"status": "warning", "reason": "لا يوجد رصيد"}
        sell = market_sell(symbol, qty)
        save_trade(symbol, s.get("entry_price"), exit_price, reason, qty, pnl,
                   sl=s.get("backup_sl"), tp=s.get("tp_price"),
                   open_time=s.get("open_time"))
        reset_symbol(symbol)
        log_action(symbol, "EXIT", f"reason={reason}")
        return {"status": "ok"}
    except Exception as e:
        s["last_error"] = str(e)
        return {"status": "error", "message": str(e)}

def handle_update_backup_sl(data):
    symbol = data.get("symbol")
    new_sl = float(data["backup_sl"])
    s = get_state(symbol)
    if not s["in_trade"] and not s["pending"]:
        return {"status": "ignored"}
    if s["pending"]:
        with state_lock:
            s["backup_sl"] = new_sl
            save_state(symbol, s)
        log_action(symbol, "UPDATE_BACKUP_SL_PENDING", f"SL={new_sl}")
        return {"status": "ok"}
    try:
        cancel_all_orders(symbol)
        sl_id = place_stop_loss(symbol, s["qty"], new_sl)
        with state_lock:
            s["backup_sl"] = new_sl
            s["be_active"] = True
            s["sl_order_id"] = sl_id
            save_state(symbol, s)
        log_action(symbol, "UPDATE_BACKUP_SL", f"SL={new_sl}")
        return {"status": "ok"}
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
            for symbol in [s for s, st in list(states.items()) if st.get("pending")]:
                s = get_state(symbol)
                if not s.get("pending") or not s.get("order_id"):
                    continue
                try:
                    order = alpaca_get(f"orders/{s['order_id']}")
                    status = order.get("status", "")
                    if status == "filled":
                        actual_qty = float(order.get("filled_qty", s["qty"]))
                        sl_id = place_stop_loss(symbol, actual_qty, s["backup_sl"])
                        with state_lock:
                            s.update({
                                "in_trade": True, "pending": False,
                                "qty": actual_qty, "sl_order_id": sl_id,
                                "open_time": datetime.utcnow().isoformat(),
                            })
                            save_state(symbol, s)
                        log_action(symbol, "PENDING_FILLED", f"qty={actual_qty}")
                    elif status in ("canceled", "expired", "rejected"):
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
                except:
                    continue
        if not data:
            return jsonify({"error": "JSON خاطئ"}), 400
        if not data.get("symbol"):
            return jsonify({"error": "لا يوجد رمز"}), 400
        data["symbol"] = clean_symbol(data["symbol"])
        action = data.get("action", "").upper()
        if is_duplicate(data):
            log_signal_event(data.get("symbol"), action, "duplicate", "duplicate ignored", data)
            return jsonify({"status": "مكرر"}), 200
        log.info(f"وصل: {json.dumps(data)}")
        if action == "ENTRY":
            # Latest strategy: ENTRY means immediate entry in Fast Green Close mode.
            # If a real pending order already exists, do not double-buy.
            r = handle_entry(data)
        elif action in ("PLACE_BUY_STOP", "PENDING_ENTRY", "BUY_STOP"):
            # Latest strategy confirmed mode sends PLACE_BUY_STOP.
            r = handle_pending_entry(data)
        elif action == "ENTRY_FILLED":
            r = handle_entry_filled(data)
        elif action == "EXIT":
            r = handle_exit(data)
        elif action == "UPDATE_BACKUP_SL":
            r = handle_update_backup_sl(data)
        elif action == "CANCEL_PENDING":
            r = handle_cancel_pending(data)
        else:
            r = {"status": "غير معروف", "action": action}
        log_signal_event(data.get("symbol"), action, str(r.get("status", "")), str(r.get("reason") or r.get("message") or r.get("action") or ""), data)
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
# الداشبورد التحليلي الشامل
# ====================================================================
def trade_dt(t, key):
    return safe_dt(t.get(key))

def reason_ar(r):
    if r == "TP": return "✅ تيك بروفت"
    if r in ("BE", "SL_MARKET"): return "➡️ بريك ايفن"
    if r == "SL": return "❌ ستوب لوز"
    return r or "—"

def calc_trade_metrics(trades):
    total = len(trades)
    total_pnl = sum(float(t.get("pnl") or 0) for t in trades)
    wins = [t for t in trades if float(t.get("pnl") or 0) > 0]
    losses = [t for t in trades if float(t.get("pnl") or 0) < 0]
    tp = [t for t in trades if t.get("exit_reason") == "TP"]
    be = [t for t in trades if t.get("exit_reason") in ("BE", "SL_MARKET")]
    sl = [t for t in trades if t.get("exit_reason") == "SL"]
    gp = sum(float(t.get("pnl") or 0) for t in wins)
    gl = abs(sum(float(t.get("pnl") or 0) for t in losses))
    rr_values = [float(t.get("rr_actual")) for t in trades if t.get("rr_actual") is not None]
    durations = [int(t.get("duration_min")) for t in trades if t.get("duration_min") is not None]
    return {"total": total, "total_pnl": total_pnl, "wins": len(wins), "losses": len(losses), "win_rate": (len(wins)/total*100) if total else None,
            "profit_factor": (gp/gl) if gl > 0 else None, "tp": len(tp), "be": len(be), "sl": len(sl),
            "avg_rr": (sum(rr_values)/len(rr_values)) if rr_values else None, "avg_duration": (sum(durations)/len(durations)) if durations else None,
            "best": max([float(t.get("pnl") or 0) for t in trades], default=None), "worst": min([float(t.get("pnl") or 0) for t in trades], default=None),
            "gross_profit": gp, "gross_loss": gl}

def stat_card(title, value, sub="", cls=""):
    return f'''<div class="stat"><div class="stat-val {cls}">{value}</div><div class="stat-lbl">{title}</div><div class="stat-sub">{sub}</div></div>'''

def metric_row(label, value, cls=""):
    return f'''<div class="row"><span class="label">{label}</span><span class="val {cls}">{value}</span></div>'''

@app.route("/", methods=["GET"])
def dashboard():
    now = datetime.utcnow()
    trades = load_trades(500)
    signals = load_signal_events(60)
    actions = load_action_events(60)
    today = [t for t in trades if trade_dt(t, "close_time") and trade_dt(t, "close_time").date() == now.date()]
    last7 = [t for t in trades if trade_dt(t, "close_time") and trade_dt(t, "close_time") >= now - timedelta(days=7)]
    m_all = calc_trade_metrics(trades); m_today = calc_trade_metrics(today); m_7 = calc_trade_metrics(last7)
    active_symbols = [sym for sym, st in states.items() if st.get("in_trade") or st.get("pending")]
    active_count = len([sym for sym in active_symbols if states[sym].get("in_trade")]); pending_count = len([sym for sym in active_symbols if states[sym].get("pending")])
    error_count = len([sym for sym, st in states.items() if st.get("last_error")])
    pf_value = "∞" if m_all["profit_factor"] is None and m_all["gross_profit"] > 0 else fmt_num(m_all["profit_factor"], 2)
    top_stats = "".join([
        stat_card("إجمالي P&L", fmt_money(m_all["total_pnl"]), "كل الصفقات", "green" if m_all["total_pnl"] >= 0 else "red"),
        stat_card("صفقات مغلقة", str(m_all["total"]), f"اليوم {m_today['total']} · 7 أيام {m_7['total']}"),
        stat_card("Win Rate", pct(m_all["win_rate"], 1), f"ربح {m_all['wins']} / خسارة {m_all['losses']}"),
        stat_card("Profit Factor", pf_value, "Gross Profit ÷ Gross Loss"),
        stat_card("TP / BE / SL", f"{m_all['tp']} / {m_all['be']} / {m_all['sl']}", "توزيع الخروج"),
        stat_card("Avg R", fmt_num(m_all["avg_rr"], 2), "متوسط R الفعلي"),
        stat_card("Today P&L", fmt_money(m_today["total_pnl"]), f"صفقات اليوم {m_today['total']}", "green" if m_today["total_pnl"] >= 0 else "red"),
        stat_card("7D P&L", fmt_money(m_7["total_pnl"]), f"صفقات 7 أيام {m_7['total']}", "green" if m_7["total_pnl"] >= 0 else "red"),
        stat_card("Active / Pending", f"{active_count} / {pending_count}", "مفتوحة / انتظار", "yellow" if active_count or pending_count else ""),
        stat_card("Errors", str(error_count), "رموز لديها آخر خطأ", "red" if error_count else "green"),
        stat_card("Best / Worst", f"{fmt_money(m_all['best'])} / {fmt_money(m_all['worst'])}", "أفضل وأسوأ صفقة"),
        stat_card("Avg Duration", (f"{m_all['avg_duration']:.0f} دقيقة" if m_all["avg_duration"] is not None else "—"), "متوسط مدة الصفقة"),
    ])
    if not active_symbols:
        active_html = '<div class="empty">لا توجد صفقات نشطة الآن</div>'
    else:
        cards=[]
        for sym in active_symbols:
            st=states[sym]; entry=safe_float(st.get("entry_price")); sl=safe_float(st.get("backup_sl")); tp=safe_float(st.get("tp_price")); qty=safe_float(st.get("qty"),0.0); current=get_current_price(sym)
            risk_unit=entry-sl if entry is not None and sl is not None else None; reward_unit=tp-entry if tp is not None and entry is not None else None
            risk_cash=risk_unit*qty if risk_unit is not None and qty else None; reward_cash=reward_unit*qty if reward_unit is not None and qty else None
            planned_rr=reward_unit/risk_unit if risk_unit and risk_unit>0 and reward_unit is not None else None; live_pnl=(current-entry)*qty if current is not None and entry is not None and qty else None
            live_r=live_pnl/risk_cash if live_pnl is not None and risk_cash and risk_cash>0 else None
            dist_sl_pct=((current-sl)/current*100) if current and sl else None; dist_tp_pct=((tp-current)/current*100) if current and tp else None
            open_dt=safe_dt(st.get("open_time")); age=int((now-open_dt).total_seconds()/60) if open_dt else None
            status_txt="🟢 صفقة مفتوحة" if st.get("in_trade") else "🟡 أمر معلّق"; status_cls="green" if st.get("in_trade") else "yellow"
            cards.append(f'''<div class="position-card"><div class="position-head"><b>{escape(sym)}</b><span class="pill {status_cls}">{status_txt}</span></div>
            {metric_row('السعر الحالي', fmt_num(current,8))}{metric_row('الدخول', fmt_num(entry,8))}{metric_row('SL', fmt_num(sl,8),'red')}{metric_row('TP', fmt_num(tp,8),'green')}{metric_row('الكمية', fmt_num(qty,8))}
            {metric_row('Risk $ / Reward $', f"{fmt_money(risk_cash)} / {fmt_money(reward_cash)}")}{metric_row('Planned RR', fmt_num(planned_rr,2))}{metric_row('Live P&L / R', f"{fmt_money(live_pnl)} / {fmt_num(live_r,2)}R", 'green' if (live_pnl or 0)>=0 else 'red')}
            {metric_row('بعده عن SL / TP', f"{pct(dist_sl_pct)} / {pct(dist_tp_pct)}")}{metric_row('العمر', f"{age} دقيقة" if age is not None else '—')}{metric_row('آخر إجراء', escape(str(st.get('last_action') or '—')))}{metric_row('آخر خطأ', escape(str(st.get('last_error') or '—')), 'red')}</div>''')
        active_html='<div class="positions-grid">'+''.join(cards)+'</div>'
    by_symbol={}
    for t in trades: by_symbol.setdefault(t.get("symbol") or "?",[]).append(t)
    symbol_rows=""
    for sym, rows_t in sorted(by_symbol.items(), key=lambda kv: calc_trade_metrics(kv[1])["total_pnl"], reverse=True):
        m=calc_trade_metrics(rows_t); cls="green" if m["total_pnl"]>=0 else "red"
        symbol_rows += f'''<tr><td><b>{escape(str(sym))}</b></td><td>{m['total']}</td><td class="{cls}">{fmt_money(m['total_pnl'])}</td><td>{pct(m['win_rate'],1)}</td><td>{fmt_num(m['profit_factor'],2) if m['profit_factor'] is not None else '∞'}</td><td>{m['tp']} / {m['be']} / {m['sl']}</td><td>{fmt_num(m['avg_rr'],2)}</td></tr>'''
    if not symbol_rows: symbol_rows='<tr><td colspan="7" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'
    trade_rows=""
    for t in trades[:80]:
        pnl_val=safe_float(t.get("pnl"),0.0); cls="green" if pnl_val>=0 else "red"; ct=trade_dt(t,"close_time"); ct_str=ct.strftime("%m-%d %H:%M") if ct else "—"
        trade_rows += f'''<tr><td>{ct_str}</td><td><b>{escape(str(t.get('symbol') or '—'))}</b></td><td>{reason_ar(t.get('exit_reason'))}</td><td>{fmt_num(t.get('entry_price'),8)}</td><td>{fmt_num(t.get('exit_price'),8)}</td><td class="red">{fmt_num(t.get('sl_price'),8)}</td><td class="green">{fmt_num(t.get('tp_price'),8)}</td><td>{fmt_num(t.get('qty'),8)}</td><td class="{cls}">{fmt_money(pnl_val)}</td><td>{pct(t.get('pnl_pct'),2)}</td><td>{fmt_num(t.get('rr_actual'),2)}R</td><td>{t.get('duration_min') or '—'}د</td></tr>'''
    if not trade_rows: trade_rows='<tr><td colspan="12" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'
    signal_rows=""
    for s in signals:
        status=str(s.get("status") or ""); cls="green" if status in ("ok","success") else "yellow" if status in ("ignored","مكرر","duplicate") else "red" if status=="error" else ""; rt=safe_dt(s.get("received_at")); rt_str=rt.strftime("%m-%d %H:%M:%S") if rt else "—"
        signal_rows += f'''<tr><td>{rt_str}</td><td><b>{escape(str(s.get('symbol') or '—'))}</b></td><td>{escape(str(s.get('action') or '—'))}</td><td class="{cls}">{escape(status or '—')}</td><td>{fmt_num(s.get('entry_price'),8)}</td><td class="red">{fmt_num(s.get('sl_price'),8)}</td><td class="green">{fmt_num(s.get('tp_price'),8)}</td><td>{fmt_num(s.get('qty'),8)}</td><td>{escape(str(s.get('reason') or '—'))}</td></tr>'''
    if not signal_rows: signal_rows='<tr><td colspan="9" class="empty">لا توجد إشارات محفوظة بعد</td></tr>'
    action_rows=""
    for a in actions[:40]:
        at=safe_dt(a.get("created_at")); at_str=at.strftime("%m-%d %H:%M:%S") if at else "—"; action_rows += f'''<tr><td>{at_str}</td><td><b>{escape(str(a.get('symbol') or '—'))}</b></td><td>{escape(str(a.get('action') or '—'))}</td><td>{escape(str(a.get('details') or '—'))}</td></tr>'''
    if not action_rows: action_rows='<tr><td colspan="4" class="empty">لا توجد أحداث بعد</td></tr>'
    html_page=f'''<!DOCTYPE html><html dir="rtl" lang="ar"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="10"><title>Alpaca Bot v6 Analytics</title><style>
*{{box-sizing:border-box;margin:0;padding:0}}body{{background:#080910;color:#e6e6ee;font-family:Arial,Tahoma,sans-serif;padding:16px;font-size:12px}}h1{{color:#00ff88;font-size:20px;margin-bottom:6px}}h2{{color:#8d8dff;font-size:14px;margin-bottom:12px}}.sub{{color:#8b8b99;font-size:11px;margin-bottom:16px;line-height:1.8}}.card,.position-card{{background:#12131d;border:1px solid #24263a;border-radius:10px;padding:14px;margin-bottom:14px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(135px,1fr));gap:10px;margin-bottom:14px}}.stat{{background:#0d0e17;border:1px solid #24263a;border-radius:9px;padding:12px;text-align:center;min-height:78px}}.stat-val{{font-size:18px;font-weight:800;color:#fff;margin-bottom:4px}}.stat-lbl{{font-size:11px;color:#aaa}}.stat-sub{{font-size:10px;color:#666;margin-top:4px}}.positions-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}}.position-head{{display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;font-size:15px}}.pill{{border-radius:20px;padding:4px 8px;background:#1b1d2c;font-size:11px}}.row{{display:flex;justify-content:space-between;gap:10px;padding:6px 0;border-bottom:1px solid #202235}}.row:last-child{{border-bottom:none}}.label{{color:#9a9aaa}}.val{{font-weight:bold;color:#fff;text-align:left;direction:ltr}}.green{{color:#00ff88!important}}.red{{color:#ff4d6d!important}}.yellow{{color:#ffd166!important}}.table-wrap{{overflow-x:auto;border-radius:8px;border:1px solid #24263a}}table{{width:100%;border-collapse:collapse;font-size:11px;min-width:850px}}th{{background:#1a1c2b;color:#9ea0ff;padding:8px;text-align:right;white-space:nowrap;position:sticky;top:0}}td{{padding:7px 8px;border-bottom:1px solid #202235;white-space:nowrap}}tr:hover td{{background:#181a28}}.empty{{color:#777;text-align:center;padding:18px}}.footer{{color:#555;font-size:10px;margin-top:14px;text-align:center}}.section-title{{display:flex;justify-content:space-between;align-items:center;margin-top:4px}}.hint{{color:#777;font-size:11px}}
</style></head><body><h1>⚡ ALPACA BOT v6 · Analytics Dashboard</h1><div class="sub">🧪 Paper/LIVE حسب إعداد ALPACA_BASE_URL · Railway/GitHub · Stocks · تحديث تلقائي كل 10 ثواني</div><div class="grid">{top_stats}</div><div class="section-title"><h2>الصفقات النشطة الآن</h2><span class="hint">السعر الحالي والربح الحي والمسافة من SL/TP</span></div>{active_html}<div class="card"><div class="section-title"><h2>تحليل الأداء حسب السهم</h2><span class="hint">يعرفك أقوى الرموز</span></div><div class="table-wrap"><table><tr><th>السهم</th><th>Trades</th><th>Net P&L</th><th>Win Rate</th><th>PF</th><th>TP/BE/SL</th><th>Avg R</th></tr>{symbol_rows}</table></div></div><div class="card"><div class="section-title"><h2>سجل الصفقات التفصيلي</h2><span class="hint">آخر 80 صفقة</span></div><div class="table-wrap"><table><tr><th>وقت الخروج</th><th>سهم</th><th>النتيجة</th><th>دخول</th><th>خروج</th><th>SL</th><th>TP</th><th>Qty</th><th>P&L</th><th>%</th><th>R</th><th>مدة</th></tr>{trade_rows}</table></div></div><div class="card"><div class="section-title"><h2>سجل إشارات TradingView Webhook</h2><span class="hint">هل وصلت/تكررت/انرفضت/تنفذت</span></div><div class="table-wrap"><table><tr><th>الوقت</th><th>سهم</th><th>Action</th><th>Status</th><th>Entry</th><th>SL</th><th>TP</th><th>Qty</th><th>Reason/Error</th></tr>{signal_rows}</table></div></div><div class="card"><div class="section-title"><h2>آخر أحداث البوت</h2><span class="hint">تنفيذ، إلغاء، تحديث SL، أخطاء</span></div><div class="table-wrap"><table><tr><th>الوقت</th><th>سهم</th><th>Action</th><th>Details</th></tr>{action_rows}</table></div></div><p class="footer">V6 Analytics · لا يعرض مفاتيح API أو أسرار الويب هوك</p></body></html>'''
    return html_page

# ====================================================================
# التشغيل
# ====================================================================
_initialized = False

def startup():
    global _initialized
    if _initialized:
        return
    _initialized = True

    # Important for Railway/Gunicorn:
    # __main__ does not run when Railway starts the app with: gunicorn main:app
    # So DB tables and the monitor thread must be initialized on import / first request.
    init_db()
    recovered = load_all_states()
    if recovered:
        states.update(recovered)
        log.info(f"✅ استعادة {len(recovered)} حالة")

    monitor_thread = threading.Thread(target=monitor_orders, daemon=True)
    monitor_thread.start()
    log.info("✅ Alpaca bot startup complete")

@app.before_request
def before_request_startup():
    startup()

# Start once when imported by Gunicorn on Railway.
try:
    startup()
except Exception as e:
    # Keep the web process alive so the dashboard can still show errors/logs.
    log.error(f"Startup error: {e}", exc_info=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
