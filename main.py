"""
Alpaca Stocks Bot - V15 FINAL PENDING ONLY v13 FULL PROTECTION + SIMPLE DASHBOARD Railway/GitHub
+ قاعدة بيانات PostgreSQL كاملة
+ Fast Webhook ACK: يرد فوراً ثم ينفذ الإشارة في الخلفية
"""

import os
import logging
import threading
import queue
import time as time_module
import json
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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

# ====================================================================
# NET PROFIT / COST FILTER
# ====================================================================
# Blocks trades where the expected net TP is too weak compared to expected net SL.
# Defaults are conservative for small stock/ETF Renko boxes.
def env_float(name, default):
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)

NET_FILTER_ENABLED = os.environ.get("NET_FILTER_ENABLED", "true").lower() == "true"
MIN_RISK_PCT       = env_float("MIN_RISK_PCT", 0.10)       # percent of entry price
MIN_NET_RR         = env_float("MIN_NET_RR", 1.20)         # net profit / net loss minimum
FEE_RATE_PER_SIDE  = env_float("FEE_RATE_PER_SIDE", 0.0)   # decimal, Alpaca stock commission often 0
SLIPPAGE_PCT_RT    = env_float("SLIPPAGE_PCT_RT", 0.05)    # percent round trip, e.g. 0.05 = 0.05%
FIXED_COST_RT      = env_float("FIXED_COST_RT", 0.0)       # fixed USD round trip

# ====================================================================
# BROKER-SIDE PROTECTION
# ====================================================================
# BRACKET means: entry + TP + SL are submitted to Alpaca together.
# If bracket/OCO cannot be created, the bot refuses the entry or closes immediately.
BROKER_PROTECTION_MODE = os.environ.get("BROKER_PROTECTION_MODE", "BRACKET").upper()
REQUIRE_BROKER_PROTECTION = os.environ.get("REQUIRE_BROKER_PROTECTION", "true").lower() == "true"


# ====================================================================
# BACKTEST MIRROR MODE
# ====================================================================
# هدفه: لا ننفذ الصفقة لايف إلا إذا السعر الحقيقي قريب من سعر دخول TradingView.
# هذا يمنع دخول متأخر يسبب خسائر R كبيرة مقارنة بالباك تست.
BACKTEST_MIRROR_MODE = os.environ.get("BACKTEST_MIRROR_MODE", "true").lower() == "true"
MAX_ENTRY_DEVIATION_PCT = env_float("MAX_ENTRY_DEVIATION_PCT", 0.05)  # max allowed live-vs-TV entry deviation %
REJECT_IF_PRICE_BEYOND_SL_TP = os.environ.get("REJECT_IF_PRICE_BEYOND_SL_TP", "true").lower() == "true"
MIRROR_REJECT_IF_NO_PRICE = os.environ.get("MIRROR_REJECT_IF_NO_PRICE", "true").lower() == "true"

# ====================================================================
# FINAL PENDING-ONLY ENTRY CONTROL
# ====================================================================
# Never chase entry with market orders. The bot places a real waiting stop-entry
# bracket at TradingView entry. If price already passed entry, reject the signal.
PENDING_ONLY_ENTRY = os.environ.get("PENDING_ONLY_ENTRY", "true").lower() == "true"
REJECT_IF_ENTRY_ALREADY_PASSED = os.environ.get("REJECT_IF_ENTRY_ALREADY_PASSED", "true").lower() == "true"




# ====================================================================
# TIMEZONE DISPLAY
# ====================================================================
APP_TZ_NAME = os.environ.get("TZ", "Asia/Dubai")
try:
    APP_TZ = ZoneInfo(APP_TZ_NAME)
except Exception:
    APP_TZ_NAME = "Asia/Dubai"
    APP_TZ = ZoneInfo("Asia/Dubai")
UTC_TZ = ZoneInfo("UTC")

def app_now():
    return datetime.now(APP_TZ)

def to_app_time(dt):
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC_TZ)
        return dt.astimezone(APP_TZ)
    except Exception:
        return dt

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
                        initial_sl_price FLOAT,
                        current_sl_price FLOAT,
                        tp_price FLOAT,
                        exit_reason VARCHAR(20),
                        qty FLOAT,
                        pnl FLOAT,
                        pnl_pct FLOAT,
                        duration_min INT,
                        rr_actual FLOAT,
                        trade_quality VARCHAR(30) DEFAULT 'Clean'
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
                        initial_sl_price FLOAT,
                        current_sl_price FLOAT,
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
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS initial_sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS current_sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(20)")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS qty FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl_pct FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS duration_min INT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS rr_actual FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS trade_quality VARCHAR(30) DEFAULT 'Clean'")

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

def save_trade(symbol, entry, exit_price, exit_reason, qty, pnl, sl=None, tp=None, open_time=None, current_sl=None, trade_quality="Clean"):
    """Save closed trade. sl = initial SL; current_sl = latest backup SL after BE updates."""
    try:
        pnl_pct = round((pnl / (entry * qty)) * 100, 4) if entry and qty and entry * qty > 0 else None
        initial_sl = sl
        current_sl = current_sl if current_sl is not None else sl
        risk = entry - initial_sl if initial_sl and entry else None
        reward = exit_price - entry if exit_price and entry else None
        rr_actual = round(reward / risk, 3) if risk and risk > 0 and reward is not None else None
        duration_min = None
        if open_time:
            try:
                if isinstance(open_time, str):
                    open_time = datetime.fromisoformat(open_time)
                delta = datetime.utcnow() - open_time
                duration_min = max(0, int(delta.total_seconds() / 60))
            except:
                pass
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                    (open_time, symbol, entry_price, exit_price, sl_price, initial_sl_price, current_sl_price, tp_price,
                     exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual, trade_quality)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (open_time, symbol, entry, exit_price, initial_sl, initial_sl, current_sl, tp,
                      exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual, trade_quality))
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
signal_queue = queue.Queue()

def fresh_state(symbol):
    return {
        "in_trade": False, "pending": False, "symbol": symbol,
        "entry_price": None, "backup_sl": None, "initial_sl": None, "current_sl": None, "tp_price": None,
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

def backtest_mirror_check(symbol, entry, sl, tp, mode="market"):
    """Return (ok, reason, current).
    mode="market": price must be close to TradingView entry now.
    mode="pending": allow current below entry because broker stop order waits at entry; reject only if already too far above entry.
    """
    if not BACKTEST_MIRROR_MODE:
        return True, "mirror off", None
    try:
        entry = float(entry); sl = float(sl); tp = float(tp)
        if entry <= 0:
            return False, "mirror: invalid entry", None
        current = get_current_price(symbol)
        if current is None or current <= 0:
            if MIRROR_REJECT_IF_NO_PRICE:
                return False, "mirror: no live price", current
            return True, "mirror: no price but allowed", current

        if REJECT_IF_PRICE_BEYOND_SL_TP:
            if sl and current <= float(sl):
                return False, f"mirror: live {current:.8f} already <= SL {float(sl):.8f}", current
            if tp and current >= float(tp):
                return False, f"mirror: live {current:.8f} already >= TP {float(tp):.8f}", current

        if mode == "pending" and current < entry:
            return True, f"mirror ok pending live={current:.8f} below entry={entry:.8f}", current

        dev_pct = abs(current - entry) / entry * 100.0
        if dev_pct > MAX_ENTRY_DEVIATION_PCT:
            return False, f"mirror: deviation {dev_pct:.4f}% > max {MAX_ENTRY_DEVIATION_PCT:.4f}% live={current:.8f} tv={entry:.8f}", current
        return True, f"mirror ok dev={dev_pct:.4f}% live={current:.8f} tv={entry:.8f}", current
    except Exception as e:
        return False, f"mirror error: {e}", None

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

def round_price_4(price):
    return str(round(float(price), 4))

def place_bracket_market_buy(symbol, qty, sl_price, tp_price):
    """Alpaca broker-side bracket: market buy with attached TP and SL.
    If this fails, no entry is opened, which prevents naked/unprotected positions.
    """
    payload = {
        "symbol": symbol,
        "qty": str(round(float(qty), 6)),
        "side": "buy",
        "type": "market",
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": round_price_4(tp_price)},
        "stop_loss": {"stop_price": round_price_4(sl_price)},
    }
    return alpaca_post("orders", payload)

def place_bracket_stop_buy(symbol, qty, entry_price, sl_price, tp_price):
    """Alpaca broker-side bracket for a buy-stop entry.
    Used for pending entries where Alpaca supports bracket stop orders.
    """
    payload = {
        "symbol": symbol,
        "qty": str(round(float(qty), 6)),
        "side": "buy",
        "type": "stop",
        "stop_price": round_price_4(entry_price),
        "time_in_force": "day",
        "order_class": "bracket",
        "take_profit": {"limit_price": round_price_4(tp_price)},
        "stop_loss": {"stop_price": round_price_4(sl_price)},
    }
    return alpaca_post("orders", payload)

def place_oco_exit(symbol, qty, sl_price, tp_price):
    """Alpaca broker-side OCO for an already-open long position."""
    payload = {
        "symbol": symbol,
        "qty": str(round(float(qty), 6)),
        "side": "sell",
        "type": "limit",
        "time_in_force": "gtc",
        "order_class": "oco",
        "take_profit": {"limit_price": round_price_4(tp_price)},
        "stop_loss": {"stop_price": round_price_4(sl_price)},
    }
    return alpaca_post("orders", payload)

def place_broker_exit_protection(symbol, qty, sl_price, tp_price):
    try:
        if BROKER_PROTECTION_MODE in ("BRACKET", "OCO", "BROKER", "FULL"):
            order = place_oco_exit(symbol, qty, sl_price, tp_price)
            return order.get("id") or "OCO"
    except Exception as e:
        log.error(f"[{symbol}] فشل OCO: {e}")
        get_state(symbol)["last_error"] = str(e)
        if REQUIRE_BROKER_PROTECTION:
            return None
    return place_stop_loss(symbol, qty, sl_price)

def emergency_close_unprotected(symbol, qty, reason="NO_BROKER_PROTECTION"):
    try:
        actual_qty = get_position_qty(symbol)
        sell_qty = actual_qty if actual_qty > 0 else float(qty or 0)
        if sell_qty <= 0:
            reset_symbol(symbol)
            return False
        current = get_current_price(symbol) or 0.0
        market_sell(symbol, sell_qty)
        st = get_state(symbol)
        entry = float(st.get("entry_price") or current or 0)
        pnl = (current - entry) * sell_qty if current and entry else 0.0
        save_trade(symbol, entry, current, reason, sell_qty, pnl,
                   sl=st.get("initial_sl") or st.get("backup_sl"),
                   current_sl=st.get("current_sl") or st.get("backup_sl"),
                   tp=st.get("tp_price"), open_time=st.get("open_time"),
                   trade_quality="ProtectionFail")
        reset_symbol(symbol)
        log_action(symbol, reason, f"closed unprotected qty={sell_qty}")
        return True
    except Exception as e:
        log.error(f"[{symbol}] emergency close failed: {e}")
        get_state(symbol)["last_error"] = str(e)
        save_state(symbol, get_state(symbol))
        return False

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

def net_profit_filter_check(entry, sl, tp, qty):
    """Return (ok, reason). Buy-only quality filter before sending broker orders."""
    if not NET_FILTER_ENABLED:
        return True, ""
    try:
        entry = float(entry); sl = float(sl); tp = float(tp); qty = float(qty)
        if entry <= 0 or qty <= 0:
            return False, "net_filter: entry/qty invalid"
        if sl >= entry:
            return False, "net_filter: SL above/at entry"
        if tp <= entry:
            return False, "net_filter: TP below/at entry"
        risk_pct = (entry - sl) / entry * 100.0
        if risk_pct < MIN_RISK_PCT:
            return False, f"net_filter: risk {risk_pct:.3f}% < min {MIN_RISK_PCT:.3f}%"

        entry_value = entry * qty
        tp_value = tp * qty
        sl_value = sl * qty
        fee_tp = (abs(entry_value) + abs(tp_value)) * FEE_RATE_PER_SIDE
        fee_sl = (abs(entry_value) + abs(sl_value)) * FEE_RATE_PER_SIDE
        slip_cost = abs(entry_value) * (SLIPPAGE_PCT_RT / 100.0)
        fixed_half = FIXED_COST_RT / 2.0

        gross_profit = (tp - entry) * qty
        gross_loss = (entry - sl) * qty
        expected_net_profit = gross_profit - fee_tp - slip_cost - fixed_half
        expected_net_loss = gross_loss + fee_sl + slip_cost + fixed_half
        if expected_net_profit <= 0:
            return False, f"net_filter: expected net TP <= 0 ({expected_net_profit:.6f})"
        if expected_net_loss <= 0:
            return False, "net_filter: expected net loss invalid"
        net_rr = expected_net_profit / expected_net_loss
        if net_rr < MIN_NET_RR:
            return False, f"net_filter: netRR {net_rr:.2f} < min {MIN_NET_RR:.2f}"
        return True, f"net_filter ok risk={risk_pct:.3f}% netRR={net_rr:.2f}"
    except Exception as e:
        return False, f"net_filter error: {e}"


# ====================================================================
# معالجات الإشارات
# ====================================================================
def handle_entry(data):
    # FINAL PENDING-ONLY RULE:
    # Even if TradingView sends action=ENTRY from strategy.order.alert_message,
    # we do NOT open market. We treat it as a request to place a waiting stop-entry bracket.
    return handle_pending_entry(data)

def handle_pending_entry(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)
    if s["in_trade"] or s["pending"]:
        return {"status": "ignored", "reason": "already in trade or pending"}

    ok_filter, filter_reason = net_profit_filter_check(entry, sl, tp, qty)
    if not ok_filter:
        return {"status": "rejected", "reason": filter_reason}

    ok_mirror, mirror_reason, mirror_price = backtest_mirror_check(symbol, entry, sl, tp, mode="pending")
    if not ok_mirror:
        return {"status": "rejected", "reason": mirror_reason}

    if not is_market_open():
        return {"status": "ignored", "reason": "السوق مغلق"}

    try:
        current = get_current_price(symbol)
        if current is None or current <= 0:
            return {"status": "rejected", "reason": "pending_only: no live price"}

        # Buy-stop entry is only valid when live price is still below the desired entry.
        # If price already touched/passed the entry, reject instead of chasing with market.
        if REJECT_IF_ENTRY_ALREADY_PASSED and current >= entry:
            return {"status": "rejected", "reason": f"pending_only: live {current:.8f} already >= entry {entry:.8f}; no market fallback"}

        cancel_all_orders(symbol)
        order = place_bracket_stop_buy(symbol, qty, entry, sl, tp)
        with state_lock:
            s.update({
                "pending": True, "in_trade": False,
                "entry_price": entry, "backup_sl": sl, "initial_sl": sl, "current_sl": sl, "tp_price": tp,
                "qty": qty, "order_id": order["id"], "sl_order_id": order.get("id") or "BRACKET_STOP_PENDING",
                "entry_order_type": "STOP_BRACKET_PENDING_ONLY",
                "open_time": datetime.utcnow().isoformat(),
            })
            save_state(symbol, s)
        log_action(symbol, "PENDING_ONLY_BRACKET_STOP", f"entry={entry} live={current}")
        return {"status": "ok", "method": "pending_only_bracket_stop", "order_id": order.get("id")}
    except Exception as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        return {"status": "error", "message": str(e)}

def handle_entry_filled(data):
    # TradingView order-fill alerts are not Alpaca fills. Never use them to market buy.
    # If there is already pending/in_trade, it will be ignored by handle_pending_entry().
    return handle_pending_entry(data)


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
                   sl=s.get("initial_sl") or s.get("backup_sl"),
                   current_sl=s.get("current_sl") or s.get("backup_sl"),
                   tp=s.get("tp_price"), open_time=s.get("open_time"))
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
            s["current_sl"] = new_sl
            s["initial_sl"] = new_sl
            save_state(symbol, s)
        log_action(symbol, "UPDATE_BACKUP_SL_PENDING", f"SL={new_sl}")
        return {"status": "ok"}
    try:
        cancel_all_orders(symbol)
        sl_id = place_broker_exit_protection(symbol, s["qty"], new_sl, s.get("tp_price"))
        if not sl_id:
            emergency_close_unprotected(symbol, s["qty"], "NO_BROKER_PROTECTION_AFTER_SL_UPDATE")
            return {"status": "error", "reason": "broker protection failed after SL update - closed"}
        with state_lock:
            s["backup_sl"] = new_sl
            s["current_sl"] = new_sl
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


def force_market_exit(symbol, reason, exit_price=None):
    """Independent protection guard.
    Closes an open long position at market if price crosses Current SL or TP,
    even if TradingView did not send EXIT or broker-side stop failed.
    """
    s = get_state(symbol)
    if not s.get("in_trade"):
        return {"status": "ignored"}
    try:
        current = exit_price if exit_price is not None else get_current_price(symbol)
        if current is None:
            return {"status": "ignored", "reason": "no_price"}
        actual_qty = get_position_qty(symbol)
        qty = actual_qty if actual_qty > 0 else float(s.get("qty") or 0)
        if qty <= 0:
            reset_symbol(symbol)
            log_action(symbol, "BROKER_FLAT_RESET", "no position qty during guard")
            return {"status": "warning", "reason": "no_qty"}
        cancel_all_orders(symbol)
        market_sell(symbol, qty)
        entry = float(s.get("entry_price") or current)
        pnl = (current - entry) * qty
        save_trade(symbol, entry, current, reason, qty, pnl,
                   sl=s.get("initial_sl") or s.get("backup_sl"),
                   current_sl=s.get("current_sl") or s.get("backup_sl"),
                   tp=s.get("tp_price"), open_time=s.get("open_time"),
                   trade_quality="Guard")
        reset_symbol(symbol)
        log_action(symbol, "FORCE_EXIT_" + reason, f"price={current} qty={qty}")
        return {"status": "ok", "reason": reason, "price": current}
    except Exception as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        log.error(f"[{symbol}] force exit failed: {e}")
        return {"status": "error", "message": str(e)}

def protection_guard_once():
    """Checks all active trades independently from TradingView alerts."""
    for symbol, st in list(states.items()):
        if not st.get("in_trade"):
            continue
        try:
            current = get_current_price(symbol)
            if current is None:
                continue
            sl = st.get("current_sl") or st.get("backup_sl")
            tp = st.get("tp_price")
            if sl is not None and current <= float(sl):
                force_market_exit(symbol, "GUARD_SL", current)
            elif tp is not None and current >= float(tp):
                force_market_exit(symbol, "GUARD_TP", current)
        except Exception as e:
            log.error(f"[{symbol}] protection guard error: {e}")

# ====================================================================
# مراقب الأوردرات
# ====================================================================
def monitor_orders():
    while True:
        try:
            time_module.sleep(5)

            # 1) Pending order monitor
            for symbol in [s for s, st in list(states.items()) if st.get("pending")]:
                s = get_state(symbol)
                if not s.get("pending") or not s.get("order_id"):
                    continue
                try:
                    order = alpaca_get(f"orders/{s['order_id']}")
                    status = order.get("status", "")
                    if status == "filled":
                        actual_qty = float(order.get("filled_qty", s["qty"]))
                        sl_id = s.get("sl_order_id") or s.get("order_id")
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
                    log.error(f"[{symbol}] pending monitor: {e}")

            # 2) Independent broker protection guard for active trades
            protection_guard_once()

        except Exception as e:
            log.error(f"monitor_orders: {e}")

# ====================================================================
# ULTRA FAST WEBHOOK QUEUE
# ====================================================================
def process_signal_task(data, action):
    try:
        log.info(f"معالجة في الخلفية: {json.dumps(data)}")
        if action == "ENTRY":
            r = handle_entry(data)
        elif action in ("PLACE_BUY_STOP", "PENDING_ENTRY", "BUY_STOP"):
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
        log_signal_event(
            data.get("symbol"), action,
            str(r.get("status", "")),
            str(r.get("reason") or r.get("message") or r.get("action") or ""),
            data
        )
    except Exception as e:
        log.error(f"فشل معالجة إشارة الخلفية: {e}", exc_info=True)
        try:
            log_signal_event(data.get("symbol"), action, "error", str(e), data)
        except Exception:
            pass

def process_raw_signal(raw):
    # Parse and process TradingView payload completely in the background.
    # /webhook returns 200 OK before JSON parsing, DB writes, duplicate checks,
    # or broker API calls. This is the fastest ACK path for TradingView.
    data = None
    try:
        raw = (raw or "").strip()
        for line in reversed(raw.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    data = json.loads(line)
                    break
                except Exception:
                    continue
        if not data:
            log_signal_event("", "BAD_JSON", "bad_json", "JSON خاطئ", {"raw": raw[:2000]})
            return
        if not data.get("symbol"):
            log_signal_event("", str(data.get("action", "")).upper(), "bad_symbol", "لا يوجد رمز", data)
            return
        data["symbol"] = clean_symbol(data["symbol"])
        action = str(data.get("action", "")).upper()
        if is_duplicate(data):
            log_signal_event(data.get("symbol"), action, "duplicate", "duplicate ignored", data)
            return
        process_signal_task(data, action)
    except Exception as e:
        log.error(f"Raw signal worker parse/process error: {e}", exc_info=True)
        try:
            log_signal_event((data or {}).get("symbol"), str((data or {}).get("action", "ERROR")).upper(), "error", str(e), data or {"raw": str(raw)[:2000]})
        except Exception:
            pass

def signal_worker():
    while True:
        try:
            raw = signal_queue.get()
            process_raw_signal(raw)
            signal_queue.task_done()
        except Exception as e:
            log.error(f"Signal worker error: {e}", exc_info=True)
            time_module.sleep(1)

# ====================================================================
# Webhook - ULTRA FAST ACK
# ====================================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    secret = request.headers.get("X-Webhook-Secret") or request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return "unauthorized", 401

    # أسرع مسار ممكن:
    # نقرأ النص، نرميه في queue، ونرجع OK فوراً.
    # لا JSON parsing، لا database، لا Binance/Alpaca، لا logging قبل الرد.
    try:
        raw = request.get_data(as_text=True, cache=False)
        signal_queue.put_nowait(raw)
        return "OK", 200, {"Content-Type": "text/plain"}
    except Exception:
        log.exception("Ultra-fast webhook enqueue failed")
        return "OK", 200, {"Content-Type": "text/plain"}

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
    mode_txt = '🧪 PAPER' if 'paper-api' in ALPACA_BASE_URL else '🔴 LIVE'
    now = app_now()
    trades = load_trades(300)
    signals = load_signal_events(20)

    today = [t for t in trades if trade_dt(t, "close_time") and to_app_time(trade_dt(t, "close_time")) and to_app_time(trade_dt(t, "close_time")).date() == now.date()]
    m_all = calc_trade_metrics(trades)
    m_today = calc_trade_metrics(today)

    active_symbols = [sym for sym, st in states.items() if st.get("in_trade") or st.get("pending")]
    active_count = len([sym for sym in active_symbols if states[sym].get("in_trade")])
    pending_count = len([sym for sym in active_symbols if states[sym].get("pending")])
    pf_value = "∞" if m_all["profit_factor"] is None and m_all["gross_profit"] > 0 else fmt_num(m_all["profit_factor"], 2)

    top_stats = "".join([
        stat_card("Net P&L", fmt_money(m_all["total_pnl"]), f"Today {fmt_money(m_today['total_pnl'])}", "green" if m_all["total_pnl"] >= 0 else "red"),
        stat_card("Trades", str(m_all["total"]), f"Today {m_today['total']}", ""),
        stat_card("Win Rate", pct(m_all["win_rate"], 1), f"W {m_all['wins']} / L {m_all['losses']}", ""),
        stat_card("PF", pf_value, "Profit factor", ""),
        stat_card("TP/BE/SL", f"{m_all['tp']}/{m_all['be']}/{m_all['sl']}", "خروج الصفقات", ""),
        stat_card("Avg R", fmt_num(m_all["avg_rr"], 2), "based on initial SL", ""),
        stat_card("Open/Pending", f"{active_count}/{pending_count}", "نشط / انتظار", "yellow" if active_count or pending_count else ""),
    ])

    active_rows = ""
    for sym in active_symbols:
        st = states[sym]
        entry = safe_float(st.get("entry_price")); initial_sl = safe_float(st.get("initial_sl") or st.get("backup_sl")); current_sl = safe_float(st.get("current_sl") or st.get("backup_sl")); tp = safe_float(st.get("tp_price")); qty = safe_float(st.get("qty"), 0.0)
        current = get_current_price(sym)
        risk_cash = (entry - initial_sl) * qty if entry is not None and initial_sl is not None and qty else None
        live_pnl = (current - entry) * qty if current is not None and entry is not None and qty else None
        live_r = live_pnl / risk_cash if live_pnl is not None and risk_cash and risk_cash > 0 else None
        status = "OPEN" if st.get("in_trade") else "PENDING"
        cls = "green" if st.get("in_trade") else "yellow"
        active_rows += f'''<tr><td><b>{escape(sym)}</b></td><td class="{cls}">{status}</td><td>{fmt_num(current,8)}</td><td>{fmt_num(entry,8)}</td><td class="red">{fmt_num(initial_sl,8)}</td><td class="yellow">{fmt_num(current_sl,8)}</td><td class="green">{fmt_num(tp,8)}</td><td>{fmt_num(qty,8)}</td><td class="{'green' if (live_pnl or 0)>=0 else 'red'}">{fmt_money(live_pnl)}</td><td>{fmt_num(live_r,2)}R</td><td>{escape(str(st.get('last_action') or '—'))}</td></tr>'''
    if not active_rows:
        active_rows = '<tr><td colspan="11" class="empty">لا توجد صفقات نشطة الآن</td></tr>'

    by_symbol = {}
    for t in trades:
        by_symbol.setdefault(t.get("symbol") or "?", []).append(t)
    symbol_rows = ""
    for sym, rows_t in sorted(by_symbol.items(), key=lambda kv: calc_trade_metrics(kv[1])["total_pnl"], reverse=True)[:10]:
        m = calc_trade_metrics(rows_t); cls = "green" if m["total_pnl"] >= 0 else "red"
        symbol_rows += f'''<tr><td><b>{escape(str(sym))}</b></td><td>{m['total']}</td><td class="{cls}">{fmt_money(m['total_pnl'])}</td><td>{pct(m['win_rate'],1)}</td><td>{fmt_num(m['profit_factor'],2) if m['profit_factor'] is not None else '∞'}</td><td>{m['tp']}/{m['be']}/{m['sl']}</td><td>{fmt_num(m['avg_rr'],2)}</td></tr>'''
    if not symbol_rows:
        symbol_rows = '<tr><td colspan="7" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'

    trade_rows = ""
    for t in trades[:25]:
        pnl_val = safe_float(t.get("pnl"), 0.0); cls = "green" if pnl_val >= 0 else "red"; ct = to_app_time(trade_dt(t, "close_time")); ct_str = ct.strftime("%m-%d %H:%M") if ct else "—"
        init_sl = t.get('initial_sl_price') if t.get('initial_sl_price') is not None else t.get('sl_price')
        cur_sl = t.get('current_sl_price') if t.get('current_sl_price') is not None else t.get('sl_price')
        trade_rows += f'''<tr><td>{ct_str}</td><td><b>{escape(str(t.get('symbol') or '—'))}</b></td><td>{reason_ar(t.get('exit_reason'))}</td><td>{fmt_num(t.get('entry_price'),8)}</td><td>{fmt_num(t.get('exit_price'),8)}</td><td class="red">{fmt_num(init_sl,8)}</td><td class="yellow">{fmt_num(cur_sl,8)}</td><td class="green">{fmt_num(t.get('tp_price'),8)}</td><td class="{cls}">{fmt_money(pnl_val)}</td><td>{fmt_num(t.get('rr_actual'),2)}R</td><td>{t.get('duration_min') or '—'}د</td></tr>'''
    if not trade_rows:
        trade_rows = '<tr><td colspan="11" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'

    signal_rows = ""
    for s in signals[:12]:
        status = str(s.get("status") or ""); cls = "green" if status in ("ok","success") else "yellow" if status in ("ignored","مكرر","duplicate") else "red" if status == "error" else ""; rt = to_app_time(safe_dt(s.get("received_at"))); rt_str = rt.strftime("%H:%M:%S") if rt else "—"
        signal_rows += f'''<tr><td>{rt_str}</td><td><b>{escape(str(s.get('symbol') or '—'))}</b></td><td>{escape(str(s.get('action') or '—'))}</td><td class="{cls}">{escape(status or '—')}</td><td>{fmt_num(s.get('entry_price'),8)}</td><td>{fmt_num(s.get('qty'),8)}</td><td>{escape(str(s.get('reason') or '—'))}</td></tr>'''
    if not signal_rows:
        signal_rows = '<tr><td colspan="7" class="empty">لا توجد إشارات بعد</td></tr>'

    html_page = f'''<!DOCTYPE html><html dir="rtl" lang="ar"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="10">
<title>Renko Bot V10 Fast</title><style>
*{{box-sizing:border-box;margin:0;padding:0}}body{{background:#080910;color:#e6e6ee;font-family:Arial,Tahoma,sans-serif;padding:14px;font-size:12px}}h1{{color:#00ff88;font-size:18px;margin-bottom:4px}}h2{{color:#9ea0ff;font-size:13px;margin:6px 0 10px}}.sub{{color:#888;font-size:11px;margin-bottom:12px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:12px}}.stat{{background:#0d0e17;border:1px solid #24263a;border-radius:8px;padding:9px;text-align:center;min-height:62px}}.stat-val{{font-size:15px;font-weight:800;color:#fff}}.stat-lbl{{font-size:10px;color:#aaa;margin-top:3px}}.stat-sub{{font-size:9px;color:#666;margin-top:3px}}.card{{background:#12131d;border:1px solid #24263a;border-radius:9px;padding:10px;margin-bottom:10px}}.table-wrap{{overflow-x:auto;border-radius:7px;border:1px solid #24263a}}table{{width:100%;border-collapse:collapse;font-size:11px;min-width:760px}}th{{background:#1a1c2b;color:#9ea0ff;padding:7px;text-align:right;white-space:nowrap}}td{{padding:6px 7px;border-bottom:1px solid #202235;white-space:nowrap}}.green{{color:#00ff88!important}}.red{{color:#ff4d6d!important}}.yellow{{color:#ffd166!important}}.empty{{color:#777;text-align:center;padding:14px}}.footer{{color:#555;text-align:center;font-size:10px;margin-top:10px}}
</style></head><body>
<h1>⚡ ALPACA BOT · Simple Dashboard</h1><div class="sub">{mode_txt} · Alpaca Stocks · تحديث كل 10 ثواني · توقيت الإمارات · مختصر للتحليل السريع</div><div class="grid">{top_stats}</div>
<div class="card"><h2>الصفقات النشطة</h2><div class="table-wrap"><table><tr><th>سهم</th><th>حالة</th><th>السعر</th><th>دخول</th><th>Initial SL</th><th>Current SL</th><th>TP</th><th>Qty</th><th>Live P&L</th><th>Live R</th><th>آخر إجراء</th></tr>{active_rows}</table></div></div>
<div class="card"><h2>الأداء حسب السهم</h2><div class="table-wrap"><table><tr><th>سهم</th><th>Trades</th><th>Net</th><th>Win%</th><th>PF</th><th>TP/BE/SL</th><th>Avg R</th></tr>{symbol_rows}</table></div></div>
<div class="card"><h2>آخر الصفقات</h2><div class="table-wrap"><table><tr><th>وقت</th><th>سهم</th><th>نتيجة</th><th>دخول</th><th>خروج</th><th>Initial SL</th><th>Current SL</th><th>TP</th><th>P&L</th><th>R</th><th>مدة</th></tr>{trade_rows}</table></div></div>
<div class="card"><h2>آخر إشارات Webhook</h2><div class="table-wrap"><table><tr><th>وقت</th><th>سهم</th><th>Action</th><th>Status</th><th>Entry</th><th>Qty</th><th>Reason</th></tr>{signal_rows}</table></div></div>
<p class="footer">V10 Fast ACK · UAE Time · R محسوب من Initial SL للصفقات الجديدة فقط</p></body></html>'''
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
    signal_thread = threading.Thread(target=signal_worker, daemon=True)
    signal_thread.start()
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
