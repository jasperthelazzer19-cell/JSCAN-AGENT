import os
import re
import time
import json
import hmac
import uuid
import hashlib
import sqlite3
import threading
import requests
import schedule
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template_string, request, jsonify
import anthropic
import sendgrid
from sendgrid.helpers.mail import Mail
import yfinance as yf

# ─── CONFIG ───────────────────────────────────────────────
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_KEY", "")
SENDGRID_KEY    = os.environ.get("SENDGRID_KEY", "")
NEWSAPI_KEY     = os.environ.get("NEWSAPI_KEY", "")
ALPACA_KEY      = os.environ.get("ALPACA_KEY", "")
ALPACA_SECRET   = os.environ.get("ALPACA_SECRET", "")
ALPACA_BASE_URL = "https://paper-api.alpaca.markets"
FROM_EMAIL      = os.environ.get("FROM_EMAIL", "")
X_API_KEY       = os.environ.get("X_API_KEY", "")
X_API_SECRET    = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN  = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_TOKEN_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET", "")
DISCORD_TOKEN   = os.environ.get("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")
ADMIN_API_KEY   = os.environ.get("ADMIN_API_KEY", "")
UNSUBSCRIBE_SECRET = os.environ.get("UNSUBSCRIBE_SECRET", "")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://jscan-agent.up.railway.app")

DB_PATH = "/app/data/agent.db"
MAX_WORKERS = 2

# Module-level singleton — thread-safe, reused across all agent calls
ANTHROPIC = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ─── TOKEN USAGE TRACKING ─────────────────────────────────
_usage_lock = threading.Lock()
_usage_totals = {"input": 0, "output": 0, "cache_create": 0, "cache_read": 0}

def reset_usage():
    with _usage_lock:
        for k in _usage_totals:
            _usage_totals[k] = 0

def record_usage(usage):
    if not usage:
        return
    with _usage_lock:
        _usage_totals["input"] += getattr(usage, "input_tokens", 0) or 0
        _usage_totals["output"] += getattr(usage, "output_tokens", 0) or 0
        _usage_totals["cache_create"] += getattr(usage, "cache_creation_input_tokens", 0) or 0
        _usage_totals["cache_read"] += getattr(usage, "cache_read_input_tokens", 0) or 0

def usage_summary():
    with _usage_lock:
        u = dict(_usage_totals)
    # Rough cost estimate. Mostly Haiku 4.5 calls + Sonnet 4.5 portfolio_manager.
    # Blended approximation: $1.50/Mtok in, $7.50/Mtok out, $3.75/Mtok cache write, $0.30/Mtok cache read
    cost = (
        u["input"] / 1_000_000 * 1.50
        + u["output"] / 1_000_000 * 7.50
        + u["cache_create"] / 1_000_000 * 3.75
        + u["cache_read"] / 1_000_000 * 0.30
    )
    return f"in={u['input']:,} out={u['output']:,} cache_w={u['cache_create']:,} cache_r={u['cache_read']:,} ~${cost:.3f}"

# ─── ANTHROPIC CALL WITH RETRY ────────────────────────────
def claude_call(model, max_tokens, messages, system=None, max_attempts=3):
    last_exc = None
    for attempt in range(max_attempts):
        try:
            kwargs = {"model": model, "max_tokens": max_tokens, "messages": messages}
            if system is not None:
                kwargs["system"] = system
            msg = ANTHROPIC.messages.create(**kwargs)
            record_usage(getattr(msg, "usage", None))
            return msg
        except Exception as e:
            last_exc = e
            if attempt < max_attempts - 1:
                sleep_s = 2 ** attempt  # 1s, 2s, 4s
                print(f"  Anthropic retry {attempt + 1}/{max_attempts} in {sleep_s}s after error: {e}")
                time.sleep(sleep_s)
    raise last_exc

# ─── MARKET HOURS ─────────────────────────────────────────
_market_clock_lock = threading.Lock()
_market_clock_cache = {"checked_at": 0.0, "open": None}

def is_market_open():
    """Check Alpaca clock; cached 60s. Returns False on error (fail closed)."""
    with _market_clock_lock:
        now = time.time()
        if _market_clock_cache["open"] is not None and now - _market_clock_cache["checked_at"] < 60:
            return _market_clock_cache["open"]
    try:
        r = requests.get(
            f"{ALPACA_BASE_URL}/v2/clock",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=5
        )
        is_open = bool(r.json().get("is_open"))
    except Exception as e:
        print(f"  Market clock check failed: {e}")
        is_open = False
    with _market_clock_lock:
        _market_clock_cache["checked_at"] = time.time()
        _market_clock_cache["open"] = is_open
    return is_open

# ─── UNSUBSCRIBE ──────────────────────────────────────────
def unsubscribe_token(email):
    secret = UNSUBSCRIBE_SECRET or "fallback-not-secure"
    return hmac.new(secret.encode(), email.encode(), hashlib.sha256).hexdigest()[:32]

def verify_unsubscribe_token(email, token):
    return hmac.compare_digest(unsubscribe_token(email), token or "")

def unsubscribe_link(email):
    if not email:
        return ""
    token = unsubscribe_token(email)
    return f"{PUBLIC_BASE_URL}/unsubscribe?email={email}&token={token}"

# ─── PREMIUM KEY ──────────────────────────────────────────
JSCAN_BASE_URL = os.environ.get("JSCAN_BASE_URL", "https://jscan-production.up.railway.app")

def ensure_premium_key(email):
    """Generate and store a premium key for a paid subscriber if they don't have one.
    Returns the key, or None if subscriber doesn't exist or isn't paid."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT premium_key, paid FROM subscribers WHERE email=? AND active=1", (email,))
    row = c.fetchone()
    if not row or not row[1]:  # not subscribed or not paid
        conn.close()
        return None
    if row[0]:
        conn.close()
        return row[0]
    new_key = uuid.uuid4().hex
    c.execute("UPDATE subscribers SET premium_key=? WHERE email=?", (new_key, email))
    conn.commit()
    conn.close()
    return new_key

def lookup_email_by_premium_key(key):
    """Return (email, paid, active) for a given premium key, or None if not found."""
    if not key or len(key) < 16:
        return None
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT email, paid, active FROM subscribers WHERE premium_key=?", (key,))
    row = c.fetchone()
    conn.close()
    return row  # (email, paid, active) or None

def portfolio_link(email):
    """Magic link to jscan.tech AI Portfolio with the user's premium key embedded."""
    key = ensure_premium_key(email)
    if not key:
        return ""
    return f"{JSCAN_BASE_URL}/?key={key}#portfolio"

# ─── EMAIL VALIDATION + RATE LIMITING ─────────────────────
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

_rate_lock = threading.Lock()
_rate_history = defaultdict(deque)

def check_rate_limit(ip, max_per_window=5, window_sec=300):
    now = time.time()
    with _rate_lock:
        dq = _rate_history[ip]
        while dq and dq[0] < now - window_sec:
            dq.popleft()
        if len(dq) >= max_per_window:
            return False
        dq.append(now)
        return True

# ─── AUTH ─────────────────────────────────────────────────
def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not ADMIN_API_KEY:
            return jsonify({"error": "ADMIN_API_KEY not configured on server"}), 500
        provided = request.headers.get("X-API-Key") or request.args.get("key")
        if provided != ADMIN_API_KEY:
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return wrapper

WATCHLIST = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK-B","JPM","V",
    "WMT","XOM","UNH","LLY","MA","JNJ","PG","HD","MRK","COST",
    "ABBV","CVX","BAC","KO","PEP","ADBE","CRM","NFLX","AMD","TMO",
    "ACN","MCD","CSCO","ABT","LIN","DHR","WFC","TXN","NEE","PM",
    "RTX","AMGN","LOW","ORCL","UPS","INTC","QCOM","CAT","NOW","INTU",
    "PLTR","SNOW","COIN","HOOD","RBLX","UBER","LYFT","ABNB","DASH","SPOT",
    "SHOP","PYPL","SOFI","AFRM","NET","DDOG","ZS","CRWD","OKTA",
    "ARM","SMCI","MU","TSM","ASML","AMAT","LRCX","KLAC","ON","MRVL",
    "DIS","WBD","CMCSA","T","VZ","TMUS","CHTR",
    "GS","MS","BLK","C","USB","PNC","TFC","SCHW","AXP","COF"
]

STOCK_NAMES = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","AMZN":"Amazon",
    "GOOGL":"Alphabet","META":"Meta","TSLA":"Tesla","BRK-B":"Berkshire Hathaway",
    "JPM":"JPMorgan","V":"Visa","WMT":"Walmart","XOM":"Exxon Mobil",
    "UNH":"UnitedHealth","LLY":"Eli Lilly","MA":"Mastercard","JNJ":"Johnson & Johnson",
    "PG":"Procter & Gamble","HD":"Home Depot","MRK":"Merck","COST":"Costco",
    "ABBV":"AbbVie","CVX":"Chevron","BAC":"Bank of America","KO":"Coca-Cola",
    "PEP":"PepsiCo","ADBE":"Adobe","CRM":"Salesforce","NFLX":"Netflix",
    "AMD":"AMD","TMO":"Thermo Fisher","ACN":"Accenture","MCD":"McDonald's",
    "CSCO":"Cisco","ABT":"Abbott","LIN":"Linde","DHR":"Danaher",
    "WFC":"Wells Fargo","TXN":"Texas Instruments","NEE":"NextEra Energy","PM":"Philip Morris",
    "RTX":"RTX Corp","AMGN":"Amgen","LOW":"Lowe's","ORCL":"Oracle",
    "UPS":"UPS","INTC":"Intel","QCOM":"Qualcomm","CAT":"Caterpillar",
    "NOW":"ServiceNow","INTU":"Intuit","PLTR":"Palantir","SNOW":"Snowflake",
    "COIN":"Coinbase","HOOD":"Robinhood","RBLX":"Roblox","UBER":"Uber",
    "LYFT":"Lyft","ABNB":"Airbnb","DASH":"DoorDash","SPOT":"Spotify",
    "SHOP":"Shopify","PYPL":"PayPal","SOFI":"SoFi",
    "AFRM":"Affirm","NET":"Cloudflare","DDOG":"Datadog","ZS":"Zscaler",
    "CRWD":"CrowdStrike","OKTA":"Okta","ARM":"ARM Holdings","SMCI":"Super Micro",
    "MU":"Micron","TSM":"TSMC","ASML":"ASML","AMAT":"Applied Materials",
    "LRCX":"Lam Research","KLAC":"KLA Corp","ON":"ON Semiconductor","MRVL":"Marvell",
    "DIS":"Disney","WBD":"Warner Bros","CMCSA":"Comcast",
    "T":"AT&T","VZ":"Verizon","TMUS":"T-Mobile","CHTR":"Charter",
    "GS":"Goldman Sachs","MS":"Morgan Stanley","BLK":"BlackRock","C":"Citigroup",
    "USB":"US Bancorp","PNC":"PNC Financial","TFC":"Truist","SCHW":"Charles Schwab",
    "AXP":"American Express","COF":"Capital One"
}

SECTOR_MAP = {
    "TECH": ["AAPL","MSFT","NVDA","GOOGL","META","AMD","INTC","QCOM","ADBE","CRM","NOW","ORCL","CSCO","SNOW","PLTR","DDOG","NET","CRWD"],
    "FINANCE": ["JPM","BAC","WFC","GS","MS","V","MA","C","USB","PNC","SCHW","BLK","AXP"],
    "HEALTH": ["UNH","LLY","JNJ","MRK","ABBV","AMGN"],
    "ENERGY": ["XOM","CVX"],
    "CONSUMER": ["AMZN","TSLA","WMT","HD","MCD","COST","LOW","DIS","NFLX"]
}

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# ─── DATABASE ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("""CREATE TABLE IF NOT EXISTS subscribers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        stocks TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        active INTEGER DEFAULT 1,
        paid INTEGER DEFAULT 0,
        premium_key TEXT
    )""")
    # Defensive migrations for older schemas
    for col_def in ["paid INTEGER DEFAULT 0", "premium_key TEXT"]:
        try:
            c.execute(f"ALTER TABLE subscribers ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass
    c.execute("""CREATE TABLE IF NOT EXISTS calls (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        date TEXT NOT NULL,
        flag TEXT NOT NULL,
        price REAL,
        thesis TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS call_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        call_id INTEGER,
        days_later INTEGER,
        price_then REAL,
        price_change_pct REAL,
        outcome TEXT,
        checked_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS weekly_budget (
        week TEXT PRIMARY KEY,
        deployed REAL DEFAULT 0,
        starting_value REAL DEFAULT 0,
        current_value REAL DEFAULT 0
    )""")
    conn.commit()
    conn.close()

# ─── DATA FETCHING ────────────────────────────────────────
def get_stock_data(symbols):
    """Batch-fetch latest OHLCV for all symbols via yfinance. Returns {sym: {...}}."""
    result = {}
    if not symbols:
        return result
    try:
        df = yf.download(
            tickers=symbols,
            period="5d",
            auto_adjust=True,
            group_by="ticker",
            threads=True,
            progress=False
        )
        if df is None or df.empty:
            print(f"  yfinance returned empty for {len(symbols)} symbols")
            return result
        print(f"  yfinance: fetched batch for {len(symbols)} symbols")

        for sym in symbols:
            try:
                sub = df[sym].dropna(how="all")
            except (KeyError, ValueError):
                continue
            if sub.empty:
                continue
            last = sub.iloc[-1]
            try:
                o = float(last["Open"])
                h = float(last["High"])
                l = float(last["Low"])
                c = float(last["Close"])
            except (KeyError, ValueError, TypeError):
                continue
            if not (o == o and h == h and l == l and c == c):  # any NaN
                continue
            v_raw = last.get("Volume", 0)
            v = int(v_raw) if v_raw == v_raw else 0
            chg = round(((c - o) / o) * 100, 2) if o else 0
            result[sym] = {
                "price": round(c, 2),
                "open": round(o, 2),
                "high": round(h, 2),
                "low": round(l, 2),
                "volume": v,
                "change_pct": chg
            }
    except Exception as e:
        print(f"Stock data error: {e}")
    return result

def get_historical_bars(symbol, days=70):
    try:
        # Yahoo uses BRK-B format directly; no conversion needed
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="3mo", auto_adjust=True)
        if hist.empty:
            return []
        # Filter NaN (c == c is False for NaN)
        return [float(c) for c in hist["Close"].tolist() if c == c]
    except Exception:
        return []

def compute_indicators(closes):
    out = {"rsi": None, "ma20": None, "ma50": None, "trend_5d": None}
    if not closes:
        return out

    if len(closes) >= 15:
        deltas = [closes[i] - closes[i - 1] for i in range(len(closes) - 14, len(closes))]
        gains = sum(d for d in deltas if d > 0) / 14
        losses = sum(-d for d in deltas if d < 0) / 14
        if losses == 0:
            out["rsi"] = 100.0
        else:
            rs = gains / losses
            out["rsi"] = round(100 - (100 / (1 + rs)), 1)

    if len(closes) >= 20:
        out["ma20"] = round(sum(closes[-20:]) / 20, 2)
    if len(closes) >= 50:
        out["ma50"] = round(sum(closes[-50:]) / 50, 2)
    if len(closes) >= 6:
        out["trend_5d"] = round(((closes[-1] - closes[-6]) / closes[-6]) * 100, 2)
    return out

def get_news(symbol, name):
    try:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": f"{name} OR {symbol} stock",
                "sortBy": "publishedAt",
                "pageSize": 5,
                "language": "en",
                "apiKey": NEWSAPI_KEY
            },
            timeout=8
        )
        articles = r.json().get("articles", [])
        return [{"title": a["title"], "source": a["source"]["name"], "published": a["publishedAt"][:10]} for a in articles if a.get("title")]
    except:
        return []

def get_alpaca_positions():
    try:
        r = requests.get(
            f"{ALPACA_BASE_URL}/v2/positions",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=8
        )
        return {p["symbol"]: p for p in r.json()}
    except:
        return {}

def get_alpaca_account():
    try:
        r = requests.get(
            f"{ALPACA_BASE_URL}/v2/account",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=8
        )
        return r.json()
    except:
        return {}

def get_alpaca_orders(limit=30, status="all"):
    """Recent orders (filled, cancelled, etc.) for the paper account."""
    try:
        r = requests.get(
            f"{ALPACA_BASE_URL}/v2/orders",
            params={"status": status, "limit": limit, "direction": "desc"},
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=8
        )
        return r.json() if r.status_code == 200 else []
    except Exception:
        return []

WEEKLY_BUDGET = int(os.environ.get("WEEKLY_BUDGET", "10000"))

def get_weekly_budget_remaining():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    week = datetime.utcnow().strftime("%Y-W%W")
    c.execute("SELECT deployed FROM weekly_budget WHERE week = ?", (week,))
    row = c.fetchone()
    conn.close()
    if not row:
        return WEEKLY_BUDGET
    return max(0, WEEKLY_BUDGET - row[0])

def record_budget_deployment(amount, portfolio_value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    week = datetime.utcnow().strftime("%Y-W%W")
    c.execute("""INSERT INTO weekly_budget (week, deployed, starting_value, current_value)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(week) DO UPDATE SET
        deployed = deployed + ?,
        current_value = ?
    """, (week, amount, portfolio_value, portfolio_value, amount, portfolio_value))
    conn.commit()
    conn.close()

def get_portfolio_history():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT week, deployed, starting_value, current_value FROM weekly_budget ORDER BY week DESC LIMIT 12")
    rows = c.fetchall()
    conn.close()
    return [{"week": r[0], "deployed": r[1], "starting": r[2], "current": r[3]} for r in rows]

def place_paper_trade(symbol, side, qty):
    try:
        url = "https://paper-api.alpaca.markets/v2/orders"
        headers = {
            "APCA-API-KEY-ID": ALPACA_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET,
            "Content-Type": "application/json"
        }
        data = {
            "symbol": symbol,
            "qty": str(qty),
            "side": side,
            "type": "market",
            "time_in_force": "day"
        }
        r = requests.post(url, json=data, headers=headers, timeout=10)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def get_previous_calls():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    c.execute("SELECT symbol, date, flag, price, thesis FROM calls WHERE date >= ? ORDER BY date DESC", (week_ago,))
    rows = c.fetchall()
    conn.close()
    return [{"symbol": r[0], "date": r[1], "flag": r[2], "price": r[3], "thesis": r[4]} for r in rows]

def save_call(symbol, flag, price, thesis):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    c.execute("INSERT INTO calls (symbol, date, flag, price, thesis) VALUES (?, ?, ?, ?, ?)",
              (symbol, today, flag, price, thesis))
    conn.commit()
    conn.close()

# ─── SELF-LEARNING ────────────────────────────────────────
def get_close_on_or_after(symbol, date_str):
    """Close on date_str, or next available trading day within 7 days."""
    try:
        start = datetime.strptime(date_str, "%Y-%m-%d")
        end = start + timedelta(days=7)
        hist = yf.Ticker(symbol).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            auto_adjust=True
        )
        if hist.empty:
            return None
        return float(hist["Close"].iloc[0])
    except Exception:
        return None

def score_past_calls():
    """For each unscored call at horizon N, fetch the actual close on call_date+N
    (or next trading day) and record outcome. Only scores when outcome date is in the past."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    scored = 0
    today = datetime.utcnow().date()

    for days_later in [1, 7, 30]:
        c.execute("""
            SELECT id, symbol, flag, price, date FROM calls
            WHERE id NOT IN (SELECT call_id FROM call_results WHERE days_later = ?)
        """, (days_later,))
        rows = c.fetchall()

        for call_id, symbol, flag, price_then, call_date_str in rows:
            try:
                cd = datetime.strptime(call_date_str, "%Y-%m-%d").date()
            except (TypeError, ValueError):
                continue
            outcome_date = cd + timedelta(days=days_later)
            if outcome_date >= today:
                # Outcome window hasn't fully closed yet — wait for tomorrow's run
                continue
            price_now = get_close_on_or_after(symbol, outcome_date.strftime("%Y-%m-%d"))
            if price_now is None or not price_then:
                continue
            change_pct = round(((price_now - price_then) / price_then) * 100, 2)

            if flag == "GREEN":
                outcome = "correct" if change_pct > 0.5 else "incorrect" if change_pct < -0.5 else "neutral"
            elif flag == "RED":
                outcome = "correct" if change_pct < -0.5 else "incorrect" if change_pct > 0.5 else "neutral"
            else:
                outcome = "neutral"

            c.execute("""
                INSERT INTO call_results (call_id, days_later, price_then, price_change_pct, outcome)
                VALUES (?, ?, ?, ?, ?)
            """, (call_id, days_later, price_now, change_pct, outcome))
            scored += 1

    conn.commit()
    conn.close()
    print(f"  Scored {scored} past calls")
    return scored

def get_track_record():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    stats = {}
    for flag in ["GREEN", "RED", "YELLOW"]:
        c.execute("""
            SELECT outcome, COUNT(*), AVG(ABS(price_change_pct)) FROM call_results cr
            JOIN calls ca ON cr.call_id = ca.id
            WHERE ca.flag = ? AND cr.days_later = 1
            GROUP BY outcome
        """, (flag,))
        rows = c.fetchall()
        total = sum(r[1] for r in rows)
        if total > 0:
            correct = sum(r[1] for r in rows if r[0] == "correct")
            avg_move = round(sum(r[2] * r[1] for r in rows if r[2]) / total, 2) if rows else 0
            accuracy = round((correct / total) * 100, 1)
            stats[flag] = {"accuracy": accuracy, "total": total, "correct": correct, "avg_move": avg_move}

    sector_stats = {}
    for sector, syms in SECTOR_MAP.items():
        placeholders = ",".join("?" * len(syms))
        c.execute(f"""
            SELECT outcome, COUNT(*) FROM call_results cr
            JOIN calls ca ON cr.call_id = ca.id
            WHERE ca.symbol IN ({placeholders}) AND cr.days_later = 1 AND ca.flag = 'GREEN'
            GROUP BY outcome
        """, syms)
        rows = dict(c.fetchall())
        total = sum(rows.values())
        if total >= 3:
            correct = rows.get("correct", 0)
            sector_stats[sector] = round((correct / total) * 100, 1)

    conn.close()
    if sector_stats:
        stats["sectors"] = sector_stats
    return stats

def get_stock_history(symbol):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT ca.date, ca.flag, ca.price, cr.price_change_pct, cr.outcome
            FROM calls ca
            LEFT JOIN call_results cr ON ca.id = cr.call_id AND cr.days_later = 1
            WHERE ca.symbol = ?
            ORDER BY ca.date DESC
            LIMIT 10
        """, (symbol,))
        rows = c.fetchall()
        conn.close()
        if not rows:
            return ""
        lines = []
        for date, flag, price, change_pct, outcome in rows:
            if outcome and change_pct is not None:
                lines.append(f"  {date}: {flag} at ${price} -> {change_pct:+.2f}% ({outcome.upper()})")
            else:
                lines.append(f"  {date}: {flag} at ${price} -> pending")
        return "\nTHIS STOCK'S HISTORY:\n" + "\n".join(lines)
    except:
        return ""

def get_market_regime():
    try:
        spy = yf.Ticker("SPY").history(period="10d", auto_adjust=True)
        if spy is None or len(spy) < 5:
            return ""
        recent = float(spy["Close"].iloc[-1])
        week_ago = float(spy["Close"].iloc[-5])
        if not week_ago:
            return ""
        spy_5d_change = round(((recent - week_ago) / week_ago) * 100, 2)

        if spy_5d_change > 2:
            regime = "BULL"
            note = "Market trending strongly up. GREEN calls more likely to succeed."
        elif spy_5d_change < -2:
            regime = "BEAR"
            note = "Market trending down. Be more cautious with GREEN calls, RED more likely to succeed."
        else:
            regime = "NEUTRAL"
            note = "Market range-bound. Stick to high-conviction signals only."

        return f"\nMARKET REGIME: {regime} (SPY 5d: {spy_5d_change:+.2f}%) - {note}"
    except Exception:
        return ""

# ─── CLAUDE ANALYSIS ──────────────────────────────────────
PORTFOLIO_MANAGER_SYSTEM = """You are an elite portfolio manager and investment analyst making final BUY/SELL/HOLD decisions on US equities. Your role is to synthesize three independent specialist analyst reports into a single, actionable trading signal for each stock.

# Your Inputs
For every decision you receive:
1. NEWS AGENT REPORT: a sentiment score (-10 to +10), the key catalyst, any risk flags, and a 1-sentence summary.
2. TECHNICAL AGENT REPORT: momentum classification, volume signal, range position, RSI(14), 20-day and 50-day moving averages, MA-cross signal, and a strength score (-10 to +10).
3. SENTIMENT AGENT REPORT: market conditions (RISK_ON/NEUTRAL/RISK_OFF), the stock's relative strength vs. market, a macro score (-10 to +10).
4. CURRENT PRICE DATA: latest price, intraday change %.
5. PRIOR CALL HISTORY: your last call on this stock within 7 days (if any) and how that call has performed since.
6. CURRENT POSITION: existing paper position (qty, avg cost, unrealized P&L) if you already hold the stock.
7. TRACK RECORD: your historical accuracy by flag type (GREEN/RED/YELLOW) and by sector, plus average move size.
8. STOCK HISTORY: this specific stock's last 10 calls and their scored outcomes.
9. MARKET REGIME: SPY 5-day trend classified as BULL / NEUTRAL / BEAR.

# Decision Framework

## Flag Selection (GREEN / YELLOW / RED)

GREEN — Bullish conviction. Use when:
- At least 2 of 3 analyst reports lean positive (sentiment > +3, technical strength > +3, or relative outperformance with macro score >= 0).
- No major risk flags from news agent.
- Stock is not technically overbought (RSI < 75).
- Market regime supports the call. In a strong BEAR regime require all three reports to lean positive.

RED — Bearish conviction. Use when:
- At least 2 of 3 analyst reports lean negative.
- Risk flags present, or downward momentum confirmed by technicals.
- Stock is technically extended on the upside (RSI > 75) and showing distribution, OR breaking below key MAs.

YELLOW — Genuine uncertainty. Use ONLY when:
- Reports conflict materially (e.g., bullish news + bearish technicals with no clear resolution).
- Critical data is missing (one or more agents reported "unavailable").
- Stock is in a tight range (price between MA20 and MA50, RSI 45-55, no catalyst).
DO NOT default to YELLOW out of caution. If 2+ reports agree, commit to GREEN or RED.

## Action Selection (BUY / HOLD / SELL / WATCH)
- BUY: GREEN flag with HIGH or MEDIUM confidence and no current position. (For low-confidence GREEN with no position, prefer WATCH.)
- HOLD: GREEN flag with existing profitable position; OR YELLOW with existing position that is not deteriorating; OR existing position where thesis is intact even if today's signal is mixed.
- SELL: RED flag with existing position; OR existing position where unrealized P&L is < -5% AND the underlying thesis has deteriorated.
- WATCH: YELLOW flag with no position; OR low-conviction GREEN/RED where you want to see confirmation.

## Confidence Calibration
- HIGH: 3 of 3 reports align in direction, market regime supports the call, technicals confirm (RSI in healthy range, price relationship to MAs supports direction).
- MEDIUM: 2 of 3 reports align with the third neutral; OR strong directional signal but mixed market regime.
- LOW: Reports mixed or marginal data quality (an agent reported "unavailable"); never use HIGH confidence when one of the inputs is missing.

# Self-Awareness Rules
- If your historical GREEN accuracy is below 50%, raise the bar: only GREEN with 3-of-3 agreement.
- If you are in a sector where your accuracy is below 45%, downgrade confidence by one level.
- If you called this stock GREEN within the past 7 days and it has dropped, do NOT mechanically re-issue GREEN. Reassess the thesis honestly. If the original thesis is broken, switch to YELLOW or RED.
- If market regime is BEAR, raise the bar for new GREEN calls.
- If market regime is BULL, raise the bar for new RED calls.
- If you already hold a profitable position, prefer HOLD over re-issuing BUY (no stacking).

# Output Format

Respond in this EXACT format with no preamble, no markdown, no extra commentary:

FLAG: [GREEN / YELLOW / RED]
BULL CASE: [1-2 sentences. The strongest specific case for upside, citing concrete data points.]
BEAR CASE: [1-2 sentences. The strongest specific case for downside, citing concrete data points.]
VERDICT: [1-2 sentences. Your final synthesis. Be specific about timeframe (next 1-7 days vs 30+ days). Reference the dominant signal.]
ACTION: [BUY / HOLD / SELL / WATCH]
CONFIDENCE: [HIGH / MEDIUM / LOW]

# Style Guidelines
- Be direct. Avoid hedging language ("could potentially", "might possibly", "it is unclear whether").
- Reference specific numbers: prices, percentages, RSI levels, MA crossovers, news catalysts.
- Distinguish short-term (1-7 days) from longer-term (30+ days). Your scoring window is 1, 7, and 30 days, so calibrate to those horizons.
- Never recommend BUY without an identifiable catalyst or technical setup.
- Never recommend SELL purely on a single down day. SELL requires a deteriorating thesis, not just a dip.
- Never invent data the analysts did not provide. If a value is N/A, treat it as missing, not as zero."""


def news_agent(symbol, name, news):
    news_text = "\n".join([f"- {n['title']} ({n['source']}, {n['published']})" for n in news]) or "No recent news."
    prompt = f"""You are a financial news analyst. Analyze recent news for {name} ({symbol}).

NEWS:
{news_text}

Respond in this EXACT format:
SENTIMENT_SCORE: [number from -10 to +10, where -10 is extremely bearish, 0 is neutral, +10 is extremely bullish]
KEY_CATALYST: [single most important news item, or "None" if no significant news]
RISK_FLAG: [any major risks mentioned in news, or "None"]
SUMMARY: [1 sentence summary of news sentiment]"""

    msg = claude_call(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def technical_agent(symbol, name, price_data, indicators=None):
    indicators = indicators or {}
    price = price_data.get("price", 0)
    open_p = price_data.get("open", 0)
    high = price_data.get("high", 0)
    low = price_data.get("low", 0)
    volume = price_data.get("volume", 0)
    change_pct = price_data.get("change_pct", 0)

    day_range = high - low if high and low else 0
    range_position = ((price - low) / day_range * 100) if day_range > 0 else 50
    body_size = abs(price - open_p) / open_p * 100 if open_p else 0

    rsi = indicators.get("rsi")
    ma20 = indicators.get("ma20")
    ma50 = indicators.get("ma50")
    trend_5d = indicators.get("trend_5d")

    rsi_str = f"{rsi}" if rsi is not None else "N/A"
    rsi_note = ""
    if rsi is not None:
        if rsi >= 70: rsi_note = " (overbought)"
        elif rsi <= 30: rsi_note = " (oversold)"
    ma20_str = f"${ma20}" if ma20 is not None else "N/A"
    ma50_str = f"${ma50}" if ma50 is not None else "N/A"
    trend_str = f"{trend_5d:+.2f}%" if trend_5d is not None else "N/A"

    if ma20 is not None and ma50 is not None and price:
        if price > ma20 > ma50:
            ma_signal = "Price above both MAs; MA20 > MA50 (bullish stack)"
        elif price < ma20 < ma50:
            ma_signal = "Price below both MAs; MA20 < MA50 (bearish stack)"
        elif ma20 > ma50:
            ma_signal = "MA20 > MA50 but price between/below; trend intact but momentum weakening"
        else:
            ma_signal = "MA20 < MA50 (bearish cross); rallies likely to be sold"
    else:
        ma_signal = "N/A (insufficient history)"

    prompt = f"""You are a technical analyst. Analyze the price action for {name} ({symbol}).

INTRADAY:
- Current: ${price} | Change: {change_pct}%
- Open: ${open_p} | High: ${high} | Low: ${low}
- Day Range: ${day_range:.2f} | Position in range: {range_position:.0f}%
- Volume: {volume:,}
- Candle body size: {body_size:.2f}%

TREND INDICATORS:
- 5-day price trend: {trend_str}
- 20-day moving average: {ma20_str}
- 50-day moving average: {ma50_str}
- MA signal: {ma_signal}
- RSI(14): {rsi_str}{rsi_note}

Respond in this EXACT format:
MOMENTUM: [STRONG_UP / UP / NEUTRAL / DOWN / STRONG_DOWN]
VOLUME_SIGNAL: [HIGH / NORMAL / LOW]
RANGE_POSITION: [TOP_THIRD / MIDDLE / BOTTOM_THIRD]
TREND: [BULLISH / NEUTRAL / BEARISH]
RSI_SIGNAL: [OVERBOUGHT / NEUTRAL / OVERSOLD]
STRENGTH_SCORE: [number from -10 to +10]
SUMMARY: [1 sentence technical assessment that incorporates the MA and RSI context]"""

    msg = claude_call(
        model="claude-haiku-4-5-20251001",
        max_tokens=250,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def sentiment_agent(symbol, name, price_data, all_prices):
    changes = [v.get("change_pct", 0) for v in all_prices.values() if v.get("change_pct") is not None]
    market_avg = round(sum(changes) / len(changes), 2) if changes else 0
    stock_change = price_data.get("change_pct", 0)
    vs_market = round(stock_change - market_avg, 2)

    prompt = f"""You are a market sentiment analyst. Assess the broader context for {name} ({symbol}).

MARKET CONTEXT:
- Stock change today: {stock_change}%
- Market average change today: {market_avg}%
- vs Market: {vs_market:+.2f}% (outperforming if positive)

Respond in this EXACT format:
MARKET_CONDITIONS: [RISK_ON / NEUTRAL / RISK_OFF]
RELATIVE_STRENGTH: [OUTPERFORMING / IN_LINE / UNDERPERFORMING]
MACRO_SCORE: [number from -10 to +10]
SUMMARY: [1 sentence macro/sentiment assessment]"""

    msg = claude_call(
        model="claude-haiku-4-5-20251001",
        max_tokens=150,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def portfolio_manager(symbol, price_data, news_report, technical_report, sentiment_report, previous_calls, positions, track_record):
    name = STOCK_NAMES.get(symbol, symbol)

    prev_call_text = ""
    for pc in previous_calls:
        if pc["symbol"] == symbol:
            price_then = pc["price"]
            price_now = price_data.get("price", 0)
            if price_then and price_now:
                delta = round(((price_now - price_then) / price_then) * 100, 2)
                prev_call_text = f"\nPREVIOUS CALL ({pc['date']}): {pc['flag']} at ${price_then}. Since then: {delta:+.2f}%."

    position_text = ""
    if symbol in positions:
        p = positions[symbol]
        position_text = f"\nCURRENT POSITION: {p.get('qty')} shares, avg ${p.get('avg_entry_price')}, P&L: ${p.get('unrealized_pl')}"

    track_text = ""
    if track_record:
        parts = []
        for flag in ["GREEN", "RED", "YELLOW"]:
            stats = track_record.get(flag, {})
            if isinstance(stats, dict) and stats.get("total", 0) >= 3:
                parts.append(f"{flag}: {stats['accuracy']}% accurate ({stats['total']} calls, avg move {stats.get('avg_move', 0)}%)")
        if parts:
            track_text = "\nYOUR TRACK RECORD (1d scoring window):\n" + "\n".join(parts)

        sectors = track_record.get("sectors", {})
        if sectors:
            best = max(sectors, key=sectors.get)
            worst = min(sectors, key=sectors.get)
            if sectors[best] != sectors[worst]:
                track_text += f"\nSECTOR ACCURACY: Best {best} ({sectors[best]}%), weakest {worst} ({sectors[worst]}%)"
                for sec, syms in SECTOR_MAP.items():
                    if symbol in syms and sec in sectors:
                        acc = sectors[sec]
                        if acc < 45:
                            track_text += f"\nNOTE: {symbol} is in {sec} where accuracy is {acc}% — be more cautious."
                        elif acc > 65:
                            track_text += f"\nNOTE: {symbol} is in {sec} where accuracy is {acc}% — high-confidence sector."
                        break

    stock_history = get_stock_history(symbol)
    market_regime = track_record.get("regime", "")

    user_msg = f"""Stock: {name} ({symbol})

ANALYST REPORTS:
NEWS AGENT:
{news_report}

TECHNICAL AGENT:
{technical_report}

SENTIMENT AGENT:
{sentiment_report}

CURRENT DATA:
Price: ${price_data.get('price')} | Change: {price_data.get('change_pct')}%
{prev_call_text}
{position_text}
{track_text}
{stock_history}
{market_regime}

Make your decision now."""

    msg = claude_call(
        model="claude-sonnet-4-5",
        max_tokens=500,
        system=[
            {
                "type": "text",
                "text": PORTFOLIO_MANAGER_SYSTEM,
                "cache_control": {"type": "ephemeral"}
            }
        ],
        messages=[{"role": "user", "content": user_msg}]
    )
    return msg.content[0].text

def analyze_stock(symbol, price_data, news, previous_calls, positions, track_record=None, all_prices=None, indicators=None):
    name = STOCK_NAMES.get(symbol, symbol)
    if all_prices is None:
        all_prices = {symbol: price_data}

    try:
        news_report = news_agent(symbol, name, news)
    except Exception:
        news_report = "SENTIMENT_SCORE: 0\nKEY_CATALYST: None\nRISK_FLAG: None\nSUMMARY: News analysis unavailable."

    try:
        tech_report = technical_agent(symbol, name, price_data, indicators)
    except Exception:
        tech_report = "MOMENTUM: NEUTRAL\nVOLUME_SIGNAL: NORMAL\nRANGE_POSITION: MIDDLE\nTREND: NEUTRAL\nRSI_SIGNAL: NEUTRAL\nSTRENGTH_SCORE: 0\nSUMMARY: Technical analysis unavailable."

    try:
        sent_report = sentiment_agent(symbol, name, price_data, all_prices)
    except Exception:
        sent_report = "MARKET_CONDITIONS: NEUTRAL\nRELATIVE_STRENGTH: IN_LINE\nMACRO_SCORE: 0\nSUMMARY: Sentiment analysis unavailable."

    return portfolio_manager(symbol, price_data, news_report, tech_report, sent_report, previous_calls, positions, track_record or {})

def parse_analysis(text):
    lines = text.strip().split("\n")
    result = {"flag": "YELLOW", "bull": "", "bear": "", "verdict": "", "action": "WATCH", "confidence": "MEDIUM", "raw": text}
    for line in lines:
        if line.startswith("FLAG:"):
            raw = line.replace("FLAG:", "").strip().upper()
            if "GREEN" in raw: result["flag"] = "GREEN"
            elif "RED" in raw: result["flag"] = "RED"
            else: result["flag"] = "YELLOW"
        elif line.startswith("BULL CASE:"):
            result["bull"] = line.replace("BULL CASE:", "").strip()
        elif line.startswith("BEAR CASE:"):
            result["bear"] = line.replace("BEAR CASE:", "").strip()
        elif line.startswith("VERDICT:"):
            result["verdict"] = line.replace("VERDICT:", "").strip()
        elif line.startswith("ACTION:"):
            result["action"] = line.replace("ACTION:", "").strip()
        elif line.startswith("CONFIDENCE:"):
            result["confidence"] = line.replace("CONFIDENCE:", "").strip()
    return result

# ─── EMAIL BUILDER ────────────────────────────────────────
def flag_to_action(flag):
    """User-facing label for a flag. GREEN→BUY, RED→SELL, YELLOW→WATCH."""
    return {"GREEN": "BUY", "RED": "SELL", "YELLOW": "WATCH"}.get(flag, flag)

def action_badge(flag):
    """Outlined pill showing the action (BUY/SELL/WATCH) in the flag color."""
    color = {"GREEN": "#00cc66", "RED": "#ff4444", "YELLOW": "#f0c040"}.get(flag, "#888")
    label = flag_to_action(flag)
    return (
        f'<span style="display:inline-block;padding:3px 10px;'
        f'border:1px solid {color};color:{color};border-radius:4px;'
        f'font-weight:700;font-size:11px;letter-spacing:0.5px">{label}</span>'
    )

def build_email(analyses, account, email=None):
    today = datetime.utcnow().strftime("%A, %B %d, %Y")
    portfolio_val = account.get("portfolio_value", "N/A")
    cash = account.get("cash", "N/A")
    unsub = unsubscribe_link(email)
    unsub_html = f'<div style="text-align:center;color:#444;font-size:11px;margin-top:8px"><a href="{unsub}" style="color:#666;text-decoration:underline">Unsubscribe</a></div>' if unsub else ""
    portfolio_url = portfolio_link(email)
    portfolio_cta = (
        f'<div style="text-align:center;margin:24px 0 16px"><a href="{portfolio_url}" '
        f'style="display:inline-block;background:#00ff88;color:#000;font-weight:700;'
        f'padding:12px 28px;border-radius:8px;text-decoration:none;font-size:14px">'
        f'View AI Portfolio →</a><div style="color:#555;font-size:11px;margin-top:6px">'
        f'See exactly what the agent is holding right now</div></div>'
        if portfolio_url else ""
    )

    green = [a for a in analyses if a["flag"] == "GREEN"]
    yellow = [a for a in analyses if a["flag"] == "YELLOW"]
    red = [a for a in analyses if a["flag"] == "RED"]

    def flag_color(f):
        return {"GREEN": "#00cc66", "YELLOW": "#f0c040", "RED": "#ff4444"}.get(f, "#888")

    def flag_emoji(f):
        return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(f, "⚪")

    rows = ""
    for a in analyses:
        fc = flag_color(a["flag"])
        fe = flag_emoji(a["flag"])
        chg = a.get("change_pct", 0)
        chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
        chg_color = "#00cc66" if chg >= 0 else "#ff4444"
        rows += f"""
        <tr style="border-bottom:1px solid #1a1a1a">
          <td style="padding:14px 16px;font-weight:700;color:#fff;font-size:15px">{fe} {a['symbol']}</td>
          <td style="padding:14px 16px;color:#aaa;font-size:13px">{a['name']}</td>
          <td style="padding:14px 16px;color:#fff;font-weight:600">${a['price']}</td>
          <td style="padding:14px 16px;color:{chg_color};font-weight:600">{chg_str}</td>
          <td style="padding:14px 16px">{action_badge(a['flag'])}</td>
          <td style="padding:14px 16px;color:#ccc;font-size:13px">{a['verdict']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="background:#0a0a0a;color:#e0e0e0;font-family:'Helvetica Neue',Arial,sans-serif;margin:0;padding:0">
  <div style="max-width:900px;margin:0 auto;padding:32px 20px">
    <div style="border-bottom:1px solid #1c1c1c;padding-bottom:20px;margin-bottom:28px">
      <div style="font-size:24px;font-weight:800;color:#00ff88;letter-spacing:-0.5px">📊 JSCAN Daily Brief</div>
      <div style="color:#555;font-size:13px;margin-top:4px">{today}</div>
    </div>
    <div style="display:flex;gap:12px;margin-bottom:28px">
      <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px;flex:1">
        <div style="font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Paper Portfolio</div>
        <div style="font-size:22px;font-weight:700;color:#00ff88">${float(portfolio_val):,.2f}</div>
      </div>
      <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px;flex:1">
        <div style="font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Cash Available</div>
        <div style="font-size:22px;font-weight:700;color:#e0e0e0">${float(cash):,.2f}</div>
      </div>
      <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:16px 20px;flex:1">
        <div style="font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Signals Today</div>
        <div style="font-size:22px;font-weight:700">
          <span style="color:#00cc66">{len(green)}🟢</span>
          <span style="color:#f0c040;margin-left:8px">{len(yellow)}🟡</span>
          <span style="color:#ff4444;margin-left:8px">{len(red)}🔴</span>
        </div>
      </div>
    </div>
    {"<div style='background:#0d0d0d;border:1px solid #00cc6633;border-radius:10px;padding:20px;margin-bottom:20px'><div style='font-size:13px;color:#00cc66;text-transform:uppercase;letter-spacing:1px;font-weight:700;margin-bottom:14px'>🟢 Buy Signals</div>" + "".join([f"<div style='margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #1a1a1a'><span style='font-weight:700;color:#fff'>{a['symbol']}</span> <span style='color:#aaa;font-size:13px'>({a['name']})</span> — <span style='color:#ccc;font-size:13px'>{a['verdict']}</span></div>" for a in green]) + "</div>" if green else ""}
    {"<div style='background:#0d0d0d;border:1px solid #ff444433;border-radius:10px;padding:20px;margin-bottom:20px'><div style='font-size:13px;color:#ff4444;text-transform:uppercase;letter-spacing:1px;font-weight:700;margin-bottom:14px'>🔴 Sell Signals</div>" + "".join([f"<div style='margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #1a1a1a'><span style='font-weight:700;color:#fff'>{a['symbol']}</span> <span style='color:#aaa;font-size:13px'>({a['name']})</span> — <span style='color:#ccc;font-size:13px'>{a['verdict']}</span></div>" for a in red]) + "</div>" if red else ""}
    <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;overflow:hidden;margin-bottom:28px">
      <div style="padding:16px 20px;border-bottom:1px solid #1c1c1c">
        <div style="font-size:13px;color:#555;text-transform:uppercase;letter-spacing:1px;font-weight:600">Full Watchlist Analysis</div>
      </div>
      <table style="width:100%;border-collapse:collapse">
        <tr style="background:#080808">
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Symbol</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Company</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Price</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Change</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Action</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Verdict</th>
        </tr>
        {rows}
      </table>
    </div>
    {portfolio_cta}
    <div style="color:#333;font-size:12px;text-align:center;padding-top:16px;border-top:1px solid #1a1a1a">
      JSCAN AI Agent · Paper trading only · Not financial advice · Powered by Claude AI
    </div>
    {unsub_html}
  </div>
</body>
</html>"""
    return html


def build_free_email(analyses, account, email=None):
    today = datetime.utcnow().strftime("%A, %B %d, %Y")
    green = [a for a in analyses if a["flag"] == "GREEN"]
    red = [a for a in analyses if a["flag"] == "RED"]
    yellow = [a for a in analyses if a["flag"] == "YELLOW"]
    unsub = unsubscribe_link(email)
    unsub_html = f'<div style="text-align:center;color:#444;font-size:11px;margin-top:8px"><a href="{unsub}" style="color:#666;text-decoration:underline">Unsubscribe</a></div>' if unsub else ""

    def flag_color(f):
        return {"GREEN": "#00cc66", "YELLOW": "#f0c040", "RED": "#ff4444"}.get(f, "#888")

    def flag_emoji(f):
        return {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴"}.get(f, "⚪")

    rows = ""
    for a in analyses:
        fc = flag_color(a["flag"])
        fe = flag_emoji(a["flag"])
        chg = a.get("change_pct", 0)
        chg_str = f"+{chg:.2f}%" if chg >= 0 else f"{chg:.2f}%"
        chg_color = "#00cc66" if chg >= 0 else "#ff4444"
        rows += f"""
        <tr style="border-bottom:1px solid #1a1a1a">
          <td style="padding:14px 16px;font-weight:700;color:#fff;font-size:15px">{fe} {a['symbol']}</td>
          <td style="padding:14px 16px;color:#aaa;font-size:13px">{a['name']}</td>
          <td style="padding:14px 16px;color:#fff;font-weight:600">${a['price']}</td>
          <td style="padding:14px 16px;color:{chg_color};font-weight:600">{chg_str}</td>
          <td style="padding:14px 16px">{action_badge(a['flag'])}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0a0a0a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif">
  <div style="max-width:600px;margin:0 auto;padding:24px 16px">
    <div style="text-align:center;margin-bottom:28px">
      <div style="font-size:28px;font-weight:800;color:#00ff88;letter-spacing:-1px">JSCAN</div>
      <div style="color:#555;font-size:13px;margin-top:4px">Daily Brief (Free) — {today}</div>
    </div>
    <div style="background:#111;border:1px solid #1c1c1c;border-radius:12px;padding:16px;margin-bottom:20px;text-align:center">
      <div style="color:#888;font-size:12px;margin-bottom:8px">TOP SIGNALS TODAY</div>
      <div style="display:flex;justify-content:center;gap:24px">
        <div><span style="font-size:20px;font-weight:700;color:#00cc66">{len(green)}</span><span style="font-size:18px"> 🟢</span></div>
        <div><span style="font-size:20px;font-weight:700;color:#f0c040">{len(yellow)}</span><span style="font-size:18px"> 🟡</span></div>
        <div><span style="font-size:20px;font-weight:700;color:#ff4444">{len(red)}</span><span style="font-size:18px"> 🔴</span></div>
      </div>
    </div>
    <div style="background:#0d0d0d;border:1px solid #1c1c1c;border-radius:12px;overflow:hidden;margin-bottom:20px">
      <table style="width:100%;border-collapse:collapse">
        <thead>
          <tr style="background:#111;border-bottom:1px solid #1c1c1c">
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Symbol</th>
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Name</th>
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Price</th>
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Change</th>
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Action</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>
    <div style="background:#0d1a0d;border:1px solid #1a3a1a;border-radius:12px;padding:20px;text-align:center;margin-bottom:20px">
      <div style="color:#00cc66;font-weight:700;font-size:15px;margin-bottom:8px">Want full analysis + thesis for all 100 stocks?</div>
      <div style="color:#888;font-size:13px;margin-bottom:16px">Upgrade to see why each signal was called, sector breakdowns, and full AI reasoning.</div>
      <a href="https://jscan-agent.up.railway.app" style="background:#00ff88;color:#000;font-weight:700;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:14px">Upgrade — $10/month</a>
    </div>
    <div style="text-align:center;color:#333;font-size:12px">
      JSCAN AI Agent · Paper trading only · Not financial advice
    </div>
    {unsub_html}
  </div>
</body>
</html>"""
    return html

# ─── PER-STOCK WORKER ─────────────────────────────────────
def analyze_one_stock(sym, price_data_all, previous_calls, positions, track_record, budget_lock, budget_state):
    if sym not in price_data_all:
        return None
    pd = price_data_all[sym]
    name = STOCK_NAMES.get(sym, sym)

    news = get_news(sym, name)
    closes = get_historical_bars(sym)
    indicators = compute_indicators(closes)

    try:
        raw = analyze_stock(sym, pd, news, previous_calls, positions, track_record, price_data_all, indicators)
    except Exception as e:
        print(f"  Error analyzing {sym}: {e}")
        return None

    parsed = parse_analysis(raw)
    parsed["symbol"] = sym
    parsed["name"] = name
    parsed["price"] = pd["price"]
    parsed["change_pct"] = pd["change_pct"]

    try:
        save_call(sym, parsed["flag"], pd["price"], parsed["verdict"])
    except Exception as e:
        print(f"  save_call error for {sym}: {e}")

    action = parsed["action"].upper()
    if "BUY" in action and parsed["flag"] == "GREEN":
        if sym in positions:
            print(f"    Skip BUY {sym} — already hold {positions[sym].get('qty')} shares")
        elif not is_market_open():
            print(f"    Skip BUY {sym} — market closed")
        else:
            price = pd["price"]
            if price and price > 0:
                with budget_lock:
                    remaining = get_weekly_budget_remaining()
                    if remaining > 0:
                        budget_per = min(budget_state["per_position"], remaining)
                        qty = max(1, int(budget_per / price))
                        cost = round(qty * price, 2)
                        if cost <= remaining:
                            result = place_paper_trade(sym, "buy", qty)
                            if "error" not in result:
                                record_budget_deployment(cost, budget_state["portfolio_val"])
                                budget_state["total_deployed"] += cost
                            print(f"    Paper BUY {qty}x {sym} @ ${price} = ${cost}: {result.get('status', result.get('error', 'unknown'))}")
    elif "SELL" in action and sym in positions:
        if not is_market_open():
            print(f"    Skip SELL {sym} — market closed")
        else:
            qty = positions[sym].get("qty", 1)
            result = place_paper_trade(sym, "sell", qty)
            print(f"    Paper SELL {qty}x {sym}: {result.get('status', result.get('error', 'unknown'))}")

    return parsed

# ─── MAIN AGENT RUN ───────────────────────────────────────
def run_agent(symbols=None, force=False):
    if not force and datetime.utcnow().weekday() >= 5:
        print(f"[{datetime.utcnow()}] Skipping — weekend, markets closed.")
        return []
    if symbols is None:
        symbols = WATCHLIST
    print(f"[{datetime.utcnow()}] Agent running for {len(symbols)} stocks (parallel x{MAX_WORKERS})...")
    reset_usage()

    price_data = get_stock_data(symbols)
    if not price_data:
        print(f"[{datetime.utcnow()}] No price data — aborting run, no emails sent.")
        return []

    positions = get_alpaca_positions()
    account = get_alpaca_account()
    previous_calls = get_previous_calls()
    portfolio_val = float(account.get("portfolio_value", 0))

    # Stop-loss pass: sell anything down >= 5% from entry
    print("  Checking stop-loss conditions...")
    stop_loss_sold = []
    for sym, p in list(positions.items()):
        try:
            avg_entry = float(p.get("avg_entry_price", 0))
            current = float(p.get("current_price", 0)) or avg_entry
            if avg_entry <= 0:
                continue
            pl_pct = ((current - avg_entry) / avg_entry) * 100
            if pl_pct <= -5.0:
                qty = p.get("qty", 1)
                if is_market_open():
                    result = place_paper_trade(sym, "sell", qty)
                    print(f"    STOP-LOSS sell {qty}x {sym} @ ${current} (down {pl_pct:.2f}%): {result.get('status', result.get('error', 'unknown'))}")
                    stop_loss_sold.append(sym)
                else:
                    print(f"    STOP-LOSS triggered for {sym} (down {pl_pct:.2f}%) but market closed; will sell next open")
        except Exception as e:
            print(f"    Stop-loss check error for {sym}: {e}")
    if stop_loss_sold:
        positions = get_alpaca_positions()  # refresh after sells

    budget_remaining = get_weekly_budget_remaining()
    per_position = round(min(budget_remaining / 20, 500), 2)

    print("  Scoring past calls...")
    score_past_calls()
    track_record = get_track_record()
    market_regime = get_market_regime()
    if market_regime:
        track_record["regime"] = market_regime
        print(f"  Market regime:{market_regime[:60]}")
    if track_record:
        for flag in ["GREEN", "RED", "YELLOW"]:
            stats = track_record.get(flag, {})
            if isinstance(stats, dict) and stats.get("total", 0) >= 3:
                print(f"  Track record — {flag}: {stats['accuracy']}% accurate ({stats['total']} calls)")

    budget_lock = threading.Lock()
    budget_state = {"per_position": per_position, "portfolio_val": portfolio_val, "total_deployed": 0}

    analyses = []
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(analyze_one_stock, sym, price_data, previous_calls, positions, track_record, budget_lock, budget_state): sym
            for sym in symbols
        }
        for future in as_completed(futures):
            sym = futures[future]
            try:
                result = future.result()
                if result is not None:
                    analyses.append(result)
                    print(f"  Done {sym}: {result['flag']}")
            except Exception as e:
                print(f"  Worker exception for {sym}: {e}")

    elapsed = time.time() - start_time
    print(f"  Analysis complete in {elapsed:.1f}s ({len(analyses)}/{len(symbols)} stocks)")

    green_signals = [a for a in analyses if a["flag"] == "GREEN"]
    if green_signals:
        per_position = round(budget_remaining / max(len(green_signals), 1), 2)
    print(f"  Weekly budget remaining: ${budget_remaining:,.0f} | Per position: ${per_position:,.0f} | GREEN signals: {len(green_signals)}")
    if budget_state["total_deployed"] > 0:
        print(f"  Total deployed this session: ${budget_state['total_deployed']:,.0f}")

    order = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    analyses.sort(key=lambda x: order.get(x["flag"], 1))

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT email, stocks, paid FROM subscribers WHERE active=1")
    subscribers = c.fetchall()
    conn.close()

    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_KEY)

    for email, stocks_json, paid in subscribers:
        try:
            user_stocks = json.loads(stocks_json)
            user_analyses = [a for a in analyses if a["symbol"] in user_stocks] if user_stocks != ["ALL"] else analyses
            if not user_analyses:
                continue

            if not paid:
                greens = [a for a in user_analyses if a["flag"] == "GREEN"][:3]
                reds = [a for a in user_analyses if a["flag"] == "RED"][:3]
                yellows = [a for a in user_analyses if a["flag"] == "YELLOW"][:2]
                user_html = build_free_email(greens + reds + yellows, account, email=email)
            else:
                # Auto-issue premium key on first paid email so they get the AI Portfolio link
                ensure_premium_key(email)
                user_html = build_email(user_analyses, account, email=email)

            message = Mail(
                from_email=FROM_EMAIL,
                to_emails=email,
                subject=f"JSCAN Daily Brief - {datetime.utcnow().strftime('%b %d')} | {len([a for a in user_analyses if a['flag']=='GREEN'])} BUY {len([a for a in user_analyses if a['flag']=='RED'])} SELL",
                html_content=user_html
            )
            sg.send(message)
            print(f"  Email sent to {email}")
        except Exception as e:
            print(f"  Email error for {email}: {e}")

    print(f"[{datetime.utcnow()}] Agent done. Analyzed {len(analyses)} stocks.")
    print(f"  Claude usage: {usage_summary()}")

    # Marketing runs in background so it doesn't block the schedule thread
    threading.Thread(target=run_marketing, args=(analyses,), daemon=True).start()
    return analyses

# ─── MARKETING AGENT ──────────────────────────────────────
def post_to_x(text):
    if not X_API_KEY or not X_ACCESS_TOKEN:
        print("  X keys not configured, skipping")
        return False
    try:
        import tweepy
        client = tweepy.Client(
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_TOKEN_SECRET
        )
        client.create_tweet(text=text[:280])
        print(f"  X tweet posted: {text[:60]}...")
        return True
    except Exception as e:
        print(f"  X post failed: {e}")
        return False

def post_to_discord(text):
    if not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        print("  Discord not configured, skipping")
        return False
    try:
        headers = {"Authorization": f"Bot {DISCORD_TOKEN}", "Content-Type": "application/json"}
        payload = {"content": text}
        r = requests.post(f"https://discord.com/api/v10/channels/{DISCORD_CHANNEL_ID}/messages",
                         headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            print(f"  Discord message posted")
            return True
        else:
            print(f"  Discord failed: {r.status_code} {r.text}")
            return False
    except Exception as e:
        print(f"  Discord post failed: {e}")
        return False

def generate_marketing_post(analyses):
    if not analyses:
        return None
    greens = [a for a in analyses if a["flag"] == "GREEN"]
    reds = [a for a in analyses if a["flag"] == "RED"]
    best = greens[0] if greens else (reds[0] if reds else analyses[0])
    flag = best["flag"]

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT ca.flag, cr.outcome FROM calls ca JOIN call_results cr ON ca.id = cr.call_id WHERE cr.days_later = 1 ORDER BY ca.created_at DESC LIMIT 50")
        rows = c.fetchall()
        conn.close()
        if rows:
            correct = sum(1 for r in rows if r[1] == "correct")
            accuracy_line = f"{round(correct/len(rows)*100)}% accuracy over {len(rows)} calls."
        else:
            accuracy_line = "Just launched - first week of tracking."
    except:
        accuracy_line = "AI-powered stock research agent."

    prompt = f"""Write a compelling tweet about today's AI stock signal. Include the signup URL as plain text.

Signal: {best['symbol']} flagged {flag_to_action(flag)} at ${best['price']}
Thesis: {best.get('verdict','')[:120]}
Today: {len(greens)} BUY, {len(reds)} SELL out of {len(analyses)} stocks analyzed
{accuracy_line}
URL: jscan-agent.up.railway.app

Rules:
- Max 255 chars total
- Sound like a real trader not a bot
- Include jscan-agent.up.railway.app as plain text
- Max 2 hashtags
- Be specific about the signal"""

    msg = claude_call(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text.strip()

def generate_discord_post(analyses):
    if not analyses:
        return None
    greens = [a for a in analyses if a["flag"] == "GREEN"]
    reds = [a for a in analyses if a["flag"] == "RED"]
    yellows = [a for a in analyses if a["flag"] == "YELLOW"]
    today = datetime.utcnow().strftime("%B %d, %Y")
    msg = f"**JSCAN Daily Signals - {today}**\n\n"
    msg += f"Analyzed **{len(analyses)} stocks** today\n"
    msg += f"**{len(greens)} BUY** | {len(yellows)} WATCH | **{len(reds)} SELL**\n\n"
    if greens:
        msg += "**TOP BUY SIGNALS:**\n"
        for c in greens[:3]:
            msg += f"🟢 **{c['symbol']}** @ ${c['price']} — {c.get('verdict','')[:80]}...\n"
    if reds:
        msg += "\n**TOP SELL SIGNALS:**\n"
        for c in reds[:3]:
            msg += f"🔴 **{c['symbol']}** @ ${c['price']} — {c.get('verdict','')[:80]}...\n"
    msg += f"\nFull analysis + signup: jscan-agent.up.railway.app"
    return msg

def run_marketing(analyses):
    print(f"[{datetime.utcnow()}] Running marketing agent...")
    if not analyses:
        print("  No analyses to market")
        return

    discord_msg = generate_discord_post(analyses)
    if discord_msg:
        post_to_discord(discord_msg)

    tweet = generate_marketing_post(analyses)
    if tweet:
        post_to_x(tweet)

    print(f"[{datetime.utcnow()}] Marketing done.")

# ─── FLASK ROUTES ─────────────────────────────────────────
SIGNUP_HTML = """<!DOCTYPE html>
<html>
<head>
<title>JSCAN Daily Brief</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:#0a0a0a;color:#e0e0e0;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.card{background:#0d0d0d;border:1px solid #1c1c1c;border-radius:16px;padding:40px;max-width:560px;width:100%}
.logo{font-size:1.6em;font-weight:800;color:#00ff88;margin-bottom:6px}
.logo span{color:#fff}
.sub{color:#555;font-size:.88em;margin-bottom:32px}
label{display:block;font-size:.78em;color:#555;text-transform:uppercase;letter-spacing:.7px;margin-bottom:6px;font-weight:500}
input[type=email]{width:100%;background:#080808;border:1px solid #1c1c1c;border-radius:8px;padding:12px 14px;color:#fff;font-size:.95em;font-family:inherit;margin-bottom:20px;outline:none;transition:border-color .2s}
input[type=email]:focus{border-color:#00ff88}
.stocks-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:24px;max-height:320px;overflow-y:auto}
.stock-check{background:#080808;border:1px solid #1c1c1c;border-radius:7px;padding:8px 10px;cursor:pointer;transition:all .2s;text-align:center}
.stock-check:hover{border-color:#2a2a2a}
.stock-check.selected{border-color:#00ff88;background:rgba(0,255,136,.05)}
.stock-check input{display:none}
.stock-sym{font-size:.85em;font-weight:700;color:#fff}
.stock-nm{font-size:.62em;color:#444;margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.actions{display:flex;gap:8px;margin-bottom:20px}
.btn-sm{background:transparent;border:1px solid #1c1c1c;color:#555;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:.75em;font-family:inherit;transition:all .2s}
.btn-sm:hover{border-color:#00ff88;color:#00ff88}
.submit{width:100%;background:#00ff88;color:#000;border:none;border-radius:8px;padding:14px;font-size:1em;font-weight:700;font-family:inherit;cursor:pointer;transition:opacity .2s}
.submit:hover{opacity:.9}
.msg{margin-top:16px;padding:12px 16px;border-radius:8px;font-size:.88em;display:none}
.msg.success{background:rgba(0,255,136,.1);border:1px solid rgba(0,255,136,.2);color:#00ff88}
.msg.error{background:rgba(255,68,68,.1);border:1px solid rgba(255,68,68,.2);color:#ff4444}
.section-label{font-size:.78em;color:#444;text-transform:uppercase;letter-spacing:.7px;margin-bottom:12px;font-weight:500}
</style>
</head>
<body>
<div class="card">
  <div class="logo">J<span>SCAN</span></div>
  <div class="sub">Get a daily AI-powered stock brief delivered to your inbox every morning at 8am.</div>
  <label>Your Email</label>
  <input type="email" id="email" placeholder="you@example.com">
  <div class="section-label">Pick Your Stocks</div>
  <div class="actions">
    <button class="btn-sm" onclick="selectAll()">Select All</button>
    <button class="btn-sm" onclick="clearAll()">Clear All</button>
  </div>
  <div class="stocks-grid" id="stocks-grid"></div>
  <button class="submit" onclick="subscribe()">Subscribe — Free</button>
  <div class="msg" id="msg"></div>
</div>
<script>
var STOCKS = """ + json.dumps({k: v for k, v in STOCK_NAMES.items()}) + """;
var selected = new Set();
function buildGrid(){
  var g = document.getElementById('stocks-grid');
  Object.keys(STOCKS).forEach(function(sym){
    var d = document.createElement('div');
    d.className='stock-check';
    d.dataset.sym=sym;
    d.innerHTML='<div class="stock-sym">'+sym+'</div><div class="stock-nm">'+STOCKS[sym]+'</div>';
    d.addEventListener('click',function(){
      if(selected.has(sym)){selected.delete(sym);d.classList.remove('selected');}
      else{selected.add(sym);d.classList.add('selected');}
    });
    g.appendChild(d);
  });
}
function selectAll(){
  Object.keys(STOCKS).forEach(function(sym){
    selected.add(sym);
    document.querySelector('[data-sym="'+sym+'"]').classList.add('selected');
  });
}
function clearAll(){
  selected.clear();
  document.querySelectorAll('.stock-check').forEach(function(d){d.classList.remove('selected');});
}
function subscribe(){
  var email=document.getElementById('email').value.trim();
  var msg=document.getElementById('msg');
  if(!email){showMsg('Please enter your email.','error');return;}
  if(selected.size===0){showMsg('Please select at least one stock.','error');return;}
  fetch('/subscribe',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({email:email,stocks:Array.from(selected)})
  }).then(function(r){return r.json();}).then(function(d){
    if(d.success){showMsg('Subscribed! Your first brief arrives tomorrow at 8am.','success');}
    else{showMsg(d.error||'Something went wrong.','error');}
  }).catch(function(){showMsg('Network error.','error');});
}
function showMsg(text,type){
  var m=document.getElementById('msg');
  m.textContent=text;
  m.className='msg '+type;
  m.style.display='block';
}
buildGrid();
</script>
</body>
</html>"""

@app.route("/")
def index():
    return render_template_string(SIGNUP_HTML)

@app.route("/subscribe", methods=["POST"])
def subscribe():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr) or "unknown"
    if "," in ip:  # X-Forwarded-For can be a comma-separated chain
        ip = ip.split(",")[0].strip()
    if not check_rate_limit(ip):
        return jsonify({"success": False, "error": "Too many requests, try again later"}), 429

    data = request.json or {}
    email = (data.get("email") or "").strip().lower()
    stocks = data.get("stocks") or []

    if not email or not EMAIL_REGEX.match(email):
        return jsonify({"success": False, "error": "Invalid email address"}), 400
    if not isinstance(stocks, list) or not stocks:
        return jsonify({"success": False, "error": "Please select at least one stock"}), 400
    if len(stocks) > 200:
        return jsonify({"success": False, "error": "Too many stocks selected"}), 400

    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # Upsert that preserves `paid` on existing rows
        c.execute("""INSERT INTO subscribers (email, stocks, active)
                     VALUES (?, ?, 1)
                     ON CONFLICT(email) DO UPDATE SET
                       stocks = excluded.stocks,
                       active = 1""",
                  (email, json.dumps(stocks)))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/unsubscribe", methods=["GET"])
def unsubscribe():
    email = (request.args.get("email") or "").strip().lower()
    token = (request.args.get("token") or "").strip()
    if not email or not token:
        return "Invalid unsubscribe link", 400
    if not verify_unsubscribe_token(email, token):
        return "Invalid or tampered unsubscribe link", 403
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE subscribers SET active=0 WHERE email=?", (email,))
        conn.commit()
        conn.close()
    except Exception as e:
        return f"Error: {e}", 500
    return f"""<html><head><title>Unsubscribed</title></head>
<body style="font-family:-apple-system,sans-serif;background:#0a0a0a;color:#e0e0e0;padding:60px;text-align:center">
<h2 style="color:#00ff88">Unsubscribed</h2>
<p>{email} will no longer receive JSCAN briefs.</p>
<p style="color:#555;font-size:13px">Changed your mind? Resubscribe at <a href="{PUBLIC_BASE_URL}" style="color:#00ff88">{PUBLIC_BASE_URL}</a></p>
</body></html>"""

@app.route("/api/portfolio", methods=["GET", "OPTIONS"])
def api_portfolio():
    """Public endpoint, gated by premium key. Returns the agent's paper portfolio
    state for jscan.tech to render on the AI Portfolio tab."""
    if request.method == "OPTIONS":
        return "", 204
    key = (request.args.get("key") or "").strip()
    row = lookup_email_by_premium_key(key)
    if not row:
        return jsonify({"error": "invalid key"}), 401
    email, paid, active = row
    if not paid or not active:
        return jsonify({"error": "subscription inactive"}), 403

    account = get_alpaca_account()
    positions_raw = get_alpaca_positions()
    orders_raw = get_alpaca_orders(limit=30, status="closed")
    track_record = get_track_record()

    positions = []
    for sym, p in positions_raw.items():
        try:
            avg = float(p.get("avg_entry_price", 0) or 0)
            cur = float(p.get("current_price", 0) or 0)
            qty = float(p.get("qty", 0) or 0)
            pl = float(p.get("unrealized_pl", 0) or 0)
            pl_pct = float(p.get("unrealized_plpc", 0) or 0) * 100
        except (TypeError, ValueError):
            continue
        positions.append({
            "symbol": sym,
            "name": STOCK_NAMES.get(sym, sym),
            "qty": qty,
            "avg_cost": round(avg, 2),
            "current": round(cur, 2),
            "market_value": round(cur * qty, 2),
            "pl": round(pl, 2),
            "pl_pct": round(pl_pct, 2),
        })
    positions.sort(key=lambda x: x["market_value"], reverse=True)

    trades = []
    for o in orders_raw:
        if o.get("status") not in ("filled", "partially_filled"):
            continue
        try:
            qty = float(o.get("filled_qty") or o.get("qty") or 0)
            price = float(o.get("filled_avg_price") or 0)
        except (TypeError, ValueError):
            continue
        trades.append({
            "symbol": o.get("symbol"),
            "side": o.get("side"),
            "qty": qty,
            "price": round(price, 2),
            "cost": round(qty * price, 2),
            "filled_at": o.get("filled_at") or o.get("submitted_at"),
        })

    # Strip stats that aren't meaningful externally; expose track record summary
    flag_stats = {}
    for flag in ["GREEN", "RED", "YELLOW"]:
        s = track_record.get(flag)
        if isinstance(s, dict) and s.get("total"):
            flag_stats[flag_to_action(flag)] = {
                "accuracy": s.get("accuracy"),
                "total": s.get("total"),
                "correct": s.get("correct"),
                "avg_move": s.get("avg_move"),
            }

    try:
        portfolio_value = float(account.get("portfolio_value", 0) or 0)
    except (TypeError, ValueError):
        portfolio_value = 0.0
    try:
        cash = float(account.get("cash", 0) or 0)
    except (TypeError, ValueError):
        cash = 0.0
    try:
        equity = float(account.get("equity", 0) or 0)
        last_equity = float(account.get("last_equity", 0) or 0)
        day_pl = round(equity - last_equity, 2) if last_equity else 0.0
        day_pl_pct = round((day_pl / last_equity) * 100, 2) if last_equity else 0.0
    except (TypeError, ValueError):
        day_pl = day_pl_pct = 0.0

    return jsonify({
        "portfolio_value": round(portfolio_value, 2),
        "cash": round(cash, 2),
        "day_pl": day_pl,
        "day_pl_pct": day_pl_pct,
        "positions": positions,
        "recent_trades": trades[:20],
        "track_record": flag_stats,
        "as_of": datetime.utcnow().isoformat() + "Z",
    })

@app.route("/run", methods=["POST"])
@require_auth
def trigger_run():
    threading.Thread(target=lambda: run_agent(force=True), daemon=True).start()
    return jsonify({"success": True, "message": "Agent started in background"})

@app.route("/status")
def status():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM subscribers WHERE active=1")
    subs = c.fetchone()[0]
    c.execute("SELECT symbol, date, flag, price FROM calls ORDER BY created_at DESC LIMIT 20")
    calls = [{"symbol": r[0], "date": r[1], "flag": r[2], "price": r[3]} for r in c.fetchall()]
    conn.close()
    account = get_alpaca_account()
    track_record = get_track_record()
    return jsonify({
        "subscribers": subs,
        "recent_calls": calls,
        "portfolio_value": account.get("portfolio_value"),
        "cash": account.get("cash"),
        "track_record": track_record
    })

@app.route("/dashboard")
@require_auth
def dashboard():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM subscribers WHERE active=1")
    subs = c.fetchone()[0]
    c.execute("""
        SELECT ca.symbol, ca.date, ca.flag, ca.price, ca.thesis,
               cr.price_change_pct, cr.outcome, cr.days_later
        FROM calls ca
        LEFT JOIN call_results cr ON ca.id = cr.call_id AND cr.days_later = 1
        ORDER BY ca.created_at DESC LIMIT 100
    """)
    rows = c.fetchall()
    calls = [{"symbol": r[0], "date": r[1], "flag": r[2], "price": r[3],
              "thesis": r[4], "change_pct": r[5], "outcome": r[6], "days": r[7]} for r in rows]
    scored = [cc for cc in calls if cc["change_pct"] is not None]
    best = sorted(scored, key=lambda x: x["change_pct"] or 0, reverse=True)[:5]
    worst = sorted(scored, key=lambda x: x["change_pct"] or 0)[:5]
    conn.close()

    account = get_alpaca_account()
    track_record = get_track_record()
    positions = get_alpaca_positions()
    portfolio_val = account.get("portfolio_value", 0)
    cash = account.get("cash", 0)

    def flag_color(f):
        return {"GREEN": "#00ff88", "YELLOW": "#f0c040", "RED": "#ff4444"}.get(f, "#888")

    def outcome_badge(o):
        if o == "correct": return '<span style="color:#00ff88;font-weight:600">✓ Correct</span>'
        if o == "incorrect": return '<span style="color:#ff4444;font-weight:600">✗ Wrong</span>'
        return '<span style="color:#555">— Pending</span>'

    tr_html = ""
    if track_record:
        for flag in ["GREEN", "RED", "YELLOW"]:
            stats = track_record.get(flag, {})
            if not isinstance(stats, dict):
                continue
            fc = flag_color(flag)
            bar_width = stats.get("accuracy", 0)
            tr_html += f"""
            <div style="margin-bottom:16px">
                <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                    <span style="color:{fc};font-weight:700">{flag_to_action(flag)}</span>
                    <span style="color:#fff;font-weight:600">{stats.get('accuracy',0)}% accurate</span>
                </div>
                <div style="background:#1a1a1a;border-radius:4px;height:8px;overflow:hidden">
                    <div style="background:{fc};height:100%;width:{bar_width}%"></div>
                </div>
                <div style="color:#444;font-size:.75em;margin-top:4px">{stats.get('correct',0)} correct out of {stats.get('total',0)} calls</div>
            </div>"""
    else:
        tr_html = '<div style="color:#444;font-size:.88em">No track record yet — needs data from scored calls</div>'

    calls_html = ""
    for call in calls[:50]:
        fc = flag_color(call["flag"])
        chg = call["change_pct"]
        chg_str = f'+{chg:.2f}%' if chg and chg >= 0 else f'{chg:.2f}%' if chg else '—'
        chg_color = "#00ff88" if chg and chg >= 0 else "#ff4444" if chg else "#555"
        calls_html += f"""
        <tr style="border-bottom:1px solid #111">
            <td style="padding:10px 16px;color:#fff;font-weight:700">{call['symbol']}</td>
            <td style="padding:10px 16px;color:#555;font-size:.85em">{call['date']}</td>
            <td style="padding:10px 16px">{action_badge(call['flag'])}</td>
            <td style="padding:10px 16px;color:#aaa">${call['price']}</td>
            <td style="padding:10px 16px;color:{chg_color};font-weight:600">{chg_str}</td>
            <td style="padding:10px 16px">{outcome_badge(call['outcome'])}</td>
            <td style="padding:10px 16px;color:#555;font-size:.8em;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{call['thesis'] or '—'}</td>
        </tr>"""

    pos_html = ""
    if positions:
        for sym, p in positions.items():
            pl = float(p.get("unrealized_pl", 0))
            pl_color = "#00ff88" if pl >= 0 else "#ff4444"
            pl_str = f'+${pl:.2f}' if pl >= 0 else f'-${abs(pl):.2f}'
            pos_html += f"""
            <tr style="border-bottom:1px solid #111">
                <td style="padding:10px 16px;color:#fff;font-weight:700">{sym}</td>
                <td style="padding:10px 16px;color:#aaa">{p.get('qty')} shares</td>
                <td style="padding:10px 16px;color:#aaa">${float(p.get('avg_entry_price',0)):.2f}</td>
                <td style="padding:10px 16px;color:#aaa">${float(p.get('current_price',0)):.2f}</td>
                <td style="padding:10px 16px;color:{pl_color};font-weight:600">{pl_str}</td>
            </tr>"""
    else:
        pos_html = '<tr><td colspan="5" style="padding:20px 16px;color:#444;text-align:center">No open positions</td></tr>'

    html = f"""<!DOCTYPE html>
<html>
<head>
<title>JSCAN Agent Dashboard</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',sans-serif;background:#0a0a0a;color:#e0e0e0;min-height:100vh}}
.header{{padding:20px 40px;border-bottom:1px solid #1c1c1c;display:flex;align-items:center;justify-content:space-between;background:#050505}}
.logo{{font-size:1.4em;font-weight:700;color:#00ff88}}
.logo span{{color:#fff}}
.nav a{{color:#555;text-decoration:none;font-size:.85em;margin-left:20px;transition:color .2s}}
.nav a:hover{{color:#00ff88}}
.container{{padding:32px 40px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:12px;margin-bottom:28px}}
.stat-card{{background:#0d0d0d;border:1px solid #1c1c1c;border-radius:10px;padding:18px 20px}}
.stat-label{{font-size:.7em;color:#444;text-transform:uppercase;letter-spacing:.8px;margin-bottom:6px}}
.stat-value{{font-size:1.6em;font-weight:700;color:#00ff88}}
.section{{background:#0d0d0d;border:1px solid #1c1c1c;border-radius:12px;margin-bottom:20px;overflow:hidden}}
.section-header{{padding:16px 20px;border-bottom:1px solid #141414;font-size:.8em;color:#555;text-transform:uppercase;letter-spacing:.8px;font-weight:600}}
.section-body{{padding:20px}}
table{{width:100%;border-collapse:collapse}}
th{{padding:10px 16px;text-align:left;font-size:.68em;color:#444;text-transform:uppercase;letter-spacing:.8px;border-bottom:1px solid #141414}}
.run-btn{{background:#00ff88;color:#000;border:none;padding:10px 20px;border-radius:8px;font-weight:700;cursor:pointer;font-family:inherit;font-size:.88em}}
.run-btn:hover{{opacity:.85}}
.run-btn:disabled{{opacity:.5;cursor:not-allowed}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">J<span>SCAN</span> <span style="color:#555;font-size:.65em;font-weight:400;margin-left:8px">Agent Dashboard</span></div>
  <div class="nav">
    <a href="/">Signup</a>
    <a href="https://jscan.tech" target="_blank">JSCAN ↗</a>
    <button class="run-btn" id="run-btn" onclick="triggerRun()">▶ Run Now</button>
  </div>
</div>
<div class="container">
  <div class="grid">
    <div class="stat-card"><div class="stat-label">Portfolio Value</div><div class="stat-value">${float(portfolio_val):,.0f}</div></div>
    <div class="stat-card"><div class="stat-label">Cash</div><div class="stat-value" style="color:#e0e0e0">${float(cash):,.0f}</div></div>
    <div class="stat-card"><div class="stat-label">Subscribers</div><div class="stat-value" style="color:#e0e0e0">{subs}</div></div>
    <div class="stat-card"><div class="stat-label">Total Calls</div><div class="stat-value" style="color:#e0e0e0">{len(calls)}</div></div>
    <div class="stat-card"><div class="stat-label">Scored Calls</div><div class="stat-value" style="color:#e0e0e0">{len(scored)}</div></div>
  </div>
  <div class="section">
    <div class="section-header">🎯 Track Record</div>
    <div class="section-body">{tr_html}</div>
  </div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:20px">
    <div class="section">
      <div class="section-header">🟢 Best Calls</div>
      <div class="section-body">
        {''.join([f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #111"><span style="color:#fff;font-weight:600">{b["symbol"]}</span><span style="color:#00ff88;font-weight:600">+{b["change_pct"]:.2f}%</span></div>' for b in best]) or '<div style="color:#444">No data yet</div>'}
      </div>
    </div>
    <div class="section">
      <div class="section-header">🔴 Worst Calls</div>
      <div class="section-body">
        {''.join([f'<div style="display:flex;justify-content:space-between;padding:6px 0;border-bottom:1px solid #111"><span style="color:#fff;font-weight:600">{w["symbol"]}</span><span style="color:#ff4444;font-weight:600">{w["change_pct"]:.2f}%</span></div>' for w in worst]) or '<div style="color:#444">No data yet</div>'}
      </div>
    </div>
  </div>
  <div class="section">
    <div class="section-header">📈 Paper Positions</div>
    <table><tr><th>Symbol</th><th>Quantity</th><th>Avg Cost</th><th>Current</th><th>P&L</th></tr>{pos_html}</table>
  </div>
  <div class="section">
    <div class="section-header">📋 Recent Calls (last 50)</div>
    <div style="overflow-x:auto">
      <table>
        <tr><th>Symbol</th><th>Date</th><th>Action</th><th>Price</th><th>1d Change</th><th>Outcome</th><th>Thesis</th></tr>
        {calls_html}
      </table>
    </div>
  </div>
</div>
<script>
function triggerRun(){{
  var btn=document.getElementById('run-btn');
  btn.disabled=true;
  btn.textContent='Running...';
  var key=new URLSearchParams(window.location.search).get('key')||'';
  fetch('/run',{{method:'POST',headers:{{'X-API-Key':key}}}})
    .then(function(r){{return r.json();}})
    .then(function(d){{
      btn.textContent='✓ Started';
      setTimeout(function(){{location.reload();}},3000);
    }})
    .catch(function(){{btn.textContent='Error';btn.disabled=false;}});
}}
</script>
</body>
</html>"""
    return html

if __name__ == "__main__":
    init_db()
    print("JSCAN Agent starting...")
    print(f"Watching {len(WATCHLIST)} stocks")

    schedule.every().day.at("14:00").do(run_agent)

    def run_schedule():
        while True:
            schedule.run_pending()
            time.sleep(60)
    threading.Thread(target=run_schedule, daemon=True).start()

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
