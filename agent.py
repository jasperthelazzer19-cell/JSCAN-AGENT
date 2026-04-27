import os
import time
import json
import sqlite3
import requests
import schedule
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, jsonify

app = Flask(__name__)

@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response
import anthropic
import sendgrid
from sendgrid.helpers.mail import Mail

# ─── CONFIG ───────────────────────────────────────────────
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_KEY", "")
SENDGRID_KEY    = os.environ.get("SENDGRID_KEY", "")
NEWSAPI_KEY     = os.environ.get("NEWSAPI_KEY", "")
POLYGON_KEY     = os.environ.get("POLYGON_KEY", "")
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

WATCHLIST = [
    "AAPL","MSFT","NVDA","AMZN","GOOGL","META","TSLA","BRK-B","JPM","V",
    "WMT","XOM","UNH","LLY","MA","JNJ","PG","HD","MRK","COST",
    "ABBV","CVX","BAC","KO","PEP","ADBE","CRM","NFLX","AMD","TMO",
    "ACN","MCD","CSCO","ABT","LIN","DHR","WFC","TXN","NEE","PM",
    "RTX","AMGN","LOW","ORCL","UPS","INTC","QCOM","CAT","NOW","INTU",
    "PLTR","SNOW","COIN","HOOD","RBLX","UBER","LYFT","ABNB","DASH","SPOT",
    "SHOP","SQ","PYPL","SOFI","AFRM","NET","DDOG","ZS","CRWD","OKTA",
    "ARM","SMCI","MU","TSM","ASML","AMAT","LRCX","KLAC","ON","MRVL",
    "DIS","NFLX","PARA","WBD","CMCSA","T","VZ","TMUS","CHTR","DISH",
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
    "SHOP":"Shopify","SQ":"Block","PYPL":"PayPal","SOFI":"SoFi",
    "AFRM":"Affirm","NET":"Cloudflare","DDOG":"Datadog","ZS":"Zscaler",
    "CRWD":"CrowdStrike","OKTA":"Okta","ARM":"ARM Holdings","SMCI":"Super Micro",
    "MU":"Micron","TSM":"TSMC","ASML":"ASML","AMAT":"Applied Materials",
    "LRCX":"Lam Research","KLAC":"KLA Corp","ON":"ON Semiconductor","MRVL":"Marvell",
    "DIS":"Disney","PARA":"Paramount","WBD":"Warner Bros","CMCSA":"Comcast",
    "T":"AT&T","VZ":"Verizon","TMUS":"T-Mobile","CHTR":"Charter","DISH":"DISH",
    "GS":"Goldman Sachs","MS":"Morgan Stanley","BLK":"BlackRock","C":"Citigroup",
    "USB":"US Bancorp","PNC":"PNC Financial","TFC":"Truist","SCHW":"Charles Schwab",
    "AXP":"American Express","COF":"Capital One"
}

app = Flask(__name__)

# ─── DATABASE ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS subscribers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        stocks TEXT NOT NULL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        active INTEGER DEFAULT 1
    )""")
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
    conn.commit()
    conn.close()

# ─── DATA FETCHING ────────────────────────────────────────
def get_stock_data(symbols):
    result = {}
    try:
        check_date = datetime.now() - timedelta(days=1)
        for _ in range(7):
            if check_date.weekday() < 5:
                break
            check_date -= timedelta(days=1)
        date_str = check_date.strftime("%Y-%m-%d")
        r = requests.get(
            f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}",
            params={"adjusted": "true", "apiKey": POLYGON_KEY},
            timeout=20
        )
        bars = {b["T"]: b for b in r.json().get("results", [])}
        for sym in symbols:
            poly_sym = sym.replace("-", ".")
            bar = bars.get(poly_sym) or bars.get(sym)
            if bar:
                c = float(bar.get("c", 0))
                o = float(bar.get("o", 0))
                h = float(bar.get("h", 0))
                l = float(bar.get("l", 0))
                v = int(bar.get("v", 0))
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

WEEKLY_BUDGET = 10000  # $10k paper money per week

def get_weekly_budget_remaining():
    """Track how much of this week's budget has been deployed."""
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    # Create budget table if not exists
    c.execute("""CREATE TABLE IF NOT EXISTS weekly_budget (
        week TEXT PRIMARY KEY,
        deployed REAL DEFAULT 0,
        starting_value REAL DEFAULT 0,
        current_value REAL DEFAULT 0
    )""")
    conn.commit()
    week = datetime.now().strftime("%Y-W%W")
    c.execute("SELECT deployed FROM weekly_budget WHERE week = ?", (week,))
    row = c.fetchone()
    conn.close()
    if not row:
        return WEEKLY_BUDGET
    return max(0, WEEKLY_BUDGET - row[0])

def record_budget_deployment(amount, portfolio_value):
    """Record money deployed this week."""
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    week = datetime.now().strftime("%Y-W%W")
    c.execute("""INSERT INTO weekly_budget (week, deployed, starting_value, current_value)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(week) DO UPDATE SET
        deployed = deployed + ?,
        current_value = ?
    """, (week, amount, portfolio_value, portfolio_value, amount, portfolio_value))
    conn.commit()
    conn.close()

def get_portfolio_history():
    """Get weekly portfolio performance for display."""
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS weekly_budget (week TEXT PRIMARY KEY, deployed REAL DEFAULT 0, starting_value REAL DEFAULT 0, current_value REAL DEFAULT 0)")
    c.execute("SELECT week, deployed, starting_value, current_value FROM weekly_budget ORDER BY week DESC LIMIT 12")
    rows = c.fetchall()
    conn.close()
    return [{"week": r[0], "deployed": r[1], "starting": r[2], "current": r[3]} for r in rows]
    try:
        r = requests.post(
            f"{ALPACA_BASE_URL}/v2/orders",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            json={"symbol": symbol, "qty": qty, "side": side, "type": "market", "time_in_force": "day"},
            timeout=8
        )
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def get_previous_calls():
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    c.execute("SELECT symbol, date, flag, price, thesis FROM calls WHERE date >= ? ORDER BY date DESC", (week_ago,))
    rows = c.fetchall()
    conn.close()
    return [{"symbol": r[0], "date": r[1], "flag": r[2], "price": r[3], "thesis": r[4]} for r in rows]

def save_call(symbol, flag, price, thesis):
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("INSERT INTO calls (symbol, date, flag, price, thesis) VALUES (?, ?, ?, ?, ?)",
              (symbol, today, flag, price, thesis))
    conn.commit()
    conn.close()

# ─── CLAUDE ANALYSIS ──────────────────────────────────────
def score_past_calls():
    """Score calls from 1, 7, and 30 days ago against actual price movement."""
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    scored = 0

    for days_back in [1, 7, 30]:
        target_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        c.execute("""
            SELECT id, symbol, flag, price FROM calls
            WHERE date = ? AND id NOT IN (
                SELECT call_id FROM call_results WHERE days_later = ?
            )
        """, (target_date, days_back))
        old_calls = c.fetchall()

        if not old_calls:
            continue

        # Get current prices
        symbols = [r[1] for r in old_calls]
        current_prices = get_stock_data(symbols)

        for call_id, symbol, flag, price_then in old_calls:
            if symbol not in current_prices:
                continue
            price_now = current_prices[symbol]["price"]
            if not price_then or not price_now:
                continue
            change_pct = round(((price_now - price_then) / price_then) * 100, 2)

            # Score: GREEN should go up, RED should go down, YELLOW = neutral
            if flag == "GREEN":
                outcome = "correct" if change_pct > 0.5 else "incorrect" if change_pct < -0.5 else "neutral"
            elif flag == "RED":
                outcome = "correct" if change_pct < -0.5 else "incorrect" if change_pct > 0.5 else "neutral"
            else:
                outcome = "neutral"

            c.execute("""
                INSERT INTO call_results (call_id, days_later, price_then, price_change_pct, outcome)
                VALUES (?, ?, ?, ?, ?)
            """, (call_id, days_back, price_now, change_pct, outcome))
            scored += 1

    conn.commit()
    conn.close()
    print(f"  Scored {scored} past calls")
    return scored

def get_track_record():
    """Get Claude's historical accuracy stats to feed back into prompts."""
    conn = sqlite3.connect("agent.db")
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
            avg_move = round(sum(r[2]*r[1] for r in rows if r[2]) / total, 2) if rows else 0
            accuracy = round((correct / total) * 100, 1)
            stats[flag] = {"accuracy": accuracy, "total": total, "correct": correct, "avg_move": avg_move}

    # Sector-level accuracy
    sector_stats = {}
    sectors = {
        "TECH": ["AAPL","MSFT","NVDA","GOOGL","META","AMD","INTC","QCOM","ADBE","CRM","NOW","ORCL","CSCO","SNOW","PLTR","DDOG","NET","CRWD"],
        "FINANCE": ["JPM","BAC","WFC","GS","MS","V","MA","C","USB","PNC","SCHW","BLK","AXP","SPGI"],
        "HEALTH": ["UNH","LLY","JNJ","MRK","ABBV","PFE","BMY","GILD","AMGN","MDT","BSX","SYK","ZTS"],
        "ENERGY": ["XOM","CVX","COP","PSX","VLO","MPC","HES","DVN","FANG","OXY","SLB","HAL"],
        "CONSUMER": ["AMZN","TSLA","WMT","HD","MCD","COST","LOW","TGT","NKE","SBUX","DIS","NFLX"]
    }
    for sector, syms in sectors.items():
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
    """Get this stock's specific call history to feed into the agent."""
    try:
        conn = sqlite3.connect("agent.db")
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
                lines.append(f"  {date}: {flag} at ${price} → {change_pct:+.2f}% ({outcome.upper()})")
            else:
                lines.append(f"  {date}: {flag} at ${price} → pending")
        return "\nTHIS STOCK'S HISTORY:\n" + "\n".join(lines)
    except:
        return ""

def get_market_regime():
    """Detect current market regime using SPY and VIX data."""
    try:
        import requests as req
        # Get SPY 5-day trend from Polygon
        spy = req.get(
            f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/2020-01-01/{datetime.now().strftime('%Y-%m-%d')}",
            params={"apiKey": POLYGON_KEY, "limit": 10, "sort": "desc"},
            timeout=6
        ).json().get("results", [])

        if len(spy) < 5:
            return ""

        recent = spy[0]["c"]
        week_ago = spy[4]["c"]
        spy_5d_change = round(((recent - week_ago) / week_ago) * 100, 2)

        # Determine regime
        if spy_5d_change > 2:
            regime = "BULL"
            note = "Market trending strongly up. GREEN calls more likely to succeed."
        elif spy_5d_change < -2:
            regime = "BEAR"
            note = "Market trending down. Be more cautious with GREEN calls, RED more likely to succeed."
        else:
            regime = "NEUTRAL"
            note = "Market range-bound. Stick to high-conviction signals only."

        return f"\nMARKET REGIME: {regime} (SPY 5d: {spy_5d_change:+.2f}%) — {note}"
    except:
        return ""
    """Get this stock's specific call history to feed into the agent."""
    conn = sqlite3.connect("agent.db")
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
            lines.append(f"  {date}: {flag} at ${price} → {change_pct:+.2f}% ({outcome.upper()})")
        else:
            lines.append(f"  {date}: {flag} at ${price} → pending")
    return "\nTHIS STOCK'S HISTORY:\n" + "\n".join(lines)
    """Agent 1: Analyzes news sentiment and identifies key catalysts."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    news_text = "\n".join([f"- {n['title']} ({n['source']}, {n['published']})" for n in news]) or "No recent news."
    prompt = f"""You are a financial news analyst. Analyze recent news for {name} ({symbol}).

NEWS:
{news_text}

Respond in this EXACT format:
SENTIMENT_SCORE: [number from -10 to +10, where -10 is extremely bearish, 0 is neutral, +10 is extremely bullish]
KEY_CATALYST: [single most important news item, or "None" if no significant news]
RISK_FLAG: [any major risks mentioned in news, or "None"]
SUMMARY: [1 sentence summary of news sentiment]"""

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def technical_agent(symbol, name, price_data):
    """Agent 2: Analyzes price action and technical indicators."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    price = price_data.get("price", 0)
    open_p = price_data.get("open", 0)
    high = price_data.get("high", 0)
    low = price_data.get("low", 0)
    volume = price_data.get("volume", 0)
    change_pct = price_data.get("change_pct", 0)

    # Calculate basic technicals
    day_range = high - low if high and low else 0
    range_position = ((price - low) / day_range * 100) if day_range > 0 else 50
    body_size = abs(price - open_p) / open_p * 100 if open_p else 0

    prompt = f"""You are a technical analyst. Analyze the price action for {name} ({symbol}).

PRICE DATA:
- Current: ${price} | Change: {change_pct}%
- Open: ${open_p} | High: ${high} | Low: ${low}
- Day Range: ${day_range:.2f} | Position in range: {range_position:.0f}%
- Volume: {volume:,}
- Candle body size: {body_size:.2f}%

Respond in this EXACT format:
MOMENTUM: [STRONG_UP / UP / NEUTRAL / DOWN / STRONG_DOWN]
VOLUME_SIGNAL: [HIGH / NORMAL / LOW]
RANGE_POSITION: [TOP_THIRD / MIDDLE / BOTTOM_THIRD]
STRENGTH_SCORE: [number from -10 to +10]
SUMMARY: [1 sentence technical assessment]"""

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def sentiment_agent(symbol, name, price_data, all_prices):
    """Agent 3: Analyzes macro environment and sector sentiment."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

    # Calculate sector context from all prices
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

    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=150,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def portfolio_manager(symbol, price_data, news_report, technical_report, sentiment_report, previous_calls, positions, track_record):
    """Agent 4: Synthesizes all reports and makes final call. Uses Sonnet for better decisions."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
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
            if stats.get("total", 0) >= 3:
                parts.append(f"{flag}: {stats['accuracy']}% accurate ({stats['total']} calls, avg move {stats.get('avg_move', 0)}%)")
        if parts:
            track_text = "\nYOUR TRACK RECORD (last 24h scoring):\n" + "\n".join(parts)

        # Add sector bias
        sectors = track_record.get("sectors", {})
        if sectors:
            best = max(sectors, key=sectors.get)
            worst = min(sectors, key=sectors.get)
            if sectors[best] != sectors[worst]:
                track_text += f"\nSECTOR ACCURACY: Best in {best} ({sectors[best]}%), weakest in {worst} ({sectors[worst]}%)"
                # Determine which sector this stock is in
                sym_sectors = {
                    "TECH": ["AAPL","MSFT","NVDA","GOOGL","META","AMD","INTC","QCOM","ADBE","CRM","NOW","ORCL","CSCO","SNOW","PLTR","DDOG","NET","CRWD"],
                    "FINANCE": ["JPM","BAC","WFC","GS","MS","V","MA","C","USB","PNC","SCHW","BLK","AXP","SPGI"],
                    "HEALTH": ["UNH","LLY","JNJ","MRK","ABBV","PFE","BMY","GILD","AMGN","MDT","BSX","SYK","ZTS"],
                    "ENERGY": ["XOM","CVX","COP","PSX","VLO","MPC","HES","DVN","FANG","OXY","SLB","HAL"],
                    "CONSUMER": ["AMZN","TSLA","WMT","HD","MCD","COST","LOW","TGT","NKE","SBUX","DIS","NFLX"]
                }
                for sec, syms in sym_sectors.items():
                    if symbol in syms and sec in sectors:
                        acc = sectors[sec]
                        if acc < 45:
                            track_text += f"\nNOTE: {symbol} is in {sec} sector where accuracy is only {acc}% — be more cautious with this call."
                        elif acc > 65:
                            track_text += f"\nNOTE: {symbol} is in {sec} sector where accuracy is {acc}% — high confidence sector."
                        break

    stock_history = get_stock_history(symbol)
    market_regime = track_record.get("regime", "")

    prompt = f"""You are a portfolio manager making a final investment decision for {name} ({symbol}).

ANALYST REPORTS:
NEWS AGENT: {news_report}

TECHNICAL AGENT: {technical_report}

SENTIMENT AGENT: {sentiment_report}

PRICE: ${price_data.get('price')} | Change: {price_data.get('change_pct')}%
{prev_call_text}
{position_text}
{track_text}
{stock_history}
{market_regime}

Based on ALL three analyst reports, make your final decision.
Be decisive — only use YELLOW if reports genuinely conflict. GREEN or RED when 2+ agents agree.

Respond in this EXACT format:
FLAG: [GREEN / YELLOW / RED]
BULL CASE: [1-2 sentences]
BEAR CASE: [1-2 sentences]
VERDICT: [1-2 sentences — your final call with conviction]
ACTION: [BUY / HOLD / SELL / WATCH] [optional qty e.g. "BUY 5 shares"]
CONFIDENCE: [HIGH / MEDIUM / LOW]"""

    msg = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    return msg.content[0].text

def analyze_stock(symbol, price_data, news, previous_calls, positions, track_record=None, all_prices=None):
    """Multi-agent analysis: runs 4 specialized agents then synthesizes."""
    name = STOCK_NAMES.get(symbol, symbol)
    if all_prices is None:
        all_prices = {symbol: price_data}

    try:
        news_report = news_agent(symbol, name, news)
    except Exception as e:
        news_report = f"SENTIMENT_SCORE: 0\nKEY_CATALYST: None\nRISK_FLAG: None\nSUMMARY: News analysis unavailable."

    try:
        tech_report = technical_agent(symbol, name, price_data)
    except Exception as e:
        tech_report = f"MOMENTUM: NEUTRAL\nVOLUME_SIGNAL: NORMAL\nRANGE_POSITION: MIDDLE\nSTRENGTH_SCORE: 0\nSUMMARY: Technical analysis unavailable."

    try:
        sent_report = sentiment_agent(symbol, name, price_data, all_prices)
    except Exception as e:
        sent_report = f"MARKET_CONDITIONS: NEUTRAL\nRELATIVE_STRENGTH: IN_LINE\nMACRO_SCORE: 0\nSUMMARY: Sentiment analysis unavailable."

    final = portfolio_manager(symbol, price_data, news_report, tech_report, sent_report, previous_calls, positions, track_record or {})
    return final

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
def build_email(analyses, account):
    today = datetime.now().strftime("%A, %B %d, %Y")
    portfolio_val = account.get("portfolio_value", "N/A")
    cash = account.get("cash", "N/A")

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
          <td style="padding:14px 16px;color:{fc};font-weight:700;font-size:13px">{a['flag']}</td>
          <td style="padding:14px 16px;color:#ccc;font-size:13px">{a['verdict']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="background:#0a0a0a;color:#e0e0e0;font-family:'Helvetica Neue',Arial,sans-serif;margin:0;padding:0">
  <div style="max-width:900px;margin:0 auto;padding:32px 20px">

    <!-- Header -->
    <div style="border-bottom:1px solid #1c1c1c;padding-bottom:20px;margin-bottom:28px">
      <div style="font-size:24px;font-weight:800;color:#00ff88;letter-spacing:-0.5px">📊 JSCAN Daily Brief</div>
      <div style="color:#555;font-size:13px;margin-top:4px">{today}</div>
    </div>

    <!-- Portfolio Summary -->
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

    <!-- Top Picks -->
    {"<div style='background:#0d0d0d;border:1px solid #00cc6633;border-radius:10px;padding:20px;margin-bottom:20px'><div style='font-size:13px;color:#00cc66;text-transform:uppercase;letter-spacing:1px;font-weight:700;margin-bottom:14px'>🟢 Green Signals — Bullish</div>" + "".join([f"<div style='margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #1a1a1a'><span style='font-weight:700;color:#fff'>{a['symbol']}</span> <span style='color:#aaa;font-size:13px'>({a['name']})</span> — <span style='color:#ccc;font-size:13px'>{a['verdict']}</span></div>" for a in green]) + "</div>" if green else ""}

    {"<div style='background:#0d0d0d;border:1px solid #ff444433;border-radius:10px;padding:20px;margin-bottom:20px'><div style='font-size:13px;color:#ff4444;text-transform:uppercase;letter-spacing:1px;font-weight:700;margin-bottom:14px'>🔴 Red Signals — Bearish</div>" + "".join([f"<div style='margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #1a1a1a'><span style='font-weight:700;color:#fff'>{a['symbol']}</span> <span style='color:#aaa;font-size:13px'>({a['name']})</span> — <span style='color:#ccc;font-size:13px'>{a['verdict']}</span></div>" for a in red]) + "</div>" if red else ""}

    <!-- Full Table -->
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
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Signal</th>
          <th style="padding:10px 16px;text-align:left;font-size:11px;color:#444;text-transform:uppercase;letter-spacing:1px">Verdict</th>
        </tr>
        {rows}
      </table>
    </div>

    <!-- Footer -->
    <div style="color:#333;font-size:12px;text-align:center;padding-top:16px;border-top:1px solid #1a1a1a">
      JSCAN AI Agent · Paper trading only · Not financial advice · Powered by Claude AI
    </div>
  </div>
</body>
</html>"""
    return html


def build_free_email(analyses, account):
    today = datetime.now().strftime("%A, %B %d, %Y")
    green = [a for a in analyses if a["flag"] == "GREEN"]
    red = [a for a in analyses if a["flag"] == "RED"]
    yellow = [a for a in analyses if a["flag"] == "YELLOW"]

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
          <td style="padding:14px 16px;color:{fc};font-weight:700;font-size:13px">{a['flag']}</td>
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
            <th style="padding:10px 16px;text-align:left;color:#555;font-size:11px;font-weight:600;text-transform:uppercase">Signal</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
    </div>

    <div style="background:#0d1a0d;border:1px solid #1a3a1a;border-radius:12px;padding:20px;text-align:center;margin-bottom:20px">
      <div style="color:#00cc66;font-weight:700;font-size:15px;margin-bottom:8px">Want full analysis + thesis for all 100 stocks?</div>
      <div style="color:#888;font-size:13px;margin-bottom:16px">Upgrade to see why each signal was called, sector breakdowns, and full AI reasoning.</div>
      <a href="https://jscan-agent.up.railway.app" style="background:#00ff88;color:#000;font-weight:700;padding:12px 28px;border-radius:8px;text-decoration:none;font-size:14px">Upgrade — $5/month</a>
    </div>

    <div style="text-align:center;color:#333;font-size:12px">
      JSCAN AI Agent · Paper trading only · Not financial advice
    </div>
  </div>
</body>
</html>"""
    return html

# ─── MAIN AGENT RUN ───────────────────────────────────────
def run_agent(symbols=None, force=False):
    # Skip weekends unless forced (manual run from dashboard)
    if not force and datetime.now().weekday() >= 5:
        print(f"[{datetime.now()}] Skipping — weekend, markets closed.")
        return []
    if symbols is None:
        symbols = WATCHLIST
    print(f"[{datetime.now()}] Agent running for {len(symbols)} stocks...")

    price_data = get_stock_data(symbols)
    positions = get_alpaca_positions()
    account = get_alpaca_account()
    previous_calls = get_previous_calls()

    # Score past calls and get track record for self-improvement
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

    analyses = []
    for sym in symbols:
        if sym not in price_data:
            print(f"  Skipping {sym} — no price data")
            continue
        print(f"  Analyzing {sym}...")
        pd = price_data[sym]
        news = get_news(sym, STOCK_NAMES.get(sym, sym))
        try:
            raw = analyze_stock(sym, pd, news, previous_calls, positions, track_record, price_data)
            parsed = parse_analysis(raw)
            parsed["symbol"] = sym
            parsed["name"] = STOCK_NAMES.get(sym, sym)
            parsed["price"] = pd["price"]
            parsed["change_pct"] = pd["change_pct"]
            analyses.append(parsed)
            save_call(sym, parsed["flag"], pd["price"], parsed["verdict"])

            # Paper trade if Claude says BUY or SELL — budget based sizing
            action = parsed["action"].upper()
            if "BUY" in action and parsed["flag"] == "GREEN":
                price = pd["price"]
                if price and price > 0:
                    # Calculate shares based on budget allocation
                    budget_per = min(per_position, get_weekly_budget_remaining())
                    qty = max(1, int(budget_per / price))
                    cost = round(qty * price, 2)
                    result = place_paper_trade(sym, "buy", qty)
                    if "error" not in result:
                        record_budget_deployment(cost, portfolio_val)
                        total_deployed += cost
                    print(f"    Paper BUY {qty}x {sym} @ ${price} = ${cost}: {result.get('status', result.get('error', 'unknown'))}")
            elif "SELL" in action and sym in positions:
                qty = positions[sym].get("qty", 1)
                result = place_paper_trade(sym, "sell", qty)
                print(f"    Paper SELL {qty}x {sym}: {result.get('status', result.get('error', 'unknown'))}")

        except Exception as e:
            print(f"  Error analyzing {sym}: {e}")
        time.sleep(0.5)  # avoid rate limiting

    # Calculate position size based on weekly budget
    green_signals = [a for a in analyses if a["flag"] == "GREEN"]
    budget_remaining = get_weekly_budget_remaining()
    per_position = round(budget_remaining / max(len(green_signals), 1), 2) if green_signals else 0
    print(f"  Weekly budget remaining: ${budget_remaining:,.0f} | Per position: ${per_position:,.0f} | GREEN signals: {len(green_signals)}")

    # Execute paper trades
    account = get_alpaca_account()
    portfolio_val = float(account.get("portfolio_value", 0))
    total_deployed = 0

    if total_deployed > 0:
        print(f"  Total deployed this session: ${total_deployed:,.0f}")

    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    # Add paid column if it doesn't exist
    try:
        c.execute("ALTER TABLE subscribers ADD COLUMN paid INTEGER DEFAULT 0")
        conn.commit()
    except:
        pass
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

            # Free tier: top 8 signals only, no thesis
            if not paid:
                # Take top 8: greens first, then reds, then yellows
                greens = [a for a in user_analyses if a["flag"] == "GREEN"][:3]
                reds = [a for a in user_analyses if a["flag"] == "RED"][:3]
                yellows = [a for a in user_analyses if a["flag"] == "YELLOW"][:2]
                free_analyses = greens + reds + yellows
                # Strip thesis for free tier
                for a in free_analyses:
                    a = dict(a)
                user_html = build_free_email(free_analyses, account)
            else:
                user_html = build_email(user_analyses, account)

            message = Mail(
                from_email=FROM_EMAIL,
                to_emails=email,
                subject=f"JSCAN Daily Brief - {datetime.now().strftime('%b %d')} | {len([a for a in user_analyses if a['flag']=='GREEN'])} GREEN {len([a for a in user_analyses if a['flag']=='RED'])} RED",
                html_content=user_html
            )
            sg.send(message)
            print(f"  Email sent to {email}")
        except Exception as e:
            print(f"  Email error for {email}: {e}")

    print(f"[{datetime.now()}] Agent done. Analyzed {len(analyses)} stocks.")
    run_marketing(analyses)
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

def generate_marketing_post(analyses, with_link=False):
    if not analyses:
        return None
    greens = [a for a in analyses if a["flag"] == "GREEN"]
    reds = [a for a in analyses if a["flag"] == "RED"]
    best = greens[0] if greens else (reds[0] if reds else analyses[0])
    flag = best["flag"]
    green_count = len(greens)
    red_count = len(reds)

    try:
        conn = sqlite3.connect("agent.db")
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

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    if with_link:
        prompt = f"""Write a compelling tweet about this AI stock signal. Include the signup URL as plain text.

Signal: {best['symbol']} flagged {flag} at ${best['price']}
Thesis: {best.get('thesis','')[:120]}
Today: {green_count} GREEN, {red_count} RED out of {len(analyses)} stocks analyzed
{accuracy_line}
URL: jscan-agent.up.railway.app

Rules:
- Max 255 chars total
- Sound like a real trader not a bot
- Include jscan-agent.up.railway.app as plain text
- Max 2 hashtags
- Be specific about the signal"""
    else:
        prompt = f"""Write a short punchy tweet about this AI stock signal. No links.

Signal: {best['symbol']} flagged {flag} at ${best['price']}
Thesis: {best.get('thesis','')[:120]}
Today: {green_count} GREEN, {red_count} RED out of {len(analyses)} stocks analyzed
{accuracy_line}

Rules:
- Max 240 chars
- Sound like a real trader not a bot
- NO links or URLs
- Max 2 hashtags (#stocks #algotrading)
- Mention JSCAN naturally
- Be specific and interesting"""

    msg = client.messages.create(
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
    today = datetime.now().strftime("%B %d, %Y")
    msg = f"**JSCAN Daily Signals - {today}**\n\n"
    msg += f"Analyzed **{len(analyses)} stocks** today\n"
    msg += f"**{len(greens)} GREEN** | {len(yellows)} YELLOW | **{len(reds)} RED**\n\n"
    if greens:
        msg += "**TOP BULLISH SIGNALS:**\n"
        for c in greens[:3]:
            msg += f"🟢 **{c['symbol']}** @ ${c['price']} — {c.get('thesis','')[:80]}...\n"
    if reds:
        msg += "\n**TOP BEARISH SIGNALS:**\n"
        for c in reds[:3]:
            msg += f"🔴 **{c['symbol']}** @ ${c['price']} — {c.get('thesis','')[:80]}...\n"
    msg += f"\nFull analysis + signup: jscan-agent.up.railway.app"
    return msg

def run_marketing(analyses):
    print(f"[{datetime.now()}] Running marketing agent...")
    if not analyses:
        print("  No analyses to market")
        return

    # Discord - full daily summary (free)
    discord_msg = generate_discord_post(analyses)
    if discord_msg:
        post_to_discord(discord_msg)

    # X - 1 link post
    link_tweet = generate_marketing_post(analyses, with_link=True)
    if link_tweet:
        post_to_x(link_tweet)
        time.sleep(60)

    # X - 4 plain text posts spaced 15 mins apart
    for i in range(4):
        plain_tweet = generate_marketing_post(analyses, with_link=False)
        if plain_tweet:
            post_to_x(plain_tweet)
            if i < 3:
                time.sleep(900)

    print(f"[{datetime.now()}] Marketing done.")

# ─── FLASK SIGNUP PAGE ────────────────────────────────────
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
    data = request.json
    email = data.get("email", "").strip()
    stocks = data.get("stocks", [])
    if not email or not stocks:
        return jsonify({"success": False, "error": "Missing email or stocks"})
    try:
        conn = sqlite3.connect("agent.db")
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO subscribers (email, stocks) VALUES (?, ?)",
                  (email, json.dumps(stocks)))
        conn.commit()
        conn.close()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route("/run", methods=["POST"])
def trigger_run():
    analyses = run_agent(force=True)
    return jsonify({"success": True, "analyzed": len(analyses)})

@app.route("/status")
def status():
    conn = sqlite3.connect("agent.db")
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
def dashboard():
    # Get all data
    conn = sqlite3.connect("agent.db")
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

    # Best and worst calls
    scored = [c for c in calls if c["change_pct"] is not None]
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

    # Build track record section
    tr_html = ""
    if track_record:
        for flag, stats in track_record.items():
            fc = flag_color(flag)
            bar_width = stats["accuracy"]
            tr_html += f"""
            <div style="margin-bottom:16px">
                <div style="display:flex;justify-content:space-between;margin-bottom:6px">
                    <span style="color:{fc};font-weight:700">{flag}</span>
                    <span style="color:#fff;font-weight:600">{stats['accuracy']}% accurate</span>
                </div>
                <div style="background:#1a1a1a;border-radius:4px;height:8px;overflow:hidden">
                    <div style="background:{fc};height:100%;width:{bar_width}%;transition:width .5s"></div>
                </div>
                <div style="color:#444;font-size:.75em;margin-top:4px">{stats['correct']} correct out of {stats['total']} calls (7-day)</div>
            </div>"""
    else:
        tr_html = '<div style="color:#444;font-size:.88em">No track record yet — needs 7 days of data</div>'

    # Build calls table
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
            <td style="padding:10px 16px;color:{fc};font-weight:700">{call['flag']}</td>
            <td style="padding:10px 16px;color:#aaa">${call['price']}</td>
            <td style="padding:10px 16px;color:{chg_color};font-weight:600">{chg_str}</td>
            <td style="padding:10px 16px">{outcome_badge(call['outcome'])}</td>
            <td style="padding:10px 16px;color:#555;font-size:.8em;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{call['thesis'] or '—'}</td>
        </tr>"""

    # Positions
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
.run-btn{{background:#00ff88;color:#000;border:none;padding:10px 20px;border-radius:8px;font-weight:700;cursor:pointer;font-family:inherit;font-size:.88em;transition:opacity .2s}}
.run-btn:hover{{opacity:.85}}
.run-btn:disabled{{opacity:.5;cursor:not-allowed}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">J<span>SCAN</span> <span style="color:#555;font-size:.65em;font-weight:400;margin-left:8px">Agent Dashboard</span></div>
  <div class="nav">
    <a href="/">Signup</a>
    <a href="https://jscan-production.up.railway.app" target="_blank">JSCAN ↗</a>
    <button class="run-btn" id="run-btn" onclick="triggerRun()">▶ Run Now</button>
  </div>
</div>
<div class="container">

  <!-- Stats -->
  <div class="grid">
    <div class="stat-card">
      <div class="stat-label">Portfolio Value</div>
      <div class="stat-value">${float(portfolio_val):,.0f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Cash</div>
      <div class="stat-value" style="color:#e0e0e0">${float(cash):,.0f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Subscribers</div>
      <div class="stat-value" style="color:#e0e0e0">{subs}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Total Calls</div>
      <div class="stat-value" style="color:#e0e0e0">{len(calls)}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Scored Calls</div>
      <div class="stat-value" style="color:#e0e0e0">{len(scored)}</div>
    </div>
  </div>

  <!-- Track Record -->
  <div class="section">
    <div class="section-header">🎯 Track Record (7-day accuracy)</div>
    <div class="section-body">{tr_html}</div>
  </div>

  <!-- Best/Worst -->
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

  <!-- Positions -->
  <div class="section">
    <div class="section-header">📈 Paper Positions</div>
    <table>
      <tr><th>Symbol</th><th>Quantity</th><th>Avg Cost</th><th>Current</th><th>P&L</th></tr>
      {pos_html}
    </table>
  </div>

  <!-- Recent Calls -->
  <div class="section">
    <div class="section-header">📋 Recent Calls (last 50)</div>
    <div style="overflow-x:auto">
      <table>
        <tr><th>Symbol</th><th>Date</th><th>Flag</th><th>Price</th><th>1d Change</th><th>Outcome</th><th>Thesis</th></tr>
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
  fetch('/run',{{method:'POST'}})
    .then(function(r){{return r.json();}})
    .then(function(d){{
      btn.textContent='✓ Done — '+d.analyzed+' stocks';
      setTimeout(function(){{location.reload();}},2000);
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
    print("Signup page: http://127.0.0.1:5001")
    print("Trigger run: POST http://127.0.0.1:5001/run")
    print("Status: http://127.0.0.1:5001/status")

    # Schedule daily run at 8am
    schedule.every().day.at("15:00").do(run_agent)  # 8am PDT = 15:00 UTC

    import threading
    def run_schedule():
        while True:
            schedule.run_pending()
            time.sleep(60)
    threading.Thread(target=run_schedule, daemon=True).start()

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
