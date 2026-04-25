import os
import time
import json
import sqlite3
import requests
import schedule
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request, jsonify
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

def place_paper_trade(symbol, side, qty):
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
            SELECT outcome, COUNT(*) FROM call_results cr
            JOIN calls ca ON cr.call_id = ca.id
            WHERE ca.flag = ? AND cr.days_later = 7
            GROUP BY outcome
        """, (flag,))
        rows = dict(c.fetchall())
        total = sum(rows.values())
        if total > 0:
            correct = rows.get("correct", 0)
            accuracy = round((correct / total) * 100, 1)
            stats[flag] = {"accuracy": accuracy, "total": total, "correct": correct}

    conn.close()
    return stats

def analyze_stock(symbol, price_data, news, previous_calls, positions, track_record=None):
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    name = STOCK_NAMES.get(symbol, symbol)

    prev_call_text = ""
    for pc in previous_calls:
        if pc["symbol"] == symbol:
            price_then = pc["price"]
            price_now = price_data.get("price", 0)
            if price_then and price_now:
                delta = round(((price_now - price_then) / price_then) * 100, 2)
                prev_call_text = f"\nPREVIOUS CALL ({pc['date']}): {pc['flag']} at ${price_then}. Since then: {delta:+.2f}%. Thesis was: {pc['thesis']}"

    position_text = ""
    if symbol in positions:
        p = positions[symbol]
        position_text = f"\nCURRENT PAPER POSITION: {p.get('qty')} shares, avg cost ${p.get('avg_entry_price')}, unrealized P&L: ${p.get('unrealized_pl')}"

    news_text = "\n".join([f"- {n['title']} ({n['source']}, {n['published']})" for n in news]) or "No recent news found."

    track_text = ""
    if track_record:
        parts = []
        for flag, stats in track_record.items():
            if stats.get("total", 0) >= 5:
                parts.append(f"{flag}: {stats['accuracy']}% accurate over {stats['total']} calls (7-day)")
        if parts:
            track_text = "\nYOUR HISTORICAL ACCURACY:\n" + "\n".join(parts) + "\nAdjust your confidence accordingly."

    prompt = f"""You are an elite stock analyst. Analyze {name} ({symbol}) and provide a concise daily briefing.

PRICE DATA:
- Current: ${price_data.get('price')}
- Change: {price_data.get('change_pct')}%
- High: ${price_data.get('high')} | Low: ${price_data.get('low')}
- Volume: {price_data.get('volume'):,}
{prev_call_text}
{position_text}
{track_text}

RECENT NEWS:
{news_text}

Provide your analysis in this EXACT format:

FLAG: [GREEN / YELLOW / RED]
BULL CASE: [1-2 sentences]
BEAR CASE: [1-2 sentences]
VERDICT: [1-2 sentences with your call]
ACTION: [BUY / HOLD / SELL / WATCH] [optional: qty for paper trade e.g. "BUY 5 shares"]

Be direct, specific, and confident. No fluff."""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text

def parse_analysis(text):
    lines = text.strip().split("\n")
    result = {"flag": "YELLOW", "bull": "", "bear": "", "verdict": "", "action": "WATCH", "raw": text}
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

# ─── MAIN AGENT RUN ───────────────────────────────────────
def run_agent(symbols=None):
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
    if track_record:
        for flag, stats in track_record.items():
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
            raw = analyze_stock(sym, pd, news, previous_calls, positions, track_record)
            parsed = parse_analysis(raw)
            parsed["symbol"] = sym
            parsed["name"] = STOCK_NAMES.get(sym, sym)
            parsed["price"] = pd["price"]
            parsed["change_pct"] = pd["change_pct"]
            analyses.append(parsed)
            save_call(sym, parsed["flag"], pd["price"], parsed["verdict"])

            # Paper trade if Claude says BUY or SELL
            action = parsed["action"].upper()
            if "BUY" in action:
                qty = 1
                for word in action.split():
                    if word.isdigit():
                        qty = int(word)
                        break
                result = place_paper_trade(sym, "buy", qty)
                print(f"    Paper BUY {qty}x {sym}: {result.get('status', result.get('error', 'unknown'))}")
            elif "SELL" in action and sym in positions:
                qty = positions[sym].get("qty", 1)
                result = place_paper_trade(sym, "sell", qty)
                print(f"    Paper SELL {qty}x {sym}: {result.get('status', result.get('error', 'unknown'))}")

        except Exception as e:
            print(f"  Error analyzing {sym}: {e}")
        time.sleep(0.5)  # avoid rate limiting

    # Sort: green first, then yellow, then red
    order = {"GREEN": 0, "YELLOW": 1, "RED": 2}
    analyses.sort(key=lambda x: order.get(x["flag"], 1))

    # Send to all active subscribers
    conn = sqlite3.connect("agent.db")
    c = conn.cursor()
    c.execute("SELECT email, stocks FROM subscribers WHERE active=1")
    subscribers = c.fetchall()
    conn.close()

    email_html = build_email(analyses, account)
    sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_KEY)

    for email, stocks_json in subscribers:
        try:
            user_stocks = json.loads(stocks_json)
            user_analyses = [a for a in analyses if a["symbol"] in user_stocks] if user_stocks != ["ALL"] else analyses
            if not user_analyses:
                continue
            user_html = build_email(user_analyses, account)
            message = Mail(
                from_email=FROM_EMAIL,
                to_emails=email,
                subject=f"📊 JSCAN Daily Brief — {datetime.now().strftime('%b %d')} | {len([a for a in user_analyses if a['flag']=='GREEN'])}🟢 {len([a for a in user_analyses if a['flag']=='RED'])}🔴",
                html_content=user_html
            )
            sg.send(message)
            print(f"  Email sent to {email}")
        except Exception as e:
            print(f"  Email error for {email}: {e}")

    print(f"[{datetime.now()}] Agent done. Analyzed {len(analyses)} stocks.")
    return analyses

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
    analyses = run_agent()
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

if __name__ == "__main__":
    init_db()
    print("JSCAN Agent starting...")
    print(f"Watching {len(WATCHLIST)} stocks")
    print("Signup page: http://127.0.0.1:5001")
    print("Trigger run: POST http://127.0.0.1:5001/run")
    print("Status: http://127.0.0.1:5001/status")

    # Schedule daily run at 8am
    schedule.every().day.at("08:00").do(run_agent)

    import threading
    def run_schedule():
        while True:
            schedule.run_pending()
            time.sleep(60)
    threading.Thread(target=run_schedule, daemon=True).start()

    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)