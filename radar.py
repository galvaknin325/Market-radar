"""
Market Radar — v1
Every run: collect world-news headlines -> filter seen -> Gemini analyses which
events may move US stocks -> Telegram alert for important ones -> log alert with
price -> after 24h check what actually happened.

Secrets (GitHub > Settings > Secrets and variables > Actions):
  GEMINI_API_KEY       – key from aistudio.google.com
  TELEGRAM_BOT_TOKEN   – token from @BotFather
Optional:
  TELEGRAM_CHAT_ID     – auto-detected if you messaged the bot recently
  MIN_SEVERITY         – default 6 (1–10)
  GEMINI_MODEL         – force a specific model name
Run locally without sending:  python radar.py --dry-run
"""

import html
import hashlib
import json
import os
import re
import sys
import time
import datetime as dt
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests

# ---------------------------------------------------------------- settings
GEMINI_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
MIN_SEVERITY = int(os.environ.get("MIN_SEVERITY", "6") or 6)
FORCED_MODEL = os.environ.get("GEMINI_MODEL", "").strip()
DRY_RUN = "--dry-run" in sys.argv
MANUAL_RUN = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"

STATE_FILE = "state.json"
ALERTS_FILE = "alerts.json"
MAX_AGE_HOURS = 3          # ignore headlines older than this
MAX_HEADLINES = 120        # per run, sent to the AI in one request
UA = {"User-Agent": "Mozilla/5.0 (MarketRadar/1.0)"}


def gnews(q):
    """Google News RSS search, last 2 hours."""
    from urllib.parse import quote_plus
    return ("https://news.google.com/rss/search?q=" + quote_plus(q + " when:2h")
            + "&hl=en-US&gl=US&ceid=US:en")


FEEDS = [
    ("BBC World", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("BBC Business", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml"),
    ("CNBC World", "https://www.cnbc.com/id/100727362/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("GDACS disasters", "https://www.gdacs.org/xml/rss.xml"),
    ("Google News: geopolitics", gnews("sanctions OR ceasefire OR invasion OR missile OR embargo OR blockade")),
    ("Google News: trade & policy", gnews("tariffs OR \"export controls\" OR \"trade deal\" OR regulation OR election")),
    ("Google News: central banks", gnews("\"Federal Reserve\" OR ECB OR \"interest rate\" OR inflation OR recession")),
    ("Google News: commodities", gnews("OPEC OR \"oil supply\" OR \"gas supply\" OR wheat OR copper OR \"port strike\" OR shipping")),
    ("Google News: climate & disasters", gnews("hurricane OR earthquake OR drought OR flood OR wildfire OR heatwave")),
    ("Google News: health & cyber", gnews("outbreak OR pandemic OR cyberattack OR \"chip ban\"")),
    ("Google News: defense contracts", gnews("\"defense contract\" OR \"Pentagon awards\" OR \"arms sale\" OR \"State Department approves\" OR \"foreign military sale\"")),
    ("Google News: Israeli defense", gnews("Rafael OR \"Israel Aerospace Industries\" OR Elbit OR \"Iron Dome\" OR \"Arrow 3\" OR \"David's Sling\"")),
    ("Google News: big deals", gnews("\"billion contract\" OR \"billion deal\" OR \"infrastructure deal\" OR \"procurement\"")),
    ("Defense News", "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"),
    ("Times of Israel", "https://www.timesofisrael.com/feed/"),
]

# Starting point for the AI — it may name any US-listed ticker.
WATCHLIST = {
    "SPY": "S&P 500", "QQQ": "Nasdaq 100", "XLE": "energy sector", "XOM": "Exxon",
    "CVX": "Chevron", "USO": "crude oil", "UNG": "natural gas", "GLD": "gold",
    "LMT": "Lockheed Martin", "RTX": "RTX", "NOC": "Northrop", "DAL": "Delta",
    "UAL": "United Airlines", "ZIM": "ZIM shipping", "FDX": "FedEx",
    "TSM": "TSMC", "NVDA": "Nvidia", "ASML": "ASML", "WEAT": "wheat",
    "CORN": "corn", "FCX": "copper/Freeport", "TLT": "long US bonds",
    "KRE": "regional banks", "ALL": "Allstate insurance", "TRV": "Travelers insurance",
    "MRNA": "Moderna", "CRWD": "CrowdStrike", "EEM": "emerging markets", "FXI": "China large caps",
    "ESLT": "Elbit Systems (Israel)", "BA": "Boeing", "GD": "General Dynamics", "LHX": "L3Harris",
    "HII": "Huntington Ingalls (shipbuilding)", "KTOS": "Kratos (drones)", "AVAV": "AeroVironment (drones)",
    "PLTR": "Palantir (defense software)", "ITA": "US aerospace & defense ETF", "XAR": "aerospace & defense ETF",
    "EIS": "Israel ETF",
    # world markets (US-listed country ETFs)
    "EWJ": "Japan", "EWG": "Germany", "EWU": "UK", "EWQ": "France", "EWI": "Italy", "EWC": "Canada",
    "EWA": "Australia", "EWZ": "Brazil", "EWW": "Mexico", "INDA": "India", "EWY": "South Korea",
    "EWT": "Taiwan", "KSA": "Saudi Arabia", "TUR": "Turkey", "EZA": "South Africa", "EWS": "Singapore",
    "VGK": "Europe", "ASHR": "China A-shares",
    # sectors
    "XLF": "financials", "XLK": "technology", "XLV": "health care", "XLI": "industrials", "XLU": "utilities",
    "XLB": "materials", "XLP": "consumer staples", "XLY": "consumer discretionary", "SMH": "semiconductors",
    "JETS": "airlines ETF", "ICLN": "clean energy", "URA": "uranium", "LIT": "lithium",
    # commodities, currencies, crypto, credit
    "SLV": "silver", "CPER": "copper", "SOYB": "soybeans", "DBA": "agriculture", "BDRY": "dry bulk shipping",
    "UUP": "US dollar", "FXE": "euro", "FXY": "Japanese yen", "IBIT": "bitcoin ETF", "HYG": "high-yield bonds",
    # Tel Aviv Stock Exchange (yfinance suffix .TA)
    "ESLT.TA": "Elbit on TASE", "TEVA.TA": "Teva on TASE", "LUMI.TA": "Bank Leumi", "POLI.TA": "Bank Hapoalim",
    "ICL.TA": "ICL chemicals", "NVMI.TA": "Nova", "TSEM.TA": "Tower Semiconductor", "ENLT.TA": "Enlight energy",
}


# ---------------------------------------------------------------- storage
def load(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


def log(*a):
    print(dt.datetime.utcnow().strftime("%H:%M:%S"), *a, flush=True)


# ---------------------------------------------------------------- 1. collect
def parse_date(text):
    if not text:
        return None
    try:
        d = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        try:
            d = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d


def strip_tags(s):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(s or ""))).strip()


def fetch_feed(name, url):
    items = []
    try:
        r = requests.get(url, headers=UA, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
    except Exception as e:
        log(f"feed failed: {name}: {e}")
        return items
    # RSS <item> or Atom <entry>, ignore namespaces
    for el in root.iter():
        tag = el.tag.split("}")[-1]
        if tag not in ("item", "entry"):
            continue
        f = {c.tag.split("}")[-1]: c for c in el}
        title = strip_tags(f["title"].text if "title" in f else "")
        link = ""
        if "link" in f:
            link = (f["link"].text or f["link"].get("href") or "").strip()
        def text_of(*names):
            for n in names:
                if n in f and f[n].text:
                    return f[n].text
            return ""
        desc = strip_tags(text_of("description", "summary"))
        published = parse_date(text_of("pubDate", "published", "updated") or None)
        if title:
            items.append({"source": name, "title": title, "link": link,
                          "summary": desc[:300], "published": published})
    return items


def norm_title(t):
    t = re.sub(r"\s+-\s+[^-]{2,40}$", "", t)          # drop " - Publisher" (Google News)
    return re.sub(r"[^a-z0-9 ]", "", t.lower()).strip()


def collect(seen):
    now = dt.datetime.now(dt.timezone.utc)
    fresh, titles = [], set()
    for name, url in FEEDS:
        for it in fetch_feed(name, url):
            key = hashlib.sha1(norm_title(it["title"]).encode()).hexdigest()[:16]
            if key in seen or key in titles:
                continue
            if it["published"] and now - it["published"] > dt.timedelta(hours=MAX_AGE_HOURS):
                continue
            it["key"] = key
            titles.add(key)
            fresh.append(it)
    fresh.sort(key=lambda x: x["published"] or now, reverse=True)
    return fresh[:MAX_HEADLINES]


# ---------------------------------------------------------------- 2. analyse
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"

PROMPT = """You are a senior macro / event-driven analyst for US equities.
Below are fresh world-news headlines (numbered). Find EVERY event that could
plausibly move US-listed stocks, sectors or ETFs in the coming hours or days,
including small ones (give those a low severity rather than leaving them out):
- geopolitics, wars, security incidents, diplomacy
- politics, elections & regulation
- trade, tariffs, export controls, sanctions (including on specific companies)
- central banks & economic data
- commodities, energy & supply chains, shipping
- climate, weather & natural disasters (hurricanes, droughts, floods, heatwaves, wildfires, earthquakes)
- health & outbreaks, major cyber incidents
- CONTRACTS & DEALS: government and defense contracts, arms sales and their approvals,
  big procurement and infrastructure orders, defense budgets, major cross-border deals.
  When the company in the news is not US-listed (e.g. Rafael or Israel Aerospace Industries,
  which are state-owned), map the impact to US-listed partners, suppliers, competitors or
  sector ETFs (e.g. Rafael co-produces systems with RTX; IAI works with Boeing (BA) on Arrow;
  Elbit Systems trades on Nasdaq as ESLT; defense ETFs ITA / XAR).
IGNORE routine company news (quarterly earnings, CEO changes, product launches),
sports, celebrity, local crime, opinion pieces and anything with no market channel.

Merge headlines about the same event into one event. For each event return:
- ids: list of headline numbers it is based on
- title_he: short title in Hebrew
- summary_he: 1–2 sentences in Hebrew, ONLY facts stated in the headlines
- category: one of geopolitics, policy, trade, macro, commodities, climate, health, cyber, contracts
- severity: 1–10 expected market impact (1–3 = small / single-stock or sector noise,
  4–5 = noticeable for a stock or sector, 6–7 = clearly market-relevant for a sector,
  8 = major multi-sector move likely, 10 = historic shock). Routine statements and
  incremental updates are usually <= 4. A large defense contract is often 3–6 for the winner.
- novelty: "new" if this looks like a new development, "update" if continuation
- impacts: 1–5 items {ticker, direction ("up"/"down"), reason_he (short Hebrew)}.
  Use real tickers: US-listed stocks/ETFs (including country, sector, commodity, currency and
  bitcoin ETFs for non-US markets), or Tel Aviv Stock Exchange stocks with the ".TA" suffix
  (e.g. ESLT.TA) when the effect is on Israeli stocks. Prefer this watchlist when relevant,
  but any valid ticker is allowed:
  WATCHLIST
- confidence: "low", "medium" or "high"
Never give buy/sell advice. Return JSON: {"events": [...]} (empty list if nothing qualifies).

Headlines:
HEADLINES
"""


def gemini_models():
    if FORCED_MODEL:
        return [FORCED_MODEL]
    try:
        r = requests.get(f"{GEMINI_BASE}/models", params={"pageSize": 200},
                         headers={"x-goog-api-key": GEMINI_KEY}, timeout=30)
        r.raise_for_status()
        names = [m["name"].split("/")[-1] for m in r.json().get("models", [])
                 if "generateContent" in m.get("supportedGenerationMethods", [])]
    except Exception as e:
        log("could not list Gemini models:", e)
        names = []
    bad = ("image", "tts", "audio", "live", "embedding", "thinking", "exp", "computer")

    def ver(n):
        return [int(x) for x in re.findall(r"\d+", n)] or [0]

    ok = [n for n in names if "gemini" in n and not any(b in n for b in bad)]
    lite = sorted([n for n in ok if "flash-lite" in n], key=lambda n: ("preview" in n, [-v for v in ver(n)]))
    flash = sorted([n for n in ok if "flash" in n and "lite" not in n], key=lambda n: ("preview" in n, [-v for v in ver(n)]))
    # Flash-Lite first: much larger free daily quota.
    return (lite + flash)[:6] or ["gemini-2.5-flash-lite", "gemini-2.5-flash"]


def analyse(headlines):
    lines = "\n".join(f"{i}. [{h['source']}] {h['title']}" + (f" — {h['summary'][:160]}" if h["summary"] else "")
                      for i, h in enumerate(headlines))
    wl = ", ".join(f"{k} ({v})" for k, v in WATCHLIST.items())
    prompt = PROMPT.replace("WATCHLIST", wl).replace("HEADLINES", lines)
    body = {"contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"}}
    last_err = None
    for model in gemini_models():
        for attempt in range(2):
            try:
                r = requests.post(f"{GEMINI_BASE}/models/{model}:generateContent",
                                  headers={"x-goog-api-key": GEMINI_KEY, "Content-Type": "application/json"},
                                  json=body, timeout=90)
                if r.status_code in (429, 503) and attempt == 0:
                    time.sleep(20)
                    continue
                if r.status_code >= 400:
                    raise RuntimeError(f"{r.status_code} {r.text[:200]}")
                text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
                text = re.sub(r"^```(json)?|```$", "", text.strip()).strip()
                events = json.loads(text).get("events", [])
                log(f"model {model}: {len(events)} events")
                return events, model
            except Exception as e:
                last_err = f"{model}: {e}"
                log("gemini error:", last_err)
                break
    raise RuntimeError(f"all Gemini models failed. Last error: {last_err}")


# ---------------------------------------------------------------- 3. prices
def price_now(ticker):
    try:
        import yfinance as yf
        p = yf.Ticker(ticker).fast_info["last_price"]
        return round(float(p), 4) if p else None
    except Exception:
        return None


# ---------------------------------------------------------------- 4. telegram
def tg(method, **params):
    r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/{method}", json=params, timeout=30)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram {method}: {data}")
    return data["result"]


def find_chat_id(state):
    if TG_CHAT:
        return TG_CHAT
    if state.get("chat_id"):
        return state["chat_id"]
    for upd in reversed(tg("getUpdates")):
        msg = upd.get("message") or upd.get("my_chat_member") or {}
        chat = msg.get("chat")
        if chat:
            state["chat_id"] = str(chat["id"])
            log("found Telegram chat id")
            return state["chat_id"]
    return None


def send(chat_id, text):
    if DRY_RUN:
        print("\n--- TELEGRAM (dry run) ---\n" + text + "\n")
        return
    tg("sendMessage", chat_id=chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True)


CAT_ICON = {"geopolitics": "🌍", "policy": "🏛️", "trade": "🚢", "macro": "🏦",
            "commodities": "⛽", "climate": "🌪️", "health": "🦠", "cyber": "💻", "contracts": "📑"}


def format_alert(ev, headlines):
    e = html.escape
    sev = int(ev.get("severity", 0))
    dot = "🔴" if sev >= 8 else "🟠" if sev >= 6 else "🟡"
    lines = [f"{dot} <b>חומרה {sev}/10 · {CAT_ICON.get(ev.get('category'), '📰')} {e(ev.get('title_he', ''))}</b>",
             e(ev.get("summary_he", "")), ""]
    for imp in ev.get("impacts", [])[:5]:
        arrow = "⬆️" if imp.get("direction") == "up" else "⬇️"
        lines.append(f"{arrow} <b>{e(str(imp.get('ticker', '')).upper())}</b> – {e(imp.get('reason_he', ''))}")
    srcs = [headlines[i] for i in ev.get("ids", []) if isinstance(i, int) and 0 <= i < len(headlines)]
    if srcs:
        lines.append("")
        for s in srcs[:2]:
            lines.append(f'🔗 <a href="{e(s["link"], quote=True)}">{e(s["source"])}</a>')
    conf = {"low": "נמוכה", "medium": "בינונית", "high": "גבוהה"}.get(ev.get("confidence"), "")
    lines.append(f"\n<i>ביטחון: {conf} · מידע בלבד, לא ייעוץ השקעות</i>")
    return "\n".join(lines)


def format_digest(events):
    e = html.escape
    lines = [f"📋 <b>עוד {len(events)} אירועים מהסריקה האחרונה</b>", ""]
    for ev in events[:15]:
        sev = int(ev.get("severity", 0) or 0)
        dot = "🔴" if sev >= 8 else "🟠" if sev >= 6 else "🟡" if sev >= 4 else "⚪"
        tks = " ".join(("⬆️" if i.get("direction") == "up" else "⬇️") + e(str(i.get("ticker", "")).upper())
                       for i in ev.get("impacts", [])[:4])
        upd = " (עדכון)" if ev.get("novelty") == "update" else ""
        lines.append(f"{dot} {sev}/10 {CAT_ICON.get(ev.get('category'), '📰')} {e(ev.get('title_he', ''))}{upd}\n      {tks}")
    if len(events) > 15:
        lines.append(f"…ועוד {len(events) - 15}. הכול מופיע בדשבורד.")
    lines.append("\n<i>מידע בלבד, לא ייעוץ השקעות</i>")
    return "\n".join(lines)


# ---------------------------------------------------------------- 5. evaluate
def evaluate(alerts):
    """24h after an alert, record actual move of each ticker."""
    now = time.time()
    done = 0
    for a in alerts:
        if a.get("evaluated") or now - a["ts"] < 24 * 3600:
            continue
        for imp in a["impacts"]:
            if imp.get("price_at_alert") and imp.get("price_24h") is None:
                p = price_now(imp["ticker"])
                if p:
                    imp["price_24h"] = p
                    chg = (p / imp["price_at_alert"] - 1) * 100
                    imp["change_24h_pct"] = round(chg, 2)
                    imp["hit"] = (chg > 0) == (imp["direction"] == "up")
        a["evaluated"] = True
        done += 1
    if done:
        log(f"evaluated {done} old alerts")


# ---------------------------------------------------------------- main
def main():
    if not GEMINI_KEY or (not TG_TOKEN and not DRY_RUN):
        sys.exit("Missing GEMINI_API_KEY or TELEGRAM_BOT_TOKEN secret.")
    state = load(STATE_FILE, {"seen": []})
    alerts = load(ALERTS_FILE, [])
    seen = set(state.get("seen", []))

    chat_id = None
    if not DRY_RUN:
        chat_id = find_chat_id(state)
        if not chat_id:
            save(STATE_FILE, state)
            sys.exit("No Telegram chat found: open your bot in Telegram, press Start, send 'hi', then run again.")
        if not state.get("welcomed"):
            send(chat_id, "✅ <b>מכ\"ם השוק מחובר!</b>\nמעכשיו אשלח לך כאן התראות על אירועים עולמיים שעלולים להזיז מניות בוול סטריט.")
            state["welcomed"] = True

    evaluate(alerts)

    headlines = collect(seen)
    log(f"{len(headlines)} new headlines")
    sent, model, events, digest = 0, "-", [], []
    if headlines:
        events, model = analyse(headlines)
        for ev in sorted(events, key=lambda x: -int(x.get("severity", 0) or 0)):
            sev = int(ev.get("severity", 0) or 0)
            if not ev.get("impacts"):
                continue
            # Every event is saved and shown on the dashboard. On Telegram, important new
            # events get their own message; everything else goes into one digest per scan.
            if sev >= MIN_SEVERITY and ev.get("novelty") != "update" or sev >= 8:
                send(chat_id, format_alert(ev, headlines))
            else:
                digest.append(ev)
            sent += 1
            alerts.append({
                "ts": int(time.time()),
                "time": dt.datetime.utcnow().isoformat(timespec="minutes") + "Z",
                "title": ev.get("title_he"), "summary": ev.get("summary_he"),
                "category": ev.get("category"),
                "severity": sev, "confidence": ev.get("confidence"), "novelty": ev.get("novelty"),
                "sources": [{"name": headlines[i]["source"], "url": headlines[i]["link"]}
                            for i in ev.get("ids", []) if isinstance(i, int) and 0 <= i < len(headlines)],
                "impacts": [{"ticker": str(i.get("ticker", "")).upper(), "direction": i.get("direction"),
                             "reason": i.get("reason_he"),
                             "price_at_alert": price_now(str(i.get("ticker", "")).upper())}
                            for i in ev.get("impacts", [])[:5]],
            })
        # mark seen only after a successful analysis (keep order: newest last)
        state["seen"] = state.get("seen", []) + [h["key"] for h in headlines]

    if digest:
        send(chat_id, format_digest(digest))

    if MANUAL_RUN and chat_id:
        send(chat_id, f"🛰️ בדיקה ידנית הסתיימה: {len(headlines)} כותרות חדשות, "
                      f"{sent} התראות נשמרו (מהן {len(digest)} בסיכום המקובץ). מודל: {model}")

    state["seen"] = state.get("seen", [])[-3000:]
    state["last_run"] = dt.datetime.utcnow().isoformat(timespec="minutes") + "Z"
    save(STATE_FILE, state)
    save(ALERTS_FILE, alerts[-2000:])
    log(f"done: {sent} alerts sent")


if __name__ == "__main__":
    main()
