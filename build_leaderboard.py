#!/usr/bin/env python3
"""
build_leaderboard.py  —  Congressional Trading ELO pipeline (100% free, runs locally)

What it does
------------
1. Downloads House + Senate disclosed trades from the free Stock Watcher datasets.
2. Pulls EOD prices for every traded ticker + the S&P 500 (^GSPC) via yfinance.
3. Scores every trade as a head-to-head "match" vs the S&P over a holding window,
   then replays a market-anchored ELO chronologically (same math as the prototype).
4. (Optional) tags each member's party from the free unitedstates/congress-legislators list.
5. Writes  data.js  next to congress_elo_leaderboard.html  ->  open the HTML, done.

Run it
------
    pip install yfinance pandas requests
    python build_leaderboard.py

Then open congress_elo_leaderboard.html in your browser. The badge turns green
("Live data") when it picks up data.js. Re-run any time to refresh.

Notes
-----
* Everything here is free and keyless. yfinance needs internet (your machine has it).
* Amounts are disclosed as ranges (STOCK Act), so every trade is equal-weighted.
  Flip WEIGHT_BY_AMOUNT = True to weight the ELO update by trade size instead.
* Tune the knobs in the CONFIG block below.
"""

import json, sys, time, math, datetime as dt
from pathlib import Path

# ----------------------------- CONFIG -----------------------------
HOLDING_DAYS   = 30      # trading days held before measuring the trade's return
K              = 32      # ELO sensitivity
MOV_CAP        = 2.0     # max margin-of-victory multiplier (lower = steadier, less jackpot)
MARKET_ELO     = 1500    # fixed rating of the S&P 500 opponent
TIE_BAND_PCT   = 0.5     # |excess| below this = a tie
MIN_TRADES     = 1       # members with fewer scored trades are dropped from output
START_DATE     = "2012-01-01"   # kadoa history starts ~2012 — include all of it

# Time-decay weights for a trade's edge: an abnormal move that shows up within a
# month counts fully; within a year, less; only over the full holding period
# (years), least — but never zero. This makes FAST correctness (the insider-
# trading tell) dominate the ELO while slow buy-and-hold still counts a little.
W_30D   = 1.00   # edge visible within ~a month
W_1Y    = 0.40   # visible within ~a year
W_SINCE = 0.10   # only over the full (multi-year) holding period
WEIGHT_BY_AMOUNT = False
TAG_PARTY      = True    # look up party from congress-legislators (best-effort)
OUT_JS         = Path(__file__).with_name("data.js")
OUT_JSON       = Path(__file__).with_name("data.json")
CACHE_DIR      = Path(__file__).with_name("_cache"); CACHE_DIR.mkdir(exist_ok=True)

# Data source — kadoa-org/congress-trading-monitor: a daily-updated, keyless,
# open dataset that aggregates the House Clerk, Senate eFD, and OGE disclosures.
#   trades.json — every disclosed transaction (filer_id, ticker, transaction_type,
#                 transaction_date, amount_range_label, ...)
#   filers.json — filer directory (id -> full_name, chamber, branch, party)
KADOA_FILERS_URL = "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/filers.json"
# Per-filer files hold each member's FULL trade history, with kadoa's own
# excess-vs-market return already computed per trade ({id} = a filer id).
KADOA_FILER_URL  = "https://raw.githubusercontent.com/kadoa-org/congress-trading-monitor/main/public/data/filer/{id}.json"
LEG_URL          = "https://unitedstates.github.io/congress-legislators/legislators-current.json"

# ----------------------------- deps -------------------------------
try:
    import requests, pandas as pd, yfinance as yf
except ImportError as e:
    sys.exit(f"Missing dependency: {e}. Run:  pip install yfinance pandas requests")


def log(*a): print(*a, flush=True)


# ------------------------- load trades ----------------------------
def fetch_json(url, cache_name, max_age_h=24):
    cache = CACHE_DIR / cache_name
    if cache.exists() and (time.time() - cache.stat().st_mtime) < max_age_h * 3600:
        return json.loads(cache.read_text())
    log(f"  downloading {url} ...")
    r = requests.get(url, timeout=60, headers={"User-Agent": "elo-leaderboard/1.0"})
    r.raise_for_status()
    data = r.json()
    cache.write_text(json.dumps(data))
    return data


def fetch_json_any(urls, cache_name, max_age_h=24):
    """Try each candidate URL in order; return JSON from the first that works.
    Returns [] if every source fails (caller decides whether that's fatal)."""
    cache = CACHE_DIR / cache_name
    if cache.exists() and (time.time() - cache.stat().st_mtime) < max_age_h * 3600:
        try:
            return json.loads(cache.read_text())
        except Exception:
            pass
    for url in urls:
        try:
            log(f"  downloading {url} ...")
            r = requests.get(url, timeout=90, headers={"User-Agent": "elo-leaderboard/1.0"})
            r.raise_for_status()
            data = r.json()
            if data:
                cache.write_text(json.dumps(data))
                return data
            log("    (empty response, trying next source)")
        except Exception as e:
            host = url.split("/")[2] if "//" in url else url
            log(f"    x {host}: {e}")
    return []


def norm_type(t):
    t = (t or "").lower()
    if "purchase" in t: return "buy"
    if "sale" in t or "sell" in t: return "sell"
    return None  # exchange / receive / other -> skip


def parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try: return dt.datetime.strptime(s.strip(), fmt).date()
        except ValueError: pass
    return None


def clean_ticker(tk):
    if not tk: return None
    tk = tk.strip().upper()
    if tk in ("", "--", "N/A", "NONE"): return None
    if any(c in tk for c in " /."): return None   # skip odd/non-equity tickers
    return tk


def sval(v):
    """Coerce a possibly-NaN/None cell (pandas or JSON) to a clean string."""
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def fnum(v):
    """Coerce to float, or None if it isn't a usable number."""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def extract_name(row):
    """Member name across schemas: a single field (representative / senator / name),
    first_name + last_name (Senate JSON), or an 'office' string like
    'Doe, Jane (Senator)'."""
    for k in ("full_name", "representative", "senator", "name", "member"):
        v = sval(row.get(k))
        if v:
            return v
    fn, ln = sval(row.get("first_name")), sval(row.get("last_name"))
    if fn or ln:
        return f"{fn} {ln}".strip()
    off = sval(row.get("office"))
    if off:
        base = off.split("(")[0].strip()            # drop "(Senator)" suffix
        if "," in base:                             # "Last, First" -> "First Last"
            last, first = base.split(",", 1)
            return f"{first.strip()} {last.strip()}".strip()
        return base
    return ""


def normalize_rows(rows, chamber):
    """Turn raw House/Senate records into the common trade shape."""
    out = []
    for row in rows:
        tk    = clean_ticker(sval(row.get("ticker")))
        side  = norm_type(sval(row.get("type")))
        tdate = parse_date(sval(row.get("transaction_date")))
        if not (tk and side and tdate): continue
        if tdate.isoformat() < START_DATE: continue
        name = extract_name(row).replace("Hon. ", "").strip()
        if not name: continue
        out.append({"name": name, "chamber": chamber, "ticker": tk,
                    "side": side, "date": tdate, "amount": sval(row.get("amount"))})
    return out


def load_trades():
    # 1. Enumerate every filer (House, Senate, and executive branch — current & former)
    log("[trades] loading filer directory")
    try:
        filers = fetch_json(KADOA_FILERS_URL, "kadoa_filers.json")
    except Exception as e:
        sys.exit(f"Could not load filer directory: {e}")
    roster = []
    for f in filers:
        fid = sval(f.get("id"))
        if not fid:
            continue
        chamber = sval(f.get("chamber")).lower()
        branch  = sval(f.get("branch")).lower()
        if chamber in ("house", "senate"):
            label = chamber.capitalize()
        elif branch == "executive" or fid.startswith("oge_"):
            label = "Executive"
        else:
            pre = fid.split("_", 1)[0]
            label = {"house": "House", "senate": "Senate",
                     "oge": "Executive"}.get(pre, pre.capitalize() or "Other")
        roster.append((fid, f, label))
    log(f"[trades] {len(roster)} filers to pull (House + Senate + Executive, current & former)")

    # 2. Pull each filer's full history; score off kadoa's own excess-vs-market return
    trades = []
    for i, (fid, f, label) in enumerate(roster, 1):
        name  = extract_name(f)
        party = sval(f.get("party"))
        try:
            doc = fetch_json(KADOA_FILER_URL.format(id=fid), f"filer_{fid}.json")
        except Exception as e:
            log(f"  x {fid}: {e}")
            continue
        rows = doc.get("trades", []) if isinstance(doc, dict) else (doc or [])
        for row in rows:
            side  = norm_type(sval(row.get("transaction_type")) or sval(row.get("type")))
            tdate = parse_date(sval(row.get("transaction_date")) or sval(row.get("date")))
            if not (side and tdate): continue
            if tdate.isoformat() < START_DATE: continue
            # kadoa's return snapshots for this trade (percent): ~30-day, ~1-year,
            # and since the trade to today. Keep whichever are available.
            r30    = fnum(row.get("ret_30d"))
            r1y    = fnum(row.get("ret_1y"))
            rsince = fnum(row.get("ret_since"))
            if r30 is None and r1y is None and rsince is None:
                continue
            trades.append({
                "name": name or extract_name(row), "chamber": label,
                "party": party, "ticker": clean_ticker(sval(row.get("ticker"))) or "",
                "side": side, "date": tdate,
                "amount": sval(row.get("amount_range_label")) or sval(row.get("amount")),
                "ret30": r30, "ret1y": r1y, "retsince": rsince,
            })
        if i % 40 == 0:
            log(f"  {i}/{len(roster)} filers · {len(trades)} trades so far")
    log(f"[trades] usable trades: {len(trades)}")
    return trades


# ------------------------- prices ---------------------------------
def download_prices(tickers, start, end):
    """Return {ticker: pandas Series of adjusted close indexed by date}."""
    prices = {}
    tickers = sorted(set(tickers) | {"^GSPC"})
    log(f"[prices] downloading {len(tickers)} symbols via yfinance ...")
    # batch in chunks to be polite / robust
    CHUNK = 40
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i+CHUNK]
        try:
            df = yf.download(chunk, start=start, end=end, auto_adjust=True,
                             progress=False, threads=True)["Close"]
        except Exception as e:
            log(f"  !! chunk failed ({e}); retrying one-by-one")
            df = None
        if df is None:
            for t in chunk:
                try:
                    s = yf.download(t, start=start, end=end, auto_adjust=True,
                                    progress=False)["Close"]
                    prices[t] = s.dropna()
                except Exception:
                    pass
            continue
        if isinstance(df, pd.Series):        # single ticker case
            prices[chunk[0]] = df.dropna()
        else:
            for t in df.columns:
                prices[t] = df[t].dropna()
        log(f"  {min(i+CHUNK,len(tickers))}/{len(tickers)}")
    return prices


def spx_window_return(spx, entry_date, days):
    """S&P 500 % return from entry_date over roughly `days` calendar days."""
    if spx is None or len(spx) == 0:
        return None
    idx = spx.index
    p0 = idx.searchsorted(pd.Timestamp(entry_date))
    if p0 >= len(spx):
        return None
    p1 = min(idx.searchsorted(pd.Timestamp(entry_date) + pd.Timedelta(days=days)), len(spx) - 1)
    if p1 <= p0:
        return None
    a, b = float(spx.iloc[p0]), float(spx.iloc[p1])
    return None if a <= 0 else (b / a - 1.0) * 100.0


def ret_over_window(series, entry_date, hold):
    """Return (excess-input) return of `series` from first trading day >= entry_date
       to `hold` trading days later. None if insufficient data."""
    if series is None or len(series) == 0: return None, None, None
    idx = series.index
    # first position on/after entry_date
    pos = idx.searchsorted(pd.Timestamp(entry_date))
    if pos >= len(series): return None, None, None
    exit_pos = min(pos + hold, len(series) - 1)
    if exit_pos <= pos: return None, None, None
    p0, p1 = float(series.iloc[pos]), float(series.iloc[exit_pos])
    if p0 <= 0: return None, None, None
    return (p1 / p0 - 1.0), idx[pos], idx[exit_pos]


# --------------------------- party --------------------------------
def load_party_map():
    if not TAG_PARTY: return {}
    try:
        legs = fetch_json(LEG_URL, "legislators.json", max_age_h=24*30)
    except Exception as e:
        log(f"[party] skip ({e})"); return {}
    m = {}
    for l in legs:
        nm = l.get("name", {})
        party = (l.get("terms", [{}])[-1].get("party") or "")[:1]  # D/R/I
        last = (nm.get("last") or "").lower()
        full = f"{nm.get('first','')} {nm.get('last','')}".lower().strip()
        if last: m.setdefault(last, party)
        if full: m[full] = party
    return m


def match_party(name, pmap):
    n = name.lower()
    if n in pmap: return pmap[n]
    last = n.split()[-1] if n.split() else n
    return pmap.get(last, "")


# ---------------------------- ELO ---------------------------------
def build():
    trades = load_trades()
    if not trades:
        sys.exit("No trades loaded — check the kadoa filer URLs in CONFIG.")

    pmap = load_party_map()

    # Benchmark: a single download of the S&P 500 (one symbol — no throttling).
    start = (min(t["date"] for t in trades) - dt.timedelta(days=5)).isoformat()
    end   = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    try:
        spx = yf.download("^GSPC", start=start, end=end, auto_adjust=True,
                          progress=False)["Close"].dropna()
    except Exception as e:
        sys.exit(f"Could not fetch S&P 500 benchmark: {e}")
    if isinstance(spx, pd.DataFrame):
        spx = spx.iloc[:, 0]

    # Blend each trade's return snapshots into ONE time-decayed excess over the S&P.
    # Each horizon's excess (return minus the S&P's move over that same window) is
    # weighted by how soon it appears — fast edges dominate, slow ones count a little.
    today = dt.date.today()
    scored = []
    for t in sorted(trades, key=lambda x: x["date"]):
        comps = []  # (weight, excess in percentage points)
        if t["ret30"] is not None:
            s = spx_window_return(spx, t["date"], 30)
            if s is not None: comps.append((W_30D, t["ret30"] - s))
        if t["ret1y"] is not None:
            s = spx_window_return(spx, t["date"], 365)
            if s is not None: comps.append((W_1Y, t["ret1y"] - s))
        if t["retsince"] is not None:
            horizon = max((today - t["date"]).days, 1)
            s = spx_window_return(spx, t["date"], horizon)
            if s is not None: comps.append((W_SINCE, t["retsince"] - s))
        if not comps: continue
        wsum = sum(w for w, _ in comps)
        scored.append({**t, "excess": sum(w * e for w, e in comps) / wsum})
    log(f"[elo] scored trades: {len(scored)}")

    members = {}
    def M(name, chamber, party=""):
        key = (name, chamber)
        if key not in members:
            members[key] = {"name": name, "chamber": chamber,
                            "party": party or match_party(name, pmap),
                            "elo": 1500.0, "wins": 0, "losses": 0, "ties": 0,
                            "matches": 0, "sumExcess": 0.0,
                            "nb": 0, "bw": 0, "bsum": 0.0,   # buys:  count, wins, sum eff
                            "ns": 0, "sw": 0, "ssum": 0.0}   # sells: count, wins, sum eff
        return members[key]

    for t in scored:
        m = M(t["name"], t["chamber"], t.get("party", ""))
        eff = t["excess"] if t["side"] == "buy" else -t["excess"]  # sells win when stock lags
        S = 1.0 if eff > TIE_BAND_PCT else (0.0 if eff < -TIE_BAND_PCT else 0.5)
        E = 1.0 / (1.0 + 10 ** ((MARKET_ELO - m["elo"]) / 400.0))
        mov = min(1.0 + math.log(1 + abs(eff)), MOV_CAP)   # margin-of-victory multiplier
        m["elo"] += K * mov * (S - E)
        m["wins"]   += S == 1.0
        m["losses"] += S == 0.0
        m["ties"]   += S == 0.5
        m["matches"] += 1
        m["sumExcess"] += eff          # direction-adjusted, so avg matches win rate
        if t["side"] == "buy":
            m["nb"] += 1; m["bw"] += S == 1.0; m["bsum"] += eff
        else:
            m["ns"] += 1; m["sw"] += S == 1.0; m["ssum"] += eff

    out = []
    for m in members.values():
        if m["matches"] < MIN_TRADES: continue
        out.append({
            "name": m["name"], "party": m["party"], "chamber": m["chamber"],
            "elo": round(m["elo"]),
            "matches": m["matches"], "wins": int(m["wins"]),
            "losses": int(m["losses"]), "ties": int(m["ties"]),
            "winrate": round(m["wins"] / m["matches"] * 100, 1),
            "avgexcess": round(m["sumExcess"] / m["matches"], 2),
            "n_buys": m["nb"],
            "buy_winrate": round(m["bw"] / m["nb"] * 100, 1) if m["nb"] else 0,
            "buy_avgexcess": round(m["bsum"] / m["nb"], 2) if m["nb"] else 0,
            "n_sells": m["ns"],
            "sell_winrate": round(m["sw"] / m["ns"] * 100, 1) if m["ns"] else 0,
            "sell_avgexcess": round(m["ssum"] / m["ns"], 2) if m["ns"] else 0,
        })
    out.sort(key=lambda x: -x["elo"])

    generated = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    payload = {"generated": generated, "holding_days": HOLDING_DAYS,
               "trades_scored": len(scored), "members": out}

    # data.json  -> fetched by the browser when hosted over http (enables Reload)
    OUT_JSON.write_text(json.dumps(payload, indent=1))
    # data.js    -> loaded via <script> so it also works from a local file:// open
    OUT_JS.write_text("window.REAL_DATA = " + json.dumps(out) + ";\n"
                      "window.REAL_META = " + json.dumps({"generated": generated}) + ";\n")

    log(f"[done] {len(out)} members, {len(scored)} trades scored")
    log(f"  wrote {OUT_JSON.name} and {OUT_JS.name}  (generated {generated})")
    log("Open congress_elo_leaderboard.html (badge should read 'Live data').")


if __name__ == "__main__":
    build()
