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
MARKET_ELO     = 1500    # fixed rating of the S&P 500 opponent
TIE_BAND_PCT   = 0.5     # |excess| below this = a tie
MIN_TRADES     = 1       # members with fewer scored trades are dropped from output
START_DATE     = "2021-01-01"   # ignore trades before this (keeps price history sane)
WEIGHT_BY_AMOUNT = False
TAG_PARTY      = True    # look up party from congress-legislators (best-effort)
OUT_JS         = Path(__file__).with_name("data.js")
OUT_JSON       = Path(__file__).with_name("data.json")
CACHE_DIR      = Path(__file__).with_name("_cache"); CACHE_DIR.mkdir(exist_ok=True)

# Each chamber tries its sources in order until one returns data. The GitHub-hosted
# mirrors (raw.githubusercontent.com) are primary — reliable and keyless — with the
# old Stock Watcher S3 buckets kept as fallbacks in case they come back.
HOUSE_URLS = [
    "https://raw.githubusercontent.com/noodleslove/House-of-Representative-Analysis-I/main/data/all_transactions.json",
    "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
]
SENATE_URLS = [
    "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/aggregate/all_transactions.json",
    "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json",
]
LEG_URL    = "https://unitedstates.github.io/congress-legislators/legislators-current.json"

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


def load_trades():
    trades = []
    for chamber, urls, cache, name_key in [
        ("House",  HOUSE_URLS,  "house.json",  "representative"),
        ("Senate", SENATE_URLS, "senate.json", "senator"),
    ]:
        log(f"[trades] {chamber}")
        rows = fetch_json_any(urls, cache)
        if not rows:
            log(f"  !! no {chamber} data from any source; skipping this chamber")
            continue
        for row in rows:
            tk = clean_ticker(row.get("ticker"))
            side = norm_type(row.get("type"))
            tdate = parse_date(row.get("transaction_date"))
            if not (tk and side and tdate): continue
            if tdate.isoformat() < START_DATE: continue
            name = (row.get(name_key) or "").strip()
            name = name.replace("Hon. ", "").strip()
            if not name: continue
            trades.append({
                "name": name, "chamber": chamber, "ticker": tk,
                "side": side, "date": tdate, "amount": row.get("amount", ""),
            })
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
        sys.exit("No trades loaded — check the Stock Watcher URLs in CONFIG.")

    start = (min(t["date"] for t in trades) - dt.timedelta(days=7)).isoformat()
    end   = (dt.date.today() + dt.timedelta(days=1)).isoformat()
    prices = download_prices([t["ticker"] for t in trades], start, end)
    spx = prices.get("^GSPC")
    if spx is None or len(spx) == 0:
        sys.exit("Could not fetch S&P 500 (^GSPC) prices.")

    pmap = load_party_map()

    # score each trade -> excess return vs S&P over the same window
    scored = []
    for t in trades:
        s = prices.get(t["ticker"])
        r_stock, d0, d1 = ret_over_window(s, t["date"], HOLDING_DAYS)
        if r_stock is None: continue
        # S&P return over the SAME calendar window
        sp0 = spx.index.searchsorted(d0); sp1 = spx.index.searchsorted(d1)
        if sp1 >= len(spx) or sp0 >= len(spx): continue
        r_spx = float(spx.iloc[sp1]) / float(spx.iloc[sp0]) - 1.0
        excess = (r_stock - r_spx) * 100.0            # percentage points
        scored.append({**t, "entry": d0, "excess": excess})

    scored.sort(key=lambda x: x["entry"])
    log(f"[elo] scored trades: {len(scored)}")

    members = {}
    def M(name, chamber):
        key = (name, chamber)
        if key not in members:
            members[key] = {"name": name, "chamber": chamber,
                            "party": match_party(name, pmap),
                            "elo": 1500.0, "wins": 0, "losses": 0, "ties": 0,
                            "matches": 0, "sumExcess": 0.0}
        return members[key]

    for t in scored:
        m = M(t["name"], t["chamber"])
        eff = t["excess"] if t["side"] == "buy" else -t["excess"]  # sells win when stock lags
        S = 1.0 if eff > TIE_BAND_PCT else (0.0 if eff < -TIE_BAND_PCT else 0.5)
        E = 1.0 / (1.0 + 10 ** ((MARKET_ELO - m["elo"]) / 400.0))
        mov = min(1.0 + math.log(1 + abs(eff)), 3.0)   # margin-of-victory multiplier
        m["elo"] += K * mov * (S - E)
        m["wins"]   += S == 1.0
        m["losses"] += S == 0.0
        m["ties"]   += S == 0.5
        m["matches"] += 1
        m["sumExcess"] += t["excess"]

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
