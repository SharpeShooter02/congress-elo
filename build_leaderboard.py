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

import csv, io, json, re, statistics, sys, time, math, datetime as dt
from pathlib import Path

# ----------------------------- CONFIG -----------------------------
HOLDING_DAYS   = 30      # trading days held before measuring the trade's return
# ELO sensitivity. At the classic chess value of 32 a single decision moved a
# rating by up to 32*4.5 = 144 points, so the published number was dominated by
# whatever happened most recently: it correlated 0.85 with a member's LAST 50
# decisions but only 0.61 with their whole record, and the median career swung
# 668 points from peak to trough. That is also why average excess return looked
# unrelated to the rating -- one is a full-sample mean, the other was a
# recency-weighted random walk.
#
# Measured by odd/even split-half correlation over every member with 20+
# decisions, effective reliability by K:
#     K=32 -> 0.62      K=16 -> 0.72      K=10 -> 0.77
#     K=8  -> 0.79      K=4  -> 0.83
# while agreement with the full-record score peaks around K=8-12 (0.76) and
# falls away below that as the rating stops responding to evidence at all.
K              = 8       # ELO sensitivity
MOV_CAP        = 4.5     # max margin-of-victory multiplier (keeps ~5–50% excesses distinct; trims only extreme outliers)
MARKET_ELO     = 1500    # fixed rating of the S&P 500 opponent
ELO_DIV        = 700     # rating scale: larger = more spread top-to-bottom (chess = 400)
TIE_BAND_PCT   = 0.5     # |excess| below this = a tie
MIN_TRADES     = 1       # members with fewer scored trades are dropped from output
FLAG_PCT       = 15.0    # a "sharp call": beat the market by this much within ~30 days
START_DATE     = "2012-01-01"   # kadoa history starts ~2012 — include all of it

# Time-decay weights for a trade's edge: an abnormal move that shows up within a
# month counts fully; within a year, less; only over the full holding period
# (years), least — but never zero. This makes FAST correctness (the insider-
# trading tell) dominate the ELO while slow buy-and-hold still counts a little.
W_30D   = 1.00   # edge visible within ~a month
W_1Y    = 0.30   # visible within ~a year
W_SINCE = 0.05   # only over the full (multi-year) holding period
EXCESS_CAP = 50.0  # clamp each horizon's excess before blending, so a multi-year hold's
                   # giant "since" return can't dominate the rating despite its low weight
WEIGHT_BY_AMOUNT = False
# Repeat trades in the same stock and direction inside this window are one
# DECISION, not several. A member buying the same stock across four filings in a
# week made one call; scoring it four times let a single decision dominate the
# rating. 7 days = one trading week: it absorbs the same-day/next-day spike (by
# far the largest, and plainly one decision split across filing line items)
# without reaching into monthly accumulation, which really is a repeated choice.
DECISION_WINDOW_DAYS = 7
# Herding: distinct members trading the same stock the SAME WAY inside this
# many days. Matched to the decision window above so both mean 'one trading
# week' rather than two different notions of 'around the same time'.
CLUSTER_WINDOW_DAYS = 7
CLUSTER_MIN_MEMBERS = 3
# --- reliability weighting -------------------------------------------------
# A rating built on 12 decisions is mostly luck; one built on 900 is mostly
# skill. Measured by splitting each member's decisions into odd/even halves and
# correlating the two independent ratings, the full-length reliability of this
# system fits the standard n/(n+k) form at k~27 with K=8. (Per-bucket estimates
# scatter, so treat 27 as an order of magnitude, not a precise constant. It was
# 67 while K was 32 -- a noisier rating needs far heavier shrinking.)
#
# Shrinking alone would squash everyone toward 1500 and destroy the spread that
# makes the number readable, so the shrunk ratings are then rescaled back out to
# the spread of the raw ones. Ranking is therefore driven by RELIABLE
# differences, while the axis stays as wide as before.
SHRINK_K = 27
RESCALE_AFTER_SHRINK = True
# Spread of the published ratings.
#
# The raw distribution is extremely peaked: the middle half of members sat
# within 46 rating points of each other while a handful of outliers ran from 455
# to 2288. Simply multiplying the spread up inflates the outliers and leaves the
# pack indistinguishable, which is the opposite of what the number is for.
#
# So the shrunk ratings are mapped onto a normal curve BY RANK: the ordering is
# preserved exactly, but the population is spread evenly, so equal rating gaps
# mean equal differences in standing. 1500 is the median member and +/-150 is
# roughly the 84th/16th percentile. This trades away the literal "expected score
# against a 1500 opponent" reading of an ELO, which the raw K=8 rating no longer
# supported anyway once it was shrunk.
RESCALE_TARGET_SD = 150
RANK_NORMALISE = True
# Funds whose return is, by construction, the benchmark itself. Scoring them as
# "beat the S&P" is circular -- a guaranteed tie that only adds noise. Sector and
# single-country funds are NOT excluded: those are genuine directional bets.
BROAD_MARKET_TICKERS = {
    # S&P 500 trackers (ETF + mutual fund share classes)
    "SPY", "IVV", "VOO", "SPLG", "SPTM", "RSP", "IVW", "IVE", "SPYG", "SPYV",
    "VFIAX", "VFINX", "FXAIX", "SWPPX", "PREIX", "SPFIX", "VINIX",
    # total US market
    "VTI", "ITOT", "SCHB", "VTSAX", "VTSMX", "FSKAX", "FZROX", "SWTSX",
    # large-cap / Russell 1000 core (effectively the same book of stocks)
    "IWB", "IWF", "IWD", "VONG", "VONV", "VV", "MGC",
    # levered or inverse S&P: return is a fixed multiple of the benchmark
    "SH", "SDS", "SPXU", "SPXS", "UPRO", "SSO", "SPUU",
}
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
# Former members too — otherwise everyone who has left Congress shows as "Unlisted".
LEG_HIST_URL     = "https://unitedstates.github.io/congress-legislators/legislators-historical.json"
# Presidents and vice presidents, with exact term dates -- the only part of the
# executive branch for which office tenure is available as free structured data.
EXEC_URL         = "https://unitedstates.github.io/congress-legislators/executive.json"
# DW-NOMINATE from Voteview (UCLA): every member of Congress placed on a single
# liberal-conservative axis derived purely from their roll-call votes. Keyless,
# one CSV, and keyed by bioguide id. Only the ideology score is used; the file
# also carries birth years and similar personal details, which have nothing to do
# with how someone trades and are deliberately left alone.
VOTEVIEW_URL     = "https://voteview.com/static/data/out/members/HSall_members.csv"
# Committee assignments (current members), keyed by bioguide id — no scraping.
COMMITTEES_URL           = "https://unitedstates.github.io/congress-legislators/committees-current.json"
COMMITTEE_MEMBERSHIP_URL = "https://unitedstates.github.io/congress-legislators/committee-membership-current.json"
# Ticker -> sector/industry (keyless, nightly-updated) so a trade can be read next
# to what the company actually does.
TICKER_SECTOR_URLS = [
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json",
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json",
    "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json",
]

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


def slugify(s):
    """URL/filesystem-safe id from a name."""
    return "".join(c if c.isalnum() else "-" for c in (s or "").lower()).strip("-") or "x"


def clip(s, n=70):
    """Bound a company/asset name so bond-style descriptions don't blow out the UI."""
    s = (s or "").strip()
    return (s[:n].rstrip() + "…") if len(s) > n else s


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


def merge_duplicate_filers(filers):
    """kadoa's filer directory lists a few members TWICE: once properly (with a
    photo_url, hence a bioguide id, a party and a state) and once as a bare stub
    parsed off a filing header -- 'John J McGuire III' alongside 'John McGuire',
    'M. Michael Rounds' alongside 'Mike Rounds'. Left alone, each pair splits one
    person's record across two leaderboard rows, and the stub half shows as
    "Unlisted" because it carries no party.

    Returns {stub_id: canonical_filer} for stubs whose surname matches exactly one
    complete filer in the same chamber. A stub matching zero or several is left
    alone -- merging the wrong two people is worse than showing a duplicate.
    """
    complete, stubs = [], []
    for f in filers:
        chamber = sval(f.get("chamber")).lower()
        if chamber not in ("house", "senate"):
            continue                              # executive filers have no photos anyway
        (complete if sval(f.get("photo_url")) else stubs).append(f)

    by_surname = {}
    for f in complete:
        parts = norm_name(extract_name(f)).split()
        if not parts: continue
        by_surname.setdefault((parts[-1], sval(f.get("chamber")).lower()), []).append(f)

    alias = {}
    for st in stubs:
        parts = norm_name(extract_name(st)).split()
        if not parts: continue
        cands = by_surname.get((parts[-1], sval(st.get("chamber")).lower()), [])
        if len(cands) != 1:
            continue
        canon = cands[0]
        # If both name a state they must agree.
        s1, s2 = sval(st.get("state")), sval(canon.get("state"))
        if s1 and s2 and s1 != s2:
            continue
        alias[sval(st.get("id"))] = canon
    if alias:
        for k, v in alias.items():
            log(f"[dedup] {k} -> {sval(v.get('id'))}")
    log(f"[dedup] merged {len(alias)} duplicate filer stub(s)")
    return alias


def load_trades():
    # 1. Enumerate every filer (House, Senate, and executive branch — current & former)
    log("[trades] loading filer directory")
    try:
        filers = fetch_json(KADOA_FILERS_URL, "kadoa_filers.json")
    except Exception as e:
        sys.exit(f"Could not load filer directory: {e}")
    alias = merge_duplicate_filers(filers)
    roster = []
    for f in filers:
        fid = sval(f.get("id"))
        if not fid:
            continue
        # A duplicate stub still has to be DOWNLOADED under its own id (that is
        # where its trades live) but is attributed to the canonical record, so the
        # two halves replay as one person in chronological order.
        canon = alias.get(fid)
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
        roster.append((fid, canon or f, label))
    log(f"[trades] {len(roster)} filers to pull (House + Senate + Executive, current & former)")

    # 2. Pull each filer's full history; score off kadoa's own excess-vs-market return
    trades = []
    for i, (fid, f, label) in enumerate(roster, 1):
        # fid = where the trades are fetched from; cid = who they are attributed to
        # (they differ only for the merged duplicate stubs).
        cid   = sval(f.get("id")) or fid
        name  = extract_name(f)
        party = sval(f.get("party"))
        # Executive-branch filers have no party, but they DO have an agency and a
        # job title — far more useful for reading a conflict than "Unlisted".
        agency = sval(f.get("agency"))
        office = sval(f.get("office"))
        state  = sval(f.get("state"))
        try:
            doc = fetch_json(KADOA_FILER_URL.format(id=fid), f"filer_{fid}.json")
        except Exception as e:
            log(f"  x {fid}: {e}")
            continue
        rows  = doc.get("trades", []) if isinstance(doc, dict) else (doc or [])
        finfo = doc.get("filer", {}) if isinstance(doc, dict) else {}
        photo = sval(finfo.get("photo_url")) or sval(f.get("photo_url"))
        bg    = photo.rsplit("/", 1)[-1].split(".")[0] if photo else ""   # .../G000061.jpg -> G000061
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
            amt_lo = fnum(row.get("amount_range_low"))
            amt_hi = fnum(row.get("amount_range_high"))
            # STOCK Act discloses a bracket, never an exact figure. The midpoint
            # is the conventional point estimate; the spread is kept so the UI can
            # be honest about how wide it is.
            amt_mid = (amt_lo + amt_hi) / 2 if (amt_lo is not None and amt_hi is not None) else None
            trades.append({
                "is_late": 1 if row.get("is_late") else 0,
                "days_to_file": fnum(row.get("days_to_file")),
                "amt_lo": amt_lo, "amt_hi": amt_hi, "amt_mid": amt_mid,
                "name": name or extract_name(row), "chamber": label,
                "party": party, "ticker": clean_ticker(sval(row.get("ticker"))) or "",
                "side": side, "date": tdate,
                "amount": sval(row.get("amount_range_label")) or sval(row.get("amount")),
                "ret30": r30, "ret1y": r1y, "retsince": rsince,
                "photo": photo, "bioguide": bg, "fid": cid,
                "agency": agency, "office": office, "state": state,
                "company": clip(sval(row.get("asset_name")).split("(")[0].split("[")[0]),
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
def pstdev(xs):
    if len(xs) < 2: return 0.0
    mu = sum(xs) / len(xs)
    return math.sqrt(sum((x - mu) ** 2 for x in xs) / len(xs))


def median(xs):
    if not xs: return None
    xs = sorted(xs); n = len(xs)
    return round(xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2, 1)


def downsample(curve, n=60):
    """Thin an ELO curve to at most n points for the sparklines. Always keeps the
       first and last point so the start (1500) and the final rating are exact."""
    if not curve: return []
    if len(curve) <= n: return curve
    step = (len(curve) - 1) / (n - 1)
    idx = sorted({int(round(i * step)) for i in range(n)} | {0, len(curve) - 1})
    return [curve[i] for i in idx]


def load_party_map():
    """Party lookup for every member of Congress, current AND former.

    Returns {"bg": {bioguide: party}, "nm": {name: (party, {states})}}.

    The bioguide map is exact and always trusted. The name map is a fallback and
    is deliberately conservative:
      * a name key shared by legislators of DIFFERENT parties is dropped rather
        than guessed -- a wrong party label is worse than an honest "Unlisted";
      * each surviving key carries the set of states its legislators served, so a
        surname-only match can be state-verified before it is trusted.
    The second rule matters because kadoa's filer directory contains people who
    are not members of Congress at all (a corporate officer filing through the
    Senate eFD system, say). Without a state check, a common surname would hand
    such a filer a confident and completely fictional party.
    """
    if not TAG_PARTY: return {"bg": {}, "nm": {}}
    legs = []
    for url, cache, age in ((LEG_URL, "legislators.json", 24 * 30),
                            (LEG_HIST_URL, "legislators_historical.json", 24 * 90)):
        try:
            legs += fetch_json(url, cache, max_age_h=age) or []
        except Exception as e:
            log(f"[party] skip {cache} ({e})")

    bg = {}
    cand = {}          # name key -> {"p": {parties}, "s": {states}}
    for l in legs:
        terms = l.get("terms") or [{}]
        party = (terms[-1].get("party") or "")[:1]      # D / R / I / other
        if not party: continue
        b = (l.get("id") or {}).get("bioguide")
        if b: bg[b] = party
        states = {sval(t.get("state")) for t in terms if t.get("state")}
        n = l.get("name", {})
        first, last = (n.get("first") or ""), (n.get("last") or "")
        keys = {n.get("official_full") or "", f"{first} {last}"}
        if n.get("nickname"): keys.add(f"{n['nickname']} {last}")
        if last: keys.add(last)                         # surname-only fallback key
        for k in keys:
            k = norm_name(k)
            if not k: continue
            e = cand.setdefault(k, {"p": set(), "s": set(), "b": set()})
            e["p"].add(party); e["s"] |= states
            if b: e["b"].add(b)

    nm = {k: (next(iter(v["p"])), v["s"],
               next(iter(v["b"])) if len(v["b"]) == 1 else "")
          for k, v in cand.items() if len(v["p"]) == 1}
    log(f"[party] {len(bg)} bioguide + {len(nm)} name entries "
        f"({len(cand) - len(nm)} ambiguous names dropped)")
    return {"bg": bg, "nm": nm}


def norm_name(n):
    """Lowercase and strip punctuation, honorifics, middle initials and
       suffixes, so 'A. Mitchell McConnell', 'Mitch McConnell' and
       'McConnell, A. Mitchell' all normalize to the same key."""
    n = (n or "").lower().replace(",", " ")
    n = re.sub(r"\b[a-z]\.", " ", n)                    # middle initials
    n = re.sub(r"[^a-z ]", " ", n)                      # remaining punctuation
    n = re.sub(r"\b(hon|mr|mrs|ms|dr|rep|sen|senator|representative)\b", " ", n)
    n = re.sub(r"\b(jr|sr|ii|iii|iv)\b", " ", n)        # generational suffixes
    return re.sub(r"\s+", " ", n).strip()


def match_party(name, pmap, bioguide="", state=""):
    """Resolve a party, in descending order of confidence:

       1. bioguide id  -- exact, always trusted;
       2. full name    -- strong evidence on its own;
       3. surname only -- trusted ONLY if the filer's state matches a state that
          legislator actually served, since a bare surname is weak evidence and
          not every filer in the source data is a member of Congress.
    """
    bg = (pmap or {}).get("bg", {})
    nm = (pmap or {}).get("nm", {})
    if bioguide and bioguide in bg:
        return bg[bioguide]

    hit = _match_legislator(name, nm, state)
    return hit[0] if hit else ""


def _match_legislator(name, nm, state=""):
    """Shared name resolution for both party and bioguide. Returns the matched
       (party, states, bioguide) tuple, or None."""
    n = norm_name(name)
    parts = n.split()
    keys = [n]
    if len(parts) > 2:
        keys.append(f"{parts[0]} {parts[-1]}")
    for key in keys:
        if key in nm:
            return nm[key]
    # Surname alone is weak evidence, so require the state to corroborate it.
    if parts and parts[-1] in nm:
        entry = nm[parts[-1]]
        if state and state in entry[1]:
            return entry
    return None


def match_bioguide(name, pmap, state=""):
    """Best-effort bioguide id for a filer that arrived without one."""
    hit = _match_legislator(name, (pmap or {}).get("nm", {}), state)
    return hit[2] if hit else ""


def load_committees():
    """bioguide -> [committee names] for current members (skips subcommittees)."""
    try:
        comms = fetch_json(COMMITTEES_URL, "committees.json", max_age_h=24 * 7)
        membs = fetch_json(COMMITTEE_MEMBERSHIP_URL, "committee_membership.json", max_age_h=24 * 7)
    except Exception as e:
        log(f"[committees] skip ({e})"); return {}
    code_name = {}
    for c in (comms or []):
        code = c.get("thomas_id")
        if code:
            code_name[code] = c.get("name", code)
    out = {}
    for code, members in (membs or {}).items():
        name = code_name.get(code)          # only top-level committees (skip subcommittee codes)
        if not name:
            continue
        for mem in members:
            bg = mem.get("bioguide")
            if bg:
                lst = out.setdefault(bg, [])
                if name not in lst:
                    lst.append(name)
    log(f"[committees] {len(out)} members with assignments")
    return out


def load_ticker_sectors():
    """ticker -> {'sector':..., 'industry':...} from public exchange listings."""
    out = {}
    for url in TICKER_SECTOR_URLS:
        fname = url.rsplit("/", 1)[-1]
        try:
            rows = fetch_json(url, "sectors_" + fname, max_age_h=24 * 7)
        except Exception as e:
            log(f"[sectors] {fname} skip ({e})"); continue
        for r in (rows or []):
            sym = sval(r.get("symbol")).upper()
            if sym:
                out[sym] = {"sector": sval(r.get("sector")), "industry": sval(r.get("industry"))}
    log(f"[sectors] {len(out)} tickers with sector/industry")
    return out


def load_ideology():
    """bioguide -> DW-NOMINATE first dimension, from the member's most recent
       Congress. Roughly -1 (most liberal) to +1 (most conservative)."""
    cache = CACHE_DIR / "voteview.json"
    if cache.exists() and (time.time() - cache.stat().st_mtime) < 24 * 30 * 3600:
        try:
            return json.loads(cache.read_text())
        except Exception:
            pass
    try:
        log(f"  downloading {VOTEVIEW_URL} ...")
        r = requests.get(VOTEVIEW_URL, timeout=120,
                         headers={"User-Agent": "elo-leaderboard/1.0"})
        r.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(r.text)))
    except Exception as e:
        log(f"[ideology] skip ({e})")
        return {}
    best = {}
    for row in rows:
        bg = (row.get("bioguide_id") or "").strip()
        dim = (row.get("nominate_dim1") or "").strip()
        if not bg or not dim:
            continue
        try:
            cong, val = int(row["congress"]), float(dim)
        except ValueError:
            continue
        if bg not in best or cong > best[bg][0]:
            best[bg] = (cong, round(val, 3))
    out = {bg: v for bg, (_, v) in best.items()}
    cache.write_text(json.dumps(out))
    log(f"[ideology] {len(out)} members scored (DW-NOMINATE)")
    return out


def load_executives():
    """Presidents and VPs -> {name key: {"party", "terms": [(start, end, type)]}}.

    Cabinet secretaries and agency heads are NOT in any free structured dataset,
    so this covers only the top of the executive branch. Everyone else is handled
    by reporting when they last filed rather than by guessing at their tenure.
    """
    try:
        ex = fetch_json(EXEC_URL, "executive.json", max_age_h=24 * 30)
    except Exception as e:
        log(f"[exec] skip ({e})"); return {}
    out = {}
    for p in ex or []:
        n = p.get("name", {})
        terms = [(sval(t.get("start")), sval(t.get("end")), sval(t.get("type")))
                 for t in (p.get("terms") or []) if t.get("start")]
        if not terms: continue
        party = ((p.get("terms") or [{}])[-1].get("party") or "")[:1]
        keys = {f"{n.get('first','')} {n.get('last','')}", n.get("official_full") or ""}
        if n.get("nickname"): keys.add(f"{n['nickname']} {n.get('last','')}")
        for k in keys:
            k = norm_name(k)
            if not k: continue
            e = out.setdefault(k, {"party": party, "terms": []})
            e["terms"] += terms
    log(f"[exec] {len(out)} president/VP name keys")
    return out


def exec_status(name, execs, today_iso):
    """(is_currently_serving, party, role) for a president/VP, or None."""
    hit = execs.get(norm_name(name))
    if not hit:
        parts = norm_name(name).split()
        if len(parts) > 2:
            hit = execs.get(f"{parts[0]} {parts[-1]}")
    if not hit: return None
    cur, role = False, ""
    for start, end, typ in hit["terms"]:
        if start <= today_iso and (not end or today_iso < end):
            cur, role = True, typ
    if not role:
        role = sorted(hit["terms"])[-1][2]
    label = {"prez": "President", "viceprez": "Vice President"}.get(role, role)
    return (cur, hit["party"], label)


def load_current_bioguides():
    """Set of bioguide ids for members CURRENTLY serving in Congress."""
    try:
        legs = fetch_json(LEG_URL, "legislators.json", max_age_h=24 * 30)
    except Exception as e:
        log(f"[current] skip ({e})"); return set()
    out = {(l.get("id") or {}).get("bioguide") for l in legs}
    out.discard(None)
    log(f"[current] {len(out)} sitting members of Congress")
    return out


# Curated: a keyword in a committee's name -> the market sectors it oversees.
# Broad tax/spending committees (Ways & Means, Appropriations, Budget, Rules) are
# deliberately omitted so they don't flag essentially every trade.
COMMITTEE_SECTORS = {
    "energy and commerce": {"Energy", "Utilities", "Health Care", "Telecommunications", "Technology"},
    "energy":             {"Energy", "Utilities"},
    "natural resources":  {"Energy", "Basic Materials", "Utilities"},
    "financial services": {"Finance", "Real Estate"},
    "banking":            {"Finance", "Real Estate"},
    "armed services":     {"Industrials"},
    "homeland security":  {"Industrials"},
    "intelligence":       {"Industrials"},
    "agriculture":        {"Consumer Staples", "Basic Materials"},
    "health":             {"Health Care"},
    "science":            {"Technology"},
    "commerce":           {"Technology", "Telecommunications", "Industrials"},
    "transportation":     {"Industrials"},
}


def build_jurisdiction(committees):
    """bg -> {sector: [committee names that oversee it]} for current members."""
    out = {}
    for bg, coms in committees.items():
        smap = {}
        for c in coms:
            cl = c.lower()
            secs = set()
            for kw, s in COMMITTEE_SECTORS.items():
                if kw in cl:
                    secs |= s
            for s in secs:
                smap.setdefault(s, [])
                if c not in smap[s]:
                    smap[s].append(c)
        if smap:
            out[bg] = smap
    return out


def luck_odds(wins, losses):
    """Rough '1-in-N chance this is luck': binomial tail (normal approx) vs a coin flip."""
    n = wins + losses
    if n < 15 or wins <= n * 0.5:
        return None
    z = (wins - 0.5 - n * 0.5) / math.sqrt(n * 0.25)
    p = max(0.5 * math.erfc(z / math.sqrt(2)), 1e-12)
    return round(1 / p)


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
        clamp = lambda x: max(-EXCESS_CAP, min(EXCESS_CAP, x))  # tame giant multi-year gains
        comps = []  # (weight, clamped excess in percentage points)
        ex30 = None  # raw (uncapped) 30-day excess — used for the honest sharp-call flag
        if t["ret30"] is not None:
            s = spx_window_return(spx, t["date"], 30)
            if s is not None:
                ex30 = t["ret30"] - s
                comps.append((W_30D, clamp(ex30)))
        if t["ret1y"] is not None:
            s = spx_window_return(spx, t["date"], 365)
            if s is not None: comps.append((W_1Y, clamp(t["ret1y"] - s)))
        if t["retsince"] is not None:
            horizon = max((today - t["date"]).days, 1)
            s = spx_window_return(spx, t["date"], horizon)
            if s is not None: comps.append((W_SINCE, clamp(t["retsince"] - s)))
        if not comps: continue
        wsum = sum(w for w, _ in comps)
        scored.append({**t, "excess": sum(w * e for w, e in comps) / wsum, "ex30": ex30})
    log(f"[elo] scored trades: {len(scored)}")

    # ---- drop benchmark-tracking funds ----
    before = len(scored)
    scored = [t for t in scored
              if (t.get("ticker") or "").upper() not in BROAD_MARKET_TICKERS]
    log(f"[elo] dropped {before - len(scored)} broad-market index trades "
        f"({len(BROAD_MARKET_TICKERS)} tickers excluded as circular)")

    # ---- collapse repeat trades into single decisions ----
    scored.sort(key=lambda t: t["date"])
    groups, index = {}, {}
    for t in scored:
        key = (t["name"], t["chamber"], (t.get("ticker") or "").upper(), t["side"])
        g = index.get(key)
        if g is not None and (t["date"] - groups[g]["anchor"]).days <= DECISION_WINDOW_DAYS:
            groups[g]["items"].append(t)
        else:
            g = len(groups)
            groups[g] = {"anchor": t["date"], "items": [t]}
            index[key] = g

    def collapse(items):
        """One decision from several filings: the earliest date (when the call was
           actually made), size-summed, and an excess averaged over the filings.
           Averaging rather than summing keeps a decision worth ONE match."""
        items = sorted(items, key=lambda x: x["date"])
        base = dict(items[0])
        n = len(items)
        base["excess"] = sum(x["excess"] for x in items) / n
        ex30 = [x["ex30"] for x in items if x.get("ex30") is not None]
        base["ex30"] = sum(ex30) / len(ex30) if ex30 else None
        base["is_late"] = 1 if any(x.get("is_late") for x in items) else 0
        dtf = [x["days_to_file"] for x in items if x.get("days_to_file") is not None]
        base["days_to_file"] = max(dtf) if dtf else None
        for k in ("amt_lo", "amt_hi", "amt_mid"):
            vals = [x[k] for x in items if x.get(k) is not None]
            base[k] = sum(vals) if vals else None
        base["n_filings"] = n
        return base

    collapsed = [collapse(g["items"]) for g in groups.values()]
    collapsed.sort(key=lambda t: t["date"])
    log(f"[elo] {len(scored)} trades -> {len(collapsed)} decisions "
        f"(window {DECISION_WINDOW_DAYS}d)")
    scored = collapsed

    members = {}
    def M(name, chamber, party=""):
        # Keyed by PERSON, not by seat: someone who moves from the Senate to a
        # cabinet post is one trader with one rating, not two half-records.
        # Each role they held is tracked separately for display.
        key = norm_name(name)
        if key not in members:
            members[key] = {"name": name, "chamber": chamber, "roles": {},
                            "party": party,   # resolved after the replay, once bioguide is known
                            "elo": 1500.0, "wins": 0, "losses": 0, "ties": 0,
                            "matches": 0, "sumExcess": 0.0,
                            "nb": 0, "bw": 0, "bsum": 0.0,   # buys:  count, wins, sum eff
                            "ns": 0, "sw": 0, "ssum": 0.0,   # sells: count, wins, sum eff
                            "sharp": 0, "conf": 0, "sconf": 0, "trades": [],
                            "first_trade": "", "last_trade": "",
                            "late": 0, "dtf": [],            # late filings, days-to-file
                            "vol_lo": 0.0, "vol_hi": 0.0, "vol_mid": 0.0,
                            "photo": "", "bioguide": "", "id": "",
                            "agency": "", "office": "", "state": "",
                            # (date, rating) after every scored trade -> sparkline
                            "curve": []}
        return members[key]

    committees = load_committees()
    sectors = load_ticker_sectors()
    current_bg = load_current_bioguides()
    juris = build_jurisdiction(committees)
    flagged = []   # individual sharp-call trades, for the "sketchiest trades" lists
    all_tr = []    # every trade (for cluster / herding detection)

    execs = load_executives()
    today_iso = dt.date.today().isoformat()

    def is_active(chamber, bioguide, name=""):
        """True/False for Congress (exact, via bioguide) and for presidents/VPs
           (exact, via term dates). None for every other executive appointee --
           their tenure simply is not available, and inferring it from trading
           activity would mark an official who stopped trading as 'Former'."""
        if chamber in ("House", "Senate"):
            return bioguide in current_bg
        st = exec_status(name, execs, today_iso)
        return st[0] if st else None

    for t in scored:
        m = M(t["name"], t["chamber"], t.get("party", ""))
        iso0 = t["date"].isoformat()
        r = m["roles"].setdefault(t["chamber"], {
            "chamber": t["chamber"], "office": "", "agency": "", "state": "",
            "first": iso0, "last": iso0, "trades": 0, "bioguide": ""})
        r["trades"] += 1
        r["first"] = min(r["first"], iso0)
        r["last"] = max(r["last"], iso0)
        for k in ("office", "agency", "state", "bioguide"):
            if not r[k] and t.get(k): r[k] = t[k]
        # The person's headline role is whichever one they filed under most recently.
        if iso0 >= m["last_trade"]:
            m["chamber"] = t["chamber"]
        if not m["photo"] and t.get("photo"): m["photo"] = t["photo"]
        if not m["bioguide"] and t.get("bioguide"): m["bioguide"] = t["bioguide"]
        if not m["id"] and t.get("fid"): m["id"] = t["fid"]
        for k in ("agency", "office", "state"):
            if not m[k] and t.get(k): m[k] = t[k]
        iso = t["date"].isoformat()
        if not m["first_trade"] or iso < m["first_trade"]: m["first_trade"] = iso
        if iso > m["last_trade"]: m["last_trade"] = iso
        m["late"] += t.get("is_late", 0)
        if t.get("days_to_file") is not None: m["dtf"].append(t["days_to_file"])
        for src, dst in (("amt_lo", "vol_lo"), ("amt_hi", "vol_hi"), ("amt_mid", "vol_mid")):
            if t.get(src) is not None: m[dst] += t[src]
        eff = t["excess"] if t["side"] == "buy" else -t["excess"]  # sells win when stock lags
        S = 1.0 if eff > TIE_BAND_PCT else (0.0 if eff < -TIE_BAND_PCT else 0.5)
        E = 1.0 / (1.0 + 10 ** ((MARKET_ELO - m["elo"]) / ELO_DIV))
        mov = min(1.0 + math.log(1 + abs(eff)), MOV_CAP)   # margin-of-victory multiplier
        m["elo"] += K * mov * (S - E)
        m["wins"]   += S == 1.0
        m["losses"] += S == 0.0
        m["ties"]   += S == 0.5
        m["matches"] += 1
        m["sumExcess"] += eff          # direction-adjusted, so avg matches win rate
        m["curve"].append([t["date"].isoformat(), round(m["elo"], 1),
                           t.get("ticker") or "", t["side"], round(eff, 1)])
        if t["side"] == "buy":
            m["nb"] += 1; m["bw"] += S == 1.0; m["bsum"] += eff
        else:
            m["ns"] += 1; m["sw"] += S == 1.0; m["ssum"] += eff
        eff30 = None
        if t.get("ex30") is not None:
            eff30 = t["ex30"] if t["side"] == "buy" else -t["ex30"]
        is_sharp = eff30 is not None and eff30 >= FLAG_PCT   # beat market 15%+ within a month
        si = sectors.get((t.get("ticker") or "").upper(), {})
        sector = si.get("sector", "")
        overlap = juris.get(t.get("bioguide", ""), {}).get(sector, []) if sector else []
        conflict = bool(overlap)           # stock is in a sector a committee they sit on oversees
        conf_com = overlap[0] if overlap else ""
        if conflict:
            m["conf"] += 1
            if is_sharp: m["sconf"] += 1
        if is_sharp:
            m["sharp"] += 1
            flagged.append({
                "name": t["name"], "id": t.get("fid", ""), "party": t.get("party", ""),
                "chamber": t["chamber"], "active": is_active(t["chamber"], t.get("bioguide", ""), t["name"]),
                "photo": t.get("photo", ""), "ticker": t.get("ticker", "") or "—",
                "company": t.get("company", ""),
                "sector": sector, "industry": si.get("industry", ""),
                "side": t["side"], "date": t["date"].isoformat(), "excess": round(eff30, 1),
                "committees": committees.get(t.get("bioguide", ""), []),
                "conflict": conflict, "conflict_committee": conf_com,
            })
        m["trades"].append({
            "date": t["date"].isoformat(), "side": t["side"],
            "ticker": t.get("ticker", "") or "—", "company": t.get("company", ""),
            "sector": sector, "excess": round(eff, 1),
            "ex30": round(eff30, 1) if eff30 is not None else None, "sharp": is_sharp,
            "late": bool(t.get("is_late")), "days_to_file": t.get("days_to_file"),
            "amt_lo": t.get("amt_lo"), "amt_hi": t.get("amt_hi"),
            "conflict": conflict, "conflict_committee": conf_com,
        })
        tkc = t.get("ticker", "") or ""
        if tkc and tkc != "—":
            all_tr.append({"ticker": tkc, "company": t.get("company", ""), "sector": sector,
                           "d": t["date"], "name": t["name"], "id": t.get("fid", ""),
                           "side": t["side"], "sharp": is_sharp})

    for m in members.values():          # ensure every member has a stable id
        if not m["id"]:
            m["id"] = slugify(m["name"] + "-" + m["chamber"])

    # Party is resolved here, not at member creation, because the bioguide id is
    # only known after we have seen a trade. Executive-branch filers genuinely
    # have no party (an agency administrator is not elected) -- they are labelled
    # by agency in the UI instead, so do not try to guess one for them.
    ideology = load_ideology()
    for m in members.values():
        m["ideology"] = ideology.get(m.get("bioguide") or "", None)
    scored_ideo = sum(1 for m in members.values() if m["ideology"] is not None)
    log(f"[ideology] matched {scored_ideo}/{len(members)} members")

    unresolved = []
    for m in members.values():
        if m["chamber"] not in ("House", "Senate"):
            st = exec_status(m["name"], execs, today_iso)
            if st:
                # A president or VP: party and office are known exactly.
                m["party"] = m["party"] or st[1]
                m["office"] = m["office"] or st[2]
            continue
        if not m.get("bioguide"):
            m["bioguide"] = match_bioguide(m["name"], pmap, m.get("state", ""))
        if m["party"] not in ("D", "R", "I"):
            m["party"] = match_party(m["name"], pmap, m.get("bioguide", ""),
                                     m.get("state", ""))
        if m["party"] and m["party"] not in ("D", "R", "I"):
            m["party"] = "I"                # historical third parties -> Independent
        if m["party"] not in ("D", "R", "I"):
            unresolved.append(m["name"])
    if unresolved:
        log(f"[party] still unresolved ({len(unresolved)}): {', '.join(sorted(unresolved)[:20])}")
    else:
        log("[party] every member of Congress resolved")

    # ---- reliability weighting: shrink toward 1500, then restore the spread ----
    rated = [m for m in members.values() if m["matches"] >= MIN_TRADES]
    for m in rated:
        m["elo_raw"] = m["elo"]
        m["reliability"] = m["matches"] / (m["matches"] + SHRINK_K)
        m["elo_shrunk"] = 1500.0 + (m["elo_raw"] - 1500.0) * m["reliability"]

    if RANK_NORMALISE and len(rated) > 1:
        order = sorted(rated, key=lambda m: m["elo_shrunk"])
        nd = statistics.NormalDist(1500.0, RESCALE_TARGET_SD)
        n_rated = len(order)
        for rank, m in enumerate(order):
            # mid-rank plotting position keeps the extremes off the infinite tails
            m["elo"] = nd.inv_cdf((rank + 0.5) / n_rated)
        log(f"[elo] reliability weighting: k={SHRINK_K}, rank-normalised to "
            f"mean 1500 / SD {RESCALE_TARGET_SD}")
    else:
        scale = 1.0
        if RESCALE_AFTER_SHRINK and len(rated) > 1:
            sd_shr = pstdev([m["elo_shrunk"] for m in rated])
            if sd_shr > 0:
                scale = RESCALE_TARGET_SD / sd_shr
        for m in rated:
            m["elo"] = 1500.0 + (m["elo_shrunk"] - 1500.0) * scale
        log(f"[elo] reliability weighting: k={SHRINK_K}, rescale x{scale:.2f}")

    for m in rated:
        # The sparkline has to land on the member's published rating, so the
        # whole path is scaled by whatever factor the final mapping applied to
        # them, with the reliability of the moment folded in. A member sitting
        # exactly at 1500 has no factor to derive, and needs none.
        gap = m["elo_shrunk"] - 1500.0
        f = (m["elo"] - 1500.0) / gap if abs(gap) > 1e-9 else 0.0
        m["curve"] = [[d, round(1500.0 + (v - 1500.0) * (i + 1) / (i + 1 + SHRINK_K) * f, 1),
                       tk, sd, ex]
                      for i, (d, v, tk, sd, ex) in enumerate(m["curve"])]

    out = []
    for m in members.values():
        if m["matches"] < MIN_TRADES: continue
        out.append({
            "name": m["name"], "party": m["party"], "chamber": m["chamber"],
            "elo": round(m["elo"]),
            "matches": m["matches"], "wins": int(m["wins"]),
            "losses": int(m["losses"]), "ties": int(m["ties"]),
            "winrate": round(m["wins"] / m["matches"] * 100, 1),
            # What the ELO actually optimises: a tie is half a win, not a loss.
            # Reporting bare wins/total contradicts the rating sitting beside it.
            "score": round((m["wins"] + 0.5 * m["ties"]) / m["matches"] * 100, 1),
            "avgexcess": round(m["sumExcess"] / m["matches"], 2),
            "n_buys": m["nb"],
            "buy_winrate": round(m["bw"] / m["nb"] * 100, 1) if m["nb"] else 0,
            "buy_avgexcess": round(m["bsum"] / m["nb"], 2) if m["nb"] else 0,
            "n_sells": m["ns"],
            "sell_winrate": round(m["sw"] / m["ns"] * 100, 1) if m["ns"] else 0,
            "sell_avgexcess": round(m["ssum"] / m["ns"], 2) if m["ns"] else 0,
            "sharp": m["sharp"],
            "conflicts": m["conf"], "sharp_conflicts": m["sconf"],
            "luck_odds": luck_odds(int(m["wins"]), int(m["losses"])),
            "id": m["id"],
            "active": is_active(m["chamber"], m["bioguide"], m["name"]),
            "photo": m["photo"],
            "agency": m["agency"], "office": m["office"], "state": m["state"],
            "first_trade": m["first_trade"], "last_trade": m["last_trade"],
            "late": m["late"],
            "late_rate": round(m["late"] / m["matches"] * 100, 1) if m["matches"] else 0,
            "median_days_to_file": median(m["dtf"]),
            "vol_lo": round(m["vol_lo"]), "vol_hi": round(m["vol_hi"]),
            "vol_mid": round(m["vol_mid"]),
            "roles": sorted(m["roles"].values(), key=lambda r: r["first"]),
            "reliability": round(m["reliability"], 3),
            "ideology": m.get("ideology"),
            "curve": downsample(m["curve"]),
        })
    out.sort(key=lambda x: -x["elo"])

    # per-member profile files (fetched on demand by the profile view)
    member_dir = Path(__file__).with_name("member"); member_dir.mkdir(exist_ok=True)
    for m in members.values():
        if m["matches"] < MIN_TRADES: continue
        prof = {
            "id": m["id"], "name": m["name"], "party": m["party"], "chamber": m["chamber"],
            "active": is_active(m["chamber"], m["bioguide"], m["name"]), "photo": m["photo"],
            "elo": round(m["elo"]), "matches": m["matches"],
            "wins": int(m["wins"]), "losses": int(m["losses"]), "ties": int(m["ties"]),
            "winrate": round(m["wins"] / m["matches"] * 100, 1),
            # What the ELO actually optimises: a tie is half a win, not a loss.
            # Reporting bare wins/total contradicts the rating sitting beside it.
            "score": round((m["wins"] + 0.5 * m["ties"]) / m["matches"] * 100, 1),
            "sharp": m["sharp"],
            "conflicts": m["conf"], "sharp_conflicts": m["sconf"],
            "luck_odds": luck_odds(int(m["wins"]), int(m["losses"])),
            "n_buys": m["nb"], "buy_winrate": round(m["bw"] / m["nb"] * 100, 1) if m["nb"] else 0,
            "n_sells": m["ns"], "sell_winrate": round(m["sw"] / m["ns"] * 100, 1) if m["ns"] else 0,
            "committees": committees.get(m["bioguide"], []),
            "agency": m["agency"], "office": m["office"], "state": m["state"],
            "first_trade": m["first_trade"], "last_trade": m["last_trade"],
            "late": m["late"],
            "late_rate": round(m["late"] / m["matches"] * 100, 1) if m["matches"] else 0,
            "median_days_to_file": median(m["dtf"]),
            "vol_lo": round(m["vol_lo"]), "vol_hi": round(m["vol_hi"]),
            "vol_mid": round(m["vol_mid"]),
            "roles": sorted(m["roles"].values(), key=lambda r: r["first"]),
            "reliability": round(m["reliability"], 3),
            "ideology": m.get("ideology"),
            "curve": downsample(m["curve"], 240),
            "trades": sorted(m["trades"], key=lambda x: x["date"], reverse=True),
        }
        (member_dir / (m["id"] + ".json")).write_text(json.dumps(prof))
    log(f"[member] wrote {len(out)} profile files")

    generated = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    # Keep the 700 most RECENT sharp trades (any magnitude) AND the 700 BIGGEST all-time,
    # so the front end can sort by recency or magnitude without losing small recent beats.
    by_recent = sorted(flagged, key=lambda x: x["date"], reverse=True)[:700]
    by_mag    = sorted(flagged, key=lambda x: -x["excess"])[:700]
    seen = set(); flagged_top = []
    for f in by_recent + by_mag:
        k = (f["name"], f["ticker"], f["date"], f["excess"])
        if k in seen: continue
        seen.add(k); flagged_top.append(f)
    earliest = min((t["date"] for t in scored), default=None)
    earliest_iso = earliest.isoformat() if earliest else ""

    # ---- cluster / herding detection ----
    # Rewritten to fix three things. It bucketed by calendar date // 10, so two
    # trades a day apart could land either side of a fixed boundary and never
    # cluster; it listed a member once per trade, so one person could fill a
    # "cluster"; and it mixed buys with sells, so three people disagreeing with
    # each other counted the same as three people piling in together.
    #
    # Now: clusters are per (ticker, DIRECTION), found with a sliding window of
    # CLUSTER_WINDOW_DAYS, and each member counts exactly once.
    by_key = {}
    for tr in all_tr:
        by_key.setdefault((tr["ticker"], tr["side"]), []).append(tr)

    clusters = []
    for (tk, side), trs in by_key.items():
        trs.sort(key=lambda x: x["d"])
        pool = list(trs)
        while len(pool) >= CLUSTER_MIN_MEMBERS:
            # Pick the densest window: the one covering the most DISTINCT members.
            best = None
            for i, anchor in enumerate(pool):
                window = [x for x in pool[i:]
                          if (x["d"] - anchor["d"]).days <= CLUSTER_WINDOW_DAYS]
                members = {x["id"] or x["name"] for x in window}
                if best is None or len(members) > len(best[1]):
                    best = (window, members)
            window, members = best
            if len(members) < CLUSTER_MIN_MEMBERS:
                break
            # One entry per member: earliest trade, and their sharp flag if any.
            first_by = {}
            for x in window:
                key = x["id"] or x["name"]
                if key not in first_by or x["d"] < first_by[key]["d"]:
                    first_by[key] = x
                if x["sharp"]:
                    first_by[key] = {**first_by[key], "sharp": True}
            entries = sorted(first_by.values(), key=lambda x: x["d"])
            clusters.append({
                "ticker": tk, "company": entries[0]["company"],
                "sector": entries[0]["sector"], "side": side,
                "start": entries[0]["d"].isoformat(),
                "end": entries[-1]["d"].isoformat(),
                "n_members": len(entries),
                "n_sharp": sum(1 for x in entries if x["sharp"]),
                "trades": [{"name": x["name"], "id": x.get("id", ""), "side": side,
                            "date": x["d"].isoformat(), "sharp": bool(x["sharp"])}
                           for x in entries],
            })
            used = {id(x) for x in window}
            pool = [x for x in pool if id(x) not in used]

    clusters.sort(key=lambda c: (c["end"], c["n_members"]), reverse=True)
    clusters = clusters[:600]
    log(f"[clusters] {len(clusters)} herding clusters")

    meta = {"generated": generated, "earliest": earliest_iso}
    payload = {"generated": generated, "holding_days": HOLDING_DAYS, "earliest": earliest_iso,
               "trades_scored": len(scored), "members": out, "flagged": flagged_top, "clusters": clusters}

    # data.json  -> fetched by the browser when hosted over http (enables Reload)
    OUT_JSON.write_text(json.dumps(payload, indent=1))
    # data.js    -> loaded via <script> so it also works from a local file:// open
    OUT_JS.write_text("window.REAL_DATA = " + json.dumps(out) + ";\n"
                      "window.REAL_FLAGGED = " + json.dumps(flagged_top) + ";\n"
                      "window.REAL_CLUSTERS = " + json.dumps(clusters) + ";\n"
                      "window.REAL_META = " + json.dumps(meta) + ";\n")

    log(f"[done] {len(out)} members, {len(scored)} trades scored")
    log(f"  wrote {OUT_JSON.name} and {OUT_JS.name}  (generated {generated})")
    log("Open congress_elo_leaderboard.html (badge should read 'Live data').")


if __name__ == "__main__":
    build()
