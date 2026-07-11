# Congressional Trading ELO

A static leaderboard that scores US House & Senate stock trades as head-to-head
**matches against the S&P 500** and ranks members with a market-anchored ELO rating.
Free to host, free to run — no paid API keys.

## How it works

`build_leaderboard.py`:
1. Pulls disclosed trades from the free **Stock Watcher** datasets (House + Senate).
2. Fetches EOD prices for each ticker + the S&P 500 (`^GSPC`) via **yfinance**.
3. Scores each trade's return over a 30-trading-day window vs the S&P (the *excess return*).
4. Replays a market-anchored ELO (S&P fixed at 1500) in chronological order.
5. Writes `data.json` (fetched by the site) and `data.js` (local `file://` fallback).

`index.html` renders the board. It fetches `data.json` when hosted (the **Reload**
button pulls the latest), falls back to `data.js` for local opens, and shows synthetic
sample data if neither exists.

## Run locally

```bash
pip install yfinance pandas requests
python build_leaderboard.py
# then open index.html
```

## Host on GitHub Pages (auto-refreshing)

1. Create a repo and add these files. Rename `congress_elo_leaderboard.html` to **`index.html`**.
2. Settings → Pages → deploy from branch (`main`, root).
3. The included Action (`.github/workflows/update-data.yml`) rebuilds and commits the data
   every weekday. Trigger it manually the first time from the **Actions** tab (Run workflow).
4. Visit your Pages URL. The badge turns green ("Live data") and **Reload** pulls the newest build.

### Notes / caveats
- **Browser refresh** pulls the latest *published* `data.json`; it does **not** recompute
  live prices (a static site can't run Python or call Yahoo directly). Freshness = last CI run.
- **Yahoo throttling:** yfinance can occasionally rate-limit cloud IPs. If an Action run pulls
  no prices, just re-run it; the script caches and retries tickers individually.
- **Stock Watcher URLs** occasionally change — if trades fail to load, update the URLs in the
  `CONFIG` block of `build_leaderboard.py`.
- Amounts are disclosed as ranges (STOCK Act), so trades are equal-weighted by default.

## Tuning
All knobs live in the `CONFIG` block of `build_leaderboard.py`: holding window, K-factor,
tie band, minimum trades, start date, amount weighting, party tagging.
