import datetime
import json
import os
import urllib.parse
import urllib.request

from config import CRYPTO_TICKERS
from utils.colors import colorize

LOG_DIR = os.path.expanduser("~/.stocks")
LOG_FILE = os.path.join(LOG_DIR, "2026-logs.log")

# High-impact events that commonly ripple through equity and crypto markets.
GEOPOLITICAL_EVENTS = [
    {"date": "2020-03-11", "name": "WHO declares COVID-19 pandemic"},
    {"date": "2020-11-03", "name": "US presidential election (2020)"},
    {"date": "2021-01-06", "name": "US Capitol attack and transition uncertainty"},
    {"date": "2022-02-24", "name": "Russia invades Ukraine"},
    {"date": "2023-10-07", "name": "Israel-Hamas war escalation"},
    {"date": "2024-11-05", "name": "US presidential election (2024)"},
]


def _ticker_for_yahoo(symbol):
    if symbol.upper() in CRYPTO_TICKERS:
        return f"{symbol.upper()}-USD"
    return symbol.upper()


def _fetch_five_year_history(symbol):
    yahoo_symbol = _ticker_for_yahoo(symbol)
    encoded = urllib.parse.quote(yahoo_symbol, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        "?range=5y&interval=1d&events=history&includeAdjustedClose=true"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "stocks-serve/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read())

    results = payload.get("chart", {}).get("result") or []
    if not results:
        return []

    result = results[0]
    timestamps = result.get("timestamp") or []
    quote = (result.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []

    points = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        date = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc).date()
        points.append((date, float(close)))
    return points


def _date_range(points):
    if not points:
        return None, None
    return points[0][0], points[-1][0]


def _closest_price(points, target_date):
    if not points:
        return None
    return min(points, key=lambda item: abs((item[0] - target_date).days))[1]


def _pct_change(a, b):
    if a in (None, 0) or b is None:
        return None
    return round((b - a) / a * 100, 3)


def _event_correlations(points):
    rows = []
    for event in GEOPOLITICAL_EVENTS:
        date = datetime.date.fromisoformat(event["date"])
        pre = _closest_price(points, date - datetime.timedelta(days=7))
        at = _closest_price(points, date)
        post = _closest_price(points, date + datetime.timedelta(days=7))
        rows.append(
            {
                "event_date": event["date"],
                "event_name": event["name"],
                "pre_7d_to_event_pct": _pct_change(pre, at),
                "event_to_post_7d_pct": _pct_change(at, post),
            }
        )
    return rows


def _first_friday(year, month):
    first = datetime.date(year, month, 1)
    offset = (4 - first.weekday()) % 7  # Friday == 4
    return first + datetime.timedelta(days=offset)


def _generate_midterm_events(start_date, end_date):
    rows = []
    for year in range(start_date.year, end_date.year + 1):
        if year % 4 != 2:
            continue
        nov_first = datetime.date(year, 11, 1)
        offset = (1 - nov_first.weekday()) % 7  # Tuesday == 1
        election_day = nov_first + datetime.timedelta(days=offset)
        if start_date <= election_day <= end_date:
            rows.append(
                {
                    "event_date": election_day.isoformat(),
                    "event_name": f"US midterm election ({year})",
                }
            )
    return rows


def _generate_bls_cps_events(start_date, end_date):
    rows = []
    cursor = datetime.date(start_date.year, start_date.month, 1)
    while cursor <= end_date:
        release_day = _first_friday(cursor.year, cursor.month)
        if start_date <= release_day <= end_date:
            month_label = release_day.strftime("%Y-%m")
            rows.append(
                {
                    "event_date": release_day.isoformat(),
                    "event_name": f"BLS Employment Situation ({month_label})",
                }
            )
            rows.append(
                {
                    "event_date": release_day.isoformat(),
                    "event_name": f"CPS labor force release ({month_label})",
                }
            )
        if cursor.month == 12:
            cursor = datetime.date(cursor.year + 1, 1, 1)
        else:
            cursor = datetime.date(cursor.year, cursor.month + 1, 1)
    return rows


def _events_to_correlation(points, events):
    rows = []
    for event in events:
        date = datetime.date.fromisoformat(event["event_date"])
        pre = _closest_price(points, date - datetime.timedelta(days=7))
        at = _closest_price(points, date)
        post = _closest_price(points, date + datetime.timedelta(days=7))
        rows.append(
            {
                "event_date": event["event_date"],
                "event_name": event["event_name"],
                "pre_7d_to_event_pct": _pct_change(pre, at),
                "event_to_post_7d_pct": _pct_change(at, post),
            }
        )
    return rows


def _fetch_yahoo_earnings_dates(symbol):
    yahoo_symbol = _ticker_for_yahoo(symbol)
    encoded = urllib.parse.quote(yahoo_symbol, safe="")
    url = (
        f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{encoded}"
        "?modules=earningsHistory,calendarEvents"
    )
    req = urllib.request.Request(url, headers={"User-Agent": "stocks-serve/1.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = json.loads(resp.read())

    results = payload.get("quoteSummary", {}).get("result") or []
    if not results:
        return []
    block = results[0]
    out = set()

    earnings_history = block.get("earningsHistory", {}).get("history") or []
    for item in earnings_history:
        quarter = item.get("quarter", {})
        quarter_fmt = quarter.get("fmt")
        if not quarter_fmt:
            continue
        try:
            out.add(datetime.date.fromisoformat(quarter_fmt))
        except ValueError:
            continue

    calendar_dates = (
        block.get("calendarEvents", {})
        .get("earnings", {})
        .get("earningsDate") or []
    )
    for item in calendar_dates:
        fmt = item.get("fmt")
        if not fmt:
            continue
        try:
            out.add(datetime.date.fromisoformat(fmt))
        except ValueError:
            continue

    return sorted(out)


def _quarter_anchor_dates(start_date, end_date):
    anchors = []
    for year in range(start_date.year, end_date.year + 1):
        for month in (2, 5, 8, 11):
            day = datetime.date(year, month, 15)
            if start_date <= day <= end_date:
                anchors.append(day)
    return anchors


def _earnings_correlations(symbol, points):
    start_date, end_date = _date_range(points)
    if not start_date or not end_date:
        return []

    dates = []
    try:
        dates = [d for d in _fetch_yahoo_earnings_dates(symbol) if start_date <= d <= end_date]
    except Exception:
        dates = []

    if len(dates) < 6:
        dates = sorted(set(dates + _quarter_anchor_dates(start_date, end_date)))

    events = [
        {"event_date": d.isoformat(), "event_name": f"{symbol} quarterly earnings"}
        for d in dates
    ]
    return _events_to_correlation(points, events)


def _trend_summary(points):
    if len(points) < 2:
        return {}
    first_date, first_close = points[0]
    last_date, last_close = points[-1]
    one_year_ago = last_date - datetime.timedelta(days=365)
    one_year_close = _closest_price(points, one_year_ago)

    return {
        "start_date": first_date.isoformat(),
        "end_date": last_date.isoformat(),
        "start_close": round(first_close, 4),
        "end_close": round(last_close, 4),
        "five_year_return_pct": _pct_change(first_close, last_close),
        "one_year_return_pct": _pct_change(one_year_close, last_close),
    }


def append_daily_history_log(symbols):
    os.makedirs(LOG_DIR, exist_ok=True)
    run_date = datetime.date.today().isoformat()
    run_ts = datetime.datetime.now().isoformat(timespec="seconds")
    lines = []
    already_logged = set()

    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as fh:
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        entry = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if entry.get("run_date") == run_date and entry.get("symbol"):
                        already_logged.add(entry["symbol"])
        except Exception:
            pass

    for symbol in symbols:
        if symbol in already_logged:
            continue
        try:
            points = _fetch_five_year_history(symbol)
            if not points:
                lines.append(
                    {
                        "timestamp": run_ts,
                        "run_date": run_date,
                        "symbol": symbol,
                        "status": "no_data",
                    }
                )
                continue

            lines.append(
                {
                    "timestamp": run_ts,
                    "run_date": run_date,
                    "symbol": symbol,
                    "status": "ok",
                    "trend": _trend_summary(points),
                    "geopolitical_event_correlations": _event_correlations(points),
                    "macro_event_correlations": _events_to_correlation(
                        points,
                        _generate_midterm_events(*_date_range(points))
                        + _generate_bls_cps_events(*_date_range(points)),
                    ),
                    "quarterly_earnings_correlations": _earnings_correlations(symbol, points),
                }
            )
        except Exception as exc:
            lines.append(
                {
                    "timestamp": run_ts,
                    "run_date": run_date,
                    "symbol": symbol,
                    "status": "error",
                    "error": str(exc),
                }
            )

    with open(LOG_FILE, "a", encoding="utf-8") as fh:
        for entry in lines:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")

    print(colorize(f"[history-log] Appended {len(lines)} records to {LOG_FILE}", fg="fg_cyan"))
