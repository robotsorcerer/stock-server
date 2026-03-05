import datetime
import json
import urllib.request

from config import asset_class
from utils.parsers import parse_dollar, parse_volume
from api.yahoo import get_day_high

NASDAQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}


def nasdaq_get(path):
    url = f"https://api.nasdaq.com/api/{path}"
    req = urllib.request.Request(url, headers=NASDAQ_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def fetch_targets(symbol):
    """
    Fetch 1-year analyst target and 52-week low from Nasdaq summary endpoint.
    Returns (target_low, target_high) or (None, None) on failure.
    """
    try:
        data = nasdaq_get(f"quote/{symbol}/summary?assetClass={asset_class(symbol)}")
        sd = data["data"]["summaryData"]

        one_yr = parse_dollar(sd.get("OneYrTarget", {}).get("value"))

        hw_raw = sd.get("FiftTwoWeekHighLow", {}).get("value", "")
        fifty_two_low = None
        if "/" in hw_raw:
            parts = hw_raw.split("/")
            fifty_two_low = parse_dollar(parts[1])

        target_high = round(one_yr, 4) if one_yr else None
        target_low  = round(fifty_two_low, 4) if fifty_two_low else None
        return target_low, target_high
    except Exception:
        return None, None


def get_extended(symbol, market_type):
    """
    Fetch pre-market or post-market price and volume from Nasdaq.
    market_type: 'pre' or 'post'
    Returns (price, volume) or (None, None).
    """
    try:
        data = nasdaq_get(
            f"quote/{symbol}/extended-trading?assetclass=stocks&markettype={market_type}"
        )
        rows = data.get("data", {}).get("tradeDetailTable", {}).get("rows") or []
        if rows:
            price  = parse_dollar(rows[0].get("price"))
            volume = parse_volume(rows[0].get("shareVolume") or rows[0].get("volume") or "0")
            return price, volume
    except Exception:
        pass
    return None, None


def get_price(symbol):
    """
    Fetch current stock data from Nasdaq info endpoint.
    Returns: (price, prev_close, day_high, volume, ext_price, ext_label, bid, ask, ext_volume)
    """
    try:
        data = nasdaq_get(f"quote/{symbol}/info?assetClass={asset_class(symbol)}")
        d = data["data"]
        primary       = d["primaryData"]
        secondary     = d.get("secondaryData")
        market_status = d.get("marketStatus", "")

        price      = parse_dollar(primary.get("lastSalePrice"))
        net_change = parse_dollar(primary.get("netChange"))
        prev_close = round(price - net_change, 4) if price and net_change is not None else None
        day_high   = get_day_high(symbol)
        volume     = parse_volume(primary.get("volume"))

        ext_price  = None
        ext_label  = None
        ext_volume = None

        if secondary:
            ext_price = parse_dollar(secondary.get("lastSalePrice"))
            ext_label = secondary.get("label")

        if ext_price is None and market_status == "Closed":
            ext_price, ext_volume = get_extended(symbol, "post")
            if ext_price:
                ext_label = "After Hours"

        if ext_price is None:
            now  = datetime.datetime.now()
            hour = now.hour + now.minute / 60
            if 4 <= hour < 9.5:
                ext_price, ext_volume = get_extended(symbol, "pre")
                if ext_price:
                    ext_label = "Pre-Market"

        bid = parse_dollar(primary.get("bidPrice"))
        ask = parse_dollar(primary.get("askPrice"))

        return price, prev_close, day_high, volume, ext_price, ext_label, bid, ask, ext_volume
    except Exception:
        return (None,) * 9
