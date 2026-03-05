import json
import urllib.request

from config import CRYPTO_TICKERS


def get_day_high(symbol):
    """
    Fetch day high from Yahoo Finance — reliable even after market close.
    Crypto symbols are mapped to Yahoo's {SYMBOL}-USD convention.
    """
    yf_symbol = f"{symbol}-USD" if symbol.upper() in CRYPTO_TICKERS else symbol
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}?interval=1m&range=1d"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            meta = json.loads(resp.read())["chart"]["result"][0]["meta"]
            return meta.get("regularMarketDayHigh")
    except Exception:
        return None
