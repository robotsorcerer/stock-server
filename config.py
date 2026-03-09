TICKERS = [
    "INBS", "LULU", "OKLO", "COIN", "CRCL", "GOOG",
    "MSTR", "ARM", "AMD", "NVDA", "MSFT", "BTC", "AAPL"
]

CRYPTO_TICKERS = {"BTC"}


def asset_class(symbol):
    return "crypto" if symbol.upper() in CRYPTO_TICKERS else "stocks"
