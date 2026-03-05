from utils.colors import ANSI, colorize
from utils.parsers import parse_dollar
from api.nasdaq import get_price

col_headers = ["SYMBOL", "$$", "BID", "ASK", "PREV_CLOSE", "CHANGE %",
               "DAY_HIGH", "VOLUME", "1-Yr Low", "NSDQ Pred", "ALERT"]
widths = [len(h) for h in col_headers]


def alert_signal(change_pct, active_price, target_high, day_high):
    if target_high and active_price >= target_high:
        return ANSI["confirmed"]
    if change_pct is not None and day_high is not None and active_price:
        pullback_pct = (day_high - active_price) / active_price * 100
        if abs(change_pct) >= 3 and pullback_pct >= 2:
            return ANSI["volatility"]
    if change_pct is not None and change_pct > 0:
        return ANSI["bullish"]
    return ANSI["bearish"]


def build_row(stock, target_low, target_high):
    price, prev_close, day_high, volume, ext_price, ext_label, bid, ask, ext_volume = get_price(stock)
    if price is None:
        return None
    ext_active     = bool(ext_price and ext_label)
    display_price  = ext_price if ext_active else price
    display_volume = (ext_volume or 0) if ext_active else volume
    active_price   = display_price
    change     = round(active_price - prev_close, 4) if prev_close is not None else None
    change_pct = round(change / prev_close * 100, 2) if prev_close else None
    return [
        stock,                                                           # 0  SYMBOL
        round(display_price, 4),                                         # 1  $$
        round(bid, 4) if bid is not None else "N/A",                     # 2  BID
        round(ask, 4) if ask is not None else "N/A",                     # 3  ASK
        round(prev_close, 4) if prev_close is not None else "N/A",       # 4  PREV_CLOSE
        change_pct if change_pct is not None else "N/A",                 # 5  CHANGE %
        round(day_high, 4) if day_high is not None else "N/A",           # 6  DAY_HIGH
        display_volume,                                                  # 7  VOLUME
        target_low if target_low is not None else "N/A",                 # 8  TARGET_LOW
        target_high if target_high is not None else "N/A",               # 9  TARGET_HIGH
        alert_signal(change_pct, active_price, target_high, day_high),   # 10 ALERT
        ext_active,                                                      # 11 hidden: row coloring
    ]


def print_table(rows):
    """Print the table and return its rendered width (for banner sizing)."""
    global widths
    n = len(col_headers)
    for row in rows:
        for i in range(n):
            widths[i] = max(widths[i], len(str(row[i])))

    def fmt_row(values):
        return " | ".join(str(values[i]).ljust(widths[i]) for i in range(n))

    separator   = "-+-".join("-" * w for w in widths)
    table_width = len(separator)

    print(colorize(fmt_row(col_headers), fg="fg_white", bg="bg_blue", bold=True))
    print(colorize(separator, fg="fg_cyan"))

    for row in rows:
        line       = fmt_row(row)
        change     = row[5]
        alerted    = row[10] == ANSI["confirmed"]
        ext_active = row[11]

        if alerted:
            print(colorize(line, fg="fg_white", bg="bg_red", bold=True))
        elif ext_active:
            print(colorize(line, fg="fg_magenta", bg="bg_dark", bold=True))
        elif change and change > 0:
            print(colorize(line, fg="fg_green", bg="bg_dark", bold=True))
        elif change and change < 0:
            print(colorize(line, fg="fg_red", bg="bg_dark", bold=True))
        else:
            print(line)

    return table_width
