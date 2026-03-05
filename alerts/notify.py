import json
import os
import urllib.request

from utils.colors import colorize, strip_ansi

WA_SERVICE_PORT = int(os.environ.get("WA_PORT", "18799"))


def _format_rows(rows, snapshot):
    lines = [f"Stock Monitor  {snapshot}", ""]
    for row in rows:
        symbol  = row[0]
        price   = row[1]
        chg     = row[5]
        alert   = strip_ansi(row[10])
        chg_str = f"{chg:+.2f}%" if isinstance(chg, float) else str(chg)
        lines.append(f"{symbol:<6} ${price:<10} {chg_str:<9} {alert}")
    return "\n".join(lines)


def send_ntfy(topic, rows, snapshot):
    message = _format_rows(rows, snapshot)
    url = f"https://ntfy.sh/{topic}"
    req = urllib.request.Request(url, data=message.encode(), method="POST")
    req.add_header("Title", "Stock Monitor Alert")
    req.add_header("Priority", "default")
    req.add_header("Content-Type", "text/plain")
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(colorize(f"[ntfy] Failed to send alert: {e}", fg="fg_yellow"))


def send_whatsapp(target, rows, snapshot):
    message = _format_rows(rows, snapshot)
    body = json.dumps({"to": target, "message": message}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{WA_SERVICE_PORT}/send",
        data=body, method="POST",
    )
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                raise RuntimeError(result.get("error", "unknown error"))
    except Exception as e:
        print(colorize(f"[whatsapp] Failed to send alert: {e}", fg="fg_yellow"))
