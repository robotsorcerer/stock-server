import re
import sys

USE_COLOR = sys.stdout.isatty()

ANSI = {
    "reset":      "\033[0m",
    "bold":       "\033[1m",
    "fg_white":   "\033[97m",
    "fg_cyan":    "\033[96m",
    "fg_green":   "\033[92m",
    "fg_red":     "\033[91m",
    "fg_yellow":  "\033[93m",
    "fg_magenta": "\033[95m",
    "bg_blue":    "\033[44m",
    "bg_dark":    "\033[100m",
    "bg_red":     "\033[41m",
    "confirmed":  "\033[1;32m✅\033[0m",
    "bullish":    "\033[1;32m🚀\033[0m",
    "bearish":    "\033[1;31m📉\033[0m",
    "volatility": "\033[1;33m🚨\033[0m",
}


def colorize(text, fg=None, bg=None, bold=False):
    if not USE_COLOR:
        return text
    parts = []
    if bold:
        parts.append(ANSI["bold"])
    if fg:
        parts.append(ANSI[fg])
    if bg:
        parts.append(ANSI[bg])
    return "".join(parts) + text + ANSI["reset"]


def strip_ansi(s):
    return re.sub(r"\033\[[0-9;]*m", "", str(s))
