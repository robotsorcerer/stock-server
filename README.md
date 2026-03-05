# Stocks Serve

Real-time stock and crypto price monitor with pre/after-hours support, terminal display,
and optional push alerts to your phone via WhatsApp or ntfy.

---

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [WhatsApp Setup](#whatsapp-setup)
- [Autostart on Login](#autostart-on-login)
- [Usage](#usage)
- [Alert Channels](#alert-channels)
- [Default Watchlist](#default-watchlist)
- [Output Columns](#output-columns)
- [Alert Signals](#alert-signals)
- [Module Reference](#module-reference)

---

## Features

- Live prices from the **Nasdaq API** (stocks) and **Yahoo Finance** (day high, crypto)
- **Pre-market** and **after-hours** prices automatically promoted into the `$$` column
- **Dynamic targets** fetched from Nasdaq at startup:
  - `NSDQ Pred (1-Yr)` — 1-year analyst consensus price target
  - `TARGET_LOW`  — 52-week low
- **Alert signals** per row: ✅ confirmed · 🚨 volatility · 🚀 bullish · 📉 bearish
- **Parallel fetching** via `ThreadPoolExecutor` — all tickers refreshed simultaneously
- **Color-coded table**: green (up) · red (down) · magenta (extended hours) · red bg (target hit)
- **Phone alerts** every 30 minutes via WhatsApp (Baileys, no third-party API cost) or ntfy
- **Trading-day guard** (`stock-guard`) — skips weekends and all NYSE holidays automatically
- **GNOME autostart** — launches on login on trading days only

---

## Project Structure

```
stocks_serve/
├── stock                   # Entry point — argparse + main polling loop
├── stock-guard             # Trading-day guard (runs stock on market days only)
├── config.py               # Default watchlist, crypto set, asset_class()
├── whatsapp_service.mjs    # Standalone Baileys HTTP service for WhatsApp delivery
├── package.json            # Node.js manifest (Baileys dependency)
├── requirements.txt        # Python runtime notes (stdlib only)
├── environment.yml         # Conda environment spec
├── utils/
│   ├── colors.py           # ANSI palette, colorize(), strip_ansi()
│   └── parsers.py          # parse_dollar(), parse_volume()
├── api/
│   ├── nasdaq.py           # nasdaq_get(), fetch_targets(), get_extended(), get_price()
│   └── yahoo.py            # get_day_high()
├── display/
│   └── table.py            # col_headers, alert_signal(), build_row(), print_table()
└── alerts/
    └── notify.py           # send_ntfy(), send_whatsapp()
```

---

## Prerequisites

### Python

Python 3.8 or later. The monitor uses **only the standard library** — no pip packages needed.

### Node.js (for WhatsApp alerts only)

| Tool      | Version  | Notes                                      |
|-----------|----------|--------------------------------------------|
| Node.js   | ≥ 18     | Tested on v24.13.0; install via nvm        |
| npm       | ≥ 8      | Ships with Node.js                         |
| Baileys   | 7.0.0-rc.9 | Installed via `npm install` (see below)  |

### Optional

| Tool      | Purpose                                          |
|-----------|--------------------------------------------------|
| openclaw  | Initial WhatsApp QR-code pairing only            |
| ntfy app  | Alternative push alerts (iOS / Android)          |
| GNOME     | Autostart desktop entry (Linux desktop only)     |

---

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/robotsorcerer/stock-server.git stocks_serve
cd stocks_serve
```

### 2. Set up the Python environment (conda)

No pip packages are required. Create a minimal environment just to pin the Python version:

```bash
conda env create -f environment.yml
conda activate stocks
```

Or use any existing Python 3.8+ environment — the stdlib is all that is needed.

### 3. Make the scripts executable

```bash
chmod +x stock stock-guard
```

### 4. Install Node.js dependencies (for WhatsApp alerts)

```bash
# If you don't have Node.js, install nvm first:
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.0/install.sh | bash
source ~/.bashrc
nvm install 24
nvm use 24

# Then install Baileys inside stocks_serve:
npm install
```

---

## WhatsApp Setup

The WhatsApp service reuses an existing authenticated session — it does **not** implement its
own QR-code pairing flow. You need to pair once using openclaw, then `whatsapp_service.mjs`
takes over for all subsequent message delivery.

### Step 1 — Pair your WhatsApp account (one-time)

Install openclaw if not already installed:

```bash
npm install -g openclaw
```

Link your WhatsApp account (scan QR in terminal):

```bash
openclaw channels login --channel whatsapp
```

Credentials are stored in:

```
$HOME/.openclaw/credentials/whatsapp/default/
```

### Step 2 — Start the WhatsApp service

```bash
node whatsapp_service.mjs
# or
npm start
```

The service listens on `http://127.0.0.1:18799` by default.

**Options:**

```bash
node whatsapp_service.mjs --port 18799 --creds $HOME/.openclaw/credentials/whatsapp/default
```

| Flag      | Env var    | Default                                                   |
|-----------|------------|-----------------------------------------------------------|
| `--port`  | `WA_PORT`  | `18799`                                                   |
| `--creds` | `WA_CREDS` | `$HOME/.openclaw/credentials/whatsapp/default`            |

**HTTP API:**

```
POST /send   { "to": "+12736458453", "message": "..." }
             -> 200 { "ok": true,  "messageId": "abc123" }
             -> 503 { "ok": false, "error": "Not connected" }

GET  /status -> 200 { "connected": true }
```

### Step 3 — Run the stock monitor with WhatsApp alerts

```bash
./stock --whatsapp +12736458453
```

The WhatsApp service is also started automatically by `stock-guard` on launch — you do not
need to start it separately when using the autostart setup.

---

## Autostart on Login

To launch the monitor automatically on login (GNOME desktop):

```bash
mkdir -p $HOME/.config/autostart
cat > $HOME/.config/autostart/stock-monitor.desktop << 'EOF'
[Desktop Entry]
Type=Application
Name=Stock Monitor
Comment=Real-time stock and crypto price monitor (trading days only)
Exec=gnome-terminal --title="Stock Monitor" -- $HOME/Downloads/shells/stocks_serve/stock-guard
Icon=utilities-terminal
Terminal=false
X-GNOME-Autostart-enabled=true
EOF
```

`stock-guard` checks whether today is a trading day (weekday, not a NYSE holiday) before
launching `stock`. On weekends and holidays it exits silently.

NYSE holidays handled:

| Holiday          | Rule                              |
|------------------|-----------------------------------|
| New Year's Day   | Jan 1 (observed Fri/Mon)          |
| MLK Day          | 3rd Monday of January             |
| Presidents Day   | 3rd Monday of February            |
| Good Friday      | Friday before Easter              |
| Memorial Day     | Last Monday of May                |
| Juneteenth       | Jun 19 (observed Fri/Mon)         |
| Independence Day | Jul 4 (observed Fri/Mon)          |
| Labor Day        | 1st Monday of September           |
| Thanksgiving     | 4th Thursday of November          |
| Christmas Day    | Dec 25 (observed Fri/Mon)         |

---

## Usage

```bash
# Built-in watchlist
./stock

# Append extra stocks
./stock AAPL TSLA
./stock --tickers AAPL TSLA
./stock -t AAPL TSLA

# Append crypto tickers
./stock --crypto ETH SOL
./stock -c ETH SOL

# Replace the default list entirely
./stock --only AAPL TSLA
./stock --only --crypto ETH SOL

# Mix stocks and crypto, no defaults
./stock -t AAPL -c ETH --only

# Custom refresh interval (default: 30 seconds)
./stock --sleep 60
./stock -s 10

# Phone alerts via WhatsApp every 30 minutes
./stock --whatsapp +12736458453

# Phone alerts via ntfy every 30 minutes
./stock --ntfy my-secret-topic-abc123

# Both simultaneously
./stock --whatsapp +12736458453 --ntfy my-secret-topic-abc123

# Full help
./stock --help
```

---

## Alert Channels

### WhatsApp

Requires the WhatsApp service running (see [WhatsApp Setup](#whatsapp-setup)).

```bash
./stock --whatsapp +12736458453
```

Every 30 minutes a message is POSTed to the local Baileys service, which delivers it to
your WhatsApp number. No API keys or subscription fees — it uses your own WhatsApp Web
session.

### ntfy (alternative)

A free, open-source push notification service. No account required.

1. Install the ntfy app:
   - iOS: https://apps.apple.com/us/app/ntfy/id1625396347
   - Android: https://play.google.com/store/apps/details?id=io.heckel.ntfy
2. Subscribe to a topic name of your choosing (keep it unguessable — topics are public).
3. Run:

```bash
./stock --ntfy my-secret-topic-abc123
```

Notifications are delivered to the ntfy app on your phone within seconds.

---

## Default Watchlist

```
INBS · LULU · OKLO · COIN · CRCL · MSTR · ARM · AMD · NVDA · MSFT · BTC
```

Defined in `config.py`. Edit `TICKERS` to change the permanent list.

---

## Output Columns

| Column       | Description                                                      |
|--------------|------------------------------------------------------------------|
| `SYMBOL`     | Ticker symbol                                                    |
| `$$`         | Last sale price — shows extended-hours price when applicable     |
| `BID`        | Current bid price                                                |
| `ASK`        | Current ask price                                                |
| `PREV_CLOSE` | Previous session close (derived from last sale minus net change) |
| `CHANGE %`   | Percentage change vs previous close                              |
| `DAY_HIGH`   | Intraday high (Yahoo Finance — always populated)                 |
| `VOLUME`     | Share/unit volume — shows extended-hours volume when applicable  |
| `1-Yr Low`   | 52-week low (Nasdaq summary)                                     |
| `NSDQ Pred`  | 1-year analyst consensus target (Nasdaq summary)                 |
| `ALERT`      | Signal emoji (see below)                                         |

### Row colors

| Color              | Meaning                                              |
|--------------------|------------------------------------------------------|
| Green on dark      | Price up from previous close                         |
| Red on dark        | Price down from previous close                       |
| Magenta on dark    | Extended hours active (pre/after)                    |
| White on red       | Price has hit or exceeded target                     |
| No color           | Change % unavailable (`N/A`) — no previous close data|

---

## Alert Signals

| Signal | Condition                                                                 |
|--------|---------------------------------------------------------------------------|
| ✅      | Price ≥ analyst target (`NSDQ Pred`)                                    |
| 🚨      | Move ≥ 3% AND price pulled back ≥ 2% from day high (haphazard swings)    |
| 🚀      | Change % positive and momentum holding (no significant pullback)          |
| 📉      | Change % negative or no upward momentum                                   |

---

## Module Reference

| Module                | Responsibility                                                  |
|-----------------------|-----------------------------------------------------------------|
| `stock`               | CLI parsing, main polling loop, alert dispatch                  |
| `stock-guard`         | NYSE trading-day check; starts WhatsApp service then execs stock|
| `config.py`           | `TICKERS`, `CRYPTO_TICKERS`, `asset_class()`                    |
| `utils/colors.py`     | `ANSI` palette, `colorize()`, `strip_ansi()`                    |
| `utils/parsers.py`    | `parse_dollar()`, `parse_volume()`                              |
| `api/nasdaq.py`       | `nasdaq_get()`, `fetch_targets()`, `get_extended()`, `get_price()` |
| `api/yahoo.py`        | `get_day_high()`                                                |
| `display/table.py`    | `alert_signal()`, `build_row()`, `print_table()`                |
| `alerts/notify.py`    | `send_ntfy()`, `send_whatsapp()`                                |
| `whatsapp_service.mjs`| Baileys WebSocket connection + HTTP `/send` and `/status` endpoints |
