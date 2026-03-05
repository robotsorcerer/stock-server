/**
 * stocks_serve/whatsapp_service.mjs
 *
 * Standalone WhatsApp notification service for the stock monitor.
 * Uses @whiskeysockets/baileys directly — no openclaw dependency at runtime.
 *
 * Reuses the existing WhatsApp session from ~/.openclaw/credentials/whatsapp/default
 * so no QR-code re-scan is needed.
 *
 * HTTP API (localhost only):
 *   POST /send   { "to": "+12678828846", "message": "..." }
 *               -> 200 { "ok": true, "messageId": "..." }
 *               -> 500 { "ok": false, "error": "..." }
 *   GET  /status -> 200 { "connected": true|false }
 *
 * Usage:
 *   node whatsapp_service.mjs [--port 18799] [--creds <path>]
 */

import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
} from "@whiskeysockets/baileys";
import { createServer } from "http";
import { homedir } from "os";
import { join } from "path";

// ---------------------------------------------------------------------------
// Config from CLI args
// ---------------------------------------------------------------------------
const argv = process.argv.slice(2);
const argOf = (flag) => {
  const i = argv.indexOf(flag);
  return i !== -1 ? argv[i + 1] : null;
};
const PORT = parseInt(argOf("--port") || process.env.WA_PORT || "18799", 10);
const CREDS_DIR =
  argOf("--creds") ||
  process.env.WA_CREDS ||
  join(homedir(), ".openclaw", "credentials", "whatsapp", "default");

// ---------------------------------------------------------------------------
// Minimal silent logger (satisfies Baileys' pino interface without pino dep)
// ---------------------------------------------------------------------------
const noop = () => {};
const silentLogger = {
  trace: noop, debug: noop, info: noop, warn: noop, error: noop,
  child: () => silentLogger,
};

// ---------------------------------------------------------------------------
// WhatsApp connection
// ---------------------------------------------------------------------------
let sock = null;
let connected = false;
let reconnectTimer = null;

function toJid(number) {
  // Strip all non-digits then append WhatsApp JID suffix
  return number.replace(/\D/g, "") + "@s.whatsapp.net";
}

async function connect() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

  console.log(`[wa] Connecting with creds from: ${CREDS_DIR}`);
  const { state, saveCreds } = await useMultiFileAuthState(CREDS_DIR);

  sock = makeWASocket({
    auth: state,
    logger: silentLogger,
    printQRInTerminal: false,
    // Use the same browser fingerprint openclaw uses so WhatsApp doesn't
    // invalidate the existing session by seeing a "new" device.
    browser: ["Stock Monitor", "Chrome", "120.0.0.0"],
    // Don't pull full message history — we only need to send.
    syncFullHistory: false,
    markOnlineOnConnect: false,
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", ({ connection, lastDisconnect, qr }) => {
    if (qr) {
      console.warn(
        "[wa] QR code received — session may have expired. " +
        "Re-link via: openclaw channels login --channel whatsapp"
      );
    }

    if (connection === "open") {
      connected = true;
      console.log("[wa] Connected to WhatsApp");
    } else if (connection === "close") {
      connected = false;
      const code = lastDisconnect?.error?.output?.statusCode;
      const loggedOut = code === DisconnectReason.loggedOut;
      console.log(
        `[wa] Connection closed (code ${code ?? "?"})`,
        loggedOut ? "— logged out, not reconnecting" : "— will reconnect in 5s"
      );
      if (!loggedOut) {
        reconnectTimer = setTimeout(connect, 5000);
      }
    }
  });
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(body);
}

const server = createServer(async (req, res) => {
  if (req.method === "GET" && req.url === "/status") {
    return json(res, 200, { connected });
  }

  if (req.method === "POST" && req.url === "/send") {
    let payload;
    try {
      payload = JSON.parse(await readBody(req));
    } catch {
      return json(res, 400, { ok: false, error: "Invalid JSON body" });
    }

    const { to, message } = payload;
    if (!to || !message) {
      return json(res, 400, { ok: false, error: "Missing 'to' or 'message'" });
    }
    if (!connected || !sock) {
      return json(res, 503, { ok: false, error: "Not connected to WhatsApp" });
    }

    try {
      const jid = toJid(to);
      const result = await sock.sendMessage(jid, { text: message });
      const messageId = result?.key?.id ?? "unknown";
      console.log(`[wa] Sent message ${messageId} -> ${to}`);
      return json(res, 200, { ok: true, messageId });
    } catch (err) {
      console.error(`[wa] Send failed: ${err}`);
      return json(res, 500, { ok: false, error: String(err) });
    }
  }

  res.writeHead(404);
  res.end();
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`[wa] Service listening on http://127.0.0.1:${PORT}`);
  console.log(`[wa] Credentials: ${CREDS_DIR}`);
  connect();
});

// Graceful shutdown
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.log(`\n[wa] Shutting down (${sig})`);
    server.close(() => process.exit(0));
  });
}
