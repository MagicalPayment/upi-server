// server.js (Payment Gateway) — fully async + robust QR worker pool
// Node >= 18
const express = require("express");
const path = require("path");
const fs = require("fs").promises;
const { randomUUID } = require("crypto");
const axios = require("axios");
const { CookieJar, Cookie } = require("tough-cookie");
const { wrapper } = require("axios-cookiejar-support");
const cors = require("cors");
const { io: ioClient } = require("socket.io-client");
const os = require("os");
const { Worker } = require("worker_threads");
require("dotenv").config();

const app = express();
app.use(express.json());
app.use(cors({ origin: process.env.CLIENT_URL, credentials: false }));
app.use(express.static(path.join(__dirname, "public")));

const BOOT_ID = randomUUID();

// ---- ENV ----
const PORT = Number(process.env.port || 3000);
const DEFAULT_UPI = process.env.UPI || "magicals@slc";
const DEFAULT_NAME = process.env.Name || "Magical Developer";
const FETCH_PER_SECONDS = Math.max(1, Number(process.env.FETCH_PER_SECONDS || 5));
const CLIENT_URL = process.env.CLIENT_URL || "http://localhost:3001";
const REDIRECT_URL = process.env.REDIRECT_URL || "/";

// ---- Freecharge endpoints / cookies ----
const FC_LIST_URL = "https://www.freecharge.in/thv/listv3?fcAppType=MSITE";
const FC_HOST = "www.freecharge.in";
const COOKIES_FILE = path.join(__dirname, "cookies.txt");

// ---- Socket.IO CLIENT (to client server) ----
const ioToClient = ioClient(CLIENT_URL, { transports: ["websocket"], reconnection: true });

// ---- In-memory sessions ----
/** @type {Record<string, {comment:string, amount:number, upiLink:string, createdAt:number, expiresAt:number, status:'PENDING'|'SUCCESS'|'FAILED'|'TIMEUP'|'CANCELLED', timer?:NodeJS.Timeout, foundTxn?:any }>} */
const sessions = Object.create(null);

/* ------------------------------------------------------------------ */
/* 1) QR worker pool (correlation IDs + timeouts)                      */
/* ------------------------------------------------------------------ */
let QR_WORKERS = Number(process.env.QR_WORKERS);
if (!Number.isFinite(QR_WORKERS) || QR_WORKERS <= 0) {
  QR_WORKERS = Math.max(1, Math.min(os.cpus().length - 1, 4));
}
const workers = [];
const pending = new Map(); // id -> { resolve, reject, to }
let rr = 0;

function createWorker() {
  const w = new Worker(path.join(__dirname, "qrWorker.js"));
  w.on("message", (msg) => {
    const { id, ok, dataUrl, error } = msg || {};
    const rec = pending.get(id);
    if (!rec) return;
    pending.delete(id);
    clearTimeout(rec.to);
    ok ? rec.resolve(dataUrl) : rec.reject(new Error(error || "QR worker error"));
  });
  w.on("error", (err) => {
    console.error("[qr-worker] error:", err?.message || err);
  });
  return w;
}
for (let i = 0; i < QR_WORKERS; i++) workers.push(createWorker());

function qrDataUrlAsync(text, options) {
  return new Promise((resolve, reject) => {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const w = workers[(rr = (rr + 1) % workers.length)];
    const to = setTimeout(() => {
      pending.delete(id);
      reject(new Error("QR worker timeout"));
    }, 10000); // 10s safety timeout
    pending.set(id, { resolve, reject, to });
    w.postMessage({ id, text, options });
  });
}

/* ------------------------------------------------------------------ */
/* 2) Cookie header cache (async)                                      */
/* ------------------------------------------------------------------ */
const cookieCache = { header: "", mtimeMs: 0, lastCheckMs: 0 };
async function buildCookieHeaderFromFile() {
  const now = Date.now();
  if (now - cookieCache.lastCheckMs < 5000 && cookieCache.header) return cookieCache.header;
  cookieCache.lastCheckMs = now;

  try {
    const st = await fs.stat(COOKIES_FILE);
    if (st.mtimeMs !== cookieCache.mtimeMs || !cookieCache.header) {
      const text = await fs.readFile(COOKIES_FILE, "utf8");
      const cookies = [];
      for (const line of text.split(/\r?\n/)) {
        if (!line || line.startsWith("#")) continue;
        const [domain, includeSubdomainsStr, cookiePath, secureStr, expiresStr, name, value] = line.split("\t");
        cookies.push({
          domain,
          includeSubdomains: (includeSubdomainsStr || "").toUpperCase() === "TRUE",
          path: cookiePath,
          secure: (secureStr || "").toUpperCase() === "TRUE",
          expires: Number(expiresStr) || 0,
          name,
          value,
        });
      }
      const host = FC_HOST;
      const header = cookies
        .filter((c) => {
          const cd = c.domain.startsWith(".") ? c.domain.slice(1) : c.domain;
          if (host === cd) return true;
          if (c.includeSubdomains) return host.endsWith("." + cd);
          return false;
        })
        .filter((c) => c.expires === 0 || c.expires > Math.floor(Date.now() / 1000))
        .map((c) => `${c.name}=${c.value}`)
        .join("; ");
      cookieCache.header = header;
      cookieCache.mtimeMs = st.mtimeMs;
    }
  } catch {
    cookieCache.header = "";
  }
  return cookieCache.header;
}
async function buildHeaders() {
  const cookieHeader = await buildCookieHeaderFromFile();
  return {
    Host: FC_HOST,
    Connection: "keep-alive",
    "sec-ch-ua-platform": '"Windows"',
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    Accept: "application/json, text/plain, */*",
    csrfRequestIdentifier: "",
    "Content-Type": "application/json",
    "sec-ch-ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    Origin: "https://www.freecharge.in",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    Referer: "https://www.freecharge.in/",
    "Accept-Language": "en-US,en;q=0.9",
    Cookie: cookieHeader,
  };
}

/* ------------------------------------------------------------------ */
/* 3) Fast cookie validity check (async)                                */
/* ------------------------------------------------------------------ */
async function loadJarFromFile() {
  const jar = new CookieJar();
  try {
    const text = await fs.readFile(COOKIES_FILE, "utf8");
    for (const line of text.split(/\r?\n/)) {
      if (!line || line.startsWith("#")) continue;
      const [domain, , cookiePath, secureFlag, expiry, name, value] = line.split("\t");
      const host = domain.startsWith(".") ? domain.slice(1) : domain;
      const opts = {
        key: name,
        value,
        domain: host,
        path: cookiePath,
        secure: (secureFlag || "").toUpperCase() === "TRUE",
        httpOnly: false,
      };
      if (expiry !== "0") opts.expires = new Date(Number(expiry) * 1000);
      jar.setCookieSync(new Cookie(opts), `https://${host}`);
    }
  } catch {}
  return jar;
}
async function isCookiesValid() {
  try {
    const jar = await loadJarFromFile();
    const client = wrapper(
      axios.create({
        jar,
        withCredentials: true,
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        },
        timeout: 8000,
      })
    );
    const res = await client.get("https://www.freecharge.in/transactions-history", {
      maxRedirects: 0,
      validateStatus: (s) => s < 400,
    });
    if (res.status === 200 && !/login\s*\/\s*register/i.test(String(res.data || ""))) return true;
    return false;
  } catch {
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* 4) Poll helpers (async, with timeouts)                              */
/* ------------------------------------------------------------------ */
function allSectionRows(txn) {
  const meta = txn?.billerInfo?.billerMetaData;
  const rows = [];
  if (Array.isArray(meta)) {
    for (let i = 0; i < meta.length; i++) {
      const sd = meta[i]?.sectionDetails;
      if (Array.isArray(sd)) for (const r of sd) rows.push({ index: i, name: r?.name, value: r?.value });
    }
  }
  return rows;
}
function sectionValue(rows, ...names) {
  for (const n of names) {
    const f = rows.find((r) => String(r?.name) === n);
    if (f && f.value != null && String(f.value).trim() !== "") return String(f.value).trim();
  }
  return undefined;
}

async function fetchTransactions() {
  const headers = await buildHeaders();
  const body = {
    userImsId: "",
    isAndroid: false,
    fromDate: null,
    toDate: null,
    paymentStatus: "",
    paymentDirection: "",
    paymentAccountType: "",
  };
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), 10000);
  try {
    const res = await fetch(FC_LIST_URL, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();
    return json?.data?.globalTransactions ?? [];
  } finally {
    clearTimeout(id);
  }
}

function findByComment(globalTxns, comment) {
  for (const t of globalTxns) {
    const rows = allSectionRows(t);
    const c = sectionValue(rows, "Comments", "Remark", "Remarks", "Note");
    if (c === comment) return { txn: t, rows };
  }
  return null;
}

/* ------------------------------------------------------------------ */
/* 5) Routes                                                           */
/* ------------------------------------------------------------------ */
app.get("/api/boot", (_, res) => res.json({ bootId: BOOT_ID }));
app.get("/api/config", (_, res) => res.json({ redirectUrl: REDIRECT_URL }));
app.get("/api/check-cookies", async (_, res) => res.json({ ok: true, valid: await isCookiesValid() }));

// Create a gateway session
app.post("/api/session", async (req, res) => {
  try {
    const { comment } = req.query;
    if (!comment || !/^[A-Za-z0-9_-]{3,}$/.test(String(comment))) {
      return res.status(400).json({ error: "INVALID_COMMENT" });
    }
    if (!(await isCookiesValid())) return res.status(401).json({ error: "COOKIES_INVALID" });

    // Ask the client server for session info
    let amount, expiresAt;
    try {
      const r = await axios.get(`${CLIENT_URL}/session/${encodeURIComponent(comment)}`, { timeout: 8000 });
      amount = r.data?.amount;
      expiresAt = r.data?.expiresAt;
    } catch {
      return res.status(404).json({ error: "COMMENT_NOT_FOUND_ON_CLIENT" });
    }
    if (!amount || !expiresAt) return res.status(404).json({ error: "COMMENT_NOT_FOUND_ON_CLIENT" });

    const params = new URLSearchParams({ pa: DEFAULT_UPI, pn: DEFAULT_NAME, am: String(amount), tn: String(comment), cu: "INR" });
    const upiLink = `upi://pay?${params.toString()}`;

    // Offload QR generation to workers
    const qrDataUrl = await qrDataUrlAsync(upiLink, { errorCorrectionLevel: "M", margin: 1, scale: 6 });

    const now = Date.now();
    sessions[comment] = { comment, amount, upiLink, createdAt: now, expiresAt, status: "PENDING" };

    // Start polling for this comment
    const pollEveryMs = FETCH_PER_SECONDS * 1000;
    sessions[comment].timer = setInterval(async () => {
      const s = sessions[comment];
      if (!s) return;
      const now = Date.now();
      if (now >= s.expiresAt) {
        s.status = "TIMEUP";
        clearInterval(s.timer);
        delete s.timer;
        ioToClient.emit("payment-timeup", comment);
        return;
      }
      try {
        const txns = await fetchTransactions();
        const found = findByComment(txns, comment);
        if (found) {
          s.foundTxn = found.txn;
          const stat = found.txn?.txnDetails?.status;
          const amt = Number(found.txn?.txnDetails?.amount);
          if (stat === "SUCCESS" && amt === s.amount) {
            s.status = "SUCCESS";
            clearInterval(s.timer);
            delete s.timer;
            ioToClient.emit("payment-success", comment);
          } else if (stat === "SUCCESS" && amt !== s.amount) {
            s.status = "FAILED";
            clearInterval(s.timer);
            delete s.timer;
            ioToClient.emit("payment-failure", comment);
          }
        }
      } catch {
        // ignore transient poll errors
      }
    }, pollEveryMs);

    // TTL guard
    setTimeout(() => {
      const s = sessions[comment];
      if (!s) return;
      if (s.status === "PENDING") s.status = "TIMEUP";
      if (s.timer) { clearInterval(s.timer); delete s.timer; }
      ioToClient.emit("payment-timeup", comment);
    }, Math.max(0, expiresAt - Date.now() + 1500));

    res.json({
      bootId: BOOT_ID,
      sessionId: comment,
      amount,
      comment,
      upiLink,
      qrDataUrl,
      pollEverySeconds: FETCH_PER_SECONDS,
      expiresAt,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "FAILED_TO_START_SESSION" });
  }
});

// Status
app.get("/api/status/:comment", (req, res) => {
  const s = sessions[req.params.comment];
  if (!s) return res.status(404).json({ error: "Session not found" });

  let details = null;
  if (s.foundTxn) {
    const rows = allSectionRows(s.foundTxn);
    const utr = sectionValue(rows, "UPI Transaction ID") || null;
    let payerName = s.foundTxn?.txnDetails?.title || "";
    if (payerName.toLowerCase().startsWith("from ")) payerName = payerName.slice(5);
    details = {
      utr,
      payerName: payerName || null,
      payerVpa: s.foundTxn?.txnDetails?.subtitle || null,
      amount: s.foundTxn?.txnDetails?.amount ?? s.amount,
      timestamp: s.foundTxn?.txnDetails?.timestamp || Date.now(),
    };
  }

  res.json({ status: s.status, remainingMs: Math.max(0, s.expiresAt - Date.now()), details });
});

// Cancel
app.post("/api/cancel/:comment", (req, res) => {
  const c = req.params.comment;
  const s = sessions[c];
  if (!s) return res.status(404).json({ error: "Session not found" });
  if (s.status === "PENDING") {
    if (s.timer) { clearInterval(s.timer); delete s.timer; }
    s.status = "CANCELLED";
  }
  ioToClient.emit("payment-cancelled", c);
  res.json({ ok: true, status: s.status });
});

// Guard invalid comment -> redirect
app.get("/:comment", (req, res) => {
  const comment = String(req.params.comment || "");
  if (!/^[A-Za-z0-9_-]{3,}$/.test(comment)) return res.redirect(REDIRECT_URL);
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.get("/", (_, res) => res.redirect(REDIRECT_URL));

app.listen(PORT, () => {
  console.log(`Gateway on http://localhost:${PORT} — worker pool: ${QR_WORKERS}`);
});
