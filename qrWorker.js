// qrWorker.js — worker thread for QR generation
const { parentPort } = require("worker_threads");
const QRCode = require("qrcode");

parentPort.on("message", async ({ id, text, options }) => {
  try {
    const dataUrl = await QRCode.toDataURL(text, options || {});
    parentPort.postMessage({ id, ok: true, dataUrl });
  } catch (err) {
    parentPort.postMessage({ id, ok: false, error: String(err?.message || err) });
  }
});
