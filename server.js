#!/usr/bin/env node
/**
 * หมาล่าทอด — Preorder Queue Server
 * ------------------------------------------------
 * Zero-dependency Node.js server (built-in http/fs only).
 * Persists ALL data to disk under ./data (auto-created if missing).
 * Serves the front-end from ./public.
 * Pushes real-time stock updates to every connected browser via SSE.
 *
 * Run:   node server.js
 * Open:  http://localhost:3000
 */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_DIR = path.join(ROOT, 'data');
const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');
const PUBLIC_DIR = path.join(ROOT, 'public');

const FILES = {
  settings: path.join(DATA_DIR, 'settings.json'),
  stock: path.join(DATA_DIR, 'stock.json'),
  orders: path.join(DATA_DIR, 'orders.json'),
  counter: path.join(DATA_DIR, 'counter.json'),
};

const MENU = [
  { id: 'bacon_enoki', name: 'เบคอนพันเห็ดเข็มทอง', emoji: '🥓' },
  { id: 'pork_enoki', name: 'หมูสามชั้นพันเห็ดเข็มทอง', emoji: '🍖' },
  { id: 'bologna', name: 'โบโลน่า', emoji: '🌭' },
  { id: 'tofu_cheese', name: 'เต้าหู้ชีส', emoji: '🧀' },
  { id: 'corn', name: 'ข้าวโพด', emoji: '🌽' },
  { id: 'sausage_cheese', name: 'ไส้กรอกชีส', emoji: '🧆' },
  { id: 'potato', name: 'มันฝรั่ง', emoji: '🥔' },
];
const PRICE = 15;
const STATUSES = ['รอตรวจสอบสลิป', 'ยืนยันคำสั่งซื้อ / กำลังทอด', 'พร้อมรับ', 'ส่งเรียบร้อย (ปิดออเดอร์)', 'ยอดไม่ตรง', 'ยกเลิก'];
const DEFAULT_SETTINGS = { shopName: 'หมาล่าทอด', qrImage: null, logoImage: null, itemImages: {}, adminPin: '0607' };

/* ================= STORAGE: ensure folder + files exist on disk ================= */
function ensureStorage() {
  for (const dir of [DATA_DIR, UPLOADS_DIR]) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      console.log('[storage] created folder:', dir);
    }
  }
  if (!fs.existsSync(FILES.settings)) fs.writeFileSync(FILES.settings, JSON.stringify(DEFAULT_SETTINGS, null, 2));
  if (!fs.existsSync(FILES.stock)) fs.writeFileSync(FILES.stock, JSON.stringify({}, null, 2));
  if (!fs.existsSync(FILES.orders)) fs.writeFileSync(FILES.orders, JSON.stringify([], null, 2));
  if (!fs.existsSync(FILES.counter)) fs.writeFileSync(FILES.counter, JSON.stringify({ count: 0 }, null, 2));
}
ensureStorage();

function readJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { return fallback; }
}

/* In-memory store, loaded once at boot = fast reads. Every mutation is
   write-through persisted to disk (async, queued per-file so writes never
   interleave/corrupt the file). This keeps the site snappy while still
   being durable across restarts. */
const db = {
  settings: readJSON(FILES.settings, { ...DEFAULT_SETTINGS }),
  stock: readJSON(FILES.stock, {}),
  orders: readJSON(FILES.orders, []),
  counter: readJSON(FILES.counter, { count: 0 }),
};
// migrate: fill in any settings keys added in later versions (e.g. logoImage, itemImages)
db.settings = { ...DEFAULT_SETTINGS, ...db.settings, itemImages: { ...(db.settings.itemImages || {}) } };

const writeQueues = {};
function persist(key) {
  const file = FILES[key];
  const snapshot = JSON.stringify(db[key], null, 2);
  const prev = writeQueues[key] || Promise.resolve();
  writeQueues[key] = prev.then(() => new Promise((resolve) => {
    const tmp = file + '.tmp';
    fs.writeFile(tmp, snapshot, (err) => {
      if (err) { console.error('[storage] write failed:', file, err); return resolve(); }
      fs.rename(tmp, file, () => resolve());
    });
  }));
  return writeQueues[key];
}

/* ================= SSE (real-time push to front-end) ================= */
const sseClients = new Set();
function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) { try { res.write(payload); } catch (e) { sseClients.delete(res); } }
}

/* ================= helpers ================= */
function publicMenu() {
  return MENU.map(m => ({
    ...m,
    price: PRICE,
    stock: (db.stock[m.id] === undefined ? null : db.stock[m.id]),
    image: (db.settings.itemImages && db.settings.itemImages[m.id]) || null,
  }));
}
function publicSettings() {
  return {
    shopName: db.settings.shopName || DEFAULT_SETTINGS.shopName,
    qrImage: db.settings.qrImage || null,
    logoImage: db.settings.logoImage || null,
  };
}
function extFromDataUrl(dataUrl) {
  const m = /^data:image\/(\w+);/.exec(dataUrl || '');
  const ext = m ? m[1] : 'jpg';
  return ext === 'jpeg' ? 'jpg' : ext;
}
function saveDataUrlImage(dataUrl, filename) {
  const m = /^data:image\/\w+;base64,(.+)$/.exec(dataUrl || '');
  if (!m) return null;
  fs.writeFileSync(path.join(UPLOADS_DIR, filename), Buffer.from(m[1], 'base64'));
  return '/uploads/' + filename;
}
function genId(prefix) { return prefix + '_' + Date.now() + '_' + crypto.randomBytes(4).toString('hex'); }
function checkPin(pin) { return !!pin && !!db.settings.adminPin && pin === db.settings.adminPin; }

function readBody(req, limit = 15 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > limit) { reject(new Error('payload too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (chunks.length === 0) return resolve(null);
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); } catch (e) { resolve(null); }
    });
    req.on('error', reject);
  });
}
function sendJSON(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml', '.json': 'application/json', '.webp': 'image/webp' };
function serveStatic(res, filePath) {
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath).toLowerCase()] || 'application/octet-stream' });
    res.end(data);
  });
}

/* ================= server ================= */
const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://' + req.headers.host);
    const p = url.pathname;
    const method = req.method;

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type,x-admin-pin');
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PATCH,OPTIONS');
    if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    if (p.startsWith('/uploads/') && method === 'GET') {
      return serveStatic(res, path.join(UPLOADS_DIR, path.basename(p)));
    }

    /* ---- real-time stock stream ---- */
    if (p === '/api/stream' && method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive' });
      res.write('retry: 3000\n\n');
      res.write(`event: menu\ndata: ${JSON.stringify(publicMenu())}\n\n`);
      sseClients.add(res);
      const keepAlive = setInterval(() => { try { res.write(':ping\n\n'); } catch (e) { } }, 25000);
      req.on('close', () => { sseClients.delete(res); clearInterval(keepAlive); });
      return;
    }

    if (p === '/api/public-settings' && method === 'GET') return sendJSON(res, 200, publicSettings());
    if (p === '/api/menu' && method === 'GET') return sendJSON(res, 200, publicMenu());

    /* ---- place order ---- */
    if (p === '/api/orders' && method === 'POST') {
      const body = await readBody(req);
      if (!body || !body.items || typeof body.items !== 'object') return sendJSON(res, 400, { error: 'ข้อมูลไม่ถูกต้อง' });

      const items = {};
      for (const [id, qtyRaw] of Object.entries(body.items)) {
        const qty = parseInt(qtyRaw, 10);
        if (qty > 0) items[id] = qty;
      }
      if (Object.keys(items).length === 0) return sendJSON(res, 400, { error: 'ไม่มีรายการสั่งซื้อ' });
      for (const id of Object.keys(items)) if (!MENU.find(m => m.id === id)) return sendJSON(res, 400, { error: 'พบรายการที่ไม่รู้จัก' });
      for (const [id, qty] of Object.entries(items)) {
        const stock = db.stock[id];
        if (stock !== undefined && stock !== null && qty > stock) {
          const item = MENU.find(m => m.id === id);
          return sendJSON(res, 409, { error: `${item.name} เหลือไม่พอ (เหลือ ${stock} ไม้)` });
        }
      }
      const customer = body.customer || {};
      if (!customer.name || !customer.room || !customer.phone) return sendJSON(res, 400, { error: 'กรอกข้อมูลผู้สั่งให้ครบ' });
      if (!body.slipImage) return sendJSON(res, 400, { error: 'แนบรูปสลิปก่อนนะ' });

      for (const [id, qty] of Object.entries(items)) {
        if (db.stock[id] !== undefined && db.stock[id] !== null) db.stock[id] = Math.max(0, db.stock[id] - qty);
      }
      db.counter.count = (db.counter.count || 0) + 1;
      const queueNo = 'A' + String(db.counter.count).padStart(3, '0');
      const id = genId('ord');
      const total = Object.entries(items).reduce((s, [, q]) => s + q * PRICE, 0);
      let slipUrl = null;
      try { slipUrl = saveDataUrlImage(body.slipImage, id + '.' + extFromDataUrl(body.slipImage)); } catch (e) { }

      const order = { id, queueNo, items, total, customer, slipImage: slipUrl, status: STATUSES[0], createdAt: new Date().toISOString() };
      db.orders.push(order);
      await Promise.all([persist('orders'), persist('stock'), persist('counter')]);
      broadcast('menu', publicMenu());
      broadcast('new-order', { queueNo }); // lightweight ping so admin screens can live-refresh; no customer data on this public channel
      return sendJSON(res, 200, order);
    }

    /* ---- track order ---- */
    if (p.startsWith('/api/orders/track/') && method === 'GET') {
      const q = decodeURIComponent(p.split('/').pop() || '').toUpperCase();
      const order = db.orders.find(o => o.queueNo === q);
      if (!order) return sendJSON(res, 404, { error: 'ไม่พบคำสั่งซื้อ' });
      return sendJSON(res, 200, order);
    }

    /* ---- admin login ---- */
    if (p === '/api/admin/login' && method === 'POST') {
      const body = await readBody(req);
      const ok = checkPin(body && body.pin);
      return sendJSON(res, ok ? 200 : 401, { ok });
    }

    /* ---- admin guarded routes ---- */
    if (p.startsWith('/api/admin/')) {
      const pin = req.headers['x-admin-pin'];
      if (!checkPin(pin)) return sendJSON(res, 401, { error: 'unauthorized' });

      if (p === '/api/admin/orders' && method === 'GET') {
        return sendJSON(res, 200, [...db.orders].sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)));
      }
      const statusMatch = p.match(/^\/api\/admin\/orders\/([^/]+)$/);
      if (statusMatch && method === 'PATCH') {
        const body = await readBody(req);
        const order = db.orders.find(o => o.id === statusMatch[1]);
        if (!order) return sendJSON(res, 404, { error: 'not found' });
        if (body && STATUSES.includes(body.status)) {
          order.status = body.status;
          await persist('orders');
          return sendJSON(res, 200, order);
        }
        return sendJSON(res, 400, { error: 'bad status' });
      }
      if (p === '/api/admin/stock' && method === 'GET') return sendJSON(res, 200, db.stock);
      if (p === '/api/admin/stock' && method === 'POST') {
        const body = await readBody(req);
        if (!body || typeof body.stock !== 'object') return sendJSON(res, 400, { error: 'bad data' });
        for (const [id, val] of Object.entries(body.stock)) {
          if (!MENU.find(m => m.id === id)) continue;
          if (val === null || val === '') db.stock[id] = null;
          else { const n = parseInt(val, 10); db.stock[id] = isNaN(n) ? null : Math.max(0, n); }
        }
        await persist('stock');
        broadcast('menu', publicMenu());
        return sendJSON(res, 200, db.stock);
      }
      if (p === '/api/admin/settings' && method === 'GET') return sendJSON(res, 200, db.settings);
      if (p === '/api/admin/settings' && method === 'POST') {
        const body = await readBody(req);
        if (!body) return sendJSON(res, 400, { error: 'bad data' });
        if (body.shopName) db.settings.shopName = String(body.shopName).slice(0, 60);
        // adminPin is intentionally NOT settable from this endpoint — change it by
        // editing DEFAULT_SETTINGS.adminPin (or the persisted settings file) in code.
        if (body.qrImage === null) db.settings.qrImage = null;
        else if (typeof body.qrImage === 'string' && body.qrImage.startsWith('data:image')) {
          db.settings.qrImage = saveDataUrlImage(body.qrImage, 'shop-qr-' + Date.now() + '.' + extFromDataUrl(body.qrImage));
        }
        if (body.logoImage === null) db.settings.logoImage = null;
        else if (typeof body.logoImage === 'string' && body.logoImage.startsWith('data:image')) {
          db.settings.logoImage = saveDataUrlImage(body.logoImage, 'shop-logo-' + Date.now() + '.' + extFromDataUrl(body.logoImage));
        }
        if (body.itemImages && typeof body.itemImages === 'object') {
          if (!db.settings.itemImages) db.settings.itemImages = {};
          for (const [id, val] of Object.entries(body.itemImages)) {
            if (!MENU.find(m => m.id === id)) continue;
            if (val === null) db.settings.itemImages[id] = null;
            else if (typeof val === 'string' && val.startsWith('data:image')) {
              db.settings.itemImages[id] = saveDataUrlImage(val, 'item-' + id + '-' + Date.now() + '.' + extFromDataUrl(val));
            }
          }
        }
        await persist('settings');
        broadcast('settings', publicSettings());
        return sendJSON(res, 200, db.settings);
      }
      if (p === '/api/admin/backup' && method === 'GET') {
        return sendJSON(res, 200, { exportedAt: new Date().toISOString(), settings: db.settings, stock: db.stock, counter: db.counter, orders: db.orders });
      }
      if (p === '/api/admin/backup/import' && method === 'POST') {
        const body = await readBody(req);
        if (!body || !Array.isArray(body.orders)) return sendJSON(res, 400, { error: 'bad format' });
        if (body.settings) db.settings = { ...DEFAULT_SETTINGS, ...body.settings };
        if (body.stock) db.stock = body.stock;
        if (body.counter) db.counter = body.counter;
        const byId = new Map(db.orders.map(o => [o.id, o]));
        for (const o of body.orders) byId.set(o.id, o);
        db.orders = [...byId.values()];
        await Promise.all([persist('settings'), persist('stock'), persist('counter'), persist('orders')]);
        broadcast('menu', publicMenu());
        broadcast('settings', publicSettings());
        return sendJSON(res, 200, { ok: true, imported: body.orders.length });
      }
      return sendJSON(res, 404, { error: 'not found' });
    }

    /* ---- static front-end ---- */
    if (method === 'GET') {
      let filePath = path.join(PUBLIC_DIR, p === '/' ? 'index.html' : p);
      if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403); res.end(); return; }
      if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) filePath = path.join(PUBLIC_DIR, 'index.html');
      return serveStatic(res, filePath);
    }

    return sendJSON(res, 404, { error: 'not found' });
  } catch (e) {
    console.error(e);
    try { sendJSON(res, 500, { error: 'internal error' }); } catch (e2) { }
  }
});

server.listen(PORT, () => {
  console.log(`\n🍢  หมาล่าทอด server กำลังทำงานที่ http://localhost:${PORT}`);
  console.log(`📁  ข้อมูลทั้งหมดถูกเก็บไว้ที่: ${DATA_DIR}\n`);
});
