'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const express = require('express');
const { createServer } = require('node:http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const MAX_PLAYERS_PER_ROOM = 24;
const MAX_ITEMS_PER_ROOM = 60;
// Admin key for the ban tool. Without it nobody can ban anyone.
const ADMIN_KEY = process.env.ADMIN_KEY || '';
// Bans and dropped items are saved here. On Render, point this at a persistent disk
// (for example DATA_DIR=/var/data), otherwise they reset whenever the service restarts.
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');

const DROPPABLE = new Set(['banana', 'mango', 'flashlight', 'keycard', 'ball']);
const HELD_KINDS = new Set([...DROPPABLE, 'banglass']);

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4096 });
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true }));

/* ---------------- persistence ---------------- */
function readJSON(file, fallback) {
  try { return JSON.parse(fs.readFileSync(path.join(DATA_DIR, file), 'utf8')); } catch { return fallback; }
}
const timers = {};
function writeJSON(file, value) {
  clearTimeout(timers[file]);
  timers[file] = setTimeout(() => {
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(path.join(DATA_DIR, file), JSON.stringify(value));
    } catch (err) {
      console.warn('Could not save ' + file + ':', err.message);
    }
  }, 1500);
}

const bans = new Map();       // key -> { key, cid, ip, name, at }
for (const b of readJSON('bans.json', [])) if (b && b.key) bans.set(b.key, b);
const roomItems = new Map();  // room -> Map(itemId -> item)
for (const [room, list] of Object.entries(readJSON('items.json', {}))) {
  roomItems.set(room, new Map((list || []).map((it) => [it.id, it])));
}
const saveBans = () => writeJSON('bans.json', [...bans.values()]);
const saveItems = () => {
  const out = {};
  for (const [room, items] of roomItems) out[room] = [...items.values()];
  writeJSON('items.json', out);
};

/* ---------------- helpers ---------------- */
const rooms = new Map();

function cleanText(value, maxLength, fallback) {
  if (typeof value !== 'string') return fallback;
  const cleaned = value.replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, maxLength);
  return cleaned || fallback;
}
const cleanId = (v) => (typeof v === 'string' && /^[A-Za-z0-9_-]{6,64}$/.test(v) ? v : null);
function getIp(request) {
  const fwd = String(request.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const ip = fwd || (request.socket && request.socket.remoteAddress) || '';
  return ip.replace(/^::ffff:/, '');
}
function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}
function broadcast(room, message, exceptId = null) {
  for (const player of room.values()) if (player.id !== exceptId) send(player.socket, message);
}
const finiteArray = (a, n, limit) =>
  Array.isArray(a) && a.length === n && a.every((v) => typeof v === 'number' && Number.isFinite(v) && Math.abs(v) <= limit);
const round3 = (a) => a.map((v) => Math.round(v * 1000) / 1000);

function sameKey(a, b) {
  const ha = crypto.createHash('sha256').update(String(a)).digest();
  const hb = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}
function banFor(cid, ip) {
  let byCid = null, byIp = null;
  for (const b of bans.values()) {
    if (cid && b.cid && b.cid === cid) byCid = b;
    else if (ip && b.ip && b.ip === ip) byIp = b;
  }
  return { byCid, byIp };
}

function cleanVRPose(value) {
  if (!value || typeof value !== 'object') return null;
  const pose = {};
  const head = value.H;
  if (head && finiteArray(head.p, 3, 20) && finiteArray(head.q, 4, 1)) {
    pose.H = { p: head.p, q: head.q };
  } else {
    pose.H = null;
  }
  for (const side of ['L', 'R']) {
    const hand = value[side];
    if (!hand || !finiteArray(hand.p, 3, 20) || !finiteArray(hand.q, 4, 1) || !finiteArray(hand.c, 3, 1)) {
      pose[side] = null;
      continue;
    }
    pose[side] = { p: hand.p, q: hand.q, c: hand.c.map((n) => Math.max(0, Math.min(1, n))) };
  }
  return pose;
}
function cleanHeld(value) {
  const out = { l: null, r: null };
  if (value && typeof value === 'object') {
    if (HELD_KINDS.has(value.l)) out.l = value.l;
    if (HELD_KINDS.has(value.r)) out.r = value.r;
  }
  return out;
}
function publicPlayer(p) {
  return { id: p.id, name: p.name, x: p.x, y: p.y, z: p.z, facing: p.facing, vr: p.vr, held: p.held, tint: !!p.tint };
}
function itemsFor(roomCode) {
  return [...(roomItems.get(roomCode) || new Map()).values()];
}

/* ---------------- tag game ---------------- */
const tags = new Map(); // room code -> { it, lastTag, startedAt, times: Map(id -> seconds as IT), names: Map(id -> name) }
const TAG_RANGE = 1.3;       // metres
const TAG_COOLDOWN = 2000;   // ms before the new IT can tag again
const TAG_ROUND = 180;       // seconds
function tagPublic(code) {
  const t = tags.get(code);
  if (!t) return { active: false, it: null, left: 0 };
  return { active: true, it: t.it, left: Math.max(0, Math.ceil(TAG_ROUND - (Date.now() - t.startedAt) / 1000)) };
}
function tagBroadcast(code) {
  const room = rooms.get(code);
  if (room) broadcast(room, { type: 'tagState', ...tagPublic(code) });
}
function tagStart(code) {
  const room = rooms.get(code);
  if (!room || room.size < 2) return false;
  const ids = [...room.keys()];
  const t = { it: ids[Math.floor(Math.random() * ids.length)], lastTag: Date.now(), startedAt: Date.now(), times: new Map(), names: new Map() };
  for (const p of room.values()) { t.times.set(p.id, 0); t.names.set(p.id, p.name); }
  tags.set(code, t);
  tagBroadcast(code);
  return true;
}
function tagEnd(code) {
  const t = tags.get(code);
  if (!t) return;
  tags.delete(code);
  const room = rooms.get(code);
  if (!room) return;
  const scores = [...t.times.entries()].map(([id, secs]) => ({ name: t.names.get(id) || 'Yeep', secs }));
  scores.sort((a, b) => a.secs - b.secs);
  broadcast(room, { type: 'tagEnd', scores });
  tagBroadcast(code);
}
function tagCheck(code) {
  const t = tags.get(code);
  const room = rooms.get(code);
  if (!t || !room) return;
  const it = room.get(t.it);
  const now = Date.now();
  if (!it || now - t.lastTag < TAG_COOLDOWN) return;
  for (const p of room.values()) {
    if (p.id === it.id) continue;
    if (Math.hypot(p.x - it.x, p.z - it.z) < TAG_RANGE && Math.abs(p.y - it.y) < 2) {
      const from = t.it;
      t.it = p.id;
      t.lastTag = now;
      broadcast(room, { type: 'tag', it: p.id, from });
      tagBroadcast(code);
      break;
    }
  }
}
setInterval(() => {
  for (const [code, t] of tags) {
    const room = rooms.get(code);
    if (!room) { tags.delete(code); continue; }
    t.times.set(t.it, (t.times.get(t.it) || 0) + 1);
    for (const p of room.values()) { t.names.set(p.id, p.name); if (!t.times.has(p.id)) t.times.set(p.id, 0); }
    if ((Date.now() - t.startedAt) / 1000 >= TAG_ROUND) tagEnd(code);
  }
}, 1000);

/* ---------------- items ---------------- */
function addItem(roomCode, item) {
  let items = roomItems.get(roomCode);
  if (!items) { items = new Map(); roomItems.set(roomCode, items); }
  const removed = [];
  while (items.size >= MAX_ITEMS_PER_ROOM) {
    const oldest = items.keys().next().value;
    items.delete(oldest);
    removed.push(oldest);
  }
  items.set(item.id, item);
  saveItems();
  return removed;
}

function removePlayer(player) {
  const room = rooms.get(player.room);
  if (!room || !room.has(player.id)) return;
  room.delete(player.id);
  clearTimeout(player.kickTimer);
  broadcast(room, { type: 'leave', id: player.id });
  const tg = tags.get(player.room);
  if (tg) {
    if (room.size < 2) tagEnd(player.room);
    else if (tg.it === player.id) {
      const ids = [...room.keys()];
      tg.it = ids[Math.floor(Math.random() * ids.length)];
      tg.lastTag = Date.now();
      tagBroadcast(player.room);
    }
  }
  if (room.size === 0) { rooms.delete(player.room); tags.delete(player.room); }
}

/* ---------------- connections ---------------- */
wss.on('connection', (socket, request) => {
  const url = new URL(request.url, 'http://localhost');
  const roomCode = cleanText(url.searchParams.get('room'), 20, 'baseplate').toLowerCase();
  const name = cleanText(url.searchParams.get('name'), 18, 'Yeep');
  const cid = cleanId(url.searchParams.get('cid'));
  const ip = getIp(request);

  const ban = banFor(cid, ip);
  if (ban.byCid) {
    send(socket, { type: 'banned' });
    socket.close(4003, 'Banned');
    return;
  }

  let room = rooms.get(roomCode);
  if (!room) { room = new Map(); rooms.set(roomCode, room); }
  if (room.size >= MAX_PLAYERS_PER_ROOM) {
    send(socket, { type: 'error', message: 'That room is full. Try another room code.' });
    socket.close(1013, 'Room full');
    return;
  }

  const id = crypto.randomUUID();
  const player = {
    id, name, room: roomCode, socket, cid, ip,
    x: 0, y: 1.2, z: 0, facing: 0, vr: null, held: { l: null, r: null },
    admin: false, authFails: 0, tokens: 80, tokenAt: Date.now(), kickTimer: null,
  };
  send(socket, {
    type: 'welcome', id, room: roomCode,
    players: [...room.values()].map(publicPlayer),
    items: itemsFor(roomCode),
    tag: tagPublic(roomCode),
  });
  room.set(id, player);
  { const t = tags.get(roomCode); if (t) { t.times.set(id, 0); t.names.set(id, name); } }
  broadcast(room, { type: 'join', player: publicPlayer(player) }, id);

  // Same network as a banned player but a different device: allowed only if the client proves it is an admin.
  if (ban.byIp) {
    player.kickTimer = setTimeout(() => {
      if (player.admin) return;
      send(socket, { type: 'banned' });
      socket.close(4003, 'Banned');
    }, 4000);
  }

  socket.on('message', (raw) => {
    const now = Date.now();
    player.tokens = Math.min(80, player.tokens + ((now - player.tokenAt) / 1000) * 60);
    player.tokenAt = now;
    if (player.tokens < 1) return;
    player.tokens -= 1;

    let message;
    try { message = JSON.parse(raw.toString()); } catch { return; }
    if (!message || typeof message.type !== 'string') return;

    switch (message.type) {
      case 'move': {
        if (!message.position || typeof message.position !== 'object') return;
        const { x, y, z } = message.position;
        const facing = message.facing;
        if (![x, y, z, facing].every(Number.isFinite)) return;
        player.x = Math.max(-1700, Math.min(1700, x));
        player.y = Math.max(0, Math.min(40, y));
        player.z = Math.max(-1700, Math.min(1700, z));
        player.facing = Math.max(-Math.PI * 4, Math.min(Math.PI * 4, facing));
        player.vr = cleanVRPose(message.vr);
        player.held = cleanHeld(message.held);
        player.tint = message.tint === true;
        broadcast(room, { type: 'move', player: publicPlayer(player) }, id);
        tagCheck(roomCode);
        break;
      }

      case 'auth': {
        // No ADMIN_KEY set: anyone who opens the game with ?addmode is an admin.
        if (!ADMIN_KEY || (typeof message.key === 'string' && sameKey(message.key, ADMIN_KEY))) {
          player.admin = true;
          clearTimeout(player.kickTimer);
          send(socket, { type: 'admin', ok: true });
        } else {
          player.authFails++;
          send(socket, { type: 'admin', ok: false, reason: 'Wrong admin key' });
          if (player.authFails >= 5) socket.close(4001, 'Too many attempts');
        }
        break;
      }

      case 'ban': {
        if (!player.admin) { send(socket, { type: 'banResult', ok: false, reason: 'Not authorised' }); break; }
        const target = room.get(String(message.id));
        if (!target || target === player || target.admin) {
          send(socket, { type: 'banResult', ok: false, reason: 'That player cannot be banned' });
          break;
        }
        const record = { key: crypto.randomBytes(4).toString('hex'), cid: target.cid, ip: target.ip, name: target.name, at: Date.now() };
        bans.set(record.key, record);
        saveBans();
        send(target.socket, { type: 'banned' });
        target.socket.close(4003, 'Banned');
        send(socket, { type: 'banResult', ok: true, name: target.name });
        send(socket, { type: 'bans', bans: [...bans.values()].map(({ key, name, at }) => ({ key, name, at })) });
        break;
      }

      case 'listBans': {
        if (!player.admin) break;
        send(socket, { type: 'bans', bans: [...bans.values()].map(({ key, name, at }) => ({ key, name, at })) });
        break;
      }

      case 'unban': {
        if (!player.admin) break;
        bans.delete(String(message.key));
        saveBans();
        send(socket, { type: 'bans', bans: [...bans.values()].map(({ key, name, at }) => ({ key, name, at })) });
        break;
      }

      case 'tagStart': {
        if (!player.admin) break;
        if (tags.has(roomCode)) break;
        if (!tagStart(roomCode)) send(socket, { type: 'tagError', reason: 'Tag needs at least 2 players' });
        break;
      }

      case 'tagStop': {
        if (!player.admin) break;
        tagEnd(roomCode);
        break;
      }

      case 'unbanAll': {
        if (!player.admin) break;
        bans.clear();
        saveBans();
        send(socket, { type: 'bans', bans: [] });
        break;
      }

      case 'itemDrop': {
        const itemId = message.id;
        if (typeof itemId !== 'string' || !/^[A-Za-z0-9_-]{3,48}$/.test(itemId)) break;
        if (!DROPPABLE.has(message.kind)) break;
        if (!finiteArray(message.p, 3, 2000) || !finiteArray(message.q, 4, 2) || !finiteArray(message.v, 3, 20)) break;
        if ((roomItems.get(roomCode) || new Map()).has(itemId)) break;
        const item = { id: itemId, kind: message.kind, p: round3(message.p), q: round3(message.q), owner: id, rest: false };
        const removed = addItem(roomCode, item);
        removed.forEach((rid) => broadcast(room, { type: 'itemGone', id: rid }));
        broadcast(room, { type: 'itemSpawn', item: { ...item, v: round3(message.v) } }, id);
        break;
      }

      case 'itemRest': {
        const item = (roomItems.get(roomCode) || new Map()).get(message.id);
        if (!item || item.owner !== id) break;
        if (!finiteArray(message.p, 3, 2000) || !finiteArray(message.q, 4, 2)) break;
        item.p = round3(message.p);
        item.q = round3(message.q);
        item.rest = true;
        saveItems();
        broadcast(room, { type: 'itemRest', id: item.id, p: item.p, q: item.q }, id);
        break;
      }

      case 'itemTake': {
        const items = roomItems.get(roomCode);
        if (items && items.has(message.id)) {
          items.delete(message.id);
          saveItems();
          broadcast(room, { type: 'itemGone', id: message.id, by: id });
        } else {
          send(socket, { type: 'itemDenied', id: message.id });
        }
        break;
      }
      default:
    }
  });

  socket.on('close', () => removePlayer(player));
  socket.on('error', () => removePlayer(player));
});

const heartbeat = setInterval(() => {
  for (const socket of wss.clients) {
    if (socket.isAlive === false) { socket.terminate(); continue; }
    socket.isAlive = false;
    socket.ping();
  }
}, 30000);
wss.on('connection', (socket) => {
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });
});
wss.on('close', () => clearInterval(heartbeat));

if (!ADMIN_KEY) console.warn('ADMIN_KEY is not set: anyone using ?addmode can ban players.');
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Yeeps multiplayer listening on port ${PORT}`);
});
