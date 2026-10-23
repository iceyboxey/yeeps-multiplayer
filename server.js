'use strict';

const path = require('node:path');
const { randomUUID } = require('node:crypto');
const express = require('express');
const { createServer } = require('node:http');
const { WebSocketServer, WebSocket } = require('ws');

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2048 });
const PORT = Number(process.env.PORT) || 10000;
const MAX_PLAYERS_PER_ROOM = 24;
const rooms = new Map();

app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true }));

function cleanText(value, maxLength, fallback) {
  if (typeof value !== 'string') return fallback;
  const cleaned = value.replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, maxLength);
  return cleaned || fallback;
}

function send(socket, message) {
  if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(message));
}

function snapshot(room, exceptId = null) {
  return [...room.values()]
    .filter((player) => player.id !== exceptId)
    .map(({ id, name, x, y, z, facing }) => ({ id, name, x, y, z, facing }));
}

function broadcast(room, message, exceptId = null) {
  for (const player of room.values()) {
    if (player.id !== exceptId) send(player.socket, message);
  }
}

function removePlayer(player) {
  const room = rooms.get(player.room);
  if (!room || !room.has(player.id)) return;
  room.delete(player.id);
  broadcast(room, { type: 'leave', id: player.id });
  if (room.size === 0) rooms.delete(player.room);
}

wss.on('connection', (socket, request) => {
  const url = new URL(request.url, 'http://localhost');
  const roomCode = cleanText(url.searchParams.get('room'), 20, 'baseplate').toLowerCase();
  const name = cleanText(url.searchParams.get('name'), 18, 'Yeep');
  let room = rooms.get(roomCode);
  if (!room) {
    room = new Map();
    rooms.set(roomCode, room);
  }
  if (room.size >= MAX_PLAYERS_PER_ROOM) {
    send(socket, { type: 'error', message: 'That room is full. Try another room code.' });
    socket.close(1013, 'Room full');
    return;
  }

  const id = randomUUID();
  const player = { id, name, room: roomCode, socket, x: 0, y: 0, z: 0, facing: 0 };
  send(socket, { type: 'welcome', id, room: roomCode, players: snapshot(room) });
  room.set(id, player);
  broadcast(room, { type: 'join', player: { id, name, x: 0, y: 0, z: 0, facing: 0 } }, id);

  socket.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (message.type !== 'move' || !message.position || typeof message.position !== 'object') return;
    const { x, y, z } = message.position;
    const facing = message.facing;
    if (![x, y, z, facing].every(Number.isFinite)) return;
    player.x = Math.max(-198, Math.min(198, x));
    player.y = Math.max(0, Math.min(20, y));
    player.z = Math.max(-198, Math.min(198, z));
    player.facing = Math.max(-Math.PI * 4, Math.min(Math.PI * 4, facing));
    broadcast(room, {
      type: 'move',
      player: { id, name, x: player.x, y: player.y, z: player.z, facing: player.facing },
    }, id);
  });

  socket.on('close', () => removePlayer(player));
  socket.on('error', () => removePlayer(player));
});

const heartbeat = setInterval(() => {
  for (const socket of wss.clients) {
    if (socket.isAlive === false) {
      socket.terminate();
      continue;
    }
    socket.isAlive = false;
    socket.ping();
  }
}, 30000);
wss.on('connection', (socket) => {
  socket.isAlive = true;
  socket.on('pong', () => { socket.isAlive = true; });
});
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Yeeps multiplayer listening on port ${PORT}`);
});
