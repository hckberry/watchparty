const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// --- serve index.html on every HTTP request ---
const server = http.createServer((req, res) => {
  const file = path.join(__dirname, 'index.html');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(500); return res.end('missing index.html'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

// --- signaling ---
const wss = new WebSocketServer({ server });
const rooms = new Map();

const genCode = () => crypto.randomBytes(4).toString('hex').slice(0, 6).toUpperCase();

wss.on('connection', (ws) => {
  ws.id = crypto.randomBytes(4).toString('hex');
  ws.room = null;

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'create') {
      let code = genCode();
      while (rooms.has(code)) code = genCode();
      rooms.set(code, new Set([ws]));
      ws.room = code;
      ws.send(JSON.stringify({ type: 'created', room: code, id: ws.id }));
      return;
    }

    if (m.type === 'join') {
      const code = String(m.room || '').toUpperCase();
      const set = rooms.get(code);
      if (!set) return ws.send(JSON.stringify({ type: 'error', msg: 'Room not found' }));
      if (set.size >= 8) return ws.send(JSON.stringify({ type: 'error', msg: 'Room full' }));
      set.add(ws);
      ws.room = code;
      ws.send(JSON.stringify({ type: 'joined', room: code, id: ws.id }));
      for (const peer of set) {
        if (peer !== ws && peer.readyState === 1) {
          peer.send(JSON.stringify({ type: 'peer-joined', id: ws.id }));
        }
      }
      return;
    }

    if (['offer', 'answer', 'ice', 'chat', 'sync', 'meta'].includes(m.type)) {
      const set = rooms.get(ws.room);
      if (!set) return;
      for (const peer of set) {
        if (peer !== ws && peer.readyState === 1) {
          peer.send(JSON.stringify({ ...m, from: ws.id }));
        }
      }
    }
  });

  ws.on('close', () => {
    const set = rooms.get(ws.room);
    if (!set) return;
    set.delete(ws);
    for (const peer of set) {
      if (peer.readyState === 1) peer.send(JSON.stringify({ type: 'peer-left', id: ws.id }));
    }
    if (set.size === 0) rooms.delete(ws.room);
  });
});

server.listen(PORT, () => console.log('up on', PORT));