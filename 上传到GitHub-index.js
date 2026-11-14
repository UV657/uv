/**
 * 枪战对决 · 联机中继服务
 * 只做两件事：配对（房间码）和转发。战斗判定仍在各自客户端，服务端不跑游戏逻辑。
 * 免费层冷启动慢，所以加 /health 供平台探活。
 */
const { WebSocketServer } = require('ws');
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_ROOM = 2;
const rooms = new Map();          // code -> { a, b, createdAt }

/* ---- 昵称注册表：全网唯一，跨设备生效 ----
 * 存在服务器进程内存 + 落盘到 data.json（免费实例重启会丢，但正常运行时是权威）。 */
const fs = require('fs');
const path = require('path');
const NICK_FILE = path.join(__dirname, 'data.json');
let nicks = {};
try { nicks = JSON.parse(fs.readFileSync(NICK_FILE, 'utf8')) || {}; } catch (e) { nicks = {}; }
let saveTimer = null;
function persistNicks() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(NICK_FILE, JSON.stringify(nicks), () => {});
  }, 500);
}

function newCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';   // 去掉易混淆的 I/O/0/1
  let c;
  do {
    c = Array.from({ length: 5 }, () => chars[crypto.randomInt(chars.length)]).join('');
  } while (rooms.has(c));
  return c;
}

// 健康检查页：平台探活 + 人肉确认服务活着
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, clients: wss ? wss.clients.size : 0 }));
    return;
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.bucket = 60;                 // 每秒消息额度，防刷
  ws.room = null;
  ws.side = null;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    // 限速：每秒 60 条足够 30Hz 状态同步
    ws.bucket--;
    if (ws.bucket <= 0) return;
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    if (msg.t === 'create') {
      if (ws.room) leave(ws);
      const code = newCode();
      rooms.set(code, { a: ws, b: null, createdAt: Date.now() });
      ws.room = code; ws.side = 'a';
      send(ws, { t: 'room', code, side: 'a' });
      return;
    }

    if (msg.t === 'join') {
      const code = String(msg.code || '').toUpperCase().trim();
      const r = rooms.get(code);
      if (!r) return send(ws, { t: 'err', msg: '房间不存在或已关闭' });
      if (r.a && r.b) return send(ws, { t: 'err', msg: '房间已满' });
      if (r.a === ws) return send(ws, { t: 'err', msg: '不能加入自己的房间' });
      if (ws.room) leave(ws);
      r.b = ws; ws.room = code; ws.side = 'b';
      send(ws, { t: 'joined', code, side: 'b' });
      send(r.a, { t: 'peer', joined: true });
      return;
    }

    /* 昵称校验：客户端登录时问一次，服务器是唯一权威 */
    if (msg.t === 'nick') {
      const name = String(msg.name || '').trim().slice(0, 16);
      const uid = String(msg.uid || '').slice(0, 20);
      if (!name || !uid) return send(ws, { t: 'nickRes', ok: false, msg: '代号或档案号为空' });
      const owner = nicks[name];
      if (owner && owner !== uid) return send(ws, { t: 'nickRes', ok: false, msg: '这个代号已被其他玩家占用' });
      if (!owner) { nicks[name] = uid; persistNicks(); }
      return send(ws, { t: 'nickRes', ok: true, name });
    }

    if (msg.t === 'leave') { leave(ws); return; }

    // 其余类型（state / shoot / hit / hello）一律原样转发给房间对手
    const r = rooms.get(ws.room);
    if (!r) return;
    const peer = ws.side === 'a' ? r.b : r.a;
    if (peer && peer.readyState === 1) send(peer, msg);
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => leave(ws));

  function leave(w) {
    const r = w.room && rooms.get(w.room);
    if (r) {
      const peer = w.side === 'a' ? r.b : r.a;
      if (peer && peer.readyState === 1) send(peer, { t: 'peer', joined: false });
      if (w.side === 'a') rooms.delete(w.room);
      else r.b = null;
    }
    w.room = null; w.side = null;
  }
});

function send(ws, obj) {
  if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj));
}

// 每秒重置额度 + 每 30 秒清死连接和超时房间
setInterval(() => {
  wss.clients.forEach((ws) => { ws.bucket = 60; });
}, 1000);

setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false; ws.ping();
  });
  const now = Date.now();
  for (const [code, r] of rooms) {
    if (!r.a && !r.b) rooms.delete(code);
    else if (!r.b && now - r.createdAt > 30 * 60 * 1000) rooms.delete(code);  // 空房 30 分钟回收
  }
}, 30000);

server.listen(PORT, () => console.log('relay listening on', PORT));
