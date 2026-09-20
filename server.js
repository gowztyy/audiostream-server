// Servidor de sinalização para o AudioStream
// Função: parear um "transmissor" (tx) e um "receptor" (rx) que usem o mesmo ID e senha,
// e repassar as mensagens de sinalização WebRTC (offer/answer/candidate) entre eles.
// O áudio em si NÃO passa por este servidor.

const WebSocket = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const wss = new WebSocket.Server({ port: PORT });

// rooms: id -> { senhaHash, tx: ws|null, rx: ws|null }
const rooms = new Map();

function hash(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

wss.on('connection', (ws) => {
  ws.role = null;
  ws.roomId = null;

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    if (msg.type === 'register') {
      const { role, id, senha } = msg;
      if (!id || !senha || (role !== 'tx' && role !== 'rx')) {
        send(ws, { type: 'registered', ok: false, reason: 'dados invalidos' });
        return;
      }

      const senhaHash = hash(senha);
      let room = rooms.get(id);

      if (!room) {
        room = { senhaHash, tx: null, rx: null };
        rooms.set(id, room);
      }

      if (room.senhaHash !== senhaHash) {
        send(ws, { type: 'registered', ok: false, reason: 'senha incorreta' });
        return;
      }

      // Substitui conexão antiga no mesmo papel, se houver (ex: reconexão)
      if (room[role]) {
        try { room[role].close(); } catch (e) {}
      }

      room[role] = ws;
      ws.role = role;
      ws.roomId = id;
      send(ws, { type: 'registered', ok: true });

      const other = role === 'tx' ? room.rx : room.tx;
      if (other) {
        send(other, { type: 'peer_joined' });
        send(ws, { type: 'peer_joined' });
      }
      return;
    }

    // Repassa sinalização (offer/answer/candidate) para o outro lado da mesma sala
    if (['offer', 'answer', 'candidate'].includes(msg.type)) {
      const room = rooms.get(ws.roomId);
      if (!room) return;
      const other = ws.role === 'tx' ? room.rx : room.tx;
      send(other, msg);
    }
  });

  ws.on('close', () => {
    const room = rooms.get(ws.roomId);
    if (!room) return;
    const other = ws.role === 'tx' ? room.rx : room.tx;
    send(other, { type: 'peer_left' });
    if (room[ws.role] === ws) room[ws.role] = null;
    if (!room.tx && !room.rx) rooms.delete(ws.roomId);
  });
});

console.log('Servidor de sinalização rodando na porta ' + PORT);
