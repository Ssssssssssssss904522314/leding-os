// LedingOS: сервер звонков. Без внешних зависимостей (только встроенный Node.js 18+).
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const HTML = fs.readFileSync(path.join(__dirname, 'index.html'));
const OK = /^\d{3}-\d{2}-\d{2}$/;
const clients = new Map(); // номер -> { id, res }
const calls = new Map();   // cid -> [номер1, номер2]
const cors = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type'
};

function push(num, msg) {
  const c = clients.get(num);
  if (!c) return false;
  c.res.write('data: ' + JSON.stringify(msg) + '\n\n');
  return true;
}

const srv = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');

  if (req.method === 'OPTIONS') { res.writeHead(204, cors); return res.end(); }

  if (u.pathname === '/' || u.pathname === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(HTML);
  }

  if (u.pathname === '/health') { res.writeHead(200, cors); return res.end('ok'); }

  // Регистрация номера: держим открытое соединение (Server-Sent Events)
  if (u.pathname === '/events' && req.method === 'GET') {
    const n = u.searchParams.get('n') || '';
    const id = u.searchParams.get('id') || '';
    if (!OK.test(n) || !id) { res.writeHead(400, cors); return res.end(); }

    const old = clients.get(n);
    if (old && old.id !== id) {
      // Номер уже занят другим устройством
      res.writeHead(200, { ...cors, 'Content-Type': 'text/event-stream' });
      res.write('data: ' + JSON.stringify({ t: 'taken' }) + '\n\n');
      return res.end();
    }
    if (old) old.res.end();

    res.writeHead(200, {
      ...cors,
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive'
    });
    res.write('data: ' + JSON.stringify({ t: 'hello' }) + '\n\n');
    const c = { id, res };
    clients.set(n, c);
    const ping = setInterval(() => res.write(': ping\n\n'), 25000);
    req.on('close', () => {
      clearInterval(ping);
      if (clients.get(n) === c) clients.delete(n);
    });
    return;
  }

  // Отправка сигнала звонка другой стороне
  if (u.pathname === '/send' && req.method === 'POST') {
    let body = '';
    req.on('data', d => { body += d; if (body.length > 100000) req.destroy(); });
    req.on('end', () => {
      let m;
      try { m = JSON.parse(body); } catch (e) { res.writeHead(400, cors); return res.end(); }
      const from = String(m.from || '');
      if (!OK.test(from) || !m.cid) { res.writeHead(400, cors); return res.end(); }

      if (m.t === 'ring') {
        const to = String(m.to || '');
        if (!OK.test(to)) { res.writeHead(400, cors); return res.end(); }
        calls.set(m.cid, [from, to]);
        setTimeout(() => calls.delete(m.cid), 5 * 60 * 1000);
        if (!push(to, { t: 'ring', from, to, cid: m.cid }))
          push(from, { t: 'nobody', cid: m.cid });
      } else {
        // accept / decline / hang / ringing / busy: пересылаем второй стороне звонка
        const p = calls.get(m.cid);
        if (!p || !p.includes(from)) { res.writeHead(204, cors); return res.end(); }
        const other = p[0] === from ? p[1] : p[0];
        push(other, { ...m, from });
      }
      res.writeHead(204, cors);
      res.end();
    });
    return;
  }

  res.writeHead(404, cors);
  res.end();
});

srv.listen(PORT, () => console.log('LedingOS server listening on port ' + PORT));
