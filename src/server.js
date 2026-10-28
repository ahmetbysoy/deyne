'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { MarketDnaRuntime } = require('./engine/marketDnaRuntime');

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml'
};

function createAppServer(options = {}) {
  const publicDir = options.publicDir || path.join(__dirname, '..', 'public');
  const runtime = new MarketDnaRuntime({
    dataDir: options.dataDir,
    backgroundIncubation: options.backgroundIncubation !== false // default true on server startup so initial coin warms up cleanly; toggleable from UI!
  });

  const initialCoin = options.initialCoin || process.env.DNA_INITIAL_COIN || 'BTCUSDT';
  runtime.selectCoin(initialCoin);

  // Broadcast real-time events to connected SSE clients
  const broadcastSse = (eventName, dataObj) => {
    if (!runtime.activeStreams.size) return;
    const msg = `event: ${eventName}\ndata: ${JSON.stringify(dataObj)}\n\n`;
    for (const res of runtime.activeStreams) {
      try { res.write(msg); } catch (_) { runtime.activeStreams.delete(res); }
    }
  };

  runtime.on('state_tick', (snap) => broadcastSse('state_tick', snap));
  runtime.on('dna_event', (ev) => broadcastSse('dna_event', ev));

  const keepAliveTimer = setInterval(() => {
    for (const res of runtime.activeStreams) {
      try { res.write(': keepalive\n\n'); } catch (_) { runtime.activeStreams.delete(res); }
    }
  }, 12_000);
  if (keepAliveTimer.unref) keepAliveTimer.unref();

  function sendJson(res, statusCode, payload) {
    const body = JSON.stringify(payload);
    res.writeHead(statusCode, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end(body);
  }

  function readJsonBody(req) {
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', (chunk) => {
        data += chunk;
        if (data.length > 65536) {
          req.destroy();
          reject(new Error('Payload too large'));
        }
      });
      req.on('end', () => {
        if (!data) return resolve({});
        try { resolve(JSON.parse(data)); } catch (e) { reject(e); }
      });
      req.on('error', reject);
    });
  }

  const server = http.createServer(async (req, res) => {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
      });
      return res.end();
    }

    const parsedUrl = new URL(req.url || '/', 'http://localhost');
    const pathname = parsedUrl.pathname;

    try {
      if (req.method === 'GET' && pathname === '/api/state') {
        const cid = parsedUrl.searchParams.get('clientId');
        if (cid) runtime.registerClientHeartbeat(cid);
        return sendJson(res, 200, runtime.getSummaryPayload());
      }

      if (req.method === 'GET' && pathname === '/api/motifs') {
        return sendJson(res, 200, runtime.getMotifsPayload());
      }

      if (req.method === 'GET' && pathname === '/api/episodes') {
        return sendJson(res, 200, runtime.getEpisodesPayload(80));
      }

      if (req.method === 'GET' && pathname === '/api/stream') {
        const cid = parsedUrl.searchParams.get('clientId') || `sse_${Date.now()}`;
        runtime.registerClientHeartbeat(cid);

        res.writeHead(200, {
          'Content-Type': 'text/event-stream; charset=utf-8',
          'Cache-Control': 'no-cache, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
          'Access-Control-Allow-Origin': '*'
        });
        res.write(`event: state_tick\ndata: ${JSON.stringify(runtime.getSummaryPayload())}\n\n`);
        runtime.activeStreams.add(res);

        req.on('close', () => {
          runtime.activeStreams.delete(res);
          runtime.releaseClientSession(cid);
        });
        return;
      }

      if (req.method === 'POST' && pathname === '/api/coin') {
        const body = await readJsonBody(req);
        if (!body.symbol || !/^[A-Za-z0-9]{4,16}$/.test(String(body.symbol))) {
          return sendJson(res, 400, { error: 'Geçersiz coin sembolü.' });
        }
        const state = runtime.selectCoin(body.symbol);
        broadcastSse('state_tick', state);
        return sendJson(res, 200, state);
      }

      if (req.method === 'POST' && pathname === '/api/settings') {
        const body = await readJsonBody(req);
        if (typeof body.backgroundIncubation === 'boolean') {
          runtime.setBackgroundIncubation(body.backgroundIncubation);
        }
        if (body.sources && typeof body.sources === 'object') {
          runtime.feeds.updateEnabledSources(body.sources);
        }
        const state = runtime.getSummaryPayload();
        broadcastSse('state_tick', state);
        return sendJson(res, 200, state);
      }

      if (req.method === 'POST' && pathname === '/api/heartbeat') {
        const body = await readJsonBody(req);
        if (body.clientId) runtime.registerClientHeartbeat(body.clientId);
        return sendJson(res, 200, { ok: true, serverTime: Date.now() });
      }

      if (req.method === 'POST' && pathname === '/api/session/end') {
        const body = await readJsonBody(req);
        if (body.clientId) runtime.releaseClientSession(body.clientId);
        return sendJson(res, 200, { ok: true });
      }

      if (req.method === 'POST' && pathname === '/api/flush') {
        const info = runtime.flushActiveCoinToDisk('manual_ui_flush');
        return sendJson(res, 200, { ok: true, saveInfo: info });
      }

      // Static frontend files (separated HTML, CSS, JS)
      const relPath = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
      const safePath = path.normalize(path.join(publicDir, relPath));
      if (!safePath.startsWith(publicDir) || !fs.existsSync(safePath) || fs.statSync(safePath).isDirectory()) {
        return sendJson(res, 404, { error: 'Not found' });
      }
      const ext = path.extname(safePath).toLowerCase();
      const contentType = MIME_TYPES[ext] || 'application/octet-stream';
      const content = fs.readFileSync(safePath);
      res.writeHead(200, {
        'Content-Type': contentType,
        'Cache-Control': 'no-cache'
      });
      return res.end(content);
    } catch (err) {
      return sendJson(res, 500, { error: err.message || 'Internal server error' });
    }
  });

  const closeAll = () => {
    clearInterval(keepAliveTimer);
    for (const res of runtime.activeStreams) {
      try { res.end(); } catch (_) {}
    }
    runtime.activeStreams.clear();
    runtime.shutdown('server_closing');
    server.close();
  };

  return { server, runtime, closeAll };
}

if (require.main === module) {
  const port = Number(process.env.PORT) || 3030;
  const host = '0.0.0.0';
  const app = createAppServer({
    backgroundIncubation: process.env.DNA_BG_INCUBATION !== 'false'
  });

  app.server.listen(port, host, () => {
    console.log(`[Market DNA V1] Server listening on http://${host}:${port} (Active Coin: ${app.runtime.activeSymbol})`);
  });

  process.on('SIGTERM', () => { app.closeAll(); process.exit(0); });
  process.on('SIGINT', () => { app.closeAll(); process.exit(0); });
}

module.exports = { createAppServer };
