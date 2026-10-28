'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const assert = require('assert');

const BASE = 'http://127.0.0.1:3030';
const VALID_BADGES = new Set([
  'GÖZLEMDE',
  'MOTİF KEŞFEDİLDİ',
  'KULUÇKADA',
  'DOĞRULANIYOR',
  'MEZUN MOTİF AKTİF',
  'YAPISAL GEÇERSİZLİK',
  'VERİ KOPUK'
]);

function httpGetText(urlPath, headers = {}) {
  return new Promise((resolve, reject) => {
    http.get(BASE + urlPath, { headers }, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
    }).on('error', reject);
  });
}

function httpPostJson(urlPath, payload) {
  return new Promise((resolve, reject) => {
    const raw = JSON.stringify(payload);
    const req = http.request(BASE + urlPath, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(raw) }
    }, (res) => {
      let body = '';
      res.on('data', (c) => { body += c; });
      res.on('end', () => resolve({ status: res.statusCode, json: JSON.parse(body) }));
    });
    req.on('error', reject);
    req.write(raw);
    req.end();
  });
}

function sampleSseEvents(durationMs = 4500) {
  return new Promise((resolve, reject) => {
    const events = [];
    const req = http.get(BASE + '/api/stream?clientId=verify_sse_client', (res) => {
      assert.strictEqual(res.statusCode, 200);
      assert(String(res.headers['content-type']).includes('text/event-stream'));
      let buf = '';
      res.on('data', (chunk) => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n\n')) !== -1) {
          const frame = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          const evLine = frame.split('\n').find((l) => l.startsWith('event: '));
          if (evLine) events.push(evLine.slice(7).trim());
        }
      });
    });
    req.on('error', reject);
    setTimeout(() => {
      req.destroy();
      resolve(events);
    }, durationMs);
  });
}

async function main() {
  console.log('[VERIFY PHASE 6] Auditing Live API + SSE + Separated Mobile Frontend...');

  // 1) Static asset separation & proxy-safety check
  const html = await httpGetText('/');
  const css = await httpGetText('/styles.css');
  const js = await httpGetText('/app.js');
  assert.strictEqual(html.status, 200);
  assert.strictEqual(css.status, 200);
  assert.strictEqual(js.status, 200);
  assert(!js.body.includes('localhost') && !js.body.includes('127.0.0.1'), 'Frontend JS must use relative URLs only (no localhost)');
  assert(html.body.includes('tab-market') && html.body.includes('tab-dna') && html.body.includes('tab-lab') && html.body.includes('tab-memory') && html.body.includes('tab-settings'), 'All 5 required mobile tabs must exist');
  assert(html.body.includes('Seçili Coini Arka Planda İzlemeye Devam Et'), 'Background incubation toggle must exist in UI');

  // 2) Live SSE push verification
  const sseFrameTypes = await sampleSseEvents(4500);
  assert(sseFrameTypes.includes('state_tick'), 'SSE stream must push live state_tick frames');

  // 3) REST API endpoints verification
  const stateRes = await httpGetText('/api/state?clientId=verify_rest');
  const stateJson = JSON.parse(stateRes.body);
  assert.strictEqual(stateRes.status, 200);
  assert(VALID_BADGES.has(stateJson.decisionCard.badge), `Invalid badge: ${stateJson.decisionCard.badge}`);

  const motifsRes = await httpGetText('/api/motifs');
  const motifsJson = JSON.parse(motifsRes.body);
  assert.strictEqual(motifsRes.status, 200);
  assert.strictEqual(Object.keys(motifsJson.dictionary).length, 13);

  const epRes = await httpGetText('/api/episodes');
  assert.strictEqual(epRes.status, 200);

  // 4) Coin switch API -> ETHUSDT -> back to BTCUSDT
  const switchEth = await httpPostJson('/api/coin', { symbol: 'ETHUSDT' });
  assert.strictEqual(switchEth.status, 200);
  assert.strictEqual(switchEth.json.activeSymbol, 'ETHUSDT');

  const switchBtc = await httpPostJson('/api/coin', { symbol: 'BTCUSDT' });
  assert.strictEqual(switchBtc.status, 200);
  assert.strictEqual(switchBtc.json.activeSymbol, 'BTCUSDT');

  // 5) Flush + Settings toggle check
  const flushRes = await httpPostJson('/api/flush', {});
  assert.strictEqual(flushRes.status, 200);
  assert(flushRes.json.saveInfo && flushRes.json.saveInfo.checksum);

  const settingsRes = await httpPostJson('/api/settings', { backgroundIncubation: true });
  assert.strictEqual(settingsRes.status, 200);
  assert.strictEqual(settingsRes.json.backgroundIncubation, true);

  const evidence = {
    verifiedAt: new Date().toISOString(),
    staticAssets: {
      htmlBytes: html.body.length,
      cssBytes: css.body.length,
      jsBytes: js.body.length,
      zeroHardcodedLocalhostInFrontend: true,
      fiveRequiredTabsPresent: true,
      backgroundIncubationTogglePresent: true
    },
    liveSseFramesReceivedIn4500ms: sseFrameTypes.length,
    liveDecisionBadge: stateJson.decisionCard.badge,
    coinSwitchRoundtripVerified: ['ETHUSDT', 'BTCUSDT'],
    manualDiskFlushChecksumPrefix: flushRes.json.saveInfo.checksum.slice(0, 16)
  };

  const outPath = path.join(__dirname, '..', 'audits', 'phase6_api_ui_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(evidence, null, 2));
  console.log('[VERIFY PHASE 6] ALL PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 6 FAILED]:', err);
  process.exit(1);
});
