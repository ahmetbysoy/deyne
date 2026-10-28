'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');
const assert = require('assert');
const { createAppServer } = require('../src/server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function httpJson(port, method, urlPath, bodyObj = null) {
  return new Promise((resolve, reject) => {
    const raw = bodyObj ? JSON.stringify(bodyObj) : null;
    const req = http.request({
      hostname: '127.0.0.1',
      port,
      path: urlPath,
      method,
      headers: raw
        ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(raw) }
        : {}
    }, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; });
      res.on('end', () => {
        try { resolve({ status: res.statusCode, json: JSON.parse(data) }); }
        catch (e) { reject(e); }
      });
    });
    req.on('error', reject);
    if (raw) req.write(raw);
    req.end();
  });
}

async function main() {
  console.log('[VERIFY PHASE 7 E2E] Starting full end-to-end live audit suite...');
  const e2eDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dna-phase7-e2e-'));

  // 1) Boot Server #1 on ephemeral port with BTCUSDT
  const app1 = createAppServer({ dataDir: e2eDataDir, initialCoin: 'BTCUSDT', backgroundIncubation: true });
  await new Promise((r) => app1.server.listen(0, '127.0.0.1', r));
  const port1 = app1.server.address().port;

  console.log('[VERIFY PHASE 7 E2E] 1/5 Streaming live BTCUSDT across 3 venues for 15s...');
  await sleep(15_000);

  const btcSnap1 = (await httpJson(port1, 'GET', '/api/state')).json;
  const btcMotifs1 = (await httpJson(port1, 'GET', '/api/motifs')).json;

  assert.strictEqual(btcSnap1.activeSymbol, 'BTCUSDT');
  assert.strictEqual(btcSnap1.feeds.integrity.liveVenues, 3, 'All 3 venues must be LIVE & synced on BTCUSDT');
  assert(btcSnap1.compressionStats.cleanTransitionEvents >= 3, 'Live BTCUSDT must encode real atomic transitions');
  assert.strictEqual(btcSnap1.lifecycleCounts.GRADUATED, 0, 'Short live window MUST NOT prematurely graduate any motif');
  assert.strictEqual(btcSnap1.baseModelAudit.leakageViolationsDetected, 0, 'Zero data leakage violations allowed');

  // 2) Forced WebSocket disconnect -> <RESET> barrier + open episode invalidation + auto-reconnect
  console.log('[VERIFY PHASE 7 E2E] 2/5 Forcing BINANCE_FUT socket drop to verify <RESET> boundary & auto-recovery...');
  const futAdapter = app1.runtime.feeds.adapters.BINANCE_FUT;
  if (futAdapter.mainWs) futAdapter.mainWs.close();
  await sleep(4500);

  const afterDropSnap = (await httpJson(port1, 'GET', '/api/state')).json;
  const resetObserved = app1.runtime.encoder.events.some((e) => e.code === '<RESET>');
  assert(resetObserved, 'Forced socket disconnect must emit <RESET> barrier event');
  assert.strictEqual(afterDropSnap.feeds.venues.BINANCE_FUT.status, 'LIVE', 'BINANCE_FUT must auto-reconnect to LIVE');

  // 3) Live Coin Switch Isolation: BTCUSDT -> ETHUSDT -> BTCUSDT
  console.log('[VERIFY PHASE 7 E2E] 3/5 Switching BTCUSDT -> ETHUSDT -> BTCUSDT to verify isolation & memory...');
  const ethSwitch = (await httpJson(port1, 'POST', '/api/coin', { symbol: 'ETHUSDT' })).json;
  assert.strictEqual(ethSwitch.activeSymbol, 'ETHUSDT');
  await sleep(6000);

  const ethLive = (await httpJson(port1, 'GET', '/api/state')).json;
  assert.strictEqual(ethLive.activeSymbol, 'ETHUSDT');
  assert(ethLive.market.mid > 100 && ethLive.market.mid < 20000, `ETHUSDT mid price (${ethLive.market.mid}) must reflect ETH, never BTC`);
  assert.strictEqual(ethLive.feeds.venues.OKX_SWAP.ctVal, 0.1, 'OKX_SWAP ctVal for ETHUSDT must be 0.1');

  const btcReturn = (await httpJson(port1, 'POST', '/api/coin', { symbol: 'BTCUSDT' })).json;
  assert.strictEqual(btcReturn.activeSymbol, 'BTCUSDT');
  assert.strictEqual(btcReturn.sessionNumber, 2, 'Returning to BTCUSDT must resume Session #2');

  // 4) Full Server Shutdown & Cold Restart -> <SESSION_GAP> & Disk Persistence Continuity
  console.log('[VERIFY PHASE 7 E2E] 4/5 Shutting down server & cold-restarting from disk to verify <SESSION_GAP>...');
  const motifCountBeforeShutdown = btcMotifs1.motifs.length;
  app1.closeAll();
  await sleep(600);

  const app2 = createAppServer({ dataDir: e2eDataDir, initialCoin: 'BTCUSDT', backgroundIncubation: true });
  await new Promise((r) => app2.server.listen(0, '127.0.0.1', r));
  const port2 = app2.server.address().port;

  const coldBootSnap = (await httpJson(port2, 'GET', '/api/state')).json;
  const coldBootMotifs = (await httpJson(port2, 'GET', '/api/motifs')).json;

  assert.strictEqual(coldBootSnap.sessionNumber, 3, 'Cold restart must resume BTCUSDT at Session #3');
  assert.strictEqual(coldBootSnap.memoryStatus.lastRecoveryReport.loadedFrom, 'PRIMARY');
  assert(coldBootSnap.recentEvents.some((e) => e.code === '<SESSION_GAP>'), 'Cold restart must inject <SESSION_GAP>');
  assert(coldBootMotifs.motifs.length >= motifCountBeforeShutdown, 'Previously discovered motifs must persist across server restart');
  assert.strictEqual(coldBootSnap.memoryStatus.storedCoins.length, 2, 'Both BTCUSDT and ETHUSDT memories must persist on disk');

  app2.closeAll();
  fs.rmSync(e2eDataDir, { recursive: true, force: true });

  // 5) Inspect Continuous Live Preview Server (:3030) Telemetry & Base Model Scoreboard
  console.log('[VERIFY PHASE 7 E2E] 5/5 Sampling continuous live server on port 3030...');
  const live3030 = (await httpJson(3030, 'GET', '/api/state')).json;
  assert.strictEqual(live3030.baseModelAudit.leakageViolationsDetected, 0);
  assert.strictEqual(live3030.lifecycleCounts.GRADUATED, 0);

  const evidence = {
    verifiedAt: new Date().toISOString(),
    e2eChecks: {
      threeVenueLiveSyncVerified: true,
      forcedWsDisconnectEmittedReset: resetObserved,
      autoReconnectToLiveVerified: true,
      coinSwitchIsolationVerified: {
        ethMidObserved: ethLive.market.mid,
        ethOkxCtVal: ethLive.feeds.venues.OKX_SWAP.ctVal,
        btcResumedSessionAfterSwitch: btcReturn.sessionNumber
      },
      coldServerRestartVerified: {
        resumedSessionNumber: coldBootSnap.sessionNumber,
        loadedFrom: coldBootSnap.memoryStatus.lastRecoveryReport.loadedFrom,
        sessionGapInjected: true,
        persistedMotifCount: coldBootMotifs.motifs.length,
        storedCoins: coldBootSnap.memoryStatus.storedCoins.map((c) => c.symbol)
      },
      dataLeakageAudit: {
        leakageChecksPassed: live3030.baseModelAudit.leakageChecksPassed,
        leakageViolationsDetected: live3030.baseModelAudit.leakageViolationsDetected
      },
      continuousLiveServerSnapshot: {
        activeSymbol: live3030.activeSymbol,
        mid: live3030.market.mid,
        spreadBps: live3030.market.spreadBps,
        decisionBadge: live3030.decisionCard.badge,
        lifecycleCounts: live3030.lifecycleCounts,
        compressionStats: live3030.compressionStats,
        resolvedEpisodesCount: live3030.resolvedEpisodesCount,
        baseModelHorizon10s: live3030.baseModelAudit.horizons['10'],
        baseModelHorizon30s: live3030.baseModelAudit.horizons['30']
      }
    }
  };

  const outPath = path.join(__dirname, '..', 'audits', 'phase7_e2e_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(evidence, null, 2));
  console.log('[VERIFY PHASE 7 E2E] ALL 5 LIVE E2E SCENARIOS PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 7 E2E FAILED]:', err);
  process.exit(1);
});
