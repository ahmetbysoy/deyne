'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const assert = require('assert');
const { CoinMemoryStore } = require('../src/storage/coinMemoryStore');
const { MarketDnaRuntime } = require('../src/engine/marketDnaRuntime');

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function main() {
  console.log('[VERIFY PHASE 5] Testing Atomic B+ CoinMemoryStore + Corruption Recovery + Staleness Warm-Up + Live Switch...');
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dna-phase5-'));
  const store = new CoinMemoryStore(tmpDir);

  // 1) Save two sequential snapshots so backup exists, plus valid + truncated JSONL lines
  store.saveCoinState('BTCUSDT', {
    sessionNumber: 1,
    motifs: [{ motifId: '+WALL_HOLD -> +FLOW_EXHAUST', lifecycleState: 'GRADUATED', warmingUp: false, warmUpRemaining: 0 }]
  });
  store.saveCoinState('BTCUSDT', {
    sessionNumber: 2,
    motifs: [{ motifId: '+WALL_HOLD -> +FLOW_EXHAUST', lifecycleState: 'GRADUATED', warmingUp: false, warmUpRemaining: 0 }, { motifId: '+BOOK_TILT -> +MICRO_FLIP', lifecycleState: 'INCUBATION' }]
  });

  store.appendJsonl('BTCUSDT', 'events', { id: 'ev_1', code: 'WALL_SPAWN', ts: 1000 });
  // Simulate power-loss half-written JSONL line!
  const segFile = path.join(store.coinDir('BTCUSDT'), 'events.seg_0001.jsonl');
  fs.appendFileSync(segFile, '{"id":"ev_broken","code":"FLOW_BUR', 'utf8');
  store.appendJsonl('BTCUSDT', 'events', { id: 'ev_2', code: 'WALL_HOLD', ts: 2000 });

  // Deliberately corrupt primary state.v1.json!
  const primaryPath = path.join(store.coinDir('BTCUSDT'), 'state.v1.json');
  fs.writeFileSync(primaryPath, '{ "schemaVersion": 1, "checksum": "bad", "payload": { CORRUPTED! ', 'utf8');

  // Load 8 hours later -> must recover from BACKUP_FALLBACK, skip 1 corrupt JSONL line, and assign STALE_LONG_GAP warm-up!
  const eightHoursLater = Date.now() + 8 * 3600 * 1000;
  const recovered = store.loadCoinState('BTCUSDT', eightHoursLater);

  assert.strictEqual(recovered.recoveryReport.corruptionDetected, true);
  assert.strictEqual(recovered.recoveryReport.loadedFrom, 'BACKUP_FALLBACK');
  assert.strictEqual(recovered.recoveryReport.corruptJsonlLinesSkipped, 1);
  assert.strictEqual(recovered.recoveryReport.stalenessLevel, 'STALE_LONG_GAP');
  assert.strictEqual(recovered.recoveryReport.warmUpEpisodesAssigned, 4);
  assert.strictEqual(recovered.payload.motifs[0].warmingUp, true);
  assert.strictEqual(recovered.payload.motifs[0].warmUpRemaining, 4);
  assert.strictEqual(recovered.recentEvents.length, 2, 'Valid JSONL lines before & after corrupt line must be preserved');

  // 2) Live MarketDnaRuntime coin switch & persistence test (BTCUSDT -> SOLUSDT -> BTCUSDT)
  const liveStoreDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dna-runtime-'));
  const rt = new MarketDnaRuntime({
    dataDir: liveStoreDir,
    backgroundIncubation: true,
    autoSaveIntervalMs: 5000
  });

  rt.selectCoin('BTCUSDT');
  await sleep(6000);
  const btcEventsBefore = rt.encoder.events.length;
  assert(btcEventsBefore >= 2, 'BTCUSDT live session must record events');

  // Switch to SOLUSDT
  rt.selectCoin('SOLUSDT');
  await sleep(5000);
  assert.strictEqual(rt.activeSymbol, 'SOLUSDT');
  const solEvents = rt.encoder.events.length;
  assert(solEvents >= 2, 'SOLUSDT live session must record events independently');

  // Switch back to BTCUSDT -> must restore sessionNumber=2 and inject <SESSION_GAP>!
  const resumedBtc = rt.selectCoin('BTCUSDT');
  assert.strictEqual(resumedBtc.sessionNumber, 2, 'Returning to BTCUSDT must increment sessionNumber to 2');
  assert.strictEqual(resumedBtc.memoryStatus.lastRecoveryReport.loadedFrom, 'PRIMARY');
  assert(resumedBtc.recentEvents.some((e) => e.code === '<SESSION_GAP>'), '<SESSION_GAP> must be injected on session resume');
  assert.strictEqual(resumedBtc.memoryStatus.storedCoins.length, 2, 'Both BTCUSDT and SOLUSDT must have isolated disk memories');

  // Test idle pause when backgroundIncubation = false and zero clients connected
  rt.setBackgroundIncubation(false);
  assert.strictEqual(rt.pausedForIdle, true, 'With backgroundIncubation=false and 0 clients, runtime must pause cleanly');

  rt.shutdown('phase5_test_complete');

  const evidence = {
    verifiedAt: new Date().toISOString(),
    corruptionAndCrashRecovery: recovered.recoveryReport,
    liveCoinSwitchContinuity: {
      btcEventsBeforeSwitch: btcEventsBefore,
      solEventsRecorded: solEvents,
      resumedBtcSessionNumber: resumedBtc.sessionNumber,
      storedCoinsOnDisk: resumedBtc.memoryStatus.storedCoins.map((c) => c.symbol),
      idlePauseWhenBackgroundOffVerified: true
    }
  };

  const outPath = path.join(__dirname, '..', 'audits', 'phase5_storage_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(evidence, null, 2));
  fs.rmSync(tmpDir, { recursive: true, force: true });
  fs.rmSync(liveStoreDir, { recursive: true, force: true });
  console.log('[VERIFY PHASE 5] ALL PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 5 FAILED]:', err);
  process.exit(1);
});
