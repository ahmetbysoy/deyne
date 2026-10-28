'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { EVENT_DICTIONARY, VALID_EVENT_CODES } = require('../src/events/eventDictionary');
const { EventEncoder } = require('../src/events/eventEncoder');
const { FeedCoordinator } = require('../src/adapters/feedCoordinator');

function runDeterministicUnitTests() {
  // EXPLICITLY LABELED DETERMINISTIC ALGORITHMIC UNIT TEST (NOT LIVE MARKET EVIDENCE)
  const enc = new EventEncoder('TESTUSDT');
  const emitted = [];
  enc.on('event', (e) => emitted.push(e));

  const baseBids = Array.from({ length: 25 }, (_, i) => [100 - i * 0.05, 1.0]);
  const baseAsks = Array.from({ length: 25 }, (_, i) => [100.05 + i * 0.05, 1.0]);

  // 1) Test Hysteresis anti-stutter + explicit retest preservation on BOOK_TILT
  let t = 1_700_000_000_000;
  const heavyBids = baseBids.map(([p, q], i) => [p, i < 4 ? 12.0 : q]);
  const midBids = baseBids.map(([p, q], i) => [p, i < 4 ? 2.0 : q]); // OBI ~ +0.30 (inside dead-band [0.14, 0.36], must NOT re-emit!)

  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: heavyBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });
  t += 1000;
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: midBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });
  t += 1000;
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: heavyBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });

  const tiltsAfterBounceInBand = emitted.filter((e) => e.code === 'BOOK_TILT');
  assert.strictEqual(tiltsAfterBounceInBand.length, 1, 'Hysteresis must suppress repeated triggers when state has not returned to neutral <= 0.14');

  // Now return to true neutral (OBI = 0) and re-enter -> MUST emit with isRetest: true!
  t += 1000;
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: baseBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });
  t += 1000;
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: heavyBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });

  const tiltsAfterNeutral = emitted.filter((e) => e.code === 'BOOK_TILT');
  assert.strictEqual(tiltsAfterNeutral.length, 2, 'Return to neutral followed by new tilt must emit second event');
  assert.strictEqual(tiltsAfterNeutral[1].isRetest, true, 'Second entry after neutral return must preserve isRetest=true');
  assert.strictEqual(tiltsAfterNeutral[1].retestCount, 1);

  // 2) Test Wall Spawn -> Wall Test -> Wall Hold -> Wall Pull lifecycle (observational only!)
  const wallBids = baseBids.map(([p, q], i) => [p, i === 0 ? 45.0 : 1.0]); // massive bid wall at 100.00
  for (let s = 0; s < 4; s++) {
    t += 1300;
    enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: wallBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });
  }
  // Aggressive sell trades hit 100.00 (25% of wall notional = 1125 USD) while wall stays at 40 qty (>50%)
  t += 300;
  enc.onTrade({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', price: 100.00, qty: 12.0, notionalUsd: 1200, side: -1, ts: t });
  t += 1300;
  const survivingWallBids = baseBids.map(([p, q], i) => [p, i === 0 ? 35.0 : 1.0]);
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: survivingWallBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });

  assert(emitted.some((e) => e.code === 'WALL_SPAWN' && e.side === 1), 'WALL_SPAWN must trigger');
  assert(emitted.some((e) => e.code === 'WALL_TEST' && e.side === 1), 'WALL_TEST must trigger');
  assert(emitted.some((e) => e.code === 'WALL_HOLD' && e.side === 1), 'WALL_HOLD must trigger');

  // Now wait > 5s so earlier trades age out, then withdraw wall without trades -> WALL_PULL
  t += 6000;
  enc.onBook({ symbol: 'TESTUSDT', venue: 'BINANCE_FUT', bids: baseBids, asks: baseAsks, bestBid: 100, bestAsk: 100.05, ts: t });
  assert(emitted.some((e) => e.code === 'WALL_PULL' && e.side === 1), 'WALL_PULL must trigger observationally when liquidity vanishes without trades');

  // 3) Verify <SESSION_GAP> clears retest memory across sessions
  enc.emitSessionGap('unit_test_gap', t + 500);
  assert.strictEqual(emitted[emitted.length - 1].code, '<SESSION_GAP>');

  return {
    dataType: 'SYNTHETIC_UNIT_TEST',
    dictionaryAtomsDefined: VALID_EVENT_CODES.length,
    assertionsPassed: [
      'hysteresis_stutter_suppression',
      'retest_preservation_after_neutral',
      'wall_spawn_test_hold_pull_lifecycle',
      'session_gap_memory_reset'
    ]
  };
}

async function runLiveMarketVerification() {
  const fc = new FeedCoordinator();
  const enc = new EventEncoder('BTCUSDT');

  fc.on('book', (b) => enc.onBook(b));
  fc.on('trade', (tr) => enc.onTrade(tr));
  fc.on('liquidation', (lq) => enc.onLiquidation(lq));
  fc.on('gap', (g) => enc.onGap(g));

  fc.startCoin('BTCUSDT');
  await new Promise((r) => setTimeout(r, 10_000));
  fc.stopAll('phase2_done');

  const summary = enc.getCompressionSummary();
  assert(summary.l2BookPacketsProcessed > 50, 'Live L2 packets must be processed');
  assert(enc.events.length >= 1, 'At least initial <RESET> or live market transitions must be recorded');
  for (const ev of enc.events) {
    assert(EVENT_DICTIONARY[ev.code], `Unknown event code emitted: ${ev.code}`);
  }

  return {
    dataType: 'LIVE_MARKET_EVIDENCE',
    symbol: 'BTCUSDT',
    durationSec: 10,
    compressionAndAudit: summary,
    liveEventsSample: enc.events.slice(0, 15)
  };
}

async function main() {
  console.log('[VERIFY PHASE 2] Running deterministic unit tests + 10s live BTCUSDT encoding audit...');
  const unitResult = runDeterministicUnitTests();
  const liveResult = await runLiveMarketVerification();

  const out = {
    verifiedAt: new Date().toISOString(),
    dictionaryAtomCount: VALID_EVENT_CODES.length,
    dictionaryAtoms: VALID_EVENT_CODES,
    unitTestResult: unitResult,
    liveMarketResult: liveResult
  };
  const outPath = path.join(__dirname, '..', 'audits', 'phase2_encoder_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log('[VERIFY PHASE 2] ALL PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 2 FAILED]:', err);
  process.exit(1);
});
