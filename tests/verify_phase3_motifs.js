'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { buildCandidateSequences } = require('../src/motifs/sequenceBuilder');
const { EpisodeManager } = require('../src/motifs/episodeManager');
const { MotifRegistry } = require('../src/motifs/motifRegistry');
const { FeedCoordinator } = require('../src/adapters/feedCoordinator');
const { EventEncoder } = require('../src/events/eventEncoder');

function runUnitTests() {
  // EXPLICITLY LABELED DETERMINISTIC ALGORITHMIC UNIT TEST (NOT LIVE MARKET EVIDENCE)
  // 1) Barrier & Cross-venue clock jitter test
  const evs = [
    { id: 'e1', venue: 'BINANCE_SPOT', code: 'BOOK_TILT', side: 1, isRetest: false, ts: 1000, cumTradeCount: 5, cumVolumeUsd: 10000, mid: 80000, spreadBps: 0.5 },
    { id: 'e2', venue: 'BINANCE_FUT', code: 'FLOW_BURST', side: 1, isRetest: false, ts: 1015, cumTradeCount: 9, cumVolumeUsd: 45000, mid: 80000, spreadBps: 0.5 }
  ];
  // With 35ms uncertainty, dt=15ms MUST be flagged SIMULTANEOUS_WITHIN_JITTER (~ prefix)
  const seqJitter = buildCandidateSequences(evs, () => 35);
  assert.strictEqual(seqJitter.length, 1);
  assert.strictEqual(seqJitter[0].hasUncertainCrossVenueOrder, true);
  assert.strictEqual(seqJitter[0].clocks[0].orderingConfidence, 'SIMULTANEOUS_WITHIN_JITTER');

  // With 5ms uncertainty, dt=15ms is VERIFIED_CROSS_VENUE_ORDER
  const seqVerified = buildCandidateSequences(evs, () => 5);
  assert.strictEqual(seqVerified[0].hasUncertainCrossVenueOrder, false);
  assert.strictEqual(seqVerified[0].clocks[0].orderingConfidence, 'VERIFIED_CROSS_VENUE_ORDER');

  // Insert <SESSION_GAP> -> must block sequence extraction across gap!
  evs.push({ id: 'g1', venue: 'SYSTEM', code: '<SESSION_GAP>', side: 0, ts: 1100, cumTradeCount: 9, cumVolumeUsd: 45000 });
  evs.push({ id: 'e3', venue: 'BINANCE_FUT', code: 'MICRO_FLIP', side: 1, isRetest: false, ts: 1200, cumTradeCount: 10, cumVolumeUsd: 46000, mid: 80001, spreadBps: 0.5 });
  const afterGap = buildCandidateSequences(evs, () => 5);
  assert.strictEqual(afterGap.length, 0, 'Must NEVER chain events across <SESSION_GAP>');

  // 2) Episodic cooldown, concurrency overlap, and integrity invalidation
  const em = new EpisodeManager('TESTUSDT', { minSameMotifGapMs: 10_000 });
  const reg = new MotifRegistry('TESTUSDT');
  em.on('episode_opened', (ep) => reg.registerEpisodeOpen(ep));
  em.on('horizon_resolved', ({ episode, horizonSec, slot }) => reg.recordHorizonResolution(episode, horizonSec, slot));

  const ep1 = em.tryOpenEpisode(seqVerified[0]);
  assert(ep1 !== null, 'First episode must open');
  // Attempt duplicate trigger 500ms later -> MUST be rejected!
  const dupSeq = { ...seqVerified[0], events: seqVerified[0].events.map((x) => ({ ...x, ts: x.ts + 500 })) };
  const epDup = em.tryOpenEpisode(dupSeq);
  assert.strictEqual(epDup, null, '500ms repeat trigger for same motif must be suppressed');

  // Open a DIFFERENT motif 1000ms later -> overlaps with ep1!
  const otherEvents = [
    { id: 'x1', venue: 'BINANCE_FUT', code: 'WALL_SPAWN', side: 1, isRetest: false, ts: 2000, cumTradeCount: 12, cumVolumeUsd: 50000, mid: 80000, spreadBps: 0.5 },
    { id: 'x2', venue: 'BINANCE_FUT', code: 'WALL_HOLD', side: 1, isRetest: false, ts: 2500, cumTradeCount: 20, cumVolumeUsd: 90000, mid: 80000, spreadBps: 0.5 }
  ];
  const otherSeq = buildCandidateSequences(otherEvents, () => 5)[0];
  const ep2 = em.tryOpenEpisode(otherSeq);
  assert(ep2.concurrentEpisodeIds.includes(ep1.episodeId), 'Overlapping episode IDs must be cross-linked');
  assert(ep1.concurrentEpisodeIds.includes(ep2.episodeId), 'First episode must record overlapping second episode');

  // Resolve 10s horizon -> concurrencyWeight must be 0.5 (2 overlapping episodes!)
  em.onPriceUpdate(80025, 13_000, 1.0);
  assert.strictEqual(ep1.horizons[10].status, 'RESOLVED');
  assert.strictEqual(ep1.horizons[10].concurrencyWeight, 0.5);
  assert.strictEqual(ep1.horizons[10].outcomeClass, 'UP');

  // Trigger <RESET> while 30s and 120s horizons are still PENDING -> must mark INTEGRITY_BROKEN!
  const brokenCount = em.invalidateOpenHorizons('primary_stream_reset', 14_000);
  assert.strictEqual(brokenCount, 2);
  assert.strictEqual(ep1.horizons[30].status, 'INTEGRITY_BROKEN');
  assert.strictEqual(ep1.integrityStatus, 'INTEGRITY_BROKEN');

  // 3) Suffix pruning test: create k=2 suffix and redundant k=3 extension with identical outcomes
  const regPrune = new MotifRegistry('TESTUSDT');
  const sufId = '+FLOW_BURST -> +WALL_HOLD';
  const extId = '+BOOK_TILT -> +FLOW_BURST -> +WALL_HOLD';
  regPrune.motifs.set(sufId, {
    motifId: sufId, lengthK: 2, suffixId: null, prunedAsRedundant: false, lifecycleState: 'INCUBATION',
    horizonLedgers: { 10: { pos: [], neg: [] }, 30: { pos: [], neg: [] }, 120: { pos: [], neg: [] } }
  });
  regPrune.motifs.set(extId, {
    motifId: extId, lengthK: 3, suffixId: sufId, prunedAsRedundant: false, lifecycleState: 'INCUBATION',
    horizonLedgers: { 10: { pos: [], neg: [] }, 30: { pos: [], neg: [] }, 120: { pos: [], neg: [] } }
  });
  for (let i = 0; i < 14; i++) {
    const cls = i % 2 === 0 ? 'UP' : 'FLAT';
    for (const h of [10, 30, 120]) {
      regPrune.motifs.get(sufId).horizonLedgers[h].pos.push({ anchorSide: 1, outcomeClass: cls, concurrencyWeight: 1.0 });
      regPrune.motifs.get(extId).horizonLedgers[h].pos.push({ anchorSide: 1, outcomeClass: cls, concurrencyWeight: 1.0 });
    }
  }
  const pruned = regPrune.runSuffixPruning(10);
  assert.strictEqual(pruned, 1, 'Redundant k=3 motif with zero information gain over suffix must be pruned');
  assert.strictEqual(regPrune.motifs.get(extId).lifecycleState, 'RETIRED');

  return {
    dataType: 'SYNTHETIC_UNIT_TEST',
    assertionsPassed: [
      'cross_venue_jitter_guard',
      'session_gap_barrier_isolation',
      'episodic_500ms_duplicate_suppression',
      'concurrency_overlap_weighting',
      'pending_horizon_integrity_invalidation',
      'redundant_suffix_pruning'
    ]
  };
}

async function runLivePhase3Test() {
  const fc = new FeedCoordinator();
  const enc = new EventEncoder('BTCUSDT');
  const em = new EpisodeManager('BTCUSDT');
  const reg = new MotifRegistry('BTCUSDT');

  em.on('episode_opened', (ep) => reg.registerEpisodeOpen(ep));
  em.on('horizon_resolved', ({ episode, horizonSec, slot }) => reg.recordHorizonResolution(episode, horizonSec, slot));

  fc.on('book', (b) => {
    enc.onBook(b);
    if (b.venue === 'BINANCE_FUT') {
      em.onPriceUpdate((b.bestBid + b.bestAsk) / 2, b.ts, 2.0);
    }
  });
  fc.on('trade', (t) => enc.onTrade(t));
  fc.on('gap', (g) => {
    enc.onGap(g);
    if (g.venue === 'ALL' || g.venue === 'BINANCE_FUT' && g.reason !== 'ws_open_sync') {
      em.invalidateOpenHorizons(g.reason, g.ts);
    }
  });

  enc.on('event', (ev) => {
    em.onSubsequentEvent(ev);
    const seqs = buildCandidateSequences(enc.events, (vA, vB) => fc.getCrossVenueUncertaintyMs(vA, vB));
    for (const cs of seqs) em.tryOpenEpisode(cs);
  });

  fc.startCoin('BTCUSDT');
  await new Promise((r) => setTimeout(r, 13_000));
  fc.stopAll('phase3_done');

  return {
    dataType: 'LIVE_MARKET_EVIDENCE',
    symbol: 'BTCUSDT',
    durationSec: 13,
    eventsEncoded: enc.events.length,
    motifsDiscovered: reg.motifs.size,
    openEpisodesCount: em.openEpisodes.length,
    resolved10sCount: Array.from(reg.motifs.values()).reduce((s, m) => s + m.horizonLedgers[10].pos.length + m.horizonLedgers[10].neg.length, 0),
    sampleMotifs: Array.from(reg.motifs.values()).slice(0, 6).map((m) => ({
      motifId: m.motifId,
      lengthK: m.lengthK,
      suffixId: m.suffixId,
      lifecycleState: m.lifecycleState,
      totalTriggered: m.totalTriggered,
      clockSummary: m.clockSummary
    }))
  };
}

async function main() {
  console.log('[VERIFY PHASE 3] Testing SequenceBuilder + EpisodeManager + MotifRegistry...');
  const unit = runUnitTests();
  const live = await runLivePhase3Test();
  const out = { verifiedAt: new Date().toISOString(), unit, live };
  const outPath = path.join(__dirname, '..', 'audits', 'phase3_motifs_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log('[VERIFY PHASE 3] ALL PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 3 FAILED]:', err);
  process.exit(1);
});
