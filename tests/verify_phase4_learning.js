'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { computeDeadZoneBps, classifyThreeStateOutcome } = require('../src/learning/outcomeLabeler');
const { computeEffectiveSampleMetrics } = require('../src/learning/effectiveSample');
const { BaseModelSuite } = require('../src/learning/baseModels');
const { MotifRegistry } = require('../src/motifs/motifRegistry');
const { QuarantineEngine, benjaminiHochbergAdjust } = require('../src/learning/quarantineEngine');
const { FeedCoordinator } = require('../src/adapters/feedCoordinator');
const { EventEncoder } = require('../src/events/eventEncoder');
const { EpisodeManager } = require('../src/motifs/episodeManager');
const { buildCandidateSequences } = require('../src/motifs/sequenceBuilder');

function runUnitTests() {
  // EXPLICITLY LABELED DETERMINISTIC ALGORITHMIC UNIT TEST (NOT LIVE MARKET EVIDENCE)
  // 1) Multi-horizon dead-zone scaling
  const dz10 = computeDeadZoneBps(10, 0.5, 2.0);
  const dz30 = computeDeadZoneBps(30, 0.5, 2.0);
  const dz120 = computeDeadZoneBps(120, 0.5, 2.0);
  assert(dz10 < dz30 && dz30 < dz120, 'FLAT dead-zone must expand across 10s < 30s < 120s horizons');
  assert.strictEqual(classifyThreeStateOutcome(100, 100.005, 10, 0.5, 2.0).label, 'FLAT');
  assert.strictEqual(classifyThreeStateOutcome(100, 100.08, 10, 0.5, 2.0).label, 'UP');

  // 2) Effective sample size: concurrency vs AR(1) serial correlation vs small sample
  const smallSample = computeEffectiveSampleMetrics([
    { t0: 1000, resolvedAt: 11000, concurrencyWeight: 0.5, anchorSignedMoveBps: 3 },
    { t0: 2000, resolvedAt: 12000, concurrencyWeight: 0.5, anchorSignedMoveBps: 3 }
  ]);
  assert.strictEqual(smallSample.smallSampleWarning, true);
  assert.strictEqual(smallSample.nUniq, 1.0);
  assert(smallSample.nEffFinal < smallSample.nRaw, 'nEffFinal must be strictly less than nRaw under overlap');

  // Positively autocorrelated smooth wave -> AR(1) factor must penalize below nUniq!
  const corrRecs = [];
  for (let i = 0; i < 16; i++) {
    const val = Math.sin(i * 0.35) * 10; // smooth positively autocorrelated sequence
    corrRecs.push({ t0: i * 15000, resolvedAt: i * 15000 + 10000, concurrencyWeight: 1.0, anchorSignedMoveBps: val });
  }
  const corrMetrics = computeEffectiveSampleMetrics(corrRecs);
  assert(corrMetrics.lag1Autocorr > 0.5, 'Positive lag-1 autocorrelation must be detected');
  assert(corrMetrics.nEffFinal < corrMetrics.nUniq * 0.5, 'Serial dependence must reduce nEffFinal well below nUniq');

  // 3) Strict Data Leakage Guard on BaseModelSuite
  const bm = new BaseModelSuite('TESTUSDT');
  const tNow = Date.now();
  const preds = bm.predictAll(tNow, 'NORMAL');
  assert(Math.abs(preds[10].leaderProbs.reduce((a, b) => a + b, 0) - 1.0) < 1e-4, 'P(DOWN)+P(FLAT)+P(UP) must equal 1');

  // Simulate state updated at tNow, then attempt lookahead prediction at tNow - 5000 -> MUST THROW!
  bm.updateFromResolved(10, 'UP', 'NORMAL', preds[10].models, tNow, 1.0);
  let leakageBlocked = false;
  try {
    bm.predictAll(tNow - 5000, 'NORMAL');
  } catch (e) {
    if (String(e.message).includes('DATA_LEAKAGE_VIOLATION')) leakageBlocked = true;
  }
  assert.strictEqual(leakageBlocked, true, 'Lookahead prediction using future-resolved state MUST throw DATA_LEAKAGE_VIOLATION');

  // 4) Benjamini-Hochberg FDR + Graduation + Quarantine demotion lifecycle (Synthetic algorithmic test)
  const reg = new MotifRegistry('TESTUSDT');
  const bm2 = new BaseModelSuite('TESTUSDT');
  const qe = new QuarantineEngine('TESTUSDT', reg, bm2);

  // Add 15 noise motifs so multiple-hypothesis BH-FDR penalty is actively engaged!
  for (let mIdx = 0; mIdx < 15; mIdx++) {
    const mId = `+NOISE_${mIdx} -> +MICRO_FLIP`;
    reg.registerEpisodeOpen({ motifId: mId, lengthK: 2, suffixId: null, t0: 1000, totalSpanMs: 500, totalTrades: 3, totalVolumeUsd: 1000 });
    for (let i = 0; i < 20; i++) {
      const cls = i % 3 === 0 ? 'UP' : (i % 3 === 1 ? 'FLAT' : 'DOWN');
      reg.motifs.get(mId).horizonLedgers[10].pos.push({
        episodeId: `n_${mIdx}_${i}`, t0: i * 20000, resolvedAt: i * 20000 + 10000,
        anchorSide: 1, regime: 'NORMAL', outcomeClass: cls, moveBps: 0, anchorSignedMoveBps: 0, deadZoneBps: 1.5,
        concurrencyWeight: 1.0, baseModelPredsAtEntry: { leaderProbs: [0.33, 0.34, 0.33] }, invalidationObservations: []
      });
    }
  }

  // Add 1 strong signal motif with 36 chronological non-overlapping episodes (so OOS slice=18 has sufficient power under 16-motif BH-FDR!)
  const sigId = '+WALL_HOLD -> +FLOW_EXHAUST';
  reg.registerEpisodeOpen({ motifId: sigId, lengthK: 2, suffixId: null, t0: 1000, totalSpanMs: 1200, totalTrades: 8, totalVolumeUsd: 25000 });
  for (let i = 0; i < 36; i++) {
    const isLoss = (i % 11 === 10);
    const cls = isLoss ? 'DOWN' : 'UP';
    const moveBps = isLoss ? -6.0 : 5.0 + ((i * 7) % 3) - 1;
    reg.motifs.get(sigId).horizonLedgers[10].pos.push({
      episodeId: `sig_${i}`, t0: i * 20000, resolvedAt: i * 20000 + 10000,
      anchorSide: 1, regime: i % 2 === 0 ? 'NORMAL' : 'VOLATILE',
      outcomeClass: cls, moveBps, anchorSignedMoveBps: moveBps, deadZoneBps: 1.5,
      concurrencyWeight: 1.0,
      baseModelPredsAtEntry: { leaderProbs: [0.30, 0.40, 0.30] },
      invalidationObservations: isLoss
        ? [{ tag: 'OPP:FLOW_BURST', ts: i * 20000 + 3000, moveBpsAtSignal: -1.2 }]
        : []
    });
  }

  qe.evaluateAllMotifs();
  assert.strictEqual(reg.motifs.get(sigId).lifecycleState, 'GRADUATED', 'Strong OOS motif surviving BH-FDR across 16 motifs must reach GRADUATED');
  for (let mIdx = 0; mIdx < 15; mIdx++) {
    assert.notStrictEqual(reg.motifs.get(`+NOISE_${mIdx} -> +MICRO_FLIP`).lifecycleState, 'GRADUATED', 'Noise motifs must NOT graduate');
  }

  // Now append 24 consecutive degraded/inverted episodes -> must automatically strip authority to QUARANTINED!
  for (let i = 36; i < 60; i++) {
    reg.motifs.get(sigId).horizonLedgers[10].pos.push({
      episodeId: `sig_bad_${i}`, t0: i * 20000, resolvedAt: i * 20000 + 10000,
      anchorSide: 1, regime: 'NORMAL', outcomeClass: 'DOWN', moveBps: -5.0, anchorSignedMoveBps: -5.0, deadZoneBps: 1.5,
      concurrencyWeight: 1.0, baseModelPredsAtEntry: { leaderProbs: [0.30, 0.40, 0.30] }, invalidationObservations: []
    });
  }
  qe.evaluateAllMotifs();
  assert.strictEqual(reg.motifs.get(sigId).lifecycleState, 'QUARANTINED', 'Degraded graduated motif must be demoted to QUARANTINED');

  return {
    dataType: 'SYNTHETIC_UNIT_TEST',
    assertionsPassed: [
      'multi_horizon_deadzone_scaling',
      'neff_concurrency_vs_serial_vs_small_sample',
      'runtime_data_leakage_blocker',
      'bh_fdr_noise_rejection_across_multiple_hypotheses',
      'walk_forward_graduation_and_automatic_quarantine_demotion'
    ]
  };
}

async function runLiveHonestyGateVerification() {
  // CRITICAL SECTION 16 CHECK: On a 14-second live BTCUSDT run, ZERO motifs may be prematurely graduated!
  const fc = new FeedCoordinator();
  const enc = new EventEncoder('BTCUSDT');
  const em = new EpisodeManager('BTCUSDT');
  const reg = new MotifRegistry('BTCUSDT');
  const bm = new BaseModelSuite('BTCUSDT');
  const qe = new QuarantineEngine('BTCUSDT', reg, bm);

  em.on('episode_opened', (ep) => {
    reg.registerEpisodeOpen(ep);
    qe.onNewEpisodeOpened(ep);
  });
  em.on('horizon_resolved', ({ episode, horizonSec, slot }) => {
    reg.recordHorizonResolution(episode, horizonSec, slot);
    qe.evaluateAllMotifs();
  });

  fc.on('book', (b) => {
    enc.onBook(b);
    if (b.venue === 'BINANCE_FUT') {
      const mid = (b.bestBid + b.bestAsk) / 2;
      const spreadBps = ((b.bestAsk - b.bestBid) / mid) * 1e4;
      bm.onBackgroundPriceTick(mid, spreadBps, 2.0, enc.primaryRegime, b.ts);
      em.onPriceUpdate(mid, b.ts, 2.0);
    }
  });
  fc.on('trade', (t) => enc.onTrade(t));
  fc.on('gap', (g) => enc.onGap(g));

  enc.on('event', (ev) => {
    em.onSubsequentEvent(ev);
    const seqs = buildCandidateSequences(enc.events, (vA, vB) => fc.getCrossVenueUncertaintyMs(vA, vB));
    const preds = bm.predictAll(ev.ts, enc.primaryRegime);
    for (const cs of seqs) {
      em.tryOpenEpisode(cs, { spreadBps: ev.spreadBps, regime: enc.primaryRegime, baseModelPreds: preds });
    }
  });

  fc.startCoin('BTCUSDT');
  await new Promise((r) => setTimeout(r, 14_000));
  qe.evaluateAllMotifs();
  const card = qe.getDecisionCardState(fc.getIntegrityState());
  const bmAudit = bm.getAuditSummary();
  fc.stopAll('phase4_done');

  const graduatedOnShortRun = Array.from(reg.motifs.values()).filter((m) => m.lifecycleState === 'GRADUATED').length;
  assert.strictEqual(graduatedOnShortRun, 0, 'ZERO motifs may be declared GRADUATED on a 14-second live sample!');
  assert.strictEqual(card.silent, true, 'Decision card MUST remain silent without proven statistical evidence');
  assert.strictEqual(bmAudit.leakageViolationsDetected, 0, 'Zero data leakage violations allowed');

  return {
    dataType: 'LIVE_MARKET_EVIDENCE',
    symbol: 'BTCUSDT',
    durationSec: 14,
    graduatedMotifsCount: graduatedOnShortRun,
    decisionCard: card,
    baseModelAudit: bmAudit
  };
}

async function main() {
  console.log('[VERIFY PHASE 4] Testing OutcomeLabeler + EffectiveSample + BaseModels + QuarantineEngine...');
  const unit = runUnitTests();
  const live = await runLiveHonestyGateVerification();
  const out = { verifiedAt: new Date().toISOString(), unit, live };
  const outPath = path.join(__dirname, '..', 'audits', 'phase4_learning_evidence.json');
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  console.log('[VERIFY PHASE 4] ALL PASSED! Saved:', outPath);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 4 FAILED]:', err);
  process.exit(1);
});
