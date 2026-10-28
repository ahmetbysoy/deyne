'use strict';

const { HORIZONS_SEC, classifyThreeStateOutcome } = require('./outcomeLabeler');

const MODEL_IDS = Object.freeze(['fixed_train', 'short_ewma', 'long_ewma', 'decoupled_regime']);

function normalize3(arr) {
  const d = Math.max(1e-6, Number(arr[0]) || 0);
  const f = Math.max(1e-6, Number(arr[1]) || 0);
  const u = Math.max(1e-6, Number(arr[2]) || 0);
  const s = d + f + u;
  return [+(d / s).toFixed(5), +(f / s).toFixed(5), +(u / s).toFixed(5)];
}

function multinomialBrier3(probs, outcomeClass) {
  const y = outcomeClass === 'DOWN' ? [1, 0, 0] : (outcomeClass === 'FLAT' ? [0, 1, 0] : [0, 0, 1]);
  return Math.pow(probs[0] - y[0], 2) + Math.pow(probs[1] - y[1], 2) + Math.pow(probs[2] - y[2], 2);
}

function multinomialLogLoss3(probs, outcomeClass) {
  const idx = outcomeClass === 'DOWN' ? 0 : (outcomeClass === 'FLAT' ? 1 : 2);
  const p = Math.max(1e-4, Math.min(0.9999, probs[idx]));
  return -Math.log(p);
}

function freshHorizonState() {
  return {
    lastUpdatedResolvedAt: 0,
    // 1. Fixed/cumulative empirical training counts (unopinionated symmetric Dirichlet prior [2,2,2])
    cumCounts: [2.0, 2.0, 2.0], // [DOWN, FLAT, UP]
    // 2. Short-window EWMA
    shortProbs: [1 / 3, 1 / 3, 1 / 3],
    // 3. Long-window EWMA
    longProbs: [1 / 3, 1 / 3, 1 / 3],
    // 4. Decoupled regime model: fast P(FLAT | regime) + slow shrunk P(UP | MOVE)
    regimeFlatEwma: { CALM: 0.5, NORMAL: 0.45, VOLATILE: 0.25, THIN: 0.35 },
    slowMoveCounts: { down: 4.0, up: 4.0 },
    // Prequential (walk-forward) scoreboard — ZERO pre-anointed winner!
    scoreboard: {
      fixed_train: { n: 0, weightedN: 0, brierSum: 0, logLossSum: 0 },
      short_ewma: { n: 0, weightedN: 0, brierSum: 0, logLossSum: 0 },
      long_ewma: { n: 0, weightedN: 0, brierSum: 0, logLossSum: 0 },
      decoupled_regime: { n: 0, weightedN: 0, brierSum: 0, logLossSum: 0 }
    }
  };
}

class BaseModelSuite {
  constructor(symbol) {
    this.symbol = String(symbol).toUpperCase();
    this.byHorizon = {};
    for (const h of HORIZONS_SEC) this.byHorizon[h] = freshHorizonState();
    this.pendingBenchmarks = [];
    this.lastBenchmarkOpenedAt = 0;
    this.leakageChecksPassed = 0;
    this.leakageViolationsDetected = 0;
  }

  /**
   * Produces 3-state [P_DOWN, P_FLAT, P_UP] predictions for all 4 candidate models
   * strictly using ONLY state updated up to `now` (zero lookahead).
   */
  predictAll(now, regime = 'NORMAL') {
    const out = {};
    for (const h of HORIZONS_SEC) {
      const st = this.byHorizon[h];
      if (st.lastUpdatedResolvedAt > now + 5) {
        this.leakageViolationsDetected += 1;
        throw new Error(`DATA_LEAKAGE_VIOLATION: model state (${st.lastUpdatedResolvedAt}) is newer than prediction time (${now})`);
      }
      this.leakageChecksPassed += 1;

      const fixed_train = normalize3(st.cumCounts);
      const short_ewma = normalize3(st.shortProbs);
      const long_ewma = normalize3(st.longProbs);

      const pFlat = Math.max(0.02, Math.min(0.96, st.regimeFlatEwma[regime] ?? 0.45));
      const pUpGivenMove = st.slowMoveCounts.up / (st.slowMoveCounts.up + st.slowMoveCounts.down);
      const decoupled_regime = normalize3([
        (1 - pFlat) * (1 - pUpGivenMove),
        pFlat,
        (1 - pFlat) * pUpGivenMove
      ]);

      const activeLeader = this.selectEmpiricalLeader(h);
      const models = { fixed_train, short_ewma, long_ewma, decoupled_regime };

      out[h] = {
        leaderModelId: activeLeader,
        leaderProbs: models[activeLeader], // [P_DOWN, P_FLAT, P_UP]
        models
      };
    }
    return out;
  }

  /**
   * Selects the best base model empirically ONLY if enough prequential samples exist;
   * otherwise uses `fixed_train` empirical counts without crowning any complex model prematurely.
   */
  selectEmpiricalLeader(horizonSec) {
    const sb = this.byHorizon[horizonSec].scoreboard;
    if (sb.fixed_train.weightedN < 5.0) return 'fixed_train';
    let bestId = 'fixed_train';
    let bestBrier = Infinity;
    for (const id of MODEL_IDS) {
      const s = sb[id];
      if (s.weightedN <= 0) continue;
      const meanBrier = s.brierSum / s.weightedN;
      if (meanBrier < bestBrier) {
        bestBrier = meanBrier;
        bestId = id;
      }
    }
    return bestId;
  }

  /**
   * Periodically samples non-event background market windows (every 5s) so base rates learn
   * unbiased market proportions cleanly and strictly prequentially.
   */
  onBackgroundPriceTick(mid, spreadBps, volRateBpsPerSec, regime, now) {
    if (!(mid > 0)) return;

    // Resolve matured background benchmarks FIRST, then open new ones
    const stillPending = [];
    for (const b of this.pendingBenchmarks) {
      let left = 0;
      for (const h of HORIZONS_SEC) {
        const slot = b.horizons[h];
        if (slot.done) continue;
        if (now < slot.dueAt) { left += 1; continue; }
        slot.done = true;
        const res = classifyThreeStateOutcome(b.mid0, mid, h, b.spreadBps, b.volRateBpsPerSec);
        if (res.valid) {
          // Concurrency weight: sampled every 5s across H-second window -> weight = min(1, 5 / H)
          const w = Math.min(1.0, 5 / h);
          this.updateFromResolved(h, res.label, b.regime, b.predsAtEntry[h].models, now, w);
        }
      }
      if (left > 0) stillPending.push(b);
    }
    this.pendingBenchmarks = stillPending;

    if (now - this.lastBenchmarkOpenedAt >= 5000) {
      this.lastBenchmarkOpenedAt = now;
      const predsAtEntry = this.predictAll(now, regime);
      const horizons = {};
      for (const h of HORIZONS_SEC) horizons[h] = { dueAt: now + h * 1000, done: false };
      this.pendingBenchmarks.push({
        t0: now,
        mid0: mid,
        spreadBps,
        volRateBpsPerSec,
        regime,
        predsAtEntry,
        horizons
      });
    }
  }

  invalidatePendingBenchmarks() {
    this.pendingBenchmarks = [];
  }

  updateFromResolved(horizonSec, outcomeClass, regime, predsAtEntryModels, resolvedAt, weight = 1.0) {
    const st = this.byHorizon[horizonSec];
    if (!st) return;
    if (resolvedAt > Date.now() + 1000) {
      this.leakageViolationsDetected += 1;
      throw new Error('DATA_LEAKAGE_VIOLATION: Attempted to update base model with future timestamp');
    }

    // Score each candidate model on its strictly pre-resolution forecast!
    if (predsAtEntryModels) {
      for (const id of MODEL_IDS) {
        const p = predsAtEntryModels[id];
        if (!p) continue;
        const sb = st.scoreboard[id];
        sb.n += 1;
        sb.weightedN = +(sb.weightedN + weight).toFixed(4);
        sb.brierSum = +(sb.brierSum + weight * multinomialBrier3(p, outcomeClass)).toFixed(6);
        sb.logLossSum = +(sb.logLossSum + weight * multinomialLogLoss3(p, outcomeClass)).toFixed(6);
      }
    }

    // Now update model parameters for future predictions
    const y = outcomeClass === 'DOWN' ? [1, 0, 0] : (outcomeClass === 'FLAT' ? [0, 1, 0] : [0, 0, 1]);
    st.cumCounts[0] += weight * y[0];
    st.cumCounts[1] += weight * y[1];
    st.cumCounts[2] += weight * y[2];

    const aShort = 1 - Math.pow(1 - 0.16, weight);
    const aLong = 1 - Math.pow(1 - 0.03, weight);
    for (let k = 0; k < 3; k++) {
      st.shortProbs[k] = (1 - aShort) * st.shortProbs[k] + aShort * y[k];
      st.longProbs[k] = (1 - aLong) * st.longProbs[k] + aLong * y[k];
    }

    const curReg = st.regimeFlatEwma[regime] ?? 0.45;
    st.regimeFlatEwma[regime] = (1 - aShort) * curReg + aShort * y[1];
    if (outcomeClass === 'UP') st.slowMoveCounts.up += weight;
    else if (outcomeClass === 'DOWN') st.slowMoveCounts.down += weight;

    st.lastUpdatedResolvedAt = Math.max(st.lastUpdatedResolvedAt, resolvedAt);
  }

  getAuditSummary() {
    const horizons = {};
    for (const h of HORIZONS_SEC) {
      const st = this.byHorizon[h];
      const scores = {};
      for (const id of MODEL_IDS) {
        const s = st.scoreboard[id];
        scores[id] = {
          n: s.n,
          weightedN: +s.weightedN.toFixed(2),
          meanBrier: s.weightedN > 0 ? +(s.brierSum / s.weightedN).toFixed(4) : null,
          meanLogLoss: s.weightedN > 0 ? +(s.logLossSum / s.weightedN).toFixed(4) : null
        };
      }
      horizons[h] = {
        empiricalDistribution: normalize3(st.cumCounts).map((v) => +(v * 100).toFixed(1)), // [Down%, Flat%, Up%]
        activeLeader: this.selectEmpiricalLeader(h),
        scoreboard: scores
      };
    }
    return {
      symbol: this.symbol,
      leakageChecksPassed: this.leakageChecksPassed,
      leakageViolationsDetected: this.leakageViolationsDetected,
      horizons
    };
  }

  serialize() {
    return { byHorizon: this.byHorizon };
  }

  deserialize(data) {
    if (!data || !data.byHorizon) return;
    for (const h of HORIZONS_SEC) {
      if (data.byHorizon[h]) {
        this.byHorizon[h] = { ...freshHorizonState(), ...data.byHorizon[h], lastUpdatedResolvedAt: 0 };
      }
    }
  }
}

module.exports = { BaseModelSuite, MODEL_IDS, multinomialBrier3, multinomialLogLoss3, normalize3 };
