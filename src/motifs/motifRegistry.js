'use strict';

const { HORIZONS_SEC } = require('../learning/outcomeLabeler');

function emptyHorizonLedger() {
  return {
    pos: [], // anchorSide = +1 resolved records
    neg: []  // anchorSide = -1 resolved records
  };
}

function createMotifRecord(seqInfo, now = Date.now()) {
  const horizonLedgers = {};
  for (const h of HORIZONS_SEC) horizonLedgers[h] = emptyHorizonLedger();

  return {
    motifId: seqInfo.motifId,
    lengthK: seqInfo.lengthK,
    suffixId: seqInfo.suffixId || null,
    isCrossVenue: !!seqInfo.isCrossVenue,
    venuesInvolved: seqInfo.venuesInvolved || [],
    firstSeenAt: now,
    lastSeenAt: now,
    lifecycleState: 'DISCOVERY', // DISCOVERY | INCUBATION | VALIDATION | GRADUATED | QUARANTINED | RETIRED
    stateReason: 'Yeni keşfedildi — ilk bağımsız sonuç gözlemleri bekleniyor.',
    warmingUp: false,
    warmUpRemaining: 0,
    prunedAsRedundant: false,
    totalTriggered: 0,
    brokenIntegrityCount: 0,
    clockSummary: {
      count: 0,
      meanSpanMs: 0,
      meanTrades: 0,
      meanVolumeUsd: 0
    },
    horizonLedgers,
    evaluation: null // populated by FAZ 4 QuarantineEngine
  };
}

/**
 * Jensen-Shannon divergence (in bits, [0, 1]) between two 3-element probability vectors P and Q.
 */
function jensenShannonDivergence(p, q) {
  const m = [0.5 * (p[0] + q[0]), 0.5 * (p[1] + q[1]), 0.5 * (p[2] + q[2])];
  const kl = (a, b) => {
    let s = 0;
    for (let i = 0; i < 3; i++) {
      if (a[i] > 1e-12 && b[i] > 1e-12) s += a[i] * Math.log2(a[i] / b[i]);
    }
    return s;
  };
  return +Math.max(0, 0.5 * kl(p, m) + 0.5 * kl(q, m)).toFixed(5);
}

/**
 * Computes smoothed 3-state distribution in anchor frame: [AGAINST, FLAT, WITH]
 */
function anchorNormalizedProbs(records, alpha = 1.0) {
  let against = alpha, flat = alpha, withAnchor = alpha;
  for (const r of records) {
    const w = Number.isFinite(r.concurrencyWeight) ? r.concurrencyWeight : 1;
    if (r.outcomeClass === 'FLAT') flat += w;
    else if (
      (r.anchorSide > 0 && r.outcomeClass === 'UP') ||
      (r.anchorSide < 0 && r.outcomeClass === 'DOWN')
    ) {
      withAnchor += w;
    } else {
      against += w;
    }
  }
  const sum = against + flat + withAnchor;
  return [against / sum, flat / sum, withAnchor / sum];
}

class MotifRegistry {
  constructor(symbol, options = {}) {
    this.symbol = String(symbol).toUpperCase();
    this.motifs = new Map();
    this.maxRecordsPerLedger = options.maxRecordsPerLedger || 300;
  }

  registerEpisodeOpen(ep) {
    let m = this.motifs.get(ep.motifId);
    if (!m) {
      m = createMotifRecord(ep, ep.t0);
      this.motifs.set(ep.motifId, m);
    }
    m.lastSeenAt = ep.t0;
    m.totalTriggered += 1;

    const c = m.clockSummary;
    c.count += 1;
    c.meanSpanMs = +(((c.meanSpanMs * (c.count - 1)) + ep.totalSpanMs) / c.count).toFixed(1);
    c.meanTrades = +(((c.meanTrades * (c.count - 1)) + ep.totalTrades) / c.count).toFixed(2);
    c.meanVolumeUsd = +(((c.meanVolumeUsd * (c.count - 1)) + ep.totalVolumeUsd) / c.count).toFixed(2);
    return m;
  }

  recordIntegrityBreak(motifId) {
    const m = this.motifs.get(motifId);
    if (m) m.brokenIntegrityCount += 1;
  }

  recordHorizonResolution(ep, horizonSec, slot) {
    if (slot.status !== 'RESOLVED' || ep.integrityStatus !== 'OK') return null;
    const m = this.motifs.get(ep.motifId);
    if (!m) return null;

    const ledgerGroup = m.horizonLedgers[horizonSec];
    if (!ledgerGroup) return null;

    const targetArr = ep.anchorSide > 0 ? ledgerGroup.pos : ledgerGroup.neg;
    const rec = {
      episodeId: ep.episodeId,
      t0: ep.t0,
      resolvedAt: slot.resolvedAt,
      anchorSide: ep.anchorSide,
      regime: ep.regime,
      outcomeClass: slot.outcomeClass,
      moveBps: slot.moveBps,
      anchorSignedMoveBps: slot.anchorSignedMoveBps,
      deadZoneBps: slot.deadZoneBps,
      concurrencyWeight: slot.concurrencyWeight,
      baseModelPredsAtEntry: slot.baseModelPredsAtEntry,
      invalidationObservations: ep.invalidationObservations
        ? Object.values(ep.invalidationObservations).filter((obs) => obs.ts < slot.resolvedAt)
        : []
    };
    targetArr.push(rec);
    if (targetArr.length > this.maxRecordsPerLedger) {
      targetArr.splice(0, targetArr.length - this.maxRecordsPerLedger);
    }

    if (m.lifecycleState === 'DISCOVERY') {
      m.lifecycleState = 'INCUBATION';
      m.stateReason = 'Kuluçkada — bağımsız epizot ve baz oran farkı toplanıyor.';
    }
    return m;
  }

  /**
   * Evaluates whether a k>=3 motif carries additional predictive information over its (k-1) suffix.
   */
  compareWithSuffix(motifId, horizonSec) {
    const m = this.motifs.get(motifId);
    if (!m || m.lengthK < 3 || !m.suffixId) {
      return { hasSuffix: false, jsDivergenceBits: null, liftDelta: null, addsInformation: true };
    }
    const suf = this.motifs.get(m.suffixId);
    if (!suf) {
      return { hasSuffix: false, jsDivergenceBits: null, liftDelta: null, addsInformation: true };
    }

    const mRecs = [...m.horizonLedgers[horizonSec].pos, ...m.horizonLedgers[horizonSec].neg];
    const sRecs = [...suf.horizonLedgers[horizonSec].pos, ...suf.horizonLedgers[horizonSec].neg];
    if (mRecs.length < 4 || sRecs.length < 4) {
      return { hasSuffix: true, suffixId: m.suffixId, insufficientData: true, jsDivergenceBits: null, liftDelta: null, addsInformation: null };
    }

    const pMotif = anchorNormalizedProbs(mRecs, 1.0);
    const pSuffix = anchorNormalizedProbs(sRecs, 1.0);
    const jsBits = jensenShannonDivergence(pMotif, pSuffix);
    // Directional edge difference in anchor frame: (P_with - P_against)
    const edgeMotif = pMotif[2] - pMotif[0];
    const edgeSuffix = pSuffix[2] - pSuffix[0];
    const liftDelta = +(Math.abs(edgeMotif) - Math.abs(edgeSuffix)).toFixed(4);

    return {
      hasSuffix: true,
      suffixId: m.suffixId,
      insufficientData: false,
      motifDist: pMotif.map((x) => +x.toFixed(3)),
      suffixDist: pSuffix.map((x) => +x.toFixed(3)),
      jsDivergenceBits: jsBits,
      liftDelta,
      addsInformation: jsBits >= 0.015 || liftDelta >= 0.04
    };
  }

  /**
   * Prunes redundant k>=3 motifs that have collected enough uniqueness-weighted observations
   * yet fail to add information over their (k-1) suffix across all horizons.
   */
  runSuffixPruning(minWeightedObs = 12) {
    let prunedCount = 0;
    for (const m of this.motifs.values()) {
      if (m.lengthK < 3 || !m.suffixId || m.prunedAsRedundant) continue;
      const recs10 = [...m.horizonLedgers[10].pos, ...m.horizonLedgers[10].neg];
      const wSum = recs10.reduce((s, r) => s + (r.concurrencyWeight || 1), 0);
      if (wSum < minWeightedObs) continue;

      let anyAddsInfo = false;
      for (const h of HORIZONS_SEC) {
        const cmp = this.compareWithSuffix(m.motifId, h);
        if (cmp.insufficientData || cmp.addsInformation) {
          anyAddsInfo = true;
          break;
        }
      }

      if (!anyAddsInfo) {
        m.prunedAsRedundant = true;
        m.lifecycleState = 'RETIRED';
        m.stateReason = `Kısa alt dizilimine (${m.suffixId}) kıyasla ek bilgi taşımadığı için budandı.`;
        prunedCount += 1;
      }
    }
    return prunedCount;
  }

  serialize() {
    const arr = [];
    for (const m of this.motifs.values()) arr.push(m);
    return arr;
  }

  deserialize(list) {
    this.motifs.clear();
    if (!Array.isArray(list)) return;
    for (const item of list) {
      if (item && item.motifId) {
        this.motifs.set(item.motifId, item);
      }
    }
  }
}

module.exports = { MotifRegistry, jensenShannonDivergence, anchorNormalizedProbs };
