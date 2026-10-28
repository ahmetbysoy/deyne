'use strict';

/**
 * Computes separated concurrency-adjusted and serial-dependence-adjusted
 * effective sample size metrics, explicitly testing small-sample limitations.
 *
 * References & Assumptions:
 *  1. Concurrency adjustment (nUniq, nKish): corrects overlapping outcome horizons [t0, t0+H],
 *     assuming concurrent episodes share the same underlying price path segment.
 *  2. AR(1) Bartlett adjustment: N_AR = N * (1 - rho1) / (1 + rho1) assumes weak stationarity
 *     and Markovian lag-1 decay. For N < 8, sample autocorrelation has SE ~ 1/sqrt(N) >= 0.35
 *     and downward small-sample bias, so raw plug-in is unreliable and bounded conservatively.
 */
function computeEffectiveSampleMetrics(records) {
  const nRaw = Array.isArray(records) ? records.length : 0;
  if (nRaw === 0) {
    return {
      nRaw: 0,
      nUniq: 0,
      nKish: 0,
      lag1Autocorr: null,
      serialFactor: 1,
      nEffFinal: 0,
      smallSampleWarning: true,
      note: 'Hiç çözülmüş epizot yok.'
    };
  }

  // Sort chronologically by episode start time
  const sorted = [...records].sort((a, b) => a.t0 - b.t0);
  let sumW = 0;
  let sumW2 = 0;
  for (const r of sorted) {
    const w = Math.max(0.01, Math.min(1.0, Number(r.concurrencyWeight) || 1.0));
    sumW += w;
    sumW2 += w * w;
  }

  const nUniq = +sumW.toFixed(3);
  const nKish = sumW2 > 0 ? +((sumW * sumW) / sumW2).toFixed(3) : 0;

  // Build non-overlapping chronological subsample (minimum gap >= median resolution span) for lag-1 serial check
  const nonOverlap = [];
  let lastEnd = -Infinity;
  for (const r of sorted) {
    if (r.t0 >= lastEnd) {
      nonOverlap.push(r);
      lastEnd = r.resolvedAt || (r.t0 + 10_000);
    }
  }

  let lag1Autocorr = null;
  let serialFactor = 1.0;
  let smallSampleWarning = nonOverlap.length < 8;

  if (nonOverlap.length >= 8) {
    const xs = nonOverlap.slice(0, -1).map((r) => Number(r.anchorSignedMoveBps) || 0);
    const ys = nonOverlap.slice(1).map((r) => Number(r.anchorSignedMoveBps) || 0);
    const n = xs.length;
    const mx = xs.reduce((a, b) => a + b, 0) / n;
    const my = ys.reduce((a, b) => a + b, 0) / n;
    let num = 0, dx = 0, dy = 0;
    for (let i = 0; i < n; i++) {
      num += (xs[i] - mx) * (ys[i] - my);
      dx += Math.pow(xs[i] - mx, 2);
      dy += Math.pow(ys[i] - my, 2);
    }
    if (dx > 1e-9 && dy > 1e-9) {
      lag1Autocorr = +(num / Math.sqrt(dx * dy)).toFixed(3);
      // Only positive autocorrelation inflates variance of the mean; clamp in [0, 0.85]
      const posRho = Math.max(0, Math.min(0.85, lag1Autocorr));
      serialFactor = +((1 - posRho) / (1 + posRho)).toFixed(3);
    }
  } else {
    // Small-sample conservative damping (cannot estimate AR(1) reliably below 8 non-overlapping points)
    serialFactor = 0.85;
  }

  const baseCount = Math.min(nUniq, nKish);
  const nEffFinal = +Math.min(nRaw, baseCount * serialFactor).toFixed(3);

  return {
    nRaw,
    nNonOverlapping: nonOverlap.length,
    nUniq,
    nKish,
    lag1Autocorr,
    serialFactor,
    nEffFinal,
    smallSampleWarning,
    note: smallSampleWarning
      ? `Küçük örneklem (${nonOverlap.length} örtüşmesiz dalga < 8): seri korelasyon tahmini belirsiz, muhafazakâr 0.85 katsayısı uygulandı.`
      : 'Örtüşme ve AR(1) seri bağımlılık düzeltmesi ayrı ayrı hesaplandı.'
  };
}

module.exports = { computeEffectiveSampleMetrics };
