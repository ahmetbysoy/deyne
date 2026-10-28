'use strict';

// Zero-dependency Node 20 (--experimental-websocket) Single-Coin Multi-Venue Audit for BTCUSDT
// Feeds:
//   1. Binance USD-M Futures (wss://fstream.binance.com): bookTicker + depth20@100ms + trade + market/markPrice@1s
//   2. Binance Spot (wss://data-stream.binance.vision): depth20@100ms + aggTrade
//   3. OKX Perpetual Swap (wss://ws.okx.com:443/ws/v5/public): BTC-USDT-SWAP books5 + trades

const fs = require('fs');
const path = require('path');

const DURATION_MS = 35_000;
const HORIZON_MS = 10_000;
const TICK_MS = 500;
const clamp = (v, lo, hi) => (Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : lo);

const state = {
  fut: { bboBid: 0, bboAsk: 0, bboBidQty: 0, bboAskQty: 0, bids: [], asks: [], trades: [], bboUpdates: 0, depthUpdates: 0, tradeCount: 0, markPrice: 0, indexPrice: 0, fundingRate: 0 },
  spot: { bids: [], asks: [], trades: [], depthUpdates: 0, tradeCount: 0 },
  okx: { bids: [], asks: [], trades: [], depthUpdates: 0, tradeCount: 0 },
  hyst: { futObi: 0, spotObi: 0, futBurst: 0, futBurstPeak: 0, futBurstStart: 0, leadVenue: 0, cd: {} },
  naiveTickActive: 0,
  naiveTickRedundant: 0,
  prevNaiveTok: 'NEUTRAL',
  prev2NaiveTok: 'NEUTRAL',
  dnaEvents: [],
  pending: [],
  resolved: [],
  // Strictly prequential (walk-forward, updated ONLY when t+H resolves -> ZERO leakage!) base-rate trackers:
  preq: {
    n: 0,
    shortUpEwma: 0.5, shortFlatEwma: 0.5, // alpha = 0.25 (~4 resolved non-overlapping eq / fast)
    longCounts: { down: 2, flat: 4, up: 2 }, // Dirichlet prior (alpha0 = 8) + cumulative prequential counts
    regimeBuckets: {
      tight: { down: 1.5, flat: 3.0, up: 1.5 }, // low vol / tight spread vs move threshold
      active: { down: 2.0, flat: 2.0, up: 2.0 } // high activity / wider volRate
    }
  }
};

function pushTrade(arr, t, price, qty, side) {
  arr.push({ t, price, notional: price * qty, side });
  if (arr.length > 4000) arr.splice(0, 1500);
}

function sumFlow(arr, now, ms) {
  let buy = 0, sell = 0, count = 0;
  for (let i = arr.length - 1; i >= 0; i--) {
    if (now - arr[i].t > ms) break;
    if (arr[i].side > 0) buy += arr[i].notional; else sell += arr[i].notional;
    count += 1;
  }
  return { buy, sell, total: buy + sell, net: buy - sell, count };
}

function topImbalance(bids, asks, depthN = 10) {
  let b = 0, a = 0;
  for (let i = 0; i < Math.min(depthN, bids.length); i++) b += bids[i][1];
  for (let i = 0; i < Math.min(depthN, asks.length); i++) a += asks[i][1];
  return (b + a) > 0 ? (b - a) / (b + a) : 0;
}

function emitDna(now, venue, type, side, mid, detail = {}) {
  const key = `${venue}:${type}:${side}`;
  if (state.hyst.cd[key] && now - state.hyst.cd[key] < 1000) return;
  state.hyst.cd[key] = now;
  const ev = { t: now, iso: new Date(now).toISOString(), venue, type, side, mid: +mid.toFixed(2), detail };
  state.dnaEvents.push(ev);
}

function observeMultiVenueBtc(now) {
  const f = state.fut;
  if (!f.bboBid || !f.bboAsk || f.bboAsk <= f.bboBid) return;
  const futMid = (f.bboBid + f.bboAsk) / 2;

  // 1) Binance Futures OBI transition
  if (f.bids.length >= 5 && f.asks.length >= 5) {
    const obi = topImbalance(f.bids, f.asks, 10);
    if (obi > 0.38 && state.hyst.futObi !== 1) { state.hyst.futObi = 1; emitDna(now, 'BIN_FUT', 'BOOK_TILT', 1, futMid, { obi: +obi.toFixed(3) }); }
    else if (obi < -0.38 && state.hyst.futObi !== -1) { state.hyst.futObi = -1; emitDna(now, 'BIN_FUT', 'BOOK_TILT', -1, futMid, { obi: +obi.toFixed(3) }); }
    else if (Math.abs(obi) < 0.12) state.hyst.futObi = 0;
  }

  // 2) Binance Spot OBI transition
  if (state.spot.bids.length >= 5 && state.spot.asks.length >= 5) {
    const sObi = topImbalance(state.spot.bids, state.spot.asks, 10);
    if (sObi > 0.38 && state.hyst.spotObi !== 1) { state.hyst.spotObi = 1; emitDna(now, 'BIN_SPOT', 'BOOK_TILT', 1, futMid, { obi: +sObi.toFixed(3) }); }
    else if (sObi < -0.38 && state.hyst.spotObi !== -1) { state.hyst.spotObi = -1; emitDna(now, 'BIN_SPOT', 'BOOK_TILT', -1, futMid, { obi: +sObi.toFixed(3) }); }
    else if (Math.abs(sObi) < 0.12) state.hyst.spotObi = 0;
  }

  // 3) Futures Aggressive Trade Burst & Exhaustion
  const f2 = sumFlow(f.trades, now, 1500);
  const f15 = sumFlow(f.trades, now, 15000);
  const base = Math.max(1000, (f15.total / 15) * 1.5);
  if (f2.count >= 4 && f2.total > base * 2.2) {
    const buyShare = f2.buy / (f2.total + 1e-9);
    if (buyShare >= 0.75 && state.hyst.futBurst !== 1) {
      state.hyst.futBurst = 1; state.hyst.futBurstPeak = f2.total; state.hyst.futBurstStart = now;
      emitDna(now, 'BIN_FUT', 'FLOW_BURST', 1, futMid, { notionalUsd: Math.round(f2.total) });
    } else if (buyShare <= 0.25 && state.hyst.futBurst !== -1) {
      state.hyst.futBurst = -1; state.hyst.futBurstPeak = f2.total; state.hyst.futBurstStart = now;
      emitDna(now, 'BIN_FUT', 'FLOW_BURST', -1, futMid, { notionalUsd: Math.round(f2.total) });
    }
  } else if (state.hyst.futBurst !== 0 && now - state.hyst.futBurstStart >= 800 && f2.total < state.hyst.futBurstPeak * 0.30) {
    emitDna(now, 'BIN_FUT', 'FLOW_EXHAUST', state.hyst.futBurst, futMid, {});
    state.hyst.futBurst = 0;
  }

  // 4) Cross-Venue Spot vs Futures Basis / Flow Divergence Event!
  const s2 = sumFlow(state.spot.trades, now, 2000);
  const o2 = sumFlow(state.okx.trades, now, 2000);
  if (s2.total > 25000 && f2.total > 40000) {
    const spotDir = s2.net / s2.total;
    const futDir = f2.net / f2.total;
    if (spotDir > 0.55 && futDir < -0.45 && state.hyst.leadVenue !== 1) {
      state.hyst.leadVenue = 1;
      emitDna(now, 'CROSS', 'SPOT_ABSORB_FUT_SELL', 1, futMid, { spotNet: Math.round(s2.net), futNet: Math.round(f2.net) });
    } else if (spotDir < -0.55 && futDir > 0.45 && state.hyst.leadVenue !== -1) {
      state.hyst.leadVenue = -1;
      emitDna(now, 'CROSS', 'SPOT_SELL_FUT_CHASE', -1, futMid, { spotNet: Math.round(s2.net), futNet: Math.round(f2.net) });
    } else if (Math.abs(spotDir - futDir) < 0.3) {
      state.hyst.leadVenue = 0;
    }
  }
  // OKX confirmation burst
  if (o2.count >= 4 && Math.abs(o2.net) / (o2.total + 1e-9) > 0.80 && f2.count >= 4 && Math.sign(o2.net) === Math.sign(f2.net)) {
    emitDna(now, 'CROSS', 'DUAL_FUT_SYNC_BURST', Math.sign(o2.net), futMid, {});
  }
}

function startConnections() {
  // 1a. Binance Futures High-Speed Book + Trades
  const wsFut1 = new WebSocket('wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@depth20@100ms/btcusdt@trade');
  wsFut1.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (!m || !m.data) return;
    const d = m.data;
    const now = Date.now();
    if (m.stream.endsWith('bookTicker')) {
      state.fut.bboBid = +d.b; state.fut.bboAsk = +d.a;
      state.fut.bboBidQty = +d.B; state.fut.bboAskQty = +d.A;
      state.fut.bboUpdates += 1;
    } else if (m.stream.endsWith('depth20@100ms')) {
      state.fut.bids = (d.b || []).map((x) => [+x[0], +x[1]]);
      state.fut.asks = (d.a || []).map((x) => [+x[0], +x[1]]);
      state.fut.depthUpdates += 1;
      observeMultiVenueBtc(now);
    } else if (m.stream.endsWith('@trade')) {
      pushTrade(state.fut.trades, +d.T || now, +d.p, +d.q, d.m ? -1 : 1);
      state.fut.tradeCount += 1;
    }
  };

  // 1b. Binance Futures Market Stream (Mark Price + Index + Funding + ForceOrder)
  const wsFut2 = new WebSocket('wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s/btcusdt@forceOrder');
  wsFut2.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (!m || !m.data) return;
    const d = m.data;
    if (m.stream.endsWith('markPrice@1s')) {
      state.fut.markPrice = +d.p;
      state.fut.indexPrice = +d.i;
      state.fut.fundingRate = +d.r;
    } else if (m.stream.endsWith('forceOrder') && d.o) {
      const side = d.o.S === 'BUY' ? 1 : -1; // forced buy = short liquidation
      emitDna(Date.now(), 'BIN_FUT', 'LIQUIDATION', side, +d.o.ap || state.fut.bboBid, { qty: +d.o.q });
    }
  };

  // 2. Binance Spot
  const wsSpot = new WebSocket('wss://data-stream.binance.vision/stream?streams=btcusdt@depth20@100ms/btcusdt@aggTrade');
  wsSpot.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (!m || !m.data) return;
    const d = m.data;
    if (m.stream.endsWith('depth20@100ms')) {
      state.spot.bids = (d.bids || []).map((x) => [+x[0], +x[1]]);
      state.spot.asks = (d.asks || []).map((x) => [+x[0], +x[1]]);
      state.spot.depthUpdates += 1;
    } else if (m.stream.endsWith('aggTrade')) {
      pushTrade(state.spot.trades, +d.T || Date.now(), +d.p, +d.q, d.m ? -1 : 1);
      state.spot.tradeCount += 1;
    }
  };

  // 3. OKX Perpetual Swap
  const wsOkx = new WebSocket('wss://ws.okx.com:443/ws/v5/public');
  wsOkx.onopen = () => {
    wsOkx.send(JSON.stringify({
      op: 'subscribe',
      args: [{ channel: 'books5', instId: 'BTC-USDT-SWAP' }, { channel: 'trades', instId: 'BTC-USDT-SWAP' }]
    }));
  };
  wsOkx.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (!m || !m.arg || !m.data) return;
    if (m.arg.channel === 'books5' && m.data[0]) {
      state.okx.bids = (m.data[0].bids || []).map((x) => [+x[0], +x[1]]);
      state.okx.asks = (m.data[0].asks || []).map((x) => [+x[0], +x[1]]);
      state.okx.depthUpdates += 1;
    } else if (m.arg.channel === 'trades') {
      for (const tr of m.data) {
        pushTrade(state.okx.trades, +tr.ts || Date.now(), +tr.px, +tr.sz * 0.01, tr.side === 'buy' ? 1 : -1); // 0.01 BTC contract size
        state.okx.tradeCount += 1;
      }
    }
  };

  return [wsFut1, wsFut2, wsSpot, wsOkx];
}

function multinomialBrier(probs3, actualIdx) {
  // probs3 = [pDown, pFlat, pUp], actualIdx in {0, 1, 2}
  let s = 0;
  for (let k = 0; k < 3; k++) {
    const y = (k === actualIdx) ? 1 : 0;
    s += Math.pow(probs3[k] - y, 2);
  }
  return s;
}

function tick500ms() {
  const now = Date.now();
  const f = state.fut;
  if (!f.bboBid || !f.bboAsk || f.bboAsk <= f.bboBid) return;
  const mid = (f.bboBid + f.bboAsk) / 2;

  // Measure naive 500ms tick token stuttering transparently
  const obi = topImbalance(f.bids, f.asks, 10);
  const f5 = sumFlow(f.trades, now, 5000);
  const dS = f5.net / (f5.total + 1e-9);
  let tok = 'NEUTRAL';
  if (obi > 0.25) tok = '+OBI';
  else if (obi < -0.25) tok = '-OBI';
  else if (dS > 0.35) tok = '+FLOW';
  else if (dS < -0.35) tok = '-FLOW';

  if (tok !== 'NEUTRAL') {
    state.naiveTickActive += 1;
    if (tok === state.prevNaiveTok || (tok === state.prev2NaiveTok && tok !== state.prevNaiveTok)) {
      state.naiveTickRedundant += 1;
    }
  }
  state.prev2NaiveTok = state.prevNaiveTok;
  state.prevNaiveTok = tok;

  // Determine current activity regime BEFORE outcome is known (zero leakage)
  const f10 = sumFlow(f.trades, now, 10_000);
  const regime = f10.total > 180_000 ? 'active' : 'tight';

  // Formulate predictions from the 4 Base-Rate Models strictly using ONLY ALREADY-RESOLVED prequential state:
  // M1: Fixed Uniform/Symmetric 3-state prior: [0.25, 0.50, 0.25]
  const m1 = [0.25, 0.50, 0.25];

  // M2: Short-Window EWMA (fast adaptation)
  const pFlatShort = state.preq.shortFlatEwma;
  const pMoveShort = 1 - pFlatShort;
  const m2 = [pMoveShort * (1 - state.preq.shortUpEwma), pFlatShort, pMoveShort * state.preq.shortUpEwma];

  // M3: Long-Window Dirichlet-Shrunk Cumulative Base Rate
  const lc = state.preq.longCounts;
  const lSum = lc.down + lc.flat + lc.up;
  const m3 = [lc.down / lSum, lc.flat / lSum, lc.up / lSum];

  // M4: Regime-Conditioned 3-State Model (learns P(Flat|Regime) dynamically, shrinks directional split toward 0.50)
  const rb = state.preq.regimeBuckets[regime];
  const rSum = rb.down + rb.flat + rb.up;
  const pFlatReg = rb.flat / rSum;
  const dirUpShrunk = (lc.up + 6) / (lc.up + lc.down + 12); // heavier shrinkage on direction than on flatness
  const m4 = [(1 - pFlatReg) * (1 - dirUpShrunk), pFlatReg, (1 - pFlatReg) * dirUpShrunk];

  state.pending.push({ t0: now, endAt: now + HORIZON_MS, mid0: mid, regime, m1, m2, m3, m4 });

  // Resolve matured predictions & THEN update prequential models
  const still = [];
  for (const p of state.pending) {
    if (now < p.endAt) { still.push(p); continue; }
    const moveBps = ((mid - p.mid0) / p.mid0) * 1e4;
    const deadBps = 1.0; // 1.0 bps threshold (~$8 on $80k BTC, ~taker+slippage floor)
    const idx = moveBps < -deadBps ? 0 : (moveBps > deadBps ? 2 : 1); // 0=Down, 1=Flat, 2=Up

    state.resolved.push({
      t0: p.t0, t1: now, moveBps, idx, regime: p.regime,
      b1: multinomialBrier(p.m1, idx),
      b2: multinomialBrier(p.m2, idx),
      b3: multinomialBrier(p.m3, idx),
      b4: multinomialBrier(p.m4, idx)
    });

    // Update prequential state AFTER resolution (strict walk-forward, weight = 1/20 per 500ms tick inside 10s horizon!)
    const uWeight = TICK_MS / HORIZON_MS; // 0.05 concurrency weight per 500ms tick!
    const isFlat = idx === 1 ? 1 : 0;
    state.preq.shortFlatEwma = 0.85 * state.preq.shortFlatEwma + 0.15 * isFlat;
    if (idx !== 1) {
      const isUp = idx === 2 ? 1 : 0;
      state.preq.shortUpEwma = 0.80 * state.preq.shortUpEwma + 0.20 * isUp;
    }
    const key3 = idx === 0 ? 'down' : (idx === 1 ? 'flat' : 'up');
    state.preq.longCounts[key3] += uWeight;
    state.preq.regimeBuckets[p.regime][key3] += uWeight;
  }
  state.pending = still;
}

function pearson(a, b) {
  const n = Math.min(a.length, b.length);
  if (n < 4) return 0;
  const ma = a.reduce((s, x) => s + x, 0) / n;
  const mb = b.reduce((s, x) => s + x, 0) / n;
  let num = 0, da = 0, db = 0;
  for (let i = 0; i < n; i++) { num += (a[i] - ma) * (b[i] - mb); da += Math.pow(a[i] - ma, 2); db += Math.pow(b[i] - mb, 2); }
  return (da > 0 && db > 0) ? num / Math.sqrt(da * db) : 0;
}

async function main() {
  console.log('[BTC SINGLE-COIN AUDIT] Connecting live to fstream.binance.com + binance.vision + okx...');
  const socks = startConnections();
  const timer = setInterval(tick500ms, TICK_MS);

  await new Promise((r) => setTimeout(r, DURATION_MS));
  clearInterval(timer);
  socks.forEach((ws) => { try { ws.close(); } catch (_) {} });

  // Transparent N_eff definitions:
  const R = state.resolved;
  const nRaw = R.length;
  // 1) Concurrency-adjusted non-overlap count: sum(TICK_MS / HORIZON_MS)
  const nNonOverlap = +(nRaw * (TICK_MS / HORIZON_MS)).toFixed(2);
  // 2) Non-overlapping strided series (every 10s) lag-1 autocorrelation -> AR(1) effective independent sample size!
  const nonOverlapMoves = [];
  for (let i = 0; i < R.length; i += Math.round(HORIZON_MS / TICK_MS)) nonOverlapMoves.push(R[i].moveBps);
  const rhoTick = +pearson(R.slice(0, -1).map((x) => x.moveBps), R.slice(1).map((x) => x.moveBps)).toFixed(3);
  const rhoNonOverlap = +pearson(nonOverlapMoves.slice(0, -1), nonOverlapMoves.slice(1)).toFixed(3);
  // Standard Bartlett / AR(1) effective sample size on the non-overlapping series: N_eff_AR1 = N_nonOverlap * (1 - rho) / (1 + rho)
  const nEffAr1 = +(nNonOverlap * clamp((1 - Math.max(0, rhoNonOverlap)) / (1 + Math.max(0, rhoNonOverlap)), 0.1, 1)).toFixed(2);

  const totalRawStreamPackets = state.fut.bboUpdates + state.fut.depthUpdates + state.spot.depthUpdates + state.okx.depthUpdates;
  const totalDepth100msPackets = state.fut.depthUpdates + state.spot.depthUpdates + state.okx.depthUpdates;
  const nCleanDna = state.dnaEvents.length;

  const mean = (fn) => R.length ? +(R.reduce((s, x) => s + fn(x), 0) / R.length).toFixed(4) : null;
  const dist = {
    downPct: R.length ? +((R.filter((x) => x.idx === 0).length / R.length) * 100).toFixed(1) : 0,
    flatPct: R.length ? +((R.filter((x) => x.idx === 1).length / R.length) * 100).toFixed(1) : 0,
    upPct: R.length ? +((R.filter((x) => x.idx === 2).length / R.length) * 100).toFixed(1) : 0
  };

  const summary = {
    coin: 'BTCUSDT',
    durationSec: DURATION_MS / 1000,
    liveFeedsVerified: {
      binanceFutures_fstream: {
        bboUpdates: state.fut.bboUpdates,
        depth20_100msUpdates: state.fut.depthUpdates,
        trades: state.fut.tradeCount,
        markPrice: state.fut.markPrice,
        indexPrice: state.fut.indexPrice,
        fundingRate: state.fut.fundingRate
      },
      binanceSpot: { depth20_100msUpdates: state.spot.depthUpdates, aggTrades: state.spot.tradeCount },
      okxPerpSwap: { books5Updates: state.okx.depthUpdates, trades: state.okx.tradeCount }
    },
    compressionRatiosClarified: {
      rawAllBookPackets_to_DnaEvents: nCleanDna ? +(totalRawStreamPackets / nCleanDna).toFixed(1) : null,
      depth100msPackets_to_DnaEvents: nCleanDna ? +(totalDepth100msPackets / nCleanDna).toFixed(1) : null,
      active500msNaiveTicks_to_DnaEvents: nCleanDna ? +(state.naiveTickActive / nCleanDna).toFixed(2) : null,
      totalBookPackets: totalRawStreamPackets,
      totalDepth100msPackets,
      naiveActiveTicks: state.naiveTickActive,
      cleanDnaEvents: nCleanDna
    },
    effectiveSampleSizeDefinitions: {
      horizonSec: HORIZON_MS / 1000,
      nRaw500msWindows: nRaw,
      nUniquenessNonOverlap: nNonOverlap,
      lag1Autocorr_500msOverlapping: rhoTick,
      lag1Autocorr_10sNonOverlapping: rhoNonOverlap,
      nEff_AR1_Adjusted: nEffAr1
    },
    empirical3StateDistribution10s: dist,
    walkForwardBaseRateComparison_3StateBrier: {
      note: 'Lower Multinomial Brier is better. Strictly prequential (updated only after t+10s resolves, concurrency-weighted).',
      model1_FixedPrior_25_50_25: mean((x) => x.b1),
      model2_ShortWindowEWMA: mean((x) => x.b2),
      model3_LongWindowShrunk: mean((x) => x.b3),
      model4_RegimeConditionedShrunk: mean((x) => x.b4)
    },
    sampleDnaEvents: state.dnaEvents.slice(0, 12)
  };

  fs.writeFileSync(path.join(__dirname, 'btc_events_trace.jsonl'), state.dnaEvents.map((e) => JSON.stringify(e)).join('\n') + '\n');
  fs.writeFileSync(path.join(__dirname, 'btc_audit_summary.json'), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
