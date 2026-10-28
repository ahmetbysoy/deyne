'use strict';

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const DURATION_MS = 60_000;
const BIN_PAIRS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'SUIUSDT', 'DOGEUSDT', 'AVAXUSDT', 'LINKUSDT', 'NEARUSDT'];
const OKX_PAIRS = ['BTC-USDT-SWAP', 'ETH-USDT-SWAP', 'SOL-USDT-SWAP', 'SUI-USDT-SWAP', 'DOGE-USDT-SWAP', 'AVAX-USDT-SWAP', 'ARB-USDT-SWAP', 'OP-USDT-SWAP'];

const clamp = (v, lo, hi) => (Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : lo);
const sigmoid = (z) => 1 / (1 + Math.exp(-Math.max(-20, Math.min(20, z))));

function makeCtx(id, venue, symbol) {
  return {
    id, venue, symbol,
    bids: new Map(), asks: new Map(),
    hasBook: false, synced: venue === 'okx', lastUpdateId: 0, buffer: [],
    trades: [], midHistory: [],
    totalTrades: 0, cumVol: 0, flowEwma: 0, volRate: 3.0, spreadEwma: 1.2, bandUnit: 0.001,
    wallTrack: { bid: null, ask: null },
    hyst: { obi: 0, micro: 0, burst: 0, burstPeak: 0, burstStart: 0, cd: {} },
    events: [],
    theses: [],
    stoppedMicro: [],   // realistic adverse move >= max(2.0 bps, 2.5 * spreadBps)
    stoppedHtml12Bps: 0, // orderbook-mobile current 1.5 * bandUnit (~12 bps) stop
    tickEval10s: [],     // { t, mid, endAt, linProb, rollBaseUp, activeMotifKey, activeMotifAnchor }
    resolvedEval10s: []
  };
}

const streams = new Map();
for (const s of BIN_PAIRS) streams.set(`BIN:${s}`, makeCtx(`BIN:${s}`, 'binance', s));
for (const s of OKX_PAIRS) streams.set(`OKX:${s}`, makeCtx(`OKX:${s}`, 'okx', s));

function sortedLevels(map, desc, limit = 100) {
  const arr = [];
  for (const [p, q] of map.entries()) if (q > 0 && Number.isFinite(p)) arr.push({ price: p, qty: q });
  arr.sort((a, b) => (desc ? b.price - a.price : a.price - b.price));
  return arr.length > limit ? arr.slice(0, limit) : arr;
}

function applyLevels(map, levels) {
  if (!Array.isArray(levels)) return;
  for (const lv of levels) {
    const p = Number(lv[0]), q = Number(lv[1]);
    if (!Number.isFinite(p)) continue;
    if (q <= 0) map.delete(p); else map.set(p, q);
  }
}

async function syncBin(ctx) {
  try {
    const r = await fetch(`https://data-api.binance.vision/api/v3/depth?symbol=${ctx.symbol}&limit=500`);
    if (!r.ok) return;
    const snap = await r.json();
    ctx.bids.clear(); ctx.asks.clear();
    applyLevels(ctx.bids, snap.bids); applyLevels(ctx.asks, snap.asks);
    ctx.lastUpdateId = Number(snap.lastUpdateId);
    for (const ev of ctx.buffer) {
      if (ev.u <= ctx.lastUpdateId) continue;
      applyLevels(ctx.bids, ev.b); applyLevels(ctx.asks, ev.a);
      ctx.lastUpdateId = ev.u;
    }
    ctx.buffer = [];
    ctx.synced = true;
    ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
    ctx.events.push({ t: Date.now(), type: '<RESET>', side: 0 });
  } catch (_) {}
}

function startFeeds() {
  const bNames = BIN_PAIRS.flatMap((s) => [`${s.toLowerCase()}@depth@100ms`, `${s.toLowerCase()}@aggTrade`]);
  const bWs = new WebSocket(`wss://data-stream.binance.vision/stream?streams=${bNames.join('/')}`);
  bWs.on('open', () => BIN_PAIRS.forEach((s) => syncBin(streams.get(`BIN:${s}`))));
  bWs.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || !m.stream || !m.data) return;
    const [sym, ch] = m.stream.split('@');
    const ctx = streams.get(`BIN:${sym.toUpperCase()}`);
    if (!ctx) return;
    const d = m.data;
    if (ch === 'depth') {
      const U = Number(d.U), u = Number(d.u);
      if (!ctx.synced) { ctx.buffer.push({ U, u, b: d.b, a: d.a }); return; }
      if (u <= ctx.lastUpdateId) return;
      if (U > ctx.lastUpdateId + 1) { ctx.synced = false; syncBin(ctx); return; }
      applyLevels(ctx.bids, d.b); applyLevels(ctx.asks, d.a);
      ctx.lastUpdateId = u; ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
      observeEvents(ctx, Date.now());
    } else if (ch === 'aggTrade') {
      const p = Number(d.p), q = Number(d.q);
      if (p > 0 && q > 0) pushTrade(ctx, Number(d.T) || Date.now(), p, p * q, d.m ? -1 : 1);
    }
  });
  bWs.on('error', () => {});

  const oWs = new WebSocket('wss://ws.okx.com:443/ws/v5/public');
  oWs.on('open', () => {
    oWs.send(JSON.stringify({
      op: 'subscribe',
      args: OKX_PAIRS.flatMap((instId) => [{ channel: 'books', instId }, { channel: 'trades', instId }])
    }));
  });
  oWs.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || !m.arg || !m.data) return;
    const ctx = streams.get(`OKX:${m.arg.instId}`);
    if (!ctx) return;
    if (m.arg.channel === 'books') {
      for (const b of m.data) {
        if (m.action === 'snapshot') { ctx.bids.clear(); ctx.asks.clear(); ctx.events.push({ t: Date.now(), type: '<RESET>', side: 0 }); }
        applyLevels(ctx.bids, b.bids); applyLevels(ctx.asks, b.asks);
        ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
      }
      observeEvents(ctx, Date.now());
    } else if (m.arg.channel === 'trades') {
      for (const tr of m.data) {
        const p = Number(tr.px), q = Number(tr.sz);
        if (p > 0 && q > 0) pushTrade(ctx, Number(tr.ts) || Date.now(), p, p * q, tr.side === 'buy' ? 1 : -1);
      }
    }
  });
  oWs.on('error', () => {});
  return [bWs, oWs];
}

function pushTrade(ctx, t, price, notional, side) {
  ctx.trades.push({ t, price, notional, side });
  if (ctx.trades.length > 4000) ctx.trades.splice(0, 1200);
  ctx.totalTrades += 1;
  ctx.cumVol += notional;
}

function sumFlow(ctx, now, ms) {
  let buy = 0, sell = 0, count = 0;
  for (let i = ctx.trades.length - 1; i >= 0; i--) {
    const tr = ctx.trades[i];
    if (now - tr.t > ms) break;
    if (tr.side > 0) buy += tr.notional; else sell += tr.notional;
    count += 1;
  }
  return { buy, sell, total: buy + sell, net: buy - sell, count };
}

function emitEvent(ctx, now, type, side, mid) {
  const key = `${type}:${side}`;
  if (ctx.hyst.cd[key] && now - ctx.hyst.cd[key] < 1200) return;
  const prev = ctx.events[ctx.events.length - 1];
  // If same event+side repeats after returning to neutral, encode explicitly as RETEST!
  let finalType = type;
  if (prev && prev.type === type && prev.side === side) finalType = `${type}_RETEST`;

  ctx.hyst.cd[key] = now;
  const ev = { t: now, type: finalType, side, mid };
  ctx.events.push(ev);

  // Check structural invalidation on active theses!
  for (const th of ctx.theses) {
    if (th.closed || th.structAt) continue;
    const lostSupport = (ev.side === th.dir) && (type === 'WALL_PULL' || type === 'WALL_BREAK');
    const opposingPush = (ev.side === -th.dir) && (type === 'FLOW_BURST' || type === 'BOOK_TILT' || type === 'WALL_SPAWN' || type === 'MICRO_FLIP');
    if (lostSupport || opposingPush) {
      th.structAt = now;
      th.structMid = mid;
      th.structEvent = `${ev.side === th.dir ? 'SAME' : 'OPP'}:${finalType}`;
    }
  }
}

function observeEvents(ctx, now) {
  if (!ctx.hasBook || (ctx.lastObs && now - ctx.lastObs < 150)) return;
  ctx.lastObs = now;
  const bids = sortedLevels(ctx.bids, true, 80);
  const asks = sortedLevels(ctx.asks, false, 80);
  if (bids.length < 5 || asks.length < 5 || !(asks[0].price > bids[0].price)) return;

  const mid = (bids[0].price + asks[0].price) / 2;
  const spread = asks[0].price - bids[0].price;
  const unit = ctx.bandUnit;

  // Walls
  const med = (arr) => {
    const q = arr.slice(0, 40).map((x) => x.qty).sort((a, b) => a - b);
    return q.length ? q[Math.floor(q.length / 2)] : 0;
  };
  const bMed = med(bids), aMed = med(asks);
  for (const [sk, sign, lvs, m] of [['bid', 1, bids, bMed], ['ask', -1, asks, aMed]]) {
    let best = null, sum = 0;
    for (const lv of lvs) {
      const dist = Math.abs(lv.price - mid) / mid;
      if (dist > unit * 4) break;
      const n = lv.price * lv.qty; sum += n;
      if (m > 0 && lv.qty >= m * 3.8 && (!best || n > best.notional)) best = { price: lv.price, notional: n, dist };
    }
    if (best && sum > 0 && best.notional / sum < 0.18) best = null;
    const cur = ctx.wallTrack[sk];
    if (!cur && best && best.dist <= unit * 2.5) {
      ctx.wallTrack[sk] = { price: best.price, peak: best.notional, samples: 1 };
      emitEvent(ctx, now, 'WALL_SPAWN', sign, mid);
    } else if (cur) {
      if (best && Math.abs(best.price - cur.price) / mid < unit * 0.5) {
        cur.samples += 1; cur.peak = Math.max(cur.peak, best.notional);
      } else {
        const breached = sign > 0 ? mid < cur.price : mid > cur.price;
        if (breached) emitEvent(ctx, now, 'WALL_BREAK', sign, mid);
        else if (cur.samples >= 3) emitEvent(ctx, now, 'WALL_PULL', sign, mid);
        ctx.wallTrack[sk] = null;
      }
    }
  }

  // Flow burst & exhaust
  const f2 = sumFlow(ctx, now, 2000), f15 = sumFlow(ctx, now, 15000);
  const base2 = Math.max(1e-6, (f15.total / 15) * 2);
  if (f2.count >= 3 && f2.total > base2 * 2.1) {
    const sb = f2.buy / (f2.total + 1e-9);
    if (sb >= 0.72 && ctx.hyst.burst !== 1) { ctx.hyst.burst = 1; ctx.hyst.burstPeak = f2.total; ctx.hyst.burstStart = now; emitEvent(ctx, now, 'FLOW_BURST', 1, mid); }
    else if (sb <= 0.28 && ctx.hyst.burst !== -1) { ctx.hyst.burst = -1; ctx.hyst.burstPeak = f2.total; ctx.hyst.burstStart = now; emitEvent(ctx, now, 'FLOW_BURST', -1, mid); }
  } else if (ctx.hyst.burst !== 0 && now - ctx.hyst.burstStart >= 800 && f2.total < ctx.hyst.burstPeak * 0.35) {
    emitEvent(ctx, now, 'FLOW_EXHAUST', ctx.hyst.burst, mid);
    ctx.hyst.burst = 0;
  }

  // OBI tilt
  let bq = 0, aq = 0;
  for (const lv of bids) { if (lv.price < mid * (1 - unit)) break; bq += lv.qty; }
  for (const lv of asks) { if (lv.price > mid * (1 + unit)) break; aq += lv.qty; }
  const obi = (bq - aq) / (bq + aq + 1e-9);
  if (obi > 0.35 && ctx.hyst.obi !== 1) { ctx.hyst.obi = 1; emitEvent(ctx, now, 'BOOK_TILT', 1, mid); }
  else if (obi < -0.35 && ctx.hyst.obi !== -1) { ctx.hyst.obi = -1; emitEvent(ctx, now, 'BOOK_TILT', -1, mid); }
  else if (Math.abs(obi) < 0.12) ctx.hyst.obi = 0;

  // Microprice flip
  const microPx = (asks[0].price * bids[0].qty + bids[0].price * asks[0].qty) / (bids[0].qty + asks[0].qty || 1);
  const mSkew = (microPx - mid) / (spread / 2 + 1e-9);
  if (mSkew > 0.55 && ctx.hyst.micro !== 1) { ctx.hyst.micro = 1; emitEvent(ctx, now, 'MICRO_FLIP', 1, mid); }
  else if (mSkew < -0.55 && ctx.hyst.micro !== -1) { ctx.hyst.micro = -1; emitEvent(ctx, now, 'MICRO_FLIP', -1, mid); }
  else if (Math.abs(mSkew) < 0.15) ctx.hyst.micro = 0;
}

function getCanon(ctx, k, now) {
  const out = [];
  for (let i = ctx.events.length - 1; i >= 0 && out.length < k; i--) {
    if (ctx.events[i].type === '<RESET>') break;
    out.unshift(ctx.events[i]);
  }
  if (out.length < k || now - out[k - 1].t > 2000 || out[k - 1].t - out[0].t > 20000) return null;
  const anchor = out[k - 1].side;
  return {
    key: out.map((e) => `${e.side === anchor ? '+' : '-'}${e.type}`).join(' -> '),
    suffix2: k === 3 ? out.slice(1).map((e) => `${e.side === anchor ? '+' : '-'}${e.type}`).join(' -> ') : null,
    anchor
  };
}

function tick(ctx, now) {
  if (!ctx.hasBook) return;
  const bids = sortedLevels(ctx.bids, true, 80), asks = sortedLevels(ctx.asks, false, 80);
  if (bids.length < 5 || asks.length < 5 || !(asks[0].price > bids[0].price)) return;
  const mid = (bids[0].price + asks[0].price) / 2;
  const spreadBps = ((asks[0].price - bids[0].price) / mid) * 1e4;
  ctx.spreadEwma = ctx.spreadEwma * 0.95 + spreadBps * 0.05;
  ctx.midHistory.push({ t: now, mid });
  if (ctx.midHistory.length > 300) ctx.midHistory.shift();

  // Compute linear model probability (faithful obm.engine.v2 style)
  let bq = 0, aq = 0;
  for (const lv of bids) { if (lv.price < mid * (1 - ctx.bandUnit)) break; bq += lv.qty; }
  for (const lv of asks) { if (lv.price > mid * (1 + ctx.bandUnit)) break; aq += lv.qty; }
  const obi = (bq - aq) / (bq + aq + 1e-9);
  const microPx = (asks[0].price * bids[0].qty + bids[0].price * asks[0].qty) / (bids[0].qty + asks[0].qty || 1);
  const micro = clamp((((microPx - mid) / mid) * 1e4) / 8, -1, 1);
  const f5 = sumFlow(ctx, now, 5000), f15 = sumFlow(ctx, now, 15000);
  const dS = f5.net / (f5.total + 1e-9), dM = f15.net / (f15.total + 1e-9);
  const rawScore = 0.30 * obi + 0.25 * micro + 0.25 * dS + 0.20 * dM;
  const linProb = clamp(0.5 + (sigmoid(2.4 * rawScore) - 0.5) * 0.75, 0.12, 0.88);

  // Open directional thesis if linear score is decisive (|linProb - 0.5| >= 0.08) to test stops!
  if (Math.abs(linProb - 0.5) >= 0.08 && (!ctx.lastThesisAt || now - ctx.lastThesisAt >= 4000)) {
    ctx.lastThesisAt = now;
    const dir = linProb > 0.5 ? 1 : -1;
    const microStopBps = Math.max(2.0, ctx.spreadEwma * 2.5); // realistic short-horizon stop (2–4 bps)
    const html12Bps = ctx.bandUnit * 1.5 * 1e4;               // current HTML stop (~12–15 bps)
    ctx.theses.push({
      t: now, dir, entryMid: mid, microStopBps, html12Bps,
      structAt: null, structMid: null, structEvent: null,
      microStopAt: null, microStopMid: null, closed: false
    });
  }

  for (const th of ctx.theses) {
    if (th.closed) continue;
    const advBps = th.dir > 0 ? ((th.entryMid - mid) / th.entryMid) * 1e4 : ((mid - th.entryMid) / th.entryMid) * 1e4;
    if (advBps >= th.html12Bps) ctx.stoppedHtml12Bps += 1;
    if (!th.microStopAt && advBps >= th.microStopBps) {
      th.microStopAt = now;
      th.microStopMid = mid;
      th.closed = true;
      ctx.stoppedMicro.push(th);
    } else if (now - th.t > 15000) {
      th.closed = true;
    }
  }

  // Rolling empirical directional base rate (on non-zero moves)
  const recent = ctx.resolvedEval10s.slice(-40).filter((x) => x.moved !== 0);
  const rollBaseUp = recent.length >= 8 ? recent.filter((x) => x.moved > 0).length / recent.length : 0.5;

  const c2 = getCanon(ctx, 2, now);
  const c3 = getCanon(ctx, 3, now);

  ctx.tickEval10s.push({
    t: now, endAt: now + 10_000, mid, linProb, rollBaseUp,
    c2Key: c2 ? c2.key : null, c2Anchor: c2 ? c2.anchor : 0,
    c3Key: c3 ? c3.key : null, c3Suffix2: c3 ? c3.suffix2 : null, c3Anchor: c3 ? c3.anchor : 0
  });

  const keep = [];
  for (const ev of ctx.tickEval10s) {
    if (now < ev.endAt) { keep.push(ev); continue; }
    const moveBps = ((mid - ev.mid) / ev.mid) * 1e4;
    const dead = Math.max(0.6, ctx.spreadEwma * 0.8);
    const moved = moveBps > dead ? 1 : (moveBps < -dead ? -1 : 0);
    ctx.resolvedEval10s.push({ ...ev, moveBps, moved });
  }
  ctx.tickEval10s = keep;
}

async function main() {
  console.log('[DNA-LAB v2] Running 60s Phase-2 test: Structural Stop Lead-Time + Brier Skill Score + Sub-Motif Pruning...');
  const socks = startFeeds();
  const timer = setInterval(() => {
    const now = Date.now();
    for (const ctx of streams.values()) tick(ctx, now);
  }, 500);

  await new Promise((r) => setTimeout(r, DURATION_MS));
  clearInterval(timer);
  socks.forEach((s) => { try { s.close(); } catch (_) {} });

  // 1. Structural Invalidation vs Realistic Micro-Stop vs HTML 12bps Stop
  let totalTheses = 0, totalHtml12Stops = 0;
  const microStops = [];
  for (const ctx of streams.values()) {
    totalTheses += ctx.theses.length;
    totalHtml12Stops += ctx.stoppedHtml12Bps;
    for (const st of ctx.stoppedMicro) microStops.push({ stream: ctx.id, ...st });
  }
  let early = 0, leads = [], saved = [];
  const reasonCounts = {};
  for (const st of microStops) {
    if (st.structAt && st.structAt < st.microStopAt) {
      early += 1;
      leads.push(st.microStopAt - st.structAt);
      const lossAtStruct = st.dir > 0 ? ((st.entryMid - st.structMid) / st.entryMid) * 1e4 : ((st.structMid - st.entryMid) / st.entryMid) * 1e4;
      const lossAtStop = st.dir > 0 ? ((st.entryMid - st.microStopMid) / st.entryMid) * 1e4 : ((st.microStopMid - st.entryMid) / st.entryMid) * 1e4;
      saved.push(lossAtStop - lossAtStruct);
      reasonCounts[st.structEvent] = (reasonCounts[st.structEvent] || 0) + 1;
    }
  }
  const med = (a) => { if (!a.length) return 0; const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };

  // 2. Chronological Train (first 50%) -> Test (second 50%): Brier Skill Score & Sub-motif Pruning!
  const allRes = [];
  for (const ctx of streams.values()) {
    for (const r of ctx.resolvedEval10s) if (r.moved !== 0) allRes.push(r);
  }
  allRes.sort((a, b) => a.t - b.t);
  const split = Math.floor(allRes.length / 2);
  const train = allRes.slice(0, split);
  const test = allRes.slice(split);

  // Learn 2-gram and 3-gram excess lift over rolling base rate on TRAIN only!
  const stats2 = new Map(), stats3 = new Map();
  for (const r of train) {
    if (r.c2Key) {
      const yAnchor = (r.moved === r.c2Anchor) ? 1 : 0;
      const baseAnchor = r.c2Anchor > 0 ? r.rollBaseUp : (1 - r.rollBaseUp);
      const s = stats2.get(r.c2Key) || { n: 0, wins: 0, baseSum: 0 };
      s.n += 1; s.wins += yAnchor; s.baseSum += baseAnchor;
      stats2.set(r.c2Key, s);
    }
    if (r.c3Key) {
      const yAnchor = (r.moved === r.c3Anchor) ? 1 : 0;
      const baseAnchor = r.c3Anchor > 0 ? r.rollBaseUp : (1 - r.rollBaseUp);
      const s = stats3.get(r.c3Key) || { n: 0, wins: 0, baseSum: 0, suffix2: r.c3Suffix2 };
      s.n += 1; s.wins += yAnchor; s.baseSum += baseAnchor;
      stats3.set(r.c3Key, s);
    }
  }

  // Sub-motif pruning check: how many k=3 motifs beat their k=2 suffix by >= 6% excess lift?
  let k3Candidates = 0, k3KeptOverSuffix = 0, k3PrunedAsRedundant = 0;
  const approvedMotifs = new Map(); // key -> shrunk excess lift

  for (const [k2, s] of stats2.entries()) {
    if (s.n >= 8) {
      const empRate = s.wins / s.n;
      const baseRate = s.baseSum / s.n;
      const lift = (empRate - baseRate) * (s.n / (s.n + 12)); // Empirical Bayes shrinkage!
      if (Math.abs(lift) >= 0.05) approvedMotifs.set(`2:${k2}`, lift);
    }
  }
  for (const [k3, s] of stats3.entries()) {
    if (s.n >= 6) {
      k3Candidates += 1;
      const emp3 = s.wins / s.n - (s.baseSum / s.n);
      const suf = stats2.get(s.suffix2);
      const emp2 = suf && suf.n >= 6 ? (suf.wins / suf.n - suf.baseSum / suf.n) : 0;
      if (Math.abs(emp3 - emp2) >= 0.06 && Math.abs(emp3) >= 0.07) {
        k3KeptOverSuffix += 1;
        approvedMotifs.set(`3:${k3}`, emp3 * (s.n / (s.n + 12)));
      } else {
        k3PrunedAsRedundant += 1;
      }
    }
  }

  // Evaluate out-of-sample on TEST set:
  // (a) Naive 0.50, (b) Rolling Base Rate, (c) Linear Engine v2, (d) DNA Shadow (speaking ONLY on approved motifs!)
  let brier05 = 0, brierBase = 0, brierLin = 0, brierDnaAll = 0;
  let dnaSpokeCount = 0, brierDnaWhenSpoke = 0, brierBaseWhenSpoke = 0, brierLinWhenSpoke = 0;

  for (const r of test) {
    const yUp = r.moved > 0 ? 1 : 0;
    const pBase = clamp(r.rollBaseUp, 0.15, 0.85);
    const pLin = r.linProb;

    let liftAnchor = null, anchor = 0;
    if (r.c3Key && approvedMotifs.has(`3:${r.c3Key}`)) {
      liftAnchor = approvedMotifs.get(`3:${r.c3Key}`);
      anchor = r.c3Anchor;
    } else if (r.c2Key && approvedMotifs.has(`2:${r.c2Key}`)) {
      liftAnchor = approvedMotifs.get(`2:${r.c2Key}`);
      anchor = r.c2Anchor;
    }

    let pDna = pBase; // When silent, falls back to neutral empirical base rate
    if (liftAnchor !== null) {
      const pUpShift = anchor > 0 ? liftAnchor : -liftAnchor;
      pDna = clamp(pBase + pUpShift, 0.12, 0.88);
      dnaSpokeCount += 1;
      brierDnaWhenSpoke += Math.pow(pDna - yUp, 2);
      brierBaseWhenSpoke += Math.pow(pBase - yUp, 2);
      brierLinWhenSpoke += Math.pow(pLin - yUp, 2);
    }

    brier05 += Math.pow(0.5 - yUp, 2);
    brierBase += Math.pow(pBase - yUp, 2);
    brierLin += Math.pow(pLin - yUp, 2);
    brierDnaAll += Math.pow(pDna - yUp, 2);
  }

  const nT = Math.max(1, test.length);
  const nS = Math.max(1, dnaSpokeCount);

  const out = {
    structuralInvalidationAudit: {
      totalDirectionalThesesOpened: totalTheses,
      currentHtml12BpsStopTriggeredCount: totalHtml12Stops,
      realisticMicroStopTriggeredCount: microStops.length,
      caughtEarlyByDnaStructureCount: early,
      caughtEarlyByDnaStructurePct: microStops.length ? +((early / microStops.length) * 100).toFixed(1) : 0,
      medianEarlyWarningLeadMs: Math.round(med(leads)),
      medianSavedLossBps: +med(saved).toFixed(2),
      topInvalidatingEvents: reasonCounts
    },
    pstPruningAudit: {
      k3MotifsEvaluated: k3Candidates,
      k3KeptAddedNewInfoOverSuffix: k3KeptOverSuffix,
      k3PrunedRedundantPct: k3Candidates ? +((k3PrunedAsRedundant / k3Candidates) * 100).toFixed(1) : 0
    },
    outOfSampleWalkForwardTest: {
      nTestMovedSamples: test.length,
      dnaSilenceRatePct: +(((test.length - dnaSpokeCount) / nT) * 100).toFixed(1),
      dnaActiveRatePct: +((dnaSpokeCount / nT) * 100).toFixed(1),
      allTicksBrier: {
        naive050: +(brier05 / nT).toFixed(4),
        rollingBaseRate: +(brierBase / nT).toFixed(4),
        linearEngineV2: +(brierLin / nT).toFixed(4),
        dnaShadowWithBaseFallback: +(brierDnaAll / nT).toFixed(4)
      },
      whenDnaSpeaksOnlyBrier: {
        nActivePredictions: dnaSpokeCount,
        rollingBaseRateBrier: +(brierBaseWhenSpoke / nS).toFixed(4),
        linearEngineV2Brier: +(brierLinWhenSpoke / nS).toFixed(4),
        dnaShadowActiveBrier: +(brierDnaWhenSpoke / nS).toFixed(4),
        brierSkillScoreVsBasePct: brierBaseWhenSpoke > 0 ? +(((1 - brierDnaWhenSpoke / brierBaseWhenSpoke)) * 100).toFixed(2) : 0
      }
    }
  };

  fs.writeFileSync(path.join(__dirname, 'live_dna_report_v2.json'), JSON.stringify(out, null, 2));
  console.log(JSON.stringify(out, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
