'use strict';

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const DURATION_MS = 75_000; // 75 seconds live capture across 12 streams = 15 stream-minutes
const TICK_MS = 500;
const HORIZONS_SEC = [5, 10, 20]; // Multi-horizon evaluation within 75s window

const BINANCE_PAIRS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'SUIUSDT'];
const OKX_PAIRS = ['BTC-USDT-SWAP', 'ETH-USDT-SWAP', 'SOL-USDT-SWAP', 'XRP-USDT-SWAP', 'DOGE-USDT-SWAP', 'SUI-USDT-SWAP'];

const clamp = (v, lo, hi) => (Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : lo);

function createStreamContext(id, venue, symbol) {
  return {
    id,
    venue,
    symbol,
    bids: new Map(),
    asks: new Map(),
    hasBook: false,
    synced: venue === 'okx', // OKX sends full snapshot first on ws
    lastUpdateId: 0,
    buffer: [],
    depthUpdates: 0,
    tradePackets: 0,
    resets: 0,
    trades: [],
    totalTradeCount: 0,
    cumNotional: 0,
    midHistory: [],
    volRate: 3.0, // bps/sec EWMA
    bandUnit: 0.0012,
    spreadEwma: 1.5,
    flowRateEwma: 0, // notional per sec
    wallTrack: { bid: null, ask: null },
    // State hysteresis memory for clean observational transitions
    hyst: {
      obiState: 0, // -1, 0, +1
      microState: 0,
      burstActive: 0, // -1, 0, +1
      burstPeakRate: 0,
      burstStartAt: 0,
      burstStartMid: 0,
      lastVoidBid: null,
      lastVoidAsk: null,
      lastCooldown: {} // eventKey -> timestamp
    },
    naiveTokens: [],       // 500ms naive threshold states (to measure stuttering)
    dnaEvents: [],         // Clean observational state-transition events
    transitions3Clock: [], // { from, to, dtMs, dTrades, dVolNorm, postVol5s, postAbsMove5s, t }
    rawPending: { 5: [], 10: [], 20: [] },
    rawResolved: { 5: [], 10: [], 20: [] },
    episodes: [],          // Motif episodes triggered in real time
    activeTheses: [],      // For testing Structural Invalidation vs Price Stop
    invalidationLogs: [],
    snapshotsForComparison: [] // { t, id, mid, linearScore, features, last3Events, move10Bps }
  };
}

const streams = new Map();
for (const sym of BINANCE_PAIRS) streams.set(`BIN:${sym}`, createStreamContext(`BIN:${sym}`, 'binance', sym));
for (const sym of OKX_PAIRS) streams.set(`OKX:${sym}`, createStreamContext(`OKX:${sym}`, 'okx', sym));

function getSortedLevels(map, desc, limit = 200) {
  const arr = [];
  for (const [p, q] of map.entries()) {
    if (q > 0 && Number.isFinite(p)) arr.push({ price: p, qty: q });
  }
  arr.sort((a, b) => (desc ? b.price - a.price : a.price - b.price));
  return arr.length > limit ? arr.slice(0, limit) : arr;
}

function applyLevels(map, levels) {
  if (!Array.isArray(levels)) return;
  for (const lv of levels) {
    const p = Number(lv[0]);
    const q = Number(lv[1]);
    if (!Number.isFinite(p)) continue;
    if (q <= 0) map.delete(p);
    else map.set(p, q);
  }
  if (map.size > 1200) {
    // trim outer levels periodically
    const keys = Array.from(map.keys()).sort((a, b) => a - b);
    if (keys.length > 1000) {
      for (let i = 0; i < 100; i++) map.delete(keys[i]);
      for (let i = keys.length - 100; i < keys.length; i++) map.delete(keys[i]);
    }
  }
}

function pushResetBoundary(ctx, reason) {
  ctx.resets += 1;
  ctx.dnaEvents.push({
    t: Date.now(),
    type: '<RESET>',
    side: 0,
    reason,
    tradeSeq: ctx.totalTradeCount,
    cumVol: ctx.cumNotional
  });
}

async function syncBinanceSnapshot(ctx) {
  try {
    const r = await fetch(`https://data-api.binance.vision/api/v3/depth?symbol=${ctx.symbol}&limit=500`);
    if (!r.ok) return;
    const snap = await r.json();
    ctx.bids.clear();
    ctx.asks.clear();
    applyLevels(ctx.bids, snap.bids);
    applyLevels(ctx.asks, snap.asks);
    ctx.lastUpdateId = Number(snap.lastUpdateId);
    for (const ev of ctx.buffer) {
      if (ev.u <= ctx.lastUpdateId) continue;
      applyLevels(ctx.bids, ev.b);
      applyLevels(ctx.asks, ev.a);
      ctx.lastUpdateId = ev.u;
    }
    ctx.buffer = [];
    ctx.synced = true;
    ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
    pushResetBoundary(ctx, 'snapshot_sync');
  } catch (_) {}
}

function startBinanceFeed() {
  const names = [];
  for (const sym of BINANCE_PAIRS) {
    const s = sym.toLowerCase();
    names.push(`${s}@depth@100ms`, `${s}@aggTrade`);
  }
  const ws = new WebSocket(`wss://data-stream.binance.vision/stream?streams=${names.join('/')}`);
  ws.on('open', () => {
    for (const sym of BINANCE_PAIRS) syncBinanceSnapshot(streams.get(`BIN:${sym}`));
  });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (!msg || !msg.stream || !msg.data) return;
    const [symLower, channel] = msg.stream.split('@');
    const ctx = streams.get(`BIN:${symLower.toUpperCase()}`);
    if (!ctx) return;
    const d = msg.data;
    if (channel === 'depth') {
      const U = Number(d.U), u = Number(d.u);
      if (!ctx.synced) {
        ctx.buffer.push({ U, u, b: d.b, a: d.a });
        if (ctx.buffer.length > 300) ctx.buffer.shift();
        return;
      }
      if (u <= ctx.lastUpdateId) return;
      if (U > ctx.lastUpdateId + 1) {
        // Sequence gap -> resync & emit <RESET>
        ctx.synced = false;
        syncBinanceSnapshot(ctx);
        return;
      }
      applyLevels(ctx.bids, d.b);
      applyLevels(ctx.asks, d.a);
      ctx.lastUpdateId = u;
      ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
      ctx.depthUpdates += 1;
      onBookOrFlowUpdate(ctx, Date.now());
    } else if (channel === 'aggTrade') {
      const price = Number(d.p), qty = Number(d.q);
      if (!Number.isFinite(price) || !Number.isFinite(qty) || qty <= 0) return;
      const t = Number(d.T) || Date.now();
      const notional = price * qty;
      const side = d.m ? -1 : 1; // m=true -> buyer is maker -> aggressive sell (-1)
      recordTrade(ctx, t, price, qty, notional, side);
    }
  });
  ws.on('error', () => {});
  return ws;
}

function startOkxFeed() {
  const ws = new WebSocket('wss://ws.okx.com:443/ws/v5/public');
  ws.on('open', () => {
    const args = [];
    for (const instId of OKX_PAIRS) {
      args.push({ channel: 'books', instId }, { channel: 'trades', instId });
    }
    ws.send(JSON.stringify({ op: 'subscribe', args }));
  });
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { return; }
    if (!msg || !msg.arg || !msg.data) return;
    const ctx = streams.get(`OKX:${msg.arg.instId}`);
    if (!ctx) return;
    if (msg.arg.channel === 'books') {
      for (const book of msg.data) {
        if (msg.action === 'snapshot') {
          ctx.bids.clear();
          ctx.asks.clear();
          pushResetBoundary(ctx, 'okx_snapshot');
        }
        applyLevels(ctx.bids, book.bids);
        applyLevels(ctx.asks, book.asks);
        ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
        ctx.depthUpdates += 1;
      }
      onBookOrFlowUpdate(ctx, Date.now());
    } else if (msg.arg.channel === 'trades') {
      for (const tr of msg.data) {
        const price = Number(tr.px), qty = Number(tr.sz);
        if (!Number.isFinite(price) || !Number.isFinite(qty) || qty <= 0) continue;
        // Note: For OKX swap, sz is contracts, px*sz gives proportional notional unit (consistent within stream)
        const notional = price * qty;
        const side = tr.side === 'buy' ? 1 : -1;
        const t = Number(tr.ts) || Date.now();
        recordTrade(ctx, t, price, qty, notional, side);
      }
    }
  });
  ws.on('error', () => {});
  return ws;
}

function recordTrade(ctx, t, price, qty, notional, side) {
  ctx.trades.push({ t, price, qty, notional, side });
  if (ctx.trades.length > 5000) ctx.trades.splice(0, 1500);
  ctx.tradePackets += 1;
  ctx.totalTradeCount += 1;
  ctx.cumNotional += notional;
}

function sumFlow(ctx, now, windowMs) {
  let buy = 0, sell = 0, count = 0;
  for (let i = ctx.trades.length - 1; i >= 0; i--) {
    const tr = ctx.trades[i];
    if (now - tr.t > windowMs) break;
    if (tr.side > 0) buy += tr.notional;
    else sell += tr.notional;
    count += 1;
  }
  return { buy, sell, total: buy + sell, net: buy - sell, count };
}

function tradesNearPrice(ctx, now, targetPrice, tolFrac, mid, windowMs) {
  let buy = 0, sell = 0;
  if (!targetPrice) return { buy, sell };
  for (let i = ctx.trades.length - 1; i >= 0; i--) {
    const tr = ctx.trades[i];
    if (now - tr.t > windowMs) break;
    if (Math.abs(tr.price - targetPrice) / mid <= tolFrac) {
      if (tr.side > 0) buy += tr.notional;
      else sell += tr.notional;
    }
  }
  return { buy, sell };
}

function emitDnaEvent(ctx, now, type, side, mid, meta = {}) {
  const key = `${type}:${side}`;
  const cd = meta.cooldownMs || 1400;
  if (ctx.hyst.lastCooldown[key] && now - ctx.hyst.lastCooldown[key] < cd) return;

  // Avoid immediate identical duplicate token
  const prev = ctx.dnaEvents.length ? ctx.dnaEvents[ctx.dnaEvents.length - 1] : null;
  if (prev && prev.type === type && prev.side === side && now - prev.t < 3500) return;

  ctx.hyst.lastCooldown[key] = now;
  const ev = {
    t: now,
    type,
    side, // +1 = bullish-side structure (e.g. bid wall / buy burst), -1 = bearish-side structure
    mid,
    tradeSeq: ctx.totalTradeCount,
    cumVol: ctx.cumNotional,
    volRate: ctx.volRate,
    unit: ctx.bandUnit,
    meta
  };

  // Record 3-Clock delta if previous event was not <RESET>
  if (prev && prev.type !== '<RESET>') {
    const dtMs = Math.max(1, now - prev.t);
    const dTrades = ctx.totalTradeCount - prev.tradeSeq;
    const baselineVolPerSec = Math.max(1e-6, ctx.flowRateEwma);
    const dVolNorm = (ctx.cumNotional - prev.cumVol) / baselineVolPerSec; // "volume-clock seconds"
    ctx.transitions3Clock.push({
      stream: ctx.id,
      from: prev.type,
      to: type,
      sameSide: prev.side === side ? 1 : -1,
      dtMs,
      dTrades,
      dVolNorm,
      t: now,
      midAtTo: mid
    });
  }

  ctx.dnaEvents.push(ev);
  checkStructuralInvalidations(ctx, ev, now);
  onNewEventEpisodeCheck(ctx, now);
}

// Event-driven high-resolution observer (throttled to 150ms per stream to catch sub-second sequences!)
function onBookOrFlowUpdate(ctx, now) {
  if (!ctx.hasBook) return;
  if (ctx.lastEventCheckAt && now - ctx.lastEventCheckAt < 150) return;
  ctx.lastEventCheckAt = now;

  const bids = getSortedLevels(ctx.bids, true, 120);
  const asks = getSortedLevels(ctx.asks, false, 120);
  if (bids.length < 5 || asks.length < 5) return;
  const bestBid = bids[0].price, bestAsk = asks[0].price;
  if (!(bestAsk > bestBid)) return;

  const mid = (bestBid + bestAsk) / 2;
  const unit = ctx.bandUnit;
  const bandWide = unit * 5;

  // Median level size for adaptive wall & void detection
  const medQty = (levels) => {
    const q = levels.slice(0, 50).map((x) => x.qty).sort((a, b) => a - b);
    return q.length ? q[Math.floor(q.length / 2)] : 0;
  };
  const bidMed = medQty(bids), askMed = medQty(asks);

  // 1) Wall Lifecycle Transitions: WALL_SPAWN, WALL_HOLD, WALL_PULL, WALL_BREAK
  const scanWall = (levels, med) => {
    let best = null;
    let sumNear = 0;
    for (const lv of levels) {
      const dist = Math.abs(lv.price - mid) / mid;
      if (dist > bandWide) break;
      const notional = lv.price * lv.qty;
      sumNear += notional;
      if (med > 0 && lv.qty >= med * 4.0) {
        if (!best || notional > best.notional) best = { price: lv.price, qty: lv.qty, notional, dist };
      }
    }
    if (best && sumNear > 0 && best.notional / sumNear >= 0.20) return best;
    return null;
  };

  for (const [sideKey, sideSign, levels, med] of [['bid', 1, bids, bidMed], ['ask', -1, asks, askMed]]) {
    const found = scanWall(levels, med);
    const cur = ctx.wallTrack[sideKey];
    if (!cur && found && found.dist <= unit * 3.0) {
      ctx.wallTrack[sideKey] = {
        price: found.price,
        peakNotional: found.notional,
        notional: found.notional,
        firstSeen: now,
        lastSeen: now,
        samples: 1,
        holdEmitted: false
      };
      emitDnaEvent(ctx, now, 'WALL_SPAWN', sideSign, mid, { price: found.price, distBps: found.dist * 1e4 });
    } else if (cur) {
      const samePrice = found && Math.abs(found.price - cur.price) / mid < unit * 0.5;
      if (samePrice) {
        cur.samples += 1;
        cur.lastSeen = now;
        cur.notional = found.notional;
        cur.peakNotional = Math.max(cur.peakNotional, found.notional);

        // Check WALL_HOLD (Observational: aggressive opposing trades hit near wall, wall survived & price didn't breach)
        if (!cur.holdEmitted && cur.samples >= 4 && Math.abs(mid - cur.price) / mid <= unit * 1.1) {
          const nearTr = tradesNearPrice(ctx, now, cur.price, unit * 0.65, mid, 4000);
          const opposingHit = sideSign > 0 ? nearTr.sell : nearTr.buy;
          if (opposingHit >= cur.peakNotional * 0.18 && cur.notional >= cur.peakNotional * 0.50) {
            cur.holdEmitted = true;
            emitDnaEvent(ctx, now, 'WALL_HOLD', sideSign, mid, { price: cur.price, opposingHit });
          }
        }
      } else {
        // Wall vanished or moved away! Classify WHY: WALL_BREAK vs WALL_PULL
        const breached = sideSign > 0 ? (mid < cur.price) : (mid > cur.price);
        const nearTr = tradesNearPrice(ctx, now, cur.price, unit * 0.65, mid, 4000);
        const opposingHit = sideSign > 0 ? nearTr.sell : nearTr.buy;
        const wasClose = Math.abs(cur.price - mid) / mid < unit * 2.5;

        if (breached && opposingHit > cur.peakNotional * 0.12) {
          emitDnaEvent(ctx, now, 'WALL_BREAK', sideSign, mid, { price: cur.price });
        } else if (cur.samples >= 3 && wasClose && opposingHit < cur.peakNotional * 0.15) {
          emitDnaEvent(ctx, now, 'WALL_PULL', sideSign, mid, { price: cur.price });
        }
        ctx.wallTrack[sideKey] = null;
      }
    }
  }

  // 2) Flow Transitions: FLOW_BURST & FLOW_EXHAUST
  const flow2s = sumFlow(ctx, now, 2000);
  const flow15s = sumFlow(ctx, now, 15000);
  const base2s = Math.max(1e-6, (flow15s.total / 15) * 2);
  ctx.flowRateEwma = ctx.flowRateEwma ? (ctx.flowRateEwma * 0.95 + (flow2s.total / 2) * 0.05) : (flow2s.total / 2);

  if (flow2s.count >= 3 && flow2s.total > base2s * 2.2) {
    const shareBuy = flow2s.buy / (flow2s.total + 1e-9);
    if (shareBuy >= 0.74 && ctx.hyst.burstActive !== 1) {
      ctx.hyst.burstActive = 1;
      ctx.hyst.burstPeakRate = flow2s.total;
      ctx.hyst.burstStartAt = now;
      ctx.hyst.burstStartMid = mid;
      emitDnaEvent(ctx, now, 'FLOW_BURST', 1, mid, { ratio: flow2s.total / base2s });
    } else if (shareBuy <= 0.26 && ctx.hyst.burstActive !== -1) {
      ctx.hyst.burstActive = -1;
      ctx.hyst.burstPeakRate = flow2s.total;
      ctx.hyst.burstStartAt = now;
      ctx.hyst.burstStartMid = mid;
      emitDnaEvent(ctx, now, 'FLOW_BURST', -1, mid, { ratio: flow2s.total / base2s });
    } else if (ctx.hyst.burstActive !== 0) {
      ctx.hyst.burstPeakRate = Math.max(ctx.hyst.burstPeakRate, flow2s.total);
    }
  } else if (ctx.hyst.burstActive !== 0 && now - ctx.hyst.burstStartAt >= 900) {
    if (flow2s.total < ctx.hyst.burstPeakRate * 0.32) {
      emitDnaEvent(ctx, now, 'FLOW_EXHAUST', ctx.hyst.burstActive, mid, {});
      ctx.hyst.burstActive = 0;
    }
  }

  // 3) Orderbook Imbalance Hysteresis Transition: BOOK_TILT
  let bq = 0, aq = 0;
  const lo = mid * (1 - unit), hi = mid * (1 + unit), dec = mid * unit;
  for (const lv of bids) { if (lv.price < lo) break; bq += lv.qty * Math.exp(-Math.abs(mid - lv.price) / dec); }
  for (const lv of asks) { if (lv.price > hi) break; aq += lv.qty * Math.exp(-Math.abs(lv.price - mid) / dec); }
  const obi = (bq - aq) / (bq + aq + 1e-9);

  if (obi > 0.36 && ctx.hyst.obiState !== 1) {
    ctx.hyst.obiState = 1;
    emitDnaEvent(ctx, now, 'BOOK_TILT', 1, mid, { obi });
  } else if (obi < -0.36 && ctx.hyst.obiState !== -1) {
    ctx.hyst.obiState = -1;
    emitDnaEvent(ctx, now, 'BOOK_TILT', -1, mid, { obi });
  } else if (Math.abs(obi) < 0.12) {
    ctx.hyst.obiState = 0;
  }

  // 4) Microprice Flip across spread: MICRO_FLIP
  const spread = bestAsk - bestBid;
  const microPx = (bestAsk * bids[0].qty + bestBid * asks[0].qty) / (bids[0].qty + asks[0].qty || 1);
  const microSkew = (microPx - mid) / (spread / 2 + 1e-9); // in [-1, +1]
  if (microSkew > 0.55 && ctx.hyst.microState !== 1) {
    ctx.hyst.microState = 1;
    emitDnaEvent(ctx, now, 'MICRO_FLIP', 1, mid, { microSkew });
  } else if (microSkew < -0.55 && ctx.hyst.microState !== -1) {
    ctx.hyst.microState = -1;
    emitDnaEvent(ctx, now, 'MICRO_FLIP', -1, mid, { microSkew });
  } else if (Math.abs(microSkew) < 0.15) {
    ctx.hyst.microState = 0;
  }
}

// Canonical symmetry encoding:
// Anchor polarity = side of the LAST event in the sequence (+1 = bull anchor, -1 = bear anchor).
// Earlier events encode whether their side agrees (+ = aligned, - = opposing) with the anchor!
// Example:
//   BID_WALL_SPAWN(+1) -> SELL_FLOW_BURST(-1) -> BID_WALL_HOLD(+1)
//   Anchor = +1 (bull ledger), Canonical pattern = "+WALL_SPAWN -> -FLOW_BURST -> +WALL_HOLD"
//   ASK_WALL_SPAWN(-1) -> BUY_FLOW_BURST(+1) -> ASK_WALL_HOLD(-1)
//   Anchor = -1 (bear ledger), Canonical pattern = "+WALL_SPAWN -> -FLOW_BURST -> +WALL_HOLD" (EXACT MIRROR!)
function extractRecentEvents(ctx, k) {
  const out = [];
  for (let i = ctx.dnaEvents.length - 1; i >= 0 && out.length < k; i--) {
    const ev = ctx.dnaEvents[i];
    if (ev.type === '<RESET>') break; // Never cross a reset/gap boundary!
    out.unshift(ev);
  }
  return out.length === k ? out : null;
}

function canonicalize(seq) {
  const anchor = seq[seq.length - 1].side;
  const tokens = seq.map((e) => `${e.side === anchor ? '+' : '-'}${e.type}`);
  return { key: tokens.join(' -> '), anchor, spanMs: seq[seq.length - 1].t - seq[0].t };
}

function onNewEventEpisodeCheck(ctx, now) {
  for (const k of [2, 3]) {
    const seq = extractRecentEvents(ctx, k);
    if (!seq) continue;
    const { key, anchor, spanMs } = canonicalize(seq);
    if (spanMs > 25_000) continue; // Events too far apart don't form a coherent microstructure episode

    // Non-overlapping per-motif episode cooldown (10s) on this stream
    const epCdKey = `ep:${key}:${anchor}`;
    if (ctx.hyst.lastCooldown[epCdKey] && now - ctx.hyst.lastCooldown[epCdKey] < 10_000) continue;
    ctx.hyst.lastCooldown[epCdKey] = now;

    const mid = seq[seq.length - 1].mid;
    ctx.episodes.push({
      stream: ctx.id,
      k,
      key,
      anchor, // +1 bull-side structure, -1 bear-side structure
      t: now,
      mid,
      spanMs,
      unitBps: ctx.bandUnit * 1e4,
      outcomes: {} // filled as horizons 5s, 10s, 20s mature
    });

    // Also register an active thesis to test Structural Invalidation vs Price Stop!
    if (k === 2 || k === 3) {
      ctx.activeTheses.push({
        t: now,
        dir: anchor, // testing structural support in direction of anchor
        entryMid: mid,
        stopDist: mid * ctx.bandUnit * 1.5, // exact formula from orderbook-mobile engineNote()!
        structInvalidAt: null,
        structInvalidMid: null,
        structReason: null,
        priceStopAt: null,
        priceStopMid: null,
        closed: false
      });
    }
  }
}

function checkStructuralInvalidations(ctx, ev, now) {
  for (const th of ctx.activeTheses) {
    if (th.closed || th.structInvalidAt) continue;
    // Structural invalidation occurs if supporting wall pulls/breaks OR opposing burst+tilt hits
    const supportBroken = (ev.side === th.dir) && (ev.type === 'WALL_PULL' || ev.type === 'WALL_BREAK');
    const counterStructure = (ev.side === -th.dir) && (ev.type === 'WALL_SPAWN' || ev.type === 'BOOK_TILT');
    if (supportBroken || counterStructure) {
      th.structInvalidAt = now;
      th.structInvalidMid = ev.mid;
      th.structReason = `${ev.side === th.dir ? 'SUPPORT' : 'COUNTER'}_${ev.type}`;
    }
  }
}

// 500ms tick loop: runs faithful port of obm.engine.v2 linear score + resolves multi-horizon outcomes
const PRIOR_W = { obi_near: 0.18, obi_mid: 0.16, micro: 0.18, delta_s: 0.16, delta_m: 0.16, wall: 0.16 };

function tickStream(ctx, now) {
  if (!ctx.hasBook) return;
  const bids = getSortedLevels(ctx.bids, true, 120);
  const asks = getSortedLevels(ctx.asks, false, 120);
  if (bids.length < 5 || asks.length < 5) return;
  const bestBid = bids[0].price, bestAsk = asks[0].price;
  if (!(bestAsk > bestBid)) return;

  const mid = (bestBid + bestAsk) / 2;
  const spreadBps = ((bestAsk - bestBid) / mid) * 1e4;
  ctx.midHistory.push({ t: now, mid });
  if (ctx.midHistory.length > 500) ctx.midHistory.shift();

  // Volatility update
  const mh = ctx.midHistory;
  if (mh.length >= 4) {
    let vs = 0, vn = 0;
    for (let i = Math.max(1, mh.length - 20); i < mh.length; i++) {
      const dt = (mh[i].t - mh[i - 1].t) / 1000;
      if (dt > 0) {
        vs += (Math.abs(mh[i].mid - mh[i - 1].mid) / mh[i - 1].mid) * 1e4 / dt;
        vn += 1;
      }
    }
    if (vn) ctx.volRate = ctx.volRate * 0.85 + (vs / vn) * 0.15;
  }
  ctx.bandUnit = clamp(0.0008 + (ctx.volRate * 1.6) / 1e4, 0.0008, 0.006);
  ctx.spreadEwma = ctx.spreadEwma * 0.95 + spreadBps * 0.05;
  const unit = ctx.bandUnit;

  // Check price stops on activeTheses
  for (const th of ctx.activeTheses) {
    if (th.closed) continue;
    const adverse = th.dir > 0 ? (th.entryMid - mid) : (mid - th.entryMid);
    if (!th.priceStopAt && adverse >= th.stopDist) {
      th.priceStopAt = now;
      th.priceStopMid = mid;
      th.closed = true;
      ctx.invalidationLogs.push(th);
    } else if (now - th.t > 20_000) {
      th.closed = true;
      if (th.priceStopAt) ctx.invalidationLogs.push(th);
    }
  }

  // Linear features (obm.engine.v2 style)
  const imb = (frac) => {
    const lo = mid * (1 - frac), hi = mid * (1 + frac), dec = mid * frac;
    let bq = 0, aq = 0;
    for (const lv of bids) { if (lv.price < lo) break; bq += lv.qty * Math.exp(-Math.abs(mid - lv.price) / dec); }
    for (const lv of asks) { if (lv.price > hi) break; aq += lv.qty * Math.exp(-Math.abs(lv.price - mid) / dec); }
    return (bq - aq) / (bq + aq + 1e-9);
  };
  const obiNear = imb(unit);
  const obiMid = imb(unit * 2.5);
  const microPx = (bestAsk * bids[0].qty + bestBid * asks[0].qty) / (bids[0].qty + asks[0].qty || 1);
  const micro = clamp((((microPx - mid) / mid) * 1e4) / 8, -1, 1);
  const f5 = sumFlow(ctx, now, 5000);
  const f15 = sumFlow(ctx, now, 15000);
  const deltaS = f5.net / (f5.total + 1e-9);
  const deltaM = f15.net / (f15.total + 1e-9);
  const wBid = ctx.wallTrack.bid ? ctx.wallTrack.bid.notional : 0;
  const wAsk = ctx.wallTrack.ask ? ctx.wallTrack.ask.notional : 0;
  const wallF = (wBid - wAsk) / (wBid + wAsk + 1e-9);

  const linearScore =
    PRIOR_W.obi_near * obiNear +
    PRIOR_W.obi_mid * obiMid +
    PRIOR_W.micro * micro +
    PRIOR_W.delta_s * deltaS +
    PRIOR_W.delta_m * deltaM +
    PRIOR_W.wall * wallF;

  // Naive 500ms state token (for Test 1: Stuttering measurement)
  let naiveTok = 'NEUTRAL';
  if (obiNear > 0.25) naiveTok = '+OBI';
  else if (obiNear < -0.25) naiveTok = '-OBI';
  else if (deltaS > 0.35) naiveTok = '+FLOW';
  else if (deltaS < -0.35) naiveTok = '-FLOW';
  ctx.naiveTokens.push({ t: now, tok: naiveTok });

  // Push overlapping 500ms pending windows (exact behavior of orderbook-mobile!)
  const deadBpsBase = Math.max(1.0, ctx.spreadEwma * 1.2);
  for (const h of HORIZONS_SEC) {
    ctx.rawPending[h].push({
      t: now,
      endAt: now + h * 1000,
      mid,
      linearScore,
      deadBps: deadBpsBase * Math.sqrt(h / 10)
    });
  }

  // Save snapshot if we have a recent 3-event DNA chain (for "Same Linear Score, Different DNA" test!)
  const seq3 = extractRecentEvents(ctx, 3);
  if (seq3 && now - seq3[2].t < 2500) {
    const can = canonicalize(seq3);
    ctx.snapshotsForComparison.push({
      t: now,
      stream: ctx.id,
      mid,
      linearScore,
      dnaKey: can.key,
      anchor: can.anchor,
      move10Bps: null
    });
  }

  // Resolve matured rawPending, episodes,transitions3Clock, and snapshotsForComparison
  for (const h of HORIZONS_SEC) {
    const still = [];
    for (const p of ctx.rawPending[h]) {
      if (now < p.endAt) { still.push(p); continue; }
      const moveBps = ((mid - p.mid) / p.mid) * 1e4;
      const state3 = moveBps > p.deadBps ? 1 : (moveBps < -p.deadBps ? -1 : 0); // +1 Up, 0 Flat, -1 Down
      ctx.rawResolved[h].push({
        t0: p.t,
        t1: now,
        moveBps,
        state3,
        linearScore: p.linearScore
      });
    }
    ctx.rawPending[h] = still;
  }

  for (const ep of ctx.episodes) {
    for (const h of HORIZONS_SEC) {
      if (ep.outcomes[h] === undefined && now >= ep.t + h * 1000) {
        const rawMoveBps = ((mid - ep.mid) / ep.mid) * 1e4;
        // Signed in the anchor's reference frame: + means moved WITH anchor side, - means moved AGAINST anchor side
        ep.outcomes[h] = {
          t1: now,
          rawMoveBps,
          anchorSignedBps: rawMoveBps * ep.anchor,
          deadBps: deadBpsBase * Math.sqrt(h / 10)
        };
      }
    }
  }

  for (const tr of ctx.transitions3Clock) {
    if (tr.postAbsMove5s === undefined && now >= tr.t + 5000) {
      tr.postAbsMove5s = Math.abs(((mid - tr.midAtTo) / tr.midAtTo) * 1e4);
    }
  }

  for (const sc of ctx.snapshotsForComparison) {
    if (sc.move10Bps === null && now >= sc.t + 10_000) {
      sc.move10Bps = ((mid - sc.mid) / sc.mid) * 1e4;
    }
  }
}

// Compute López de Prado Uniqueness Effective Sample Size (N_eff) vs Raw Count (N_raw)
function computeEffectiveSampleSize(intervals) {
  if (!intervals.length) return { nRaw: 0, nEff: 0, distinctWaves: 0 };
  // Build concurrency grid at 500ms resolution
  const minT = Math.min(...intervals.map((x) => x.t0));
  const maxT = Math.max(...intervals.map((x) => x.t1));
  const step = 500;
  const bins = new Int32Array(Math.ceil((maxT - minT) / step) + 2);

  for (const iv of intervals) {
    const b0 = Math.floor((iv.t0 - minT) / step);
    const b1 = Math.max(b0, Math.floor((iv.t1 - minT) / step));
    for (let b = b0; b <= b1; b++) bins[b] += 1;
  }

  // Distinct waves = connected components of active bins > 0 with directional displacement
  let distinctWaves = 0;
  let inWave = false;
  for (let b = 0; b < bins.length; b++) {
    if (bins[b] > 0 && !inWave) { distinctWaves += 1; inWave = true; }
    else if (bins[b] === 0) inWave = false;
  }

  // Average uniqueness u_i = mean_{t in [t0, t1]} (1 / concurrency_t)
  let sumUniqueness = 0;
  for (const iv of intervals) {
    const b0 = Math.floor((iv.t0 - minT) / step);
    const b1 = Math.max(b0, Math.floor((iv.t1 - minT) / step));
    let u = 0, cnt = 0;
    for (let b = b0; b <= b1; b++) {
      u += 1 / Math.max(1, bins[b]);
      cnt += 1;
    }
    sumUniqueness += u / Math.max(1, cnt);
  }
  return { nRaw: intervals.length, nEff: sumUniqueness, distinctWaves };
}

function pearsonCorr(xs, ys) {
  const n = Math.min(xs.length, ys.length);
  if (n < 3) return 0;
  let mx = 0, my = 0;
  for (let i = 0; i < n; i++) { mx += xs[i]; my += ys[i]; }
  mx /= n; my /= n;
  let num = 0, dx = 0, dy = 0;
  for (let i = 0; i < n; i++) {
    const vx = xs[i] - mx, vy = ys[i] - my;
    num += vx * vy; dx += vx * vx; dy += vy * vy;
  }
  return (dx > 0 && dy > 0) ? num / Math.sqrt(dx * dy) : 0;
}

async function main() {
  console.log(`[DNA-LAB] Starting ${DURATION_MS / 1000}s live multi-venue L2 + Trade microstructure capture across 12 streams...`);
  const tStart = Date.now();
  const bWs = startBinanceFeed();
  const oWs = startOkxFeed();

  const tickTimer = setInterval(() => {
    const now = Date.now();
    for (const ctx of streams.values()) tickStream(ctx, now);
  }, TICK_MS);

  const progressTimer = setInterval(() => {
    const elapsed = Math.round((Date.now() - tStart) / 1000);
    let totDepth = 0, totTrades = 0, totEvents = 0, totEp = 0;
    for (const ctx of streams.values()) {
      totDepth += ctx.depthUpdates;
      totTrades += ctx.tradePackets;
      totEvents += ctx.dnaEvents.length;
      totEp += ctx.episodes.length;
    }
    console.log(`[+${elapsed}s] depthUpdates=${totDepth} trades=${totTrades} cleanDnaEvents=${totEvents} episodes=${totEp}`);
  }, 15_000);

  await new Promise((r) => setTimeout(r, DURATION_MS));

  clearInterval(tickTimer);
  clearInterval(progressTimer);
  try { bWs.close(); } catch (_) {}
  try { oWs.close(); } catch (_) {}

  // ==================== AGGREGATE & ANALYZE EMPIRICAL RESULTS ====================
  const report = {
    meta: { durationSec: DURATION_MS / 1000, streamCount: streams.size, capturedAt: new Date().toISOString() },
    streamSummary: [],
    test1_stutteringVsTransition: {},
    test2_threeStateBaseRates: {},
    test3_sampleInflation: {},
    test4_orderAsymmetry: [],
    test4_sameLinearDifferentDna: [],
    test5_threeClocks: {},
    test6_structuralInvalidation: {},
    test7_quarantineFunnel: {}
  };

  let totalDepth = 0, totalTrades = 0, totalNaiveChanges = 0, totalNaiveStutters = 0, totalCleanEvents = 0;

  for (const ctx of streams.values()) {
    totalDepth += ctx.depthUpdates;
    totalTrades += ctx.tradePackets;
    const cleanNoReset = ctx.dnaEvents.filter((e) => e.type !== '<RESET>');
    totalCleanEvents += cleanNoReset.length;

    // Count naive consecutive repeats & A-B-A ping-pong stutters
    for (let i = 1; i < ctx.naiveTokens.length; i++) {
      const cur = ctx.naiveTokens[i].tok;
      const prev = ctx.naiveTokens[i - 1].tok;
      if (cur !== 'NEUTRAL') {
        totalNaiveChanges += 1;
        if (cur === prev || (i >= 2 && cur === ctx.naiveTokens[i - 2].tok && cur !== prev)) {
          totalNaiveStutters += 1;
        }
      }
    }

    report.streamSummary.push({
      id: ctx.id,
      depthUpdates: ctx.depthUpdates,
      trades: ctx.tradePackets,
      cleanEvents: cleanNoReset.length,
      episodes: ctx.episodes.length,
      resets: ctx.resets
    });
  }

  report.test1_stutteringVsTransition = {
    totalL2DepthPackets: totalDepth,
    totalTradePackets: totalTrades,
    naiveActiveTickTokens: totalNaiveChanges,
    naiveRedundantOrPingPongPct: totalNaiveChanges ? +((totalNaiveStutters / totalNaiveChanges) * 100).toFixed(1) : 0,
    cleanStateTransitionEvents: totalCleanEvents,
    compressionRatioL2ToDna: totalCleanEvents ? +(totalDepth / totalCleanEvents).toFixed(1) : 0
  };

  // Test 2 & Test 3: 3-State Base Rates + Sample Inflation (500ms pending windows vs N_eff)
  for (const h of HORIZONS_SEC) {
    let up = 0, flat = 0, down = 0, total = 0;
    let sumRaw = 0, sumEff = 0, sumAutocorr = 0, acStreams = 0;

    for (const ctx of streams.values()) {
      const res = ctx.rawResolved[h];
      if (!res.length) continue;
      for (const r of res) {
        total += 1;
        if (r.state3 > 0) up += 1;
        else if (r.state3 < 0) down += 1;
        else flat += 1;
      }
      const eff = computeEffectiveSampleSize(res);
      sumRaw += eff.nRaw;
      sumEff += eff.nEff;

      if (res.length >= 10) {
        const a = res.slice(0, -1).map((x) => x.moveBps);
        const b = res.slice(1).map((x) => x.moveBps);
        sumAutocorr += pearsonCorr(a, b);
        acStreams += 1;
      }
    }

    report.test2_threeStateBaseRates[`${h}s`] = {
      nTicks: total,
      pUpPct: total ? +((up / total) * 100).toFixed(1) : 0,
      pFlatDeadZonePct: total ? +((flat / total) * 100).toFixed(1) : 0,
      pDownPct: total ? +((down / total) * 100).toFixed(1) : 0,
      upGivenMovedPct: (up + down) ? +((up / (up + down)) * 100).toFixed(1) : 50
    };

    report.test3_sampleInflation[`${h}s`] = {
      rawResolvedCount: sumRaw,
      effectiveSampleSizeNeff: +sumEff.toFixed(1),
      inflationFactor: sumEff > 0 ? +(sumRaw / sumEff).toFixed(1) : 1,
      lag1MoveAutocorr: acStreams ? +(sumAutocorr / acStreams).toFixed(3) : 0
    };
  }

  // Test 4A: Order Asymmetry (A -> B vs B -> A) across 10s horizon
  const motifMap = new Map();
  for (const ctx of streams.values()) {
    for (const ep of ctx.episodes) {
      const out10 = ep.outcomes[10];
      const out5 = ep.outcomes[5];
      const out20 = ep.outcomes[20];
      if (!out10) continue;
      if (!motifMap.has(ep.key)) {
        motifMap.set(ep.key, {
          key: ep.key,
          k: ep.k,
          episodes: [],
          bullOutcomes10: [], // anchor = +1
          bearOutcomes10: []  // anchor = -1
        });
      }
      const m = motifMap.get(ep.key);
      m.episodes.push({
        stream: ep.stream,
        t0: ep.t,
        t1: out10.t1,
        anchor: ep.anchor,
        bps5: out5 ? out5.anchorSignedBps : null,
        bps10: out10.anchorSignedBps,
        bps20: out20 ? out20.anchorSignedBps : null,
        dead10: out10.deadBps
      });
      if (ep.anchor > 0) m.bullOutcomes10.push(out10.anchorSignedBps);
      else m.bearOutcomes10.push(out10.anchorSignedBps);
    }
  }

  // Compare reversed 2-gram pairs: X -> Y vs Y -> X (using raw event type pairs!)
  const pairTypeStats = new Map();
  for (const ctx of streams.values()) {
    for (const ep of ctx.episodes) {
      if (ep.k !== 2 || !ep.outcomes[10]) continue;
      // Extract canonical tokens
      const parts = ep.key.split(' -> ');
      if (parts.length !== 2) continue;
      if (!pairTypeStats.has(ep.key)) pairTypeStats.set(ep.key, []);
      pairTypeStats.get(ep.key).push(ep.outcomes[10].anchorSignedBps);
    }
  }

  // Build clean reversal pairs on canonical type transitions (e.g., -FLOW_BURST -> +WALL_HOLD vs +WALL_HOLD -> -FLOW_BURST, etc.)
  // Note: When reversing [e1, e2] to [e2, e1], if relative signs were (-X -> +Y), the reversed order ending at X has relative signs (-Y -> +X) or (+Y -> +X) if same-side!
  const visitedPairs = new Set();
  for (const [k1, arr1] of pairTypeStats.entries()) {
    const [tA, tB] = k1.split(' -> ');
    const typeA = tA.slice(1), signA = tA[0];
    const typeB = tB.slice(1), signB = tB[0];
    if (typeA === typeB) continue;
    // Reversed sequence [B, A] canonicalized to new anchor A:
    // If signA === '+', A and B had same side -> reversed is '+typeB -> +typeA'
    // If signA === '-', A and B had opposite side -> reversed is '-typeB -> +typeA'
    const k2 = `${signA}${typeB} -> +${typeA}`;
    if (visitedPairs.has(k1) || visitedPairs.has(k2)) continue;
    const arr2 = pairTypeStats.get(k2);
    if (arr1 && arr2 && arr1.length >= 4 && arr2.length >= 4) {
      visitedPairs.add(k1);
      visitedPairs.add(k2);
      const mean = (a) => a.reduce((s, v) => s + v, 0) / a.length;
      const posRate = (a) => a.filter((v) => v > 0).length / a.length;
      report.test4_orderAsymmetry.push({
        forward: k1,
        nForward: arr1.length,
        meanAnchorBpsForward: +mean(arr1).toFixed(2),
        followThroughPctForward: +(posRate(arr1) * 100).toFixed(1),
        reversed: k2,
        nReversed: arr2.length,
        meanAnchorBpsReversed: +mean(arr2).toFixed(2),
        followThroughPctReversed: +(posRate(arr2) * 100).toFixed(1),
        orderSpreadBps: +Math.abs(mean(arr1) - mean(arr2)).toFixed(2)
      });
    }
  }
  report.test4_orderAsymmetry.sort((a, b) => b.orderSpreadBps - a.orderSpreadBps);

  // Test 4B: Find concrete pairs on same stream with nearly identical linearScore (|diff| < 0.015) but different DNA sequence & divergent outcome
  for (const ctx of streams.values()) {
    const valid = ctx.snapshotsForComparison.filter((s) => s.move10Bps !== null);
    for (let i = 0; i < valid.length; i++) {
      for (let j = i + 1; j < valid.length; j++) {
        if (Math.abs(valid[i].t - valid[j].t) < 12_000) continue;
        if (valid[i].dnaKey === valid[j].dnaKey) continue;
        if (Math.abs(valid[i].linearScore - valid[j].linearScore) <= 0.015) {
          const outcomeGap = Math.abs(valid[i].move10Bps - valid[j].move10Bps);
          if (outcomeGap >= 3.0) {
            report.test4_sameLinearDifferentDna.push({
              stream: ctx.id,
              linearScoreA: +valid[i].linearScore.toFixed(3),
              dnaA: `${valid[i].anchor > 0 ? '[BULL]' : '[BEAR]'} ${valid[i].dnaKey}`,
              move10BpsA: +valid[i].move10Bps.toFixed(2),
              linearScoreB: +valid[j].linearScore.toFixed(3),
              dnaB: `${valid[j].anchor > 0 ? '[BULL]' : '[BEAR]'} ${valid[j].dnaKey}`,
              move10BpsB: +valid[j].move10Bps.toFixed(2),
              outcomeDivergenceBps: +outcomeGap.toFixed(2)
            });
            break;
          }
        }
      }
    }
  }
  report.test4_sameLinearDifferentDna.sort((a, b) => b.outcomeDivergenceBps - a.outcomeDivergenceBps);
  report.test4_sameLinearDifferentDna = report.test4_sameLinearDifferentDna.slice(0, 6);

  // Test 5: Three Clocks Analysis (dtMs vs dTrades vs dVolNorm)
  const allTr = [];
  for (const ctx of streams.values()) {
    for (const tr of ctx.transitions3Clock) {
      if (tr.postAbsMove5s !== undefined) allTr.push(tr);
    }
  }
  if (allTr.length >= 10) {
    const logDt = allTr.map((x) => Math.log1p(x.dtMs));
    const logDN = allTr.map((x) => Math.log1p(x.dTrades));
    const logDV = allTr.map((x) => Math.log1p(Math.max(0, x.dVolNorm)));
    const postMove = allTr.map((x) => x.postAbsMove5s);

    // Count actor regimes where clocks disagree!
    let whaleSweeps = 0, botSwarms = 0, passiveDecays = 0;
    for (const x of allTr) {
      if (x.dtMs < 600 && x.dTrades <= 4 && x.dVolNorm > 1.8) whaleSweeps += 1;
      else if (x.dtMs < 600 && x.dTrades >= 12) botSwarms += 1;
      else if (x.dtMs > 3000 && x.dVolNorm < 0.5) passiveDecays += 1;
    }

    report.test5_threeClocks = {
      nTransitionsMeasured: allTr.length,
      corr_dt_vs_dTrades: +pearsonCorr(logDt, logDN).toFixed(3),
      corr_dt_vs_dVol: +pearsonCorr(logDt, logDV).toFixed(3),
      corr_dTrades_vs_dVol: +pearsonCorr(logDN, logDV).toFixed(3),
      corrWithNext5sAbsMove_dt: +pearsonCorr(logDt, postMove).toFixed(3),
      corrWithNext5sAbsMove_dTrades: +pearsonCorr(logDN, postMove).toFixed(3),
      corrWithNext5sAbsMove_dVol: +pearsonCorr(logDV, postMove).toFixed(3),
      divergentClockSignatures: {
        fastLowCountHighVol_WhaleSweep: whaleSweeps,
        fastHighCount_BotSwarm: botSwarms,
        slowLowVol_PassiveBookDecay: passiveDecays
      }
    };
  }

  // Test 6: Structural Invalidation vs Traditional Price Stop
  const allInv = [];
  for (const ctx of streams.values()) {
    for (const inv of ctx.invalidationLogs) {
      if (inv.priceStopAt) allInv.push({ stream: ctx.id, ...inv });
    }
  }
  let earlyCaught = 0, leadMsList = [], savedBpsList = [];
  for (const inv of allInv) {
    if (inv.structInvalidAt && inv.structInvalidAt < inv.priceStopAt) {
      earlyCaught += 1;
      leadMsList.push(inv.priceStopAt - inv.structInvalidAt);
      const lossAtStruct = inv.dir > 0
        ? ((inv.entryMid - inv.structInvalidMid) / inv.entryMid) * 1e4
        : ((inv.structInvalidMid - inv.entryMid) / inv.entryMid) * 1e4;
      const lossAtPriceStop = inv.dir > 0
        ? ((inv.entryMid - inv.priceStopMid) / inv.entryMid) * 1e4
        : ((inv.priceStopMid - inv.entryMid) / inv.entryMid) * 1e4;
      savedBpsList.push(lossAtPriceStop - lossAtStruct);
    }
  }
  const median = (arr) => {
    if (!arr.length) return 0;
    const s = [...arr].sort((a, b) => a - b);
    return s[Math.floor(s.length / 2)];
  };
  report.test6_structuralInvalidation = {
    totalStoppedTheses: allInv.length,
    caughtEarlyByStructureCount: earlyCaught,
    caughtEarlyPct: allInv.length ? +((earlyCaught / allInv.length) * 100).toFixed(1) : 0,
    medianLeadTimeMs: Math.round(median(leadMsList)),
    medianSavedAdverseBps: +median(savedBpsList).toFixed(2)
  };

  // Test 7: C+ Quarantine Lifecycle Funnel Simulation (01 Discovery -> 02 Incubation -> 03 Walk-Forward -> 04 Shadow Active)
  let stage1_discovered = 0;
  let stage2_incubated = 0;
  let stage3_walkForwardPassed = 0;
  const candidateDetails = [];

  for (const [key, m] of motifMap.entries()) {
    stage1_discovered += 1;
    // Compute concurrency-adjusted effective episodes across streams
    let nEff = 0;
    const byStream = {};
    for (const ep of m.episodes) {
      (byStream[ep.stream] = byStream[ep.stream] || []).push(ep);
    }
    for (const sEps of Object.values(byStream)) {
      nEff += computeEffectiveSampleSize(sEps).nEff;
    }

    if (nEff >= 6) {
      stage2_incubated += 1;
      // Chronological split: First 50% (Train/Incubation) vs Second 50% (Out-of-sample Walk-Forward)
      const sorted = [...m.episodes].sort((a, b) => a.t0 - b.t0);
      const half = Math.floor(sorted.length / 2);
      const firstHalf = sorted.slice(0, half);
      const secondHalf = sorted.slice(half);
      const meanBps = (arr) => arr.reduce((s, x) => s + x.bps10, 0) / Math.max(1, arr.length);
      const m1 = meanBps(firstHalf);
      const m2 = meanBps(secondHalf);
      const bullMean = m.bullOutcomes10.length ? m.bullOutcomes10.reduce((a, b) => a + b, 0) / m.bullOutcomes10.length : null;
      const bearMean = m.bearOutcomes10.length ? m.bearOutcomes10.reduce((a, b) => a + b, 0) / m.bearOutcomes10.length : null;

      // Require same sign in walk-forward AND non-trivial excess magnitude (> 0.35 bps) in both halves
      const passedWF = (Math.sign(m1) === Math.sign(m2)) && Math.abs(m1) >= 0.35 && Math.abs(m2) >= 0.35;
      if (passedWF) stage3_walkForwardPassed += 1;

      candidateDetails.push({
        motif: key,
        k: m.k,
        nRaw: m.episodes.length,
        nEff: +nEff.toFixed(1),
        trainFirstHalfBps: +m1.toFixed(2),
        walkForwardSecondHalfBps: +m2.toFixed(2),
        bullLedgerMeanBps: bullMean !== null ? +bullMean.toFixed(2) : null,
        bearLedgerMeanBps: bearMean !== null ? +bearMean.toFixed(2) : null,
        bullN: m.bullOutcomes10.length,
        bearN: m.bearOutcomes10.length,
        passedWalkForward: passedWF
      });
    }
  }

  candidateDetails.sort((a, b) => (b.passedWalkForward - a.passedWalkForward) || (Math.abs(b.walkForwardSecondHalfBps) - Math.abs(a.walkForwardSecondHalfBps)));

  report.test7_quarantineFunnel = {
    stage01_discoveredUniqueMotifs: stage1_discovered,
    stage02_passedMinEffectiveSample: stage2_incubated,
    stage03_passedChronologicalWalkForward: stage3_walkForwardPassed,
    rejectionRatePct: stage1_discovered ? +(((stage1_discovered - stage3_walkForwardPassed) / stage1_discovered) * 100).toFixed(1) : 0,
    topMotifLedgerExamples: candidateDetails.slice(0, 10)
  };

  const outPath = path.join(__dirname, 'live_dna_report.json');
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log('\n[DNA-LAB] Experiment complete! Saved full report to:', outPath);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
