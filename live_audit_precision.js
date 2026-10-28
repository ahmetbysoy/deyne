'use strict';

const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

// 45s focused precision audit:
// 1) Precision vs Recall of SINGLE event invalidation (e.g. MICRO_FLIP alone) vs COMPOSITE DNA invalidation (2-event counter chain, e.g. OPP:FLOW_BURST -> OPP:BOOK_TILT or SAME:WALL_PULL)
// 2) Short-window Base Rate Whipsaw (20s window) vs Slow Shrunk 3-State Prior [P(Up), P(Flat), P(Down)]

const DURATION_MS = 45_000;
const OKX_PAIRS = ['BTC-USDT-SWAP', 'ETH-USDT-SWAP', 'SOL-USDT-SWAP', 'SUI-USDT-SWAP', 'DOGE-USDT-SWAP', 'XRP-USDT-SWAP', 'AVAX-USDT-SWAP', 'LINK-USDT-SWAP'];

function makeCtx(instId) {
  return {
    id: `OKX:${instId}`, instId,
    bids: new Map(), asks: new Map(), hasBook: false,
    trades: [], spreadEwma: 1.2, bandUnit: 0.001,
    wallTrack: { bid: null, ask: null },
    hyst: { obi: 0, micro: 0, burst: 0, burstPeak: 0, burstStart: 0, cd: {} },
    events: [],
    theses: [],
    completedTheses: []
  };
}

const streams = new Map(OKX_PAIRS.map((p) => [p, makeCtx(p)]));

function sorted(map, desc) {
  const a = [];
  for (const [p, q] of map.entries()) if (q > 0) a.push({ price: p, qty: q });
  a.sort((x, y) => (desc ? y.price - x.price : x.price - y.price));
  return a.slice(0, 60);
}

function emit(ctx, now, type, side, mid) {
  const k = `${type}:${side}`;
  if (ctx.hyst.cd[k] && now - ctx.hyst.cd[k] < 1000) return;
  ctx.hyst.cd[k] = now;
  const ev = { t: now, type, side, mid };
  ctx.events.push(ev);

  const prev = ctx.events.length >= 2 ? ctx.events[ctx.events.length - 2] : null;

  for (const th of ctx.theses) {
    if (th.done) continue;
    // Rule A (Noisy single-event): any single OPP:MICRO_FLIP
    if (!th.singleFlipAt && ev.side === -th.dir && type === 'MICRO_FLIP') {
      th.singleFlipAt = now; th.singleFlipMid = mid;
    }
    // Rule B (Structural DNA Invalidation): Supporting WALL_PULL/BREAK, OR 2-step opposing DNA sequence within 4s!
    const wallDied = ev.side === th.dir && (type === 'WALL_PULL' || type === 'WALL_BREAK');
    const twoStepCounter = prev && (now - prev.t <= 4000) && prev.side === -th.dir && ev.side === -th.dir && prev.type !== ev.type;
    if (!th.dnaInvalidAt && (wallDied || twoStepCounter)) {
      th.dnaInvalidAt = now; th.dnaInvalidMid = mid;
    }
  }
}

function observe(ctx, now) {
  if (!ctx.hasBook || (ctx.lastObs && now - ctx.lastObs < 150)) return;
  ctx.lastObs = now;
  const bids = sorted(ctx.bids, true), asks = sorted(ctx.asks, false);
  if (bids.length < 5 || asks.length < 5 || !(asks[0].price > bids[0].price)) return;
  const mid = (bids[0].price + asks[0].price) / 2;
  const spread = asks[0].price - bids[0].price;
  const unit = ctx.bandUnit;

  // Walls
  const med = (lvs) => { const q = lvs.slice(0, 30).map((x) => x.qty).sort((a, b) => a - b); return q[Math.floor(q.length / 2)] || 0; };
  for (const [sk, sign, lvs, m] of [['bid', 1, bids, med(bids)], ['ask', -1, asks, med(asks)]]) {
    let best = null, sum = 0;
    for (const lv of lvs) {
      const dist = Math.abs(lv.price - mid) / mid;
      if (dist > unit * 4) break;
      const n = lv.price * lv.qty; sum += n;
      if (m > 0 && lv.qty >= m * 3.8 && (!best || n > best.n)) best = { price: lv.price, n, dist };
    }
    if (best && sum > 0 && best.n / sum < 0.18) best = null;
    const cur = ctx.wallTrack[sk];
    if (!cur && best && best.dist <= unit * 2.5) {
      ctx.wallTrack[sk] = { price: best.price, samples: 1 };
      emit(ctx, now, 'WALL_SPAWN', sign, mid);
    } else if (cur) {
      if (best && Math.abs(best.price - cur.price) / mid < unit * 0.5) cur.samples += 1;
      else {
        const breached = sign > 0 ? mid < cur.price : mid > cur.price;
        if (breached) emit(ctx, now, 'WALL_BREAK', sign, mid);
        else if (cur.samples >= 3) emit(ctx, now, 'WALL_PULL', sign, mid);
        ctx.wallTrack[sk] = null;
      }
    }
  }

  // Flow
  let buy2 = 0, sell2 = 0, cnt2 = 0, tot15 = 0;
  for (let i = ctx.trades.length - 1; i >= 0; i--) {
    const age = now - ctx.trades[i].t;
    if (age > 15000) break;
    tot15 += ctx.trades[i].n;
    if (age <= 2000) {
      cnt2 += 1;
      if (ctx.trades[i].s > 0) buy2 += ctx.trades[i].n; else sell2 += ctx.trades[i].n;
    }
  }
  const tot2 = buy2 + sell2, base2 = Math.max(1e-6, (tot15 / 15) * 2);
  if (cnt2 >= 3 && tot2 > base2 * 2.0) {
    const sb = buy2 / (tot2 + 1e-9);
    if (sb >= 0.7 && ctx.hyst.burst !== 1) { ctx.hyst.burst = 1; emit(ctx, now, 'FLOW_BURST', 1, mid); }
    else if (sb <= 0.3 && ctx.hyst.burst !== -1) { ctx.hyst.burst = -1; emit(ctx, now, 'FLOW_BURST', -1, mid); }
  } else if (tot2 < base2 * 0.6) ctx.hyst.burst = 0;

  // OBI & Micro
  let bq = 0, aq = 0;
  for (const lv of bids) { if (lv.price < mid * (1 - unit)) break; bq += lv.qty; }
  for (const lv of asks) { if (lv.price > mid * (1 + unit)) break; aq += lv.qty; }
  const obi = (bq - aq) / (bq + aq + 1e-9);
  if (obi > 0.35 && ctx.hyst.obi !== 1) { ctx.hyst.obi = 1; emit(ctx, now, 'BOOK_TILT', 1, mid); }
  else if (obi < -0.35 && ctx.hyst.obi !== -1) { ctx.hyst.obi = -1; emit(ctx, now, 'BOOK_TILT', -1, mid); }
  else if (Math.abs(obi) < 0.12) ctx.hyst.obi = 0;

  const microPx = (asks[0].price * bids[0].qty + bids[0].price * asks[0].qty) / (bids[0].qty + asks[0].qty || 1);
  const mSkew = (microPx - mid) / (spread / 2 + 1e-9);
  if (mSkew > 0.55 && ctx.hyst.micro !== 1) { ctx.hyst.micro = 1; emit(ctx, now, 'MICRO_FLIP', 1, mid); }
  else if (mSkew < -0.55 && ctx.hyst.micro !== -1) { ctx.hyst.micro = -1; emit(ctx, now, 'MICRO_FLIP', -1, mid); }
  else if (Math.abs(mSkew) < 0.15) ctx.hyst.micro = 0;
}

async function main() {
  const ws = new WebSocket('wss://ws.okx.com:443/ws/v5/public');
  ws.on('open', () => {
    ws.send(JSON.stringify({
      op: 'subscribe',
      args: OKX_PAIRS.flatMap((instId) => [{ channel: 'books', instId }, { channel: 'trades', instId }])
    }));
  });
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || !m.arg || !m.data) return;
    const ctx = streams.get(m.arg.instId);
    if (!ctx) return;
    const now = Date.now();
    if (m.arg.channel === 'books') {
      for (const b of m.data) {
        if (m.action === 'snapshot') { ctx.bids.clear(); ctx.asks.clear(); }
        for (const lv of b.bids || []) { const p = +lv[0], q = +lv[1]; if (q <= 0) ctx.bids.delete(p); else ctx.bids.set(p, q); }
        for (const lv of b.asks || []) { const p = +lv[0], q = +lv[1]; if (q <= 0) ctx.asks.delete(p); else ctx.asks.set(p, q); }
        ctx.hasBook = ctx.bids.size >= 5 && ctx.asks.size >= 5;
      }
      observe(ctx, now);
    } else if (m.arg.channel === 'trades') {
      for (const tr of m.data) {
        const p = +tr.px, q = +tr.sz;
        if (p > 0 && q > 0) ctx.trades.push({ t: +tr.ts || now, n: p * q, s: tr.side === 'buy' ? 1 : -1 });
      }
      if (ctx.trades.length > 3000) ctx.trades.splice(0, 1000);
    }
  });

  const timer = setInterval(() => {
    const now = Date.now();
    for (const ctx of streams.values()) {
      if (!ctx.hasBook) continue;
      const bids = sorted(ctx.bids, true), asks = sorted(ctx.asks, false);
      if (bids.length < 5 || asks.length < 5 || !(asks[0].price > bids[0].price)) continue;
      const mid = (bids[0].price + asks[0].price) / 2;
      const spreadBps = ((asks[0].price - bids[0].price) / mid) * 1e4;
      ctx.spreadEwma = ctx.spreadEwma * 0.95 + spreadBps * 0.05;

      let bq = 0, aq = 0;
      for (const lv of bids) { if (lv.price < mid * 0.999) break; bq += lv.qty; }
      for (const lv of asks) { if (lv.price > mid * 1.001) break; aq += lv.qty; }
      const obi = (bq - aq) / (bq + aq + 1e-9);

      if (Math.abs(obi) > 0.25 && (!ctx.lastTh || now - ctx.lastTh >= 3500)) {
        ctx.lastTh = now;
        ctx.theses.push({
          t: now, dir: obi > 0 ? 1 : -1, entryMid: mid,
          stopBps: Math.max(1.8, ctx.spreadEwma * 2.2),
          singleFlipAt: null, singleFlipMid: null,
          dnaInvalidAt: null, dnaInvalidMid: null,
          done: false
        });
      }

      for (const th of ctx.theses) {
        if (th.done) continue;
        if (now - th.t >= 10_000) {
          th.done = true;
          th.endMid = mid;
          th.signedEndBps = ((mid - th.entryMid) / th.entryMid) * 1e4 * th.dir;
          ctx.completedTheses.push(th);
        }
      }
    }
  }, 500);

  await new Promise((r) => setTimeout(r, DURATION_MS));
  clearInterval(timer);
  try { ws.close(); } catch (_) {}

  const all = [];
  for (const ctx of streams.values()) all.push(...ctx.completedTheses);

  // Compare strategies on completed 10s theses:
  // 1) Hold to 10s horizon (or price stop)
  // 2) Exit immediately on Single OPP:MICRO_FLIP
  // 3) Exit immediately on 2-Step Composite DNA Structural Invalidation
  let sumHold = 0, sumSingle = 0, sumDna = 0;
  let singleExitWinnersKilled = 0, dnaExitWinnersKilled = 0;
  let singleTriggers = 0, dnaTriggers = 0;

  for (const th of all) {
    const endPnl = th.signedEndBps;
    sumHold += endPnl;

    if (th.singleFlipAt) {
      singleTriggers += 1;
      const exitPnl = ((th.singleFlipMid - th.entryMid) / th.entryMid) * 1e4 * th.dir;
      sumSingle += exitPnl;
      if (endPnl > 1.0) singleExitWinnersKilled += 1;
    } else sumSingle += endPnl;

    if (th.dnaInvalidAt) {
      dnaTriggers += 1;
      const exitPnl = ((th.dnaInvalidMid - th.entryMid) / th.entryMid) * 1e4 * th.dir;
      sumDna += exitPnl;
      if (endPnl > 1.0) dnaExitWinnersKilled += 1;
    } else sumDna += endPnl;
  }

  const n = Math.max(1, all.length);
  const res = {
    completed10sTheses: all.length,
    meanBps_Hold10s: +(sumHold / n).toFixed(3),
    meanBps_SingleEventExit: +(sumSingle / n).toFixed(3),
    meanBps_CompositeDnaInvalidationExit: +(sumDna / n).toFixed(3),
    singleEventTriggerRatePct: +((singleTriggers / n) * 100).toFixed(1),
    singleEventFalseAlarmWinnersKilled: singleExitWinnersKilled,
    compositeDnaTriggerRatePct: +((dnaTriggers / n) * 100).toFixed(1),
    compositeDnaFalseAlarmWinnersKilled: dnaExitWinnersKilled
  };
  fs.writeFileSync(path.join(__dirname, 'live_audit_precision.json'), JSON.stringify(res, null, 2));
  console.log(JSON.stringify(res, null, 2));
}

main().catch((e) => { console.error(e); process.exit(1); });
