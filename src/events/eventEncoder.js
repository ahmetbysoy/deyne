'use strict';

const { EventEmitter } = require('events');
const { EVENT_DICTIONARY, BARRIER_CODES } = require('./eventDictionary');

const clamp = (v, lo, hi) => (Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : lo);

function createVenueTracker(venue) {
  return {
    venue,
    lastMid: 0,
    lastSpreadBps: 1.0,
    volRateBpsPerSec: 2.5,
    bandUnit: 0.001,
    midHistory: [], // { ts, mid }
    trades: [],     // { ts, price, notionalUsd, side }
    lastBookEvalAt: 0,
    walls: { bid: null, ask: null },
    obiState: 0,
    microState: 0,
    burstState: 0,
    burstPeakVol2s: 0,
    burstStartedAt: 0,
    voidBaseAsk: null,
    voidBaseBid: null,
    voidStateAsk: false,
    voidStateBid: false,
    lastSweepAt: 0,
    cooldowns: {},
    retestMemory: {} // `${code}:${side}` -> { lastClearedAt, count }
  };
}

class EventEncoder extends EventEmitter {
  constructor(symbol) {
    super();
    this.symbol = String(symbol).toUpperCase();
    this.seq = 0;
    this.venueTrackers = new Map();
    this.events = []; // bounded in-memory ring (full archive written by FAZ 5 storage)
    this.maxInMemoryEvents = 2500;

    // Unified coin activity counters for the 3-Clock (dt, dN, dV)
    this.cumTradeCount = 0;
    this.cumVolumeUsd = 0;
    this.primaryMid = 0;
    this.primarySpreadBps = 1.0;
    this.primaryRegime = 'NORMAL';

    // Audit compression metrics (explicitly separated per Phase 0 audit!)
    this.auditStats = {
      l2BookPacketsProcessed: 0,
      tradePacketsProcessed: 0,
      naive500msTicksEvaluated: 0,
      naive500msActiveTokens: 0,
      naive500msRedundantTokens: 0,
      prevNaiveToken: 'NEUTRAL',
      prev2NaiveToken: 'NEUTRAL',
      cleanTransitionEvents: 0,
      retestEventsPreserved: 0,
      barrierEventsEmitted: 0,
      lastNaiveEvalAt: 0
    };
  }

  _getVenue(venue) {
    if (!this.venueTrackers.has(venue)) {
      this.venueTrackers.set(venue, createVenueTracker(venue));
    }
    return this.venueTrackers.get(venue);
  }

  emitSessionGap(reason = 'session_boundary', ts = Date.now()) {
    // Clear transient hysteresis across all venues so old states never carry across session gaps
    this.venueTrackers.clear();
    return this._recordEvent({
      venue: 'SYSTEM',
      code: '<SESSION_GAP>',
      side: 0,
      isRetest: false,
      retestCount: 0,
      ts,
      venueTs: ts,
      rxTs: ts,
      mid: this.primaryMid,
      spreadBps: this.primarySpreadBps,
      integrity: 'BOUNDARY',
      metrics: { reason }
    });
  }

  onGap(gapInfo) {
    const venue = gapInfo.venue || 'SYSTEM';
    if (venue === 'ALL') this.venueTrackers.clear();
    else this.venueTrackers.delete(venue);

    const now = gapInfo.ts || Date.now();
    // Deduplicate rapid back-to-back <RESET> within 400ms from multi-socket startup
    const last = this.events.length ? this.events[this.events.length - 1] : null;
    if (last && BARRIER_CODES.has(last.code) && Math.abs(now - last.ts) < 400 && last.venue === venue) {
      return last;
    }

    return this._recordEvent({
      venue,
      code: '<RESET>',
      side: 0,
      isRetest: false,
      retestCount: 0,
      ts: now,
      venueTs: now,
      rxTs: Date.now(),
      mid: this.primaryMid,
      spreadBps: this.primarySpreadBps,
      integrity: 'BOUNDARY',
      metrics: { reason: gapInfo.reason || 'stream_gap' }
    });
  }

  onTrade(tr) {
    if (!tr || tr.symbol !== this.symbol || !(tr.price > 0 && tr.notionalUsd > 0)) return;
    this.auditStats.tradePacketsProcessed += 1;
    this.cumTradeCount += 1;
    this.cumVolumeUsd += tr.notionalUsd;

    const vt = this._getVenue(tr.venue);
    const now = tr.ts || Date.now();
    vt.trades.push({ ts: now, price: tr.price, notionalUsd: tr.notionalUsd, side: tr.side });
    if (vt.trades.length > 3500) vt.trades.splice(0, 1200);

    const refMid = vt.lastMid || tr.price;
    this._evaluateFlowTransitions(vt, now, tr.venueTs || now, tr.rxTs || now, refMid);
  }

  onLiquidation(liq) {
    if (!liq || liq.symbol !== this.symbol) return;
    // Liquidations feed directly as high-priority flow bursts if significant
    this.onTrade(liq);
  }

  onBook(bk) {
    if (!bk || bk.symbol !== this.symbol) return;
    const { bids, asks, bestBid, bestAsk, ts, rxTs, venue } = bk;
    if (!(bestBid > 0 && bestAsk > bestBid) || !bids.length || !asks.length) return;

    this.auditStats.l2BookPacketsProcessed += 1;
    const vt = this._getVenue(venue);
    const now = ts || Date.now();
    const mid = (bestBid + bestAsk) / 2;
    const spreadBps = ((bestAsk - bestBid) / mid) * 1e4;

    vt.lastMid = mid;
    vt.lastSpreadBps = spreadBps;
    if (venue === 'BINANCE_FUT' || this.primaryMid === 0) {
      this.primaryMid = mid;
      this.primarySpreadBps = spreadBps;
    }

    // Update rolling mid history & adaptive bandUnit
    const mh = vt.midHistory;
    if (!mh.length || now - mh[mh.length - 1].ts >= 200) {
      mh.push({ ts: now, mid });
      if (mh.length > 240) mh.shift();
      if (mh.length >= 6) {
        let vSum = 0, vN = 0;
        for (let i = Math.max(1, mh.length - 25); i < mh.length; i++) {
          const dt = (mh[i].ts - mh[i - 1].ts) / 1000;
          if (dt > 0.05) {
            vSum += ((Math.abs(mh[i].mid - mh[i - 1].mid) / mh[i - 1].mid) * 1e4) / dt;
            vN += 1;
          }
        }
        if (vN > 0) vt.volRateBpsPerSec = vt.volRateBpsPerSec * 0.88 + (vSum / vN) * 0.12;
      }
      vt.bandUnit = clamp(0.0006 + (vt.volRateBpsPerSec * 1.4) / 1e4, 0.0006, 0.005);
      if (venue === 'BINANCE_FUT' || this.primaryRegime) {
        this.primaryRegime = spreadBps > 3.5 ? 'THIN'
          : (vt.volRateBpsPerSec > 4.5 ? 'VOLATILE' : (vt.volRateBpsPerSec < 0.8 ? 'CALM' : 'NORMAL'));
      }
    }

    // Audit naive 500ms tick baseline on primary venue for transparent Tick-to-Event ratio
    if ((venue === 'BINANCE_FUT' || this.venueTrackers.size === 1) && now - this.auditStats.lastNaiveEvalAt >= 500) {
      this.auditStats.lastNaiveEvalAt = now;
      this._updateNaiveTickAudit(vt, bids, asks, mid, now);
    }

    // Throttle full L2 transition scan to 120ms per venue
    if (vt.lastBookEvalAt && now - vt.lastBookEvalAt < 120) return;
    vt.lastBookEvalAt = now;

    this._evaluateWallLifecycle(vt, bids, asks, mid, now, rxTs || now);
    this._evaluateBookTransitions(vt, bk, bids, asks, mid, spreadBps, now, rxTs || now);
    this._evaluateSweepReject(vt, mid, spreadBps, now, rxTs || now);
    this._evaluateFlowTransitions(vt, now, now, rxTs || now, mid);
  }

  _sumFlow(vt, now, windowMs) {
    let buy = 0, sell = 0, count = 0;
    for (let i = vt.trades.length - 1; i >= 0; i--) {
      const t = vt.trades[i];
      if (now - t.ts > windowMs) break;
      if (t.side > 0) buy += t.notionalUsd;
      else sell += t.notionalUsd;
      count += 1;
    }
    return { buy, sell, total: buy + sell, net: buy - sell, count };
  }

  _tradesNearPrice(vt, now, price, tolFrac, mid, windowMs) {
    let buy = 0, sell = 0;
    for (let i = vt.trades.length - 1; i >= 0; i--) {
      const t = vt.trades[i];
      if (now - t.ts > windowMs) break;
      if (Math.abs(t.price - price) / mid <= tolFrac) {
        if (t.side > 0) buy += t.notionalUsd;
        else sell += t.notionalUsd;
      }
    }
    return { buy, sell };
  }

  _markStateCleared(vt, code, side, now) {
    if (!side) return;
    const k = `${code}:${side}`;
    const prev = vt.retestMemory[k] || { lastClearedAt: 0, count: 0 };
    prev.lastClearedAt = now;
    vt.retestMemory[k] = prev;
  }

  _consumeRetestStatus(vt, code, side, now) {
    const k = `${code}:${side}`;
    const mem = vt.retestMemory[k];
    if (mem && mem.lastClearedAt > 0 && now - mem.lastClearedAt <= 25_000) {
      mem.count += 1;
      mem.lastClearedAt = 0;
      return { isRetest: true, retestCount: mem.count };
    }
    vt.retestMemory[k] = { lastClearedAt: 0, count: 0 };
    return { isRetest: false, retestCount: 0 };
  }

  _tryEmitTransition(vt, code, side, now, venueTs, rxTs, mid, metrics, cooldownMs = 900) {
    const cdKey = `${code}:${side}`;
    if (vt.cooldowns[cdKey] && now - vt.cooldowns[cdKey] < cooldownMs) return null;
    vt.cooldowns[cdKey] = now;

    const { isRetest, retestCount } = this._consumeRetestStatus(vt, code, side, now);
    return this._recordEvent({
      venue: vt.venue,
      code,
      side,
      isRetest,
      retestCount,
      ts: now,
      venueTs,
      rxTs,
      mid,
      spreadBps: vt.lastSpreadBps,
      integrity: 'OK',
      metrics
    });
  }

  _evaluateWallLifecycle(vt, bids, asks, mid, now, rxTs) {
    const unit = vt.bandUnit;
    const bandWide = unit * 4.5;

    const medianQty = (levels) => {
      const arr = levels.slice(0, 40).map((x) => x[1]).sort((a, b) => a - b);
      return arr.length ? arr[Math.floor(arr.length / 2)] : 0;
    };
    const bidMed = medianQty(bids);
    const askMed = medianQty(asks);

    const findDominantWall = (levels, med) => {
      let best = null, bandSum = 0;
      for (const [p, q] of levels) {
        const dist = Math.abs(p - mid) / mid;
        if (dist > bandWide) break;
        const notional = p * q;
        bandSum += notional;
        if (med > 0 && q >= med * 4.0) {
          if (!best || notional > best.notional) {
            best = { price: p, qty: q, notional, dist, multiple: +(q / med).toFixed(2) };
          }
        }
      }
      if (best && bandSum > 0 && best.notional / bandSum >= 0.18) {
        best.bandShare = +(best.notional / bandSum).toFixed(3);
        return best;
      }
      return null;
    };

    for (const [slot, sideSign, levels, med] of [['bid', 1, bids, bidMed], ['ask', -1, asks, askMed]]) {
      const found = findDominantWall(levels, med);
      const cur = vt.walls[slot];

      if (!cur) {
        if (found && found.dist <= unit * 3.0) {
          vt.walls[slot] = {
            price: found.price,
            peakNotional: found.notional,
            notional: found.notional,
            firstSeen: now,
            samples: 1,
            tested: false,
            held: false
          };
          this._tryEmitTransition(vt, 'WALL_SPAWN', sideSign, now, now, rxTs, mid, {
            wallPrice: found.price,
            notionalUsd: Math.round(found.notional),
            medianMultiple: found.multiple,
            bandShare: found.bandShare,
            distBps: +(found.dist * 1e4).toFixed(2)
          }, 1200);
        }
        continue;
      }

      // Check if current tracked wall still exists at ~same price
      let currentQtyAtWall = 0;
      for (const [p, q] of levels) {
        if (Math.abs(p - cur.price) / mid <= unit * 0.35) currentQtyAtWall += q;
      }
      const currentNotional = currentQtyAtWall * cur.price;
      const distNow = Math.abs(mid - cur.price) / mid;

      if (currentNotional >= cur.peakNotional * 0.35 && distNow <= bandWide) {
        cur.samples += 1;
        cur.notional = currentNotional;
        if (currentNotional > cur.peakNotional) cur.peakNotional = currentNotional;

        const nearTrades = this._tradesNearPrice(vt, now, cur.price, unit * 0.65, mid, 5000);
        const opposingHit = sideSign > 0 ? nearTrades.sell : nearTrades.buy;

        // WALL_TEST transition
        if (!cur.tested && distNow <= unit * 0.85 && opposingHit >= cur.peakNotional * 0.05) {
          cur.tested = true;
          this._tryEmitTransition(vt, 'WALL_TEST', sideSign, now, now, rxTs, mid, {
            wallPrice: cur.price,
            opposingHitUsd: Math.round(opposingHit),
            wallNotionalUsd: Math.round(currentNotional)
          }, 1000);
        } else if (cur.tested && distNow >= unit * 1.60) {
          cur.tested = false;
          cur.held = false;
          this._markStateCleared(vt, 'WALL_TEST', sideSign, now);
          this._markStateCleared(vt, 'WALL_HOLD', sideSign, now);
        }

        // WALL_HOLD transition
        if (cur.tested && !cur.held && cur.samples >= 3 && opposingHit >= cur.peakNotional * 0.20 && currentNotional >= cur.peakNotional * 0.50) {
          const breached = sideSign > 0 ? (mid < cur.price) : (mid > cur.price);
          if (!breached) {
            cur.held = true;
            this._tryEmitTransition(vt, 'WALL_HOLD', sideSign, now, now, rxTs, mid, {
              wallPrice: cur.price,
              absorbedUsd: Math.round(opposingHit),
              survivingRatio: +(currentNotional / cur.peakNotional).toFixed(2)
            }, 1500);
          }
        }
      } else {
        // Wall vanished or shrank < 35% of peak -> classify observationally: WALL_BREAK vs WALL_PULL
        const breached = sideSign > 0 ? (mid < cur.price) : (mid > cur.price);
        const nearTrades = this._tradesNearPrice(vt, now, cur.price, unit * 0.70, mid, 5000);
        const opposingHit = sideSign > 0 ? nearTrades.sell : nearTrades.buy;

        if (breached && opposingHit >= cur.peakNotional * 0.15) {
          this._tryEmitTransition(vt, 'WALL_BREAK', sideSign, now, now, rxTs, mid, {
            wallPrice: cur.price,
            peakNotionalUsd: Math.round(cur.peakNotional),
            tradedThroughUsd: Math.round(opposingHit)
          }, 1000);
        } else if (cur.samples >= 3 && distNow <= unit * 2.5 && opposingHit < cur.peakNotional * 0.15) {
          // Purely observational liquidity reduction without matching trades
          this._tryEmitTransition(vt, 'WALL_PULL', sideSign, now, now, rxTs, mid, {
            wallPrice: cur.price,
            peakNotionalUsd: Math.round(cur.peakNotional),
            remainingNotionalUsd: Math.round(currentNotional),
            tradedNearUsd: Math.round(opposingHit)
          }, 1000);
        }
        this._markStateCleared(vt, 'WALL_SPAWN', sideSign, now);
        vt.walls[slot] = null;
      }
    }
  }

  _evaluateFlowTransitions(vt, now, venueTs, rxTs, mid) {
    if (!mid || vt.trades.length < 5) return;
    const oldestInBuf = vt.trades[0].ts;
    const observedSpanSec = Math.min(15, (now - oldestInBuf) / 1000);
    // Require at least 4s of observed trade history on this venue so baseline isn't underestimated on startup!
    if (observedSpanSec < 4.0) return;

    const f2 = this._sumFlow(vt, now, 2000);
    const f15 = this._sumFlow(vt, now, 15000);
    const expected2s = Math.max(500, (f15.total / observedSpanSec) * 2);

    if (f2.count >= 3 && f2.total >= expected2s * 2.2) {
      const buyShare = f2.buy / (f2.total + 1e-9);
      if (buyShare >= 0.72 && vt.burstState !== 1) {
        if (vt.burstState === -1) this._markStateCleared(vt, 'FLOW_BURST', -1, now);
        vt.burstState = 1;
        vt.burstPeakVol2s = f2.total;
        vt.burstStartedAt = now;
        this._tryEmitTransition(vt, 'FLOW_BURST', 1, now, venueTs, rxTs, mid, {
          vol2sUsd: Math.round(f2.total),
          volMultiple: +(f2.total / expected2s).toFixed(2),
          buyShare: +buyShare.toFixed(2),
          tradeCount2s: f2.count
        }, 900);
      } else if (buyShare <= 0.28 && vt.burstState !== -1) {
        if (vt.burstState === 1) this._markStateCleared(vt, 'FLOW_BURST', 1, now);
        vt.burstState = -1;
        vt.burstPeakVol2s = f2.total;
        vt.burstStartedAt = now;
        this._tryEmitTransition(vt, 'FLOW_BURST', -1, now, venueTs, rxTs, mid, {
          vol2sUsd: Math.round(f2.total),
          volMultiple: +(f2.total / expected2s).toFixed(2),
          sellShare: +(1 - buyShare).toFixed(2),
          tradeCount2s: f2.count
        }, 900);
      } else if (vt.burstState !== 0) {
        vt.burstPeakVol2s = Math.max(vt.burstPeakVol2s, f2.total);
      }
    } else if (vt.burstState !== 0) {
      const prevBurstSide = vt.burstState;
      if (now - vt.burstStartedAt >= 700 && f2.total <= vt.burstPeakVol2s * 0.32) {
        this._tryEmitTransition(vt, 'FLOW_EXHAUST', prevBurstSide, now, venueTs, rxTs, mid, {
          peakBurstUsd: Math.round(vt.burstPeakVol2s),
          currentVol2sUsd: Math.round(f2.total),
          durationMs: now - vt.burstStartedAt
        }, 900);
        this._markStateCleared(vt, 'FLOW_BURST', prevBurstSide, now);
        vt.burstState = 0;
      } else if (f2.total < expected2s * 0.85) {
        this._markStateCleared(vt, 'FLOW_BURST', prevBurstSide, now);
        vt.burstState = 0;
      }
    }
  }

  _evaluateBookTransitions(vt, bk, bids, asks, mid, spreadBps, now, rxTs) {
    const unit = vt.bandUnit;
    const lo = mid * (1 - unit), hi = mid * (1 + unit), dec = mid * unit;
    let bq = 0, aq = 0;
    for (const [p, q] of bids) { if (p < lo) break; bq += q * Math.exp(-Math.abs(mid - p) / dec); }
    for (const [p, q] of asks) { if (p > hi) break; aq += q * Math.exp(-Math.abs(p - mid) / dec); }
    const obi = (bq - aq) / (bq + aq + 1e-9);

    // 1) BOOK_TILT with strict hysteresis [enter: 0.36, exit: 0.14]
    if (obi >= 0.36 && vt.obiState !== 1) {
      if (vt.obiState === -1) this._markStateCleared(vt, 'BOOK_TILT', -1, now);
      vt.obiState = 1;
      this._tryEmitTransition(vt, 'BOOK_TILT', 1, now, now, rxTs, mid, { obiNear: +obi.toFixed(3) }, 800);
    } else if (obi <= -0.36 && vt.obiState !== -1) {
      if (vt.obiState === 1) this._markStateCleared(vt, 'BOOK_TILT', 1, now);
      vt.obiState = -1;
      this._tryEmitTransition(vt, 'BOOK_TILT', -1, now, now, rxTs, mid, { obiNear: +obi.toFixed(3) }, 800);
    } else if (Math.abs(obi) <= 0.14 && vt.obiState !== 0) {
      this._markStateCleared(vt, 'BOOK_TILT', vt.obiState, now);
      vt.obiState = 0;
    }

    // 2) MICRO_FLIP with strict hysteresis [enter: 0.58, exit: 0.18]
    const b0 = bk.bestBidQty || bids[0][1];
    const a0 = bk.bestAskQty || asks[0][1];
    const spread = bk.bestAsk - bk.bestBid;
    const microPrice = (bk.bestAsk * b0 + bk.bestBid * a0) / (b0 + a0 + 1e-9);
    const microSkew = (microPrice - mid) / (spread / 2 + 1e-9);

    if (microSkew >= 0.58 && vt.microState !== 1) {
      if (vt.microState === -1) this._markStateCleared(vt, 'MICRO_FLIP', -1, now);
      vt.microState = 1;
      this._tryEmitTransition(vt, 'MICRO_FLIP', 1, now, now, rxTs, mid, { microSkew: +microSkew.toFixed(3) }, 800);
    } else if (microSkew <= -0.58 && vt.microState !== -1) {
      if (vt.microState === 1) this._markStateCleared(vt, 'MICRO_FLIP', 1, now);
      vt.microState = -1;
      this._tryEmitTransition(vt, 'MICRO_FLIP', -1, now, now, rxTs, mid, { microSkew: +microSkew.toFixed(3) }, 800);
    } else if (Math.abs(microSkew) <= 0.18 && vt.microState !== 0) {
      this._markStateCleared(vt, 'MICRO_FLIP', vt.microState, now);
      vt.microState = 0;
    }

    // 3) VOID_OPEN (Liquidity vacuum expansion)
    const medQty = (arr) => {
      const q = arr.slice(0, 40).map((x) => x[1]).sort((a, b) => a - b);
      return q.length ? q[Math.floor(q.length / 2)] : 0;
    };
    const barrierDistBps = (arr, med) => {
      for (const [p, q] of arr) {
        const dBps = (Math.abs(p - mid) / mid) * 1e4;
        if (dBps > unit * 4.5 * 1e4) break;
        if (med > 0 && q >= med * 3.2) return dBps;
      }
      return unit * 4.5 * 1e4;
    };
    const distAsk = barrierDistBps(asks, medQty(asks));
    const distBid = barrierDistBps(bids, medQty(bids));
    vt.voidBaseAsk = vt.voidBaseAsk === null ? distAsk : vt.voidBaseAsk * 0.96 + distAsk * 0.04;
    vt.voidBaseBid = vt.voidBaseBid === null ? distBid : vt.voidBaseBid * 0.96 + distBid * 0.04;

    if (!vt.voidStateAsk && distAsk >= vt.voidBaseAsk * 1.85 && distAsk >= Math.max(1.5, spreadBps * 2.5)) {
      vt.voidStateAsk = true;
      this._tryEmitTransition(vt, 'VOID_OPEN', 1, now, now, rxTs, mid, { askBarrierBps: +distAsk.toFixed(2), baselineBps: +vt.voidBaseAsk.toFixed(2) }, 2000);
    } else if (vt.voidStateAsk && distAsk <= vt.voidBaseAsk * 1.20) {
      vt.voidStateAsk = false;
      this._markStateCleared(vt, 'VOID_OPEN', 1, now);
    }

    if (!vt.voidStateBid && distBid >= vt.voidBaseBid * 1.85 && distBid >= Math.max(1.5, spreadBps * 2.5)) {
      vt.voidStateBid = true;
      this._tryEmitTransition(vt, 'VOID_OPEN', -1, now, now, rxTs, mid, { bidBarrierBps: +distBid.toFixed(2), baselineBps: +vt.voidBaseBid.toFixed(2) }, 2000);
    } else if (vt.voidStateBid && distBid <= vt.voidBaseBid * 1.20) {
      vt.voidStateBid = false;
      this._markStateCleared(vt, 'VOID_OPEN', -1, now);
    }
  }

  _evaluateSweepReject(vt, mid, spreadBps, now, rxTs) {
    if (now - vt.lastSweepAt < 3000 || vt.midHistory.length < 5) return;
    let anchorMid = null;
    let minMid = mid, maxMid = mid;
    for (let i = vt.midHistory.length - 1; i >= 0; i--) {
      const pt = vt.midHistory[i];
      if (now - pt.ts > 3000) break;
      anchorMid = pt.mid;
      if (pt.mid < minMid) minMid = pt.mid;
      if (pt.mid > maxMid) maxMid = pt.mid;
    }
    if (!anchorMid) return;
    const minExcursionBps = Math.max(0.8, spreadBps * 1.5);
    const downBps = ((anchorMid - minMid) / anchorMid) * 1e4;
    const upBps = ((maxMid - anchorMid) / anchorMid) * 1e4;

    if (downBps >= minExcursionBps && (mid - minMid) >= (anchorMid - minMid) * 0.60) {
      vt.lastSweepAt = now;
      this._tryEmitTransition(vt, 'SWEEP_REJECT', 1, now, now, rxTs, mid, {
        excursionBps: +downBps.toFixed(2),
        retracedPct: +(((mid - minMid) / (anchorMid - minMid)) * 100).toFixed(1)
      }, 3000);
    } else if (upBps >= minExcursionBps && (maxMid - mid) >= (maxMid - anchorMid) * 0.60) {
      vt.lastSweepAt = now;
      this._tryEmitTransition(vt, 'SWEEP_REJECT', -1, now, now, rxTs, mid, {
        excursionBps: +upBps.toFixed(2),
        retracedPct: +(((maxMid - mid) / (maxMid - anchorMid)) * 100).toFixed(1)
      }, 3000);
    }
  }

  _updateNaiveTickAudit(vt, bids, asks, mid, now) {
    this.auditStats.naive500msTicksEvaluated += 1;
    let bq = 0, aq = 0;
    for (let i = 0; i < Math.min(10, bids.length); i++) bq += bids[i][1];
    for (let i = 0; i < Math.min(10, asks.length); i++) aq += asks[i][1];
    const obi = (bq - aq) / (bq + aq + 1e-9);
    const f5 = this._sumFlow(vt, now, 5000);
    const dS = f5.net / (f5.total + 1e-9);

    let tok = 'NEUTRAL';
    if (obi > 0.25) tok = '+OBI';
    else if (obi < -0.25) tok = '-OBI';
    else if (dS > 0.35) tok = '+FLOW';
    else if (dS < -0.35) tok = '-FLOW';

    if (tok !== 'NEUTRAL') {
      this.auditStats.naive500msActiveTokens += 1;
      if (tok === this.auditStats.prevNaiveToken || (tok === this.auditStats.prev2NaiveToken && tok !== this.auditStats.prevNaiveToken)) {
        this.auditStats.naive500msRedundantTokens += 1;
      }
    }
    this.auditStats.prev2NaiveToken = this.auditStats.prevNaiveToken;
    this.auditStats.prevNaiveToken = tok;
  }

  _recordEvent(fields) {
    const ev = {
      id: `ev_${++this.seq}`,
      symbol: this.symbol,
      venue: fields.venue,
      code: fields.code,
      labelTr: EVENT_DICTIONARY[fields.code] ? EVENT_DICTIONARY[fields.code].labelTr : fields.code,
      side: fields.side,
      isRetest: !!fields.isRetest,
      retestCount: fields.retestCount || 0,
      ts: Math.round(fields.ts),
      venueTs: Math.round(fields.venueTs || fields.ts),
      rxTs: Math.round(fields.rxTs || Date.now()),
      mid: fields.mid ? +fields.mid.toFixed(6) : 0,
      spreadBps: fields.spreadBps ? +fields.spreadBps.toFixed(3) : 0,
      regime: this.primaryRegime,
      cumTradeCount: this.cumTradeCount,
      cumVolumeUsd: +this.cumVolumeUsd.toFixed(2),
      integrity: fields.integrity || 'OK',
      metrics: fields.metrics || {}
    };

    if (BARRIER_CODES.has(ev.code)) {
      this.auditStats.barrierEventsEmitted += 1;
    } else {
      this.auditStats.cleanTransitionEvents += 1;
      if (ev.isRetest) this.auditStats.retestEventsPreserved += 1;
    }

    this.events.push(ev);
    if (this.events.length > this.maxInMemoryEvents) {
      this.events.splice(0, this.events.length - this.maxInMemoryEvents);
    }

    this.emit('event', ev);
    return ev;
  }

  getCompressionSummary() {
    const clean = Math.max(1, this.auditStats.cleanTransitionEvents);
    return {
      l2BookPacketsProcessed: this.auditStats.l2BookPacketsProcessed,
      tradePacketsProcessed: this.auditStats.tradePacketsProcessed,
      naive500msActiveTokens: this.auditStats.naive500msActiveTokens,
      naive500msRedundantPct: this.auditStats.naive500msActiveTokens
        ? +((this.auditStats.naive500msRedundantTokens / this.auditStats.naive500msActiveTokens) * 100).toFixed(1)
        : 0,
      cleanTransitionEvents: this.auditStats.cleanTransitionEvents,
      retestEventsPreserved: this.auditStats.retestEventsPreserved,
      barrierEventsEmitted: this.auditStats.barrierEventsEmitted,
      l2PacketsPerEvent: this.auditStats.cleanTransitionEvents
        ? +(this.auditStats.l2BookPacketsProcessed / clean).toFixed(2)
        : null,
      activeTicksPerEvent: this.auditStats.cleanTransitionEvents
        ? +(this.auditStats.naive500msActiveTokens / clean).toFixed(2)
        : null
    };
  }
}

module.exports = { EventEncoder };
