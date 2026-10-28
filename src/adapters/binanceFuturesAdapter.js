'use strict';

const { EventEmitter } = require('events');
const { WsClient } = require('../lib/wsClient');
const { ClockTracker } = require('../lib/clockTracker');

class BinanceFuturesAdapter extends EventEmitter {
  constructor(symbol) {
    super();
    this.venue = 'BINANCE_FUT';
    this.symbol = String(symbol).toUpperCase();
    this.active = false;
    this.bookWs = null;
    this.marketWs = null;
    this.reconnectTimerBook = null;
    this.reconnectTimerMarket = null;
    this.retryBook = 0;
    this.retryMarket = 0;

    this.clock = new ClockTracker(this.venue);
    this.bids = new Map();
    this.asks = new Map();
    this.bbo = { bid: 0, bidQty: 0, ask: 0, askQty: 0, u: 0, ts: 0 };
    this.mark = { markPrice: 0, indexPrice: 0, fundingRate: 0, nextFundingTime: 0, ts: 0 };
    this.synced = false;
    this.lastDepth20U = 0;
    this.lastDiffU = 0;
    this.lastTradeStreamAt = 0;

    this.counters = {
      bookTicker: 0,
      depth20: 0,
      depthDiff: 0,
      trade: 0,
      aggTradeMarketStream: 0,
      markPrice: 0,
      forceOrder: 0,
      ignoredNonMarketTrades: 0,
      malformedPackets: 0,
      sequenceGaps: 0,
      reconnects: 0
    };
    this.status = 'IDLE'; // IDLE | CONNECTING | LIVE | DEGRADED | DISCONNECTED
  }

  start() {
    if (this.active) return;
    this.active = true;
    this.status = 'CONNECTING';
    this._probeClockSkew();
    this._connectBookStream();
    this._connectMarketStream();
  }

  async _probeClockSkew() {
    try {
      const t0 = Date.now();
      const res = await fetch('https://data-api.binance.vision/api/v3/time');
      const t1 = Date.now();
      if (res.ok) {
        const data = await res.json();
        this.clock.recordRestSkew(t0, t1, Number(data.serverTime));
      }
    } catch (_) {}
  }

  _connectBookStream() {
    if (!this.active) return;
    const s = this.symbol.toLowerCase();
    const streams = `${s}@bookTicker/${s}@depth20@100ms/${s}@depth@100ms/${s}@trade`;
    const url = `wss://fstream.binance.com/stream?streams=${streams}`;
    const ws = new WsClient(url, { handshakeTimeoutMs: 9000, pingIntervalMs: 15000 });
    this.bookWs = ws;

    ws.on('open', () => {
      if (this.bookWs !== ws || !this.active) return;
      this.retryBook = 0;
      this._updateStatus();
      this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'ws_open_sync', ts: Date.now() });
    });

    ws.on('pong', (rtt) => this.clock.recordWsRtt(rtt));

    ws.on('message', (raw, rxTs) => {
      if (this.bookWs !== ws || !this.active) return;
      this._handleBookChannelMessage(raw, rxTs);
    });

    ws.on('error', () => {});

    ws.on('close', () => {
      if (this.bookWs !== ws) return;
      this.bookWs = null;
      this.synced = false;
      this._updateStatus();
      if (!this.active) return;
      this.counters.reconnects += 1;
      this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'book_ws_closed', ts: Date.now() });
      const wait = Math.min(15000, 1000 * Math.pow(1.8, Math.min(this.retryBook++, 6)));
      this.reconnectTimerBook = setTimeout(() => this._connectBookStream(), wait);
    });
  }

  _connectMarketStream() {
    if (!this.active) return;
    const s = this.symbol.toLowerCase();
    const streams = `${s}@markPrice@1s/${s}@forceOrder/${s}@aggTrade`;
    const url = `wss://fstream.binance.com/market/stream?streams=${streams}`;
    const ws = new WsClient(url, { handshakeTimeoutMs: 9000, pingIntervalMs: 18000 });
    this.marketWs = ws;

    ws.on('open', () => {
      if (this.marketWs !== ws || !this.active) return;
      this.retryMarket = 0;
      this._updateStatus();
    });

    ws.on('pong', (rtt) => this.clock.recordWsRtt(rtt));

    ws.on('message', (raw, rxTs) => {
      if (this.marketWs !== ws || !this.active) return;
      this._handleMarketChannelMessage(raw, rxTs);
    });

    ws.on('error', () => {});

    ws.on('close', () => {
      if (this.marketWs !== ws) return;
      this.marketWs = null;
      this._updateStatus();
      if (!this.active) return;
      const wait = Math.min(20000, 1500 * Math.pow(1.8, Math.min(this.retryMarket++, 6)));
      this.reconnectTimerMarket = setTimeout(() => this._connectMarketStream(), wait);
    });
  }

  _updateStatus() {
    if (!this.active) {
      this.status = 'IDLE';
      return;
    }
    const bookLive = !!(this.bookWs && this.bookWs.connected);
    const mktLive = !!(this.marketWs && this.marketWs.connected);
    if (bookLive && mktLive) this.status = 'LIVE';
    else if (bookLive) this.status = 'LIVE'; // primary L2 + trade live
    else if (mktLive) this.status = 'DEGRADED';
    else this.status = 'DISCONNECTED';
    this.emit('status', this.getHealth());
  }

  _applyLevels(map, arr) {
    if (!Array.isArray(arr)) return;
    for (const lv of arr) {
      const p = Number(lv[0]);
      const q = Number(lv[1]);
      if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(q)) continue;
      if (q <= 0) map.delete(p);
      else map.set(p, q);
    }
  }

  _trimBook() {
    if (this.bids.size > 400) {
      const sorted = Array.from(this.bids.keys()).sort((a, b) => b - a);
      for (let i = 300; i < sorted.length; i++) this.bids.delete(sorted[i]);
    }
    if (this.asks.size > 400) {
      const sorted = Array.from(this.asks.keys()).sort((a, b) => a - b);
      for (let i = 300; i < sorted.length; i++) this.asks.delete(sorted[i]);
    }
  }

  _handleBookChannelMessage(raw, rxTs) {
    let pkt;
    try { pkt = JSON.parse(raw); } catch (_) { this.counters.malformedPackets += 1; return; }
    if (!pkt || !pkt.stream || !pkt.data) return;
    const stream = pkt.stream;
    const d = pkt.data;
    const evTs = Number(d.T) || Number(d.E) || rxTs;
    this.clock.observePacket(evTs, rxTs);
    const normTs = this.clock.normalizeTimestamp(evTs, rxTs);

    if (stream.endsWith('@bookTicker')) {
      const bid = Number(d.b), bidQty = Number(d.B), ask = Number(d.a), askQty = Number(d.A);
      if (!(bid > 0 && ask > bid)) return;
      this.counters.bookTicker += 1;
      this.bbo = { bid, bidQty, ask, askQty, u: Number(d.u) || 0, ts: normTs, rxTs };
      this.emit('bbo', { venue: this.venue, symbol: this.symbol, ...this.bbo });
    } else if (stream.endsWith('@depth20@100ms')) {
      const u = Number(d.u) || Number(d.lastUpdateId) || 0;
      const bidsArr = d.b || d.bids || [];
      const asksArr = d.a || d.asks || [];
      if (!bidsArr.length || !asksArr.length) return;
      this.counters.depth20 += 1;

      if (!this.synced) {
        this.bids.clear();
        this.asks.clear();
        this._applyLevels(this.bids, bidsArr);
        this._applyLevels(this.asks, asksArr);
        this.lastDepth20U = u;
        this.synced = true;
        this._updateStatus();
      } else if (u >= this.lastDepth20U && u >= this.lastDiffU) {
        // Refresh top-20 authoritative levels cleanly only when depth20 u is at least as fresh as diff stream
        const topBids = bidsArr.map((x) => [Number(x[0]), Number(x[1])]).filter((x) => x[0] > 0);
        const topAsks = asksArr.map((x) => [Number(x[0]), Number(x[1])]).filter((x) => x[0] > 0);
        if (topBids.length && topAsks.length) {
          const minTopBid = topBids[topBids.length - 1][0];
          const maxTopAsk = topAsks[topAsks.length - 1][0];
          for (const p of this.bids.keys()) if (p >= minTopBid) this.bids.delete(p);
          for (const p of this.asks.keys()) if (p <= maxTopAsk) this.asks.delete(p);
          for (const [p, q] of topBids) if (q > 0) this.bids.set(p, q);
          for (const [p, q] of topAsks) if (q > 0) this.asks.set(p, q);
          this.lastDepth20U = u;
        }
      }
      this._emitBookSnapshot(normTs, rxTs);
    } else if (stream.endsWith('@depth@100ms')) {
      const u = Number(d.u) || 0;
      const pu = d.pu !== undefined ? Number(d.pu) : null;
      this.counters.depthDiff += 1;
      if (!this.synced) {
        this.lastDiffU = u;
        return; // wait for next depth20 seed (~100ms)
      }
      if (this.lastDiffU > 0 && u <= this.lastDiffU) return;
      if (pu !== null && this.lastDiffU > 0 && pu !== this.lastDiffU) {
        // True sequence gap within @depth@100ms stream itself!
        this.counters.sequenceGaps += 1;
        this.synced = false;
        this.lastDiffU = 0;
        this.bids.clear();
        this.asks.clear();
        this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'pu_sequence_gap', expectedPu: this.lastDiffU, gotPu: pu, ts: normTs });
        return;
      }
      this._applyLevels(this.bids, d.b);
      this._applyLevels(this.asks, d.a);
      this.lastDiffU = u;
      this._trimBook();
      this._emitBookSnapshot(normTs, rxTs);
    } else if (stream.endsWith('@trade')) {
      const price = Number(d.p);
      const qty = Number(d.q);
      // PHASE 0 AUDIT FIX: Filter out non-orderbook settlement prints (p="0.00" or X !== "MARKET")
      if (!(price > 0 && qty > 0) || (d.X && d.X !== 'MARKET')) {
        this.counters.ignoredNonMarketTrades += 1;
        return;
      }
      this.counters.trade += 1;
      this.lastTradeStreamAt = rxTs;
      this.emit('trade', {
        venue: this.venue,
        symbol: this.symbol,
        price,
        qty,
        notionalUsd: price * qty,
        side: d.m ? -1 : 1, // m=true -> buyer is maker -> aggressive seller (-1)
        tradeId: d.t,
        venueTs: evTs,
        ts: normTs,
        rxTs
      });
    }
  }

  _handleMarketChannelMessage(raw, rxTs) {
    let pkt;
    try { pkt = JSON.parse(raw); } catch (_) { this.counters.malformedPackets += 1; return; }
    if (!pkt || !pkt.stream || !pkt.data) return;
    const stream = pkt.stream;
    const d = pkt.data;
    // NOTE: On @markPrice@1s, d.T is nextFundingTime (hours in future!), while d.E is Event Time!
    const evTs = stream.endsWith('@markPrice@1s')
      ? (Number(d.E) || rxTs)
      : (Number(d.T) || Number(d.E) || rxTs);
    this.clock.observePacket(evTs, rxTs);
    const normTs = this.clock.normalizeTimestamp(evTs, rxTs);

    if (stream.endsWith('@markPrice@1s')) {
      this.counters.markPrice += 1;
      this.mark = {
        markPrice: Number(d.p) || 0,
        indexPrice: Number(d.i) || 0,
        fundingRate: Number(d.r) || 0,
        nextFundingTime: Number(d.T) || 0,
        ts: normTs
      };
      this.emit('mark', { venue: this.venue, symbol: this.symbol, ...this.mark });
    } else if (stream.endsWith('@forceOrder') && d.o) {
      const price = Number(d.o.ap) || Number(d.o.p) || 0;
      const qty = Number(d.o.q) || 0;
      if (price > 0 && qty > 0) {
        this.counters.forceOrder += 1;
        // Forced BUY = short liquidated (+1 aggressive buy pressure); Forced SELL = long liquidated (-1)
        const side = d.o.S === 'BUY' ? 1 : -1;
        this.emit('liquidation', {
          venue: this.venue,
          symbol: this.symbol,
          price,
          qty,
          notionalUsd: price * qty,
          side,
          venueTs: evTs,
          ts: normTs,
          rxTs
        });
      }
    } else if (stream.endsWith('@aggTrade')) {
      this.counters.aggTradeMarketStream += 1;
      // Only use as fallback if primary @trade stream has been silent > 10s (anti-double-count)
      if (rxTs - this.lastTradeStreamAt < 10_000) return;
      const price = Number(d.p), qty = Number(d.q);
      if (price > 0 && qty > 0) {
        this.emit('trade', {
          venue: this.venue,
          symbol: this.symbol,
          price,
          qty,
          notionalUsd: price * qty,
          side: d.m ? -1 : 1,
          tradeId: d.a,
          venueTs: evTs,
          ts: normTs,
          rxTs
        });
      }
    }
  }

  _emitBookSnapshot(normTs, rxTs) {
    if (!this.synced || this.bids.size < 3 || this.asks.size < 3) return;
    const bids = Array.from(this.bids.entries()).sort((a, b) => b[0] - a[0]).slice(0, 80);
    const asks = Array.from(this.asks.entries()).sort((a, b) => a[0] - b[0]).slice(0, 80);
    if (!bids.length || !asks.length || !(asks[0][0] > bids[0][0])) return;
    this.emit('book', {
      venue: this.venue,
      symbol: this.symbol,
      bids,
      asks,
      bestBid: this.bbo.bid > 0 && this.bbo.ask > this.bbo.bid ? this.bbo.bid : bids[0][0],
      bestAsk: this.bbo.ask > this.bbo.bid ? this.bbo.ask : asks[0][0],
      bestBidQty: this.bbo.bid > 0 ? this.bbo.bidQty : bids[0][1],
      bestAskQty: this.bbo.ask > 0 ? this.bbo.askQty : asks[0][1],
      ts: normTs,
      rxTs
    });
  }

  getHealth() {
    return {
      venue: this.venue,
      symbol: this.symbol,
      status: this.status,
      synced: this.synced,
      bookLevels: { bids: this.bids.size, asks: this.asks.size },
      bbo: this.bbo,
      mark: this.mark,
      counters: { ...this.counters },
      clock: this.clock.snapshot()
    };
  }

  stop() {
    this.active = false;
    this.synced = false;
    if (this.reconnectTimerBook) { clearTimeout(this.reconnectTimerBook); this.reconnectTimerBook = null; }
    if (this.reconnectTimerMarket) { clearTimeout(this.reconnectTimerMarket); this.reconnectTimerMarket = null; }
    if (this.bookWs) { this.bookWs.close(); this.bookWs = null; }
    if (this.marketWs) { this.marketWs.close(); this.marketWs = null; }
    this.bids.clear();
    this.asks.clear();
    this.status = 'IDLE';
  }
}

module.exports = { BinanceFuturesAdapter };
