'use strict';

const { EventEmitter } = require('events');
const { WsClient } = require('../lib/wsClient');
const { ClockTracker } = require('../lib/clockTracker');

class BinanceSpotAdapter extends EventEmitter {
  constructor(symbol) {
    super();
    this.venue = 'BINANCE_SPOT';
    this.requestedSymbol = String(symbol).toUpperCase();
    this.spotSymbol = this.requestedSymbol.replace(/^1000(?=[A-Z]+USDT$)/, '');
    this.priceScale = this.spotSymbol !== this.requestedSymbol ? 1000 : 1;

    this.active = false;
    this.ws = null;
    this.reconnectTimer = null;
    this.retry = 0;
    this.syncingSnapshot = false;

    this.clock = new ClockTracker(this.venue);
    this.bids = new Map();
    this.asks = new Map();
    this.buffer = [];
    this.synced = false;
    this.lastUpdateId = 0;

    this.counters = {
      depthDiff: 0,
      aggTrade: 0,
      snapshotsSynced: 0,
      sequenceGaps: 0,
      malformedPackets: 0,
      reconnects: 0
    };
    this.status = 'IDLE'; // IDLE | CONNECTING | LIVE | UNSUPPORTED_SYMBOL | DISCONNECTED
  }

  start() {
    if (this.active) return;
    this.active = true;
    this.status = 'CONNECTING';
    this._probeClockAndConnect();
  }

  async _probeClockAndConnect() {
    try {
      const t0 = Date.now();
      const res = await fetch('https://data-api.binance.vision/api/v3/time');
      const t1 = Date.now();
      if (res.ok) {
        const j = await res.json();
        this.clock.recordRestSkew(t0, t1, Number(j.serverTime));
      }
    } catch (_) {}
    if (!this.active) return;
    this._connectWs();
  }

  _connectWs() {
    if (!this.active) return;
    const s = this.spotSymbol.toLowerCase();
    const url = `wss://data-stream.binance.vision/stream?streams=${s}@depth@100ms/${s}@aggTrade`;
    const ws = new WsClient(url, { handshakeTimeoutMs: 9000, pingIntervalMs: 15000 });
    this.ws = ws;

    ws.on('open', () => {
      if (this.ws !== ws || !this.active) return;
      this.retry = 0;
      this.emit('gap', { venue: this.venue, symbol: this.requestedSymbol, reason: 'spot_ws_open', ts: Date.now() });
      this._syncDepthSnapshot();
    });

    ws.on('pong', (rtt) => this.clock.recordWsRtt(rtt));

    ws.on('message', (raw, rxTs) => {
      if (this.ws !== ws || !this.active) return;
      this._handleMessage(raw, rxTs);
    });

    ws.on('error', () => {});

    ws.on('close', () => {
      if (this.ws !== ws) return;
      this.ws = null;
      this.synced = false;
      if (!this.active) { this.status = 'IDLE'; return; }
      if (this.status === 'UNSUPPORTED_SYMBOL') return;
      this.status = 'DISCONNECTED';
      this.counters.reconnects += 1;
      this.emit('status', this.getHealth());
      this.emit('gap', { venue: this.venue, symbol: this.requestedSymbol, reason: 'spot_ws_closed', ts: Date.now() });
      const wait = Math.min(18000, 1200 * Math.pow(1.8, Math.min(this.retry++, 6)));
      this.reconnectTimer = setTimeout(() => this._connectWs(), wait);
    });
  }

  async _syncDepthSnapshot() {
    if (!this.active || this.syncingSnapshot) return;
    this.syncingSnapshot = true;
    try {
      const url = `https://data-api.binance.vision/api/v3/depth?symbol=${encodeURIComponent(this.spotSymbol)}&limit=500`;
      const res = await fetch(url);
      if (res.status === 400) {
        this.status = 'UNSUPPORTED_SYMBOL';
        this.emit('status', this.getHealth());
        if (this.ws) { this.ws.close(); this.ws = null; }
        this.syncingSnapshot = false;
        return;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const snap = await res.json();
      if (!this.active) return;

      this.bids.clear();
      this.asks.clear();
      this._applyLevels(this.bids, snap.bids);
      this._applyLevels(this.asks, snap.asks);
      this.lastUpdateId = Number(snap.lastUpdateId) || 0;

      const buffered = this.buffer.splice(0);
      for (const ev of buffered) {
        if (ev.u <= this.lastUpdateId) continue;
        this._applyLevels(this.bids, ev.b);
        this._applyLevels(this.asks, ev.a);
        this.lastUpdateId = ev.u;
      }
      this.synced = this.bids.size >= 3 && this.asks.size >= 3;
      this.counters.snapshotsSynced += 1;
      this.status = this.synced ? 'LIVE' : 'CONNECTING';
      this.emit('status', this.getHealth());
    } catch (_) {
      if (this.active && this.status !== 'UNSUPPORTED_SYMBOL') {
        setTimeout(() => this._syncDepthSnapshot(), 2500).unref();
      }
    } finally {
      this.syncingSnapshot = false;
    }
  }

  _applyLevels(map, arr) {
    if (!Array.isArray(arr)) return;
    for (const lv of arr) {
      const p = Number(lv[0]) * this.priceScale;
      const q = Number(lv[1]) / this.priceScale;
      if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(q)) continue;
      if (q <= 0) map.delete(p);
      else map.set(p, q);
    }
  }

  _handleMessage(raw, rxTs) {
    let pkt;
    try { pkt = JSON.parse(raw); } catch (_) { this.counters.malformedPackets += 1; return; }
    if (!pkt || !pkt.stream || !pkt.data) return;
    const d = pkt.data;
    const evTs = Number(d.T) || Number(d.E) || rxTs;
    this.clock.observePacket(evTs, rxTs);
    const normTs = this.clock.normalizeTimestamp(evTs, rxTs);

    if (pkt.stream.endsWith('@depth@100ms')) {
      const U = Number(d.U), u = Number(d.u);
      this.counters.depthDiff += 1;
      if (!this.synced) {
        this.buffer.push({ U, u, b: d.b, a: d.a });
        if (this.buffer.length > 400) this.buffer.shift();
        return;
      }
      if (u <= this.lastUpdateId) return;
      if (U > this.lastUpdateId + 1) {
        this.counters.sequenceGaps += 1;
        this.synced = false;
        this.emit('gap', { venue: this.venue, symbol: this.requestedSymbol, reason: 'spot_U_sequence_gap', expectedMaxU: this.lastUpdateId + 1, gotU: U, ts: normTs });
        this._syncDepthSnapshot();
        return;
      }
      this._applyLevels(this.bids, d.b);
      this._applyLevels(this.asks, d.a);
      this.lastUpdateId = u;

      if (this.bids.size > 400) {
        const s = Array.from(this.bids.keys()).sort((a, b) => b - a);
        for (let i = 300; i < s.length; i++) this.bids.delete(s[i]);
      }
      if (this.asks.size > 400) {
        const s = Array.from(this.asks.keys()).sort((a, b) => a - b);
        for (let i = 300; i < s.length; i++) this.asks.delete(s[i]);
      }

      const bids = Array.from(this.bids.entries()).sort((a, b) => b[0] - a[0]).slice(0, 80);
      const asks = Array.from(this.asks.entries()).sort((a, b) => a[0] - b[0]).slice(0, 80);
      if (bids.length >= 3 && asks.length >= 3 && asks[0][0] > bids[0][0]) {
        this.emit('book', {
          venue: this.venue,
          symbol: this.requestedSymbol,
          bids,
          asks,
          bestBid: bids[0][0],
          bestAsk: asks[0][0],
          bestBidQty: bids[0][1],
          bestAskQty: asks[0][1],
          ts: normTs,
          rxTs
        });
      }
    } else if (pkt.stream.endsWith('@aggTrade')) {
      const price = Number(d.p) * this.priceScale;
      const qty = Number(d.q) / this.priceScale;
      if (!(price > 0 && qty > 0)) return;
      this.counters.aggTrade += 1;
      this.emit('trade', {
        venue: this.venue,
        symbol: this.requestedSymbol,
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

  getHealth() {
    return {
      venue: this.venue,
      symbol: this.requestedSymbol,
      spotSymbol: this.spotSymbol,
      status: this.status,
      synced: this.synced,
      bookLevels: { bids: this.bids.size, asks: this.asks.size },
      counters: { ...this.counters },
      clock: this.clock.snapshot()
    };
  }

  stop() {
    this.active = false;
    this.synced = false;
    this.buffer = [];
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.bids.clear();
    this.asks.clear();
    this.status = 'IDLE';
  }
}

module.exports = { BinanceSpotAdapter };
