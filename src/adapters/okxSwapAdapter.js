'use strict';

const { EventEmitter } = require('events');
const { WsClient } = require('../lib/wsClient');
const { ClockTracker } = require('../lib/clockTracker');

function symbolToOkxInstId(symbol) {
  const upper = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
  const baseRaw = upper.endsWith('USDT') ? upper.slice(0, -4) : upper;
  const base = baseRaw.replace(/^1000(?=[A-Z]+$)/, '');
  return { instId: `${base}-USDT-SWAP`, priceScale: base !== baseRaw ? 1000 : 1 };
}

class OkxSwapAdapter extends EventEmitter {
  constructor(symbol) {
    super();
    this.venue = 'OKX_SWAP';
    this.symbol = String(symbol).toUpperCase();
    const mapped = symbolToOkxInstId(this.symbol);
    this.instId = mapped.instId;
    this.priceScale = mapped.priceScale;

    this.active = false;
    this.ws = null;
    this.reconnectTimer = null;
    this.retry = 0;

    this.ctVal = null; // Contract value in base coin (verified from OKX REST API)
    this.tickSz = null;
    this.clock = new ClockTracker(this.venue);
    this.bids = new Map();
    this.asks = new Map();
    this.synced = false;
    this.lastSeqId = null;

    this.counters = {
      bookSnapshots: 0,
      bookUpdates: 0,
      trades: 0,
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
    this._initInstrumentAndConnect();
  }

  async _initInstrumentAndConnect() {
    try {
      const t0 = Date.now();
      const timeRes = await fetch('https://www.okx.com/api/v5/public/time');
      const t1 = Date.now();
      if (timeRes.ok) {
        const tj = await timeRes.json();
        if (tj && tj.data && tj.data[0]) {
          this.clock.recordRestSkew(t0, t1, Number(tj.data[0].ts));
        }
      }
    } catch (_) {}

    try {
      const instUrl = `https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${encodeURIComponent(this.instId)}`;
      const res = await fetch(instUrl);
      if (res.ok) {
        const j = await res.json();
        if (!j.data || !j.data.length) {
          this.status = 'UNSUPPORTED_SYMBOL';
          this.emit('status', this.getHealth());
          return;
        }
        const info = j.data[0];
        this.ctVal = Number(info.ctVal) || 1;
        this.tickSz = Number(info.tickSz) || 0;
      }
    } catch (_) {
      // Do not guess ctVal if instrument lookup failed; retry cleanly
      if (this.active) {
        this.reconnectTimer = setTimeout(() => this._initInstrumentAndConnect(), 3000);
      }
      return;
    }

    if (!this.active || !this.ctVal) return;
    this._connectWs();
  }

  _connectWs() {
    if (!this.active) return;
    const ws = new WsClient('wss://ws.okx.com:443/ws/v5/public', { handshakeTimeoutMs: 9000, pingIntervalMs: 15000 });
    this.ws = ws;

    ws.on('open', () => {
      if (this.ws !== ws || !this.active) return;
      this.retry = 0;
      ws.send(JSON.stringify({
        op: 'subscribe',
        args: [
          { channel: 'books', instId: this.instId },
          { channel: 'trades', instId: this.instId }
        ]
      }));
      this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'okx_ws_open', ts: Date.now() });
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
      this.lastSeqId = null;
      if (!this.active || this.status === 'UNSUPPORTED_SYMBOL') return;
      this.status = 'DISCONNECTED';
      this.counters.reconnects += 1;
      this.emit('status', this.getHealth());
      this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'okx_ws_closed', ts: Date.now() });
      const wait = Math.min(18000, 1200 * Math.pow(1.8, Math.min(this.retry++, 6)));
      this.reconnectTimer = setTimeout(() => this._connectWs(), wait);
    });
  }

  _applyLevels(map, levels) {
    if (!Array.isArray(levels) || !this.ctVal) return;
    for (const lv of levels) {
      const p = Number(lv[0]) * this.priceScale;
      const contracts = Number(lv[1]);
      if (!Number.isFinite(p) || p <= 0 || !Number.isFinite(contracts)) continue;
      const baseQty = (contracts * this.ctVal) / this.priceScale;
      if (baseQty <= 0) map.delete(p);
      else map.set(p, baseQty);
    }
  }

  _handleMessage(raw, rxTs) {
    let msg;
    try { msg = JSON.parse(raw); } catch (_) { this.counters.malformedPackets += 1; return; }
    if (!msg) return;
    if (msg.event === 'error') {
      if (String(msg.code) === '60018' || String(msg.msg || '').includes("doesn't exist")) {
        this.status = 'UNSUPPORTED_SYMBOL';
        this.emit('status', this.getHealth());
        if (this.ws) { this.ws.close(); this.ws = null; }
      }
      return;
    }
    if (!msg.arg || !Array.isArray(msg.data)) return;

    if (msg.arg.channel === 'books') {
      for (const b of msg.data) {
        const evTs = Number(b.ts) || rxTs;
        this.clock.observePacket(evTs, rxTs);
        const normTs = this.clock.normalizeTimestamp(evTs, rxTs);
        const seqId = b.seqId !== undefined ? Number(b.seqId) : null;
        const prevSeqId = b.prevSeqId !== undefined ? Number(b.prevSeqId) : null;

        if (msg.action === 'snapshot') {
          this.bids.clear();
          this.asks.clear();
          this._applyLevels(this.bids, b.bids);
          this._applyLevels(this.asks, b.asks);
          this.lastSeqId = seqId;
          this.synced = this.bids.size >= 3 && this.asks.size >= 3;
          this.counters.bookSnapshots += 1;
          this.status = this.synced ? 'LIVE' : 'CONNECTING';
          this.emit('status', this.getHealth());
        } else if (msg.action === 'update') {
          if (!this.synced) continue;
          if (seqId !== null && this.lastSeqId !== null && seqId <= this.lastSeqId) continue;
          if (prevSeqId !== null && this.lastSeqId !== null && prevSeqId !== -1 && prevSeqId !== this.lastSeqId) {
            // OKX sequence gap -> trigger clean resubscribe
            this.counters.sequenceGaps += 1;
            this.synced = false;
            this.emit('gap', { venue: this.venue, symbol: this.symbol, reason: 'okx_seq_gap', expectedPrev: this.lastSeqId, gotPrev: prevSeqId, ts: normTs });
            if (this.ws) this.ws.close();
            return;
          }
          this._applyLevels(this.bids, b.bids);
          this._applyLevels(this.asks, b.asks);
          if (seqId !== null) this.lastSeqId = seqId;
          this.counters.bookUpdates += 1;
        }

        if (this.bids.size > 400) {
          const s = Array.from(this.bids.keys()).sort((a, x) => x - a);
          for (let i = 300; i < s.length; i++) this.bids.delete(s[i]);
        }
        if (this.asks.size > 400) {
          const s = Array.from(this.asks.keys()).sort((a, x) => a - x);
          for (let i = 300; i < s.length; i++) this.asks.delete(s[i]);
        }

        const bids = Array.from(this.bids.entries()).sort((a, x) => x[0] - a[0]).slice(0, 80);
        const asks = Array.from(this.asks.entries()).sort((a, x) => a[0] - x[0]).slice(0, 80);
        if (bids.length >= 3 && asks.length >= 3 && asks[0][0] > bids[0][0]) {
          this.emit('book', {
            venue: this.venue,
            symbol: this.symbol,
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
      }
    } else if (msg.arg.channel === 'trades' && this.ctVal) {
      for (const tr of msg.data) {
        const price = Number(tr.px) * this.priceScale;
        const contracts = Number(tr.sz);
        if (!(price > 0 && contracts > 0)) continue;
        const qty = (contracts * this.ctVal) / this.priceScale;
        const evTs = Number(tr.ts) || rxTs;
        this.clock.observePacket(evTs, rxTs);
        const normTs = this.clock.normalizeTimestamp(evTs, rxTs);
        this.counters.trades += 1;
        this.emit('trade', {
          venue: this.venue,
          symbol: this.symbol,
          price,
          qty,
          notionalUsd: price * qty,
          side: tr.side === 'buy' ? 1 : -1,
          tradeId: tr.tradeId,
          venueTs: evTs,
          ts: normTs,
          rxTs
        });
      }
    }
  }

  getHealth() {
    return {
      venue: this.venue,
      symbol: this.symbol,
      instId: this.instId,
      ctVal: this.ctVal,
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
    this.lastSeqId = null;
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.ws) { this.ws.close(); this.ws = null; }
    this.bids.clear();
    this.asks.clear();
    this.status = 'IDLE';
  }
}

module.exports = { OkxSwapAdapter, symbolToOkxInstId };
