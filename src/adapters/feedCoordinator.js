'use strict';

const { EventEmitter } = require('events');
const { BinanceFuturesAdapter } = require('./binanceFuturesAdapter');
const { BinanceSpotAdapter } = require('./binanceSpotAdapter');
const { OkxSwapAdapter } = require('./okxSwapAdapter');

const SUPPORTED_SYMBOLS = [
  'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT',
  'SUIUSDT', 'BNBUSDT', 'AVAXUSDT', 'LINKUSDT', 'ADAUSDT',
  'NEARUSDT', 'ARBUSDT', 'OPUSDT', 'APTUSDT', 'LTCUSDT', 'INJUSDT'
];

class FeedCoordinator extends EventEmitter {
  constructor(options = {}) {
    super();
    this.symbol = null;
    this.running = false;
    this.enabledSources = {
      BINANCE_FUT: options.BINANCE_FUT !== false,
      BINANCE_SPOT: options.BINANCE_SPOT !== false,
      OKX_SWAP: options.OKX_SWAP !== false
    };
    this.adapters = {};
    this.watchdogTimer = null;
    this.staleFlag = false;
    this.lastAnyPacketAt = 0;
    this.startedAt = 0;
  }

  startCoin(rawSymbol) {
    const clean = String(rawSymbol || 'BTCUSDT').toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (this.running && this.symbol === clean) return this.symbol;
    if (this.running) this.stopAll('coin_switch');

    this.symbol = clean;
    this.running = true;
    this.startedAt = Date.now();
    this.lastAnyPacketAt = Date.now();
    this.staleFlag = false;

    this._mountAdapters();
    this._startWatchdog();
    this.emit('coin_started', { symbol: this.symbol, ts: this.startedAt });
    return this.symbol;
  }

  updateEnabledSources(partial) {
    for (const k of ['BINANCE_FUT', 'BINANCE_SPOT', 'OKX_SWAP']) {
      if (typeof partial[k] === 'boolean') this.enabledSources[k] = partial[k];
    }
    if (this.running && this.symbol) {
      const sym = this.symbol;
      this.stopAll('source_toggle');
      this.startCoin(sym);
    }
    return { ...this.enabledSources };
  }

  _mountAdapters() {
    const sym = this.symbol;
    const defs = [
      ['BINANCE_FUT', () => new BinanceFuturesAdapter(sym)],
      ['BINANCE_SPOT', () => new BinanceSpotAdapter(sym)],
      ['OKX_SWAP', () => new OkxSwapAdapter(sym)]
    ];

    for (const [key, factory] of defs) {
      if (!this.enabledSources[key]) continue;
      const ad = factory();
      this.adapters[key] = ad;

      for (const evName of ['bbo', 'book', 'trade', 'mark', 'liquidation']) {
        ad.on(evName, (payload) => {
          if (!this.running || payload.symbol !== this.symbol) return;
          this.lastAnyPacketAt = Date.now();
          if (this.staleFlag) {
            this.staleFlag = false;
            this.emit('integrity', { symbol: this.symbol, status: 'OK', reason: 'stream_recovered', ts: Date.now() });
          }
          this.emit(evName, payload);
        });
      }

      ad.on('gap', (gapInfo) => {
        if (!this.running || gapInfo.symbol !== this.symbol) return;
        this.emit('gap', gapInfo);
      });

      ad.on('status', (health) => {
        if (!this.running) return;
        this.emit('venue_status', health);
      });

      ad.start();
    }
  }

  _startWatchdog() {
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    this.watchdogTimer = setInterval(() => {
      if (!this.running) return;
      const silenceMs = Date.now() - this.lastAnyPacketAt;
      if (silenceMs > 7000 && !this.staleFlag) {
        this.staleFlag = true;
        this.emit('gap', { venue: 'ALL', symbol: this.symbol, reason: 'stream_silence_timeout', silenceMs, ts: Date.now() });
        this.emit('integrity', { symbol: this.symbol, status: 'STALE', silenceMs, ts: Date.now() });
      }
    }, 2000);
    if (this.watchdogTimer.unref) this.watchdogTimer.unref();
  }

  /**
   * Computes conservative clock uncertainty (in ms) between two venues.
   * Used to prevent false cross-venue lead-lag assertions when |dt| < uncertainty.
   */
  getCrossVenueUncertaintyMs(venueA, venueB) {
    if (venueA === venueB) return 0;
    const a = this.adapters[venueA] && this.adapters[venueA].clock;
    const b = this.adapters[venueB] && this.adapters[venueB].clock;
    if (!a || !b || a.samples < 5 || b.samples < 5) return Infinity; // Uncalibrated -> refuse lead-lag claim
    const jitA = a.jitterMs || 25;
    const jitB = b.jitterMs || 25;
    const skewPenalty = (a.restClockSkewMs === null || b.restClockSkewMs === null) ? 50 : 10;
    return +(2 * Math.sqrt(jitA * jitA + jitB * jitB) + skewPenalty).toFixed(1);
  }

  getIntegrityState() {
    if (!this.running) return { ok: false, label: 'PAUSED', liveVenues: 0 };
    let liveVenues = 0;
    for (const ad of Object.values(this.adapters)) {
      if (ad.status === 'LIVE' && ad.synced) liveVenues += 1;
    }
    if (this.staleFlag || liveVenues === 0) {
      return { ok: false, label: 'DISCONNECTED', liveVenues };
    }
    return { ok: true, label: 'OK', liveVenues };
  }

  getSnapshot() {
    const venues = {};
    for (const [k, ad] of Object.entries(this.adapters)) {
      venues[k] = ad.getHealth();
    }
    return {
      symbol: this.symbol,
      running: this.running,
      startedAt: this.startedAt,
      enabledSources: { ...this.enabledSources },
      integrity: this.getIntegrityState(),
      venues
    };
  }

  stopAll(reason = 'stopped') {
    const prevSymbol = this.symbol;
    this.running = false;
    if (this.watchdogTimer) { clearInterval(this.watchdogTimer); this.watchdogTimer = null; }
    for (const ad of Object.values(this.adapters)) {
      ad.removeAllListeners();
      ad.stop();
    }
    this.adapters = {};
    if (prevSymbol) {
      this.emit('coin_stopped', { symbol: prevSymbol, reason, ts: Date.now() });
    }
  }
}

module.exports = { FeedCoordinator, SUPPORTED_SYMBOLS };
