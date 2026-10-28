'use strict';

const { EventEmitter } = require('events');
const { FeedCoordinator, SUPPORTED_SYMBOLS } = require('../adapters/feedCoordinator');
const { EventEncoder } = require('../events/eventEncoder');
const { EVENT_DICTIONARY } = require('../events/eventDictionary');
const { buildCandidateSequences } = require('../motifs/sequenceBuilder');
const { EpisodeManager } = require('../motifs/episodeManager');
const { MotifRegistry } = require('../motifs/motifRegistry');
const { BaseModelSuite } = require('../learning/baseModels');
const { QuarantineEngine } = require('../learning/quarantineEngine');
const { CoinMemoryStore } = require('../storage/coinMemoryStore');

class MarketDnaRuntime extends EventEmitter {
  constructor(options = {}) {
    super();
    this.store = new CoinMemoryStore(options.dataDir);
    this.feeds = new FeedCoordinator(options.sources || {});
    this.backgroundIncubation = options.backgroundIncubation === true;
    this.sessionLeaseTimeoutMs = options.sessionLeaseTimeoutMs || 25_000;
    this.autoSaveIntervalMs = options.autoSaveIntervalMs || 15_000;

    this.activeSymbol = null;
    this.sessionNumber = 0;
    this.pausedForIdle = false;
    this.lastRecoveryReport = null;
    this.lastSaveInfo = null;
    this.lastCausalTs = 0;

    // Active per-coin domain modules
    this.encoder = null;
    this.episodes = null;
    this.registry = null;
    this.baseModels = null;
    this.quarantine = null;

    // Live microstructure telemetry for Market tab
    this.liveTelemetry = {
      mid: 0,
      bestBid: 0,
      bestAsk: 0,
      spreadBps: 0,
      markPrice: 0,
      indexPrice: 0,
      basisBps: 0,
      fundingRate: 0,
      regime: 'NORMAL',
      recentTradesUsd15s: 0,
      buyShare15s: 50,
      recentPriceTrail: [] // { ts, mid } last 90s
    };
    this.recentTradeWindow = [];

    // Connected UI clients & lease tracking (Section 10)
    this.activeStreams = new Set(); // active SSE res objects
    this.clientLeases = new Map();  // clientId -> lastHeartbeatTs

    this._bindFeedHandlers();
    this._startMaintenanceTimers();
  }

  _causalNow(candidateTs) {
    const raw = Number.isFinite(candidateTs) && candidateTs > 0 ? candidateTs : Date.now();
    this.lastCausalTs = Math.max(this.lastCausalTs, raw);
    return this.lastCausalTs;
  }

  _bindFeedHandlers() {
    this.feeds.on('bbo', (bbo) => {
      if (bbo.venue === 'BINANCE_FUT') {
        const mid = (bbo.bid + bbo.ask) / 2;
        this.liveTelemetry.bestBid = bbo.bid;
        this.liveTelemetry.bestAsk = bbo.ask;
        this.liveTelemetry.mid = +mid.toFixed(6);
        this.liveTelemetry.spreadBps = +(((bbo.ask - bbo.bid) / mid) * 1e4).toFixed(3);
      }
    });

    this.feeds.on('mark', (mk) => {
      this.liveTelemetry.markPrice = mk.markPrice;
      this.liveTelemetry.indexPrice = mk.indexPrice;
      this.liveTelemetry.fundingRate = mk.fundingRate;
      if (mk.markPrice > 0 && mk.indexPrice > 0) {
        this.liveTelemetry.basisBps = +(((mk.markPrice - mk.indexPrice) / mk.indexPrice) * 1e4).toFixed(2);
      }
    });

    this.feeds.on('book', (bk) => {
      if (!this.encoder || bk.symbol !== this.activeSymbol) return;
      this.encoder.onBook(bk);

      if (bk.venue === 'BINANCE_FUT' || !this.feeds.enabledSources.BINANCE_FUT) {
        const mid = (bk.bestBid + bk.bestAsk) / 2;
        const spreadBps = ((bk.bestAsk - bk.bestBid) / mid) * 1e4;
        this.liveTelemetry.mid = +mid.toFixed(6);
        this.liveTelemetry.bestBid = bk.bestBid;
        this.liveTelemetry.bestAsk = bk.bestAsk;
        this.liveTelemetry.spreadBps = +spreadBps.toFixed(3);
        this.liveTelemetry.regime = this.encoder.primaryRegime;

        const causalNow = this._causalNow(bk.recvTs || Date.now());
        const trail = this.liveTelemetry.recentPriceTrail;
        if (!trail.length || causalNow - trail[trail.length - 1].ts >= 500) {
          trail.push({ ts: causalNow, mid: +mid.toFixed(6) });
          while (trail.length > 180) trail.shift();
        }

        const vt = this.encoder.venueTrackers.get(bk.venue);
        const volRate = vt ? vt.volRateBpsPerSec : 1.5;
        this.baseModels.onBackgroundPriceTick(mid, spreadBps, volRate, this.encoder.primaryRegime, causalNow);
        this.episodes.onPriceUpdate(mid, causalNow, volRate);
      }
    });

    this.feeds.on('trade', (tr) => {
      if (!this.encoder || tr.symbol !== this.activeSymbol) return;
      this.encoder.onTrade(tr);

      const now = Date.now();
      this.recentTradeWindow.push({ ts: now, notionalUsd: tr.notionalUsd, side: tr.side });
      while (this.recentTradeWindow.length && now - this.recentTradeWindow[0].ts > 15_000) {
        this.recentTradeWindow.shift();
      }
      let buy = 0, tot = 0;
      for (const x of this.recentTradeWindow) {
        tot += x.notionalUsd;
        if (x.side > 0) buy += x.notionalUsd;
      }
      this.liveTelemetry.recentTradesUsd15s = Math.round(tot);
      this.liveTelemetry.buyShare15s = tot > 0 ? +((buy / tot) * 100).toFixed(1) : 50;
    });

    this.feeds.on('liquidation', (lq) => {
      if (!this.encoder || lq.symbol !== this.activeSymbol) return;
      this.encoder.onLiquidation(lq);
    });

    this.feeds.on('gap', (gap) => {
      if (!this.encoder || gap.symbol !== this.activeSymbol) return;
      this.encoder.onGap(gap);
      const isInitialOpen = String(gap.reason || '').endsWith('_open') || gap.reason === 'ws_open_sync';
      if (!isInitialOpen && (gap.venue === 'ALL' || gap.venue === 'BINANCE_FUT')) {
        this.episodes.invalidateOpenHorizons(gap.reason, gap.ts);
        this.baseModels.invalidatePendingBenchmarks();
        this.quarantine.onStreamDisruption(gap.reason, gap.ts);
      }
    });

    this.feeds.on('venue_status', () => {
      this.emit('state_tick', this.getSummaryPayload());
    });
  }

  _startMaintenanceTimers() {
    this.saveTimer = setInterval(() => {
      if (this.activeSymbol && !this.pausedForIdle) {
        this.flushActiveCoinToDisk('periodic_autosave');
      }
      this._checkSessionIdlePolicy();
    }, this.autoSaveIntervalMs);
    if (this.saveTimer.unref) this.saveTimer.unref();

    this.broadcastTimer = setInterval(() => {
      if (this.activeStreams.size > 0 && this.activeSymbol) {
        this.emit('state_tick', this.getSummaryPayload());
      }
    }, 1000);
    if (this.broadcastTimer.unref) this.broadcastTimer.unref();
  }

  /**
   * Switches active single-coin focus cleanly:
   * 1) Closes active episode chain & flushes previous coin to disk
   * 2) Loads persistent B+ memory for new coin (or initializes fresh)
   * 3) Injects <SESSION_GAP> barrier and starts multi-venue live feeds
   */
  selectCoin(rawSymbol) {
    const nextSymbol = String(rawSymbol || 'BTCUSDT').toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (this.activeSymbol === nextSymbol && !this.pausedForIdle && this.feeds.running) {
      return this.getSummaryPayload();
    }

    // 1. Cleanly close & persist previous coin if one was active
    if (this.activeSymbol) {
      this._closeCurrentCoinSession('coin_switch_out');
    }

    // 2. Initialize domain modules for nextSymbol & load persistent memory
    this.activeSymbol = nextSymbol;
    this.pausedForIdle = false;
    this.recentTradeWindow = [];
    this.liveTelemetry = {
      mid: 0, bestBid: 0, bestAsk: 0, spreadBps: 0,
      markPrice: 0, indexPrice: 0, basisBps: 0, fundingRate: 0,
      regime: 'NORMAL', recentTradesUsd15s: 0, buyShare15s: 50, recentPriceTrail: []
    };

    this.encoder = new EventEncoder(nextSymbol);
    this.episodes = new EpisodeManager(nextSymbol);
    this.registry = new MotifRegistry(nextSymbol);
    this.baseModels = new BaseModelSuite(nextSymbol);
    this.quarantine = new QuarantineEngine(nextSymbol, this.registry, this.baseModels);

    const loaded = this.store.loadCoinState(nextSymbol);
    this.lastRecoveryReport = loaded.recoveryReport;

    if (loaded.payload) {
      this.sessionNumber = (loaded.payload.sessionNumber || 0) + 1;
      if (Number.isFinite(loaded.payload.eventSeq)) this.encoder.seq = loaded.payload.eventSeq;
      if (loaded.payload.auditStats) {
        Object.assign(this.encoder.auditStats, loaded.payload.auditStats);
      }
      this.registry.deserialize(loaded.payload.motifs || []);
      this.baseModels.deserialize(loaded.payload.baseModels);
      this.episodes.restoreState({
        seq: loaded.payload.episodeSeq || 0,
        recentResolved: loaded.recentEpisodes || []
      });
      if (Array.isArray(loaded.recentEvents) && loaded.recentEvents.length) {
        this.encoder.events = loaded.recentEvents.slice(-300);
      }
      this.quarantine.evaluateAllMotifs();
    } else {
      this.sessionNumber = 1;
    }

    // Wire internal domain pipeline for this coin
    this.encoder.on('event', (ev) => {
      this.store.appendJsonl(nextSymbol, 'events', ev);
      this.episodes.onSubsequentEvent(ev);
      const seqs = buildCandidateSequences(
        this.encoder.events,
        (vA, vB) => this.feeds.getCrossVenueUncertaintyMs(vA, vB)
      );
      if (seqs.length) {
        const causalNow = this._causalNow(ev.recvTs || Date.now());
        const refMid = this.liveTelemetry.mid > 0 ? this.liveTelemetry.mid : ev.midPrice;
        const refSpread = this.liveTelemetry.spreadBps > 0 ? this.liveTelemetry.spreadBps : ev.spreadBps;
        const preds = this.baseModels.predictAll(causalNow, this.encoder.primaryRegime);
        for (const cs of seqs) {
          this.episodes.tryOpenEpisode(
            { ...cs, t0: causalNow, entryMid: refMid },
            {
              spreadBps: refSpread,
              regime: this.encoder.primaryRegime,
              baseModelPreds: preds
            }
          );
        }
      }
      this.emit('dna_event', ev);
    });

    this.episodes.on('episode_opened', (ep) => {
      this.registry.registerEpisodeOpen(ep);
      this.quarantine.onNewEpisodeOpened(ep);
    });

    this.episodes.on('candidate_invalidation', (payload) => {
      this.quarantine.onCandidateInvalidation(payload);
    });

    this.episodes.on('episodes_invalidated', ({ episodeIds }) => {
      for (const ep of this.episodes.resolvedEpisodes.slice(-episodeIds.length)) {
        if (episodeIds.includes(ep.episodeId)) {
          this.registry.recordIntegrityBreak(ep.motifId);
        }
      }
    });

    this.episodes.on('horizon_resolved', ({ episode, horizonSec, slot }) => {
      this.registry.recordHorizonResolution(episode, horizonSec, slot);
      this.quarantine.evaluateAllMotifs();
    });

    this.episodes.on('episode_archived', (ep) => {
      this.store.appendJsonl(nextSymbol, 'episodes', ep);
    });

    // 3. Emit explicit <SESSION_GAP> barrier at session start before opening live feeds!
    this.encoder.emitSessionGap(
      loaded.payload ? `session_resume_${loaded.recoveryReport.stalenessLevel}` : 'initial_session_start'
    );

    this.feeds.startCoin(nextSymbol);
    this.flushActiveCoinToDisk('session_opened');
    return this.getSummaryPayload();
  }

  _closeCurrentCoinSession(reason) {
    if (!this.activeSymbol) return;
    this.feeds.stopAll(reason);
    if (this.episodes) this.episodes.invalidateOpenHorizons(reason, Date.now());
    if (this.baseModels) this.baseModels.invalidatePendingBenchmarks();
    if (this.quarantine) this.quarantine.onStreamDisruption(reason, Date.now());
    if (this.encoder) this.encoder.emitSessionGap(reason, Date.now());
    this.flushActiveCoinToDisk(reason);
  }

  flushActiveCoinToDisk(reason = 'manual') {
    if (!this.activeSymbol || !this.registry || !this.baseModels || !this.encoder || !this.episodes) return null;
    const info = this.store.saveCoinState(this.activeSymbol, {
      reason,
      sessionNumber: this.sessionNumber,
      eventSeq: this.encoder.seq,
      episodeSeq: this.episodes.seq,
      auditStats: this.encoder.auditStats,
      motifs: this.registry.serialize(),
      baseModels: this.baseModels.serialize()
    });
    this.lastSaveInfo = { ...info, reason };
    return this.lastSaveInfo;
  }

  setBackgroundIncubation(enabled) {
    this.backgroundIncubation = !!enabled;
    if (this.backgroundIncubation && this.pausedForIdle && this.activeSymbol) {
      this.selectCoin(this.activeSymbol);
    } else if (!this.backgroundIncubation) {
      this._checkSessionIdlePolicy();
    }
    return this.backgroundIncubation;
  }

  registerClientHeartbeat(clientId) {
    if (clientId) this.clientLeases.set(String(clientId), Date.now());
    if (this.pausedForIdle && this.activeSymbol) {
      this.selectCoin(this.activeSymbol);
    }
  }

  releaseClientSession(clientId) {
    if (clientId) this.clientLeases.delete(String(clientId));
    this._checkSessionIdlePolicy();
  }

  _hasActiveClientConnections() {
    if (this.activeStreams.size > 0) return true;
    const now = Date.now();
    for (const [cid, ts] of this.clientLeases.entries()) {
      if (now - ts <= this.sessionLeaseTimeoutMs) return true;
      this.clientLeases.delete(cid);
    }
    return false;
  }

  _checkSessionIdlePolicy() {
    if (!this.activeSymbol || this.pausedForIdle || this.backgroundIncubation) return;
    if (!this._hasActiveClientConnections()) {
      // Background incubation is OFF and client lease expired -> pause live feed & save memory cleanly!
      this.pausedForIdle = true;
      this._closeCurrentCoinSession('idle_session_timeout');
      this.emit('paused_idle', { symbol: this.activeSymbol, ts: Date.now() });
    }
  }

  getSummaryPayload() {
    const feedSnap = this.feeds.getSnapshot();
    const integrity = this.pausedForIdle
      ? { ok: false, label: 'IDLE_PAUSED', liveVenues: 0 }
      : feedSnap.integrity;

    const decisionCard = this.quarantine
      ? this.quarantine.getDecisionCardState(integrity)
      : { badge: 'VERİ KOPUK', silent: true, headline: 'Motor başlatılmadı.', explanation: '' };

    const allMotifs = this.registry ? Array.from(this.registry.motifs.values()) : [];
    const lifecycleCounts = { DISCOVERY: 0, INCUBATION: 0, VALIDATION: 0, GRADUATED: 0, QUARANTINED: 0, RETIRED: 0 };
    for (const m of allMotifs) lifecycleCounts[m.lifecycleState] = (lifecycleCounts[m.lifecycleState] || 0) + 1;

    return {
      serverTime: Date.now(),
      activeSymbol: this.activeSymbol,
      supportedSymbols: SUPPORTED_SYMBOLS,
      sessionNumber: this.sessionNumber,
      pausedForIdle: this.pausedForIdle,
      backgroundIncubation: this.backgroundIncubation,
      connectedClients: this.activeStreams.size + this.clientLeases.size,
      market: this.liveTelemetry,
      feeds: feedSnap,
      decisionCard,
      lifecycleCounts,
      recentEvents: this.encoder ? this.encoder.events.slice(-35) : [],
      compressionStats: this.encoder ? this.encoder.getCompressionSummary() : null,
      openEpisodesCount: this.episodes ? this.episodes.openEpisodes.length : 0,
      resolvedEpisodesCount: this.episodes ? this.episodes.resolvedEpisodes.length : 0,
      baseModelAudit: this.baseModels ? this.baseModels.getAuditSummary() : null,
      memoryStatus: {
        lastSaveInfo: this.lastSaveInfo,
        lastRecoveryReport: this.lastRecoveryReport,
        storedCoins: this.store.listStoredCoins()
      }
    };
  }

  getMotifsPayload() {
    if (!this.registry) return { symbol: this.activeSymbol, motifs: [] };
    const list = Array.from(this.registry.motifs.values()).map((m) => {
      const pos10 = m.horizonLedgers[10].pos.length;
      const neg10 = m.horizonLedgers[10].neg.length;
      const pos30 = m.horizonLedgers[30].pos.length;
      const neg30 = m.horizonLedgers[30].neg.length;
      const pos120 = m.horizonLedgers[120].pos.length;
      const neg120 = m.horizonLedgers[120].neg.length;
      return {
        motifId: m.motifId,
        lengthK: m.lengthK,
        suffixId: m.suffixId,
        isCrossVenue: m.isCrossVenue,
        venuesInvolved: m.venuesInvolved,
        lifecycleState: m.lifecycleState,
        stateReason: m.stateReason,
        warmingUp: m.warmingUp,
        warmUpRemaining: m.warmUpRemaining,
        prunedAsRedundant: m.prunedAsRedundant,
        totalTriggered: m.totalTriggered,
        brokenIntegrityCount: m.brokenIntegrityCount,
        firstSeenAt: m.firstSeenAt,
        lastSeenAt: m.lastSeenAt,
        clockSummary: m.clockSummary,
        resolvedCounts: {
          '10s': { pos: pos10, neg: neg10, total: pos10 + neg10 },
          '30s': { pos: pos30, neg: neg30, total: pos30 + neg30 },
          '120s': { pos: pos120, neg: neg120, total: pos120 + neg120 }
        },
        evaluation: m.evaluation
      };
    });

    const rankOrder = { GRADUATED: 0, VALIDATION: 1, INCUBATION: 2, QUARANTINED: 3, DISCOVERY: 4, RETIRED: 5 };
    list.sort((a, b) => (rankOrder[a.lifecycleState] - rankOrder[b.lifecycleState]) || (b.totalTriggered - a.totalTriggered));

    return {
      symbol: this.activeSymbol,
      dictionary: EVENT_DICTIONARY,
      motifs: list
    };
  }

  getEpisodesPayload(limit = 60) {
    if (!this.episodes) return { symbol: this.activeSymbol, open: [], recentResolved: [] };
    return {
      symbol: this.activeSymbol,
      open: this.episodes.openEpisodes.slice(-30).reverse(),
      recentResolved: this.episodes.resolvedEpisodes.slice(-limit).reverse()
    };
  }

  shutdown(reason = 'server_shutdown') {
    if (this.saveTimer) { clearInterval(this.saveTimer); this.saveTimer = null; }
    if (this.broadcastTimer) { clearInterval(this.broadcastTimer); this.broadcastTimer = null; }
    this._closeCurrentCoinSession(reason);
  }
}

module.exports = { MarketDnaRuntime };
