'use strict';

const { EventEmitter } = require('events');
const { HORIZONS_SEC, classifyThreeStateOutcome } = require('../learning/outcomeLabeler');
const { BARRIER_CODES } = require('../events/eventDictionary');

const CANDIDATE_INVALIDATION_CODES = new Set(['WALL_PULL', 'WALL_BREAK', 'FLOW_BURST', 'MICRO_FLIP', 'BOOK_TILT']);

class EpisodeManager extends EventEmitter {
  constructor(symbol, options = {}) {
    super();
    this.symbol = String(symbol).toUpperCase();
    this.seq = 0;
    this.minSameMotifGapMs = options.minSameMotifGapMs || 10_000;
    this.lastTriggeredAt = new Map(); // `${motifId}:${anchorSide}` -> ts
    this.openEpisodes = [];
    this.resolvedEpisodes = [];
    this.maxInMemoryResolved = options.maxInMemoryResolved || 2000;
    this.latestMid = 0;
    this.latestMidTs = 0;
    this.latestVolRate = 1.5;
  }

  restoreState(savedMeta) {
    if (!savedMeta) return;
    if (Number.isFinite(savedMeta.seq)) this.seq = savedMeta.seq;
    if (Array.isArray(savedMeta.recentResolved)) {
      this.resolvedEpisodes = savedMeta.recentResolved.slice(-this.maxInMemoryResolved);
    }
  }

  onPriceUpdate(mid, ts, volRateBpsPerSec) {
    if (!(mid > 0)) return;
    this.latestMid = mid;
    this.latestMidTs = ts || Date.now();
    if (Number.isFinite(volRateBpsPerSec) && volRateBpsPerSec > 0) {
      this.latestVolRate = volRateBpsPerSec;
    }
    this._resolveMaturedHorizons(this.latestMid, this.latestMidTs);
  }

  /**
   * Called on <SESSION_GAP> or primary price venue <RESET>.
   * Immediately invalidates all unresolved horizon outcomes so broken-stream windows NEVER enter training!
   */
  invalidateOpenHorizons(reason, ts = Date.now()) {
    const affected = [];
    for (const ep of this.openEpisodes) {
      let brokeAny = false;
      for (const h of HORIZONS_SEC) {
        const slot = ep.horizons[h];
        if (slot && slot.status === 'PENDING') {
          slot.status = 'INTEGRITY_BROKEN';
          slot.brokenReason = reason;
          slot.brokenAt = ts;
          brokeAny = true;
        }
      }
      if (brokeAny) {
        ep.integrityStatus = 'INTEGRITY_BROKEN';
        affected.push(ep.episodeId);
      }
      ep.fullyClosed = true;
      this._archiveEpisode(ep);
    }
    this.openEpisodes = [];
    if (affected.length) {
      this.emit('episodes_invalidated', { symbol: this.symbol, reason, ts, count: affected.length, episodeIds: affected });
    }
    return affected.length;
  }

  /**
   * Observes subsequent atomic events to record candidate structural invalidation occurrences
   * (without dogmatically cancelling outcomes — both raw outcome and post-event outcome are tracked for validation!).
   */
  onSubsequentEvent(ev) {
    if (!ev || BARRIER_CODES.has(ev.code) || !CANDIDATE_INVALIDATION_CODES.has(ev.code)) return;
    if (!(ev.mid > 0)) return;

    for (const ep of this.openEpisodes) {
      if (ev.ts <= ep.t0) continue;
      // Candidate invalidating signals relative to anchorSide:
      //  - SAME side WALL_PULL or WALL_BREAK (loss of supporting liquidity)
      //  - OPPOSING side FLOW_BURST, MICRO_FLIP, BOOK_TILT, WALL_SPAWN
      const isSameLoss = (ev.side === ep.anchorSide) && (ev.code === 'WALL_PULL' || ev.code === 'WALL_BREAK');
      const isOppPush = (ev.side === -ep.anchorSide) && (ev.code === 'FLOW_BURST' || ev.code === 'MICRO_FLIP' || ev.code === 'BOOK_TILT');
      if (!isSameLoss && !isOppPush) continue;

      const candidateTag = `${isSameLoss ? 'SAME' : 'OPP'}:${ev.code}`;
      if (!ep.invalidationObservations[candidateTag]) {
        ep.invalidationObservations[candidateTag] = {
          tag: candidateTag,
          eventId: ev.id,
          venue: ev.venue,
          ts: ev.ts,
          leadElapsedMs: ev.ts - ep.t0,
          midAtSignal: ev.mid,
          moveBpsAtSignal: +(((ev.mid - ep.refMid) / ep.refMid) * 1e4).toFixed(4)
        };
        this.emit('candidate_invalidation', { episodeId: ep.episodeId, motifId: ep.motifId, observation: ep.invalidationObservations[candidateTag] });
      }
    }
  }

  /**
   * Opens a discrete episode for a newly completed candidate sequence if not inside cooldown.
   */
  tryOpenEpisode(candidateSeq, contextSnap = {}) {
    const lastEv = candidateSeq.events[candidateSeq.events.length - 1];
    const refMid = lastEv.mid || this.latestMid;
    if (!(refMid > 0)) return null;

    const now = lastEv.ts;
    const dedupKey = `${candidateSeq.motifId}|${candidateSeq.anchorSide}`;
    if (this.lastTriggeredAt.has(dedupKey) && now - this.lastTriggeredAt.get(dedupKey) < this.minSameMotifGapMs) {
      return null;
    }
    this.lastTriggeredAt.set(dedupKey, now);

    const episodeId = `ep_${this.symbol}_${++this.seq}`;
    const spreadBpsAtEntry = lastEv.spreadBps || contextSnap.spreadBps || 0.5;
    const volRateBpsPerSec = contextSnap.volRateBpsPerSec || this.latestVolRate || 1.5;

    // Identify currently open episodes whose 30s outcome window overlaps [t0, t0 + 30s]
    const concurrentIds = [];
    for (const other of this.openEpisodes) {
      if (other.t0 + 30_000 > now) {
        concurrentIds.push(other.episodeId);
        if (!other.concurrentEpisodeIds.includes(episodeId)) {
          other.concurrentEpisodeIds.push(episodeId);
        }
      }
    }

    const horizons = {};
    for (const h of HORIZONS_SEC) {
      horizons[h] = {
        horizonSec: h,
        dueAt: now + h * 1000,
        status: 'PENDING', // PENDING | RESOLVED | INTEGRITY_BROKEN
        exitMid: null,
        resolvedAt: null,
        moveBps: null,
        anchorSignedMoveBps: null,
        deadZoneBps: null,
        outcomeClass: null, // 'UP' | 'FLAT' | 'DOWN'
        concurrencyWeight: 1.0,
        baseModelPredsAtEntry: contextSnap.baseModelPreds ? contextSnap.baseModelPreds[h] || null : null
      };
    }

    const ep = {
      episodeId,
      motifId: candidateSeq.motifId,
      lengthK: candidateSeq.lengthK,
      suffixId: candidateSeq.suffixId,
      symbol: this.symbol,
      venuesInvolved: candidateSeq.venuesInvolved,
      isCrossVenue: candidateSeq.isCrossVenue,
      hasUncertainCrossVenueOrder: candidateSeq.hasUncertainCrossVenueOrder,
      t0: now,
      refMid,
      spreadBpsAtEntry,
      volRateBpsPerSec,
      anchorSide: candidateSeq.anchorSide,
      regime: lastEv.regime || contextSnap.regime || 'NORMAL',
      integrityStatus: 'OK',
      eventChain: candidateSeq.events.map((e) => ({
        id: e.id,
        venue: e.venue,
        code: e.code,
        side: e.side,
        isRetest: e.isRetest,
        ts: e.ts,
        mid: e.mid
      })),
      clocks: candidateSeq.clocks,
      totalSpanMs: candidateSeq.totalSpanMs,
      totalTrades: candidateSeq.totalTrades,
      totalVolumeUsd: candidateSeq.totalVolumeUsd,
      concurrentEpisodeIds: concurrentIds,
      invalidationObservations: {},
      horizons,
      fullyClosed: false
    };

    this.openEpisodes.push(ep);
    this.emit('episode_opened', ep);
    return ep;
  }

  _resolveMaturedHorizons(currentMid, now) {
    if (!this.openEpisodes.length) return;
    const stillOpen = [];

    for (const ep of this.openEpisodes) {
      let pendingLeft = 0;

      for (const h of HORIZONS_SEC) {
        const slot = ep.horizons[h];
        if (slot.status !== 'PENDING') continue;
        if (now < slot.dueAt) {
          pendingLeft += 1;
          continue;
        }

        const classified = classifyThreeStateOutcome(
          ep.refMid,
          currentMid,
          h,
          ep.spreadBpsAtEntry,
          ep.volRateBpsPerSec
        );

        // Concurrency uniqueness weight based on overlapping episodes active during [t0, t0+H]
        const overlapCount = 1 + ep.concurrentEpisodeIds.length;
        const concurrencyWeight = +(1 / overlapCount).toFixed(4);

        slot.status = 'RESOLVED';
        slot.exitMid = currentMid;
        slot.resolvedAt = now;
        slot.moveBps = classified.moveBps;
        slot.anchorSignedMoveBps = +(classified.moveBps * ep.anchorSide).toFixed(4);
        slot.deadZoneBps = classified.deadZoneBps;
        slot.outcomeClass = classified.label;
        slot.concurrencyWeight = concurrencyWeight;

        this.emit('horizon_resolved', {
          episode: ep,
          horizonSec: h,
          slot
        });
      }

      if (pendingLeft > 0) {
        stillOpen.push(ep);
      } else {
        ep.fullyClosed = true;
        this._archiveEpisode(ep);
      }
    }

    this.openEpisodes = stillOpen;
  }

  _archiveEpisode(ep) {
    this.resolvedEpisodes.push(ep);
    if (this.resolvedEpisodes.length > this.maxInMemoryResolved) {
      this.resolvedEpisodes.splice(0, this.resolvedEpisodes.length - this.maxInMemoryResolved);
    }
    this.emit('episode_archived', ep);
  }
}

module.exports = { EpisodeManager, CANDIDATE_INVALIDATION_CODES };
