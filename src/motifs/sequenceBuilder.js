'use strict';

const { BARRIER_CODES } = require('../events/eventDictionary');

const MAX_SEQUENCE_SPAN_MS = 45_000;

/**
 * Extracts valid event sequences of lengths k in [2, 3, 4] ending at the newest event,
 * respecting <RESET> and <SESSION_GAP> boundaries and cross-venue clock uncertainty.
 */
function buildCandidateSequences(events, getCrossVenueUncertaintyMs) {
  if (!Array.isArray(events) || events.length < 2) return [];
  const newest = events[events.length - 1];
  if (!newest || BARRIER_CODES.has(newest.code) || newest.side === 0) return [];

  // Walk backwards collecting valid predecessor events without crossing barriers
  const brokenVenues = new Set();
  const chainRev = [newest];

  for (let i = events.length - 2; i >= 0 && chainRev.length < 4; i--) {
    const ev = events[i];
    if (!ev) continue;
    if (ev.code === '<SESSION_GAP>') break;
    if (ev.code === '<RESET>') {
      if (ev.venue === 'ALL' || ev.venue === 'SYSTEM' || ev.venue === 'BINANCE_FUT') {
        break; // Primary price reference or global stream broke -> hard stop!
      }
      brokenVenues.add(ev.venue);
      continue;
    }
    if (brokenVenues.has(ev.venue)) continue; // That venue reset between ev and newest!
    const lastAccepted = chainRev[chainRev.length - 1];
    if (lastAccepted.ts - ev.ts > MAX_SEQUENCE_SPAN_MS) break;
    // Prevent intra-packet JS loop-order artifacts: events from the exact same L2 snapshot (dt < 20ms on same venue)
    // are simultaneous observations of one frame, NOT a sequential A -> B transition!
    if (ev.venue === lastAccepted.venue && (lastAccepted.ts - ev.ts) < 20) continue;
    if (lastAccepted.ts < ev.ts) continue;
    chainRev.push(ev);
  }

  if (chainRev.length < 2) return [];
  const fullChain = chainRev.reverse(); // chronological order ending at newest
  const out = [];

  for (const k of [2, 3, 4]) {
    if (fullChain.length < k) continue;
    const sub = fullChain.slice(fullChain.length - k);
    const encoded = canonicalizeSequence(sub, getCrossVenueUncertaintyMs);
    if (encoded) out.push(encoded);
  }
  return out;
}

function formatStepToken(ev, anchorSide, anchorVenue, simultaneousWithPrev) {
  const pol = ev.side === anchorSide ? '+' : '-';
  const retestSuffix = ev.isRetest ? ':R' : '';
  const venuePrefix = ev.venue === anchorVenue ? '' : `${ev.venue}@`;
  const simTag = simultaneousWithPrev ? '~' : '';
  return `${simTag}${pol}${venuePrefix}${ev.code}${retestSuffix}`;
}

function canonicalizeSequence(seq, getCrossVenueUncertaintyMs) {
  const k = seq.length;
  const lastEv = seq[k - 1];
  const anchorSide = lastEv.side; // +1 or -1
  const anchorVenue = lastEv.venue;

  const clocks = [];
  const simultaneousFlags = new Array(k).fill(false);
  let hasUncertainCrossVenueOrder = false;

  for (let i = 1; i < k; i++) {
    const prev = seq[i - 1];
    const cur = seq[i];
    const dtMs = Math.max(0, cur.ts - prev.ts);
    const dTrades = Math.max(0, cur.cumTradeCount - prev.cumTradeCount);
    const dVolumeUsd = +Math.max(0, cur.cumVolumeUsd - prev.cumVolumeUsd).toFixed(2);

    let orderingConfidence = 'SAME_VENUE_EXACT';
    if (prev.venue !== cur.venue) {
      const uncMs = typeof getCrossVenueUncertaintyMs === 'function'
        ? getCrossVenueUncertaintyMs(prev.venue, cur.venue)
        : Infinity;
      if (!Number.isFinite(uncMs)) {
        orderingConfidence = 'UNCALIBRATED_CLOCK';
        hasUncertainCrossVenueOrder = true;
        simultaneousFlags[i] = true;
      } else if (dtMs <= uncMs) {
        orderingConfidence = 'SIMULTANEOUS_WITHIN_JITTER';
        hasUncertainCrossVenueOrder = true;
        simultaneousFlags[i] = true;
      } else {
        orderingConfidence = 'VERIFIED_CROSS_VENUE_ORDER';
      }
    }

    clocks.push({
      fromEventId: prev.id,
      toEventId: cur.id,
      fromVenue: prev.venue,
      toVenue: cur.venue,
      dtMs,
      dTrades,
      dVolumeUsd,
      orderingConfidence
    });
  }

  const tokens = seq.map((ev, idx) => formatStepToken(ev, anchorSide, anchorVenue, simultaneousFlags[idx]));
  const motifId = tokens.join(' -> ');

  let suffixId = null;
  if (k >= 3) {
    const suffixSeq = seq.slice(1);
    const suffixTokens = suffixSeq.map((ev, idx) => formatStepToken(ev, anchorSide, anchorVenue, idx === 0 ? false : simultaneousFlags[idx + 1]));
    suffixId = suffixTokens.join(' -> ');
  }

  const venuesInvolved = Array.from(new Set(seq.map((e) => e.venue)));

  return {
    motifId,
    lengthK: k,
    suffixId,
    anchorSide,
    anchorVenue,
    venuesInvolved,
    isCrossVenue: venuesInvolved.length > 1,
    hasUncertainCrossVenueOrder,
    events: seq,
    clocks,
    totalSpanMs: Math.max(0, lastEv.ts - seq[0].ts),
    totalTrades: Math.max(0, lastEv.cumTradeCount - seq[0].cumTradeCount),
    totalVolumeUsd: +Math.max(0, lastEv.cumVolumeUsd - seq[0].cumVolumeUsd).toFixed(2)
  };
}

module.exports = { buildCandidateSequences, canonicalizeSequence, MAX_SEQUENCE_SPAN_MS };
