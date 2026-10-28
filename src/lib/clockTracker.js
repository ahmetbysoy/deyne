'use strict';

/**
 * Tracks venue clock offset + feed delivery delay (localRxMs - venueEventMs)
 * and network jitter standard deviation (ms).
 * Used to determine whether two cross-venue events can be ordered with statistical confidence.
 */
class ClockTracker {
  constructor(venue) {
    this.venue = venue;
    this.samples = 0;
    this.delayMeanMs = 0;
    this.delayVarMs = 25;
    this.minDelayMs = Infinity;
    this.rttMs = null;
    this.restClockSkewMs = null; // localNow - serverTime from REST handshake
    this.lastEventTs = 0;
    this.lastLocalRxTs = 0;
    this.outOfOrderCount = 0;
  }

  recordRestSkew(localBefore, localAfter, serverTimeMs) {
    if (!Number.isFinite(serverTimeMs) || serverTimeMs <= 0) return;
    const rtt = Math.max(1, localAfter - localBefore);
    const estLocalAtServer = localBefore + rtt / 2;
    this.restClockSkewMs = +(estLocalAtServer - serverTimeMs).toFixed(2);
    if (this.rttMs === null) this.rttMs = rtt;
  }

  recordWsRtt(rttMs) {
    if (Number.isFinite(rttMs) && rttMs >= 0) {
      this.rttMs = this.rttMs === null ? rttMs : +(0.7 * this.rttMs + 0.3 * rttMs).toFixed(2);
    }
  }

  observePacket(venueEventMs, localRxMs) {
    this.lastLocalRxTs = localRxMs;
    if (!Number.isFinite(venueEventMs) || venueEventMs <= 0) return;
    if (this.lastEventTs > 0 && venueEventMs < this.lastEventTs) {
      this.outOfOrderCount += 1;
    } else {
      this.lastEventTs = venueEventMs;
    }

    const rawLag = localRxMs - venueEventMs;
    if (Math.abs(rawLag) > 60_000) return; // guard bogus timestamps

    this.samples += 1;
    if (rawLag < this.minDelayMs) this.minDelayMs = rawLag;
    if (this.samples === 1) {
      this.delayMeanMs = rawLag;
      this.delayVarMs = 16;
    } else {
      const diff = rawLag - this.delayMeanMs;
      this.delayMeanMs += 0.05 * diff;
      this.delayVarMs = 0.95 * (this.delayVarMs + 0.05 * diff * diff);
    }
  }

  get jitterMs() {
    return +Math.sqrt(Math.max(1, this.delayVarMs)).toFixed(2);
  }

  /**
   * Estimates comparable UTC event timestamp adjusted for measured clock skew.
   */
  normalizeTimestamp(venueEventMs, localRxMs) {
    if (Number.isFinite(venueEventMs) && venueEventMs > 0) {
      const skew = this.restClockSkewMs !== null ? this.restClockSkewMs : 0;
      return venueEventMs + skew;
    }
    return localRxMs;
  }

  snapshot() {
    return {
      venue: this.venue,
      samples: this.samples,
      rttMs: this.rttMs,
      restClockSkewMs: this.restClockSkewMs,
      meanDeliveryLagMs: this.samples ? +this.delayMeanMs.toFixed(2) : null,
      minDeliveryLagMs: Number.isFinite(this.minDelayMs) ? +this.minDelayMs.toFixed(2) : null,
      jitterStdMs: this.samples ? this.jitterMs : null,
      outOfOrderCount: this.outOfOrderCount,
      lastLocalRxTs: this.lastLocalRxTs
    };
  }
}

module.exports = { ClockTracker };
