'use strict';

const HORIZONS_SEC = Object.freeze([10, 30, 120]);
const MIN_EXEC_COST_BPS = 1.5;

/**
 * Computes transparent, spread + volatility + execution-cost adjusted FLAT dead-zone (in bps)
 * for horizon H (seconds).
 *
 * Formula:
 *   deadZoneBps(H) = max(MIN_EXEC_COST_BPS, 1.8 * spreadBps, 0.35 * volRateBpsPerSec * sqrt(H))
 */
function computeDeadZoneBps(horizonSec, spreadBps, volRateBpsPerSec) {
  const spreadFloor = 1.8 * Math.max(0.05, Number(spreadBps) || 0.5);
  const volFloor = 0.35 * Math.max(0.2, Number(volRateBpsPerSec) || 1.0) * Math.sqrt(horizonSec);
  return +Math.max(MIN_EXEC_COST_BPS, spreadFloor, volFloor).toFixed(3);
}

/**
 * Classifies realized price displacement into UP, FLAT, or DOWN.
 */
function classifyThreeStateOutcome(entryMid, exitMid, horizonSec, spreadBps, volRateBpsPerSec) {
  if (!(entryMid > 0 && exitMid > 0)) {
    return { valid: false, label: 'INVALID', moveBps: 0, deadZoneBps: 0 };
  }
  const moveBps = +(((exitMid - entryMid) / entryMid) * 1e4).toFixed(4);
  const deadZoneBps = computeDeadZoneBps(horizonSec, spreadBps, volRateBpsPerSec);
  let label = 'FLAT';
  if (moveBps > deadZoneBps) label = 'UP';
  else if (moveBps < -deadZoneBps) label = 'DOWN';

  return {
    valid: true,
    label, // 'UP' | 'FLAT' | 'DOWN'
    moveBps,
    deadZoneBps
  };
}

module.exports = {
  HORIZONS_SEC,
  MIN_EXEC_COST_BPS,
  computeDeadZoneBps,
  classifyThreeStateOutcome
};
