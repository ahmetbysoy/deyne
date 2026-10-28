'use strict';

const { HORIZONS_SEC } = require('./outcomeLabeler');
const { computeEffectiveSampleMetrics } = require('./effectiveSample');
const { multinomialBrier3, normalize3 } = require('./baseModels');

/**
 * Exact one-sided binomial sign-test p-value: P(X >= k | n, p=0.5)
 * Used to test H0: Motif does NOT beat contemporaneous Base Model more than 50% of the time
 * on strictly out-of-sample non-overlapping episodes (distribution-free, safe for heavy tails).
 */
function oneSidedSignTestPValue(wins, total) {
  if (total <= 0 || wins <= 0) return 1.0;
  // Exact log-sum or direct sum for n <= 100, normal approx for n > 100
  if (total > 100) {
    const z = (wins - 0.5 * total - 0.5) / Math.sqrt(0.25 * total);
    // Abramowitz-Stegun upper tail approx
    const t = 1 / (1 + 0.2316419 * Math.abs(z));
    const d = 0.3989423 * Math.exp((-z * z) / 2);
    const prob = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    return z >= 0 ? Math.max(1e-9, prob) : 1 - prob;
  }
  let tail = 0;
  let logFactN = 0;
  for (let i = 2; i <= total; i++) logFactN += Math.log(i);
  const logHalfPowN = -total * Math.LN2;

  const logFact = (m) => {
    let s = 0;
    for (let i = 2; i <= m; i++) s += Math.log(i);
    return s;
  };

  for (let x = wins; x <= total; x++) {
    const logComb = logFactN - logFact(x) - logFact(total - x);
    tail += Math.exp(logComb + logHalfPowN);
  }
  return Math.min(1.0, Math.max(1e-9, tail));
}

/**
 * Benjamini-Hochberg False Discovery Rate (FDR) adjustment across M simultaneous motif*horizon hypotheses.
 */
function benjaminiHochbergAdjust(items) {
  // items: array of { id, pValue }
  const m = items.length;
  if (!m) return new Map();
  const sorted = [...items].sort((a, b) => a.pValue - b.pValue);
  const qValues = new Array(m);
  let prevQ = 1.0;
  for (let i = m - 1; i >= 0; i--) {
    const rank = i + 1;
    const rawQ = (sorted[i].pValue * m) / rank;
    prevQ = Math.min(prevQ, rawQ);
    qValues[i] = +Math.min(1.0, prevQ).toFixed(5);
  }
  const map = new Map();
  for (let i = 0; i < m; i++) map.set(sorted[i].id, qValues[i]);
  return map;
}

/**
 * Computes calibrated Dirichlet-smoothed 3-state forecast [P_DOWN, P_FLAT, P_UP]
 * from training slice records, shrunk conservatively toward contemporaneous baseProbs!
 */
function calibratedMotifProbs3(trainRecords, baseProbs, priorStrength = 6.0) {
  const counts = [
    priorStrength * baseProbs[0],
    priorStrength * baseProbs[1],
    priorStrength * baseProbs[2]
  ];
  for (const r of trainRecords) {
    const w = Math.max(0.05, Number(r.concurrencyWeight) || 1.0);
    if (r.outcomeClass === 'DOWN') counts[0] += w;
    else if (r.outcomeClass === 'FLAT') counts[1] += w;
    else if (r.outcomeClass === 'UP') counts[2] += w;
  }
  return normalize3(counts);
}

/**
 * Evaluates candidate structural invalidation events empirically on resolved episodes.
 */
function evaluateInvalidationCandidates(records, expectedDirSign, exitFrictionBps = 0.8) {
  if (!records || records.length < 4 || !expectedDirSign) {
    return { validatedTriggers: [], candidateStats: [] };
  }
  const byTag = new Map();
  for (const r of records) {
    if (!Array.isArray(r.invalidationObservations)) continue;
    for (const obs of r.invalidationObservations) {
      if (!byTag.has(obs.tag)) {
        byTag.set(obs.tag, {
          tag: obs.tag,
          triggeredCount: 0,
          earlyCaughtLosers: 0,
          falseCancelledWinners: 0,
          leadElapsedSumMs: 0,
          netGainBpsSum: 0
        });
      }
      const st = byTag.get(obs.tag);
      st.triggeredCount += 1;
      st.leadElapsedSumMs += Math.max(0, (r.resolvedAt - obs.ts));

      const holdSignedBps = r.moveBps * expectedDirSign;
      const exitSignedBps = (obs.moveBpsAtSignal * expectedDirSign) - exitFrictionBps;
      const diffBps = exitSignedBps - holdSignedBps;
      st.netGainBpsSum += diffBps;

      if (holdSignedBps < -r.deadZoneBps && diffBps > 0) st.earlyCaughtLosers += 1;
      if (holdSignedBps > r.deadZoneBps && exitSignedBps < holdSignedBps) st.falseCancelledWinners += 1;
    }
  }

  const candidateStats = [];
  const validatedTriggers = [];

  for (const st of byTag.values()) {
    const netBenefitPerTriggerBps = +(st.netGainBpsSum / st.triggeredCount).toFixed(3);
    const netBenefitPerAllEpisodesBps = +(st.netGainBpsSum / records.length).toFixed(3);
    const falseCancelRate = +(st.falseCancelledWinners / st.triggeredCount).toFixed(3);
    const earlyCatchRate = +(st.earlyCaughtLosers / st.triggeredCount).toFixed(3);
    const meanLeadMs = Math.round(st.leadElapsedSumMs / st.triggeredCount);

    const validated = st.triggeredCount >= 5 && netBenefitPerTriggerBps > 0.15 && falseCancelRate <= 0.35;
    const summary = {
      tag: st.tag,
      triggeredCount: st.triggeredCount,
      earlyCatchRate,
      falseCancelRate,
      meanLeadMs,
      netBenefitPerTriggerBps,
      netBenefitPerAllEpisodesBps,
      validated
    };
    candidateStats.push(summary);
    if (validated) validatedTriggers.push(summary);
  }

  candidateStats.sort((a, b) => b.netBenefitPerTriggerBps - a.netBenefitPerTriggerBps);
  return { validatedTriggers, candidateStats };
}

class QuarantineEngine {
  constructor(symbol, motifRegistry, baseModelSuite) {
    this.symbol = String(symbol).toUpperCase();
    this.registry = motifRegistry;
    this.baseModels = baseModelSuite;
    this.fdrAlpha = 0.10;
    // Variance-derived minimum statistical power requirements:
    // Under H0 (p=0.5 sign test), at least 8 independent OOS evaluations are needed even for 8/8 (p=0.0039)
    // to survive BH-FDR across multiple motifs; hence minTrainNeff=8, minValNeff=8 (total Neff >= 16).
    this.minTrainNeff = 8.0;
    this.minValNeff = 8.0;

    this.activeSignal = null; // Currently active graduated motif episode (if any)
    this.lastObservedEventInfo = null;
    this.lastEvaluationAt = 0;
  }

  /**
   * Re-evaluates all motifs in the registry using strict chronological split:
   *   First 50% of resolved episodes -> Training/Incubation calibration slice
   *   Second 50% of resolved episodes -> Strictly Unseen Walk-Forward Validation slice
   * Applies Benjamini-Hochberg FDR across ALL evaluated motif*polarity*horizon hypotheses!
   */
  evaluateAllMotifs() {
    this.lastEvaluationAt = Date.now();
    this.registry.runSuffixPruning(12);

    const hypotheses = [];
    const evalDrafts = new Map();

    for (const m of this.registry.motifs.values()) {
      if (m.prunedAsRedundant || m.lifecycleState === 'RETIRED') continue;

      const motifEval = {
        evaluatedAt: this.lastEvaluationAt,
        byPolarityAndHorizon: {},
        bestValidatedCandidate: null
      };

      for (const polarity of ['pos', 'neg']) {
        for (const h of HORIZONS_SEC) {
          const recs = m.horizonLedgers[h][polarity];
          const effAll = computeEffectiveSampleMetrics(recs);
          const key = `${polarity}_${h}s`;

          if (effAll.nEffFinal < this.minTrainNeff) {
            motifEval.byPolarityAndHorizon[key] = {
              polarity,
              horizonSec: h,
              stage: recs.length === 0 ? 'DISCOVERY' : 'INCUBATION',
              effAll,
              reason: `Kuluçka verisi toplanıyor (N_eff=${effAll.nEffFinal.toFixed(2)} / min ${this.minTrainNeff}).`
            };
            continue;
          }

          // Chronological split: strictly earlier records = Train, strictly later records = Out-of-Sample Walk-Forward!
          const sorted = [...recs].sort((a, b) => a.t0 - b.t0);
          const splitIdx = Math.max(4, Math.floor(sorted.length * 0.5));
          const trainSlice = sorted.slice(0, splitIdx);
          const valSlice = sorted.slice(splitIdx);

          // Assert zero temporal overlap between train resolution and validation entry (anti-leakage!)
          const maxTrainEntry = trainSlice[trainSlice.length - 1].t0;
          const cleanValSlice = valSlice.filter((r) => r.t0 > maxTrainEntry);

          const effTrain = computeEffectiveSampleMetrics(trainSlice);
          const effVal = computeEffectiveSampleMetrics(cleanValSlice);

          // Fit smoothed 3-state distribution ONLY on trainSlice
          const fallbackBase = this.baseModels.predictAll(this.lastEvaluationAt, 'NORMAL')[h].leaderProbs;
          const trainProbs3 = calibratedMotifProbs3(trainSlice, fallbackBase, 5.0);

          if (effVal.nEffFinal < this.minValNeff) {
            motifEval.byPolarityAndHorizon[key] = {
              polarity,
              horizonSec: h,
              stage: 'VALIDATION',
              effAll,
              effTrain,
              effVal,
              trainProbs3,
              reason: `İleri doğrulama (walk-forward) örneklemi bekleniyor (OOS N_eff=${effVal.nEffFinal.toFixed(2)} / min ${this.minValNeff}).`
            };
            continue;
          }

          // Score strictly on cleanValSlice against contemporaneous Base Model!
          let motifBrierSum = 0;
          let baseBrierSum = 0;
          let wSum = 0;
          let oosBeatsBaseWins = 0;
          let oosNonTies = 0;
          const regimeScores = {};

          for (const r of cleanValSlice) {
            const w = Math.max(0.05, Number(r.concurrencyWeight) || 1.0);
            const baseAtEntry = (r.baseModelPredsAtEntry && r.baseModelPredsAtEntry.leaderProbs)
              ? r.baseModelPredsAtEntry.leaderProbs
              : fallbackBase;
            const bMotif = multinomialBrier3(trainProbs3, r.outcomeClass);
            const bBase = multinomialBrier3(baseAtEntry, r.outcomeClass);

            motifBrierSum += w * bMotif;
            baseBrierSum += w * bBase;
            wSum += w;

            if (Math.abs(bBase - bMotif) > 1e-5) {
              oosNonTies += 1;
              if (bMotif < bBase) oosBeatsBaseWins += 1;
            }

            const reg = r.regime || 'NORMAL';
            if (!regimeScores[reg]) regimeScores[reg] = { n: 0, motifBrier: 0, baseBrier: 0 };
            regimeScores[reg].n += 1;
            regimeScores[reg].motifBrier += bMotif;
            regimeScores[reg].baseBrier += bBase;
          }

          const oosMotifBrier = motifBrierSum / Math.max(1e-9, wSum);
          const oosBaseBrier = baseBrierSum / Math.max(1e-9, wSum);
          const brierSkillScore = oosBaseBrier > 0 ? +((1 - oosMotifBrier / oosBaseBrier)).toFixed(4) : 0;

          // Effective independent sign-test count scaled conservatively by effVal / rawVal ratio
          const scaleRatio = cleanValSlice.length > 0 ? Math.min(1, effVal.nEffFinal / cleanValSlice.length) : 1;
          const effTrials = Math.max(1, Math.round(oosNonTies * scaleRatio));
          const effWins = Math.min(effTrials, Math.round(oosBeatsBaseWins * scaleRatio));
          const pValue = oneSidedSignTestPValue(effWins, effTrials);

          // Regime consistency check
          let regimeConsistent = true;
          for (const rs of Object.values(regimeScores)) {
            if (rs.n >= 4 && rs.motifBrier > rs.baseBrier * 1.10) {
              regimeConsistent = false;
            }
          }

          // Suffix information gain check for k>=3
          const suffixCheck = this.registry.compareWithSuffix(m.motifId, h);
          const suffixOk = suffixCheck.addsInformation !== false;

          // Determine expected directional lean from trainProbs3: [DOWN, FLAT, UP]
          const expectedDirSign = trainProbs3[2] > trainProbs3[0] + 0.05 ? 1
            : (trainProbs3[0] > trainProbs3[2] + 0.05 ? -1 : 0);
          const invalidationEval = evaluateInvalidationCandidates(cleanValSlice, expectedDirSign);

          const hypId = `${m.motifId}|${key}`;
          hypotheses.push({ id: hypId, pValue });

          motifEval.byPolarityAndHorizon[key] = {
            hypId,
            polarity,
            horizonSec: h,
            stage: 'VALIDATION_SCORED',
            effAll,
            effTrain,
            effVal,
            trainProbs3,
            oosMotifBrier: +oosMotifBrier.toFixed(4),
            oosBaseBrier: +oosBaseBrier.toFixed(4),
            brierSkillScore,
            rawPValue: +pValue.toFixed(5),
            fdrQValue: null, // populated after BH-FDR pass
            regimeConsistent,
            suffixCheck,
            suffixOk,
            invalidationEval
          };
        }
      }
      evalDrafts.set(m.motifId, motifEval);
    }

    // Apply Benjamini-Hochberg FDR across ALL evaluated motif*polarity*horizon tests!
    const qMap = benjaminiHochbergAdjust(hypotheses);

    for (const [motifId, motifEval] of evalDrafts.entries()) {
      const m = this.registry.motifs.get(motifId);
      let bestGrad = null;
      let hasValidationStage = false;
      let hasIncubationStage = false;
      let degradedPreviouslyGraduated = false;

      for (const cell of Object.values(motifEval.byPolarityAndHorizon)) {
        if (cell.stage === 'INCUBATION') hasIncubationStage = true;
        if (cell.stage === 'VALIDATION') hasValidationStage = true;
        if (cell.stage === 'VALIDATION_SCORED') {
          hasValidationStage = true;
          cell.fdrQValue = qMap.get(cell.hypId) ?? 1.0;

          const passesAllGates =
            cell.brierSkillScore > 0.02 &&
            cell.fdrQValue <= this.fdrAlpha &&
            cell.regimeConsistent &&
            cell.suffixOk;

          cell.passesAllGates = passesAllGates;
          if (passesAllGates) {
            if (!bestGrad || cell.brierSkillScore > bestGrad.brierSkillScore) {
              bestGrad = cell;
            }
          } else if (m.lifecycleState === 'GRADUATED' && cell.brierSkillScore < -0.02) {
            degradedPreviouslyGraduated = true;
          }
        }
      }

      motifEval.bestValidatedCandidate = bestGrad;
      m.evaluation = motifEval;

      if (bestGrad && !m.warmingUp) {
        m.lifecycleState = 'GRADUATED';
        m.stateReason = `Mezun (${bestGrad.horizonSec}s, BSS=${(bestGrad.brierSkillScore * 100).toFixed(1)}%, FDR q=${bestGrad.fdrQValue}).`;
      } else if (bestGrad && m.warmingUp) {
        m.lifecycleState = 'VALIDATION';
        m.stateReason = `Yeni oturum yeniden ısınma doğrulamasında (${m.warmUpRemaining} taze epizot kaldı).`;
      } else if (m.lifecycleState === 'GRADUATED' || degradedPreviouslyGraduated) {
        m.lifecycleState = 'QUARANTINED';
        m.stateReason = 'Performans / ileri doğrulama bozuldu — tahmin yetkisi karantinaya alındı.';
      } else if (hasValidationStage) {
        m.lifecycleState = 'VALIDATION';
        m.stateReason = 'İleriye dönük doğrulama (walk-forward & FDR kontrolü) sürüyor.';
      } else if (hasIncubationStage) {
        m.lifecycleState = 'INCUBATION';
        m.stateReason = 'Kuluçkada — bağımsız epizot ve baz model karşılaştırması toplanıyor.';
      } else {
        m.lifecycleState = 'DISCOVERY';
        m.stateReason = 'Keşfedildi — ilk sonuç pencerelerinin kapanması bekleniyor.';
      }
    }
  }

  /**
   * Called when a new episode opens in real time.
   * Only activates a live shadow prediction if the motif is genuinely GRADUATED on that polarity!
   */
  onNewEpisodeOpened(ep) {
    const m = this.registry.motifs.get(ep.motifId);
    if (!m) return;

    // Decrement warm-up counter if motif was reloaded from prior session
    if (m.warmingUp && m.warmUpRemaining > 0) {
      m.warmUpRemaining -= 1;
      if (m.warmUpRemaining <= 0) m.warmingUp = false;
    }

    this.lastObservedEventInfo = {
      motifId: ep.motifId,
      episodeId: ep.episodeId,
      lifecycleState: m.lifecycleState,
      stateReason: m.stateReason,
      ts: ep.t0
    };

    if (m.lifecycleState !== 'GRADUATED' || m.warmingUp || !m.evaluation) return;

    const polKey = ep.anchorSide > 0 ? 'pos' : 'neg';
    let bestCell = null;
    for (const h of HORIZONS_SEC) {
      const c = m.evaluation.byPolarityAndHorizon[`${polKey}_${h}s`];
      if (c && c.passesAllGates) {
        if (!bestCell || c.brierSkillScore > bestCell.brierSkillScore) bestCell = c;
      }
    }
    if (!bestCell) return;

    const probs = bestCell.trainProbs3; // [P_DOWN, P_FLAT, P_UP]
    let direction = 'FLAT';
    if (probs[2] > probs[0] && probs[2] > probs[1]) direction = 'UP';
    else if (probs[0] > probs[2] && probs[0] > probs[1]) direction = 'DOWN';

    this.activeSignal = {
      episodeId: ep.episodeId,
      motifId: ep.motifId,
      anchorSide: ep.anchorSide,
      horizonSec: bestCell.horizonSec,
      openedAt: ep.t0,
      expiresAt: ep.t0 + bestCell.horizonSec * 1000,
      refMid: ep.refMid,
      probs3: {
        DOWN: +(probs[0] * 100).toFixed(1),
        FLAT: +(probs[1] * 100).toFixed(1),
        UP: +(probs[2] * 100).toFixed(1)
      },
      direction,
      brierSkillScore: bestCell.brierSkillScore,
      fdrQValue: bestCell.fdrQValue,
      nEffOos: bestCell.effVal.nEffFinal,
      validatedInvalidators: bestCell.invalidationEval.validatedTriggers,
      candidateInvalidators: bestCell.invalidationEval.candidateStats.slice(0, 3),
      invalidated: false,
      invalidatedBy: null,
      invalidatedAt: null
    };
  }

  /**
   * Checks if an incoming observational event triggers a VALIDATED structural invalidation
   * on the currently active graduated signal, or if a stream gap broke data continuity.
   */
  onCandidateInvalidation({ episodeId, observation }) {
    if (!this.activeSignal || this.activeSignal.episodeId !== episodeId || this.activeSignal.invalidated) return;
    const isValidatedTrigger = this.activeSignal.validatedInvalidators.some((v) => v.tag === observation.tag);
    if (isValidatedTrigger) {
      this.activeSignal.invalidated = true;
      this.activeSignal.invalidatedBy = observation;
      this.activeSignal.invalidatedAt = observation.ts;
    }
  }

  onStreamDisruption(reason, ts = Date.now()) {
    if (this.activeSignal && !this.activeSignal.invalidated) {
      this.activeSignal.invalidated = true;
      this.activeSignal.invalidatedBy = { tag: '<RESET>', reason, ts };
      this.activeSignal.invalidatedAt = ts;
    }
  }

  /**
   * Produces the exact, truthful status card payload for the Mobile Lab UI.
   */
  getDecisionCardState(feedIntegrity, now = Date.now()) {
    if (!feedIntegrity || !feedIntegrity.ok) {
      return {
        badge: 'VERİ KOPUK',
        silent: true,
        headline: 'Canlı borsa akışı bekleniyor veya yeniden senkronize ediliyor.',
        explanation: 'Veri sürekliliği sağlanmadan olay dizilimi birleştirilmez ve yön iddiası üretilmez.',
        activeSignal: null,
        lastMotif: this.lastObservedEventInfo
      };
    }

    if (this.activeSignal) {
      if (this.activeSignal.invalidated && now - this.activeSignal.invalidatedAt < 12_000) {
        return {
          badge: 'YAPISAL GEÇERSİZLİK',
          silent: true,
          headline: `Aktif motif tezi yapısal olayla (${this.activeSignal.invalidatedBy.tag}) geri çekildi.`,
          explanation: 'Doğrulanmış geçersizlik olayı gözlendiği için tahmin süresi dolmadan iptal edildi.',
          activeSignal: this.activeSignal,
          lastMotif: this.lastObservedEventInfo
        };
      }
      if (!this.activeSignal.invalidated && now < this.activeSignal.expiresAt) {
        return {
          badge: 'MEZUN MOTİF AKTİF',
          silent: false,
          headline: `${this.activeSignal.motifId} (${this.activeSignal.horizonSec}s ufuk)`,
          explanation: `İleri doğrulamayı ve BH-FDR yanlış keşif testini geçmiş motif aktif (BSS +${(this.activeSignal.brierSkillScore * 100).toFixed(1)}%, q=${this.activeSignal.fdrQValue}).`,
          activeSignal: this.activeSignal,
          lastMotif: this.lastObservedEventInfo
        };
      }
      this.activeSignal = null;
    }

    const counts = { DISCOVERY: 0, INCUBATION: 0, VALIDATION: 0, GRADUATED: 0, QUARANTINED: 0, RETIRED: 0 };
    for (const m of this.registry.motifs.values()) {
      counts[m.lifecycleState] = (counts[m.lifecycleState] || 0) + 1;
    }

    if (this.lastObservedEventInfo && now - this.lastObservedEventInfo.ts < 8000) {
      const st = this.lastObservedEventInfo.lifecycleState;
      if (st === 'VALIDATION') {
        return {
          badge: 'DOĞRULANIYOR',
          silent: true,
          headline: `Motif ileri doğrulamada: ${this.lastObservedEventInfo.motifId}`,
          explanation: `${this.lastObservedEventInfo.stateReason} Henüz canlı tahmin yetkisi verilmedi; sistem suskun kalıyor.`,
          activeSignal: null,
          lifecycleCounts: counts,
          lastMotif: this.lastObservedEventInfo
        };
      }
      if (st === 'INCUBATION') {
        return {
          badge: 'KULUÇKADA',
          silent: true,
          headline: `Motif kuluçkada izleniyor: ${this.lastObservedEventInfo.motifId}`,
          explanation: `${this.lastObservedEventInfo.stateReason} Kanıt olgunlaşmadan yön tahmini üretilmez.`,
          activeSignal: null,
          lifecycleCounts: counts,
          lastMotif: this.lastObservedEventInfo
        };
      }
      if (st === 'DISCOVERY') {
        return {
          badge: 'MOTİF KEŞFEDİLDİ',
          silent: true,
          headline: `Yeni olay dizilimi kaydedildi: ${this.lastObservedEventInfo.motifId}`,
          explanation: 'Keşif aşamasında yön iddiası yoktur; 10s / 30s / 120s sonuç pencereleri izleniyor.',
          activeSignal: null,
          lifecycleCounts: counts,
          lastMotif: this.lastObservedEventInfo
        };
      }
    }

    return {
      badge: 'GÖZLEMDE',
      silent: true,
      headline: counts.GRADUATED > 0
        ? `Piyasa izleniyor — şu an tetiklenmiş mezun motif yok (${counts.GRADUATED} mezun motif hazırda).`
        : `Piyasa gözlemleniyor — henüz mezuniyet kanıtını tamamlayan motif yok.`,
      explanation: `Keşfedilen: ${counts.DISCOVERY} · Kuluçkada: ${counts.INCUBATION} · Doğrulamada: ${counts.VALIDATION} · Mezun: ${counts.GRADUATED}. Kanıt yoksa Market DNA susar.`,
      activeSignal: null,
      lifecycleCounts: counts,
      lastMotif: this.lastObservedEventInfo
    };
  }
}

module.exports = {
  QuarantineEngine,
  oneSidedSignTestPValue,
  benjaminiHochbergAdjust,
  calibratedMotifProbs3,
  evaluateInvalidationCandidates
};
