(function () {
  'use strict';

  const PRESET_COINS = [
    'BTCUSDT', 'ETHUSDT', 'SOLUSDT',
    'XRPUSDT', 'BNBUSDT', 'DOGEUSDT',
    'SUIUSDT', 'AVAXUSDT', 'LINKUSDT'
  ];

  const VENUE_NAMES = {
    BINANCE_FUT: 'Binance USDT-M Vadeli',
    BINANCE_SPOT: 'Binance Spot',
    OKX_SWAP: 'OKX USDT Swap'
  };

  const clientId = 'ui_' + Math.random().toString(36).slice(2, 10) + '_' + Date.now();
  const state = {
    activeTab: 'market',
    labFilter: 'ALL',
    summary: null,
    motifsPayload: null,
    episodesPayload: null,
    sse: null,
    sseConnected: false
  };

  const $ = (id) => document.getElementById(id);

  function fmtPrice(v) {
    if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—';
    const n = Number(v);
    if (n >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (n >= 1) return n.toFixed(4);
    return n.toFixed(6);
  }

  function fmtNum(v, d = 2) {
    if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—';
    return Number(v).toFixed(d);
  }

  function fmtUsdCompact(v) {
    if (!Number.isFinite(Number(v))) return '—';
    const n = Number(v);
    if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
    if (n >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
    return '$' + n.toFixed(0);
  }

  function fmtClockTime(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
  }

  async function apiGet(relPath) {
    const res = await fetch(relPath, { cache: 'no-store' });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return res.json();
  }

  async function apiPost(relPath, body) {
    const res = await fetch(relPath, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {})
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return res.json();
  }

  function switchTab(tabName) {
    state.activeTab = tabName;
    document.querySelectorAll('.nav-item').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.tab === tabName);
    });
    document.querySelectorAll('.tab-pane').forEach((pane) => {
      pane.classList.toggle('active', pane.id === 'tab-' + tabName);
    });
    // Hero card stays visible on Piyasa & DNA for first-glance clarity
    $('heroDnaCard').classList.toggle('hidden', tabName !== 'market' && tabName !== 'dna');

    if (tabName === 'lab') refreshMotifs();
    if (tabName === 'dna') refreshEpisodes();
    renderAll();
  }

  async function refreshMotifs() {
    try {
      state.motifsPayload = await apiGet('api/motifs');
      renderLab();
    } catch (_) {}
  }

  async function refreshEpisodes() {
    try {
      state.episodesPayload = await apiGet('api/episodes');
      renderDnaTab();
    } catch (_) {}
  }

  function renderPriceSparkline(points) {
    const canvas = $('priceCanvas');
    if (!canvas) return;
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = Math.max(260, Math.floor(rect.width || 340));
    const h = 120;
    if (canvas.width !== w * dpr || canvas.height !== h * dpr) {
      canvas.width = w * dpr;
      canvas.height = h * dpr;
    }
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);

    if (!points || points.length < 2) {
      ctx.fillStyle = '#5d6a84';
      ctx.font = '12px sans-serif';
      ctx.fillText('Canlı fiyat serisi toplanıyor…', 14, h / 2);
      $('chartRangeLabel').textContent = 'Isınıyor';
      return;
    }

    let minP = Infinity;
    let maxP = -Infinity;
    for (const p of points) {
      if (p.mid < minP) minP = p.mid;
      if (p.mid > maxP) maxP = p.mid;
    }
    const span = Math.max(maxP - minP, minP * 0.0001, 1e-8);
    $('chartRangeLabel').textContent = fmtPrice(minP) + ' – ' + fmtPrice(maxP);

    const padX = 8;
    const padY = 12;
    const plotW = w - padX * 2;
    const plotH = h - padY * 2;
    const upMove = points[points.length - 1].mid >= points[0].mid;
    const stroke = upMove ? '#22d3ee' : '#f87171';

    ctx.beginPath();
    points.forEach((pt, idx) => {
      const x = padX + (idx / (points.length - 1)) * plotW;
      const y = padY + plotH - ((pt.mid - minP) / span) * plotH;
      if (idx === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 1.8;
    ctx.stroke();

    // Soft area fill
    ctx.lineTo(padX + plotW, padY + plotH);
    ctx.lineTo(padX, padY + plotH);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, padY, 0, padY + plotH);
    grad.addColorStop(0, upMove ? 'rgba(34,211,238,0.22)' : 'rgba(248,113,113,0.22)');
    grad.addColorStop(1, 'rgba(8,11,17,0.0)');
    ctx.fillStyle = grad;
    ctx.fill();
  }

  function renderHeaderAndHero() {
    const s = state.summary;
    if (!s) return;

    $('hdrSymbol').textContent = s.activeSymbol || '—';
    $('hdrSession').textContent = 'Oturum #' + (s.sessionNumber || 1);
    $('hdrMid').textContent = fmtPrice(s.market && s.market.mid);
    $('hdrSpread').textContent = s.market && s.market.spreadBps !== null
      ? fmtNum(s.market.spreadBps, 2) + ' bps'
      : '— bps';

    // Connection indicator — strictly bound to real feed + SSE health
    const venues = (s.feeds && s.feeds.venues) ? Object.values(s.feeds.venues) : [];
    const enabledVenues = venues.filter((v) => v.enabled !== false);
    const liveVenues = enabledVenues.filter((v) => v.status === 'LIVE' && v.synced);
    const feedBadge = $('hdrFeedBadge');
    const feedText = $('hdrFeedText');

    if (!state.sseConnected || s.pausedForIdle || liveVenues.length === 0) {
      feedBadge.dataset.state = 'offline';
      feedText.textContent = s.pausedForIdle ? 'Duraklatıldı' : 'Veri Kopuk';
    } else if (liveVenues.length < enabledVenues.length) {
      feedBadge.dataset.state = 'degraded';
      feedText.textContent = liveVenues.length + '/' + enabledVenues.length + ' Canlı';
    } else {
      feedBadge.dataset.state = 'live';
      feedText.textContent = liveVenues.length + '/' + enabledVenues.length + ' Canlı';
    }

    // Hero Decision Card — strictly bound to engine decisionCard
    const card = s.decisionCard || { badge: 'VERİ KOPUK', headline: 'Veri bekleniyor', explanation: '', silent: true };
    const badgeText = !state.sseConnected ? 'VERİ KOPUK' : card.badge;
    $('heroDnaCard').dataset.badge = badgeText;
    $('dnaStatusBadge').dataset.badge = badgeText;
    $('dnaStatusBadge').textContent = badgeText;
    $('dnaHeadline').textContent = card.headline || 'Gözlem sürüyor';
    $('dnaExplanation').textContent = card.explanation || '';

    const sigBox = $('activeSignalBox');
    if (badgeText === 'MEZUN MOTİF AKTİF' && card.activePrediction) {
      const ap = card.activePrediction;
      sigBox.classList.remove('hidden');
      $('sigProbUp').textContent = fmtNum(ap.probs.UP * 100, 1) + '%';
      $('sigProbFlat').textContent = fmtNum(ap.probs.FLAT * 100, 1) + '%';
      $('sigProbDown').textContent = fmtNum(ap.probs.DOWN * 100, 1) + '%';
      $('sigMetaLine').textContent =
        `Ufuk: ${ap.horizonSec}s · Yön: ${ap.polarity} · N_eff: ${ap.nEff} · BSS: +${ap.brierSkillScore} · FDR q: ${ap.fdrQValue}`;
      const invs = (ap.validatedInvalidators || []).map((i) => i.atom).join(', ');
      $('dnaInvalidationText').textContent = invs
        ? `Doğrulanmış iptal olayları: ${invs} veya <RESET>/<SESSION_GAP>.`
        : 'Akış kesintisi (<RESET> / <SESSION_GAP>) oluşursa tahmin anında iptal edilir.';
    } else {
      sigBox.classList.add('hidden');
      $('dnaInvalidationText').textContent =
        'Kanıtlanmış mezun motif aktif değil (Sessiz Mod). Açık gözlem epizotları <RESET> veya <SESSION_GAP> gelirse INTEGRITY_BROKEN olarak kapatılır.';
    }

    $('hdrRegimeTag').textContent = (s.market && s.market.regime) ? s.market.regime : '—';

    // Live Chain Ribbon
    const ribbon = $('liveChainRibbon');
    const evs = (s.recentEvents || []).slice(-10);
    if (!evs.length) {
      ribbon.innerHTML = '<span class="empty-muted">Henüz durum geçişi oluşmadı…</span>';
    } else {
      ribbon.innerHTML = evs.map((ev) => {
        const isBarrier = ev.code === '<RESET>' || ev.code === '<SESSION_GAP>';
        const cls = isBarrier ? 'barrier' : (ev.side === '+' ? 'pos' : (ev.side === '-' ? 'neg' : ''));
        const vShort = ev.venue === 'BINANCE_SPOT' ? 'SPOT:' : (ev.venue === 'OKX_SWAP' ? 'OKX:' : '');
        const ret = ev.retest ? ':R' : '';
        const prefix = (!isBarrier && (ev.side === '+' || ev.side === '-')) ? ev.side : '';
        return `<span class="atom-chip ${cls}" title="${fmtClockTime(ev.ts)}">${vShort}${prefix}${ev.code}${ret}</span>`;
      }).join('');
    }
  }

  function renderMarketTab() {
    const s = state.summary;
    if (!s) return;

    const m = s.market || {};
    renderPriceSparkline(m.recentPriceTrail || []);
    $('mktVol15s').textContent = fmtUsdCompact(m.recentTradesUsd15s || 0);
    $('mktBuyShare').textContent = (m.buyShare15s !== null && m.buyShare15s !== undefined)
      ? fmtNum(m.buyShare15s, 1) + '%'
      : '—';
    $('mktBasis').textContent = m.basisBps !== null && m.basisBps !== undefined
      ? fmtNum(m.basisBps, 2) + ' bps'
      : '—';
    $('mktFunding').textContent = m.fundingRate !== null && m.fundingRate !== undefined
      ? fmtNum(m.fundingRate * 100, 4) + '%'
      : '—';

    const venuesObj = (s.feeds && s.feeds.venues) || {};
    const enabledObj = (s.feeds && s.feeds.enabledSources) || {};
    const keys = Object.keys(venuesObj);
    const liveCnt = (s.feeds && s.feeds.integrity && s.feeds.integrity.liveVenues) || 0;
    $('venueCountLabel').textContent = `${liveCnt}/${keys.length} borsa senkron`;

    $('venueCardsList').innerHTML = keys.map((k) => {
      const v = venuesObj[k];
      const c = v.clock || {};
      const cnt = v.counters || {};
      const isEn = enabledObj[k] !== false;
      const stClass = !isEn ? 'off' : (v.status === 'LIVE' && v.synced ? 'live' : 'warn');
      const stLabel = !isEn ? 'KAPALI' : (v.status + (v.synced ? ' · SENKRON' : ' · BEKLİYOR'));
      const pkts = (cnt.bookUpdates || 0) + (cnt.depth20Snapshots || 0) + (cnt.bookTickers || 0) + (cnt.trades || 0);
      const extra = k === 'OKX_SWAP' && v.ctVal ? ` · ctVal=${v.ctVal}` : '';
      return `
        <div class="venue-item">
          <div class="venue-top">
            <span class="venue-name">${VENUE_NAMES[k] || k}</span>
            <span class="venue-status ${stClass}">${stLabel}</span>
          </div>
          <div class="venue-metrics">
            <span>Kademe: ${v.bookLevels ? (v.bookLevels.bids + '/' + v.bookLevels.asks) : '—'}</span>
            <span>Paket: ${pkts}</span>
            <span>İşlem: ${cnt.trades || 0}</span>
            <span>RTT: ${c.wsRttMs !== null && c.wsRttMs !== undefined ? fmtNum(c.wsRttMs, 0) + ' ms' : '—'}</span>
            <span>Jitter: ±${fmtNum(c.jitterMs || 0, 1)} ms</span>
            <span>Gap: ${cnt.sequenceGaps || 0}${extra}</span>
          </div>
        </div>
      `;
    }).join('');
  }

  function renderDnaTab() {
    const s = state.summary;
    if (!s) return;

    const openEps = (state.episodesPayload && state.episodesPayload.open) || [];
    $('openEpBadge').textContent = openEps.length + ' açık epizot';
    if (!openEps.length) {
      $('openEpisodesList').innerHTML = '<div class="empty-box">Şu anda sonuçlanması beklenen açık epizot bulunmuyor veya yenileniyor…</div>';
    } else {
      $('openEpisodesList').innerHTML = openEps.slice(0, 12).map((ep) => {
        const done = Object.entries(ep.horizons || {})
          .map(([h, v]) => `${h}s:${v && v.resolved ? v.label : '⏳'}`)
          .join(' · ');
        return `
          <div class="ev-row">
            <div class="ev-head">
              <span class="ev-code ${ep.polarity === 'pos' ? 'pos' : 'neg'}">${ep.motifId} (${ep.polarity})</span>
              <span class="ev-meta">${fmtClockTime(ep.t0)}</span>
            </div>
            <div class="ev-meta">Giriş Mid: ${fmtPrice(ep.entryMid)} · Rejim: ${ep.regime} · Ufuklar: ${done}</div>
          </div>
        `;
      }).join('');
    }

    const evs = (s.recentEvents || []).slice().reverse();
    $('eventCountBadge').textContent = ((s.compressionStats && s.compressionStats.cleanTransitionEvents) || evs.length) + ' kodlanan olay';
    if (!evs.length) {
      $('dnaEventStream').innerHTML = '<div class="empty-box">Henüz olay kaydı yok.</div>';
    } else {
      $('dnaEventStream').innerHTML = evs.map((ev) => {
        const isBarrier = ev.code === '<RESET>' || ev.code === '<SESSION_GAP>';
        const cls = isBarrier ? 'barrier' : (ev.side === '+' ? 'pos' : (ev.side === '-' ? 'neg' : ''));
        const metricsStr = Object.entries(ev.metrics || {})
          .map(([mk, mv]) => `${mk}=${mv}`)
          .join(' · ');
        const clocksStr = ev.clocks
          ? `Δt=${ev.clocks.dtMs}ms · Δİşlem=${ev.clocks.dTrades} · ΔHacim=${fmtUsdCompact(ev.clocks.dVolUsd)}`
          : '';
        return `
          <div class="ev-row">
            <div class="ev-head">
              <span class="ev-code ${cls}">[${ev.venue}] ${ev.side && ev.side !== '0' ? ev.side : ''}${ev.code}${ev.retest ? ' (RETEST)' : ''}</span>
              <span class="ev-meta">${fmtClockTime(ev.ts)}</span>
            </div>
            <div class="ev-meta">${metricsStr || 'Bariyer kaydı'} ${clocksStr ? ' | ' + clocksStr : ''}</div>
          </div>
        `;
      }).join('');
    }
  }

  function renderLab() {
    const s = state.summary;
    const lc = (s && s.lifecycleCounts) || {};
    const total = (lc.DISCOVERY || 0) + (lc.INCUBATION || 0) + (lc.VALIDATION || 0) +
                  (lc.GRADUATED || 0) + (lc.QUARANTINED || 0) + (lc.RETIRED || 0);

    $('fnAll').textContent = total;
    $('fnDisc').textContent = lc.DISCOVERY || 0;
    $('fnInc').textContent = lc.INCUBATION || 0;
    $('fnVal').textContent = lc.VALIDATION || 0;
    $('fnGrad').textContent = lc.GRADUATED || 0;
    $('fnQuar').textContent = (lc.QUARANTINED || 0) + (lc.RETIRED || 0);

    const mp = state.motifsPayload;
    if (!mp) return;

    if (mp.dictionary) {
      $('dictEntries').innerHTML = Object.values(mp.dictionary).map((d) => `
        <div class="ev-row">
          <div class="ev-head">
            <span class="ev-code">${d.code}</span>
            <span class="ev-meta">${d.category}</span>
          </div>
          <div class="ev-meta">${d.description} — Formül: ${d.formula}</div>
        </div>
      `).join('');
    }

    let list = mp.motifs || [];
    if (state.labFilter !== 'ALL') {
      if (state.labFilter === 'QUARANTINED') {
        list = list.filter((m) => ['QUARANTINED', 'RETIRED'].includes(m.lifecycleState) || m.prunedAsRedundant);
      } else {
        list = list.filter((m) => m.lifecycleState === state.labFilter);
      }
    }

    $('labMotifCount').textContent = list.length + ' motif gösteriliyor';
    if (!list.length) {
      $('motifCardsList').innerHTML = '<div class="empty-box">Bu filtrede henüz motif bulunmuyor.</div>';
      return;
    }

    $('motifCardsList').innerHTML = list.slice(0, 50).map((m) => {
      const rc10 = (m.resolvedCounts && m.resolvedCounts['10s']) || { pos: 0, neg: 0 };
      const bc = m.evaluation && m.evaluation.bestCell;
      const evalLine = bc
        ? `En iyi hücre (${bc.polarity}/${bc.horizonSec}s): N=${bc.nTotal}, N_eff=${bc.nEff}, BSS=${bc.brierSkillScore}, FDR q=${bc.fdrQValue ?? '—'}`
        : (m.stateReason || 'Örneklem birikiyor');
      const warmTag = m.warmingUp ? ` · ISINIYOR (${m.warmUpRemaining} kaldı)` : '';
      const clk = m.clockSummary || {};
      return `
        <div class="motif-card">
          <div class="motif-id">${m.motifId}</div>
          <div class="motif-sub">
            <span>Durum: <strong>${m.lifecycleState}${warmTag}</strong></span>
            <span>Tetik: ${m.totalTriggered || 0} (10s +:${rc10.pos} / -:${rc10.neg})</span>
            <span>Ort. Δt: ${fmtNum(clk.meanSpanMs, 0)} ms</span>
            <span>Ort. İşlem: ${fmtNum(clk.meanTradeCount, 1)}</span>
          </div>
          <div class="ev-meta" style="margin-top:5px;">${evalLine}</div>
        </div>
      `;
    }).join('');
  }

  function renderMemoryTab() {
    const s = state.summary;
    if (!s) return;
    const mem = s.memoryStatus || {};
    const rec = mem.lastRecoveryReport || {};
    const save = mem.lastSaveInfo || {};

    const rows = [
      ['Aktif Coin', s.activeSymbol],
      ['Oturum Numarası', '#' + (s.sessionNumber || 1)],
      ['Son Yükleme Kaynağı', rec.loadedFrom || 'FRESH_COIN'],
      ['Bayatlama Durumu', rec.stalenessLevel || 'FRESH'],
      ['Bozuk Satır Kurtarma', (rec.corruptJsonlLinesSkipped || 0) + ' satır atlandı'],
      ['Son Atomik Kayıt SHA-256', save.checksum ? save.checksum.slice(0, 16) + '…' : 'Henüz yazılmadı'],
      ['Arka Plan Kuluçkası', s.backgroundIncubation ? 'AÇIK' : 'KAPALI']
    ];
    $('memoryStatusGrid').innerHTML = rows.map(([k, v]) =>
      `<div class="kv-row"><span>${k}</span><strong>${v}</strong></div>`
    ).join('');

    const coins = mem.storedCoins || [];
    $('storedCoinsBar').innerHTML = coins.length
      ? coins.map((c) => `
          <button class="mem-chip ${c.symbol === s.activeSymbol ? 'active' : ''}" data-switch-coin="${c.symbol}" type="button">
            ${c.symbol} (${c.motifCount} motif · Oturum #${c.sessionNumber})
          </button>
        `).join('')
      : '<span class="empty-muted">Henüz diske yazılmış coin yok.</span>';

    // Base models table
    const bm = s.baseModelAudit || {};
    $('leakageBadge').textContent =
      `Sızıntı İhlali: ${bm.leakageViolationsDetected || 0} / Kontrol: ${bm.leakageChecksPassed || 0}`;

    const byH = bm.horizons || bm.byHorizon || {};
    $('baseModelTables').innerHTML = [10, 30, 120].map((h) => {
      const hd = byH[h];
      if (!hd || !hd.scoreboard) return '';
      const trs = Object.entries(hd.scoreboard).map(([mId, sc]) => `
        <tr>
          <td>${mId === hd.activeLeader ? '★ ' + mId : mId}</td>
          <td>${sc.weightedN}</td>
          <td>${sc.meanBrier !== null ? sc.meanBrier : 'Isınıyor'}</td>
        </tr>
      `).join('');
      return `
        <div class="bm-box">
          <div class="bm-title">
            <span>Ufuk: ${h} saniye</span>
            <span>Lider: ${hd.activeLeader}</span>
          </div>
          <table class="bm-table">
            <thead><tr><th>Baz Model</th><th>Ağırlıklı N</th><th>Prequential Brier</th></tr></thead>
            <tbody>${trs}</tbody>
          </table>
        </div>
      `;
    }).join('');

    // Compression stats
    const cs = s.compressionStats || {};
    const compRows = [
      ['L2 Emir Defteri Paketleri', cs.l2BookPacketsProcessed || 0],
      ['İşlem (Trade) Paketleri', cs.tradePacketsProcessed || 0],
      ['Temiz Durum Geçiş Olayları', cs.cleanTransitionEvents || 0],
      ['Korunan Gerçek Retestler (:R)', cs.retestEventsPreserved || 0],
      ['L2 Paket / Olay Sıkıştırması', cs.l2PacketsPerEvent ? cs.l2PacketsPerEvent + 'x' : '—'],
      ['500ms Tekrar Baskılama Oranı', (cs.naive500msRedundantPct || 0) + '%']
    ];
    $('compressionAuditList').innerHTML = compRows.map(([k, v]) =>
      `<div class="kv-row"><span>${k}</span><strong>${v}</strong></div>`
    ).join('');
  }

  function renderSettingsTab() {
    const s = state.summary;
    if (!s) return;

    $('coinSelectorGrid').innerHTML = PRESET_COINS.map((sym) => `
      <button class="coin-choice ${sym === s.activeSymbol ? 'active' : ''}" data-switch-coin="${sym}" type="button">
        ${sym}
      </button>
    `).join('');

    $('bgIncubationToggle').checked = Boolean(s.backgroundIncubation);
    const enabled = (s.feeds && s.feeds.enabledSources) || {};
    document.querySelectorAll('[data-source]').forEach((chk) => {
      const src = chk.dataset.source;
      if (enabled[src] !== undefined) chk.checked = Boolean(enabled[src]);
    });
  }

  function renderAll() {
    renderHeaderAndHero();
    if (state.activeTab === 'market') renderMarketTab();
    if (state.activeTab === 'dna') renderDnaTab();
    if (state.activeTab === 'lab') renderLab();
    if (state.activeTab === 'memory') renderMemoryTab();
    if (state.activeTab === 'settings') renderSettingsTab();
  }

  async function selectCoin(symbol) {
    const clean = String(symbol || '').trim().toUpperCase();
    if (!clean) return;
    try {
      state.summary = await apiPost('api/coin', { symbol: clean });
      renderAll();
      if (state.activeTab === 'lab') refreshMotifs();
      if (state.activeTab === 'dna') refreshEpisodes();
    } catch (err) {
      alert('Coin değiştirilemedi: ' + err.message);
    }
  }

  function connectSse() {
    if (state.sse) {
      try { state.sse.close(); } catch (_) {}
    }
    const es = new EventSource('api/stream?clientId=' + encodeURIComponent(clientId));
    state.sse = es;

    es.onopen = () => {
      state.sseConnected = true;
      renderHeaderAndHero();
    };

    es.addEventListener('state_tick', (e) => {
      state.sseConnected = true;
      try {
        state.summary = JSON.parse(e.data);
        renderAll();
      } catch (_) {}
    });

    es.addEventListener('dna_event', (e) => {
      try {
        const ev = JSON.parse(e.data);
        if (state.summary && Array.isArray(state.summary.recentEvents)) {
          state.summary.recentEvents.push(ev);
          if (state.summary.recentEvents.length > 40) state.summary.recentEvents.shift();
          renderHeaderAndHero();
          if (state.activeTab === 'dna') renderDnaTab();
        }
      } catch (_) {}
    });

    es.onerror = () => {
      state.sseConnected = false;
      renderHeaderAndHero();
    };
  }

  function bindEvents() {
    // Bottom nav
    document.querySelectorAll('.nav-item').forEach((btn) => {
      btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });

    // Quick coin button -> jump to Ayarlar coin selector
    $('quickCoinBtn').addEventListener('click', () => switchTab('settings'));

    // Delegated coin switch clicks
    document.body.addEventListener('click', (e) => {
      const coinBtn = e.target.closest('[data-switch-coin]');
      if (coinBtn) {
        selectCoin(coinBtn.dataset.switchCoin);
      }
    });

    // Custom coin form
    $('customCoinForm').addEventListener('submit', (e) => {
      e.preventDefault();
      const val = $('customCoinInput').value.trim().toUpperCase();
      if (val) {
        selectCoin(val);
        $('customCoinInput').value = '';
      }
    });

    // Lab filter buttons
    document.querySelectorAll('.funnel-pill').forEach((pill) => {
      pill.addEventListener('click', () => {
        state.labFilter = pill.dataset.filter;
        document.querySelectorAll('.funnel-pill').forEach((p) => p.classList.toggle('active', p === pill));
        renderLab();
      });
    });

    // Toggle dictionary
    $('toggleDictBtn').addEventListener('click', () => {
      $('dictPanel').classList.toggle('hidden');
    });

    // Manual disk flush
    $('manualFlushBtn').addEventListener('click', async () => {
      try {
        await apiPost('api/flush', {});
        state.summary = await apiGet('api/state?clientId=' + encodeURIComponent(clientId));
        renderMemoryTab();
      } catch (_) {}
    });

    // Background incubation toggle
    $('bgIncubationToggle').addEventListener('change', async (e) => {
      try {
        state.summary = await apiPost('api/settings', { backgroundIncubation: e.target.checked });
        renderAll();
      } catch (_) {}
    });

    // Venue source toggles
    document.querySelectorAll('[data-source]').forEach((chk) => {
      chk.addEventListener('change', async () => {
        const sources = {};
        document.querySelectorAll('[data-source]').forEach((c) => {
          sources[c.dataset.source] = c.checked;
        });
        try {
          state.summary = await apiPost('api/settings', { sources });
          renderAll();
        } catch (_) {}
      });
    });

    // Periodic refresh of Lab/Episodes if open + heartbeat lease
    setInterval(() => {
      apiPost('api/heartbeat', { clientId }).catch(() => {});
      if (state.activeTab === 'lab') refreshMotifs();
      if (state.activeTab === 'dna') refreshEpisodes();
    }, 5000);

    // Explicit pagehide session release (complemented by server-side heartbeat lease timeout!)
    window.addEventListener('pagehide', () => {
      if (navigator.sendBeacon) {
        const blob = new Blob([JSON.stringify({ clientId })], { type: 'application/json' });
        navigator.sendBeacon('api/session/end', blob);
      }
    });
  }

  async function init() {
    bindEvents();
    try {
      state.summary = await apiGet('api/state?clientId=' + encodeURIComponent(clientId));
      renderAll();
    } catch (_) {}
    connectSse();
    refreshMotifs();
    refreshEpisodes();
  }

  init();
})();
