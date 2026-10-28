'use strict';

/**
 * MARKET DNA V1 — Observational Atomic Event Dictionary
 *
 * CRITICAL PRINCIPLE:
 * Every event represents a measurable microstructure state transition ONLY.
 * Zero bullish/bearish predictive opinion is hardcoded into any event atom.
 * Specifically, WALL_PULL represents observed liquidity reduction without matching executions —
 * it does NOT assert spoofing or manipulation intent.
 */

const EVENT_DICTIONARY = {
  WALL_SPAWN: {
    code: 'WALL_SPAWN',
    category: 'WALL',
    labelTr: 'Duvar Oluşumu',
    mathCondition: 'q_level >= 4.0 * median(q_top40) AND notional_level / sum(notional_band) >= 0.18 AND |p_level - mid| / mid <= 3.0 * bandUnit',
    liveDataFields: ['book.bids', 'book.asks', 'book.bestBid', 'book.bestAsk'],
    sideSemantics: { '+1': 'Alış (Bid) kademesinde yoğun limit likiditesi belirdi', '-1': 'Satış (Ask) kademesinde yoğun limit likiditesi belirdi' },
    startCondition: 'Kademe büyüklüğü eşiği ilk kez aştığında ve o tarafta aktif izlenen eşdeğer duvar yokken başlar.',
    endCondition: 'Duvar WALL_PULL, WALL_BREAK veya fiyattan > 4.5 * bandUnit uzaklaşma ile sonlanır.',
    hysteresis: { enterMultiple: 4.0, retainMultiple: 2.2, bandShareEnter: 0.18, rearmRequiresClearMs: 1200 }
  },

  WALL_TEST: {
    code: 'WALL_TEST',
    category: 'WALL',
    labelTr: 'Duvar Testi',
    mathCondition: 'Aktif duvar varken |mid - p_wall| / mid <= 0.85 * bandUnit VE duvar etrafında karşı yönlü işlem akışı > 0.05 * peakNotional',
    liveDataFields: ['book.bestBid', 'book.bestAsk', 'trade.price', 'trade.notionalUsd', 'trade.side'],
    sideSemantics: { '+1': 'Alış duvarı fiyat ve satış işlemleriyle test ediliyor', '-1': 'Satış duvarı fiyat ve alım işlemleriyle test ediliyor' },
    startCondition: 'Fiyat aktif duvarın test bandına girip ilk karşı yönlü işlemleri aldığında tetiklenir.',
    endCondition: 'Fiyat > 1.6 * bandUnit uzaklaştığında sıfırlanır; tekrar yaklaşırsa isRetest=true ile yeniden tetiklenir.',
    hysteresis: { enterDistUnit: 0.85, resetDistUnit: 1.60, minOpposingRatio: 0.05 }
  },

  WALL_HOLD: {
    code: 'WALL_HOLD',
    category: 'WALL',
    labelTr: 'Duvar Tutunması',
    mathCondition: 'WALL_TEST sonrası 5 sn pencerede opposingHit >= 0.20 * peakNotional VE currentNotional >= 0.50 * peakNotional VE fiyat duvarı kırmadı',
    liveDataFields: ['book.bids', 'book.asks', 'trade.price', 'trade.notionalUsd', 'trade.side'],
    sideSemantics: { '+1': 'Alış duvarı karşı satış hacmini karşılayıp kademeyi korudu', '-1': 'Satış duvarı karşı alım hacmini karşılayıp kademeyi korudu' },
    startCondition: 'Karşı işlem hacmi %20 eşiğini aşmasına rağmen duvarın >= %50 büyüklükle yerinde kalmasıyla oluşur.',
    endCondition: 'Test penceresi kapandığında veya duvar kırıldığında/çekildiğinde biter.',
    hysteresis: { minOpposingHitFrac: 0.20, minSurvivingFrac: 0.50, retestResetDistUnit: 1.50 }
  },

  WALL_PULL: {
    code: 'WALL_PULL',
    category: 'WALL',
    labelTr: 'Likidite Çekilmesi',
    mathCondition: 'samples >= 3 olan yakın duvar (dist <= 2.5 * bandUnit) notional < 0.35 * peakNotional düzeyine düşerken, o kademedeki işlem hacmi < 0.15 * peakNotional',
    liveDataFields: ['book.bids', 'book.asks', 'trade.price', 'trade.notionalUsd'],
    sideSemantics: { '+1': 'Alış kademesindeki yoğun likidite işlem görmeden azaldı/çekildi', '-1': 'Satış kademesindeki yoğun likidite işlem görmeden azaldı/çekildi' },
    startCondition: 'Gözlenen duvar hacmi gerçekleşen işlemle açıklanamayacak biçimde %65+ düştüğünde anlık geçiş olarak kaydedilir.',
    endCondition: 'Anlık durum geçişidir; duvar kaydı kapatılır (manipülasyon varsayımı içermez).',
    hysteresis: { minObservedSamples: 3, maxTradedRatio: 0.15, dropBelowPeakRatio: 0.35 }
  },

  WALL_BREAK: {
    code: 'WALL_BREAK',
    category: 'WALL',
    labelTr: 'Duvar Kırılımı',
    mathCondition: 'Aktif duvar kademesi mid tarafından karşı yönde geçildi (bid için mid < p_wall, ask için mid > p_wall) VE karşı işlem hacmi >= 0.15 * peakNotional',
    liveDataFields: ['book.bestBid', 'book.bestAsk', 'trade.price', 'trade.notionalUsd', 'trade.side'],
    sideSemantics: { '+1': 'Alış duvarı kademesi satış baskısıyla aşağı geçildi', '-1': 'Satış duvarı kademesi alım baskısıyla yukarı geçildi' },
    startCondition: 'Mid fiyat duvar seviyesini karşı işlem eşliğinde aştığında oluşur.',
    endCondition: 'Anlık geçiş olayıdır; ilgili duvar izlemesi sonlanır.',
    hysteresis: { minTradedBreakRatio: 0.15 }
  },

  FLOW_BURST: {
    code: 'FLOW_BURST',
    category: 'FLOW',
    labelTr: 'Agresif Akış Patlaması',
    mathCondition: '2 sn işlem hacmi >= 2.2 * E[V_2s] (15 sn tabanına göre) VE işlem sayısı >= 3 VE tek yön payı >= 0.72',
    liveDataFields: ['trade.notionalUsd', 'trade.side', 'trade.ts'],
    sideSemantics: { '+1': 'Agresif alıcı (taker buy) hacim yoğunlaşması', '-1': 'Agresif satıcı (taker sell) hacim yoğunlaşması' },
    startCondition: 'Hacim çarpanı ve yön baskınlığı eşiği aşıldığında başlar.',
    endCondition: 'FLOW_EXHAUST gerçekleştiğinde veya akış taban seviyeye (< 0.85 * E[V_2s]) indiğinde biter.',
    hysteresis: { enterVolMultiple: 2.2, resetVolMultiple: 0.85, enterDirShare: 0.72, resetDirShare: 0.55 }
  },

  FLOW_EXHAUST: {
    code: 'FLOW_EXHAUST',
    category: 'FLOW',
    labelTr: 'Akış Sönümlenmesi',
    mathCondition: 'Aktif FLOW_BURST başlangıcından >= 700 ms sonra V_2s <= 0.32 * peakBurstVol2s düzeyine düşer',
    liveDataFields: ['trade.notionalUsd', 'trade.side', 'trade.ts'],
    sideSemantics: { '+1': 'Önceki agresif alım dalgası hız kesti', '-1': 'Önceki agresif satış dalgası hız kesti' },
    startCondition: 'Aktif bir FLOW_BURST sonrasında hacim hızı tepe değerinin %32 altına indiğinde tetiklenir.',
    endCondition: 'Burst durumunu 0 (nötr) konumuna sıfırlar.',
    hysteresis: { minBurstAgeMs: 700, dropRatioFromPeak: 0.32 }
  },

  BOOK_TILT: {
    code: 'BOOK_TILT',
    category: 'BOOK',
    labelTr: 'Emir Defteri Dengesizlik Geçişi',
    mathCondition: 'Üstel ağırlıklı yakın bant dengesizliği OBI_near = (B - A)/(B + A); |OBI_near| >= 0.36 giriş, |OBI_near| <= 0.14 nötr sıfırlama',
    liveDataFields: ['book.bids', 'book.asks', 'book.bestBid', 'book.bestAsk'],
    sideSemantics: { '+1': 'Yakın emir defteri likiditesi alış tarafına ağırlıklandı', '-1': 'Yakın emir defteri likiditesi satış tarafına ağırlıklandı' },
    startCondition: 'Nötr veya karşı durumdan |OBI_near| >= 0.36 bölgesine geçildiğinde üretilir.',
    endCondition: '|OBI_near| <= 0.14 bandına geri döndüğünde durum sıfırlanır (sonraki aynı yönlü giriş yeniden test sayılır).',
    hysteresis: { enterAbsObi: 0.36, exitAbsObi: 0.14 }
  },

  MICRO_FLIP: {
    code: 'MICRO_FLIP',
    category: 'BOOK',
    labelTr: 'Spread İçi Mikrofiyat Kayması',
    mathCondition: 'microSkew = (microPrice - mid) / (spread / 2); |microSkew| >= 0.58 giriş, |microSkew| <= 0.18 sıfırlama',
    liveDataFields: ['book.bestBid', 'book.bestAsk', 'book.bestBidQty', 'book.bestAskQty'],
    sideSemantics: { '+1': 'BBO kuyruk ağırlığı üst kademeye (ask yönüne) kaydı', '-1': 'BBO kuyruk ağırlığı alt kademeye (bid yönüne) kaydı' },
    startCondition: 'BBO kuyruk dengesizliği spread içi mikrofiyatı +-0.58 eşiğinin ötesine taşıdığında başlar.',
    endCondition: '|microSkew| <= 0.18 bölgesine indiğinde sıfırlanır.',
    hysteresis: { enterSkew: 0.58, exitSkew: 0.18, minCooldownMs: 800 }
  },

  VOID_OPEN: {
    code: 'VOID_OPEN',
    category: 'BOOK',
    labelTr: 'Likidite Boşluğu Açılması',
    mathCondition: 'İlk >= 3.2 * medianQty bariyerine olan mesafe (distBps) önceki ortalamasının >= 1.85 katına çıkar VE >= 2.5 * spreadBps olur',
    liveDataFields: ['book.bids', 'book.asks', 'book.bestBid', 'book.bestAsk'],
    sideSemantics: { '+1': 'Satış (üst) tarafında likidite inceldi / boşluk açıldı', '-1': 'Alış (alt) tarafında likidite inceldi / boşluk açıldı' },
    startCondition: 'İlgili yöndeki kademe yoğunluğu seyrekleşip bariyer mesafesi sıçradığında oluşur.',
    endCondition: 'Bariyer mesafesi <= 1.2 * taban ortalamasına daraldığında sıfırlanır.',
    hysteresis: { enterExpansionRatio: 1.85, exitExpansionRatio: 1.20 }
  },

  SWEEP_REJECT: {
    code: 'SWEEP_REJECT',
    category: 'REACTION',
    labelTr: 'Süpürme ve Hızlı Geri Çekilme',
    mathCondition: 'Son 3 sn içinde mid >= 1.5 * spreadBps (veya >= 0.8 bps) tek yöne uzanıp >= %60 oranında başlangıç seviyesine geri döner',
    liveDataFields: ['book.bestBid', 'book.bestAsk', 'trade.price', 'trade.notionalUsd'],
    sideSemantics: { '+1': 'Aşağı yönlü hızlı uzanma sonrası yukarı geri çekilme gözlendi', '-1': 'Yukarı yönlü hızlı uzanma sonrası aşağı geri çekilme gözlendi' },
    startCondition: 'Kısa süreli fiyat uzanması aynı pencere içinde %60+ geri alındığında kaydedilir.',
    endCondition: 'Anlık tepki geçişidir; yeni ekstrem oluşana kadar tekrar tetiklenmez.',
    hysteresis: { windowMs: 3000, retracementRatio: 0.60, cooldownMs: 3000 }
  },

  '<RESET>': {
    code: '<RESET>',
    category: 'CONTINUITY',
    labelTr: 'Veri Akışı Yeniden Senkronizasyon Sınırı',
    mathCondition: 'WebSocket yeniden bağlanması, L2 sıra numarası kopukluğu (pu/U/seqId gap) veya > 7 sn akış sessizliği',
    liveDataFields: ['adapter.gap', 'adapter.status'],
    sideSemantics: { '0': 'Veri sürekliliği kesildi; öncesi ve sonrası asla tek dizilimde birleştirilmez' },
    startCondition: 'Adaptör gap veya staleness sinyali ürettiğinde anında eklenir.',
    endCondition: 'Kesin dizilim bariyeridir.',
    hysteresis: { hardBarrier: true }
  },

  '<SESSION_GAP>': {
    code: '<SESSION_GAP>',
    category: 'CONTINUITY',
    labelTr: 'Oturum Arası Boşluk Sınırı',
    mathCondition: 'Sunucu yeniden başlatma, coin değiştirip geri dönme veya kalıcı hafızadan yükleme',
    liveDataFields: ['storage.sessionLoad', 'coordinator.coinSwitch'],
    sideSemantics: { '0': 'Farklı oturumlar arasında dizilim ve açık tahmin sınırı' },
    startCondition: 'Kalıcı coin hafızası yüklendiğinde veya oturum kapandığında eklenir.',
    endCondition: 'Kesin oturum bariyeridir; açık kararsız epizotları INTEGRITY_BROKEN yapar.',
    hysteresis: { hardBarrier: true }
  }
};

const VALID_EVENT_CODES = Object.freeze(Object.keys(EVENT_DICTIONARY));
const BARRIER_CODES = new Set(['<RESET>', '<SESSION_GAP>']);

module.exports = { EVENT_DICTIONARY, VALID_EVENT_CODES, BARRIER_CODES };
