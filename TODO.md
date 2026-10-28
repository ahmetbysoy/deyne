# MARKET DNA V1 — UYGULAMA VE DENETİM PLANI (`TODO.md`)

**Çalışma Alanı:** `/home/user/dna-lab/`  
**Korunan Referans Dosya (Dokunulmayacak):** `/home/user/uploads/orderbook-mobile (5).html`  
**Temel İlke:** Sıfır sahte veri, sıfır doğrulanmamış mezuniyet iddiası, tam izlenebilir istatistiksel denetim. Yeterli ileriye dönük kanıt yoksa sistem dürüstçe `GÖZLEMDE` veya `KULUÇKADA` kalır.

Durum Etiketleri: `DONE` | `PARTIAL` | `BLOCKED` | `TODO`

---

## FAZ 0 — Mevcut Çalışma Alanı Denetimi [Durum: `DONE`]

- [x] **[DONE]** Mevcut dosyaları ve mimariyi incele (`btc_single_coin_audit.js`, `btc_events_trace.jsonl`, `btc_audit_summary.json`, `live_experiment*.js`, `live_audit_precision.*`, `orderbook-mobile (5).html`).
- [x] **[DONE]** Canlı denetim kodunu satır satır incele (`btc_single_coin_audit.js` ve önceki prototipler).
- [x] **[DONE]** Ham olay kayıtlarının (`btc_events_trace.jsonl`) yapısını, zaman damgalarını ve fiyat davranışını doğrula.
- [x] **[DONE]** Mevcut deney iddialarını gerçek hesaplarla ve ham verilerle karşılaştır (`25,8x` vs `5,11x` payda farkı, 35 sn `flatPct=%100` örneklem yetersizliği, `N_eff=2,45`).
- [x] **[DONE]** Eksikleri, metodolojik hataları ve mühendislik risklerini raporla (`@trade` içinde `p="0.00" / X!="MARKET"` paketleri, OKX `ctVal` çarpan eksikliği, saat farkı ölçümü eksikliği, eksik olay atomları).
  - **Kanıt:** `audits/phase0_workspace_audit.md`

---

## FAZ 1 — Canlı Veri Katmanı [Durum: `DONE`]

- [x] **[DONE]** Sıfır dış bağımlılıklı, RFC-6455 uyumlu TLS WebSocket istemcisi (`src/lib/wsClient.js` — gerçek ping/pong RTT gecikme ölçümü, HTTP durum kodu yakalama, kontrollü yeniden bağlanma).
- [x] **[DONE]** Binance USDT-M Futures adaptörü (`src/adapters/binanceFuturesAdapter.js` — `/stream`: `bookTicker`, `depth20@100ms`, `depth@100ms`, `trade` [`X==="MARKET" && p>0` filtreli] + `/market/stream`: `markPrice@1s` [`d.E` zaman damgası düzeltmeli], `forceOrder`, `aggTrade` doğrulaması).
- [x] **[DONE]** Binance Spot adaptörü (`src/adapters/binanceSpotAdapter.js` — `data-stream.binance.vision/stream` + REST `/api/v3/depth` snapshot `U/u` senkronizasyonu).
- [x] **[DONE]** OKX USDT Perpetual Swap adaptörü (`src/adapters/okxSwapAdapter.js` — `ws.okx.com:443` `books` + `trades`, dinamik `ctVal` sözleşme büyüklüğü doğrulaması: BTC=0.01, ETH=0.1).
- [x] **[DONE]** Mesaj doğrulama, borsa saati farkı (`restClockSkewMs`) ve gecikme belirsizliği (`jitterStdMs`, `crossVenueUncertaintyMs`) ölçümü (`src/lib/clockTracker.js`).
- [x] **[DONE]** Bağlantı kopması, sıra boşluğu (sequence gap) algılama ve otomatik yeniden senkronizasyon.
- [x] **[DONE]** Tek coin abonelik yöneticisi (`src/adapters/feedCoordinator.js` — coin değişiminde sıfır sızıntılı geçiş).
  - **Kanıt:** `tests/verify_phase1_feeds.js` & `audits/phase1_feeds_evidence.json`

---

## FAZ 2 — Olay Kodlayıcı [Durum: `DONE`]

- [x] **[DONE]** Gözlemsel Atomik Olay Sözlüğü (`src/events/eventDictionary.js` — `WALL_SPAWN`, `WALL_TEST`, `WALL_HOLD`, `WALL_PULL`, `WALL_BREAK`, `FLOW_BURST`, `FLOW_EXHAUST`, `BOOK_TILT`, `MICRO_FLIP`, `VOID_OPEN`, `SWEEP_REJECT`, `<RESET>`, `<SESSION_GAP>`). Sıfır yön önyargısı; `WALL_PULL` yalnızca likidite çekilmesi gözlemi.
- [x] **[DONE]** Durum geçiş motoru (`src/events/eventEncoder.js` — tekrarlayan eşik kekelemelerinin filtrelenmesi + başlangıç 4 sn akış ısınma koruması).
- [x] **[DONE]** Açık histerezis bantları (giriş eşiği $\neq$ sıfırlanma eşiği) ve olay yaşam döngüsü (başlangıç/bitiş).
- [x] **[DONE]** Anlamlı yeniden test (`retestCount`, `isRetest`) olaylarının korunması.
- [x] **[DONE]** Veri boşluğu sınırları (`<RESET>`, `<SESSION_GAP>`) ve veri güvenilirlik durumu (`integrity`).
- [x] **[DONE]** Tam izlenebilir olay kayıtları (zaman damgaları, kaynak borsa, coin, ham metrik kanıtı).
  - **Kanıt:** `tests/verify_phase2_encoder.js` & `audits/phase2_encoder_evidence.json`

---

## FAZ 3 — DNA Motif Motoru [Durum: `DONE`]

- [x] **[DONE]** Olay dizilimi çıkarıcı (`src/motifs/sequenceBuilder.js` — 2..4 uzunlukta kanonik dizilimler, aynı L2 paketi döngü-sırası artefakt filtresi [`dt >= 20ms`], üçlü saat $\Delta t, \Delta N, \Delta V$, kopukluk sınırlarını asla aşmama, borsalar arası gecikme belirsizliği kontrolü).
- [x] **[DONE]** Epizot yöneticisi (`src/motifs/episodeManager.js` — 500 ms tekrar üretmeyen epizodik tetikleme, tam epizot şeması, kopmada `INTEGRITY_BROKEN`).
- [x] **[DONE]** Örtüşen sonuç pencerelerinin (`concurrentEpisodeIds`) kaydı ve eşzamanlılık ağırlık analizi.
- [x] **[DONE]** Motif keşfi ve kayıt defteri (`src/motifs/motifRegistry.js` — ortak yapı + ayrı yön/polarite izleme `pos`/`neg`).
- [x] **[DONE]** Kısa alt dizilim (suffix) karşılaştırması (Jensen-Shannon bilgi kazancı ve yön farkı).
- [x] **[DONE]** Ek bilgi taşımayan uzun dizilimlerin budanması (`runSuffixPruning` $\to$ `RETIRED`).
  - **Kanıt:** `tests/verify_phase3_motifs.js` & `audits/phase3_motifs_evidence.json`

---

## FAZ 4 — Öğrenme ve Doğrulama [Durum: `DONE`]

- [x] **[DONE]** Üç durumlu sonuç etiketleme (`UP`, `FLAT`, `DOWN`) — spread, volatilite ve işlem maliyeti tabanlı açık `FLAT` tanımı (`src/learning/outcomeLabeler.js`).
- [x] **[DONE]** Çoklu tahmin ufukları (`10s`, `30s`, `120s`) ve ufuk bazlı bağımsız değerlendirme.
- [x] **[DONE]** Dört aday baz modelin (`fixed_train`, `short_ewma`, `long_ewma`, `decoupled_regime`) sıfır sızıntılı prequential yarışması (`src/learning/baseModels.js` — önceden kazanan ilan edilmez, çalışma zamanı sızıntı kilidi aktif).
- [x] **[DONE]** Etkin örneklem (`N_eff`) modülü (`src/learning/effectiveSample.js` — örtüşme ağırlığı $N_{\text{uniq}}$, Kish $N_{\text{Kish}}$ ve seri korelasyon $N_{\text{eff,AR}}$ ayrı hesaplanır; $N<8$ küçük örneklem sınırı test edildi).
- [x] **[DONE]** Kronolojik ileriye dönük doğrulama (walk-forward), kalibrasyon (Dirichlet / Multinomial Brier) ve Benjamini-Hochberg FDR yanlış keşif kontrolü (`src/learning/quarantineEngine.js`).
- [x] **[DONE]** 6 aşamalı yaşam döngüsü (`DISCOVERY`, `INCUBATION`, `VALIDATION`, `GRADUATED`, `QUARANTINED`, `RETIRED`) + yapısal geçersizleşme doğrulayıcısı (erken yakalama vs. doğru sinyali erken kesme maliyeti).
  - **Kanıt:** `tests/verify_phase4_learning.js` & `audits/phase4_learning_evidence.json`

---

## FAZ 5 — Kalıcı Hafıza (B+) [Durum: `DONE`]

- [x] **[DONE]** Coin bazlı izole disk kayıt yapısı (`src/storage/coinMemoryStore.js` — `data/coins/<SYMBOL>/`, atomik `tmp + fsync + backup + rename` durum dosyası + SHA-256 doğrulama + 2 MB parçalı append-only JSONL arşivleri).
- [x] **[DONE]** Uygulama yeniden başladığında ve coin geri seçildiğinde önceki hafızadan devam etme (`src/engine/marketDnaRuntime.js`).
- [x] **[DONE]** Oturumlar arası `<SESSION_GAP>` enjeksiyonu ve açık kalan kararsız tahminlerin `INTEGRITY_BROKEN` olarak iptali.
- [x] **[DONE]** Bozuk/yarım satırlı kayıtlardan kontrollü kurtarma (`BACKUP_FALLBACK` + yarım JSONL satır onarımı) ve şema sürümlendirme (`schemaVersion: 1`).
- [x] **[DONE]** Eski motifler için bayatlama (`staleness`) değerlendirmesi ve yeni oturumda yeniden ısınma (`warmingUp`) doğrulaması.
  - **Kanıt:** `tests/verify_phase5_storage.js` & `audits/phase5_storage_evidence.json`

---

## FAZ 6 — Canlı API ve Mobil Panel [Durum: `DONE`]

- [x] **[DONE]** Backend–frontend gerçek zamanlı iletişim katmanı (`src/server.js` — `0.0.0.0:3030` bağlama, REST API + gerçek zamanlı SSE akışı, oturum kira/zaman aşımı yönetimi).
- [x] **[DONE]** Mobil öncelikli, ayrıştırılmış frontend (`public/index.html`, `public/styles.css`, `public/app.js`).
- [x] **[DONE]** 5 sekmeli alt navigasyon (**Piyasa**, **DNA**, **Laboratuvar**, **Hafıza**, **Ayarlar**) ve gerçek motor durumlarına bağlı ana DNA kartı (`GÖZLEMDE`, `MOTİF KEŞFEDİLDİ`, `KULUÇKADA`, `DOĞRULANIYOR`, `MEZUN MOTİF AKTİF`, `YAPISAL GEÇERSİZLİK`, `VERİ KOPUK`).
- [x] **[DONE]** Coin seçici (temiz geçiş + disk kaydı) ve `Seçili Coini Arka Planda İzlemeye Devam Et` anahtarı.
  - **Kanıt:** `tests/verify_phase6_api_ui.js` & `audits/phase6_api_ui_evidence.json` + canlı önizleme sunucusu (`0.0.0.0:3030`)

---

## FAZ 7 — Uçtan Uca Canlı Denetim [Durum: `DONE`]

- [x] **[DONE]** Gerçek `BTCUSDT` çoklu kaynak bağlantı testi (`BINANCE_FUT` + `BINANCE_SPOT` + `OKX_SWAP` eşzamanlı canlı senkron).
- [x] **[DONE]** Coin değiştirme (`BTCUSDT` → `ETHUSDT` → `BTCUSDT`) izolasyon ve hafıza koruma testi (`ETHUSDT` `mid ≈ 2,343.6`, `OKX ctVal = 0.1`, sıfır veri karışması).
- [x] **[DONE]** WebSocket kopma ve `<RESET>` sınır testi (zorunlu soket kapatmada `<RESET>` enjeksiyonu, açık epizotların `INTEGRITY_BROKEN` iptali ve otomatik `LIVE` kurtarma).
- [x] **[DONE]** Sunucu yeniden başlatma ve `<SESSION_GAP>` hafıza devamlılığı testi (soğuk yeniden başlatmada `Session #3`, `PRIMARY` SHA-256 diskten yükleme, `<SESSION_GAP>` enjeksiyonu ve 215 motifin korunması).
- [x] **[DONE]** Veri sızıntısı (data leakage) denetimi (`3.093` çalışma zamanı nedensel zaman damgası kontrolü, `0` sızıntı ihlali).
- [x] **[DONE]** İleri doğrulama ve baz model karşılaştırma raporu (10 sn ufkunda `fixed_train: 0.6540`, 30 sn ufkunda `long_ewma: 0.6675` prequential Brier liderliği; kısa koşuda dürüst `GRADUATED: 0`).
- [x] **[DONE]** Nihai canlı denetim ve bilinen sınırlamalar raporu (`tests/verify_phase7_e2e.js`, `audits/phase7_e2e_evidence.json`, `audits/FINAL_AUDIT_REPORT.md`).
