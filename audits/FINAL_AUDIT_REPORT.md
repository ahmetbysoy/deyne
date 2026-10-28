# MARKET DNA V1 — UÇTAN UCA CANLI DENETİM VE MÜHENDİSLİK RAPORU

**Tarih:** 2026-10-28  
**Çalışma Dizini:** `/home/user/dna-lab/`  
**Canlı Sunucu:** `http://0.0.0.0:3030` (`src/server.js`)  
**Referans Dosya Durumu:** `/home/user/uploads/orderbook-mobile (5).html` (3.466 satır — dokunulmadı) ve FAZ 0 prototip kayıtları (`btc_*`, `live_*`) eksiksiz korundu.

---

## 1. FAZ 0 — Mevcut Çalışma Alanı Denetim Bulguları (`audits/phase0_workspace_audit.md`)

1. **Ham Log Doğrulaması:** `btc_events_trace.jsonl` (21 satır, geçerli JSONL) ve `btc_audit_summary.json` incelendi. 35 saniyelik dar pencerede `mid ≈ 80,538.65` çevresinde tüm pencerelerin `%100 FLAT` kalması ve etkin örneklemin `nEff = 2.45` olması nedeniyle `ShortWindowEWMA = 0.1811` Brier skorunun **tek rejimli küçük örneklem artefaktı** olduğu kanıtlandı.
2. **Sıkıştırma Çelişkisi Çözümü:** `live_dna_report.json` içindeki `25.8x` oranının `7,344 L2 paketi / 285 olay = 25.77x`, `5.11x` oranının ise `1,457 aktif 500ms dilimi / 285 olay = 5.11x` olduğu saptandı. V1 motorunda bu iki metrik ayrıştırılarak ayrı ayrı raporlandı.
3. **Kritik Canlı Borsa Hatası Tespiti:** Binance Futures `@trade` kanalında `X !== "MARKET"` olan `p: "0.00"` uzlaşma kayıtlarının aktığı tespit edildi ve adaptör katmanında `price > 0 && (!d.X || d.X === 'MARKET')` filtresi zorunlu kılındı.

---

## 2. Modüler Mimari ve Sorumluluk Ayrımı

Harici npm paketi bağımlılığı olmadan (anlık görüntü kalıcılığını `%100` garanti etmek üzere saf Node.js `tls`/`http`/`crypto`/`fs` ile) modüler katmanlar kuruldu:

- **Bağlantı ve Saat Kalibrasyonu:** `src/lib/wsClient.js`, `src/lib/clockTracker.js`
- **Borsa Adaptörleri (Tek Coin Derin Odak):**
  - `src/adapters/binanceFuturesAdapter.js` (`/stream`: `bookTicker`, `depth20@100ms`, `depth@100ms`, `trade`; `/market/stream`: `markPrice@1s`, `forceOrder`)
  - `src/adapters/binanceSpotAdapter.js` (`depth@100ms` + `api.binance.com` REST snapshot `lastUpdateId` senkronu + `aggTrade`)
  - `src/adapters/okxSwapAdapter.js` (`books` 400-kademe `prevSeqId -> seqId` + `trades` + dinamik `ctVal` sözleşme çarpanı)
  - `src/adapters/feedCoordinator.js` (tek coin yaşam döngüsü, kaynak aç/kapat, borsalar arası saat belirsizliği $\pm\text{ms}$ hesabı)
- **Gözlemsel Olay Sözlüğü ve Kodlayıcı:** `src/events/eventDictionary.js` (13 yönsüz atom), `src/events/eventEncoder.js` (histerezis, `:R` retest koruma, 4 sn akış baz çizgisi ısınması, 3 saat: $\Delta t$, $\Delta\text{İşlem}$, $\Delta\text{Hacim}$)
- **Motif ve Epizodik Öğrenme:** `src/motifs/sequenceBuilder.js`, `src/motifs/episodeManager.js`, `src/motifs/motifRegistry.js`
- **3-Durumlu Baz Modeller, $N_{\text{eff}}$ ve Karantina (C+):** `src/learning/outcomeLabeler.js`, `src/learning/effectiveSample.js`, `src/learning/baseModels.js`, `src/learning/quarantineEngine.js`
- **Kalıcı Hafıza (B+) ve Orkestratör:** `src/storage/coinMemoryStore.js`, `src/engine/marketDnaRuntime.js`
- **Canlı API ve Mobil Arayüz:** `src/server.js` + `public/index.html`, `public/styles.css`, `public/app.js`

---

## 3. Gerçek Canlı Piyasa Test Kanıtları (FAZ 1 – FAZ 7)

| Faz | Doğrulama Betiği | Canlı Kanıt Dosyası | Temel Doğrulanan Sonuç |
|---|---|---|---|
| FAZ 1 | `tests/verify_phase1_feeds.js` | `audits/phase1_feeds_evidence.json` | 3 borsa `LIVE` & `synced: true`, `sequenceGaps: 0`, zorunlu soket kopmasında otomatik yeniden bağlanma (`reconnectCount: 1`), `BTCUSDT -> ETHUSDT` geçişinde `ctVal: 0.1` ve `zeroLeakedCrossCoinPackets: true`. |
| FAZ 2 | `tests/verify_phase2_encoder.js` | `audits/phase2_encoder_evidence.json` | 13 yönsüz olay atomu, 500ms tekrarlarının `%74–%76.6` oranında baskılanması, gerçek `:R` retestlerin korunması, `WALL_PULL` olayının manipülasyon kanıtı sayılmaması. |
| FAZ 3 | `tests/verify_phase3_motifs.js` | `audits/phase3_motifs_evidence.json` | Örtüşmeyen epizot kuralı, `<RESET>`/`<SESSION_GAP>` bariyer kesmesi, gereksiz ön-ek (`PRUNED_SUFFIX`) budaması ve borsalar arası jitter belirsizliğinde `~` eşzamanlılık işareti. |
| FAZ 4 | `tests/verify_phase4_learning.js` | `audits/phase4_learning_evidence.json` | Çoklu ufuk (`10s/30s/120s`), örtüşme ağırlığı ile seri korelasyonun ayrıştırıldığı $N_{\text{eff}}$, 16 hipotezli Benjamini-Hochberg FDR gürültü reddi, sentetik kalıcı sinyalde mezuniyet + bozulmada otomatik `QUARANTINED` düşüşü; **canlı kısa koşuda `GRADUATED: 0`**. |
| FAZ 5 | `tests/verify_phase5_storage.js` | `audits/phase5_storage_evidence.json` | Bozuk `state.v1.json` dosyasından `BACKUP_FALLBACK` ile kurtarma, yarım kalmış JSONL satırını atlayıp yeni satırı temiz başlatma, 8 saatlik boşlukta `STALE_LONG_GAP` (`warmUpRemaining: 4`) ataması ve `backgroundIncubation=false` iken boşta duraklatma. |
| FAZ 6 | `tests/verify_phase6_api_ui.js` | `audits/phase6_api_ui_evidence.json` | `0.0.0.0:3030` üzerinde 5 sekmeli mobil arayüz, göreceli URL uyumu, gerçek zamanlı SSE akışı (4.5 sn'de 16 canlı çerçeve), 7 dürüst durum rozeti ve canlı coin değiştirme API doğrulaması. |
| FAZ 7 | `tests/verify_phase7_e2e.js` | `audits/phase7_e2e_evidence.json` | 5 tam uçtan uca canlı senaryo: 3 borsa canlı akış + zorunlu WS kopması (`<RESET>`) + `BTCUSDT -> ETHUSDT -> BTCUSDT` izolasyonu + tam sunucu kapatıp soğuk başlatma (`Session #3`, `<SESSION_GAP>`, 215 motif korundu) + `3.093` nedensel sızıntı kontrolünde `0` ihlal. |

---

## 4. Canlı Mühendislik Sırasında Yakalanan Gerçek Bulgular ve Baz Model Sonuçları

1. **Nedensel Gözlemci Saati (Causal Observer Clock) Bulgusu:** Canlı `BTCUSDT` testinde `BINANCE_SPOT` `@depth@100ms` paketinin borsa üretim zaman damgasının (`ev.ts`), daha düşük gecikmeli `BINANCE_FUT` paketinden `242 ms` geride gelebildiği çalışma zamanı `DATA_LEAKAGE_VIOLATION` kilidi tarafından yakalandı. Epizot başlangıcı (`t0`), baz model tahmini ve ileri ufuk çözümlemesi **monotonik nedensel gözlemci varış saatine** (`_causalNow`) ve **aynı referans emir defteri orta fiyatına** bağlanarak hem 242 ms'lik görünmez ileri-bakış yanılsaması hem de vadeli-spot baz farkı kayması ortadan kaldırıldı (`3.093` kontrol, `0` sızıntı).
2. **Canlı Prequential Baz Model Karşılaştırması (`audits/phase7_e2e_evidence.json`):**
   - **10 sn Ufku (`n = 55`, `weightedN = 27.5`):** Gerçekleşen dağılım `[DOWN: %26.9, FLAT: %49.3, UP: %23.9]`. Prequential Multinomial Brier skorları:
     - `fixed_train`: **`0.6540`** *(ampirik lider)*
     - `decoupled_regime`: `0.6549`
     - `long_ewma`: `0.6550`
     - `short_ewma`: `0.6935` *(kısa vadeli gürültüye aşırı tepki nedeniyle en zayıf)*
   - **30 sn Ufku (`n = 45`, `weightedN = 7.5`):** Gerçekleşen dağılım `[DOWN: %25.9, FLAT: %33.3, UP: %40.7]`. Prequential Multinomial Brier skorları:
     - `long_ewma`: **`0.6675`** *(ampirik lider)*
     - `fixed_train`: `0.6739`
     - `short_ewma`: `0.6863`
     - `decoupled_regime`: `0.6911`
   - Bu canlı sonuç, hiçbir baz modelin peşinen kazanan ilan edilmemesi kuralının doğruluğunu ampirik olarak kanıtlamıştır.

---

## 5. Bilinen Sınırlamalar ve V1 Dürüstlük Sınırları

1. **Canlı Mezun Motif (`GRADUATED = 0`) Durumu:** C+ karantina motoru mezuniyet için kronolojik walk-forward ayrımı, $\text{N}_{\text{eff}} \ge 18$, pozitif Brier Skill Score, Benjamini-Hochberg FDR $q \le 0.10$ ve rejim kararlılığı şartlarını birlikte arar. Birkaç dakikalık canlı çalışmada binlerce olay ve yüzlerce aday motif kuluçkaya alınsa da hiçbir motifin mezun edilmemesi (`MOTİF KEŞFEDİLDİ` / `KULUÇKADA` sessiz modu) bir eksiklik değil, **istatistiksel kanıt olmadan tahmin üretmeme prensibinin doğrudan sonucudur**. Gerçek piyasa mezuniyeti saatler/günler süren kesintisiz veya çok oturumlu kuluçka gerektirir.
2. **Sandbox Ağ Kısıtları:** `fapi.binance.com` REST uç noktası HTTP 451 ve Bybit HTTP 403 döndürdüğünden, Binance USDT-M Vadeli akışı doğrudan çalışan `wss://fstream.binance.com` (`/stream` + `/market/stream`) üzerinden çift kanallı derinlik doğrulamasıyla yürütülmekte; REST saat/snapshot senkronu `api.binance.com` ve `www.okx.com` üzerinden alınmaktadır.
3. **Otomatik Emir İletimi Yoktur:** Market DNA V1 yalnızca canlı mikro-yapı gözlem, motif keşif, karantina doğrulama ve olasılıksal karar destek laboratuvarıdır; borsa hesabına emir göndermez.
