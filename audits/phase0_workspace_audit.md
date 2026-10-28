# FAZ 0 — Mevcut Çalışma Alanı Denetim Raporu

**Denetim Tarihi:** 2026-10-28  
**Çalışma Alanı:** `/home/user/dna-lab/`  
**Denetlenen Dosyalar:**
1. `/home/user/dna-lab/btc_single_coin_audit.js` (17.980 bayt)
2. `/home/user/dna-lab/btc_events_trace.jsonl` (2.871 bayt, 21 satır)
3. `/home/user/dna-lab/btc_audit_summary.json` (3.988 bayt)
4. `/home/user/dna-lab/live_experiment.js` (42.894 bayt) & `live_dna_report.json`
5. `/home/user/dna-lab/live_experiment_v2.js` (21.603 bayt) & `live_dna_report_v2.json`
6. `/home/user/dna-lab/live_audit_precision.js` (10.281 bayt) & `live_audit_precision.json`
7. `/home/user/uploads/orderbook-mobile (5).html` (182.680 bayt — salt okunur referans, müdahale edilmedi)

---

## 1. Mevcut Kanıtların Doğrulanması ve Hesap Karşılaştırması

### 1.1 `btc_events_trace.jsonl` ve `btc_audit_summary.json` İncelemesi
- **Kayıt Bütünlüğü:** `btc_events_trace.jsonl` içinde `2026-10-28T17:29:26.397Z` ile `17:30:00.465Z` arasında kaydedilmiş **21 geçerli JSONL satırı** bulundu; bu sayı `btc_audit_summary.json` içindeki `cleanDnaEvents: 21` alanıyla birebir uyuşmaktadır.
- **Fiyat Davranışı Denetimi:** 21 olayın tamamında `mid: 80538.65` olarak kaydedilmiştir. Canlı doğrulama ile Binance USDT-M `BTCUSDT` vadeli tahtasının `$0.10` adım büyüklüğünde (`80538.60 / 80538.70`) kısa pencerelerde yoğun BBO likiditesiyle kilitlenebildiği doğrulandı. Ancak **35 saniyelik bu pencerede `flatPct = %100` (`downPct = %0`, `upPct = %0`) gerçekleştiği için**, `model2_ShortWindowEWMA = 0.1811` Brier skorunun diğer modellere üstün görünmesi **tek rejimli küçük örneklem artefaktıdır (`nEff = 2.45`)**. Bu pencereden genel geçer baz model üstünlüğü iddia edilemez.

### 1.2 Sıkıştırma Oranı (`25,8x` vs `5,11x`)Mutabakatı
`live_dna_report.json` (`test1_stutteringVsTransition`) incelendiğinde:
- `totalL2DepthPackets = 7.344`, `cleanStateTransitionEvents = 285` $\Rightarrow 7344 / 285 = 25,77 \approx \mathbf{25,8\times}$ (*Ham L2 derinlik paketi / olay oranı*).
- `naiveActiveTickTokens = 1.457`, `cleanStateTransitionEvents = 285` $\Rightarrow 1457 / 285 = \mathbf{5,11\times}$ (*500 ms aktif eşik tick'i / olay oranı*).
- **Sonuç:** Önceki özetlerde payda açık belirtilmediği için kavram karmaşası oluşmuştur. Market DNA V1'de her iki oran ayrı metrik adlarıyla (`l2PacketsPerEvent` ve `activeTicksPerEvent`) raporlanacaktır.

---

## 2. Prototip Kodlarda Tespit Edilen Kritik Hatalar ve Eksikler

1. **Binance Futures `@trade` Sıfır Fiyatlı Paket Sızıntısı (`btc_single_coin_audit.js` satır 161):**
   - `wss://fstream.binance.com` üzerindeki `@trade` akışı zaman zaman piyasa dışı uzlaşma/RPI kayıtları (`p: "0.00"`, `X !== "MARKET"`) göndermektedir. `btc_single_coin_audit.js` içinde `d.p > 0 && d.X === "MARKET"` filtresi bulunmadığından `notional = 0` işlemler işlem sayacını (`tradeCount`) şişirme riski taşımaktadır.
2. **OKX Swap Sözleşme Çarpanı (`ctVal`) Tutarsızlığı (`live_experiment.js` satır 196):**
   - OKX USDT-Swap işlemlerinde `sz` alanı coin miktarı değil **kontrat adedidir** (ör. `BTC-USDT-SWAP` için `1 kontrat = 0.01 BTC`, `ETH-USDT-SWAP` için `0.1 ETH`, `XRP-USDT-SWAP` için `100 XRP`). `live_experiment.js` kontrat adedini doğrudan fiyatla çarpmış; `btc_single_coin_audit.js` ise yalnızca BTC için sabit `0.01` yazmıştır. FAZ 1'de OKX `/api/v5/public/instruments?instType=SWAP` uç noktasından gerçek `ctVal` çekilecektir.
3. **Binance `/stream` ve `/market/stream` Kanal Ayrımı:**
   - Standart `wss://fstream.binance.com/stream` üzerinde `@bookTicker`, `@depth20@100ms`, `@depth@100ms` ve `@trade` aktif mesaj üretirken, `@markPrice@1s` ve `@forceOrder` mesajları `wss://fstream.binance.com/market/stream` üzerinden gelmektedir. FAZ 1 adaptörü her iki kanalı ayrı yönetip gerçek mesaj sayaçlarıyla kanıtlayacaktır.
4. **Sadece Zamanlayıcı (Cooldown) ile Tekrar Engelleme Kusuru (`btc_events_trace.jsonl` satır 7–8):**
   - `CROSS:DUAL_FUT_SYNC_BURST` olayı 1.063 ms arayla (`17:29:33.314Z` ve `17:29:34.377Z`) aynı akış dalgası içinde iki kez üretilmiştir; çünkü durum nötr bölgeye dönmeden yalnızca `1000 ms` zamanlayıcıyla yeniden tetiklenmiştir. FAZ 2'de her olay için açık **başlangıç / bitiş / histerezis sıfırlama** makinesi kurulacaktır.
5. **Borsalar Arası Saat Farkı ve Gecikme Belirsizliği Ölçülmemiş:**
   - Prototipler olayları `Date.now()` yerel varış zamanıyla sıralamıştır. Farklı borsaların sunucu saat farkı (`clockOffsetMs`) ve ağ gecikme oynaklığı (`jitterMs`) ölçülmeden borsalar arası kesin öncül-artçıl sıra iddia edilemez.
6. **Eksik Olay Atomları, Eksik `120s` Ufku ve Kalıcı Hafıza Yokluğu:**
   - `WALL_TEST`, `VOID_OPEN`, `SWEEP_REJECT` ve `<SESSION_GAP>` atomları prototiplerde yer almamaktadır.
   - `10s / 30s / 120s` üçlü ufuk yapısı, Benjamini-Hochberg FDR kontrolü ve atomik disk hafızası henüz kurulmamıştır.

---

## 3. FAZ 0 Kararı

Mevcut prototip dosyalar **silinmeden arşiv referansı olarak korunacak**, ancak içlerindeki hiçbir ön bulgu Market DNA V1 içinde "önceden doğrulanmış mezun kural" olarak kabul edilmeyecektir. FAZ 1'den itibaren tüm modüller temiz bir `src/` mimarisi altında sıfırdan inşa edilecektir.
