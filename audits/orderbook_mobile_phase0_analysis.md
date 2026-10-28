# FAZ 0 — `orderbook-mobile.html` Mevcut Dosya Analizi Raporu

**İncelenen Kaynak Dosya:** `/home/user/uploads/orderbook-mobile (5).html` (`3.466` satır)  
**Çalışma Kopyası:** `/home/user/dna-lab/orderbook-mobile.html`

---

## 3.1. Veri Kaynakları ve Görselleştirme Mimarisi (`dosya: satır aralığı`)

| Bileşen | Satır Aralığı | Gerçek Kod Davranışı ve Tespitler |
|---|---|---|
| Sabitler ve Borsa Tanımları | `723–747` | `FAPI = 'https://fapi.binance.com/fapi/v1'`, `VISION_API = 'https://data-api.binance.vision/api/v3'`, `WS_ROOT = 'wss://fstream.binance.com/stream?streams='`, `BYBIT_WS = 'wss://stream.bybit.com/v5/public/linear'`. |
| Stream Abonelikleri (`connectSocket`) | `1036–1168` | Tek birleşik WebSocket üzerinden `['@depth20@100ms', '@depth@100ms', '@trade', '@aggTrade', '@ticker']` kanallarına abone olunur (`1045`). `@depth20@100ms` her 100 ms'de 20 kademelik tam görünüm verir (`1115–1128`). |
| Bybit Adaptörü ve Farkları (`switchVenue`, `receiveBybit`) | `1084–1092`, `1171–1258` | İlk 9 sn'de (`FAILOVER_AFTER_MS = 9000`) kitap oluşmazsa `switchVenue` Bybit'e geçer (`orderbook.50.<SYM>`, `publicTrade.<SYM>`, `tickers.<SYM>`). Bybit WS kendi `snapshot` + `delta` akışını verir (`partialOnly = true`, 50 seviye); `pu` alanı yoktur; işlemler dizi olarak gelir (`trade.p`, `trade.v`, `trade.S`). |
| Snapshot ve Diff-Depth Senkronu (`requestSnapshot`, `syncSnapshot`, `receivePartialDepth`, `receiveDepthEvent`) | `1373–1523` | REST `/fapi/v1/depth` başarılıysa `lastUpdateId` ve `pu === lastUpdateId` zinciri kurulur (`1398–1416`, `1489–1492`). REST engelliyse (HTTP 451 / CORS / `file://`) 2 hatadan sonra `restDisabled = true; forcePartial = true` olur (`1421–1428`) ve kitap kesintisiz olarak `@depth20@100ms` üzerinden güncellenir (`1435–1474`). |
| Kopma, Yeniden Bağlanma ve Veri Boşluğu (`startWatchdog`, `scheduleReconnect`) | `1149–1167`, `1260–1286`, `1493–1505` | 8 sn sessizlikte (`STALE_MS = 8000`) `GECİKME`, 25 sn sessizlikte (`WATCHDOG_MS = 25000`) soket tazeleme yapılır. `receiveDepthEvent` içinde `!chained` olduğunda `gapCount` ve `gapStreak` artar; mevcut motor akışı kesmeden diff uygular ve arka planda snapshot onarımı ister. |
| İşlem Akışı, Agresif Yön ve CVD (`receiveAggTrade`, `sumFlow`, `cvdSlope`) | `1988–2056` | `aggTrade` aktifse 30 sn boyunca `@trade` çift sayımı önlemek için atlanır (`1992–1993`). `event.m === true` satıcı agresif (`side = -1`), `false` alıcı agresif (`side = +1`) sayılır (`1998`). **Kritik tespit:** `1995` satırında `qty <= 0` filtrelenirken `price <= 0` veya `event.X !== 'MARKET'` kontrolü yoktur (`p: "0.00"` uzlaşma paketleri `notional: 0` olarak diziye girebilir). |
| Fiyat, Spread, Mid, Microprice ve Likidite Metrikleri | `1572–1586`, `2148–2295` | `mid = (bestBid + bestAsk)/2`, `spreadBps = (spread/mid)*1e4`, `microPrice = (bestAsk*bq0 + bestBid*aq0)/(bq0+aq0)` ve `micro = ((microPrice - mid)/mid)*1e4` (`2181–2185`), üstel ağırlıklı `imbalance`, `slope`, `findWall`/`trackWall`, `absorb` ve `barrierDistance` (`voidF`) hesaplanır. |
| Isı Haritası DOM ve Render Zamanlaması | `630–691`, `917–923`, `1567–1609`, `1709–1736` | `#panel-heat` içinde `#engineCard` ve altında 9 alış + 9 satış fiyat bandından oluşan ısı haritası (`#askHeatRows`, `#heatMid`, `#bidHeatRows`, `#bidWall`, `#askWall`) bulunur. Render işlemi `scheduleRender()` (110 ms throttle) ve `engineTick()` (500 ms, yalnızca `activeTab === 'heat'` iken) üzerinden tetiklenir. |

---

## 3.2. Mevcut Yön Motoru (`obm.engine.v2`) İncelemesi

1. **Tahmin Üretimi ve Ufuk (`2126–2392`):** `engineTick()` her `ENGINE_TICK_MS = 500` ms'de çalışır (`1029`). Ufuk `horizonSec = round(clamp(15 / max(0.4, volRate), 8, 60))` (`2154`) ile dinamik belirlenir; etiketler `'up' | 'down' | 'neutral'` (`2343–2344`) olarak atanır.
2. **500 ms Bağımlı Örneklem Sorunu (Doğrulandı — `2383–2386`):** `engineTick()` her **500 ms'de bir koşulsuz olarak** `engine.pending.push({ t: now, mid, ... horizonSec })` kaydı açmaktadır. Örneğin `horizonSec = 30` sn iken aynı fiyat hareketini ölçen **60 adet örtüşen ve birbirine bağımlı tahmin** açılır; tek bir fiyat dalgası 60 kez `resolved`/`hits` sayacına ve AdaGrad ağırlık güncellemesine girer.
3. **Çözümleme, Brier ve AdaGrad (`2400–2505`):** `resolvePending(now)` süresi dolan kayıtları `engine.midHistory` üzerinden çözer (`moveBps === 0` ise `frozenSkipped` sayar). `deadZone = clamp(0.35 * max(1.2, |expectedBps|), 1.5, 10)` kullanılır. İkili Brier skoru `brierSum += (pDisp - (moveBps > 0 ? 1 : 0))^2` ile toplanır (`2423`); `learnFrom` rejim bazlı AdaGrad (`grad2[key]`) ve Platt kalibrasyonunu (`calib.a`, `calib.b`) günceller (`2459–2477`).
4. **Sembol Değişimi ve Sekme Davranışı (`1028–1031`, `1604–1608`, `1919–1965`):** `resetEngineForSymbol(symbol)` tüm geçici dizileri (`pending`, `trades`, `midHistory`, `wallTrack`) sıfırlar ve `obm.engine.v2.<SYMBOL>` kaydını yükler. Isı Haritası sekmesi kapalıyken de WebSocket alımı ve `engineTick` + `resolvePending` çalışmaya devam eder; yalnızca DOM çizimi (`renderEnginePanel`) atlanır.

---

## 3.3. Yeniden Kullanım, Kısıtlar ve Çakışma Önleme Planı

1. **Yeniden Kullanılacak Yardımcılar (Salt-Okunur):** `fmtPrice`, `fmtCompact`, `fmtClock`, `getSortedLevels`, `engClamp`, `feed` (salt-okunur kitap/durum okuma) ve gölge mod eşleştirmesi için salt-okunur `engineSnap()`.
2. **Gerçek Veriden Üretilebilir vs. Kısıtlı Özellikler:**
   - L3 tekil emir iptal kimliği WebSocket akışında bulunmadığından `BID_WALL_PULL` / `ASK_WALL_PULL` kesin manipülasyon/spoofing iddiası olarak değil, **ilgili kademedeki işlem hacmiyle açıklanamayan net likidite çekilmesi** olarak kanıt alanlarıyla (`shrunkNotional`, `tradedNearNotional`) kaydedilir.
   - `fapi.binance.com` REST engelli ağlarda kitap `@depth20@100ms` (`partialOnly = true`, en iyi 20 alış/satış kademesi) üzerinden çalıştığından, `MarketDNAEventDetector` duvar ve `VOID_OPEN` tespitini **yalnızca o an gözlemlenebilir fiyat bandı (`[minBid, maxAsk]`) içinde** yapar; 20. kademenin dışına taşan fiyatları sahte `WALL_PULL` veya `VOID_OPEN` olarak etiketlemez.
3. **Sıfır Çakışma Garantisi:**
   - Mevcut `engine` nesnesine, ağırlıklarına, `pending` dizisine, `obm.engine.v2.*` `localStorage` anahtarlarına ve WebSocket aboneliklerine **kesinlikle müdahale edilmez**.
   - İkinci bir WebSocket bağlantısı veya ikinci bir `setInterval` döngüsü açılmaz; Market DNA mevcut akış noktalarından beslenir ve kalıcı hafızasını izole `obm.dna.v1.<SYMBOL>` anahtarlarında (boyut kotalı olarak) saklar.
