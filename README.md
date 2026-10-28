# Market DNA V1 (`deyne`)

**Tek Coin Odaklı Canlı Mikro-Yapı Gözlem, Olay Dizilimi (Motif) Keşif ve İstatistiksel Karantina Laboratuvarı**

Market DNA V1; seçilen tek bir kripto pariteyi (`BTCUSDT`, `ETHUSDT`, `SOLUSDT` vb.) aynı anda birden fazla canlı borsa akışından izleyen, emir defteri ve işlem akışını **sıfır yön önyargılı atomik durum geçişlerine** dönüştüren, aday motifleri **C+ karantina hunisinde** test eden ve kanıtlanmamış hiçbir deseni sinyal olarak sunmayan modüler bir Node.js + Mobil Web laboratuvarıdır.

---

## Temel Özellikler

- **3 Canlı Borsa Kaynağı (Sıfır Sahte Veri):**
  1. **Binance USDT-M Futures** (`bookTicker`, `depth20@100ms`, `depth@100ms`, `trade`, `markPrice@1s`, `forceOrder`)
  2. **Binance Spot** (`depth@100ms` REST snapshot senkronlu + `aggTrade`)
  3. **OKX USDT Perpetual Swap** (`books` 400-kademe `prevSeqId -> seqId` + `trades` + dinamik `ctVal`)
- **13 Atomik Gözlemsel Olay Sözlüğü:** `WALL_SPAWN`, `WALL_TEST`, `WALL_HOLD`, `WALL_PULL`, `WALL_BREAK`, `FLOW_BURST`, `FLOW_EXHAUST`, `BOOK_TILT`, `MICRO_FLIP`, `VOID_OPEN`, `SWEEP_REJECT`, `<RESET>`, `<SESSION_GAP>`.
- **Üçlü Saat ve Borsalar Arası Jitter Koruması:** Zaman ($\Delta t$), işlem sayısı ($\Delta N$) ve hacim ($\Delta V$) saatleri birlikte ölçülür; borsalar arası gecikme belirsizliği altında sahte öncülük-ardıllık (lead-lag) iddiasında bulunulmaz.
- **Sıfır Sızıntılı Öğrenme ve C+ Karantina Hunisi:**
  - `10s`, `30s`, `120s` ufuklarında spread + volatilite + işlem maliyeti duyarlı `UP` / `FLAT` / `DOWN` etiketleme.
  - 4 aday baz modelin (`fixed_train`, `short_ewma`, `long_ewma`, `decoupled_regime`) ileriye dönük (prequential) Multinomial Brier yarışması.
  - Kronolojik walk-forward doğrulama, örtüşme ağırlığı ve seri korelasyonu ayrıştırılmış $N_{\text{eff}}$ hesabı ve Benjamini-Hochberg FDR yanlış keşif kontrolü (`DISCOVERY` → `INCUBATION` → `VALIDATION` → `GRADUATED` ↔ `QUARANTINED` → `RETIRED`).
- **Kalıcı Coin Hafızası (B+):** Coin bazlı izole atomik kayıt (`tmp + fsync + backup + rename`), SHA-256 doğrulama, bozuk satır kurtarma ve oturumlar arası `<SESSION_GAP>` / yeniden ısınma (`warmingUp`) kontrolü.
- **Mobil Öncelikli 5 Sekmeli Canlı Panel:** `Piyasa`, `DNA`, `Laboratuvar`, `Hafıza`, `Ayarlar` (`public/index.html`, `public/styles.css`, `public/app.js`).

---

## Kurulum ve Çalıştırma

Proje **sıfır harici npm çalışma zamanı bağımlılığına** sahiptir (saf Node.js v20+ yerleşik `http`, `tls`, `crypto`, `fs` modülleriyle çalışır):

```bash
# Sunucuyu başlat (varsayılan port: 3030)
node src/server.js

# Özel port veya özel veri dizini ile başlatma
PORT=3030 DNA_DATA_DIR=./data/coins node src/server.js
```

Tarayıcıdan `http://localhost:3030` adresini açarak canlı mobil laboratuvar paneline erişebilirsiniz.

---

## Doğrulama ve Canlı Denetim Testleri

```bash
node tests/verify_phase1_feeds.js     # FAZ 1: 3 borsa canlı akış ve saat senkronu
node tests/verify_phase2_encoder.js   # FAZ 2: Atomik olay kodlayıcı ve histerezis
node tests/verify_phase3_motifs.js    # FAZ 3: Dizilim, epizot ve ek bilgi kazancı budaması
node tests/verify_phase4_learning.js  # FAZ 4: N_eff, baz modeller, sızıntı kilidi ve BH-FDR
node tests/verify_phase5_storage.js   # FAZ 5: Atomik B+ hafıza ve çökme kurtarma
node tests/verify_phase7_e2e.js       # FAZ 7: Uçtan uca canlı denetim senaryoları
```

Detaylı mühendislik ve denetim bulguları için: [`audits/FINAL_AUDIT_REPORT.md`](audits/FINAL_AUDIT_REPORT.md) ve [`TODO.md`](TODO.md).
