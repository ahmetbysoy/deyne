'use strict';

const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { FeedCoordinator } = require('../src/adapters/feedCoordinator');

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function main() {
  console.log('[VERIFY PHASE 1] Testing zero-dependency WsClient + 3 Live Adapters + Single-Coin Switch + Reconnect...');
  const fc = new FeedCoordinator();
  const eventsSeen = { BTCUSDT: { book: 0, trade: 0, bbo: 0, mark: 0, gaps: [] }, ETHUSDT: { book: 0, trade: 0, bbo: 0, mark: 0, gaps: [] } };
  let leakedOldCoinPackets = 0;

  fc.on('book', (p) => {
    if (p.symbol !== fc.symbol) leakedOldCoinPackets += 1;
    else eventsSeen[p.symbol].book += 1;
  });
  fc.on('trade', (p) => {
    if (p.symbol !== fc.symbol) leakedOldCoinPackets += 1;
    else eventsSeen[p.symbol].trade += 1;
  });
  fc.on('bbo', (p) => {
    if (p.symbol !== fc.symbol) leakedOldCoinPackets += 1;
    else eventsSeen[p.symbol].bbo += 1;
  });
  fc.on('mark', (p) => {
    if (p.symbol !== fc.symbol) leakedOldCoinPackets += 1;
    else eventsSeen[p.symbol].mark += 1;
  });
  fc.on('gap', (g) => {
    if (eventsSeen[g.symbol]) eventsSeen[g.symbol].gaps.push(g.reason);
  });

  // Step 1: Subscribe to BTCUSDT for 8 seconds
  fc.startCoin('BTCUSDT');
  await sleep(8000);
  const btcSnap = fc.getSnapshot();

  assert.strictEqual(btcSnap.symbol, 'BTCUSDT');
  assert.strictEqual(btcSnap.venues.BINANCE_FUT.status, 'LIVE', 'Binance Futures must be LIVE');
  assert.strictEqual(btcSnap.venues.BINANCE_SPOT.status, 'LIVE', 'Binance Spot must be LIVE');
  assert.strictEqual(btcSnap.venues.OKX_SWAP.status, 'LIVE', 'OKX Swap must be LIVE');
  assert.strictEqual(btcSnap.venues.OKX_SWAP.ctVal, 0.01, 'OKX BTC-USDT-SWAP ctVal must be verified as 0.01 BTC');
  assert(btcSnap.venues.BINANCE_FUT.counters.bookTicker > 20, 'Must receive real Binance Futures bookTicker packets');
  assert(btcSnap.venues.BINANCE_FUT.counters.depth20 > 10, 'Must receive real Binance Futures depth20 packets');
  assert(btcSnap.venues.BINANCE_FUT.counters.markPrice >= 3, 'Must receive real Binance Futures /market/stream markPrice packets');
  assert(btcSnap.venues.BINANCE_SPOT.counters.depthDiff > 10, 'Must receive real Binance Spot depth packets');
  assert(btcSnap.venues.OKX_SWAP.counters.bookUpdates > 5, 'Must receive real OKX Swap book updates');

  // Step 2: Simulate unexpected socket drop on Binance Futures bookWs & verify gap + auto-reconnect!
  console.log('[VERIFY PHASE 1] Simulating unexpected socket drop on BINANCE_FUT...');
  const futAd = fc.adapters.BINANCE_FUT;
  futAd.bookWs.socket.destroy();
  await sleep(4500);
  const btcAfterReconnect = fc.getSnapshot();
  assert.strictEqual(btcAfterReconnect.venues.BINANCE_FUT.status, 'LIVE', 'Binance Futures must auto-reconnect to LIVE');
  assert(btcAfterReconnect.venues.BINANCE_FUT.counters.reconnects >= 1, 'Reconnect counter must increment');
  assert(eventsSeen.BTCUSDT.gaps.includes('book_ws_closed'), 'Gap event must be emitted on socket drop');

  // Step 3: Switch coin to ETHUSDT & verify zero BTC leakage + ETH ctVal = 0.1
  console.log('[VERIFY PHASE 1] Switching active coin BTCUSDT -> ETHUSDT...');
  fc.startCoin('ETHUSDT');
  await sleep(6000);
  const ethSnap = fc.getSnapshot();
  const uncertaintyFutOkx = fc.getCrossVenueUncertaintyMs('BINANCE_FUT', 'OKX_SWAP');
  const uncertaintyFutSpot = fc.getCrossVenueUncertaintyMs('BINANCE_FUT', 'BINANCE_SPOT');
  assert.strictEqual(btcSnap.venues.BINANCE_FUT.synced, true, 'Binance Futures book must be synced');
  assert.strictEqual(btcSnap.venues.BINANCE_FUT.counters.sequenceGaps, 0, 'Zero false pu_sequence_gaps on steady stream');
  assert.strictEqual(ethSnap.symbol, 'ETHUSDT');
  assert.strictEqual(leakedOldCoinPackets, 0, 'Zero packets from previous coin may leak after coin switch');
  assert.strictEqual(ethSnap.venues.OKX_SWAP.ctVal, 0.1, 'OKX ETH-USDT-SWAP ctVal must be verified as 0.1 ETH');
  assert(eventsSeen.ETHUSDT.book > 15, 'ETHUSDT book packets must flow');

  fc.stopAll('test_complete');

  const evidence = {
    verifiedAt: new Date().toISOString(),
    zeroLeakedCrossCoinPackets: leakedOldCoinPackets === 0,
    btcSnapshot: btcSnap,
    btcReconnectVerified: {
      statusAfterReconnect: btcAfterReconnect.venues.BINANCE_FUT.status,
      reconnectCount: btcAfterReconnect.venues.BINANCE_FUT.counters.reconnects,
      gapsRecorded: eventsSeen.BTCUSDT.gaps
    },
    ethSwitchSnapshot: {
      symbol: ethSnap.symbol,
      okxCtVal: ethSnap.venues.OKX_SWAP.ctVal,
      eventsReceived: eventsSeen.ETHUSDT,
      crossVenueUncertaintyMs_FutVsOkx: uncertaintyFutOkx,
      crossVenueUncertaintyMs_FutVsSpot: uncertaintyFutSpot
    }
  };

  const outFile = path.join(__dirname, '..', 'audits', 'phase1_feeds_evidence.json');
  fs.writeFileSync(outFile, JSON.stringify(evidence, null, 2));
  console.log('[VERIFY PHASE 1] ALL ASSERTIONS PASSED! Evidence saved to:', outFile);
}

main().catch((err) => {
  console.error('[VERIFY PHASE 1 FAILED]:', err);
  process.exit(1);
});
