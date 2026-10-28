'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SCHEMA_VERSION = 1;
const MAX_SEGMENT_BYTES = 2 * 1024 * 1024; // 2 MB segmented JSONL logs

function sha256(str) {
  return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
}

function resolveDefaultBaseDir() {
  if (process.env.DNA_DATA_DIR) return process.env.DNA_DATA_DIR;
  if (fs.existsSync('/home/user')) return '/home/user/out/dna-data/coins';
  return path.join(__dirname, '..', '..', 'data', 'coins');
}

class CoinMemoryStore {
  constructor(baseDir) {
    this.baseDir = baseDir || resolveDefaultBaseDir();
    fs.mkdirSync(this.baseDir, { recursive: true });
  }

  coinDir(symbol) {
    const clean = String(symbol).toUpperCase().replace(/[^A-Z0-9]/g, '');
    if (!clean) throw new Error('Invalid coin symbol');
    return path.join(this.baseDir, clean);
  }

  listStoredCoins() {
    if (!fs.existsSync(this.baseDir)) return [];
    const entries = fs.readdirSync(this.baseDir, { withFileTypes: true });
    const out = [];
    for (const ent of entries) {
      if (!ent.isDirectory()) continue;
      const stateFile = path.join(this.baseDir, ent.name, 'state.v1.json');
      if (fs.existsSync(stateFile)) {
        try {
          const stat = fs.statSync(stateFile);
          out.push({ symbol: ent.name, updatedAt: stat.mtimeMs, sizeBytes: stat.size });
        } catch (_) {}
      }
    }
    return out.sort((a, b) => b.updatedAt - a.updatedAt);
  }

  /**
   * Atomically saves per-coin learning state using write -> fsync -> backup -> rename.
   */
  saveCoinState(symbol, statePayload) {
    const dir = this.coinDir(symbol);
    fs.mkdirSync(dir, { recursive: true });

    const targetPath = path.join(dir, 'state.v1.json');
    const backupPath = path.join(dir, 'state.v1.bak.json');
    const tmpPath = path.join(dir, `state.v1.tmp.${process.pid}.${Date.now()}`);

    const core = {
      schemaVersion: SCHEMA_VERSION,
      symbol: String(symbol).toUpperCase(),
      savedAt: Date.now(),
      ...statePayload
    };
    const coreJson = JSON.stringify(core);
    const envelope = JSON.stringify({
      schemaVersion: SCHEMA_VERSION,
      checksum: sha256(coreJson),
      payload: core
    });

    const fd = fs.openSync(tmpPath, 'w');
    try {
      fs.writeFileSync(fd, envelope, 'utf8');
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    // Preserve previous valid state as backup before atomic replace
    if (fs.existsSync(targetPath)) {
      try {
        const existingRaw = fs.readFileSync(targetPath, 'utf8');
        const parsed = JSON.parse(existingRaw);
        if (parsed && parsed.payload && parsed.checksum === sha256(JSON.stringify(parsed.payload))) {
          fs.copyFileSync(targetPath, backupPath);
        }
      } catch (_) {
        // Current target is corrupted; do NOT overwrite valid backup with corrupt target!
      }
    }

    fs.renameSync(tmpPath, targetPath);
    return { savedAt: core.savedAt, path: targetPath, checksum: sha256(coreJson) };
  }

  /**
   * Loads coin state with schema version check, SHA-256 verification, controlled backup fallback,
   * staleness evaluation, and JSONL tail recovery.
   */
  loadCoinState(symbol, now = Date.now()) {
    const dir = this.coinDir(symbol);
    const targetPath = path.join(dir, 'state.v1.json');
    const backupPath = path.join(dir, 'state.v1.bak.json');

    const recoveryReport = {
      existed: false,
      loadedFrom: null, // 'PRIMARY' | 'BACKUP_FALLBACK' | 'FRESH'
      corruptionDetected: false,
      corruptionReason: null,
      corruptJsonlLinesSkipped: 0,
      gapMs: null,
      stalenessLevel: 'FRESH_INIT',
      warmUpEpisodesAssigned: 0
    };

    const tryParseVerified = (filePath) => {
      const raw = fs.readFileSync(filePath, 'utf8');
      const env = JSON.parse(raw);
      if (!env || env.schemaVersion !== SCHEMA_VERSION || !env.payload) {
        throw new Error(`Unsupported or missing schemaVersion in ${path.basename(filePath)}`);
      }
      const expectedHash = sha256(JSON.stringify(env.payload));
      if (env.checksum !== expectedHash) {
        throw new Error(`SHA-256 checksum mismatch in ${path.basename(filePath)}`);
      }
      if (env.payload.symbol !== String(symbol).toUpperCase()) {
        throw new Error(`Cross-coin symbol mismatch: expected ${symbol}, got ${env.payload.symbol}`);
      }
      return env.payload;
    };

    let payload = null;
    if (fs.existsSync(targetPath)) {
      recoveryReport.existed = true;
      try {
        payload = tryParseVerified(targetPath);
        recoveryReport.loadedFrom = 'PRIMARY';
      } catch (errPrimary) {
        recoveryReport.corruptionDetected = true;
        recoveryReport.corruptionReason = errPrimary.message;
        if (fs.existsSync(backupPath)) {
          try {
            payload = tryParseVerified(backupPath);
            recoveryReport.loadedFrom = 'BACKUP_FALLBACK';
          } catch (errBak) {
            recoveryReport.corruptionReason += ` | Backup also failed: ${errBak.message}`;
            recoveryReport.loadedFrom = 'FRESH';
          }
        } else {
          recoveryReport.loadedFrom = 'FRESH';
        }
      }
    } else {
      recoveryReport.loadedFrom = 'FRESH';
    }

    // Recover recent events from segmented JSONL archive (skipping any truncated/corrupt lines safely)
    const recentEventsResult = this.readRecentJsonl(symbol, 'events', 250);
    recoveryReport.corruptJsonlLinesSkipped += recentEventsResult.corruptLinesSkipped;

    const recentEpisodesResult = this.readRecentJsonl(symbol, 'episodes', 120);
    recoveryReport.corruptJsonlLinesSkipped += recentEpisodesResult.corruptLinesSkipped;

    if (payload) {
      const gapMs = Math.max(0, now - (payload.savedAt || now));
      recoveryReport.gapMs = gapMs;

      if (gapMs < 15 * 60 * 1000) {
        recoveryReport.stalenessLevel = 'RECENT_SESSION';
        recoveryReport.warmUpEpisodesAssigned = 1;
      } else if (gapMs < 6 * 3600 * 1000) {
        recoveryReport.stalenessLevel = 'MODERATE_SESSION_GAP';
        recoveryReport.warmUpEpisodesAssigned = 2;
      } else {
        recoveryReport.stalenessLevel = 'STALE_LONG_GAP';
        recoveryReport.warmUpEpisodesAssigned = 4;
      }

      // Mark any previously GRADUATED motifs as requiring warm-up re-observation in the new session!
      if (Array.isArray(payload.motifs)) {
        for (const m of payload.motifs) {
          if (m.lifecycleState === 'GRADUATED') {
            m.warmingUp = true;
            m.warmUpRemaining = recoveryReport.warmUpEpisodesAssigned;
          }
        }
      }
    }

    return {
      payload,
      recentEvents: recentEventsResult.records,
      recentEpisodes: recentEpisodesResult.records,
      recoveryReport
    };
  }

  /**
   * Appends a single record to a segmented JSONL stream (`events` or `episodes`).
   */
  appendJsonl(symbol, streamName, record) {
    const dir = this.coinDir(symbol);
    fs.mkdirSync(dir, { recursive: true });
    const segPath = this._activeSegmentPath(dir, streamName);
    if (fs.existsSync(segPath)) {
      try {
        const st = fs.statSync(segPath);
        if (st.size > 0) {
          const buf = Buffer.alloc(1);
          const fd = fs.openSync(segPath, 'r');
          try { fs.readSync(fd, buf, 0, 1, st.size - 1); } finally { fs.closeSync(fd); }
          if (buf[0] !== 0x0a) fs.appendFileSync(segPath, '\n', 'utf8');
        }
      } catch (_) {}
    }
    const line = JSON.stringify(record) + '\n';
    fs.appendFileSync(segPath, line, 'utf8');
  }

  _activeSegmentPath(dir, streamName) {
    const prefix = `${streamName}.seg_`;
    const files = fs.readdirSync(dir)
      .filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl'))
      .sort();

    if (!files.length) return path.join(dir, `${prefix}0001.jsonl`);
    const latest = path.join(dir, files[files.length - 1]);
    try {
      const st = fs.statSync(latest);
      if (st.size < MAX_SEGMENT_BYTES) return latest;
      const idx = parseInt(files[files.length - 1].slice(prefix.length, -6), 10) || 1;
      return path.join(dir, `${prefix}${String(idx + 1).padStart(4, '0')}.jsonl`);
    } catch (_) {
      return latest;
    }
  }

  readRecentJsonl(symbol, streamName, limit = 200) {
    const dir = this.coinDir(symbol);
    if (!fs.existsSync(dir)) return { records: [], corruptLinesSkipped: 0 };
    const prefix = `${streamName}.seg_`;
    const files = fs.readdirSync(dir)
      .filter((f) => f.startsWith(prefix) && f.endsWith('.jsonl'))
      .sort();

    const records = [];
    let corruptLinesSkipped = 0;

    for (let fIdx = Math.max(0, files.length - 2); fIdx < files.length; fIdx++) {
      const fullPath = path.join(dir, files[fIdx]);
      let content = '';
      try { content = fs.readFileSync(fullPath, 'utf8'); } catch (_) { continue; }
      const lines = content.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
          const obj = JSON.parse(trimmed);
          records.push(obj);
        } catch (_) {
          corruptLinesSkipped += 1;
        }
      }
    }

    return {
      records: records.slice(-limit),
      corruptLinesSkipped
    };
  }
}

module.exports = { CoinMemoryStore, SCHEMA_VERSION };
