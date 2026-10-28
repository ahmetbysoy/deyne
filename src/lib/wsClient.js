'use strict';

const tls = require('tls');
const net = require('net');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { URL } = require('url');

/**
 * Zero-dependency RFC-6455 WebSocket Client with built-in Ping/Pong RTT measurement,
 * fragmented frame reassembly, and deterministic teardown (no dangling sockets).
 */
class WsClient extends EventEmitter {
  constructor(rawUrl, options = {}) {
    super();
    this.url = new URL(rawUrl);
    this.options = options;
    this.socket = null;
    this.connected = false;
    this.closed = false;
    this.handshakeDone = false;
    this.rxBuffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentOpcode = 0;
    this.pingTimer = null;
    this.connectTimeoutTimer = null;
    this.lastPingSentAt = 0;
    this.rttMs = null;
    this.lastRxAt = 0;
    this.bytesReceived = 0;
    this.messagesReceived = 0;
    this._connect();
  }

  _connect() {
    const isSecure = this.url.protocol === 'wss:';
    const port = Number(this.url.port) || (isSecure ? 443 : 80);
    const host = this.url.hostname;
    const pathWithQuery = (this.url.pathname || '/') + (this.url.search || '');
    const key = crypto.randomBytes(16).toString('base64');
    this.expectedAccept = crypto
      .createHash('sha1')
      .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
      .digest('base64');

    const onConnect = () => {
      if (this.closed) return;
      const headers = [
        `GET ${pathWithQuery} HTTP/1.1`,
        `Host: ${host}${this.url.port ? ':' + this.url.port : ''}`,
        'Upgrade: websocket',
        'Connection: Upgrade',
        `Sec-WebSocket-Key: ${key}`,
        'Sec-WebSocket-Version: 13',
        'User-Agent: MarketDNA-V1/1.0',
        '\r\n'
      ].join('\r\n');
      this.socket.write(headers);
    };

    this.socket = isSecure
      ? tls.connect({ host, port, servername: host, rejectUnauthorized: true }, onConnect)
      : net.connect({ host, port }, onConnect);

    this.socket.setNoDelay(true);

    const timeoutMs = this.options.handshakeTimeoutMs || 10_000;
    this.connectTimeoutTimer = setTimeout(() => {
      if (!this.handshakeDone && !this.closed) {
        this._fail(new Error(`WS handshake timeout after ${timeoutMs}ms (${this.url.host})`));
      }
    }, timeoutMs);

    this.socket.on('data', (chunk) => this._onData(chunk));
    this.socket.on('error', (err) => {
      if (!this.closed) this._fail(err);
    });
    this.socket.on('close', () => {
      const wasConnected = this.connected;
      this._cleanupTimers();
      this.connected = false;
      if (!this.closed) {
        this.closed = true;
        this.emit('close', { clean: wasConnected });
      }
    });
  }

  _onData(chunk) {
    if (this.closed) return;
    this.lastRxAt = Date.now();
    this.bytesReceived += chunk.length;
    this.rxBuffer = this.rxBuffer.length ? Buffer.concat([this.rxBuffer, chunk]) : chunk;

    if (!this.handshakeDone) {
      const headerEnd = this.rxBuffer.indexOf('\r\n\r\n');
      if (headerEnd === -1) {
        if (this.rxBuffer.length > 16384) this._fail(new Error('HTTP header overflow'));
        return;
      }
      const headerStr = this.rxBuffer.subarray(0, headerEnd).toString('utf8');
      this.rxBuffer = this.rxBuffer.subarray(headerEnd + 4);
      const lines = headerStr.split('\r\n');
      const statusLine = lines[0] || '';
      const match = statusLine.match(/^HTTP\/1\.[01]\s+(\d{3})/);
      const statusCode = match ? Number(match[1]) : 0;
      if (statusCode !== 101) {
        this._fail(new Error(`Unexpected HTTP status ${statusCode}: ${statusLine}`));
        return;
      }
      let acceptHeader = '';
      for (let i = 1; i < lines.length; i++) {
        const idx = lines[i].indexOf(':');
        if (idx > 0 && lines[i].slice(0, idx).trim().toLowerCase() === 'sec-websocket-accept') {
          acceptHeader = lines[i].slice(idx + 1).trim();
        }
      }
      if (acceptHeader !== this.expectedAccept) {
        this._fail(new Error('Invalid Sec-WebSocket-Accept header'));
        return;
      }
      clearTimeout(this.connectTimeoutTimer);
      this.connectTimeoutTimer = null;
      this.handshakeDone = true;
      this.connected = true;

      const pingEvery = this.options.pingIntervalMs || 15_000;
      this.pingTimer = setInterval(() => {
        if (this.connected && !this.closed) this.ping();
      }, pingEvery);
      // Measure initial RTT immediately upon open
      this.ping();
      this.emit('open');
    }

    this._drainFrames();
  }

  _drainFrames() {
    while (this.rxBuffer.length >= 2 && !this.closed) {
      const b0 = this.rxBuffer[0];
      const b1 = this.rxBuffer[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;

      if (len === 126) {
        if (this.rxBuffer.length < 4) return;
        len = this.rxBuffer.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.rxBuffer.length < 10) return;
        const bigLen = this.rxBuffer.readBigUInt64BE(2);
        if (bigLen > BigInt(32 * 1024 * 1024)) {
          this._fail(new Error('WS frame exceeds 32MB safety cap'));
          return;
        }
        len = Number(bigLen);
        offset = 10;
      }

      const maskBytes = masked ? 4 : 0;
      if (this.rxBuffer.length < offset + maskBytes + len) return;

      let payload = this.rxBuffer.subarray(offset + maskBytes, offset + maskBytes + len);
      if (masked) {
        const mask = this.rxBuffer.subarray(offset, offset + 4);
        const unmasked = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ mask[i & 3];
        payload = unmasked;
      } else {
        payload = Buffer.from(payload);
      }
      this.rxBuffer = this.rxBuffer.subarray(offset + maskBytes + len);

      // Control frames (Close=8, Ping=9, Pong=10)
      if (opcode === 0x8) {
        this._sendFrame(0x8, Buffer.alloc(0));
        this.close();
        return;
      } else if (opcode === 0x9) {
        this._sendFrame(0x0a, payload);
        continue;
      } else if (opcode === 0x0a) {
        if (this.lastPingSentAt > 0) {
          this.rttMs = Date.now() - this.lastPingSentAt;
          this.emit('pong', this.rttMs);
        }
        continue;
      }

      // Data frames (Continuation=0, Text=1, Binary=2)
      if (opcode === 0x1 || opcode === 0x2) {
        if (fin) {
          this.messagesReceived += 1;
          this.emit('message', opcode === 0x1 ? payload.toString('utf8') : payload, this.lastRxAt);
        } else {
          this.fragmentOpcode = opcode;
          this.fragments.push(payload);
        }
      } else if (opcode === 0x0) {
        this.fragments.push(payload);
        if (fin) {
          const full = Buffer.concat(this.fragments);
          const op = this.fragmentOpcode;
          this.fragments = [];
          this.fragmentOpcode = 0;
          this.messagesReceived += 1;
          this.emit('message', op === 0x1 ? full.toString('utf8') : full, this.lastRxAt);
        }
      }
    }
  }

  _sendFrame(opcode, payloadBuf) {
    if (!this.socket || this.closed || !this.connected) return false;
    const len = payloadBuf.length;
    let headerLen = 2;
    if (len >= 126 && len <= 65535) headerLen = 4;
    else if (len > 65535) headerLen = 10;

    const frame = Buffer.allocUnsafe(headerLen + 4 + len);
    frame[0] = 0x80 | (opcode & 0x0f);
    if (len < 126) {
      frame[1] = 0x80 | len;
    } else if (len <= 65535) {
      frame[1] = 0x80 | 126;
      frame.writeUInt16BE(len, 2);
    } else {
      frame[1] = 0x80 | 127;
      frame.writeBigUInt64BE(BigInt(len), 2);
    }

    const mask = crypto.randomBytes(4);
    mask.copy(frame, headerLen);
    for (let i = 0; i < len; i++) {
      frame[headerLen + 4 + i] = payloadBuf[i] ^ mask[i & 3];
    }
    try {
      this.socket.write(frame);
      return true;
    } catch (_) {
      return false;
    }
  }

  send(data) {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(String(data), 'utf8');
    return this._sendFrame(0x1, buf);
  }

  ping() {
    this.lastPingSentAt = Date.now();
    return this._sendFrame(0x9, Buffer.from('dna'));
  }

  _cleanupTimers() {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    if (this.connectTimeoutTimer) { clearTimeout(this.connectTimeoutTimer); this.connectTimeoutTimer = null; }
  }

  _fail(err) {
    this._cleanupTimers();
    const wasOpen = this.connected;
    this.connected = false;
    this.closed = true;
    if (this.socket) {
      try { this.socket.destroy(); } catch (_) {}
    }
    this.emit('error', err);
    this.emit('close', { clean: false, wasOpen, error: err.message });
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    this._cleanupTimers();
    if (this.socket) {
      try {
        this.socket.end();
        setTimeout(() => { try { this.socket.destroy(); } catch (_) {} }, 300).unref();
      } catch (_) {}
    }
    this.emit('close', { clean: true });
  }
}

module.exports = { WsClient };
