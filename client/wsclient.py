"""
Minimal WebSocket client (RFC 6455) with optional HTTP CONNECT proxy support.

Pure standard library, so it runs anywhere Python is available with no extra
packages to install. Blocking sockets + threads; used by agent.py and local.py.
"""

import base64
import os
import socket
import ssl
import struct
import threading
from urllib.parse import urlsplit

OP_CONT = 0x0
OP_TEXT = 0x1
OP_BIN = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


def _read_headers(sock):
    """Read an HTTP response header block (up to the blank line) without
    consuming any bytes past it, so the following WebSocket frames stay intact."""
    data = b""
    while b"\r\n\r\n" not in data:
        ch = sock.recv(1)
        if not ch:
            raise ConnectionError("connection closed while reading HTTP headers")
        data += ch
        if len(data) > 65536:
            raise ConnectionError("HTTP header block too large")
    return data.decode("latin-1")


def proxy_connect(proxy, host, port, proxy_auth=None, timeout=30):
    """Open a raw TCP tunnel to host:port through an HTTP CONNECT proxy.

    proxy: "host:port". proxy_auth: optional "user:pass" for Basic auth.
    Returns a connected socket carrying a transparent byte tunnel (no TLS).
    """
    phost, pport = proxy.rsplit(":", 1)
    sock = socket.create_connection((phost, int(pport)), timeout)
    lines = [
        "CONNECT %s:%d HTTP/1.1" % (host, port),
        "Host: %s:%d" % (host, port),
        "Proxy-Connection: keep-alive",
    ]
    if proxy_auth:
        token = base64.b64encode(proxy_auth.encode()).decode()
        lines.append("Proxy-Authorization: Basic %s" % token)
    sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
    resp = _read_headers(sock)
    status = resp.split("\r\n", 1)[0]
    parts = status.split(" ", 2)
    if len(parts) < 2 or parts[1] != "200":
        sock.close()
        raise ConnectionError("proxy CONNECT failed: %s" % status)
    return sock


class WSConn:
    def __init__(self, sock):
        self.sock = sock
        self._send_lock = threading.Lock()
        self._closed = False

    @classmethod
    def connect(cls, url, proxy=None, proxy_auth=None, headers=None, timeout=30):
        """Establish a WebSocket connection to a ws:// or wss:// URL.

        If proxy ("host:port") is given, tunnel out via HTTP CONNECT first
        (the only outbound path on the VDI). TLS is negotiated after the
        tunnel, end-to-end to the relay host.
        """
        u = urlsplit(url)
        secure = u.scheme == "wss"
        host = u.hostname
        port = u.port or (443 if secure else 80)
        path = u.path or "/"
        if u.query:
            path += "?" + u.query

        if proxy:
            sock = proxy_connect(proxy, host, port, proxy_auth, timeout)
        else:
            sock = socket.create_connection((host, port), timeout)

        if secure:
            ctx = ssl.create_default_context()
            sock = ctx.wrap_socket(sock, server_hostname=host)

        key = base64.b64encode(os.urandom(16)).decode()
        req = [
            "GET %s HTTP/1.1" % path,
            "Host: %s:%d" % (host, port),
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Key: %s" % key,
            "Sec-WebSocket-Version: 13",
        ]
        for k, v in (headers or {}).items():
            req.append("%s: %s" % (k, v))
        sock.sendall(("\r\n".join(req) + "\r\n\r\n").encode("latin-1"))

        resp = _read_headers(sock)
        status = resp.split("\r\n", 1)[0]
        if "101" not in status:
            sock.close()
            raise ConnectionError("WebSocket handshake failed: %s" % status)

        sock.settimeout(None)
        return cls(sock)

    def _send_frame(self, opcode, data=b""):
        if isinstance(data, str):
            data = data.encode()
        length = len(data)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header += struct.pack("!H", length)
        else:
            header.append(0x80 | 127)
            header += struct.pack("!Q", length)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i & 3] for i, b in enumerate(data))
        with self._send_lock:
            self.sock.sendall(bytes(header) + masked)

    def send_bytes(self, data):
        self._send_frame(OP_BIN, data)

    def send_text(self, data):
        self._send_frame(OP_TEXT, data)

    def _recv_exact(self, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed")
            buf += chunk
        return bytes(buf)

    def _recv_frame(self):
        b0, b1 = self._recv_exact(2)
        fin = b0 & 0x80
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_exact(8))[0]
        mask = self._recv_exact(4) if masked else None
        payload = self._recv_exact(length) if length else b""
        if mask:
            payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        return fin, opcode, payload

    def recv(self):
        """Return (kind, data): kind is 'binary', 'text', or 'close'.
        Ping/pong are handled transparently."""
        data = bytearray()
        cur_op = None
        while True:
            fin, opcode, payload = self._recv_frame()
            if opcode == OP_CLOSE:
                try:
                    self._send_frame(OP_CLOSE)
                except Exception:
                    pass
                return ("close", b"")
            if opcode == OP_PING:
                self._send_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            if opcode in (OP_TEXT, OP_BIN):
                cur_op = opcode
                data = bytearray(payload)
            elif opcode == OP_CONT:
                data += payload
            if fin:
                if cur_op == OP_TEXT:
                    return ("text", bytes(data).decode("utf-8", "replace"))
                return ("binary", bytes(data))

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._send_frame(OP_CLOSE)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass
