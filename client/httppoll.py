"""
Long-poll HTTP client: a fallback transport for a corporate proxy that lets
plain request/response HTTPS through but strips the WebSocket upgrade
headers (see wsclient.py for the normal path, and relay.py for why this
exists). Pure standard library - uses http.client's built-in CONNECT-tunnel
support (HTTPConnection.set_tunnel) instead of a hand-rolled proxy dance,
since there's no WebSocket framing to hand-roll here.
"""

import base64
import http.client
from urllib.parse import urlsplit


class PollClient:
    """One keep-alive HTTPS connection, reused across repeated requests
    against a single relay. Reconnects once, transparently, on any
    transport-level failure (proxies and idle timeouts drop connections)."""

    def __init__(self, relay, proxy=None, proxy_auth=None, timeout=35):
        u = urlsplit(relay)
        self.host = u.hostname
        self.port = u.port or 443
        self.proxy = proxy
        self.proxy_auth = proxy_auth
        self.timeout = timeout
        self.conn = None

    def _connect(self):
        if self.proxy:
            phost, pport = self.proxy.rsplit(":", 1)
            conn = http.client.HTTPSConnection(phost, int(pport), timeout=self.timeout)
            tunnel_headers = {}
            if self.proxy_auth:
                tok = base64.b64encode(self.proxy_auth.encode()).decode()
                tunnel_headers["Proxy-Authorization"] = "Basic %s" % tok
            conn.set_tunnel(self.host, self.port, headers=tunnel_headers or None)
        else:
            conn = http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout)
        return conn

    def request(self, method, path, body=None):
        """Issue one request, return (status, body_bytes). Retries once
        (with a fresh connection) on any transport-level error."""
        for attempt in (1, 2):
            try:
                if self.conn is None:
                    self.conn = self._connect()
                self.conn.request(method, path, body=body)
                resp = self.conn.getresponse()
                data = resp.read()
                return resp.status, data
            except Exception:
                self.close()
                if attempt == 2:
                    raise

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
