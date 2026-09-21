#!/usr/bin/env python3
"""
Local listener.

Runs on your own machine. Listens on a local TCP port; for each incoming
connection it opens a WebSocket to the broker, which pairs it with the worker.

Two modes:

- Fixed target (pass --target): every connection tunnels to that one
  host:port, exactly like a plain port-forward. Use this for a single
  non-HTTP TCP service.

    python3 local.py \
        --relay wss://status-api.onrender.com \
        --token YOUR_TOKEN \
        --listen 127.0.0.1:8443 \
        --target example.com:443

    Then, e.g.:  curl https://example.com/ --connect-to example.com:443:127.0.0.1:8443
    or add a hosts entry / use SNI so your client speaks TLS straight to the target.

- Dynamic proxy (omit --target): acts as a local HTTP/HTTPS forward proxy.
  Point a browser's proxy settings at --listen and every site it visits gets
  its own tunnel stream to whatever host:port that site actually needs - the
  target is read per-connection from the CONNECT request line (HTTPS) or the
  Host header (plain HTTP), instead of being fixed up front. Use this when a
  page needs more than one upstream host (logins, APIs, SSO redirects, etc.).
"""

import argparse
import socket
import sys
import threading
import time
from urllib.parse import urlencode, urlsplit

from wsclient import WSConn
from pump import bridge


def log(*a):
    print(time.strftime("%H:%M:%S"), "[local]", *a, file=sys.stderr, flush=True)


def read_request_head(conn, limit=65536):
    """Read (and return, unconsumed by anything else) bytes up through the
    blank line ending the request headers, so the rest of the request/body
    stays intact for straight pass-through afterwards."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionError("connection closed before headers were complete")
        data += chunk
        if len(data) > limit:
            raise ConnectionError("request header too large")
    return data


def parse_target(head):
    """Return (target 'host:port', is_connect) from a proxy-style request:
    a CONNECT line for HTTPS, or an absolute-URI / Host header for plain
    HTTP. Raises ValueError if neither is present."""
    request_line = head.split(b"\r\n", 1)[0].decode("latin-1", "replace")
    parts = request_line.split(" ")
    if len(parts) < 2:
        raise ValueError("malformed request line: %r" % request_line)
    method, target_part = parts[0], parts[1]

    if method.upper() == "CONNECT":
        return target_part, True

    if target_part.startswith("http://") or target_part.startswith("https://"):
        u = urlsplit(target_part)
        port = u.port or (443 if u.scheme == "https" else 80)
        return "%s:%d" % (u.hostname, port), False

    for line in head.split(b"\r\n")[1:]:
        if line.lower().startswith(b"host:"):
            host = line.split(b":", 1)[1].strip().decode("latin-1")
            return (host if ":" in host else host + ":80"), False

    raise ValueError("no absolute-URI and no Host header; can't determine target")


def handle_conn_fixed(args, conn, addr):
    """Fixed-target mode: tunnel straight to --target."""
    q = urlencode({"token": args.token, "room": args.room, "target": args.target})
    client_url = "%s/client?%s" % (args.relay.rstrip("/"), q)
    try:
        ws = WSConn.connect(client_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
    except Exception as e:
        log("broker connect failed:", e)
        try:
            conn.close()
        except Exception:
            pass
        return
    log("conn from", "%s:%d" % addr, "->", args.target)
    bridge(ws, conn)
    log("conn from", "%s:%d" % addr, "done")


def handle_conn_proxy(args, conn, addr):
    """Dynamic-proxy mode: figure out the target per connection."""
    try:
        head = read_request_head(conn)
        target, is_connect = parse_target(head)
    except Exception as e:
        log("conn from", "%s:%d" % addr, "bad request:", e)
        try:
            conn.close()
        except Exception:
            pass
        return

    q = urlencode({"token": args.token, "room": args.room, "target": target})
    client_url = "%s/client?%s" % (args.relay.rstrip("/"), q)
    try:
        ws = WSConn.connect(client_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
    except Exception as e:
        log("broker connect failed:", e)
        try:
            conn.close()
        except Exception:
            pass
        return

    if is_connect:
        try:
            conn.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        except Exception:
            ws.close()
            conn.close()
            return
    else:
        # Not consumed by anything else, so forward it as the start of the
        # tunneled byte stream instead of dropping it.
        try:
            ws.send_bytes(head)
        except Exception:
            ws.close()
            conn.close()
            return

    log("conn from", "%s:%d" % addr, "->", target)
    bridge(ws, conn)
    log("conn from", "%s:%d" % addr, "done")


def run(args):
    handle_conn = handle_conn_fixed if args.target else handle_conn_proxy
    host, port = args.listen.rsplit(":", 1)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(128)
    if args.target:
        log("listening on", args.listen, "-> target", args.target, "via", args.relay)
    else:
        log("listening on", args.listen, "as a dynamic HTTP/HTTPS proxy via", args.relay)
    while True:
        conn, addr = srv.accept()
        threading.Thread(
            target=handle_conn, args=(args, conn, addr), daemon=True
        ).start()


def main():
    p = argparse.ArgumentParser(description="Local forwarding listener / dynamic proxy")
    p.add_argument("--relay", required=True, help="wss://<name>.onrender.com")
    p.add_argument("--token", default="", help="shared TUNNEL_TOKEN")
    p.add_argument("--room", default="default", help="room name to pair with agent.py")
    p.add_argument("--listen", default="127.0.0.1:8443", help="local bind host:port")
    p.add_argument("--target", default=None,
                   help="fixed target host:port; omit to run as a dynamic "
                        "HTTP/HTTPS proxy that reads the target per connection")
    p.add_argument("--proxy", default=None,
                   help="optional HTTP CONNECT proxy for this host (usually none)")
    p.add_argument("--proxy-auth", default=None, help="proxy Basic auth 'user:pass'")
    args = p.parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
