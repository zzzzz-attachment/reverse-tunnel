#!/usr/bin/env python3
"""
Local listener.

Runs on your own machine. Listens on a local TCP port; for each incoming
connection it opens a WebSocket to the broker, which pairs it with the worker.
Point your browser or tool at the local port to reach the target.

Example:
    python3 local.py \
        --relay wss://status-api.onrender.com \
        --token YOUR_TOKEN \
        --listen 127.0.0.1:8443 \
        --target example.com:443

Then, e.g.:  curl https://example.com/ --connect-to example.com:443:127.0.0.1:8443
or add a hosts entry / use SNI so your client speaks TLS straight to the target.
"""

import argparse
import socket
import sys
import threading
import time
from urllib.parse import urlencode

from wsclient import WSConn
from pump import bridge


def log(*a):
    print(time.strftime("%H:%M:%S"), "[local]", *a, file=sys.stderr, flush=True)


def handle_conn(args, conn, addr):
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


def run(args):
    host, port = args.listen.rsplit(":", 1)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, int(port)))
    srv.listen(128)
    log("listening on", args.listen, "-> target", args.target, "via", args.relay)
    while True:
        conn, addr = srv.accept()
        threading.Thread(
            target=handle_conn, args=(args, conn, addr), daemon=True
        ).start()


def main():
    p = argparse.ArgumentParser(description="Local forwarding listener")
    p.add_argument("--relay", required=True, help="wss://<name>.onrender.com")
    p.add_argument("--token", default="", help="shared TUNNEL_TOKEN")
    p.add_argument("--room", default="default", help="room name to pair with agent.py")
    p.add_argument("--listen", default="127.0.0.1:8443", help="local bind host:port")
    p.add_argument("--target", required=True,
                   help="target host:port the worker should dial")
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
