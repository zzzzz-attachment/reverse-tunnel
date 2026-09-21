#!/usr/bin/env python3
"""
VDI-side agent (the exit node).

Runs on the locked-down VDI. Holds a control WebSocket to the Render relay
(punched out through the corporate HTTP proxy). For each session the relay
announces, it opens a data WebSocket and dials the lab target, then bridges
raw bytes between them. TLS from your host app to the target stays end-to-end.

Only depends on the standard library, so nothing needs installing on the VDI.

Example:
    python3 agent.py \
        --relay wss://reverse-tunnel-relay.onrender.com \
        --token YOUR_TOKEN \
        --proxy 127.0.0.1:3128 \
        --default-target example.com:443
"""

import argparse
import json
import socket
import sys
import threading
import time
from urllib.parse import urlencode

from wsclient import WSConn, proxy_connect
from pump import bridge


def log(*a):
    print(time.strftime("%H:%M:%S"), "[agent]", *a, file=sys.stderr, flush=True)


def dial_target(target, proxy, proxy_auth):
    host, port = target.rsplit(":", 1)
    port = int(port)
    if proxy:
        # Target is external / only reachable via the corporate proxy.
        return proxy_connect(proxy, host, port, proxy_auth)
    # Target is directly reachable from the VDI's network.
    return socket.create_connection((host, port), 30)


def handle_session(args, sid, target):
    q = urlencode({"sid": sid, "token": args.token, "room": args.room})
    data_url = "%s/agent/data?%s" % (args.relay.rstrip("/"), q)
    try:
        ws = WSConn.connect(data_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
    except Exception as e:
        log("session", sid, "data WS failed:", e)
        return

    tproxy = args.proxy if args.target_via_proxy else None
    try:
        tsock = dial_target(target, tproxy, args.proxy_auth)
    except Exception as e:
        log("session", sid, "target", target, "connect failed:", e)
        ws.close()
        return

    log("session", sid, "->", target, "open")
    bridge(ws, tsock)
    log("session", sid, "closed")


def run(args):
    q = urlencode({"token": args.token, "room": args.room})
    ctrl_url = "%s/agent?%s" % (args.relay.rstrip("/"), q)
    backoff = 1
    while True:
        try:
            log("connecting control channel via proxy", args.proxy or "(direct)")
            ctrl = WSConn.connect(ctrl_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
            log("control channel up; waiting for sessions")
            backoff = 1
            while True:
                kind, data = ctrl.recv()
                if kind == "close":
                    raise ConnectionError("relay closed control channel")
                if kind != "text":
                    continue
                msg = json.loads(data)
                if msg.get("type") == "connect":
                    sid = msg["sid"]
                    target = msg.get("target") or args.default_target
                    if not target:
                        log("session", sid, "no target and no --default-target; skipping")
                        continue
                    threading.Thread(
                        target=handle_session, args=(args, sid, target), daemon=True
                    ).start()
        except KeyboardInterrupt:
            return
        except Exception as e:
            log("control channel error:", e, "- reconnecting in", backoff, "s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    p = argparse.ArgumentParser(description="VDI-side reverse tunnel agent")
    p.add_argument("--relay", required=True, help="wss://<name>.onrender.com")
    p.add_argument("--token", default="", help="shared TUNNEL_TOKEN")
    p.add_argument("--room", default="default", help="room name to pair with local.py")
    p.add_argument("--proxy", default="127.0.0.1:3128",
                   help="corporate HTTP CONNECT proxy host:port (used to reach the relay)")
    p.add_argument("--proxy-auth", default=None, help="proxy Basic auth 'user:pass'")
    p.add_argument("--default-target", default=None,
                   help="fallback target host:port if local.py sends none")
    p.add_argument("--target-via-proxy", action="store_true",
                   help="reach the target through the same corporate proxy "
                        "(use when the target is external, not on the VDI LAN)")
    args = p.parse_args()
    run(args)


if __name__ == "__main__":
    main()
