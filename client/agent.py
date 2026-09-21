#!/usr/bin/env python3
"""
Worker.

Holds a persistent control WebSocket to the broker (dialed outbound, optionally
through an HTTP CONNECT proxy). For each stream the broker announces, it opens a
data WebSocket and dials the requested target, then forwards raw bytes between
them. TLS between the caller and the target stays end-to-end.

Standard library only, so nothing extra needs installing.

Example:
    python3 agent.py \
        --relay wss://status-api.onrender.com \
        --token YOUR_TOKEN \
        --proxy 127.0.0.1:3128 \
        --default-target example.com:443

If the egress proxy strips WebSocket upgrade headers (common on inspecting
corporate gateways - the control channel will fail with "WebSocket handshake
failed: HTTP/1.1 400 Bad Request" on every retry), add --transport poll to
fall back to plain HTTPS long-polling instead. Check with local.py first:
if it also can't complete a WebSocket handshake from wherever it's running,
the relay itself is unreachable and --transport poll won't help either.
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
from httppoll import PollClient


def log(*a):
    print(time.strftime("%H:%M:%S"), "[worker]", *a, file=sys.stderr, flush=True)


def dial_target(target, proxy, proxy_auth):
    host, port = target.rsplit(":", 1)
    port = int(port)
    if proxy:
        # Reach the target through the HTTP proxy.
        return proxy_connect(proxy, host, port, proxy_auth)
    # Target is directly reachable from this host's network.
    return socket.create_connection((host, port), 30)


def handle_session(args, sid, target):
    q = urlencode({"sid": sid, "token": args.token, "room": args.room})
    data_url = "%s/agent/data?%s" % (args.relay.rstrip("/"), q)
    try:
        ws = WSConn.connect(data_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
    except Exception as e:
        log("stream", sid, "data WS failed:", e)
        return

    tproxy = args.proxy if args.target_via_proxy else None
    try:
        tsock = dial_target(target, tproxy, args.proxy_auth)
    except Exception as e:
        log("stream", sid, "target", target, "connect failed:", e)
        ws.close()
        return

    log("stream", sid, "->", target, "open")
    bridge(ws, tsock)
    log("stream", sid, "closed")


def run(args):
    q = urlencode({"token": args.token, "room": args.room})
    ctrl_url = "%s/agent?%s" % (args.relay.rstrip("/"), q)
    backoff = 1
    while True:
        try:
            log("connecting control channel via proxy", args.proxy or "(direct)")
            ctrl = WSConn.connect(ctrl_url, proxy=args.proxy, proxy_auth=args.proxy_auth)
            log("control channel up; waiting for streams")
            backoff = 1
            while True:
                kind, data = ctrl.recv()
                if kind == "close":
                    raise ConnectionError("broker closed control channel")
                if kind != "text":
                    continue
                msg = json.loads(data)
                if msg.get("type") == "connect":
                    sid = msg["sid"]
                    target = msg.get("target") or args.default_target
                    if not target:
                        log("stream", sid, "no target and no --default-target; skipping")
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


def handle_session_poll(args, sid, target):
    """Same job as handle_session, but over long-poll HTTP instead of a data
    WebSocket: one thread posts target->relay bytes, another long-polls for
    relay->target bytes. Used when the egress proxy strips WS upgrade headers."""
    tproxy = args.proxy if args.target_via_proxy else None
    try:
        tsock = dial_target(target, tproxy, args.proxy_auth)
    except Exception as e:
        log("stream", sid, "target", target, "connect failed:", e)
        return

    q = urlencode({"token": args.token, "room": args.room, "sid": sid})
    base = args.relay.rstrip("/")
    send_path, recv_path, close_path = (
        "/stream/send?%s" % q, "/stream/recv?%s" % q, "/stream/close?%s" % q,
    )
    stop = threading.Event()
    sender = PollClient(base, proxy=args.proxy, proxy_auth=args.proxy_auth)
    receiver = PollClient(base, proxy=args.proxy, proxy_auth=args.proxy_auth)

    def target_to_relay():
        try:
            while not stop.is_set():
                data = tsock.recv(65536)
                if not data:
                    break
                status, _ = sender.request("POST", send_path, body=data)
                if status not in (200, 204):
                    break
        except Exception:
            pass
        finally:
            stop.set()
            try:
                sender.request("POST", close_path)
            except Exception:
                pass
            sender.close()

    def relay_to_target():
        try:
            while not stop.is_set():
                status, data = receiver.request("GET", recv_path)
                if status == 200:
                    if data:
                        tsock.sendall(data)
                elif status == 204:
                    continue
                else:
                    break
        except Exception:
            pass
        finally:
            stop.set()
            receiver.close()
            try:
                tsock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                tsock.close()
            except Exception:
                pass

    log("stream", sid, "->", target, "open (poll)")
    t1 = threading.Thread(target=target_to_relay, daemon=True)
    t2 = threading.Thread(target=relay_to_target, daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    log("stream", sid, "closed")


def run_poll(args):
    base = args.relay.rstrip("/")
    q = urlencode({"token": args.token, "room": args.room})
    ctrl_path = "/agent/poll?%s" % q
    ctrl = PollClient(base, proxy=args.proxy, proxy_auth=args.proxy_auth)
    log("polling control channel via proxy", args.proxy or "(direct)")
    backoff = 1
    while True:
        try:
            status, data = ctrl.request("GET", ctrl_path)
            if status == 200:
                msg = json.loads(data)
                sid = msg["sid"]
                target = msg.get("target") or args.default_target
                backoff = 1
                if not target:
                    log("stream", sid, "no target and no --default-target; skipping")
                    continue
                threading.Thread(
                    target=handle_session_poll, args=(args, sid, target), daemon=True
                ).start()
            elif status == 204:
                backoff = 1
            else:
                raise ConnectionError("control poll failed: HTTP %d" % status)
        except KeyboardInterrupt:
            return
        except Exception as e:
            ctrl.close()
            log("control poll error:", e, "- retrying in", backoff, "s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


def main():
    p = argparse.ArgumentParser(description="WebSocket forwarding worker")
    p.add_argument("--relay", required=True, help="wss://<name>.onrender.com")
    p.add_argument("--token", default="", help="shared TUNNEL_TOKEN")
    p.add_argument("--room", default="default", help="room name to pair with local.py")
    p.add_argument("--proxy", default="127.0.0.1:3128",
                   help="HTTP CONNECT proxy host:port used to reach the broker")
    p.add_argument("--proxy-auth", default=None, help="proxy Basic auth 'user:pass'")
    p.add_argument("--default-target", default=None,
                   help="fallback target host:port if local.py sends none")
    p.add_argument("--target-via-proxy", action="store_true",
                   help="reach the target through the same HTTP proxy "
                        "(use when the target is not on this host's LAN)")
    p.add_argument("--transport", choices=["ws", "poll"], default="ws",
                   help="'ws' (default) or 'poll' for a proxy that strips "
                        "WebSocket upgrade headers")
    args = p.parse_args()
    if args.transport == "poll":
        run_poll(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
