# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Scope

This repo is a reverse tunnel for reaching a target that's only reachable from a
locked-down VDI, from your own host machine, by relaying raw TCP over WebSocket
through a public Render relay. It is built for a pen-test lab the user owns and is
authorized to test — keep changes aligned with that scope (e.g. don't add features
aimed at reaching arbitrary/unauthorized targets or evading detection).

## Architecture

Three processes, always outbound-only (nothing needs to be reachable inbound):

```
your host                         Render                         VDI (locked down)
+-----------+   wss :443   +-----------------+   wss :443   +------------------+   TCP
| local.py  | -----------> |    relay.py     | <----------- |     agent.py     | ----> target
| (listener)|              | (rendezvous)    |  via HTTP    | (exit node)      |
+-----------+              +-----------------+  CONNECT     +------------------+
                                                 proxy 3128
```

- **`relay/relay.py`** — aiohttp broker deployed to Render. Pairs one persistent
  "worker" control connection (`GET /agent`, one per room) with short-lived
  "initiator" connections (`GET /client`, one per stream). When `local.py` opens
  a `/client` stream, the relay generates a `sid`, pushes a JSON `{"type":
  "connect", "sid", "target"}` message down the agent's control channel, then
  waits for the agent to open a matching `/agent/data?sid=...` channel and pipes
  raw bytes between the two WebSockets (`_pipe`). Rooms let multiple independent
  agent/local pairs share one relay (`?room=` on every endpoint). Every endpoint
  requires `?token=` matching `TUNNEL_TOKEN` (`ok_token`).
- **`client/agent.py`** — runs on the VDI. Dials the relay's `/agent` control
  channel (through the corporate HTTP CONNECT proxy, since that's the VDI's only
  way out), and for each `connect` message opens a `/agent/data` WebSocket, dials
  the real target (`dial_target`, directly or via the same proxy with
  `--target-via-proxy`), and bridges the two with `pump.bridge`.
- **`client/local.py`** — runs on the user's host. Listens on a local TCP port;
  each accepted connection opens a `/client` WebSocket to the relay (with
  `?target=host:port`) and bridges it to the local socket with `pump.bridge`.
  One running agent can serve different targets from different `local.py`
  listeners (target is chosen by the initiator, not the agent).
- **`client/wsclient.py`** — hand-rolled RFC 6455 WebSocket client (`WSConn`)
  plus HTTP CONNECT proxy tunneling (`proxy_connect`), built on blocking sockets
  + threads. Deliberately **pure standard library** — this file (and `pump.py`,
  `agent.py`) is copied onto the VDI where nothing can be `pip install`ed. Do not
  add third-party dependencies to any file the VDI runs.
- **`client/pump.py`** — `bridge(ws, sock)`: spins up two threads to pump bytes
  bidirectionally between a `WSConn` and a raw socket until either side closes.
- **`client/httppoll.py`** — fallback transport (`PollClient`, `agent.py
  --transport poll`) for a proxy that lets plain HTTPS through but strips
  WebSocket upgrade headers (some inspecting corporate gateways do this even
  over a working HTTP CONNECT tunnel). Uses `http.client`'s built-in
  `set_tunnel()` instead of the hand-rolled WS framing. `relay.py` mirrors this
  with a parallel set of endpoints (`/agent/poll`, `/stream/send`,
  `/stream/recv`, `/stream/close`) built on short-lived long-poll
  request/response cycles rather than a persistent/streamed connection —
  necessary because some gateways also fully buffer streamed response bodies
  instead of relaying them in real time (verified locally with the relay's
  `/probe-stream` diagnostic endpoint). The initiator side (`local.py`) is
  unaffected either way; a room's worker can be on either transport
  transparently (`worker_present()` / `PollSession` in `relay.py`).

TLS from the user's tool to the actual target is end-to-end and opaque to the
tunnel — the tunnel only ever carries bytes. The client connecting through
`local.py` must supply the target's real hostname via SNI/`Host` (see README
`--resolve`/`--connect-to` example), since `local.py` itself binds to
`127.0.0.1`.

## Running

There's no build step or test suite; this is small enough to run directly.

- Relay (needs `aiohttp`, installed via `relay/requirements.txt`):
  `python relay/relay.py` (reads `PORT` and `TUNNEL_TOKEN` from env; deploy via
  `render.yaml` as a Render Blueprint).
- Agent (stdlib only, run from `client/`, same dir as `wsclient.py`/`pump.py`):
  `python3 agent.py --relay wss://<host> --token TOKEN --proxy 127.0.0.1:3128 --default-target host:port`
- Local listener (stdlib only, run from `client/`):
  `python3 local.py --relay wss://<host> --token TOKEN --listen 127.0.0.1:8443 --target host:port`

When editing `agent.py`, `wsclient.py`, or `pump.py`, remember they need to run
unmodified on a locked-down VDI with only a stock Python 3 install — no venv, no
pip.
