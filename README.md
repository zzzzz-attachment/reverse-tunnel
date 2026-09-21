# reverse-tunnel

A small reverse tunnel for a **pen-test lab you own**: reach a target that is only
reachable from a locked-down VDI, from your own host machine, by relaying raw TCP
over WebSocket through a public Render relay.

```
your host                         Render                         VDI (locked down)
+-----------+   wss :443   +-----------------+   wss :443   +------------------+   TCP
| local.py  | -----------> |    relay.py     | <----------- |     agent.py     | ----> target
| (listener)|              | (rendezvous)    |  via HTTP    | (exit node)      |      (e.g.
+-----------+              +-----------------+  CONNECT     +------------------+       example.com:443)
     ^                                          proxy 3128
 browser / curl / tool
```

Both ends dial **outbound** to the relay — the VDI can't be reached directly, and
its only way out is the corporate HTTP CONNECT proxy at `127.0.0.1:3128`, which is
allowed to reach `*.onrender.com`. TLS from your host tool to the target stays
**end-to-end**; the tunnel just carries bytes.

The VDI side (`agent.py`, `wsclient.py`, `pump.py`) is **pure standard library** —
nothing to `pip install` on the locked-down box.

## 1. Deploy the relay to Render

Push this repo to GitHub and create a **Blueprint** from `render.yaml`, or make a
Web Service manually:

- Build: `pip install -r relay/requirements.txt`
- Start: `python relay/relay.py`
- Env: `TUNNEL_TOKEN` = a strong shared secret (the blueprint generates one)

Note the service URL, e.g. `https://status-api.onrender.com`. Use it as
`wss://status-api.onrender.com` for the clients. Health check: open the
URL in a browser, you should see `reverse-tunnel relay ok`.

> Free plan sleeps when idle and cold-starts on the first request; fine for a lab.

## 2. Run the agent on the VDI

Copy `client/wsclient.py`, `client/pump.py`, `client/agent.py` to the VDI, then:

```bash
python3 agent.py \
  --relay wss://status-api.onrender.com \
  --token YOUR_TOKEN \
  --proxy 127.0.0.1:3128 \
  --default-target example.com:443
```

- `--proxy` is the corporate proxy used to reach the relay (the only way out).
- `--proxy-auth user:pass` if the proxy needs Basic auth.
- If the target is **external** and also only reachable via the corporate proxy,
  add `--target-via-proxy`. If the target is on the VDI's own LAN, leave it off
  (the agent dials it directly).

## 3. Run the listener on your host

```bash
python3 client/local.py \
  --relay wss://status-api.onrender.com \
  --token YOUR_TOKEN \
  --listen 127.0.0.1:8443 \
  --target example.com:443
```

`--target` tells the agent what to dial for each connection, so one running agent
can serve different targets from different listeners.

## 4. Use it

Point your tool at the local port. Because TLS is end-to-end, your client must send
the target's real hostname (SNI + `Host`) while connecting to `127.0.0.1`:

```bash
curl https://example.com/ --resolve example.com:8443:127.0.0.1 --connect-to example.com:443:127.0.0.1:8443
# or add "127.0.0.1 example.com" to /etc/hosts and browse https://example.com:8443/
```

For a plain HTTP target, just `curl http://127.0.0.1:8443/`.

## Notes

- **Rooms**: run isolated pairs with matching `--room NAME` on agent and listener.
- **Auth**: every relay endpoint requires `?token=`; keep `TUNNEL_TOKEN` set. The
  token rides in the URL query — fine over the wss/TLS leg; treat relay logs as
  sensitive.
- **Concurrency**: each accepted local connection is its own WebSocket session, so
  parallel requests (e.g. a browser) work.
- **Scope**: this is for a lab environment you own and are authorized to test.
