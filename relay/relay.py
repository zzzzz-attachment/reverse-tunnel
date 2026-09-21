#!/usr/bin/env python3
"""
Connection broker.

A small public rendezvous service. Two WebSocket clients connect outbound to
it; the broker pairs them and forwards raw bytes between them. One client (the
worker) holds a persistent control channel; the other (the initiator) opens a
short-lived connection per stream. Neither client needs to be reachable
inbound.

Endpoints (WebSocket transport):
    GET /            health check
    GET /agent       worker control channel (one per room)
    GET /agent/data  worker per-stream data channel (?sid=...)
    GET /client      initiator per-stream channel (?target=host:port)

Endpoints (long-poll transport, for a worker whose only egress is a proxy
that strips WebSocket upgrade headers - e.g. an inspecting corporate
gateway; the initiator side always uses the WebSocket transport above):
    GET  /agent/poll   long-poll for the next pending stream (?room=...)
    POST /stream/send  push bytes for a stream (?room=...&sid=...)
    GET  /stream/recv  long-poll for bytes for a stream (?room=...&sid=...)
    POST /stream/close signal a stream is done (?room=...&sid=...)

A room's worker can be either transport; the initiator (/client) doesn't
need to know which - it always talks to a plain WebSocketResponse, and this
service bridges that to whichever transport the worker is using.

Auth: set TUNNEL_TOKEN in the environment; every endpoint requires ?token=.
"""

import asyncio
import json
import logging
import os
import time
import uuid

from aiohttp import WSMsgType, web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("svc")

TOKEN = os.environ.get("TUNNEL_TOKEN", "")
HEARTBEAT = 30
POLL_WAIT = 25          # seconds a long-poll request waits before returning empty
POLL_ALIVE_WINDOW = 40  # a poll worker counts as "connected" if it polled this recently


class Room:
    def __init__(self):
        self.agent_ctrl = None
        self.pending = {}  # sid -> {"client_ws", "done"}  (WebSocket-transport worker)
        self.poll_ctrl = asyncio.Queue()  # pending {"sid","target"} for a poll-transport worker
        self.poll_last_seen = 0.0
        self.poll_sessions = {}  # sid -> PollSession  (poll-transport worker)


rooms = {}


def get_room(name):
    room = rooms.get(name)
    if room is None:
        room = Room()
        rooms[name] = room
    return room


def worker_present(room):
    if room.agent_ctrl is not None and not room.agent_ctrl.closed:
        return True
    return (time.time() - room.poll_last_seen) < POLL_ALIVE_WINDOW


class PollSession:
    """Bridges a /client WebSocket to a worker that's polling over plain HTTP."""

    def __init__(self, client_ws):
        self.client_ws = client_ws
        self.to_worker = asyncio.Queue()  # bytes (or None for "client closed")
        self.to_client = asyncio.Queue()  # bytes (or None for "worker closed")
        self.closed = asyncio.Event()


def ok_token(request):
    return (not TOKEN) or request.query.get("token") == TOKEN


async def health(request):
    return web.Response(text="ok\n")


async def agent_ctrl(request):
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room_name = request.query.get("room", "default")
    room = get_room(room_name)
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT)
    await ws.prepare(request)
    room.agent_ctrl = ws
    log.info("worker control connected [room=%s]", room_name)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        if room.agent_ctrl is ws:
            room.agent_ctrl = None
        log.info("worker control disconnected [room=%s]", room_name)
    return ws


async def _pipe(a, b):
    async def one(src, dst):
        try:
            async for msg in src:
                if msg.type == WSMsgType.BINARY:
                    await dst.send_bytes(msg.data)
                elif msg.type == WSMsgType.TEXT:
                    await dst.send_bytes(msg.data.encode())
                else:
                    break
        except Exception:
            pass

    t1 = asyncio.create_task(one(a, b))
    t2 = asyncio.create_task(one(b, a))
    await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
    for t in (t1, t2):
        t.cancel()
    for w in (a, b):
        try:
            await w.close()
        except Exception:
            pass


async def agent_data(request):
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room = get_room(request.query.get("room", "default"))
    sid = request.query.get("sid", "")
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT, max_msg_size=0)
    await ws.prepare(request)
    sess = room.pending.pop(sid, None)
    if not sess:
        log.warning("data channel for unknown sid=%s", sid)
        await ws.close()
        return ws
    await _pipe(sess["client_ws"], ws)
    sess["done"].set()
    return ws


async def _poll_pipe(sess):
    async def ws_to_queue():
        try:
            async for msg in sess.client_ws:
                if msg.type == WSMsgType.BINARY:
                    await sess.to_worker.put(msg.data)
                elif msg.type == WSMsgType.TEXT:
                    await sess.to_worker.put(msg.data.encode())
                else:
                    break
        except Exception:
            pass
        finally:
            await sess.to_worker.put(None)
            sess.closed.set()

    async def queue_to_ws():
        try:
            while True:
                data = await sess.to_client.get()
                if data is None:
                    break
                await sess.client_ws.send_bytes(data)
        except Exception:
            pass
        finally:
            sess.closed.set()

    t1 = asyncio.create_task(ws_to_queue())
    t2 = asyncio.create_task(queue_to_ws())
    await sess.closed.wait()
    for t in (t1, t2):
        t.cancel()
    try:
        await sess.client_ws.close()
    except Exception:
        pass


async def client_conn(request):
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room_name = request.query.get("room", "default")
    room = get_room(room_name)
    target = request.query.get("target", "")
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT, max_msg_size=0)
    await ws.prepare(request)

    if not worker_present(room):
        log.warning("initiator connected but no worker in room=%s", room_name)
        await ws.close()
        return ws

    sid = uuid.uuid4().hex

    if room.agent_ctrl is not None and not room.agent_ctrl.closed:
        done = asyncio.Event()
        room.pending[sid] = {"client_ws": ws, "done": done}
        try:
            await room.agent_ctrl.send_str(
                json.dumps({"type": "connect", "sid": sid, "target": target})
            )
        except Exception as e:
            log.warning("failed to notify worker: %s", e)
            room.pending.pop(sid, None)
            await ws.close()
            return ws

        log.info("stream %s [room=%s] target=%s", sid, room_name, target)
        try:
            await asyncio.wait_for(done.wait(), timeout=30 * 60)
        except asyncio.TimeoutError:
            room.pending.pop(sid, None)
            try:
                await ws.close()
            except Exception:
                pass
        return ws

    sess = PollSession(ws)
    room.poll_sessions[sid] = sess
    await room.poll_ctrl.put({"sid": sid, "target": target})
    log.info("stream %s [room=%s] target=%s (poll worker)", sid, room_name, target)
    await _poll_pipe(sess)
    room.poll_sessions.pop(sid, None)
    return ws


async def poll_ctrl(request):
    """Poll-transport worker: long-poll for the next pending stream."""
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room = get_room(request.query.get("room", "default"))
    room.poll_last_seen = time.time()
    try:
        item = await asyncio.wait_for(room.poll_ctrl.get(), timeout=POLL_WAIT)
    except asyncio.TimeoutError:
        return web.Response(status=204)
    return web.json_response(item)


async def poll_send(request):
    """Poll-transport worker: push bytes destined for the initiator."""
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room = get_room(request.query.get("room", "default"))
    sess = room.poll_sessions.get(request.query.get("sid", ""))
    if sess is None:
        return web.Response(status=410, text="unknown or closed stream")
    data = await request.read()
    if data:
        await sess.to_client.put(data)
    return web.Response(status=204)


async def poll_recv(request):
    """Poll-transport worker: long-poll for bytes sent by the initiator."""
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room = get_room(request.query.get("room", "default"))
    sess = room.poll_sessions.get(request.query.get("sid", ""))
    if sess is None:
        return web.Response(status=410, text="unknown or closed stream")
    try:
        data = await asyncio.wait_for(sess.to_worker.get(), timeout=POLL_WAIT)
    except asyncio.TimeoutError:
        return web.Response(status=204)
    if data is None:
        return web.Response(status=410, text="closed")
    return web.Response(body=data, content_type="application/octet-stream")


async def poll_close(request):
    """Poll-transport worker: signal that it's done with a stream."""
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room = get_room(request.query.get("room", "default"))
    sess = room.poll_sessions.get(request.query.get("sid", ""))
    if sess is not None:
        await sess.to_client.put(None)
    return web.Response(status=204)


async def probe_stream(request):
    """Diagnostic: writes 3 chunks a few seconds apart so a client behind a
    proxy can tell whether it sees them trickle in (streaming works) or all
    at once at the end (the proxy is buffering the whole body)."""
    resp = web.StreamResponse(headers={"Content-Type": "text/plain"})
    resp.enable_chunked_encoding()
    await resp.prepare(request)
    for i in range(1, 4):
        await resp.write(("chunk-%d %s\n" % (i, time.strftime("%H:%M:%S"))).encode())
        if i < 3:
            await asyncio.sleep(3)
    await resp.write_eof()
    return resp


def make_app():
    app = web.Application()
    app.add_routes([
        web.get("/", health),
        web.get("/agent", agent_ctrl),
        web.get("/agent/data", agent_data),
        web.get("/client", client_conn),
        web.get("/agent/poll", poll_ctrl),
        web.post("/stream/send", poll_send),
        web.get("/stream/recv", poll_recv),
        web.post("/stream/close", poll_close),
        web.get("/probe-stream", probe_stream),
    ])
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    if not TOKEN:
        log.warning("TUNNEL_TOKEN is empty - service is UNAUTHENTICATED")
    web.run_app(make_app(), host="0.0.0.0", port=port)
