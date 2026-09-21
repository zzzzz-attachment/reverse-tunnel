#!/usr/bin/env python3
"""
Render relay.

A public rendezvous point. Both ends connect OUTBOUND to it (the VDI can only
make outbound HTTP CONNECT calls, so it cannot be dialed directly). The relay
pairs a host-side client connection with the VDI agent and shuttles raw bytes.

Endpoints:
    GET /            health check
    GET /agent       agent control channel (one per room)
    GET /agent/data  agent per-session data channel (?sid=...)
    GET /client      host per-connection channel (?target=host:port)

Auth: set TUNNEL_TOKEN in the environment; every endpoint requires ?token=.
"""

import asyncio
import json
import logging
import os
import uuid

from aiohttp import WSMsgType, web

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("relay")

TOKEN = os.environ.get("TUNNEL_TOKEN", "")
HEARTBEAT = 30


class Room:
    def __init__(self):
        self.agent_ctrl = None
        self.pending = {}  # sid -> {"client_ws", "done"}


rooms = {}


def get_room(name):
    room = rooms.get(name)
    if room is None:
        room = Room()
        rooms[name] = room
    return room


def ok_token(request):
    return (not TOKEN) or request.query.get("token") == TOKEN


async def health(request):
    return web.Response(text="reverse-tunnel relay ok\n")


async def agent_ctrl(request):
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room_name = request.query.get("room", "default")
    room = get_room(room_name)
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT)
    await ws.prepare(request)
    room.agent_ctrl = ws
    log.info("agent control connected [room=%s]", room_name)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        if room.agent_ctrl is ws:
            room.agent_ctrl = None
        log.info("agent control disconnected [room=%s]", room_name)
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
        log.warning("agent data for unknown sid=%s", sid)
        await ws.close()
        return ws
    await _pipe(sess["client_ws"], ws)
    sess["done"].set()
    return ws


async def client_conn(request):
    if not ok_token(request):
        return web.Response(status=403, text="forbidden")
    room_name = request.query.get("room", "default")
    room = get_room(room_name)
    target = request.query.get("target", "")
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT, max_msg_size=0)
    await ws.prepare(request)

    if room.agent_ctrl is None or room.agent_ctrl.closed:
        log.warning("client connected but no agent in room=%s", room_name)
        await ws.close()
        return ws

    sid = uuid.uuid4().hex
    done = asyncio.Event()
    room.pending[sid] = {"client_ws": ws, "done": done}
    try:
        await room.agent_ctrl.send_str(
            json.dumps({"type": "connect", "sid": sid, "target": target})
        )
    except Exception as e:
        log.warning("failed to notify agent: %s", e)
        room.pending.pop(sid, None)
        await ws.close()
        return ws

    log.info("session %s [room=%s] target=%s", sid, room_name, target)
    try:
        await asyncio.wait_for(done.wait(), timeout=30 * 60)
    except asyncio.TimeoutError:
        room.pending.pop(sid, None)
        try:
            await ws.close()
        except Exception:
            pass
    return ws


def make_app():
    app = web.Application()
    app.add_routes([
        web.get("/", health),
        web.get("/agent", agent_ctrl),
        web.get("/agent/data", agent_data),
        web.get("/client", client_conn),
    ])
    return app


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    if not TOKEN:
        log.warning("TUNNEL_TOKEN is empty - relay is UNAUTHENTICATED")
    web.run_app(make_app(), host="0.0.0.0", port=port)
