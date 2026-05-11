#!/usr/bin/env python3
import asyncio
import json
import os
import time
import uuid
from aiohttp import web

players = {}
STALE_TIMEOUT = 60  # seconds without any message → disconnect

async def handle_connection(request):
    """Handle root path — health check or WebSocket upgrade."""
    upgrade = request.headers.get("Upgrade", "").lower()
    if upgrade == "websocket":
        return await handle_ws(request)
    return web.Response(text="OK")

async def handle_health(request):
    return web.Response(text="OK")

async def _cleanup_player(pid):
    """Remove a player and broadcast leave + phantom_remove."""
    p = players.get(pid)
    if not p:
        return
    p["username"] = None
    leave = json.dumps({"type": "player_leave", "id": pid})
    phantom_rm = json.dumps({"type": "phantom_remove", "id": pid})
    others = [p2 for p2 in players.values() if p2["username"]]
    await asyncio.gather(*[p2["ws"].send_str(leave) for p2 in others], return_exceptions=True)
    await asyncio.gather(*[p2["ws"].send_str(phantom_rm) for p2 in others], return_exceptions=True)
    players.pop(pid, None)

async def _stale_checker():
    """Periodically disconnect players who haven't sent a message in STALE_TIMEOUT seconds."""
    while True:
        await asyncio.sleep(STALE_TIMEOUT)
        now = time.time()
        stale = [pid for pid, p in list(players.items()) if p["username"] and now - p.get("last_seen", 0) > STALE_TIMEOUT]
        for pid in stale:
            p = players.get(pid)
            if p:
                try:
                    await p["ws"].close()
                except Exception:
                    pass
                await _cleanup_player(pid)

async def handle_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    pid = str(uuid.uuid4())
    p = {"id": pid, "ws": ws, "username": None, "color": "#ff4444", "px": 0, "py": 0, "pz": 0, "ry": 0, "world": "main", "last_seen": time.time()}
    players[pid] = p

    try:
        async for msg in ws:
            p["last_seen"] = time.time()
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except:
                    continue
            elif msg.type == web.WSMsgType.ERROR:
                break
            else:
                continue

            t = data.get("type")

            if t == "join":
                username = data.get("username", "").strip()
                if not username or len(username) > 20:
                    await ws.send_str(json.dumps({"type": "error", "message": "Invalid username"}))
                    continue
                taken = {p2["username"] for p2 in players.values() if p2["username"]}
                if username in taken:
                    await ws.send_str(json.dumps({"type": "error", "message": "Username taken"}))
                    continue

                p["username"] = username
                p["color"] = data.get("color", "#ff4444")
                existing = [
                    {"id": pid2, "username": p2["username"], "color": p2["color"], "px": p2["px"], "py": p2["py"], "pz": p2["pz"], "ry": p2["ry"], "world": p2["world"]}
                    for pid2, p2 in players.items() if p2["username"] and pid2 != pid
                ]
                await ws.send_str(json.dumps({"type": "joined", "id": pid, "color": p["color"], "players": existing}))

                broadcast = json.dumps({"type": "player_join", "id": pid, "username": username, "color": p["color"]})
                await asyncio.gather(*[
                    p2["ws"].send_str(broadcast) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "position":
                if not p["username"]:
                    continue
                p["px"] = data.get("px", 0)
                p["py"] = data.get("py", 0)
                p["pz"] = data.get("pz", 0)
                p["ry"] = data.get("ry", 0)
                p["world"] = data.get("world", "main")

                relay = json.dumps({"type": "position", "id": pid, "px": p["px"], "py": p["py"], "pz": p["pz"], "ry": p["ry"], "world": p["world"]})
                await asyncio.gather(*[
                    p2["ws"].send_str(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "chat":
                if not p["username"]:
                    continue
                message = data.get("message", "").strip()
                if not message:
                    continue
                relay = json.dumps({"type": "chat", "id": pid, "username": p["username"], "message": message})
                await asyncio.gather(*[
                    p2["ws"].send_str(relay) for p2 in players.values() if p2["username"]
                ], return_exceptions=True)

            elif t == "phantom_place":
                if not p["username"]:
                    continue
                relay = json.dumps({
                    "type": "phantom_place", "id": pid,
                    "x": data.get("x"), "y": data.get("y"), "z": data.get("z"),
                    "oreIdx": data.get("oreIdx"), "name": data.get("name"),
                    "world": data.get("world", "main")
                })
                await asyncio.gather(*[
                    p2["ws"].send_str(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "color_update":
                if not p["username"]:
                    continue
                new_color = data.get("color", "#ff4444")
                p["color"] = new_color
                relay = json.dumps({"type": "color_update", "id": pid, "color": new_color})
                await asyncio.gather(*[
                    p2["ws"].send_str(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "player_count":
                if not p["username"]:
                    continue
                count = sum(1 for p2 in players.values() if p2["username"])
                await ws.send_str(json.dumps({"type": "player_count", "count": count}))

            elif t == "phantom_remove":
                if not p["username"]:
                    continue
                relay = json.dumps({"type": "phantom_remove", "id": pid})
                await asyncio.gather(*[
                    p2["ws"].send_str(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

    except Exception:
        pass
    finally:
        if pid in players:
            await _cleanup_player(pid)

    return ws


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app = web.Application()
    app.router.add_get("/", handle_connection)
    app.router.add_get("/health", handle_health)

    async def _start_background_tasks(app):
        app["stale_checker"] = asyncio.create_task(_stale_checker())

    async def _cleanup_background_tasks(app):
        task = app.get("stale_checker")
        if task:
            task.cancel()

    app.on_startup.append(_start_background_tasks)
    app.on_cleanup.append(_cleanup_background_tasks)

    print("Mining RNG Simulator 3D - Multiplayer Server")
    print(f"Listening on 0.0.0.0:{port}")
    web.run_app(app, host="0.0.0.0", port=port)
