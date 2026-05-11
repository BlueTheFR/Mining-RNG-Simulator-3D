#!/usr/bin/env python3
import asyncio
import json
import os
import uuid
import websockets

players = {}

async def handler(ws):
    pid = str(uuid.uuid4())
    p = {"id": pid, "ws": ws, "username": None, "px": 0, "py": 0, "pz": 0, "ry": 0, "world": "main"}
    players[pid] = p

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except:
                continue

            t = msg.get("type")

            if t == "join":
                username = msg.get("username", "").strip()
                if not username or len(username) > 20:
                    await ws.send(json.dumps({"type": "error", "message": "Invalid username"}))
                    continue
                taken = {p2["username"] for p2 in players.values() if p2["username"]}
                if username in taken:
                    await ws.send(json.dumps({"type": "error", "message": "Username taken"}))
                    continue

                p["username"] = username
                existing = [
                    {"id": pid2, "username": p2["username"], "px": p2["px"], "py": p2["py"], "pz": p2["pz"], "ry": p2["ry"], "world": p2["world"]}
                    for pid2, p2 in players.items() if p2["username"] and pid2 != pid
                ]
                await ws.send(json.dumps({"type": "joined", "id": pid, "players": existing}))

                broadcast = json.dumps({"type": "player_join", "id": pid, "username": username})
                await asyncio.gather(*[
                    p2["ws"].send(broadcast) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "position":
                if not p["username"]:
                    continue
                p["px"] = msg.get("px", 0)
                p["py"] = msg.get("py", 0)
                p["pz"] = msg.get("pz", 0)
                p["ry"] = msg.get("ry", 0)
                p["world"] = msg.get("world", "main")

                relay = json.dumps({"type": "position", "id": pid, "px": p["px"], "py": p["py"], "pz": p["pz"], "ry": p["ry"], "world": p["world"]})
                await asyncio.gather(*[
                    p2["ws"].send(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "chat":
                if not p["username"]:
                    continue
                message = msg.get("message", "").strip()
                if not message:
                    continue
                relay = json.dumps({"type": "chat", "id": pid, "username": p["username"], "message": message})
                await asyncio.gather(*[
                    p2["ws"].send(relay) for p2 in players.values() if p2["username"]
                ], return_exceptions=True)

            elif t == "phantom_place":
                if not p["username"]:
                    continue
                relay = json.dumps({
                    "type": "phantom_place", "id": pid,
                    "x": msg.get("x"), "y": msg.get("y"), "z": msg.get("z"),
                    "oreIdx": msg.get("oreIdx"), "name": msg.get("name"),
                    "world": msg.get("world", "main")
                })
                await asyncio.gather(*[
                    p2["ws"].send(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

            elif t == "phantom_remove":
                if not p["username"]:
                    continue
                relay = json.dumps({"type": "phantom_remove", "id": pid})
                await asyncio.gather(*[
                    p2["ws"].send(relay) for pid2, p2 in players.items()
                    if pid2 != pid and p2["username"]
                ], return_exceptions=True)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if p["username"]:
            # Free the username *before* sending notifications,
            # so a reconnecting player with the same name isn't rejected.
            p["username"] = None
            leave = json.dumps({"type": "player_leave", "id": pid})
            phantom_rm = json.dumps({"type": "phantom_remove", "id": pid})
            await asyncio.gather(*[
                p2["ws"].send(leave) for pid2, p2 in players.items()
                if pid2 != pid and p2["username"]
            ], return_exceptions=True)
            await asyncio.gather(*[
                p2["ws"].send(phantom_rm) for pid2, p2 in players.items()
                if pid2 != pid and p2["username"]
            ], return_exceptions=True)
        players.pop(pid, None)


async def process_request(path, req):
    if path in ("/", "/health"):
        return 200, [("Content-Type", "text/plain")], b"OK"

async def main():
    port = int(os.environ.get("PORT", 3000))
    print("Mining RNG Simulator 3D - Multiplayer Server")
    print(f"Listening on ws://0.0.0.0:{port}")
    async with websockets.serve(handler, "0.0.0.0", port, process_request=process_request, ping_interval=30):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
