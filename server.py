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


async def proxy_bridge(reader, writer, ws_reader, ws_writer):
    """Bidirectional proxy between two connections until one closes."""
    async def forward(src, dst):
        try:
            while True:
                data = await src.read(65536)
                if not data:
                    break
                dst.write(data)
                await dst.drain()
        except:
            pass
        finally:
            try:
                dst.close()
            except:
                pass

    await asyncio.gather(
        forward(reader, ws_writer),
        forward(ws_reader, writer),
    )


async def main():
    port = int(os.environ.get("PORT", 3000))

    # Start the WebSocket server on a random local port
    ws_server = await websockets.serve(handler, "127.0.0.1", 0, ping_interval=30)
    ws_port = ws_server.sockets[0].getsockname()[1]

    # Public TCP server: handles health checks + proxies WebSocket to the internal server
    async def public_handler(reader, writer):
        try:
            request_line = await asyncio.wait_for(reader.readline(), 5)
        except asyncio.TimeoutError:
            writer.close()
            return
        if not request_line:
            writer.close()
            return

        line = request_line.decode(errors="replace").strip()
        parts = line.split()
        method = parts[0] if parts else ""
        path = parts[1] if len(parts) > 1 else "/"

        # HEAD health check
        if method == "HEAD":
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        # Read the rest of the request headers
        remaining = b""
        try:
            while True:
                chunk = await asyncio.wait_for(reader.read(4096), 2)
                if not chunk:
                    break
                remaining += chunk
                if b"\r\n\r\n" in remaining or len(remaining) > 8192:
                    break
        except asyncio.TimeoutError:
            pass

        full_request = request_line + remaining
        full_str = full_request.decode(errors="replace").lower()

        # GET health check (no upgrade header = not a WebSocket request)
        if method == "GET" and path in ("/", "/health") and "upgrade" not in full_str:
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK")
            await writer.drain()
            writer.close()
            return

        # Any other non-WebSocket request
        if method != "GET" or "upgrade" not in full_str:
            writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        # WebSocket upgrade — proxy to the internal WS server
        try:
            ws_reader, ws_writer = await asyncio.open_connection("127.0.0.1", ws_port)
        except:
            writer.close()
            return

        ws_writer.write(full_request)
        await ws_writer.drain()

        await proxy_bridge(reader, writer, ws_reader, ws_writer)

    server = await asyncio.start_server(public_handler, "0.0.0.0", port)
    print("Mining RNG Simulator 3D - Multiplayer Server")
    print(f"Listening on ws://0.0.0.0:{port}")

    async with server:
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
