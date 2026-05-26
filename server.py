"""
MemeBlast Relay Server v2
Gere les health checks HTTP de Render + WebSocket relay
"""
import asyncio, json, logging, os
from collections import defaultdict
from http import HTTPStatus
import websockets
from websockets.asyncio.server import serve, ServerConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("memeblast")

rooms: dict[str, set[ServerConnection]] = defaultdict(set)

async def handler(ws: ServerConnection):
    room = ws.request.path.strip("/") or "default"
    rooms[room].add(ws)
    log.info(f"[+] room={room!r}  total={len(rooms[room])}")
    try:
        async for raw in ws:
            peers = rooms[room] - {ws}
            if peers:
                results = await asyncio.gather(
                    *[p.send(raw) for p in peers],
                    return_exceptions=True,
                )
                dead = {p for p, r in zip(peers, results) if isinstance(r, Exception)}
                rooms[room] -= dead
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.warning(f"handler: {e}")
    finally:
        rooms[room].discard(ws)
        log.info(f"[-] room={room!r}  total={len(rooms[room])}")
        if not rooms[room]:
            del rooms[room]

async def health_check(path, headers):
    """Repond aux health checks HTTP de Render avec 200 OK."""
    if headers.get("Upgrade", "").lower() != "websocket":
        return HTTPStatus.OK, [("Content-Type", "text/plain")], b"MemeBlast relay OK\n"
    return None  # laisser passer la connexion WebSocket normalement

async def main():
    port = int(os.environ.get("PORT", 8765))
    log.info(f"MemeBlast relay -> 0.0.0.0:{port}")
    async with serve(
        handler,
        "0.0.0.0",
        port,
        process_request=health_check,
        max_size=60 * 1024 * 1024,
        ping_interval=30,
        ping_timeout=20,
    ):
        await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())
