"""
MemeBlast Relay Server v3
Compatible websockets 12+ et Python 3.14
"""
import asyncio, json, logging, os
from collections import defaultdict
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
    log.info(f"[+] room={room!r}  clients={len(rooms[room])}")
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
        log.info(f"[-] room={room!r}  clients={len(rooms[room])}")
        if not rooms[room]:
            del rooms[room]

async def health_check(connection, request):
    """
    Health check pour Render.
    Dans websockets 13+/Python 3.14, process_request recoit
    (connection, request) et request.headers est un objet Headers.
    """
    upgrade = request.headers.get("Upgrade", "")
    if upgrade.lower() != "websocket":
        # Repondre 200 OK au health check HTTP
        from websockets.http11 import Response
        from http import HTTPStatus
        return Response(HTTPStatus.OK, "OK",
                       [("Content-Type", "text/plain"),
                        ("Content-Length", "18")],
                       b"MemeBlast relay OK")
    # Laisser passer la connexion WebSocket
    return None

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
