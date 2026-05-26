"""
MemeBlast Relay Server
======================
Déploie sur Render.com (gratuit) — voir README_SERVEUR.md

Local : python server.py
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
log = logging.getLogger("memeblast-relay")

# room_id → ensemble de connexions actives
rooms: dict[str, set[ServerConnection]] = defaultdict(set)

async def handler(ws: ServerConnection):
    # L'URL est  ws://host:port/<room_id>
    room = ws.request.path.strip("/") or "default"
    rooms[room].add(ws)
    n = len(rooms[room])
    log.info(f"[+] room={room!r}  clients={n}")

    try:
        async for raw in ws:
            # Broadcast à tous les AUTRES membres de la room
            peers = rooms[room] - {ws}
            if peers:
                results = await asyncio.gather(
                    *[p.send(raw) for p in peers],
                    return_exceptions=True,
                )
                # Retire les connexions mortes
                dead = {p for p, r in zip(peers, results) if isinstance(r, Exception)}
                rooms[room] -= dead
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.warning(f"handler error: {e}")
    finally:
        rooms[room].discard(ws)
        log.info(f"[-] room={room!r}  clients={len(rooms[room])}")
        if not rooms[room]:
            del rooms[room]

async def main():
    port = int(os.environ.get("PORT", 8765))
    host = "0.0.0.0"
    log.info(f"MemeBlast relay  →  ws://{host}:{port}")
    async with serve(
        handler, host, port,
        max_size=60 * 1024 * 1024,   # 60 Mo max par message
        ping_interval=30,
        ping_timeout=20,
    ):
        await asyncio.get_running_loop().create_future()  # tourne indéfiniment

if __name__ == "__main__":
    asyncio.run(main())
