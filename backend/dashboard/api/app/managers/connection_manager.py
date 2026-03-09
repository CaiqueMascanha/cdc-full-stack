from fastapi import WebSocket
from collections import defaultdict

class ConnectionManager:
    def __init__(self):
        # topic -> lista de conexões
        self._connections: dict[str, list[WebSocket]] = defaultdict(list)

    async def connect(self, topic: str, websocket: WebSocket):
        await websocket.accept()
        self._connections[topic].append(websocket)

    def disconnect(self, topic: str, websocket: WebSocket):
        self._connections[topic].remove(websocket)

    async def broadcast(self, topic: str, message: str):
        dead = []
        for ws in self._connections[topic]:
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections[topic].remove(ws)

manager = ConnectionManager()