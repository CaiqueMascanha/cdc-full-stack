from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from app.managers.connection_manager import manager

router = APIRouter()

@router.websocket("/ws/{topic}")
async def websocket_endpoint(websocket: WebSocket, topic: str):
    await manager.connect(topic, websocket)
    try:
        while True:
            await websocket.receive_text()  # mantém conexão viva
    except WebSocketDisconnect:
        manager.disconnect(topic, websocket)