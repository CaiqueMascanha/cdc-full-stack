import asyncio
import json
from aiokafka import AIOKafkaConsumer
from app.core.config import settings
from app.managers.connection_manager import manager
import uuid

async def consume():
    consumer = AIOKafkaConsumer(
        *settings.TOPICS,
        bootstrap_servers=settings.KAFKA_BROKER,
        group_id=settings.KAFKA_GROUP_ID,
        # group_id=None,
        auto_offset_reset="latest",
        value_deserializer=lambda v: v.decode("utf-8"),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await manager.broadcast(msg.topic, msg.value)
    finally:
        await consumer.stop()