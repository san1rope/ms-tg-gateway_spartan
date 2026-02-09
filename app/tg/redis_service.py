import asyncio
from typing import Optional, Union, List

from redis.asyncio import Redis
from redis import AuthenticationError, BusyLoadingError
from telethon import types

from app.config import Config
from app.utils import Utils as Ut


class RedisInterface:
    REDIS: Optional[Redis] = None

    MSG_K = lambda mid: f"msg:{mid}"

    @classmethod
    async def init_redis(cls, retries: int = 3) -> bool:
        try:
            cls.REDIS = await Redis(
                host=Config.REDIS_IP, port=6379, db=0, decode_responses=False, socket_keepalive=True,
                password=Config.REDIS_PASSWORD, health_check_interval=15, socket_connect_timeout=5
            )
            return True

        except ConnectionError:
            Config.LOGGER.critical("Failed to connect to Redis!")
            return False

        except AuthenticationError:
            Config.LOGGER.critical("Incorrect password for Redis!")
            return False

        except BusyLoadingError:
            if retries:
                Config.LOGGER.error(
                    f"Unable to connect to Redis at this time, will reconnect in 10 seconds! Retries: {retries}")
                await asyncio.sleep(10)

                return await cls.init_redis(retries=retries - 1)

            Config.LOGGER.critical("Could not connect to Redis!")
            return False

    @classmethod
    async def get_chat_id_of_del_msg(cls, message_id: Union[int, List[int]]) -> Union[int, None]:
        if isinstance(message_id, int):
            message_id = [message_id]

        for msg_id in message_id:
            chat_id = cls.REDIS.get(cls.MSG_K(msg_id))
            if chat_id:
                return chat_id

        return None

    @classmethod
    async def load_messages_from_groups(cls, batch_size: int = 1000):
        await Ut.log("Prepare to load data in redis...")

        pipe = cls.REDIS.pipeline(transaction=False)
        queued = 0

        await Ut.log("Loading messages data from small groups...")
        async for dialog in Config.TG_CLIENT.iter_dialogs():
            chat = dialog.entity
            if not isinstance(chat, types.Chat):
                continue

            async for msg in Config.TG_CLIENT.iter_messages(chat, limit=200):
                await pipe.setnx(cls.MSG_K(msg.id), f"-{chat.id}")

                queued += 1
                if queued >= batch_size:
                    await pipe.execute()
                    pipe = cls.REDIS.pipeline(transaction=False)
                    queued = 0

        if queued:
            await pipe.execute()

        await Ut.log("Data from small groups has been loaded!")
