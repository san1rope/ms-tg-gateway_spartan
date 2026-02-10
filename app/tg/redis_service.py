import asyncio
from typing import Optional, Union, List, Tuple

from redis.asyncio import Redis
from redis import AuthenticationError, BusyLoadingError
from telethon import types

from app.config import Config
from app.utils import Utils as Ut


class RedisInterface:
    REDIS: Optional[Redis] = None

    F_KEY_GROUPS_MSG = lambda msg_id: f"msg:{msg_id}"
    F_KEY_TOPIC_DATA = lambda chat_id, topic_id: f"topic:{chat_id}:{topic_id}"
    F_KEY_CHAT_DATA = lambda chat_id: f"chat:{chat_id}"

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
    async def set_topic_data(
            cls, chat_id: Union[str, int], topic_id: Union[int, str], title: str, icon_color: int) -> bool:
        try:
            await cls.REDIS.set(
                cls.F_KEY_TOPIC_DATA(chat_id, topic_id),
                str({"title": title, "icon_color": icon_color})
            )
            return True

        except Exception as ex:
            Config.LOGGER.error(f"RedisInterface.new_topic | {ex}")
            return False

    @classmethod
    async def get_topic_data(cls, chat_id: Union[str, int], topic_id: Union[str, int]) -> Union[Tuple[str], None]:
        try:
            result = await cls.REDIS.get(cls.F_KEY_TOPIC_DATA(chat_id, topic_id))
            if result:
                return result.split(":")

        except Exception as ex:
            Config.LOGGER.error(f"RedisInterface.get_topic_data | {ex}")

        return None

    @classmethod
    async def set_chat_data(cls, chat_id: Union[str, int], chat_info) -> bool:
        try:
            await cls.REDIS.set(cls.F_KEY_CHAT_DATA(chat_id), str(chat_info.model_dump()))
            return True

        except Exception as ex:
            Config.LOGGER.error(f"RedisInterface.set_chat_data | {ex}")
            return False

    @classmethod
    async def get_chat_data(cls, chat_id: Union[str, int]):
        try:
            result = await cls.REDIS.get(cls.F_KEY_CHAT_DATA(chat_id))
            if result:
                pass

        except Exception as ex:
            Config.LOGGER.error(f"RedisInterface.get_chat_data | {ex}")

    @classmethod
    async def set_chat_id(cls, chat_id: int, msg_id: Union[str, int]) -> bool:
        try:
            await cls.REDIS.set(cls.F_KEY_GROUPS_MSG(msg_id), chat_id)
            return True

        except Exception as ex:
            Config.LOGGER.error(f"RedisInterface.set_chat_id | {ex}")
            return False

    @classmethod
    async def get_chat_id_of_del_msg(cls, message_id: Union[int, List[int]]) -> Union[int, None]:
        if isinstance(message_id, int):
            message_id = [message_id]

        for msg_id in message_id:
            chat_id = cls.REDIS.get(cls.F_KEY_GROUPS_MSG(msg_id))
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
                await pipe.setnx(cls.F_KEY_GROUPS_MSG(msg.id), f"-{chat.id}")

                queued += 1
                if queued >= batch_size:
                    await pipe.execute()
                    pipe = cls.REDIS.pipeline(transaction=False)
                    queued = 0

        if queued:
            await pipe.execute()

        await Ut.log("Data from small groups has been loaded!")
