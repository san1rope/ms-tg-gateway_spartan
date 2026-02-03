import asyncio
import os
import logging
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Dict, Union

from telethon.errors import PeerIdInvalidError
from telethon.tl import types

from app.config import Config, LOG_LIST


class Utils:
    STATUS_SUCCESS = "success"
    STATUS_FAIL = "fail"

    MSG_INDEX_KEY = "msg:index"
    MSG_K = lambda mid: f"msg:{mid}"

    @staticmethod
    async def add_logging(process_id: int, datetime_of_start: Union[datetime, str]) -> Logger:
        if isinstance(datetime_of_start, str):
            file_dir = datetime_of_start

        elif isinstance(datetime_of_start, datetime):
            file_dir = datetime_of_start.strftime(Config.DATETIME_FORMAT)

        else:
            raise TypeError("datetime_of_start must be str or datetime")

        log_filepath = Path(os.path.abspath(f"{Config.LOGGING_DIR}/{file_dir}/{process_id}.txt"))
        log_filepath.parent.mkdir(parents=True, exist_ok=True)
        log_filepath.touch(exist_ok=True)

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - ' + str(
            process_id) + '| %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_filepath, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger

    @staticmethod
    async def log(text: str, log_level: int = 0):
        if log_level == 1:
            Config.LOGGER.warning(text)

        elif log_level == 2:
            Config.LOGGER.error(text)

        elif log_level == 3:
            Config.LOGGER.critical(text)

        else:
            Config.LOGGER.info(text)

        if Config.DEBUG:
            lvl_text = "INFO"
            if log_level == 1:
                lvl_text = "WARNING"

            elif log_level == 2:
                lvl_text = "ERROR"

            elif log_level == 3:
                lvl_text = "CRITICAL"

            LOG_LIST.append(
                f"{datetime.now(tz=Config.DEBUG_TIMEZONE).strftime('%d.%m.%Y %H:%M:%S')} | {lvl_text} | {text}"
            )

    @staticmethod
    async def best_photo_size(photo) -> Union[Dict, None]:
        best = None
        best_pixels = -1

        for s in photo.sizes:
            if isinstance(s, types.PhotoStrippedSize):
                continue

            if isinstance(s, types.PhotoSize):
                w, h = s.w, s.h
                size_bytes = getattr(s, "size", None)
            elif isinstance(s, types.PhotoSizeProgressive):
                w, h = s.w, s.h
                size_bytes = s.sizes[-1] if s.sizes else None
            else:
                continue

            pixels = (w or 0) * (h or 0)
            if pixels > best_pixels:
                best_pixels = pixels
                best = {"w": w, "h": h, "size_bytes": size_bytes}

        return best

    @staticmethod
    async def logging_queue():
        while True:
            await asyncio.sleep(3)

            if not LOG_LIST:
                continue

            try:
                await Config.TG_CLIENT.send_message(
                    Config.DEBUG_USER_ID,
                    "\n".join(LOG_LIST),
                    parse_mode="html"
                )

            except PeerIdInvalidError as ex:
                Config.LOGGER.error(f"Can't send the log to the Telegram group! \nex = {ex}")
                continue

            except ValueError as ex:
                Config.LOGGER.error(f"Failed to send log to administrator! {ex}")
                continue

            LOG_LIST.clear()

    @staticmethod
    async def load_data_in_redis(batch_size: int = 1000):
        await Utils.log("Prepare to load data in redis...")

        pipe = Config.REDIS.pipeline(transaction=False)
        queued = 0

        await Utils.log("Loading messages data from small groups...")
        async for dialog in Config.TG_CLIENT.iter_dialogs():
            chat = dialog.entity
            if not isinstance(chat, types.Chat):
                continue

            async for msg in Config.TG_CLIENT.iter_messages(chat, limit=200):
                await pipe.sadd(Utils.MSG_INDEX_KEY, msg.id)

                await pipe.setnx(Utils.MSG_K(msg.id), f"-{chat.id}")

                queued += 2
                if queued >= batch_size:
                    await pipe.execute()
                    pipe = Config.REDIS.pipeline(transaction=False)
                    queued = 0

        if queued:
            await pipe.execute()

        await Utils.log("Data from small groups has been loaded!")
