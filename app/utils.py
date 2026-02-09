import asyncio
import os
import logging
from datetime import datetime
from logging import Logger
from pathlib import Path
from typing import Dict, Union

from telethon import TelegramClient, errors as te
from telethon.errors import PeerIdInvalidError
from telethon.tl import types

from app.config import Config, LOG_LIST


class Utils:
    STATUS_SUCCESS = "success"
    STATUS_FAIL = "fail"

    @staticmethod
    async def init_telegram_client(retries: int = 3) -> bool:
        try:
            Config.TG_CLIENT = TelegramClient(session="work-app", api_id=Config.TG_API_ID, api_hash=Config.TG_API_HASH)
            await Config.TG_CLIENT.start(phone=Config.PHONE_NUMBER)
            return True

        except te.SessionPasswordNeededError:
            Config.LOGGER.critical("Could not initialize TelegramClient. 2fa Password required.")

        except te.PhoneNumberInvalidError:
            Config.LOGGER.critical("Unable to initialize TelegramClient. Incorrect phone number!")

        except (te.PhoneCodeInvalidError, te.PhoneCodeExpiredError):
            Config.LOGGER.critical("Unable to initialize TelegramClient. Incorrect phone code!")

        except te.ApiIdInvalidError:
            Config.LOGGER.critical("Unable to initialize TelegramClient. Incorrect telegram account id!")

        except te.AuthKeyDuplicatedError:
            Config.LOGGER.critical(
                "Unable to initialize TelegramClient. The session file is being accessed from multiple locations!")

        except te.UserDeactivatedBanError:
            Config.LOGGER.critical("Unable to initialize TelegramClient. Telegram account banned!")

        except te.FloodWaitError as ex:
            if retries:
                Config.LOGGER.warning(
                    f"Unable to initialize TelegramClient. FloodWaitError {ex.seconds} seconds. retries: {retries}")

                await asyncio.sleep(ex.seconds + 5)
                return await Utils.init_telegram_client(retries=retries - 1)

            Config.LOGGER.critical("Unable to initialize TelegramClient. FloodWaitError, retries is 0")

        return False

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
