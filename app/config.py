import os.path
from logging import Logger
from pathlib import Path
from typing import Optional, List
from asyncio import Queue

from aiohttp import ClientSession
from dotenv import load_dotenv
from fastapi import FastAPI
from redis.asyncio import Redis
from telethon import TelegramClient
from pytz import timezone

load_dotenv()

LOG_LIST: List[str] = []


class Config:
    TG_API_ID = int(os.getenv("TG_API_ID").strip())
    TG_API_HASH = os.getenv("TG_API_HASH").strip()
    TG_CLIENT: Optional[TelegramClient] = None

    DATETIME_FORMAT = os.getenv("DATETIME_FORMAT").strip()
    LOGGING_DIR = Path(os.path.abspath("logs"))
    LOGGER: Optional[Logger] = None

    REDIS_IP: str = os.getenv("REDIS_IP").strip()
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD").strip()
    REDIS: Optional[Redis] = None

    KAFKA_INTERFACE_OBJ = None
    KAFKA_BOOTSTRAP_IP: str = os.getenv("KAFKA_BOOTSTRAP_IP").strip()
    KAFKA_TOPIC_COMMANDS: str = os.getenv("KAFKA_TOPIC_COMMANDS").strip()
    KAFKA_TOPIC_RESPONSES: str = os.getenv("KAFKA_TOPIC_RESPONSES").strip()

    BASE_URL: str = os.getenv("BASE_URL").strip()
    AIOHTTP_SESSION: Optional[ClientSession] = None

    PHONE_NUMBER = os.getenv("PHONE_NUMBER").strip()
    IGNORE_CHATS = list(map(int, os.getenv("IGNORE_CHATS").split(',')))

    DEBUG: bool = bool(int(os.getenv("DEBUG").strip()))
    DEBUG_USER_ID = int(os.getenv("DEBUG_USER_ID").strip())
    DEBUG_TIMEZONE = timezone(os.getenv("DEBUG_TIMEZONE").strip())

    QUEUE_WORKER: Optional[Queue] = None

    REST_APP: Optional[FastAPI] = None
    UVICORN_HOST: str = os.getenv("UVICORN_HOST").strip()
    UVICORN_PORT: int = int(os.getenv("UVICORN_PORT").strip())

    DATABASE_CLEANUP = bool(int(os.getenv("DATABASE_CLEANUP")))
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
