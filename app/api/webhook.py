import json
from typing import Union, List, Optional

from aiohttp import ClientSession
from pydantic import BaseModel
from telethon.tl import types
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

from app.config import Config
from app.tg.redis_service import RedisInterface


class FromUser(BaseModel):
    id: int
    first_name: str
    username: Optional[str] = None
    language_code: Optional[str] = None

    @staticmethod
    async def obj_from_sender(sender):
        obj = None
        print(f"sender isinstance; sender = {type(sender)}")
        if isinstance(sender, types.User) and (not sender.bot):
            obj = FromUser(
                id=sender.id,
                first_name=sender.first_name,
                username=sender.username,
                language_code=sender.lang_code
            )

        return obj


class ChatInfo(BaseModel):
    title: str
    username: Optional[str]
    type: str
    is_forum: bool
    member_count: int

    @staticmethod
    async def assemble_obj(input_obj, only_chat_id=False, chat: bool = False, use_cache: bool = True):
        flag_update_cache = False
        if isinstance(input_obj, (types.PeerChannel, types.InputPeerChannel, types.Channel)):
            participants_count = None

            if use_cache:
                chat_data = await RedisInterface().get_chat_data(input_obj.id)
                if chat_data:
                    participants_count = json.loads(chat_data)["member_count"]

            if (not participants_count) or (not isinstance(input_obj, types.Channel)):
                full_chat = await Config.TG_CLIENT(GetFullChannelRequest(input_obj))
                input_obj = full_chat.chats[0]
                participants_count = full_chat.full_chat.participants_count
                flag_update_cache = True

            title = input_obj.title
            chat_id = int(f"-100{input_obj.id}")
            chat_type = "supergroup" if input_obj.megagroup else "channel"

        elif isinstance(input_obj, (types.PeerChat, types.InputPeerChat, types.Chat)) or chat:
            participants_count = None
            if not isinstance(input_obj, types.Chat):
                full_chat = await Config.TG_CLIENT(GetFullChatRequest(getattr(input_obj, "chat_id", input_obj)))
                input_obj = full_chat.chats[0]
                participants_count = len(full_chat.full_chat.participants.participants)
                flag_update_cache = True

            title = input_obj.title
            chat_id = int(f"-{input_obj.id}")
            chat_type = "chat"

        else:
            return None if only_chat_id else (None, None)

        chat_info = ChatInfo(
            title=title,
            username=getattr(input_obj, "username", None),
            type=chat_type,
            is_forum=getattr(input_obj, "forum", False),
            member_count=participants_count
        )

        if flag_update_cache:
            if str(chat_id).startswith("-100"):
                chat_id = str(chat_id).replace("-100", "")

            else:
                chat_id = str(chat_id * -1)

            await RedisInterface().set_chat_data(chat_id=chat_id, chat_info=chat_info)

        return chat_id if only_chat_id else (chat_id, chat_info)


class MediaPhoto(BaseModel):
    type_name: str = "photo"
    file_size: int
    mime_type: str
    width: int
    height: int


class MediaSticker(BaseModel):
    type_name: str = "sticker"
    file_size: int
    mime_type: str
    emoji: str
    set_name: str


class MediaDocument(BaseModel):
    type_name: str = "document"
    file_size: int
    mime_type: str
    file_name: str


class MediaAudio(BaseModel):
    type_name: str = "audio_voice"
    file_size: int
    mime_type: str
    duration: int
    is_voice: bool


class MediaVideoGIF(BaseModel):
    type_name: str = "video_gif"
    file_size: int
    mime_type: str
    duration: float
    width: int
    height: int
    supports_streaming: bool


class MessageCreated(BaseModel):
    type: str = "message_created"
    chat_id: int
    message_id: int
    text: Optional[str] = None
    message_type: int = 1
    topic_id: Optional[int] = None
    sender: FromUser
    chat_info: ChatInfo
    timestamp: str
    media: Union[None, MediaPhoto, MediaSticker, MediaDocument, MediaAudio, MediaVideoGIF]


class MessageEdited(BaseModel):
    type: str = "message_update"
    chat_id: int
    message_id: int
    text: Optional[str] = None
    message_type: int
    topic_id: Optional[int] = None
    sender: FromUser
    chat_info: ChatInfo
    timestamp: str
    media: Union[None, MediaPhoto, MediaSticker, MediaDocument, MediaAudio, MediaVideoGIF]


class MessageDeleted(BaseModel):
    type: str = "message_deleted"
    chat_id: int
    message_ids: List[int]
    topic_id: Optional[int] = None
    deleted_by: Optional[FromUser] = None
    chat_info: ChatInfo
    timestamp: str


class TopicCreated(BaseModel):
    type: str = "topic_created"
    chat_id: int
    topic_id: int
    title: str
    icon_color: int
    created_by: FromUser
    chat_info: ChatInfo
    timestamp: str


class TopicEdited(BaseModel):
    type: str = "topic_updated"
    chat_id: int
    topic_id: int
    title: str
    icon_color: int
    sender: FromUser
    chat_info: ChatInfo
    timestamp: str


class TopicDeleted(BaseModel):
    type: str = "topic_deleted"
    chat_id: int
    topic_id: int
    sender: FromUser
    chat_info: ChatInfo
    timestamp: str


class BotAdded(BaseModel):
    type: str = "bot_added_to_chat"
    chat_id: int
    chat_info: ChatInfo
    owner_info: FromUser
    added_by: FromUser
    timestamp: str


class BotDeleted(BaseModel):
    type: str = "bot_deleted_from_chat"
    chat_id: int
    timestamp: str


class APIInterface:

    @staticmethod
    async def send_request(req_model: Union[
        MessageCreated, MessageEdited, MessageDeleted,
        TopicCreated, TopicEdited, TopicDeleted,
        BotAdded, BotDeleted
    ], utils_obj):
        if not Config.AIOHTTP_SESSION:
            Config.AIOHTTP_SESSION = ClientSession()

        url = Config.BASE_URL
        if isinstance(req_model, MessageCreated):
            url += "/webhook/telegram/create"
            text = "Event: Message Created"

        elif isinstance(req_model, MessageEdited):
            url += "/webhook/telegram/update"
            text = "Event: Message Edited"

        elif isinstance(req_model, MessageDeleted):
            url += "/webhook/telegram/delete"
            text = "Event: Message Deleted"

        elif isinstance(req_model, TopicCreated):
            url += "/webhook/telegram/topic_created"
            text = "Event: Topic Created"

        elif isinstance(req_model, TopicEdited):
            url += "/webhook/telegram/topicEdited"
            text = "Event: Topic Edited"

        elif isinstance(req_model, TopicDeleted):
            url += "/webhook/telegram/topicDeleted"
            text = "Event: Topic Deleted"

        elif isinstance(req_model, BotAdded):
            url += "/webhook/telegram/bot_added"
            text = "Event: Bot Added To Chat"

        elif isinstance(req_model, BotDeleted):
            url += "/webhook/telegram/deleteChat"
            text = "Event: Bot Deleted From Chat"

        else:
            return TypeError

        headers = {
            "Content-Type": "application/json"
        }

        async with Config.AIOHTTP_SESSION.post(
                url=url, headers=headers, json=req_model.model_dump(), timeout=15) as response:
            answer = await response.json()

            body_request = "<pre>" + json.dumps(req_model.model_dump(), indent=2, ensure_ascii=False) + "</pre>"

            await utils_obj.log(f"{text}\nRequest | {response.status} | {url} \n{body_request}")

        return answer
