from typing import Union

from telethon import events
from telethon.tl import types

from app.config import Config
from app.tg.handlers import HandleEvents
from app.tg.redis_service import RedisInterface


class EventsCatcher:

    @staticmethod
    async def check_chat_id(chat_id: Union[int, types.PeerChat, types.PeerChannel, types.PeerUser]) -> bool:
        if isinstance(chat_id, types.PeerChat):
            chat_id = int(f"-{chat_id.chat_id}")

        elif isinstance(chat_id, types.PeerChannel):
            chat_id = int(f"-100{chat_id.channel_id}")

        elif not isinstance(chat_id, int):
            return False

        if chat_id in Config.IGNORE_CHATS:
            return False

        return True

    @staticmethod
    async def event_new_message(event: events.NewMessage.Event):
        Config.LOGGER.info("New event: NewMessage")

        if not await EventsCatcher.check_chat_id(event.message.peer_id):
            return

        await Config.QUEUE_WORKER.put(HandleEvents.processing_new_message(event))

    @staticmethod
    async def event_message_edited(event: events.MessageEdited.Event):
        Config.LOGGER.info("New event: MessageEdited")

        if await EventsCatcher.check_chat_id(event.message.peer_id):
            await Config.QUEUE_WORKER.put(HandleEvents.processing_message_edited(event))

    @staticmethod
    async def event_message_deleted(event: events.MessageDeleted.Event):
        Config.LOGGER.info("New event: MessageDeleted")

        org_upd = event.original_update
        if isinstance(org_upd, types.UpdateDeleteChannelMessages):
            chat_id = int(f"100{org_upd.channel_id}")

        elif isinstance(org_upd, types.UpdateDeleteMessages):
            chat_id = await RedisInterface().get_chat_id_of_del_msg(org_upd.messages)
            if chat_id is None:
                return

        else:
            return

        if await EventsCatcher.check_chat_id(int(chat_id)):
            await Config.QUEUE_WORKER.put(HandleEvents.processing_message_deleted(event))

    @staticmethod
    async def event_chat_action(event: events.ChatAction.Event):
        Config.LOGGER.info("New event: ChatAction")

        act_msg = event.action_message
        if not await EventsCatcher.check_chat_id(act_msg.peer_id):
            return

        me = await Config.TG_CLIENT.get_me()
        if isinstance(act_msg.action, types.MessageActionChatAddUser) and me.id in act_msg.action.users:
            await Config.QUEUE_WORKER.put(HandleEvents.processing_action_add_chat_user(event))

        elif isinstance(act_msg.action, types.MessageActionChatDeleteUser) and me.id == act_msg.action.user_id:
            await Config.QUEUE_WORKER.put(HandleEvents.processing_action_chat_delete_user(event))

    @staticmethod
    async def event_raw(event: events.Raw):
        if isinstance(event, types.UpdateNewChannelMessage):
            action = event.message.action
            if not action:
                return

            if isinstance(action, types.MessageActionTopicCreate):
                Config.LOGGER.info("New event: Raw:MessageActionTopicCreate")
                if not await EventsCatcher.check_chat_id(event.message.peer_id):
                    return

                await Config.QUEUE_WORKER.put(HandleEvents.processing_create_topic(event))

            elif isinstance(action, types.MessageActionTopicEdit):
                Config.LOGGER.info("New event: Raw:MessageActionTopicEdit")
                if not await EventsCatcher.check_chat_id(event.message.peer_id):
                    return

                await Config.QUEUE_WORKER.put(HandleEvents.processing_topic_edited(event))
