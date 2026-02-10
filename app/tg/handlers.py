from datetime import datetime, timezone

from telethon import events
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import PeerChat

from app.api.webhook import *
from app.config import Config
from app.utils import Utils as Ut
from app.tg.tg_tools import TgTools


class HandleEvents:

    @staticmethod
    async def processing_create_topic(event: types.UpdateNewChannelMessage):
        msg_obj = event.message

        # chat_id, chat_info = await ChatInfo.obj_from_peer(msg_obj.peer_id)
        # if not chat_id:
        #     return
        #
        # topic_id, title, icon_color = await TgTools.get_topic_data_from_msg(msg_obj)
        #
        # sender = await msg_obj.get_sender()
        # from_user = await FromUser.obj_from_sender(sender)
        # if not from_user:
        #     return
        #

        sender_obj = await event.get_sender()
        from_user = await FromUser.obj_from_sender(sender_obj)

        print(f"sender = {sender_obj}")

        # await APIInterface.send_request(
        #     utils_obj=Ut,
        #     req_model=TopicCreated(
        #         chat_id=msg_obj.peer_id.channel_id,
        #         topic_id=msg_obj.id,
        #         title=msg_obj.action.title,
        #         icon_color=msg_obj.action.icon_color,
        #         created_by=from_user,
        #         chat_info=chat_info,
        #         timestamp=msg_obj.date.strftime("%Y-%m-%dT%H:%M:%SZ")
        #     )
        # )

    @staticmethod
    async def processing_new_message(event: events.NewMessage.Event):
        msg_obj = event.message

        sender = await msg_obj.get_sender()
        from_user = await FromUser.obj_from_sender(sender)
        if not from_user:
            return

        chat_id, chat_info = await ChatInfo.obj_from_peer(msg_obj.peer_id)
        if not chat_id:
            return

        if chat_info.type == "chat":
            await Config.REDIS.set(f"msg:{msg_obj.id}", chat_id)

        topic_id = await TgTools.get_topic_data_from_msg(msg_obj, only_id=True)
        msg_type, media = await TgTools.get_media_data_from_msg(msg_obj)

        await APIInterface.send_request(
            utils_obj=Ut,
            req_model=MessageCreated(
                chat_id=chat_id,
                message_id=msg_obj.id,
                text=msg_obj.message,
                message_type=msg_type,
                topic_id=topic_id,
                sender=from_user,
                chat_info=chat_info,
                timestamp=msg_obj.date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                media=media
            )
        )

    @staticmethod
    async def processing_message_edited(event: events.MessageEdited.Event):
        msg_obj = event.message

        chat_id, chat_info = await ChatInfo.obj_from_peer(msg_obj.peer_id)
        if not chat_id:
            return

        sender = await msg_obj.get_sender()
        from_user = await FromUser.obj_from_sender(sender)
        if not from_user:
            return

        msg_type, media = await TgTools.get_media_data_from_msg(msg_obj)
        topic_id = await TgTools.get_topic_data_from_msg(msg_obj, only_id=True)

        await APIInterface.send_request(
            utils_obj=Ut,
            req_model=MessageEdited(
                chat_id=chat_id,
                message_id=msg_obj.id,
                text=msg_obj.message,
                message_type=msg_type,
                topic_id=topic_id,
                sender=from_user,
                chat_info=chat_info,
                timestamp=msg_obj.date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                media=media
            )
        )

    @staticmethod
    async def processing_message_deleted(event: events.MessageDeleted.Event):
        org_upd = event.original_update
        if isinstance(org_upd, types.UpdateDeleteChannelMessages):
            input_chat = await event.get_input_chat()
            chat_id, chat_info = await ChatInfo.obj_from_peer(input_chat)
            if not chat_id:
                return

            message_ids = org_upd.messages
            result = await TgTools.get_userdata_deleted_by(message_ids=message_ids, input_chat=input_chat)
            if not result:
                return

            if result["type"] == "delete_messages":
                req_model = MessageDeleted(
                    chat_id=chat_id,
                    message_ids=message_ids,
                    topic_id=result["topic_id"],
                    deleted_by=result["deleted_by"],
                    chat_info=chat_info,
                    timestamp=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                )

            elif result["type"] == "delete_topic":
                req_model = TopicDeleted(
                    chat_id=chat_id,
                    topic_id=result["topic_id"],
                    sender=result["deleted_by"],
                    chat_info=chat_info,
                    timestamp=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                )

            else:
                return

        elif isinstance(org_upd, types.UpdateDeleteMessages):
            message_ids = org_upd.messages
            chat_id = None
            for msg_id in message_ids:
                chat_id = await Config.REDIS.get(f"msg:{msg_id}")
                if chat_id:
                    break

            if not chat_id:
                return

            chat_id, chat_info = await ChatInfo.obj_from_peer(PeerChat(chat_id=int(chat_id[1:])))
            if not chat_info:
                return

            req_model = MessageDeleted(
                chat_id=chat_id,
                message_ids=message_ids,
                topic_id=None,
                deleted_by=None,
                chat_info=chat_info,
                timestamp=datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )

        else:
            return

        await APIInterface.send_request(utils_obj=Ut, req_model=req_model)

    @staticmethod
    async def processing_action_add_chat_user(event: events.ChatAction.Event):
        act_msg = event.action_message

        chat_id, chat_info = await ChatInfo.obj_from_peer(act_msg.peer_id)
        if not chat_id:
            return

        added_by_user = await Config.TG_CLIENT.get_entity(act_msg.from_id)
        added_by = await FromUser.obj_from_sender(added_by_user)
        if not added_by:
            return

        owner_info = None
        if isinstance(act_msg.peer_id, types.PeerChat):
            full_chat = await Config.TG_CLIENT(GetFullChatRequest(act_msg.peer_id.chat_id))
            for member in full_chat.full_chat.participants.participants:
                if isinstance(member, types.ChatParticipantCreator):
                    user = await Config.TG_CLIENT.get_entity(member)
                    owner_info = await FromUser.obj_from_sender(user)
                    break

        elif isinstance(act_msg.peer_id, types.PeerChannel):
            channel_admins = await Config.TG_CLIENT(GetParticipantsRequest(
                channel=act_msg.peer_id,
                filter=types.ChannelParticipantsAdmins(),
                offset=0,
                limit=200,
                hash=0
            ))

            for member in channel_admins.participants:
                if not isinstance(member, types.ChannelParticipantCreator):
                    continue

                for user in channel_admins.users:
                    if member.user_id != user.id:
                        continue

                    owner_info = await FromUser.obj_from_sender(user)
                    break

                break

        else:
            return

        await APIInterface.send_request(
            utils_obj=Ut,
            req_model=BotAdded(
                chat_id=chat_id,
                chat_info=chat_info,
                owner_info=owner_info,
                added_by=added_by,
                timestamp=act_msg.date.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        )

    @staticmethod
    async def processing_action_chat_delete_user(event: events.ChatAction.Event):
        act_msg = event.action_message
        if isinstance(act_msg.peer_id, types.PeerChannel):
            chat_id = int(f"-100{act_msg.peer_id.channel_id}")

        elif isinstance(act_msg.peer_id, types.PeerChat):
            chat_id = int(f"-{act_msg.peer_id.chat_id}")

        else:
            return

        await APIInterface.send_request(
            utils_obj=Ut,
            req_model=BotDeleted(
                chat_id=chat_id,
                timestamp=act_msg.date.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        )

    @staticmethod
    async def processing_topic_edited(event: types.UpdateNewChannelMessage):
        msg_obj = event.message

        sender = await Config.TG_CLIENT.get_entity(msg_obj.from_id)
        from_user = await FromUser.obj_from_sender(sender)
        if not from_user:
            return None

        chat_id, chat_info = await ChatInfo.obj_from_peer(msg_obj.peer_id)
        if not chat_id:
            return None

        topic_id, title, icon_color = await TgTools.get_topic_data_from_msg(msg_obj)
        return await APIInterface.send_request(
            utils_obj=Ut,
            req_model=TopicEdited(
                chat_id=chat_id,
                topic_id=topic_id,
                title=title,
                icon_color=icon_color,
                sender=from_user,
                chat_info=chat_info,
                timestamp=msg_obj.date.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
        )
