import traceback
from typing import List

from telethon.tl import types
from telethon.errors import ChatAdminRequiredError
from telethon.tl.functions.channels import GetAdminLogRequest
from telethon.tl.functions.messages import GetStickerSetRequest, GetForumTopicsByIDRequest

from app.api.webhook import FromUser, MediaPhoto, MediaSticker, MediaAudio, MediaVideoGIF, MediaDocument
from app.config import Config
from app.tg.redis_service import RedisInterface
from app.utils import Utils as Ut


class TgTools:

    @staticmethod
    async def get_userdata_deleted_by(message_ids: List[int], input_chat, retries: int = 3):
        try:
            act_filter = types.ChannelAdminLogEventsFilter(
                join=False, leave=False, invite=False, ban=False, unban=True, promote=False, demote=False, info=False,
                settings=False, pinned=False, edit=False, delete=True, sub_extend=False, send=False, invites=False,
                group_call=False, forums=True
            )

            limit = 200
            max_id = 0
            for _ in range(5):
                res = await Config.TG_CLIENT(
                    GetAdminLogRequest(
                        channel=input_chat, events_filter=act_filter, limit=limit, max_id=max_id, min_id=0, q=""))
                if not res.events:
                    break

                for res_event in res.events:
                    act = res_event.action
                    if isinstance(act, types.ChannelAdminLogEventActionDeleteMessage):
                        mid = getattr(act.message, "id", None)
                        print(res_event)
                        if mid in set(message_ids):
                            user = await Config.TG_CLIENT.get_entity(res_event.user_id)
                            deleted_by = await FromUser.obj_from_sender(user)
                            if not deleted_by:
                                raise ValueError("Variable `deleted_by` is empty")

                            topic_id = await TgTools.get_topic_data_from_msg(act.message, only_id=True)
                            return {"type": "delete_messages", "deleted_by": deleted_by, "topic_id": topic_id}

                    elif isinstance(act, types.ChannelAdminLogEventActionDeleteTopic):
                        if act.topic.id in message_ids:
                            user = await Config.TG_CLIENT.get_entity(res_event.user_id)
                            deleted_by = await FromUser.obj_from_sender(user)
                            if not deleted_by:
                                raise ValueError("Variable `deleted_by` is empty")

                            return {"type": "delete_topic", "deleted_by": deleted_by, "topic_id": act.topic.id}

                max_id = min((ev.id for ev in res.events), default=0)

        except ChatAdminRequiredError:
            await Ut.log(
                f"Could not obtain the event log, insufficient administrator rights! chat_id: -100{input_chat.channel_id}"
            )

        except Exception:
            print(traceback.format_exc())
            if retries:
                return await TgTools.get_userdata_deleted_by(message_ids, input_chat, retries - 1)

        return None

    @staticmethod
    async def get_media_data_from_msg(msg_obj: types.Message):
        media = None
        msg_type = 1

        if isinstance(msg_obj.media, types.MessageMediaPhoto):
            best_size = await Ut.best_photo_size(photo=msg_obj.media.photo)
            media = MediaPhoto(
                file_size=best_size["size_bytes"],
                mime_type="image/jpeg",
                width=best_size["w"],
                height=best_size["h"],
            )
            msg_type = 2

        elif isinstance(msg_obj.media, types.MessageMediaDocument):
            doc = msg_obj.media.document

            attr_sticker, attr_video, attr_audio, attr_filename, attr_animated = None, None, None, None, None
            for attr in doc.attributes:
                if isinstance(attr, types.DocumentAttributeSticker):
                    attr_sticker = attr

                elif isinstance(attr, types.DocumentAttributeVideo):
                    attr_video = attr

                elif isinstance(attr, types.DocumentAttributeAudio):
                    attr_audio = attr

                elif isinstance(attr, types.DocumentAttributeFilename):
                    attr_filename = attr

                elif isinstance(attr, types.DocumentAttributeAnimated):
                    attr_animated = attr

            if attr_sticker:
                if isinstance(attr_sticker.stickerset, types.InputStickerSetShortName):
                    short_name = attr_sticker.stickerset.short_name

                else:
                    sticker_set_res = await Config.TG_CLIENT(
                        GetStickerSetRequest(stickerset=attr_sticker.stickerset, hash=0))
                    short_name = getattr(sticker_set_res.set, "short_name", None)

                media = MediaSticker(
                    file_size=doc.size,
                    mime_type=doc.mime_type,
                    set_name=short_name,
                    emoji=attr_sticker.alt
                )
                msg_type = 4

            elif attr_audio:
                media = MediaAudio(
                    file_size=doc.size,
                    mime_type=doc.mime_type,
                    duration=attr_audio.duration,
                    is_voice=attr_audio.voice
                )
                msg_type = 7

            elif attr_animated:
                media = MediaVideoGIF(
                    file_size=doc.size,
                    mime_type="image/gif",
                    duration=attr_video.duration,
                    width=attr_video.w,
                    height=attr_video.h,
                    supports_streaming=attr_video.supports_streaming
                )
                msg_type = 11

            elif attr_video:
                media = MediaVideoGIF(
                    file_size=doc.size,
                    mime_type=doc.mime_type,
                    duration=attr_video.duration,
                    width=attr_video.w,
                    height=attr_video.h,
                    supports_streaming=attr_video.supports_streaming
                )
                msg_type = 11

            elif attr_filename:
                media = MediaDocument(
                    file_size=doc.size,
                    mime_type=doc.mime_type,
                    file_name=attr_filename.file_name
                )
                msg_type = 6

        return msg_type, media

    @staticmethod
    async def get_topic_data_from_msg(msg_obj: types.Message, only_id: bool = False, use_cache: bool = True):
        topic_id = None
        title = ""
        icon_color = 0
        if isinstance(msg_obj.reply_to, types.MessageReplyHeader) and msg_obj.reply_to.forum_topic:
            topic_id = msg_obj.reply_to.reply_to_top_id if msg_obj.reply_to.reply_to_top_id \
                else msg_obj.reply_to.reply_to_msg_id

            if only_id:
                return topic_id

            flag_update_cache = False
            title, icon_color = None, None
            if hasattr(msg_obj, "action") and isinstance(msg_obj.action, types.MessageActionTopicEdit):
                flag_update_cache = True
                if hasattr(msg_obj.action, "title"):
                    title = msg_obj.action.title

                if hasattr(msg_obj.action, "icon_color"):
                    icon_color = msg_obj.action.icon_color

            if use_cache and ((not title) or (not icon_color)):
                topic_data = await RedisInterface().get_topic_data(msg_obj.peer_id.channel_id, topic_id)
                title = topic_data[0] if not title else title
                icon_color = topic_data[1] if not icon_color else icon_color

            if (not title) or (not icon_color):
                flag_update_cache = True
                topic_data = await Config.TG_CLIENT(GetForumTopicsByIDRequest(peer=msg_obj.peer_id, topics=[topic_id]))
                if topic_data:
                    title = topic_data.topics[0].title
                    icon_color = topic_data.topics[0].icon_color

            if flag_update_cache:
                await RedisInterface().set_topic_data(
                    chat_id=msg_obj.peer_id.channel_id, topic_id=topic_id, title=title, icon_color=icon_color
                )

        if only_id:
            return topic_id

        return topic_id, title, icon_color
