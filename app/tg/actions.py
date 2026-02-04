from typing import Union

from telethon.errors import MessageAuthorRequiredError, MessageNotModifiedError, BadRequestError
from telethon.tl.functions.messages import CreateForumTopicRequest, EditForumTopicRequest, DeleteTopicHistoryRequest
from telethon.tl import types as tt

from app.api.kafka import *
from app.api.kafka_models import MediaFileInfoRequest
from app.config import Config
from app.utils import Utils as Ut


class UserActions:

    @staticmethod
    async def get_peer_from_id(chat_id: Union[str, int]):
        chat_id = str(chat_id)
        if chat_id.startswith("-100"):
            return tt.PeerChannel(channel_id=int(chat_id[4:]))

        elif chat_id.startswith("-"):
            return tt.PeerChat(chat_id=int(chat_id[1:]))

        else:
            return tt.PeerUser(user_id=int(chat_id))

    @staticmethod
    async def send_message(payload: SendMessageRequest):
        try:
            result = await Config.TG_CLIENT.send_message(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                message=payload.text,
                parse_mode=payload.parse_mode,
                silent=payload.disable_notification,
                reply_to=payload.topic_id if payload.topic_id else payload.reply_to_message_id
            )
            print(f"result send_message = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def edit_message(payload: EditMessageRequest):
        try:
            result = await Config.TG_CLIENT.edit_message(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                message=payload.message_id,
                text=payload.text,
                parse_mode=payload.parse_mode
            )
            print(f"result edit_message = {result}")

        except MessageAuthorRequiredError:
            Config.LOGGER.error("Не удалось отредактировать сообщение! Бот не отправитель")

        except MessageNotModifiedError:
            Config.LOGGER.error("Не удалось отредактировать сообщение! Присланное содержимое не изменилось")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def delete_message(payload: DeleteMessageRequest):
        try:
            result = await Config.TG_CLIENT.delete_messages(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                message_ids=payload.message_id
            )
            print(f"result delete_message = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def message_pin(payload: MessagePinRequest):
        try:
            result = await Config.TG_CLIENT.pin_message(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                message=payload.message_id
            )
            print(f"result message_pin = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def message_unpin(payload: MessageUnpinRequest):
        try:
            result = await Config.TG_CLIENT.unpin_message(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                message=payload.message_id
            )
            print(f"result message_unpin = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_photo(payload: SendPhotoRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.photo,
                caption=payload.caption,
                reply_to=payload.topic_id,
                parse_mode=payload.parse_mode
            )
            print(f"result send_photo = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_video(payload: SendVideoRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.video,
                caption=payload.caption,
                reply_to=payload.topic_id,
                parse_mode=payload.parse_mode
            )
            print(f"result send_photo = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_audio(payload: SendAudioRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.audio,
                caption=payload.caption,
                reply_to=payload.topic_id,
                parse_mode=payload.parse_mode
            )
            print(f"result send_photo = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_document(payload: SendDocumentRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.document,
                caption=payload.caption,
                reply_to=payload.topic_id,
                parse_mode=payload.parse_mode
            )
            print(f"result send_photo = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_sticker(payload: SendStickerRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.sticker,
                reply_to=payload.topic_id
            )
            print(f"result send_sticker = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_voice(payload: SendVoiceRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.voice,
                caption=payload.caption,
                reply_to=payload.topic_id,
                voice_note=True
            )
            print(f"result send_voice = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def send_gif(payload: SendGIFRequest):
        try:
            result = await Config.TG_CLIENT.send_file(
                entity=await UserActions.get_peer_from_id(payload.chat_id),
                file=payload.gif,
                caption=payload.caption,
                parse_mode=payload.parse_mode,
                reply_to=payload.topic_id,
                video_note=False
            )
            print(f"result send_gif = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def create_topic(payload: CreateTopicRequest):
        try:
            result = await Config.TG_CLIENT(CreateForumTopicRequest(
                peer=await UserActions.get_peer_from_id(payload.chat_id),
                title=payload.title,
                icon_color=payload.icon_color
            ))
            print(f"result create_topic = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def edit_topic(payload: EditTopicRequest):
        try:
            result = await Config.TG_CLIENT(EditForumTopicRequest(
                peer=await UserActions.get_peer_from_id(payload.chat_id),
                topic_id=payload.topic_id,
                title=payload.title
            ))
            print(f"result edit_topic = {result}")

        except BadRequestError as ex:
            Config.LOGGER.error(f"Не удалось отредактировать топик! ex: {ex}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def delete_topic(payload: DeleteTopicRequest):
        try:
            result = await Config.TG_CLIENT(DeleteTopicHistoryRequest(
                peer=await UserActions.get_peer_from_id(payload.chat_id),
                top_msg_id=payload.topic_id
            ))
            print(f"result delete_topic = {result}")

        except Exception:
            print(traceback.format_exc())

    @staticmethod
    async def get_media_file_info(payload: MediaFileInfoRequest):
        try:
            entity = await Config.TG_CLIENT.get_entity(payload.chat_id)
            msg = await Config.TG_CLIENT.get_messages(entity, ids=payload.message_id)
            if not msg or not msg.media:
                Config.LOGGER.error(f"Не нашел медиа по chat_id={payload.chat_id}; msg_id={payload.message_id}!")
                return

            if isinstance(msg.media, tt.MessageMediaPhoto):
                largest_size = msg.media.photo.sizes[-1]
                info_obj = MediaFileInfoResponse(
                    status=Ut.STATUS_SUCCESS,
                    request_id=payload.request_id,
                    media_info=MediaFileInfo(
                        file_type="photo",
                        file_name=None,
                        mime_type="image/jpeg",
                        file_size=getattr(largest_size, "size", 0),
                        width=largest_size.w if hasattr(largest_size, "w") else None,
                        height=largest_size.h if hasattr(largest_size, "h") else None,
                        created_at=msg.date.isoformat()
                    )
                )

            elif isinstance(msg.media, tt.MessageMediaDocument):
                result = {
                    "file_type": "document"
                }
                for attr in msg.media.document.attributes:
                    if isinstance(attr, tt.DocumentAttributeFilename):
                        result["file_name"] = attr.file_name

                    elif isinstance(attr, tt.DocumentAttributeVideo):
                        result["file_type"] = "video"
                        result["width"] = attr.w
                        result["height"] = attr.h

                    elif isinstance(attr, tt.DocumentAttributeAudio):
                        result["file_type"] = "voice" if attr.voice else "audio"

                    elif isinstance(attr, tt.DocumentAttributeSticker):
                        result["file_type"] = "sticker"

                    elif isinstance(attr, tt.DocumentAttributeImageSize):
                        result["width"] = attr.w
                        result["height"] = attr.h

                info_obj = MediaFileInfoResponse(
                    status=Ut.STATUS_SUCCESS,
                    request_id=payload.request_id,
                    media_info=MediaFileInfo(
                        mime_type=msg.media.document.mime_type,
                        file_size=msg.media.document.size,
                        created_at=msg.date.isoformat(),
                        **result
                    )
                )

            else:
                info_obj = MediaFileInfoResponse(status=Ut.STATUS_FAIL, request_id=payload.request_id, media_info=None)

            print(f"result info_obj = {info_obj}")
            result = await Config.KAFKA_INTERFACE_OBJ.send_msg(payload=info_obj, topic="tg-responses")
            print(f"response kafka msg = {result}")

        except Exception:
            print(traceback.format_exc())
