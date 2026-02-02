import json

from aiokafka import AIOKafkaConsumer

from app.config import Config
from app.api.kafka_models import *
from app.tg.actions import UserActions
from app.utils import Utils as Ut


class KafkaInterface:

    @staticmethod
    async def coroutine_from_payload(payload):
        rt = payload.get("request_type")
        if not rt:
            return None

        print(f"rt = {rt}")
        payload.pop("request_type")
        if rt == "send_message":
            return UserActions.send_message(SendMessageRequest(**payload))

        elif rt == "edit_message":
            return UserActions.edit_message(EditMessageRequest(**payload))

        elif rt == "delete_message":
            return UserActions.delete_message(DeleteMessageRequest(**payload))

        elif rt == "message_pin":
            return UserActions.message_pin(MessagePinRequest(**payload))

        elif rt == "message_unpin":
            return UserActions.message_unpin(MessageUnpinRequest(**payload))

        elif rt == "send_photo":
            return UserActions.send_photo(SendPhotoRequest(**payload))

        elif rt == "send_video":
            return UserActions.send_video(SendVideoRequest(**payload))

        elif rt == "send_audio":
            return UserActions.send_audio(SendAudioRequest(**payload))

        elif rt == "send_document":
            return UserActions.send_document(SendDocumentRequest(**payload))

        elif rt == "send_sticker":
            return UserActions.send_sticker(SendStickerRequest(**payload))

        elif rt == "send_voice":
            return UserActions.send_voice(SendVoiceRequest(**payload))

        elif rt == "send_gif":
            return UserActions.send_gif(SendGIFRequest(**payload))

        elif rt == "create_topic":
            return UserActions.create_topic(CreateTopicRequest(**payload))

        elif rt == "edit_topic":
            return UserActions.edit_topic(EditTopicRequest(**payload))

        elif rt == "delete_topic":
            return UserActions.delete_topic(DeleteTopicRequest(**payload))

        elif rt == "media_file_info":
            return UserActions.get_media_file_info(MediaFileInfoRequest(**payload))

        else:
            return None

    @classmethod
    async def start_polling(cls):
        await Ut.log("Kafka listener has been started!")

        consumer = AIOKafkaConsumer(
            Config.KAFKA_TOPIC_COMMANDS,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_IP,
            group_id="demo-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await consumer.start()

        try:
            async for msg in consumer:
                print(f"{msg.topic}:{msg.partition}@{msg.offset} key={msg.key} value={msg.value}")

                payload_coroutine = await cls.coroutine_from_payload(msg.value)
                if payload_coroutine:
                    await Config.QUEUE_WORKER.put(payload_coroutine)

        finally:
            await consumer.stop()
