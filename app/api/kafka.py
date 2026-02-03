import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from app.config import Config
from app.api.kafka_models import *
from app.tg.actions import UserActions
from app.utils import Utils as Ut


class KafkaInterface:
    BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", Config.KAFKA_BOOTSTRAP_IP)
    PRODUCER: Optional[AIOKafkaProducer] = None
    CONSUMER: Optional[AIOKafkaConsumer] = None

    @classmethod
    async def init_producer(cls) -> bool:
        if cls.PRODUCER is None:
            cls.PRODUCER = AIOKafkaProducer(
                bootstrap_servers=cls.BOOTSTRAP,
                enable_idempotence=True,
                acks="all",
                max_batch_size=16384,
                value_serializer=lambda v: json.loads(v.decode("utf-8")),
                key_serializer=lambda k: k.encode("utf-8")
            )

            try:
                await cls.PRODUCER.start()
                Config.LOGGER.info("Kafka Producer has been init")
                return True

            except KafkaConnectionError as ex:
                Config.LOGGER.critical(f"Kafka Connection Error! ex: {ex}")
                return False

    @classmethod
    async def init_consumer(cls) -> bool:
        if cls.CONSUMER is None:
            cls.CONSUMER = AIOKafkaConsumer(
                Config.KAFKA_TOPIC_COMMANDS,
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_IP,
                group_id="demo-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            try:
                await cls.CONSUMER.start()
                Config.LOGGER.info("Kafka Consumer has been init")
                return True

            except KafkaConnectionError as ex:
                Config.LOGGER.critical(f"Kafka Connection Error! ex: {ex}")
                return False

    @staticmethod
    async def coroutine_from_payload(payload):
        rt = payload.get("request_type")
        if not rt:
            return None

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
    async def start_polling(cls) -> Optional[bool]:
        await Ut.log("Kafka listener has been started!")

        if not cls.CONSUMER:
            result = await cls.init_consumer()
            if not result:
                Config.LOGGER.critical("Kafka Consumer is not initialized.")
                return False

        try:
            async for msg in cls.CONSUMER:
                print(f"{msg.topic}:{msg.partition}@{msg.offset} key={msg.key} value={msg.value}")

                payload_coroutine = await cls.coroutine_from_payload(msg.value)
                if payload_coroutine:
                    await Config.QUEUE_WORKER.put(payload_coroutine)

        finally:
            await cls.CONSUMER.stop()

    @classmethod
    async def send_msg(cls, payload: BaseModel, topic: str):
        if cls.PRODUCER is None:
            raise RuntimeError("Kafka Producer is not initialized.")

        data = payload.model_dump()
        data["request_id"] = payload.request_id
        data["request_type"] = "media_file_info"

        try:
            metadata = await cls.PRODUCER.send_and_wait(topic=topic, key=data["request_id"], value=data)
            return {
                "message_id": data["request_id"],
                "topic": metadata.topic,
                "partition": metadata.partition,
                "offset": metadata.offset,
            }

        except Exception as ex:
            return {"error": str(ex)}

