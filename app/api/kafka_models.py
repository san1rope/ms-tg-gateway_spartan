from typing import Optional

from pydantic import BaseModel


class PhotoData(BaseModel):
    width: int
    height: int
    file_size: int


class VideoData(BaseModel):
    width: int
    height: int
    duration: int
    file_size: int


class AudioData(BaseModel):
    duration: int
    title: str
    performer: str
    file_size: int


class DocumentData(BaseModel):
    file_name: str
    mime_type: str
    file_size: int


class StickerData(BaseModel):
    width: int
    height: int
    is_animated: bool
    file_size: int


class VoiceData(BaseModel):
    duration: int
    mime_type: str
    file_size: int


class GIFData(BaseModel):
    width: int
    height: int
    duration: int
    file_size: int


class MediaFileInfo(BaseModel):
    file_type: str
    file_name: Optional[str] = None
    mime_type: str
    file_size: int
    width: Optional[int] = None
    height: Optional[int] = None
    created_at: str


class MediaFileInfoResponse(BaseModel):
    status: str
    media_info: Optional[MediaFileInfo] = None


class SendMessageRequest(BaseModel):
    request_id: str
    chat_id: int
    text: str
    topic_id: Optional[int] = None
    parse_mode: str
    disable_notification: bool
    reply_to_message_id: Optional[int] = None


class EditMessageRequest(BaseModel):
    request_id: str
    chat_id: int
    message_id: int
    text: str
    parse_mode: str


class DeleteMessageRequest(BaseModel):
    request_id: str
    chat_id: int
    message_id: int


class MessagePinRequest(BaseModel):
    request_id: str
    chat_id: int
    message_id: int


class MessageUnpinRequest(BaseModel):
    request_id: str
    chat_id: int
    message_id: int


class SendPhotoRequest(BaseModel):
    request_id: str
    chat_id: int
    photo: str
    caption: str
    topic_id: Optional[int] = None
    parse_mode: str


class SendVideoRequest(BaseModel):
    request_id: str
    chat_id: int
    video: str
    caption: str
    topic_id: Optional[int] = None
    parse_mode: str


class SendAudioRequest(BaseModel):
    request_id: str
    chat_id: int
    audio: str
    caption: str
    topic_id: Optional[int] = None
    parse_mode: str


class SendDocumentRequest(BaseModel):
    request_id: str
    chat_id: int
    document: str
    caption: str
    topic_id: Optional[int] = None
    parse_mode: str


class SendStickerRequest(BaseModel):
    request_id: str
    chat_id: int
    sticker: str
    topic_id: int


class SendVoiceRequest(BaseModel):
    request_id: str
    chat_id: int
    voice: str
    caption: str
    topic_id: Optional[int] = None


class SendGIFRequest(BaseModel):
    request_id: str
    chat_id: int
    gif: str
    caption: str
    topic_id: Optional[int] = None
    parse_mode: str


class CreateTopicRequest(BaseModel):
    request_id: str
    chat_id: int
    title: str
    icon_color: int


class EditTopicRequest(BaseModel):
    request_id: str
    chat_id: int
    topic_id: int
    title: str


class DeleteTopicRequest(BaseModel):
    request_id: str
    chat_id: int
    topic_id: int


class MediaFileInfoRequest(BaseModel):
    request_id: str
    chat_id: int
    message_id: int
