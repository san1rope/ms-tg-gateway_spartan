import asyncio
from time import time

from fastapi.responses import StreamingResponse
from fastapi import Header, Query

from app.config import Config


@Config.REST_APP.get("/internal/stream/{chat_id}/{msg_id}")
async def stream_video_from_tg(chat_id: int, msg_id: int, offset: int = Query(0)):
    async def generate_chunks():
        start_time = time.time()
        chunk_count = 0

        try:
            msg = await Config.TG_CLIENT.get_messages(chat_id, ids=msg_id)
            if not msg or not msg.media:
                Config.LOGGER.warning(
                    f"No media found for the specified parameters! chat_id: {chat_id}; msg_id: {msg_id}")
                return

            Config.LOGGER.info(f"Starting video stream! msg_id: {msg_id}; DC switch took: {time}")

            async for chunk in Config.TG_CLIENT.iter_download(
                    msg.media, offset=offset, chunk_size=524288, request_size=524288):
                chunk_count += 1

                if chunk_count % 10 == 0:
                    elapsed = time() - start_time
                    Config.LOGGER.info(
                        f"Video stream {msg_id} | Sent {chunk_count} chunks {chunk_count * 0.5}MB. Speed: {(chunk_count * 0.5) / elapsed:.2f} MB/s")

                yield chunk

        except (ConnectionResetError, asyncio.CancelledError):
            print(f"[{msg_id}] Stream interrupted by client (Connection Reset)")

        except Exception as ex:
            Config.LOGGER.error(ex)

    return StreamingResponse(
        generate_chunks(),
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Type": "video/mp4",
        }
    )
