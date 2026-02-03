import asyncio
import time

from fastapi.responses import StreamingResponse
from fastapi import Header, Query

from app.config import Config


@Config.REST_APP.get("/internal/stream/{chat_id}/{msg_id}")
async def stream_video_from_tg(chat_id: int, msg_id: int, offset: int = Query(0)):
    async def generate_chunks():
        start_time = time.time()
        chunk_count = 0

        try:
            # 1. Отримуємо метадані повідомлення
            # Це також ініціює переключення на потрібний DC (Exporting auth)
            msg = await Config.TG_CLIENT.get_messages(chat_id, ids=msg_id)

            if not msg or not msg.media:
                print(f"[{msg_id}] Media not found")
                return

            print(f"[{msg_id}] Starting stream. DC switch took: {time.time() - start_time:.2f}s")

            # 2. Потокове завантаження
            # Використовуємо 512KB (524288) — рідний і найшвидший розмір для Telegram API
            async for chunk in Config.TG_CLIENT.iter_download(
                    msg.media,
                    offset=offset,
                    chunk_size=524288,
                    request_size=524288
            ):
                chunk_count += 1

                # Логуємо кожен 10-й чанк (~5МБ), щоб не забивати консоль, але бачити прогрес
                if chunk_count % 10 == 0:
                    elapsed = time.time() - start_time
                    print(
                        f"[{msg_id}] Sent {chunk_count} chunks (~{chunk_count * 0.5}MB). Speed: {(chunk_count * 0.5) / elapsed:.2f} MB/s")

                yield chunk

        except (ConnectionResetError, asyncio.CancelledError):
            # Це стається, коли користувач закрив вкладку або перетащив повзунок плеєра
            print(f"[{msg_id}] Stream interrupted by client (Connection Reset)")

        except Exception as e:
            print(f"[{msg_id}] Unexpected error during streaming: {e}")

        finally:
            print(f"[{msg_id}] Stream finished. Client connected: {Config.TG_CLIENT.is_connected()}")

    # Повертаємо стрім з відповідним MIME-типом
    return StreamingResponse(
        generate_chunks(),
        media_type="video/mp4",
        headers={
            "Accept-Ranges": "bytes",
            "Content-Type": "video/mp4",
        }
    )

    return StreamingResponse(generate_chunks(), media_type="video/mp4")
