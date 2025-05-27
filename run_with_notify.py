# run_with_notify.py
import os      
import asyncio, logging, time, traceback
from datetime import timedelta

import error_notifier  

try:
    from hrclubrtbot import bot as main_bot        # вариант, если проект оформят как пакет
except ModuleNotFoundError:
    import bot as main_bot                         # работает, пока файл bot.py лежит рядом

from error_notifier import (
    notify_startup, notify_exception, notify_log_record, notify_heartbeat
)

HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "3600"))  # раз в час


async def _heartbeat_task(start_time: float):
    """Периодический пинг «бот жив»."""
    while True:
        await asyncio.sleep(HEARTBEAT_SECONDS)
        uptime = timedelta(seconds=int(time.time() - start_time))
        await notify_heartbeat(str(uptime))


async def _runner():
    loop = asyncio.get_running_loop()
    start_time = time.time()

    # ➊ Cообщаем о старте
    await notify_startup()
    await error_notifier.start_background_tasks(loop)


    # ➋ Ловим необработанные исключения в тасках
    def loop_exception_handler(loop, context):
        exc = context.get("exception")
        if exc:
            loop.create_task(notify_exception(exc))
        else:
            loop.create_task(
                notify_log_record(f"Loop error: {context!r}")
            )
    loop.set_exception_handler(loop_exception_handler)

    # ➌ Перекидываем все logging.ERROR в Telegram
    class TgErrorHandler(logging.Handler):
        def emit(self, record):
            if record.levelno >= logging.ERROR:
                loop.create_task(notify_log_record(self.format(record)))
    handler = TgErrorHandler()
    handler.setFormatter(logging.Formatter(
        "%(levelname)s • %(name)s – %(message)s\n%(pathname)s:%(lineno)d"
    ))
    logging.getLogger().addHandler(handler)

    # ➍ Запускаем heartbeat-пинг
    loop.create_task(_heartbeat_task(start_time))

    # ➎ Запускаем «родной» main()
    try:
        await main_bot.main()
    except Exception as e:             # крайний рубеж
        await notify_exception(e)
        raise


if __name__ == "__main__":
    asyncio.run(_runner())
