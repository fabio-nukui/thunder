from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from typing import Callable

import aiofiles
from aiofiles.threadpool.text import AsyncTextIOWrapper


class ExtraDataFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if data := getattr(record, "data", None):
            return f"{super().format(record)}; data={json.dumps(data)}"
        return super().format(record)


def set_default_logger(logger_cls: type[logging.Logger]):
    logging.root.manager.setLoggerClass(logger_cls)
    for name, logger in logging.root.manager.loggerDict.items():
        if isinstance(logger, logger_cls):
            continue
        if isinstance(logger, logging.Logger):
            new_logger = logger_cls(name, logger.level)
            for handler in logger.handlers:
                new_logger.addHandler(handler)
            for filter in logger.filters:
                new_logger.addFilter(filter)
            new_logger.manager = logging.root.manager
            logging.root.manager.loggerDict[name] = new_logger
            logging.root.manager._fixupParents(new_logger)  # type: ignore
            if name in sys.modules and getattr(sys.modules[name], "log", None) is logger:
                setattr(sys.modules[name], "log", new_logger)


def _apply_root_configs(logger: logging.Logger):
    for handler in logging.root.handlers:
        logger.addHandler(handler)
    for filter_ in logging.root.filters:
        logger.addFilter(filter_)
    if logger.level == logging.NOTSET:
        logger.level = logging.root.level


class AsyncLogger(logging.Logger):
    def __init__(
        self,
        name: str,
        level: int | str = logging.NOTSET,
        apply_root_configs: bool = False,
    ):
        super().__init__(name, level)
        self._loop = asyncio.get_event_loop()
        if apply_root_configs:
            _apply_root_configs(self)

    def callHandlers(self, record: logging.LogRecord):
        c: logging.Logger | None = self
        while c:
            for handler in c.handlers:
                if record.levelno >= handler.level:
                    if isinstance(handler, AsyncHandler) and not self._loop.is_closed():
                        self._loop.create_task(handler.async_handle(record))
                    else:
                        handler.handle(record)
            if not c.propagate:
                return
            c = c.parent


class AsyncReformatterLogger(AsyncLogger):
    def __init__(
        self,
        name: str,
        level: int | str = logging.NOTSET,
        formatter: Callable = lambda x: x,
        apply_root_configs: bool = False,
    ):
        super().__init__(name, level, apply_root_configs)
        self._formatter = formatter

    def _log(self, level, msg, *args, **kwargs):
        super()._log(level, self._formatter(msg), *args, **kwargs)


class ReformatterLogger(logging.Logger):
    def __init__(
        self,
        name: str,
        level: int | str = logging.NOTSET,
        formatter: Callable = lambda x: x,
        apply_root_configs: bool = False,
    ):
        super().__init__(name, level)
        self._formatter = formatter
        if apply_root_configs:
            _apply_root_configs(self)

    def _log(self, level, msg, *args, **kwargs):
        super()._log(level, self._formatter(msg), *args, **kwargs)


class AsyncHandler(logging.Handler, ABC):
    def __init__(self, level: str | int, loop: asyncio.AbstractEventLoop = None):
        super().__init__(level=level)
        self.loop = asyncio.get_event_loop() if loop is None else loop

    async def async_handle(self, record: logging.LogRecord) -> bool:
        if rv := self.filter(record):
            self.acquire()
            try:
                await self.async_emit(record)
            except (RuntimeError, asyncio.CancelledError):
                self.emit(record)
            except Exception:
                self.handleError(record)
            finally:
                self.release()
        return rv

    @abstractmethod
    async def async_emit(self, record: logging.LogRecord):
        ...


class AsyncFileHandler(AsyncHandler):
    def __init__(
        self,
        filename: str,
        mode: str = "a",
        encoding: str = None,
        level: str | int = logging.NOTSET,
    ):
        super().__init__(level=level)
        self.file_path = os.path.abspath(filename)
        self.mode = mode
        self.encoding = encoding

        self._stream: AsyncTextIOWrapper | None = None
        self._initialization_lock = asyncio.Lock()

    def close(self):
        self.sync_close_stream()
        super().close()

    def sync_close_stream(self):
        try:
            self.loop.run_until_complete(self.close_stream())
        except RuntimeError:  # event loop probable closed
            pass

    async def close_stream(self):
        if self._stream is None:
            return
        await self._stream.flush()
        await self._stream.close()
        self._stream = None

    async def _get_stream(self) -> AsyncTextIOWrapper:
        return await aiofiles.open(
            file=self.file_path,
            mode=self.mode,  # type: ignore
            encoding=self.encoding,  # type: ignore
            loop=self.loop,
        )

    def emit(self, record: logging.LogRecord):
        print(f"Fallback emit: {self.format(record)}")
        if self._stream is not None:
            self.sync_close_stream()
        with open(self.file_path, self.mode, encoding=self.encoding) as f:
            f.write(self.format(record) + "\n")

    async def async_emit(self, record: logging.LogRecord):
        async with self._initialization_lock:
            if self._stream is None:
                self._stream = await self._get_stream()

        await self._stream.write(self.format(record) + "\n")
        await self._stream.flush()


class AsyncRotatingFileHandler(AsyncFileHandler):
    def __init__(
        self,
        filename: str,
        mode: str = "a",
        encoding: str = None,
        level: str | int = logging.NOTSET,
        maxBytes: int = None,
        backupCount: int = 0,
    ):
        super().__init__(filename=filename, mode=mode, encoding=encoding, level=level)
        self.maxBytes = maxBytes
        self.backupCount = backupCount
        self._rollover_lock = asyncio.Lock()

    async def async_emit(self, record: logging.LogRecord):
        if await self.should_rollover():
            async with self._rollover_lock:
                await self.do_rollover()
        await super().async_emit(record)

    async def should_rollover(self) -> bool:
        if self._stream is None or self._stream.closed or self.maxBytes is None:
            return False
        return await self._stream.tell() >= self.maxBytes

    async def do_rollover(self):
        if self._stream:
            await self._stream.close()
            self._stream = None
        if not os.path.exists(self.file_path):
            return
        for i in range(self.backupCount - 1, -1, -1):
            src = f"{self.file_path}.{i}" if i > 0 else self.file_path
            dst = f"{self.file_path}.{i + 1}"
            if os.path.exists(src):
                if os.path.exists(dst):
                    os.remove(dst)
                os.rename(src, dst)
