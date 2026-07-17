from __future__ import annotations

import logging
import random
import time
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar, cast

from sqlalchemy.exc import DBAPIError


LOGGER = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")

READ_RETRY_MIN_DELAY_SECONDS = 0.1
READ_RETRY_MAX_DELAY_SECONDS = 0.25
_READ_RETRY_DEPTH: ContextVar[int] = ContextVar("price_cache_read_retry_depth", default=0)


def retry_disconnected_read(method: Callable[P, R]) -> Callable[P, R]:
    """Retry one complete repository read after an invalidated DB connection."""

    operation = method.__qualname__

    @wraps(method)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        if _READ_RETRY_DEPTH.get() > 0:
            return method(*args, **kwargs)

        depth_token = _READ_RETRY_DEPTH.set(1)
        try:
            retried = False
            while True:
                try:
                    result = method(*args, **kwargs)
                except DBAPIError as exc:
                    if not exc.connection_invalidated:
                        raise
                    if retried:
                        LOGGER.error(
                            "Repository read failed after connection retry: "
                            "operation=%s attempt=2 error_type=%s",
                            operation,
                            type(exc).__name__,
                        )
                        raise

                    retried = True
                    LOGGER.warning(
                        "Repository read encountered a disconnected database connection; retrying: "
                        "operation=%s attempt=1",
                        operation,
                    )
                    repository = cast(Any, args[0])
                    repository.engine.dispose()
                    delay = random.uniform(
                        READ_RETRY_MIN_DELAY_SECONDS,
                        READ_RETRY_MAX_DELAY_SECONDS,
                    )
                    time.sleep(delay)
                    continue

                if retried:
                    LOGGER.info(
                        "Repository read recovered after connection retry: operation=%s attempt=2",
                        operation,
                    )
                return result
        finally:
            _READ_RETRY_DEPTH.reset(depth_token)

    return wrapped
