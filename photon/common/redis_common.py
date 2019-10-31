import os
import logging
from typing import List
from pathlib import Path

from redis import Redis

from photon.common.redis_semaphore import Semaphore
from photon.common.config_context_common import ConfigContextCommon


class RedisCommon(object):
    """
    Wrapper class for accessing Redis.

    """

    def __init__(self, config: ConfigContextCommon) -> None:
        """
        Args:
            config: A config object.
        """

        logname = Path(__file__).stem
        self._logger = logging.getLogger(f"{config.PACKAGE_NAME}.{logname}")
        redis_host = os.environ.get("REDISHOST", "localhost")
        redis_port = int(os.environ.get("REDISPORT", 6379))
        self._redis = Redis(host=redis_host, port=redis_port)
        self._au_key = f"app:{config.PACKAGE_NICKNAME}.util:{config.util_cmd}"
        self._semaphore_value = 100
        self._semaphore_timeout = 60 * 60  # an hour
        self._semaphore_name = "default"
        self._semaphores: List[Semaphore] = []

    def get_semaphore(
        self, name: str = "", value: int = 0, timeout: int = 0, sleep: float = 0.1
    ) -> Semaphore:
        """
        Initialize a redis-based distributed multi-process/multi-thread
        Semaphore object for the calling thread.

        Args:
            name: The shared name of the semaphore.
            timeout: Duration until expiration.
            sleep: The seconds to sleep between polls
                   when trying to acquire the semaphore.

        Returns:
            A Semaphore object for acquire() & release() or use as a context mgr (with).
        """
        name = name or self._semaphore_name
        aus_key = f"{self._au_key}.semaphore:{name}"
        timeout = timeout or self._semaphore_timeout
        value = value or self._semaphore_value

        semaphore = Semaphore(
            self._redis, name=aus_key, value=value, timeout=timeout, sleep=sleep
        )

        self._semaphores.append(semaphore)
        return semaphore

    def failfast(self) -> None:
        """
        Upon a failfast event, an app should call this method, e.g. in its failfast.py,
        to explicitly delete any traces of its semaphores in redis.
        """

        for semaphore in self._semaphores:
            try:
                semaphore.release()
            except Exception:
                pass
