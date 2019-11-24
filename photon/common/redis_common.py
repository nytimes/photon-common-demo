import os
import logging
from pathlib import Path
from typing import Any, List, Mapping, Optional

from redis import Redis
from redis.exceptions import ResponseError
from redistimeseries.client import Client as RedisTS

from photon.common.redis_semaphore import ParamsNT, Semaphore
from photon.common.config_context_common import ConfigContextCommon


class RedisTimeSeriesCommon(object):
    """
    Wrapper class for accessing RedisTimeSeries.

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
        self._redists = RedisTS(host=redis_host, port=redis_port)
        self._app_key = f"app:{config.PACKAGE_NICKNAME}"
        self._app = config.PACKAGE_NICKNAME
        self._name = "A"

    def create(
        self,
        name: str = "A",
        thread: int = 0,
        retentionms: int = 0,
        **kwargs: Mapping[str, Any],
    ) -> None:
        self._name = name
        ts = f"ts:{name}.app:{self._app}.T:{thread}"
        labeld = {"ts": name, "app": self._app, "T": thread}
        labeld.update(kwargs)
        print(f"create: {ts}")
        self._redists.create(ts, retention_msecs=retentionms, labels=labeld)

    def _alter(
        self,
        name: str = "A",
        thread: int = 0,
        retentionms: int = 0,
        **kwargs: Mapping[str, Any],
    ) -> None:
        self._name = name
        ts = f"ts:{name}.app:{self._app}.T:{thread}"
        labeld = {"ts": name, "app": self._app, "T": thread}
        labeld.update(kwargs)
        self._redists.alter(ts, retention_msecs=retentionms, labels=labeld)

    def ensure(
        self,
        name: str = "A",
        thread: int = 0,
        retentionms: int = 0,
        **kwargs: Mapping[str, Any],
    ) -> None:
        try:
            self.create(name=name, retentionms=retentionms, thread=thread, **kwargs)
        except ResponseError:
            self._alter(name=name, retentionms=retentionms, thread=thread, **kwargs)

    def delete(self, name: str = "", thread: int = 0,) -> None:
        ts = f"ts:{name or self._name}.app:{self._app}.T:{thread}"
        print(f"delete: {ts}")
        self._redists.delete(ts)

    def add_value(
        self, value: int = 0, timestampms: int = 0, name: str = "", thread: int = 0
    ) -> None:
        ts = f"ts:{name or self._name}.app:{self._app}.T:{thread}"
        self._redists.add(ts, timestampms or "*", value)


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
        self._app_key = f"app:{config.PACKAGE_NICKNAME}"
        self._semaphores: List[Semaphore] = []

    def get_semaphore(self, name: str = "default") -> Semaphore:
        """
        Initialize a redis-based distributed multi-process/multi-thread
        Semaphore object for the calling thread.

        Args:
            name: The shared name of the semaphore.

        Returns:
            A Semaphore object for acquire() & release() or use as a context mgr (with).
        """
        app_sem_key = f"{self._app_key}.semaphore:{name}"
        semaphore = Semaphore(self._redis, name=app_sem_key)
        self._semaphores.append(semaphore)

        return semaphore

    def get_semaphore_params(self, name: str = "default") -> Optional[ParamsNT]:
        """
        Proxy to Semaphore get_params() static method

        """
        return Semaphore.get_params(self._redis, name=name)

    def set_semaphore_params(
        self,
        name: str = "default",
        capacity: int = 100,
        timeoutms: int = 60 * 60 * 1000,
        decay: float = 0.9999,
        sleepms: int = 100,
    ) -> None:
        """
        Proxy to Semaphore set_params() static method

        """
        Semaphore.set_params(
            self._redis,
            name=name,
            capacity=capacity,
            timeoutms=timeoutms,
            sleepms=sleepms,
            decay=decay,
        )

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
