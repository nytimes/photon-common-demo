import os
import logging
from pathlib import Path
from typing import Any, List, Mapping, Optional, Sequence

import pandas as pd
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
        self._name = "A"

    def create(
        self,
        name: str = "A",
        thread: int = 0,
        retentionms: int = 0,
        **kwargs: Mapping[str, Any],
    ) -> None:
        self._name = name
        key = f"ts:{name}.T:{thread}"
        labeld = {"ts": name, "T": thread}
        labeld.update(kwargs)
        self._redists.create(key, retention_msecs=retentionms, labels=labeld)

    def _alter(
        self,
        name: str = "A",
        thread: int = 0,
        retentionms: int = 0,
        **kwargs: Mapping[str, Any],
    ) -> None:
        self._name = name
        key = f"ts:{name}.T:{thread}"
        labeld = {"ts": name, "T": thread}
        labeld.update(kwargs)
        self._redists.alter(key, retention_msecs=retentionms, labels=labeld)

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

    def delete(self, name: str = "", thread: int = 0) -> None:
        key = f"ts:{name or self._name}.T:{thread}"
        self._redists.delete(key)

    def add_value(
        self, value: int = 0, timestampms: int = 0, name: str = "", thread: int = 0
    ) -> None:
        key = f"ts:{name or self._name}.T:{thread}"
        self._redists.add(key, timestampms or "*", value)

    def get_keys_by_names(self, names: Sequence[str]) -> List[str]:
        namelist = (",").join(names)
        filters = [f"ts=({namelist})"]
        keys = self._redists.queryindex(filters)

        return keys  # type: ignore

    def get_threads_by_name(self, name: str) -> List[int]:
        filters = [f"ts={name}"]
        keys = self._redists.queryindex(filters)
        threads = [int(key.split("T:")[-1]) for key in keys]

        return threads

    def get_dataframe(self, key: str) -> pd.DataFrame:
        tsts = self._redists.range(key, 0, -1)  # get all datapoints for the key
        tsds = [{"dt": dt, "columns": key, "value": float(value)} for dt, value in tsts]
        tsdf = pd.DataFrame(tsds)
        tsdf["dt"] = pd.to_datetime(tsdf.dt, unit="ms")
        tspivotdf = tsdf.pivot(index="dt", columns="columns", values="value")

        return tspivotdf


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
