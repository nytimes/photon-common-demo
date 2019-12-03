import os
import time
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from redis import Redis
from redis.exceptions import ResponseError
from redistimeseries.client import Client as RedisTimeSeries

from photon.common.redis_semaphore import ParamsNT, Semaphore
from photon.common.config_context_common import ConfigContextCommon


class RedisTimeSeriesCommon(object):
    """
    Wrapper class for accessing RedisTimeSeries.

    """

    def __init__(
        self,
        config: ConfigContextCommon,
        name: str = "",
        thread: int = 0,
        transitionms: int = 0,
        retentionms: int = 0,
    ) -> None:
        """
        Args:
            config: A config object.
        """

        logname = Path(__file__).stem
        self._logger = logging.getLogger(f"{config.PACKAGE_NAME}.{logname}")
        redis_host = os.environ.get("REDISHOST", "localhost")
        redis_port = int(os.environ.get("REDISPORT", 6379))
        self._rts = RedisTimeSeries(host=redis_host, port=redis_port)
        self._name = name or getattr(config, "name", "A")
        self._thread = thread or getattr(config, "thread", 0)
        self._transitionms = transitionms or getattr(config, "transitionms", 100)

        self._retentionms = retentionms or getattr(
            config, "retentionms", 7 * 24 * 60 * 60 * 1000
        )

        self._previous_value = 0

    def create(
        self,
        name: str = "",
        thread: int = 0,
        transitionms: int = 0,
        retentionms: int = 0,
    ) -> None:
        if name:
            self._name = name

        if thread:
            self._thread = thread

        if transitionms:
            self._transitionms = transitionms

        if retentionms:
            self._retentionms = retentionms

        key = f"ts:{self._name}.T:{self._thread}"
        labeld = {"ts": self._name, "T": self._thread}
        self._rts.create(key, retention_msecs=self._retentionms, labels=labeld)

    def delete(self, name: str = "", thread: int = 0) -> None:
        key = f"ts:{name or self._name}.T:{thread or self._thread}"
        self._rts.delete(key)

    def _add(self, key: str, value: int) -> None:
        labeld = {"ts": self._name, "T": self._thread}

        i = 0
        while True:
            try:
                self._rts.add(  # use server timestamp
                    key, "*", value, retention_msecs=self._retentionms, labels=labeld
                )

                return
            except ResponseError:  # whoops - too quick, delay a bit
                if i < 5:
                    i += 1
                    time.sleep(0.001)
                else:
                    raise

    def add_value(self, value: int = 0, name: str = "", thread: int = 0) -> None:
        key = f"ts:{name or self._name}.T:{thread or self._thread}"

        if self._transitionms and value != self._previous_value:
            self._add(key, self._previous_value)
            time.sleep(self._transitionms / 1000)
            self._add(key, value)
            self._previous_value = value
        else:
            self._add(key, value)

    def get_keytuples_by_names(
        self, names: Sequence[str] = []
    ) -> List[Tuple[str, int]]:
        namelist = (",").join(names or [self._name])
        filters = [f"ts=({namelist})"]
        keys = self._rts.queryindex(filters)

        keytuples = []
        for key in keys:
            eles = key.split(".")
            keytuple = (eles[0].split(":")[-1], int(eles[1].split(":")[-1]))
            keytuples.append(keytuple)

        return keytuples

    def get_threads_by_name(self, name: str = "") -> Tuple[int, ...]:
        keytuples = self.get_keytuples_by_names([name or self._name])
        _, threads = zip(*keytuples)  # discard names

        return threads

    def get_dataframe(
        self, name: str = "", thread: int = 0, timestampms: int = 0
    ) -> pd.DataFrame:
        key = f"ts:{name or self._name}.T:{thread or self._thread}"
        datapointts = self._rts.range(key, timestampms, -1)

        if not datapointts:
            return pd.DataFrame()

        dts, values = zip(*datapointts)
        datapointdf = pd.DataFrame({"dt": dts, key: [float(v) for v in values]})
        datapointdf["dt"] = pd.to_datetime(datapointdf.dt, unit="ms")
        return datapointdf.set_index("dt")


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
        timeoutms: int = 10 * 60 * 1000,
        decay: float = 0.95,
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
                semaphore.failfast()
            except Exception:
                pass
