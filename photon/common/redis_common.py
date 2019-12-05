import os
import time
import pickle
import logging
from pathlib import Path
from typing import Any, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Union

import pandas as pd
from redis import Redis
from time_uuid import TimeUUID
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

    def _add_value(
        self,
        key: str,
        timestampms: Union[int, str],
        value: int,
        labeld: Mapping[str, Any],
    ) -> int:
        i = 0
        while True:
            try:
                timestampms_return = self._rts.add(
                    key,
                    timestampms,
                    value,
                    retention_msecs=self._retentionms,
                    labels=labeld,
                )

                return timestampms_return  # type: ignore
            except ResponseError:  # whoops - too quick, delay a bit
                if i < 5:
                    i += 1
                    time.sleep(0.001)
                else:
                    raise

    def add_value(self, value: int = 0, name: str = "", thread: int = 0) -> None:
        key = f"ts:{name or self._name}.T:{thread or self._thread}"
        labeld = {"ts": name or self._name, "T": thread or self._thread}

        if self._transitionms and value != self._previous_value:
            self._add_value(key, "*", self._previous_value, labeld)
            time.sleep(self._transitionms / 1000)
            self._add_value(key, "*", value, labeld)
            self._previous_value = value
        else:
            self._add_value(key, "*", value, labeld)

    def add_slot_values(self, values: Sequence[int] = [], name: str = "") -> int:
        if not values:
            return 0

        keybase = f"ts:{name or self._name}.S:"
        labeld = {"ts": name or self._name, "S": 0}
        timestampms = self._add_value(f"{keybase}0", "*", values[0], labeld)

        for i, value in enumerate(values[1:]):
            labeld["S"] = i + 1
            self._add_value(f"{keybase}{i + 1}", timestampms, value, labeld)

        return timestampms

    def get_keytuples_by_names(
        self, names: Sequence[str] = [], types: Sequence[str] = ["T"]
    ) -> List[Tuple[str, int]]:
        namelist = (",").join(names or [self._name])
        filters = [f"ts=({namelist})"]
        keys = self._rts.queryindex(filters)

        keytuples = []
        for key in keys:
            eles = key.split(".")
            ele0 = eles[0].split(":")  # ("ts", <name>)
            ele1 = eles[1].split(":")  # ("T" or "S", <int>)
            keytuple = (ele0[1], int(ele1[1]))  # (<name>, <int>)

            if ele1[0] in types:
                keytuples.append(keytuple)

        return keytuples

    def get_threads_by_name(self, name: str = "") -> Tuple[int, ...]:
        keytuples = self.get_keytuples_by_names([name or self._name], types=["T"])
        names, threads = zip(*keytuples)

        return threads  # discard names

    def get_slots_by_name(self, name: str = "") -> Tuple[int, ...]:
        keytuples = self.get_keytuples_by_names([name or self._name], types=["S"])
        names, slots = zip(*keytuples)

        return slots  # discard names

    def get_dataframe(
        self, name: str = "", thread: int = 0, timestampms: int = 0, type: str = "T"
    ) -> pd.DataFrame:
        key = f"ts:{name or self._name}.{type}:{thread or self._thread}"
        datapointts = self._rts.range(key, timestampms, -1)

        if not datapointts:
            return pd.DataFrame()

        dts, values = zip(*datapointts)
        datapointdf = pd.DataFrame({"dt": dts, key: [float(v) for v in values]})
        datapointdf["dt"] = pd.to_datetime(datapointdf.dt, unit="ms")
        return datapointdf.set_index("dt")


class RedisWorkNT(NamedTuple):
    """
    Work info submitted to Incoming via Redis.

    """

    workload: int
    workid: TimeUUID
    runtuuid: TimeUUID


RedisWorkNT.workload.__doc__ = "int (field 0): The work resources required."
RedisWorkNT.workid.__doc__ = "TimeUUID (field 1): The tuuid for the work to be done."
RedisWorkNT.workload.__doc__ = "TimeUUID (field 2): The tuuid of the submitting run."


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

    def publish_redis_worknt(
        self, redis_worknt: RedisWorkNT, name: str = "default"
    ) -> None:
        """
        Queue a RedisWorkNT.

        The RedisWorkNT NamedTuple is pickled.

        Args:
            The RedisWorkNT.

        Raises:
            RunTimeError if no subscriber.
        """
        awf_key = f"{self._app_key}.workflow:{name}"
        redis_worknt_pkl = pickle.dumps(redis_worknt)

        if not self._redis.publish(awf_key, redis_worknt_pkl):
            raise RuntimeError(f"No subscriber to '{awf_key}'")

    def snapshot_workq(
        self, items: Sequence[Any], name: str = "A", timeoutms: int = 10 * 60 * 1000
    ) -> None:
        tssnap_key = f"ts:{name}.snap:workq"
        item_pkls = [pickle.dumps(item) for item in items]
        item_pkls.reverse()

        if items:
            (
                self._redis.pipeline()  # type: ignore
                .delete(tssnap_key)
                .lpush(tssnap_key, *item_pkls)
                .pexpire(tssnap_key, timeoutms)
                .execute()
            )
        else:
            self._redis.delete(tssnap_key)  # type: ignore

    def get_snapshot_workq(self, name: str = "A") -> List[Any]:
        tssnap_key = f"ts:{name}.snap:workq"
        item_pkls = self._redis.lrange(tssnap_key, 0, -1)  # type: ignore
        items = [pickle.loads(item_pkl) for item_pkl in item_pkls]

        return items

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
