import time
import uuid
from typing import Any, Callable, NamedTuple, Optional

from redis import Redis
from redis.exceptions import LockError


class ParamsNT(NamedTuple):
    capacity: int
    timeoutms: int
    sleepms: int


ParamsNT.capacity.__doc__ = "bool (field 0): The workload capacity of the Semaphore"
ParamsNT.timeoutms.__doc__ = "bool (field 1): The maximum inactivity in milliseconds."
ParamsNT.sleepms.__doc__ = "int (field 2): The spin interval in milliseconds"


class Semaphore:
    """
    A distributed Semaphore.

    Using Redis allows the Semaphore to be shared across all threads,
    processes and machines that connect to the same Redis instance.
    """

    lua_acquire: Callable = None  # type: ignore
    lua_release: Callable = None  # type: ignore

    # KEYS[1] - semaphore name
    # ARGV[1] - semaphore instance id
    # ARGV[2] - requested value
    # ARGV[3] - max value
    # ARGV[4] - timeout
    # return the acquired value otherwise 0 if cannot yet acquire
    LUA_ACQUIRE_SCRIPT = """
        -- Note: before this is called an entry must exist in the queue
        --       this is handled by the class but for debugging can be done manually
        local semaphore_name = KEYS[1]
        local sem_iid = ARGV[1]
        local requested_value_str = ARGV[2]

        local set_name  = semaphore_name .. ".set"
        local queue_name = semaphore_name .. ".queue"
        local params_name = semaphore_name .. ".params"

        local capacity_str = redis.call("hget", params_name, "capacity")
        local timeoutms_str = redis.call("hget", params_name, "timeoutms")
        local sleepms_str = redis.call("hget", params_name, "sleepms")

        if not (capacity_str and timeoutms_str and sleepms_str) then
            return -3 -- oops! params have not been set - raise an exception
        end

        local capacity = tonumber(capacity_str)
        local timeoutms = tonumber(timeoutms_str)
        local sleepms = tonumber(sleepms_str)

        -- get the 1st & 2nd queue entries
        local queue_entries = redis.call("lrange", queue_name, 0, 1)
        local head_sem_iid = queue_entries[1]  -- the 1st is the head entry
        local next_sem_iid = queue_entries[2]  -- the 2nd is the next entry
        local head_sem_name_iid = semaphore_name .. ".iid:" .. head_sem_iid

        redis.call("pexpire", queue_name, timeoutms)  -- extend the life of the queue
        redis.call("pexpire", set_name, timeoutms)  -- and the set, if it exists

        -- part 1: is the head entry "dead"? are we at the head of the acquire queue?
        local head_exists = redis.call("exists", head_sem_name_iid)  -- check expiration

        if head_exists == 0 then  -- oops the head entry has expired - app died?
            redis.call("lpop", queue_name)  -- bye bye

            if next_sem_iid then  -- if there actually IS an entry set it up
                local next_sem_name_iid = semaphore_name .. ".iid:" .. next_sem_iid
                redis.call("set", next_sem_name_iid, 0, "px", sleepms * 10)
            end

            if head_sem_iid == sem_iid then  -- oops! The expired head is us...
                return -2  -- ...raise an exception
            end
        end

        -- head entry must still exist...
        if head_sem_iid ~= sem_iid then   -- but we are not yet at the head of the queue
            redis.call("pexpire", head_sem_name_iid, sleepms * 10)
            return -1  -- ...go around again
        end

        -- ...we ARE at the head of the queue

        -- part 2: is there enough total value remaining to acquire our requested value?
        local rem_sem_iids = {}
        local acquired_value_total = 0  -- TODO: consider caching the total
        for index, iid in next, redis.call("smembers", set_name) do
            local sem_name_iid = semaphore_name .. ".iid:" .. iid
            local acquired_value_sem_iid = redis.call("get", sem_name_iid)

            if acquired_value_sem_iid then  -- accumulate the acquired_value
                acquired_value_total = acquired_value_total + acquired_value_sem_iid
            else  -- oops that iid has expired
                table.insert(rem_sem_iids, iid)
            end
        end

        for index, iid in next, rem_sem_iids do  -- remove expired iids from the set
            redis.call("srem", set_name, iid)
        end

        local requested_value = tonumber(requested_value_str)
        local capacity = tonumber(capacity_str)

        if requested_value > (capacity - acquired_value_total) then
            -- not enough value yet so app must call us again soon
            redis.call("pexpire", head_sem_name_iid, sleepms * 10)
            return 0  -- the requested value is not yet available so go around again
        end

        -- part 3: Acquire the requested value and update data structures
        redis.call("set", head_sem_name_iid, requested_value, "px", timeoutms)
        redis.call("sadd", set_name, sem_iid)
        redis.call("pexpire", set_name, timeoutms)  -- may have been empty previously
        redis.call("lpop", queue_name)

        if next_sem_iid then  -- if there actually IS an entry set it up
            local next_sem_name_iid = semaphore_name .. ".iid:" .. next_sem_iid
            redis.call("set", next_sem_name_iid, 0, "px", sleepms * 10)
        end

        return requested_value  -- got it!
    """

    # KEYS[1] - semaphore name
    # ARGV[1] - instance id
    # return released value.
    LUA_RELEASE_SCRIPT = """
        local semaphore_name = KEYS[1]
        local sem_iid = ARGV[1]

        local set_name  = semaphore_name .. ".set"
        local sem_name_iid = semaphore_name .. ".iid:" .. sem_iid
        local released_value_str = redis.call("get", sem_name_iid)
        local released_value = tonumber(released_value_str)
        redis.call("del", sem_name_iid)  -- delete the key/value entry for this iid
        redis.call("srem", set_name, sem_iid)  -- remove the iid from the semaphore set

        if released_value then
            return released_value
        else
            return 0
        end
    """

    def __init__(self, redis: Redis, name: str = "default") -> None:
        """
        Create a new Semaphore instance named ``name`` using the Redis client.

        The client may call get_params() to verify that capacity, timeout, and sleep
        are set appropriately. set_params() may be called to reset them.

        Args:
            redis: The redis client

            name: The name to use for the Semaphore.

        """

        paramsnt = Semaphore.get_params(redis, name)

        if not paramsnt:
            raise ValueError(
                f"params are not yet initialized in Redis for Semaphore: '{name}'"
            )

        self._redis = redis
        self._name = name
        self._capacity = paramsnt.capacity
        self._sleepms = paramsnt.sleepms
        self._queue_name = f"{name}.queue"
        self._sem_iid = str(uuid.uuid1())
        self._acquired_value = 0
        self._register_scripts()

    @staticmethod
    def get_params(redis: Redis, name: str = "default") -> Optional[ParamsNT]:
        """
        Get the params for a Semaphore named ``name``.

        Args:
            redis: A Redis instance

            name: The name of the Semaphore.

        Returns:
            A ParamsNT imnstance or None if not found.

        """
        params_name = f"{name}.params"
        paramsbd = redis.hgetall(params_name)  # type: ignore

        if paramsbd:
            paramsd = {k.decode(): int(v) for k, v in paramsbd.items()}
            return ParamsNT(**paramsd)
        else:
            return None

    @staticmethod
    def set_params(
        redis: Redis,
        name: str = "default",
        capacity: int = 100,
        timeoutms: int = 60 * 60 * 1000,
        sleepms: int = 100,
    ) -> None:
        """
        Set the params for a Semaphore named ``name``.

        Args:
            redis: A Redis instance

            name: The name of the Semaphore.

            capacity: The workload capacity of the Semaphore

            timeoutms: The maximum inactivity for the Semaphore in milliseconds.

            sleepms: The amount of time in milliseconds to sleep per loop iteration
            when acquire() is not possible because the requested value is
            not yet available.

        """
        if capacity < 1:
            raise ValueError("'capacity' must be greater than 0")

        if timeoutms < 1:
            raise ValueError("'timeoutms' must be greater than 0")

        if sleepms >= timeoutms:
            raise ValueError("'sleep' must be less than 'timeout'")

        params_name = f"{name}.params"
        paramsd = {"capacity": capacity, "sleepms": sleepms, "timeoutms": timeoutms}
        redis.hmset(params_name, paramsd)  # type: ignore  # no expiration

    def _register_scripts(self) -> None:
        """
        Register the lua scripts once for the class (including all Semaphore instances).

        """
        cls = self.__class__

        if cls.lua_acquire is None:
            cls.lua_acquire = self._redis.register_script(self.LUA_ACQUIRE_SCRIPT)

        if cls.lua_release is None:
            cls.lua_release = self._redis.register_script(self.LUA_RELEASE_SCRIPT)

    def __enter__(self, **kwargs: Any) -> object:
        if self.acquire(**kwargs):
            return self

        raise LockError("Unable to acquire Semaphore")

    def __exit__(self, *args: Any) -> None:
        self.release()

    def acquire(self, requested_value: int = 1) -> int:
        """
        Use Redis to acquire a shared, distributed semaphore for this instanec_id
        and return when the semaphore is acquired with the requested value.

        Returns:
            The acquired value.

        Raises:
            ValueError.
            LockError.

        """
        if requested_value < 1 or requested_value > self._capacity:
            raise ValueError(
                f"'requested_value' must be between 1 and "
                f"'capacity': {self._capacity}"
            )

        if self._acquired_value:
            raise LockError("Cannot acquire an already acquired Semaphore instance")

        sem_name_iid = f"{self._name}.iid:{self._sem_iid}"

        (  # initialize by queuing the iid and setting the key - queue may be empty
            self._redis.pipeline()  # type: ignore
            .rpush(self._queue_name, self._sem_iid)
            .set(sem_name_iid, 0, px=self._sleepms * 100)
            .execute()
        )

        while True:  # spin until acquired
            self._acquired_value = self.lua_acquire(
                keys=[self._name],
                args=[self._sem_iid, requested_value],
                client=self._redis,
            )

            if self._acquired_value > 0:
                return self._acquired_value

            if self._acquired_value == -2:
                raise LockError("Semaphore queue entry timed out")

            if self._acquired_value == -3:
                raise LockError("Semaphore params not found in Redis")

            time.sleep(self._sleepms / 1000)  # sleep then spin

    def release(self) -> int:
        """
        Release an acquired lock.

        Returns:
            The released value.

        Raises:
            LockError.

        """
        if not self._acquired_value:
            raise LockError("Cannot release a Semaphore that is not acquired")

        released_value = self.lua_release(
            keys=[self._name], args=[self._sem_iid], client=self._redis
        )

        self._acquired_value = 0

        return int(released_value)
