import time
import uuid
from typing import Any, Callable

from redis import Redis
from redis.exceptions import LockError


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
        local max_value_str = ARGV[3]
        local timeout_str = ARGV[4]
        local sleep_str = ARGV[5]

        local timeout = tonumber(timeout_str)
        local sleep = tonumber(sleep_str)
        local queue_name = semaphore_name .. ".queue"
        -- get the 1st & 2nd entries
        local queue_entries = redis.call("lrange", queue_name, 0, 1)
        local head_sem_iid = queue_entries[1]  -- the 1st is the head entry
        local next_sem_iid = queue_entries[2]  -- the 2nd is the next entry
        local head_sem_name_iid = semaphore_name .. ".iid:" .. head_sem_iid

        redis.call("pexpire", queue_name, timeout)  -- extend the life of the queue

        -- part 1: is the head entry "dead"? are we at the head of the acquire queue?
        local exists = redis.call("exists", head_sem_name_iid)  -- check expiration

        if exists == 0 then  -- oops the head entry has expired - app died?
            redis.call("lpop", queue_name)  -- bye bye

            if next_sem_iid then  -- if there actually IS an entry set it up
                local next_sem_name_iid = semaphore_name .. ".iid:" .. next_sem_iid
                redis.call("set", next_sem_name_iid, 0, "px", sleep * 100)
            end

            if head_sem_iid == sem_iid then  -- oops! The expired head is us...
                return -2  -- ...so raise an exception
            end
        end

        -- so head entry must still exist...
        if head_sem_iid ~= sem_iid then   -- but we are not yet at the head of the queue
            redis.call("pexpire", head_sem_name_iid, sleep * 100)  -- extend expiration
            return -1  -- ...so go around again
        end

        -- ...and we ARE at the head of the queue

        -- part 2: is there enough total value remaining to acquire our requested value?
        local del_sem_iids = {}
        local acquired_value_total = 0  -- TODO: consider caching the total
        for index, iid in next, redis.call("smembers", semaphore_name) do
            local sem_name_iid = semaphore_name .. ".iid:" .. iid
            local acquired_value_sem_iid = redis.call("get", sem_name_iid)

            if acquired_value_sem_iid then  -- accumulate the acquired_value
                acquired_value_total = acquired_value_total + acquired_value_sem_iid
            else
                table.insert(del_sem_iids, iid)  -- oops that iid has expired
            end
        end

        for index, iid in next, del_sem_iids do  -- delete expired iids from the set
            redis.call("srem", semaphore_name, iid)
        end

        local requested_value = tonumber(requested_value_str)
        local max_value = tonumber(max_value_str)

        if requested_value > (max_value - acquired_value_total) then
            -- not enough value yet so app must call us again soon
            redis.call("pexpire", head_sem_name_iid, sleep * 100)  -- extend expiration
            return 0  -- the requested value is not yet available so go around again
        end

        -- part 3: Acquire the requested value and update data structures
        redis.call("set", head_sem_name_iid, requested_value, "px", timeout)
        redis.call("sadd", semaphore_name, sem_iid)
        redis.call("pexpire", semaphore_name, timeout)
        redis.call("lpop", queue_name)

        if next_sem_iid then  -- if there actually IS an entry set it up
            local next_sem_name_iid = semaphore_name .. ".iid:" .. next_sem_iid
            redis.call("set", next_sem_name_iid, 0, "px", sleep * 100)
        end

        return requested_value  -- got it!
    """

    # KEYS[1] - semaphore name
    # ARGV[1] - instance id
    # return released value.
    LUA_RELEASE_SCRIPT = """
        local semaphore_name = KEYS[1]
        local sem_iid = ARGV[1]

        local sem_name_iid = semaphore_name .. ".iid:" .. sem_iid
        local released_value_str = redis.call("get", sem_name_iid)
        local released_value = tonumber(released_value_str)
        redis.call("del", sem_name_iid)  -- delete the key/value entry for this iid
        redis.call("srem", semaphore_name, sem_iid)  -- remove the iid from the set

        if released_value then
            return released_value
        else
            return 0
        end
    """

    def __init__(
        self,
        redis: Redis,
        name: str,
        value: int = 100,
        timeout: int = 3600,
        sleep: float = 0.1,
    ) -> None:
        """
        Create a new Semaphore instance named ``name`` using the Redis client.

        Args:
            redis: The redis client

            name: the name to use for the RLock.

            timeout: The maximum inactivity for the Semaphore in seconds.

            sleep: The amount of time in seconds to sleep per loop iteration
            when acquire() is not possible because the requested value is
            not yet available.

        """
        self._redis = redis

        if timeout <= 0:
            raise ValueError("'timeout' must be greater than 0")

        if sleep > timeout:
            raise ValueError("'sleep' must be less than 'timeout'")

        self._sleep = sleep
        self._sleepms = int(sleep * 1000)
        self._timeoutms = timeout * 1000
        self._name = name
        self._queue_name = f"{name}.queue"
        self._max_value = value
        self._sem_iid = str(uuid.uuid1())
        self._acquired_value = 0
        self._register_scripts()
        # TODO: store the instance parameters in Redis as a hash associated w sem_iid

    def _register_scripts(self) -> None:
        """
        Register the lua scripts once for the class (including all RLock instances).

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
        if requested_value < 1 or requested_value > self._max_value:
            raise ValueError(
                f"'requested_value' must be between 1 and "
                f"'max_value': {self._max_value}"
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
                args=[
                    self._sem_iid,
                    requested_value,
                    self._max_value,
                    self._timeoutms,
                    self._sleepms,
                ],
                client=self._redis,
            )

            if self._acquired_value > 0:
                return self._acquired_value

            if self._acquired_value == -2:
                raise LockError("Semaphore queue entry timed out")

            # return values of 0 and -1 -> spin again after sleeping
            time.sleep(self._sleep)

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
