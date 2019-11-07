import time
import uuid
from typing import Any, Callable, NamedTuple, Optional, Tuple

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
    # return the acquired value otherwise 0 if cannot yet acquire
    LUA_ACQUIRE_SCRIPT = """
        -- Note: before this is called an entry must exist in the queue
        --       this is handled by the class but for debugging can be done manually
        local semaphore_name = KEYS[1]
        local iid = ARGV[1]
        local requested_value_str = ARGV[2]

        local set_name  = semaphore_name .. ".set"
        local zset_name = semaphore_name .. ".zset"
        local params_name = semaphore_name .. ".params"
        local value_key = semaphore_name .. ".acquired_value"
        local requested_value = tonumber(requested_value_str)

        local capacity_str = redis.call("hget", params_name, "capacity")
        local timeoutms_str = redis.call("hget", params_name, "timeoutms")
        local sleepms_str = redis.call("hget", params_name, "sleepms")

        if not (capacity_str and timeoutms_str and sleepms_str) then
            return -3, 0 -- oops! params have not been set - raise an exception
        end

        local capacity = tonumber(capacity_str)
        local timeoutms = tonumber(timeoutms_str)
        local sleepms = tonumber(sleepms_str)

        local function get_zset_entry(semaphore_name, zset_name, sleepms)
            local head_iid = ""
            local zset_exists = redis.call("exists", zset_name)

            if zset_exists == 1 then
                local iids = redis.call("zrange", zset_name, 0, 0)
                head_iid = iids[1]
                local head_name_iid = semaphore_name .. ".iid:" .. head_iid
                local head_exists = redis.call("exists", head_name_iid)

                if head_exists == 0 then  -- oops the head entry has expired - app died?
                    redis.call("zrem", zset_name, head_iid)  -- bye bye
                    head_iid = ""
                end
            end

            return head_iid
        end

        local function calculate_acquired_value_total(set_name)
            local rem_iids = {}
            local acquired_value_total = 0
            for index, iid in next, redis.call("smembers", set_name) do
                local name_iid = semaphore_name .. ".iid:" .. iid
                local acquired_value_iid = redis.call("get", name_iid)

                if acquired_value_iid then  -- accumulate the acquired_value
                    acquired_value_total = acquired_value_total + acquired_value_iid
                else  -- oops that iid has expired
                    table.insert(rem_iids, iid)
                end
            end

            for index, iid in next, rem_iids do  -- remove expired iids from the set
                redis.call("srem", set_name, iid)
            end

            return acquired_value_total
        end

        local function get_acquired_value_total(set_name, value_key, sleepms)
            local acquired_value_total = redis.call("get", value_key)

            if not acquired_value_total then
                acquired_value_total = calculate_acquired_value_total(set_name)
                redis.call("set", value_key, acquired_value_total, "px", sleepms * 10)
            end

            return acquired_value_total
        end

        local function get_acquired_value(
            set_name, name_iid, requested_value, capacity, sleepms
        )
            local acquired_value_total = get_acquired_value_total(
                set_name, value_key, sleepms
            )

            local acquired_value =  0
            if requested_value > (capacity - acquired_value_total) then
                acquired_value = 0  -- not yet available - go around again
            else
                acquired_value = requested_value  -- got it!
            end

            return acquired_value
        end

        -- MAIN
        redis.call("pexpire", zset_name, timeoutms)  -- extend the sorted set
        redis.call("pexpire", set_name, timeoutms)  -- and the set, if they exist

        local score = redis.call("zscore", zset_name, iid)
        local name_iid = semaphore_name .. ".iid:" .. iid
        local name_iid_exists = redis.call("exists", name_iid)

        -- the existential question
        if name_iid_exists == 0 or not score then    -- oops - app died?
            redis.call("zrem", zset_name, iid)  -- bye bye
            redis.call("del", name_iid)  -- cleanup if necessary
            return {-2, 0}  -- raise an exception
        end

        -- we exist!
        -- are we at the head of the acquire zset (lowest score)?
        local head_iid = get_zset_entry(semaphore_name, zset_name, sleepms)

        if iid ~= head_iid then  -- not at the head or head expired
            -- exponentially decay the score of this entry to avoid starvation
            score = score * 0.9999
            redis.call("zadd", zset_name, score, iid)  -- reset
            redis.call("pexpire", name_iid, sleepms * 100)  -- extend
            return {-1, score}  -- go around again
        end

        -- we are at the head
        -- is there enough total value remaining to acquire our requested value?
        local acquired_value = get_acquired_value(
            set_name, name_iid, requested_value, capacity, sleepms
        )

        if acquired_value > 0 then  -- update data structures
            redis.call("set", name_iid, acquired_value, "px", timeoutms)
            redis.call("sadd", set_name, iid)  -- create if necessary
            redis.call("pexpire", set_name, timeoutms)  -- may have been empty
            redis.call("zrem", zset_name, iid)

            if redis.call("exists", value_key) == 1 then
                redis.call("incrby", value_key, acquired_value)
            end
        end

        return {acquired_value, score}
    """

    # KEYS[1] - semaphore name
    # ARGV[1] - instance id
    # return released value.
    LUA_RELEASE_SCRIPT = """
        local semaphore_name = KEYS[1]
        local iid = ARGV[1]

        local set_name  = semaphore_name .. ".set"
        local name_iid = semaphore_name .. ".iid:" .. iid
        local value_key = semaphore_name .. ".acquired_value"

        local acquired_value_str = redis.call("get", name_iid)
        local acquired_value = tonumber(acquired_value_str)

        local released_value = 0
        if acquired_value then
            local acquired_value_total_str = redis.call("get", value_key)
            local acquired_value_total = tonumber(acquired_value_total_str)

            if acquired_value_total and acquired_value_total >= acquired_value then
                redis.call("incrby", value_key, -acquired_value)
            end

            released_value = acquired_value
        end

        redis.call("del", name_iid)  -- delete the key/value entry for this iid
        redis.call("srem", set_name, iid)  -- remove the iid from the semaphore set

        return released_value
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
        self._zset_name = f"{name}.zset"
        self._iid = str(uuid.uuid1())
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

    def acquire(self, requested_value: int = 1) -> Tuple[int, float]:
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

        name_iid = f"{self._name}.iid:{self._iid}"

        (  # initialize by adding the iid and setting the key - zset may be empty
            self._redis.pipeline()  # type: ignore
            .zadd(self._zset_name, {self._iid: requested_value})
            .set(name_iid, 0, px=self._sleepms * 100)
            .execute()
        )

        while True:  # spin until acquired
            self._acquired_value, score = self.lua_acquire(
                keys=[self._name],
                args=[self._iid, requested_value],
                client=self._redis,
            )

            if self._acquired_value > 0:
                return self._acquired_value, float(score)

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
            keys=[self._name], args=[self._iid], client=self._redis
        )

        self._acquired_value = 0

        return int(released_value)
