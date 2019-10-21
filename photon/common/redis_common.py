import os
import logging
from pathlib import Path

from redis import Redis

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
