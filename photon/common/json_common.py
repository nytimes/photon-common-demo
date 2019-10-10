import logging
import pathlib

import json
from typing import Any
from datetime import datetime, timedelta

from photon.common.tuuid_common import TUUIDCommon
from photon.common.config_context_common import ConfigContextCommon


class JSONCommon(object):
    """
    Helper class for working with JSON.

    """

    def __init__(self, config: ConfigContextCommon) -> None:
        """
        Args:
            config: A config object.
        """
        logname = pathlib.Path(__file__).stem
        self._logger = logging.getLogger(f"{config.PACKAGE_NAME}.{logname}")
        self._logger.debug("")
        self._tuuid = TUUIDCommon(config)

    def jsonsafe(self, val: Any) -> Any:
        """
        Create a safe version of the input value.

        Args:
            val: A value of any type.

        Returns:
            The value as a type suitable for JSON conversion or database
            insert.
        """
        newval: Any

        if isinstance(val, bool):
            newval = "true" if val else "false"
        elif isinstance(val, (str, int, float)) or val is None:
            newval = val
        elif isinstance(val, datetime):
            newval = self._tuuid.get_tza_utcdt_isoz(val)
        elif isinstance(val, timedelta):
            newval = val.total_seconds()
        elif isinstance(val, bytes):
            newval = val.decode("ascii")  # e.g. for hash
        elif isinstance(val, dict):
            newval = {k: self.jsonsafe(v) for k, v in val.items()}
        elif isinstance(val, list):
            newval = [self.jsonsafe(v) for v in val]
        elif isinstance(val, tuple):
            newval = tuple(map(self.jsonsafe, val))
        else:  # if all else fails... (e.g. TimeUUID)
            newval = str(val)

        return newval

    def jsondumps(self, val: Any) -> str:
        """
        Invoke json.dumps on the 'jsonsafe'd input value w standard options.

        Args:
            val: A value of any type.

        Returns:
            A JSON string.
        """
        return json.dumps(
            self.jsonsafe(val), ensure_ascii=False, indent=2, sort_keys=True
        )
