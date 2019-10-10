import pathlib
import logging
from logging import Logger
from logging.handlers import RotatingFileHandler

from photon.common.config_context_common import ConfigContextCommon


class LoggingCommon:
    """
    Helper class for logging.

    """

    def __init__(self, config: ConfigContextCommon) -> None:
        """
        Args:
            config: A config object.
        """
        self._root_logger = logging.getLogger()
        self._root_logger.setLevel(config.LOG_LEVEL)
        self._formatter = logging.Formatter(fmt=config.LOG_FORMAT, style="{")
        self._logname = config.PACKAGE_NAME

        self._logdirp = (
            pathlib.Path("/usr/local/var") / config.PACKAGE_FAMILY / config.PACKAGE_NAME
        )

        self._file_handler()
        self._console_handler()

    def _file_handler(self) -> None:
        self._logdirp.mkdir(parents=True, exist_ok=True)  # mkdir for mac
        logp = self._logdirp / f"{self._logname}.log"

        file_handler = RotatingFileHandler(
            str(logp), maxBytes=128 * 1024 * 1024, backupCount=3
        )

        file_handler.setFormatter(self._formatter)
        self._root_logger.addHandler(file_handler)  # log to /usr/local/var

    def _console_handler(self) -> None:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self._formatter)
        self._root_logger.addHandler(console_handler)  # log to stdout

    def get_root_logger(self) -> Logger:
        """
        Return the root logger.

        Returns:
            The root logger.
        """
        return self._root_logger
