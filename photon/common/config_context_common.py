from pathlib import Path
from logging import Logger


class ConfigContextCommon:
    """
    Set up Config & Context common type definitions.

    """

    ROOTP: Path
    DEPLOYP: Path
    PACKAGE_NAME: str
    PACKAGE_FAMILY: str
    PACKAGE_HIERARCHY: str
    PACKAGE_NICKNAME: str
    PACKAGE_TYPE: str
    PACKAGE_FULLNAME: str
    LOG_LEVEL: int
    LOG_FORMAT: str

    _logger: Logger
    util_cmd: str
