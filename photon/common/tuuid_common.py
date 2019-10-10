import logging
import pathlib
from datetime import datetime, timezone
from typing import cast, Optional

from time_uuid import TimeUUID

from photon.common.config_context_common import ConfigContextCommon


class TUUIDCommon:
    """
    Class for working with TimeUUIDs and datetimes in a standard way.

    """

    def __init__(self, config: ConfigContextCommon) -> None:
        """
        Args:
            config: A config object.
        """
        logname = pathlib.Path(__file__).stem
        self._logger = logging.getLogger(f"{config.PACKAGE_NAME}.{logname}")
        self._logger.debug("")

    def get_tuuid(
        self, hex: Optional[str] = None, timestamp: Optional[float] = None
    ) -> TimeUUID:
        """
        Get a TimeUUID.

        Args:
            hex: The optional TimeUUID as hex.
            timestamp: The optional timestamp to use in the TimeUUID
        Returns:
            The TimeUUID.
        """
        if hex:
            tuuid = TimeUUID(hex=hex)
        elif timestamp:
            tuuid = TimeUUID.with_timestamp(timestamp)
        else:
            tuuid = TimeUUID.with_utcnow()

        return tuuid

    def extract_datetime(self, tuuid: TimeUUID) -> datetime:
        """
        Extract the datetime from a TimeUUID.

        Args:
            tuuid: A TimeUUID.
        Returns:
            The timezone-aware UTC datetime.
        """
        dt = tuuid.get_datetime().replace(tzinfo=timezone.utc)
        return cast(datetime, dt)

    def get_tza_utcdt(self, utcdt: Optional[datetime] = None) -> datetime:
        """
        Return a timezone-aware UTC datetime based on the input or utcnow().

        WARNING: if provided, utcdt should NOT be local!

        Args:
            utcdt: The optional UTC datetime
        Returns:
            The timezone-aware datetime.
        """
        if not utcdt:
            utcdt = datetime.utcnow()

        return utcdt.replace(tzinfo=timezone.utc)

    def get_tza_utcdt_iso(self, utcdt: Optional[datetime] = None) -> str:
        """
        Return a timezone-aware UTC datetime in default ISO str format.

        Args:
            utcdt: The optional UTC datetime
        Returns:
            The timezone-aware UTC default ISO str.
        """
        tza_utcdt = self.get_tza_utcdt(utcdt=utcdt)
        return tza_utcdt.isoformat()

    def get_tza_utcdt_isoz(self, utcdt: Optional[datetime] = None) -> str:
        """
        Return a timezone-aware UTC datetime in ISO fmt using the 'Z' option.

        Args:
            utcdt: The optional UTC datetime
        Returns:
            The timezone-aware UTC ISO str with 'Z'.
        """
        tza_utcdt_iso = self.get_tza_utcdt_iso(utcdt=utcdt)
        return tza_utcdt_iso.replace("+00:00", "Z")
