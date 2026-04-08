"""
Swift Reader - Message reader.

Corresponds to SwiftReader.java on the Java side.
"""

import threading
from typing import Optional, Tuple

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftReader:
    """
    Read messages from a Swift topic.

    All methods are thread-safe (internally locked).
    Default timeout unit is microseconds (consistent with Java side):
      - read default timeout: 3_000_000 us = 3s
    """

    DEFAULT_READ_TIMEOUT_US = 3 * 1000 * 1000  # 3s

    def __init__(self, api: SwiftClientApi, reader_ptr: int):
        self._api = api
        self._reader_ptr = reader_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Read Messages                                                       #
    # ------------------------------------------------------------------ #

    def read(self, timeout_us: int = DEFAULT_READ_TIMEOUT_US):
        """
        Read a single message, returns a parsed Message protobuf object.

        :param timeout_us: Timeout in microseconds
        :return: (timestamp_us, Message) tuple
        :raises SwiftException: Raised on read failure
        """
        with self._lock:
            self._check_open()
            ec, timestamp, data = self._api.read_message(self._reader_ptr, timeout_us)
            self._raise_if_error(ec)
            return timestamp, self._parse_message(data)

    def reads(self, timeout_us: int = DEFAULT_READ_TIMEOUT_US):
        """
        Read messages in batch, returns a parsed Messages protobuf object.

        :param timeout_us: Timeout in microseconds
        :return: (timestamp_us, Messages) tuple
        :raises SwiftException: Raised on read failure
        """
        with self._lock:
            self._check_open()
            ec, timestamp, data = self._api.read_messages(self._reader_ptr, timeout_us)
            self._raise_if_error(ec)
            return timestamp, self._parse_messages(data)

    # ------------------------------------------------------------------ #
    #  Seek Operations                                                     #
    # ------------------------------------------------------------------ #

    def seek_by_timestamp(self, timestamp_us: int, force: bool = False):
        """
        Seek read position by timestamp.

        :param timestamp_us: Target timestamp in microseconds
        :param force:        Whether to force seek (even if timestamp is out of current range)
        :raises SwiftException: Raised on operation failure
        """
        with self._lock:
            self._check_open()
            ec = self._api.seek_by_timestamp(self._reader_ptr, timestamp_us, force)
            self._raise_if_error(ec)

    def seek_by_message_id(self, msg_id: int):
        """
        Seek read position by message ID.

        :param msg_id: Target message ID
        :raises SwiftException: Raised on operation failure
        """
        with self._lock:
            self._check_open()
            ec = self._api.seek_by_message_id(self._reader_ptr, msg_id)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  Other Operations                                                    #
    # ------------------------------------------------------------------ #

    def set_timestamp_limit(self, time_limit_us: int) -> int:
        """
        Set the timestamp upper limit for reading.

        :param time_limit_us: Timestamp upper limit in microseconds
        :return: Actual effective accept_timestamp in microseconds
        :raises SwiftException: Raised when Reader is closed
        """
        with self._lock:
            self._check_open()
            return self._api.set_timestamp_limit(self._reader_ptr, time_limit_us)

    def get_partition_status(self) -> Tuple[int, int, int]:
        """
        Get current partition status.

        :return: (refresh_time_us, max_message_id, max_message_timestamp_us)
        :raises SwiftException: Raised when Reader is closed
        """
        with self._lock:
            self._check_open()
            return self._api.get_partition_status(self._reader_ptr)

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def is_closed(self) -> bool:
        with self._lock:
            return self._reader_ptr == 0

    def close(self):
        with self._lock:
            if self._reader_ptr == 0:
                return
            self._api.delete_swift_reader(self._reader_ptr)
            self._reader_ptr = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    # ------------------------------------------------------------------ #
    #  Internal Utilities                                                  #
    # ------------------------------------------------------------------ #

    def _check_open(self):
        if self._reader_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "Reader is closed")

    @staticmethod
    def _raise_if_error(ec_int: int):
        ec = ErrorCode.from_int(ec_int)
        if ec != ErrorCode.ERROR_NONE:
            raise SwiftException(ec)

    @staticmethod
    def _parse_message(data: bytes):
        try:
            from .proto.SwiftMessage_pb2 import Message
            msg = Message()
            msg.ParseFromString(data)
            return msg
        except ImportError:
            # Proto files not generated, return raw bytes
            return data

    @staticmethod
    def _parse_messages(data: bytes):
        try:
            from .proto.SwiftMessage_pb2 import Messages
            msgs = Messages()
            msgs.ParseFromString(data)
            return msgs
        except ImportError:
            return data
