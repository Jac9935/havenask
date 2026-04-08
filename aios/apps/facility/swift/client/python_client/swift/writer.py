"""
Swift Writer - Message writer.

Corresponds to SwiftWriter.java on the Java side.
"""

import threading

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftWriter:
    """
    Write messages to a Swift topic.

    All methods are thread-safe (internally locked).
    Default timeout unit is microseconds (consistent with Java side):
      - wait_finished default timeout: 30_000_000 us = 30s
      - wait_sent     default timeout: 3_000_000  us = 3s
    """

    DEFAULT_WAIT_FINISHED_TIMEOUT_US = 30 * 1000 * 1000  # 30s
    DEFAULT_WAIT_SENT_TIMEOUT_US = 3 * 1000 * 1000       # 3s

    def __init__(self, api: SwiftClientApi, writer_ptr: int):
        self._api = api
        self._writer_ptr = writer_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Write Messages                                                      #
    # ------------------------------------------------------------------ #

    def write(self, msg):
        """
        Write a single message.

        :param msg: WriteMessageInfo protobuf object, or serialized bytes
        :raises SwiftException: Raised on write failure
        """
        with self._lock:
            self._check_open()
            data = msg if isinstance(msg, (bytes, bytearray)) else msg.SerializeToString()
            ec = self._api.write_message(self._writer_ptr, bytes(data))
            self._raise_if_error(ec)

    def write_batch(self, msg_vec, wait_sent: bool = False):
        """
        Write messages in batch.

        :param msg_vec:   WriteMessageInfoVec protobuf object, or serialized bytes
        :param wait_sent: Whether to wait for messages to be sent
        :raises SwiftException: Raised on write failure
        """
        with self._lock:
            self._check_open()
            data = msg_vec if isinstance(msg_vec, (bytes, bytearray)) else msg_vec.SerializeToString()
            ec = self._api.write_messages(self._writer_ptr, bytes(data), wait_sent)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  Wait Operations                                                     #
    # ------------------------------------------------------------------ #

    def wait_finished(self, timeout_us: int = DEFAULT_WAIT_FINISHED_TIMEOUT_US):
        """
        Wait for all messages to be written and persisted.

        :param timeout_us: Timeout in microseconds, default 30s
        :raises SwiftException: Raised on timeout or failure
        """
        with self._lock:
            self._check_open()
            ec = self._api.wait_finished(self._writer_ptr, timeout_us)
            self._raise_if_error(ec)

    def wait_sent(self, timeout_us: int = DEFAULT_WAIT_SENT_TIMEOUT_US):
        """
        Wait for all messages to be sent to the Broker (persistence not required).

        :param timeout_us: Timeout in microseconds, default 3s
        :raises SwiftException: Raised on timeout or failure
        """
        with self._lock:
            self._check_open()
            ec = self._api.wait_sent(self._writer_ptr, timeout_us)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  Other Operations                                                    #
    # ------------------------------------------------------------------ #

    def get_committed_checkpoint_id(self) -> int:
        """
        Get the committed checkpoint ID.

        :return: Checkpoint ID (-1 means no checkpoint committed yet)
        :raises SwiftException: Raised when Writer is closed
        """
        with self._lock:
            self._check_open()
            return self._api.get_committed_checkpoint_id(self._writer_ptr)

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def is_closed(self) -> bool:
        with self._lock:
            return self._writer_ptr == 0

    def close(self):
        with self._lock:
            if self._writer_ptr == 0:
                return
            self._api.delete_swift_writer(self._writer_ptr)
            self._writer_ptr = 0

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
        if self._writer_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "Writer is closed")

    @staticmethod
    def _raise_if_error(ec_int: int):
        ec = ErrorCode.from_int(ec_int)
        if ec != ErrorCode.ERROR_NONE:
            raise SwiftException(ec)
