"""
Swift Writer - 消息写入器。

对应 Java 端的 SwiftWriter.java。
"""

import threading

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftWriter:
    """
    向 Swift topic 写入消息。

    所有方法均线程安全（内部加锁）。
    默认超时单位为微秒（与 Java 端一致）：
      - wait_finished 默认超时：30_000_000 us = 30s
      - wait_sent     默认超时：3_000_000  us = 3s
    """

    DEFAULT_WAIT_FINISHED_TIMEOUT_US = 30 * 1000 * 1000  # 30s
    DEFAULT_WAIT_SENT_TIMEOUT_US = 3 * 1000 * 1000       # 3s

    def __init__(self, api: SwiftClientApi, writer_ptr: int):
        self._api = api
        self._writer_ptr = writer_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  写入消息                                                             #
    # ------------------------------------------------------------------ #

    def write(self, msg):
        """
        写入单条消息。

        :param msg: WriteMessageInfo protobuf 对象，或已序列化的 bytes
        :raises SwiftException: 写入失败时抛出
        """
        with self._lock:
            self._check_open()
            data = msg if isinstance(msg, (bytes, bytearray)) else msg.SerializeToString()
            ec = self._api.write_message(self._writer_ptr, bytes(data))
            self._raise_if_error(ec)

    def write_batch(self, msg_vec, wait_sent: bool = False):
        """
        批量写入消息。

        :param msg_vec:   WriteMessageInfoVec protobuf 对象，或已序列化的 bytes
        :param wait_sent: 是否等待消息发送完成
        :raises SwiftException: 写入失败时抛出
        """
        with self._lock:
            self._check_open()
            data = msg_vec if isinstance(msg_vec, (bytes, bytearray)) else msg_vec.SerializeToString()
            ec = self._api.write_messages(self._writer_ptr, bytes(data), wait_sent)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  等待操作                                                             #
    # ------------------------------------------------------------------ #

    def wait_finished(self, timeout_us: int = DEFAULT_WAIT_FINISHED_TIMEOUT_US):
        """
        等待所有消息写入并持久化完成。

        :param timeout_us: 超时时间（微秒），默认 30s
        :raises SwiftException: 超时或失败时抛出
        """
        with self._lock:
            self._check_open()
            ec = self._api.wait_finished(self._writer_ptr, timeout_us)
            self._raise_if_error(ec)

    def wait_sent(self, timeout_us: int = DEFAULT_WAIT_SENT_TIMEOUT_US):
        """
        等待所有消息发送到 Broker（不要求持久化）。

        :param timeout_us: 超时时间（微秒），默认 3s
        :raises SwiftException: 超时或失败时抛出
        """
        with self._lock:
            self._check_open()
            ec = self._api.wait_sent(self._writer_ptr, timeout_us)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  其他操作                                                             #
    # ------------------------------------------------------------------ #

    def get_committed_checkpoint_id(self) -> int:
        """
        获取已提交的 checkpoint ID。

        :return: checkpoint ID（-1 表示尚无提交）
        :raises SwiftException: Writer 已关闭时抛出
        """
        with self._lock:
            self._check_open()
            return self._api.get_committed_checkpoint_id(self._writer_ptr)

    # ------------------------------------------------------------------ #
    #  生命周期                                                             #
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
    #  内部工具                                                             #
    # ------------------------------------------------------------------ #

    def _check_open(self):
        if self._writer_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "Writer is closed")

    @staticmethod
    def _raise_if_error(ec_int: int):
        ec = ErrorCode.from_int(ec_int)
        if ec != ErrorCode.ERROR_NONE:
            raise SwiftException(ec)
