"""
Swift Reader - 消息读取器。

对应 Java 端的 SwiftReader.java。
"""

import threading
from typing import Optional, Tuple

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftReader:
    """
    从 Swift topic 读取消息。

    所有方法均线程安全（内部加锁）。
    默认超时单位为微秒（与 Java 端一致）：
      - read 默认超时：3_000_000 us = 3s
    """

    DEFAULT_READ_TIMEOUT_US = 3 * 1000 * 1000  # 3s

    def __init__(self, api: SwiftClientApi, reader_ptr: int):
        self._api = api
        self._reader_ptr = reader_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  读取消息                                                             #
    # ------------------------------------------------------------------ #

    def read(self, timeout_us: int = DEFAULT_READ_TIMEOUT_US):
        """
        读取一条消息，返回解析后的 Message protobuf 对象。

        :param timeout_us: 超时时间（微秒）
        :return: (timestamp_us, Message) 元组
        :raises SwiftException: 读取失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, timestamp, data = self._api.read_message(self._reader_ptr, timeout_us)
            self._raise_if_error(ec)
            return timestamp, self._parse_message(data)

    def reads(self, timeout_us: int = DEFAULT_READ_TIMEOUT_US):
        """
        批量读取消息，返回解析后的 Messages protobuf 对象。

        :param timeout_us: 超时时间（微秒）
        :return: (timestamp_us, Messages) 元组
        :raises SwiftException: 读取失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, timestamp, data = self._api.read_messages(self._reader_ptr, timeout_us)
            self._raise_if_error(ec)
            return timestamp, self._parse_messages(data)

    # ------------------------------------------------------------------ #
    #  Seek 操作                                                            #
    # ------------------------------------------------------------------ #

    def seek_by_timestamp(self, timestamp_us: int, force: bool = False):
        """
        按时间戳定位读取位置。

        :param timestamp_us: 目标时间戳（微秒）
        :param force:        是否强制 seek（即使时间戳超出当前范围）
        :raises SwiftException: 操作失败时抛出
        """
        with self._lock:
            self._check_open()
            ec = self._api.seek_by_timestamp(self._reader_ptr, timestamp_us, force)
            self._raise_if_error(ec)

    def seek_by_message_id(self, msg_id: int):
        """
        按消息 ID 定位读取位置。

        :param msg_id: 目标消息 ID
        :raises SwiftException: 操作失败时抛出
        """
        with self._lock:
            self._check_open()
            ec = self._api.seek_by_message_id(self._reader_ptr, msg_id)
            self._raise_if_error(ec)

    # ------------------------------------------------------------------ #
    #  其他操作                                                             #
    # ------------------------------------------------------------------ #

    def set_timestamp_limit(self, time_limit_us: int) -> int:
        """
        设置读取的时间戳上限。

        :param time_limit_us: 时间戳上限（微秒）
        :return: 实际生效的 accept_timestamp（微秒）
        :raises SwiftException: Reader 已关闭时抛出
        """
        with self._lock:
            self._check_open()
            return self._api.set_timestamp_limit(self._reader_ptr, time_limit_us)

    def get_partition_status(self) -> Tuple[int, int, int]:
        """
        获取当前分区状态。

        :return: (refresh_time_us, max_message_id, max_message_timestamp_us)
        :raises SwiftException: Reader 已关闭时抛出
        """
        with self._lock:
            self._check_open()
            return self._api.get_partition_status(self._reader_ptr)

    # ------------------------------------------------------------------ #
    #  生命周期                                                             #
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
    #  内部工具                                                             #
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
            # proto 文件未生成时，返回原始字节
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
