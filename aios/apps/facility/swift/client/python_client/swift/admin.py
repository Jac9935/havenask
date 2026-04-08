"""
Swift Admin Adaptor - Topic 管理操作。

对应 Java 端的 SwiftAdminAdaptor.java。
"""

import time
import threading
from typing import Optional

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftAdminAdaptor:
    """
    执行 Swift Topic 管理操作：创建/删除/查询 Topic、获取分区信息等。

    所有方法均线程安全（内部加锁）。
    """

    def __init__(self, api: SwiftClientApi, admin_ptr: int):
        self._api = api
        self._admin_ptr = admin_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Broker 地址查询                                                      #
    # ------------------------------------------------------------------ #

    def get_broker_address(self, topic_name: str, partition_id: int) -> str:
        """
        获取指定 topic 分区对应的 Broker 地址。

        :return: "host:port" 格式的地址字符串
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, address = self._api.get_broker_address(self._admin_ptr, topic_name, partition_id)
            self._raise_if_error(ec)
            return address

    # ------------------------------------------------------------------ #
    #  Topic CRUD                                                           #
    # ------------------------------------------------------------------ #

    def create_topic(self, request):
        """
        创建 Topic。

        :param request: TopicCreationRequest protobuf 对象，或已序列化的 bytes
        :raises SwiftException: 失败时抛出（含 ERROR_ADMIN_TOPIC_HAS_EXISTED）
        """
        with self._lock:
            self._check_open()
            data = request if isinstance(request, (bytes, bytearray)) else request.SerializeToString()
            ec = self._api.create_topic(self._admin_ptr, bytes(data))
            self._raise_if_error(ec)

    def delete_topic(self, topic_name: str):
        """
        删除 Topic。

        :raises SwiftException: 失败时抛出（含 ERROR_ADMIN_TOPIC_NOT_EXISTED）
        """
        with self._lock:
            self._check_open()
            ec = self._api.delete_topic(self._admin_ptr, topic_name)
            self._raise_if_error(ec)

    def wait_topic_ready(self, topic_name: str, timeout_sec: int = 60) -> bool:
        """
        轮询等待 Topic 进入 RUNNING 状态（与 Java 端逻辑一致，每 2s 轮询一次）。

        :param topic_name:  Topic 名称
        :param timeout_sec: 最长等待秒数
        :return: True 表示 Topic 已就绪，False 表示超时
        :raises SwiftException: 非"Topic 不存在"的错误时抛出
        """
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            try:
                response = self.get_topic_info(topic_name)
                topic_info = response.topicInfo
                # TopicStatus.TOPIC_STATUS_RUNNING == 2
                if topic_info.status == 2:
                    return True
            except SwiftException as e:
                if e.ec != ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED:
                    raise
            time.sleep(2)
        return False

    # ------------------------------------------------------------------ #
    #  Topic 查询                                                           #
    # ------------------------------------------------------------------ #

    def get_topic_info(self, topic_name: str):
        """
        查询 Topic 信息。

        :return: TopicInfoResponse protobuf 对象（如果 proto 未生成则返回 bytes）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_topic_info(self._admin_ptr, topic_name)
            self._raise_if_error(ec)
            return self._parse_topic_info_response(data)

    def get_all_topic_info(self):
        """
        查询所有 Topic 信息。

        :return: AllTopicInfoResponse protobuf 对象（如果 proto 未生成则返回 bytes）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_all_topic_info(self._admin_ptr)
            self._raise_if_error(ec)
            return self._parse_all_topic_info_response(data)

    def get_partition_count(self, topic_name: str) -> int:
        """
        获取 Topic 的分区数。

        :return: 分区数
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, count = self._api.get_partition_count(self._admin_ptr, topic_name)
            self._raise_if_error(ec)
            return count

    def get_partition_info(self, topic_name: str, partition_id: int):
        """
        查询单个分区信息。

        :return: PartitionInfoResponse protobuf 对象（如果 proto 未生成则返回 bytes）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_partition_info(self._admin_ptr, topic_name, partition_id)
            self._raise_if_error(ec)
            return self._parse_partition_info_response(data)

    # ------------------------------------------------------------------ #
    #  生命周期                                                             #
    # ------------------------------------------------------------------ #

    def is_closed(self) -> bool:
        with self._lock:
            return self._admin_ptr == 0

    def close(self):
        with self._lock:
            self._admin_ptr = 0

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
        if self._admin_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "AdminAdaptor is closed")

    @staticmethod
    def _raise_if_error(ec_int: int):
        ec = ErrorCode.from_int(ec_int)
        if ec != ErrorCode.ERROR_NONE:
            raise SwiftException(ec)

    @staticmethod
    def _parse_topic_info_response(data: bytes):
        try:
            from .proto.AdminRequestResponse_pb2 import TopicInfoResponse
            resp = TopicInfoResponse()
            resp.ParseFromString(data)
            return resp
        except ImportError:
            return data

    @staticmethod
    def _parse_all_topic_info_response(data: bytes):
        try:
            from .proto.AdminRequestResponse_pb2 import AllTopicInfoResponse
            resp = AllTopicInfoResponse()
            resp.ParseFromString(data)
            return resp
        except ImportError:
            return data

    @staticmethod
    def _parse_partition_info_response(data: bytes):
        try:
            from .proto.AdminRequestResponse_pb2 import PartitionInfoResponse
            resp = PartitionInfoResponse()
            resp.ParseFromString(data)
            return resp
        except ImportError:
            return data
