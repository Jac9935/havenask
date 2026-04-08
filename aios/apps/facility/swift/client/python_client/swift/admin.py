"""
Swift Admin Adaptor - Topic management operations.

Corresponds to SwiftAdminAdaptor.java on the Java side.
"""

import time
import threading
from typing import Optional

from .api import SwiftClientApi
from .exception import ErrorCode, SwiftException


class SwiftAdminAdaptor:
    """
    Performs Swift Topic management operations: create/delete/query topics,
    get partition info, etc.

    All methods are thread-safe (internally locked).
    """

    def __init__(self, api: SwiftClientApi, admin_ptr: int):
        self._api = api
        self._admin_ptr = admin_ptr
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Broker Address Query                                                #
    # ------------------------------------------------------------------ #

    def get_broker_address(self, topic_name: str, partition_id: int) -> str:
        """
        Get the broker address for the specified topic partition.

        :return: Address string in "host:port" format
        :raises SwiftException: Raised on failure
        """
        with self._lock:
            self._check_open()
            ec, address = self._api.get_broker_address(self._admin_ptr, topic_name, partition_id)
            self._raise_if_error(ec)
            return address

    # ------------------------------------------------------------------ #
    #  Topic CRUD                                                          #
    # ------------------------------------------------------------------ #

    def create_topic(self, request):
        """
        Create a Topic.

        :param request: TopicCreationRequest protobuf object, or serialized bytes
        :raises SwiftException: Raised on failure (including ERROR_ADMIN_TOPIC_HAS_EXISTED)
        """
        with self._lock:
            self._check_open()
            data = request if isinstance(request, (bytes, bytearray)) else request.SerializeToString()
            ec = self._api.create_topic(self._admin_ptr, bytes(data))
            self._raise_if_error(ec)

    def delete_topic(self, topic_name: str):
        """
        Delete a Topic.

        :raises SwiftException: Raised on failure (including ERROR_ADMIN_TOPIC_NOT_EXISTED)
        """
        with self._lock:
            self._check_open()
            ec = self._api.delete_topic(self._admin_ptr, topic_name)
            self._raise_if_error(ec)

    def wait_topic_ready(self, topic_name: str, timeout_sec: int = 60) -> bool:
        """
        Poll until the Topic enters RUNNING state (consistent with Java client logic,
        polling every 2 seconds).

        :param topic_name:  Topic name
        :param timeout_sec: Maximum wait time in seconds
        :return: True if Topic is ready, False on timeout
        :raises SwiftException: Raised on errors other than "Topic does not exist"
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
    #  Topic Query                                                         #
    # ------------------------------------------------------------------ #

    def get_topic_info(self, topic_name: str):
        """
        Query Topic information.

        :return: TopicInfoResponse protobuf object (returns bytes if proto is not generated)
        :raises SwiftException: Raised on failure
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_topic_info(self._admin_ptr, topic_name)
            self._raise_if_error(ec)
            return self._parse_topic_info_response(data)

    def get_all_topic_info(self):
        """
        Query all Topic information.

        :return: AllTopicInfoResponse protobuf object (returns bytes if proto is not generated)
        :raises SwiftException: Raised on failure
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_all_topic_info(self._admin_ptr)
            self._raise_if_error(ec)
            return self._parse_all_topic_info_response(data)

    def get_partition_count(self, topic_name: str) -> int:
        """
        Get the partition count of a Topic.

        :return: Partition count
        :raises SwiftException: Raised on failure
        """
        with self._lock:
            self._check_open()
            ec, count = self._api.get_partition_count(self._admin_ptr, topic_name)
            self._raise_if_error(ec)
            return count

    def get_partition_info(self, topic_name: str, partition_id: int):
        """
        Query single partition information.

        :return: PartitionInfoResponse protobuf object (returns bytes if proto is not generated)
        :raises SwiftException: Raised on failure
        """
        with self._lock:
            self._check_open()
            ec, data = self._api.get_partition_info(self._admin_ptr, topic_name, partition_id)
            self._raise_if_error(ec)
            return self._parse_partition_info_response(data)

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
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
    #  Internal Utilities                                                  #
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
