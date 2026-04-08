"""
SwiftClient - Swift 客户端主入口。

对应 Java 端的 SwiftClient.java。
"""

import threading
from typing import List, Optional

from .api import SwiftClientApi
from .reader import SwiftReader
from .writer import SwiftWriter
from .admin import SwiftAdminAdaptor
from .exception import ErrorCode, SwiftException


class SwiftClient:
    """
    Swift 客户端主类，负责管理 Reader、Writer 和 AdminAdaptor 的生命周期。

    使用示例：
        client = SwiftClient(lib_dir="/path/to/so/files")
        client.init("zkPath=zfs://10.0.0.1:2181/swift/service;logConfigFile=./alog.conf")
        reader = client.create_reader("topicName=my_topic;partitionId=0")
        writer = client.create_writer("topicName=my_topic")
        admin  = client.get_admin_adapter()
        ...
        client.close()

    推荐使用 with 语句管理生命周期：
        with SwiftClient(lib_dir=...) as client:
            client.init(config_str)
            ...
    """

    def __init__(self, lib_dir: Optional[str] = None):
        """
        :param lib_dir: 原生 .so 文件所在目录。
                        如果为 None，则依赖 LD_LIBRARY_PATH 环境变量。
        """
        self._api: Optional[SwiftClientApi] = None
        self._lib_dir = lib_dir
        self._client_ptr: int = 0
        self._lock = threading.Lock()

        self._readers: List[SwiftReader] = []
        self._writers: List[SwiftWriter] = []
        self._admins: List[SwiftAdminAdaptor] = []

    # ------------------------------------------------------------------ #
    #  初始化                                                               #
    # ------------------------------------------------------------------ #

    def init(self, client_config_str: str):
        """
        初始化 Swift 客户端，加载原生库并建立连接。

        :param client_config_str: 客户端配置字符串，格式：
            "zkPath=zfs://host:port/swift/service;logConfigFile=./alog.conf;useFollowerAdmin=false"
        :raises SwiftException: 初始化失败时抛出
        """
        with self._lock:
            if self._client_ptr != 0:
                return

            if self._api is None:
                self._api = SwiftClientApi(self._lib_dir)

            ec, ptr = self._api.create_swift_client(client_config_str)
            error_code = ErrorCode.from_int(ec)
            if error_code != ErrorCode.ERROR_NONE or ptr == 0:
                raise SwiftException(error_code, "createSwiftClient failed")
            self._client_ptr = ptr

    # ------------------------------------------------------------------ #
    #  创建 Reader / Writer / Admin                                         #
    # ------------------------------------------------------------------ #

    def create_reader(self, reader_config_str: str) -> SwiftReader:
        """
        创建 Swift Reader。

        :param reader_config_str: Reader 配置字符串，格式：
            "topicName=my_topic;partitionId=0"
            "topicName=my_topic;partitionId=0;readFromOffset=readFromBeginning"
        :return: SwiftReader 实例（调用方负责 close）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_initialized()
            ec, reader_ptr = self._api.create_swift_reader(self._client_ptr, reader_config_str)
            error_code = ErrorCode.from_int(ec)
            if error_code != ErrorCode.ERROR_NONE or reader_ptr == 0:
                raise SwiftException(error_code, "createSwiftReader failed")
            reader = SwiftReader(self._api, reader_ptr)
            self._readers.append(reader)
            return reader

    def create_writer(self, writer_config_str: str) -> SwiftWriter:
        """
        创建 Swift Writer。

        :param writer_config_str: Writer 配置字符串，格式：
            "topicName=my_topic"
            "topicName=my_topic;functionChain=HASH,hashId2partId"
        :return: SwiftWriter 实例（调用方负责 close）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_initialized()
            ec, writer_ptr = self._api.create_swift_writer(self._client_ptr, writer_config_str)
            error_code = ErrorCode.from_int(ec)
            if error_code != ErrorCode.ERROR_NONE or writer_ptr == 0:
                raise SwiftException(error_code, "createSwiftWriter failed")
            writer = SwiftWriter(self._api, writer_ptr)
            self._writers.append(writer)
            return writer

    def get_admin_adapter(self, zk_path: Optional[str] = None) -> SwiftAdminAdaptor:
        """
        获取 Admin Adaptor 用于管理操作。

        :param zk_path: 可选，指定 ZK 路径；为 None 时使用 init() 中配置的路径
        :return: SwiftAdminAdaptor 实例（调用方负责 close）
        :raises SwiftException: 失败时抛出
        """
        with self._lock:
            self._check_initialized()
            if zk_path:
                admin_ptr = self._api.get_admin_adapter_by_zk(self._client_ptr, zk_path)
            else:
                admin_ptr = self._api.get_admin_adapter(self._client_ptr)
            if admin_ptr == 0:
                raise SwiftException(ErrorCode.ERROR_UNKNOWN, "getAdminAdapter returned null")
            adaptor = SwiftAdminAdaptor(self._api, admin_ptr)
            self._admins.append(adaptor)
            return adaptor

    # ------------------------------------------------------------------ #
    #  生命周期                                                             #
    # ------------------------------------------------------------------ #

    def is_closed(self) -> bool:
        with self._lock:
            return self._client_ptr == 0

    def close(self):
        """
        关闭客户端及所有关联的 Reader、Writer、Admin（与 Java 端行为一致）。
        """
        with self._lock:
            if self._client_ptr == 0:
                return

            for writer in self._writers:
                writer.close()
            self._writers.clear()

            for reader in self._readers:
                reader.close()
            self._readers.clear()

            for admin in self._admins:
                admin.close()
            self._admins.clear()

            self._api.delete_swift_client(self._client_ptr)
            self._client_ptr = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    # ------------------------------------------------------------------ #
    #  内部工具                                                             #
    # ------------------------------------------------------------------ #

    def _check_initialized(self):
        if self._client_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "SwiftClient is not initialized, call init() first")
