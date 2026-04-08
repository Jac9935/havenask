"""
SwiftClient - Main client entry point.

Corresponds to SwiftClient.java on the Java side.
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
    Main Swift client class, responsible for managing the lifecycle of
    Reader, Writer, and AdminAdaptor instances.

    Example:
        client = SwiftClient(lib_dir="/path/to/so/files")
        client.init("zkPath=zfs://10.0.0.1:2181/swift/service;logConfigFile=./alog.conf")
        reader = client.create_reader("topicName=my_topic;partitionId=0")
        writer = client.create_writer("topicName=my_topic")
        admin  = client.get_admin_adapter()
        ...
        client.close()

    Recommended: use with statement for lifecycle management:
        with SwiftClient(lib_dir=...) as client:
            client.init(config_str)
            ...
    """

    def __init__(self, lib_dir: Optional[str] = None):
        """
        :param lib_dir: Directory containing native .so files.
                        If None, relies on the LD_LIBRARY_PATH environment variable.
        """
        self._api: Optional[SwiftClientApi] = None
        self._lib_dir = lib_dir
        self._client_ptr: int = 0
        self._lock = threading.Lock()

        self._readers: List[SwiftReader] = []
        self._writers: List[SwiftWriter] = []
        self._admins: List[SwiftAdminAdaptor] = []

    # ------------------------------------------------------------------ #
    #  Initialization                                                      #
    # ------------------------------------------------------------------ #

    def init(self, client_config_str: str):
        """
        Initialize the Swift client, load native libraries and establish connection.

        :param client_config_str: Client configuration string, format:
            "zkPath=zfs://host:port/swift/service;logConfigFile=./alog.conf;useFollowerAdmin=false"
        :raises SwiftException: Raised on initialization failure
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
    #  Create Reader / Writer / Admin                                      #
    # ------------------------------------------------------------------ #

    def create_reader(self, reader_config_str: str) -> SwiftReader:
        """
        Create a Swift Reader.

        :param reader_config_str: Reader configuration string, format:
            "topicName=my_topic;partitionId=0"
            "topicName=my_topic;partitionId=0;readFromOffset=readFromBeginning"
        :return: SwiftReader instance (caller is responsible for closing)
        :raises SwiftException: Raised on failure
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
        Create a Swift Writer.

        :param writer_config_str: Writer configuration string, format:
            "topicName=my_topic"
            "topicName=my_topic;functionChain=HASH,hashId2partId"
        :return: SwiftWriter instance (caller is responsible for closing)
        :raises SwiftException: Raised on failure
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
        Get an Admin Adaptor for management operations.

        :param zk_path: Optional ZK path; if None, uses the path configured in init()
        :return: SwiftAdminAdaptor instance (caller is responsible for closing)
        :raises SwiftException: Raised on failure
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
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def is_closed(self) -> bool:
        with self._lock:
            return self._client_ptr == 0

    def close(self):
        """
        Close the client and all associated Readers, Writers, and Admins
        (consistent with Java client behavior).
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
    #  Internal Utilities                                                  #
    # ------------------------------------------------------------------ #

    def _check_initialized(self):
        if self._client_ptr == 0:
            raise SwiftException(ErrorCode.ERROR_UNKNOWN, "SwiftClient is not initialized, call init() first")
