"""
ctypes wrapper for the Swift native library.

Corresponds to SwiftClientApiLibrary.java on the Java side (calling libswift_client_minimal.so via JNA).
Library loading order must be consistent with the Java side:
  libprotobuf -> libzookeeper_mt -> libalog -> libanet -> libarpc -> libautil -> libswift_client_minimal
"""

import ctypes
import ctypes.util
import os
from typing import Optional, Tuple


# On Linux x86_64, C long is 64-bit, consistent with Java NativeLong
c_long_p = ctypes.POINTER(ctypes.c_long)
c_int_p = ctypes.POINTER(ctypes.c_int)
c_void_pp = ctypes.POINTER(ctypes.c_void_p)


def _load_lib(name: str, lib_dir: Optional[str] = None, flags: int = ctypes.RTLD_GLOBAL) -> ctypes.CDLL:
    """Load a shared library, preferring lib_dir if specified."""
    if lib_dir:
        path = os.path.join(lib_dir, name)
        if os.path.exists(path):
            return ctypes.CDLL(path, mode=flags)
    return ctypes.CDLL(name, mode=flags)


class SwiftClientApi:
    """
    Wraps the C interfaces provided by libswift_client_minimal.so.

    All method signatures strictly follow the native declarations in SwiftClientApiLibrary.java.
    """

    # Dependency library loading order (consistent with Java side)
    _DEP_LIBS = [
        "libprotobuf.so.7.0.0",
        "libzookeeper_mt.so.2.0.0",
        "libalog.so.13.2.2",
        "libanet.so.13.2.1",
        "libarpc.so.13.2.2",
        "libautil.so.9.3",
    ]
    _MAIN_LIB = "libswift_client_minimal.so.107.2"

    def __init__(self, lib_dir: Optional[str] = None):
        """
        Initialize and load native libraries.

        :param lib_dir: Directory containing .so files; if None, relies on LD_LIBRARY_PATH
        """
        self._lib_dir = lib_dir
        self._dep_handles = []
        self._lib: Optional[ctypes.CDLL] = None
        self._load_libraries()
        self._setup_signatures()

    def _load_libraries(self):
        """Load all dependency libraries in order, then load the main library."""
        for dep in self._DEP_LIBS:
            handle = _load_lib(dep, self._lib_dir, ctypes.RTLD_GLOBAL)
            self._dep_handles.append(handle)

        self._lib = _load_lib(self._MAIN_LIB, self._lib_dir, ctypes.RTLD_GLOBAL)

    def _setup_signatures(self):
        """Set parameter types and return types for all native functions."""
        lib = self._lib

        # ---- Swift Client ----
        # int createSwiftClient(const char* clientConfStr, long& clientPtr)
        lib.createSwiftClient.argtypes = [ctypes.c_char_p, c_long_p]
        lib.createSwiftClient.restype = ctypes.c_int

        # void deleteSwiftClient(long swiftClientPtr)
        lib.deleteSwiftClient.argtypes = [ctypes.c_long]
        lib.deleteSwiftClient.restype = None

        # ---- Swift Reader ----
        # int createSwiftReader(long clientPtr, const char* readerConfStr, long& readerPtr)
        lib.createSwiftReader.argtypes = [ctypes.c_long, ctypes.c_char_p, c_long_p]
        lib.createSwiftReader.restype = ctypes.c_int

        # int readMessage(long readerPtr, long& timeStamp, char** msgStr, int& msgLen, long timeout)
        lib.readMessage.argtypes = [ctypes.c_long, c_long_p, c_void_pp, c_int_p, ctypes.c_long]
        lib.readMessage.restype = ctypes.c_int

        # int readMessages(long readerPtr, long& timeStamp, char** msgStr, int& msgLen, long timeout)
        lib.readMessages.argtypes = [ctypes.c_long, c_long_p, c_void_pp, c_int_p, ctypes.c_long]
        lib.readMessages.restype = ctypes.c_int

        # int seekByTimestamp(long readerPtr, long timeStamp, bool force)
        lib.seekByTimestamp.argtypes = [ctypes.c_long, ctypes.c_long, ctypes.c_byte]
        lib.seekByTimestamp.restype = ctypes.c_int

        # int seekByMessageId(long readerPtr, long msgId)
        lib.seekByMessageId.argtypes = [ctypes.c_long, ctypes.c_long]
        lib.seekByMessageId.restype = ctypes.c_int

        # void setTimestampLimit(long readerPtr, long timeLimit, long& acceptTimestamp)
        lib.setTimestampLimit.argtypes = [ctypes.c_long, ctypes.c_long, c_long_p]
        lib.setTimestampLimit.restype = None

        # void getPartitionStatus(long readerPtr, long& refreshTime, long& maxMessageId, long& maxMessageTimestamp)
        lib.getPartitionStatus.argtypes = [ctypes.c_long, c_long_p, c_long_p, c_long_p]
        lib.getPartitionStatus.restype = None

        # void deleteSwiftReader(long readerPtr)
        lib.deleteSwiftReader.argtypes = [ctypes.c_long]
        lib.deleteSwiftReader.restype = None

        # ---- Swift Writer ----
        # int createSwiftWriter(long clientPtr, const char* writerConfStr, long& writerPtr)
        lib.createSwiftWriter.argtypes = [ctypes.c_long, ctypes.c_char_p, c_long_p]
        lib.createSwiftWriter.restype = ctypes.c_int

        # int writeMessage(long writerPtr, char* msgInfoStr, int msgLen)
        lib.writeMessage.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_int]
        lib.writeMessage.restype = ctypes.c_int

        # int writeMessages(long writerPtr, char* msgInfoStr, int msgLen, bool waitSend)
        lib.writeMessages.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_int, ctypes.c_byte]
        lib.writeMessages.restype = ctypes.c_int

        # long getCommittedCheckpointId(long writerPtr)
        lib.getCommittedCheckpointId.argtypes = [ctypes.c_long]
        lib.getCommittedCheckpointId.restype = ctypes.c_long

        # int waitFinished(long writerPtr, long timeout)
        lib.waitFinished.argtypes = [ctypes.c_long, ctypes.c_long]
        lib.waitFinished.restype = ctypes.c_int

        # int waitSent(long writerPtr, long timeout)
        lib.waitSent.argtypes = [ctypes.c_long, ctypes.c_long]
        lib.waitSent.restype = ctypes.c_int

        # void deleteSwiftWriter(long writerPtr)
        lib.deleteSwiftWriter.argtypes = [ctypes.c_long]
        lib.deleteSwiftWriter.restype = None

        # ---- Admin Adapter ----
        # long getAdminAdapter(long swiftClientPtr)
        lib.getAdminAdapter.argtypes = [ctypes.c_long]
        lib.getAdminAdapter.restype = ctypes.c_long

        # long getAdminAdapterByZk(long swiftClientPtr, const char* zkPath)
        lib.getAdminAdapterByZk.argtypes = [ctypes.c_long, ctypes.c_char_p]
        lib.getAdminAdapterByZk.restype = ctypes.c_long

        # int getBrokerAddress(long adminPtr, const char* topicName, int partId, char** addressStr, int& strLen)
        lib.getBrokerAddress.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_int, c_void_pp, c_int_p]
        lib.getBrokerAddress.restype = ctypes.c_int

        # int createTopic(long adminPtr, char* topicConfigStr, int len)
        lib.createTopic.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_int]
        lib.createTopic.restype = ctypes.c_int

        # int deleteTopic(long adminPtr, const char* topicName)
        lib.deleteTopic.argtypes = [ctypes.c_long, ctypes.c_char_p]
        lib.deleteTopic.restype = ctypes.c_int

        # int getTopicInfo(long adminPtr, const char* topicName, char** responseStr, int& responseLen)
        lib.getTopicInfo.argtypes = [ctypes.c_long, ctypes.c_char_p, c_void_pp, c_int_p]
        lib.getTopicInfo.restype = ctypes.c_int

        # int getPartitionCount(long adminPtr, const char* topicName, int& partCount)
        lib.getPartitionCount.argtypes = [ctypes.c_long, ctypes.c_char_p, c_int_p]
        lib.getPartitionCount.restype = ctypes.c_int

        # int getPartitionInfo(long adminPtr, const char* topicName, int partId, char** responseStr, int& responseLen)
        lib.getPartitionInfo.argtypes = [ctypes.c_long, ctypes.c_char_p, ctypes.c_int, c_void_pp, c_int_p]
        lib.getPartitionInfo.restype = ctypes.c_int

        # int getAllTopicInfo(long adminPtr, char** responseStr, int& responseLen)
        lib.getAllTopicInfo.argtypes = [ctypes.c_long, c_void_pp, c_int_p]
        lib.getAllTopicInfo.restype = ctypes.c_int

        # void freeString(char* str)
        lib.freeString.argtypes = [ctypes.c_void_p]
        lib.freeString.restype = None

    # ------------------------------------------------------------------ #
    #  Internal Utility Methods                                            #
    # ------------------------------------------------------------------ #

    def _read_output_string(self, ptr: ctypes.c_void_p, length: int) -> bytes:
        """Read bytes from a native output pointer and free the memory."""
        if not ptr.value:
            return b""
        data = ctypes.string_at(ptr.value, length)
        self._lib.freeString(ptr)
        return data

    # ------------------------------------------------------------------ #
    #  Client Interface                                                    #
    # ------------------------------------------------------------------ #

    def create_swift_client(self, config_str: str) -> Tuple[int, int]:
        """
        Create a Swift client.

        :return: (error_code, client_ptr)
        """
        client_ptr = ctypes.c_long(0)
        ec = self._lib.createSwiftClient(config_str.encode("utf-8"), ctypes.byref(client_ptr))
        return ec, client_ptr.value

    def delete_swift_client(self, client_ptr: int):
        self._lib.deleteSwiftClient(ctypes.c_long(client_ptr))

    # ------------------------------------------------------------------ #
    #  Reader Interface                                                    #
    # ------------------------------------------------------------------ #

    def create_swift_reader(self, client_ptr: int, reader_config: str) -> Tuple[int, int]:
        """
        Create a Swift Reader.

        :return: (error_code, reader_ptr)
        """
        reader_ptr = ctypes.c_long(0)
        ec = self._lib.createSwiftReader(
            ctypes.c_long(client_ptr),
            reader_config.encode("utf-8"),
            ctypes.byref(reader_ptr),
        )
        return ec, reader_ptr.value

    def read_message(self, reader_ptr: int, timeout: int) -> Tuple[int, int, bytes]:
        """
        Read a single message.

        :param reader_ptr: Reader pointer
        :param timeout:    Timeout in microseconds
        :return: (error_code, timestamp_us, serialized_Message_bytes)
        """
        timestamp = ctypes.c_long(0)
        msg_ptr = ctypes.c_void_p(0)
        msg_len = ctypes.c_int(0)
        ec = self._lib.readMessage(
            ctypes.c_long(reader_ptr),
            ctypes.byref(timestamp),
            ctypes.byref(msg_ptr),
            ctypes.byref(msg_len),
            ctypes.c_long(timeout),
        )
        data = self._read_output_string(msg_ptr, msg_len.value) if ec == 0 else b""
        return ec, timestamp.value, data

    def read_messages(self, reader_ptr: int, timeout: int) -> Tuple[int, int, bytes]:
        """
        Read messages in batch (returns serialized Messages bytes).

        :return: (error_code, timestamp_us, serialized_Messages_bytes)
        """
        timestamp = ctypes.c_long(0)
        msg_ptr = ctypes.c_void_p(0)
        msg_len = ctypes.c_int(0)
        ec = self._lib.readMessages(
            ctypes.c_long(reader_ptr),
            ctypes.byref(timestamp),
            ctypes.byref(msg_ptr),
            ctypes.byref(msg_len),
            ctypes.c_long(timeout),
        )
        data = self._read_output_string(msg_ptr, msg_len.value) if ec == 0 else b""
        return ec, timestamp.value, data

    def seek_by_timestamp(self, reader_ptr: int, timestamp: int, force: bool) -> int:
        return self._lib.seekByTimestamp(
            ctypes.c_long(reader_ptr),
            ctypes.c_long(timestamp),
            ctypes.c_byte(1 if force else 0),
        )

    def seek_by_message_id(self, reader_ptr: int, msg_id: int) -> int:
        return self._lib.seekByMessageId(ctypes.c_long(reader_ptr), ctypes.c_long(msg_id))

    def set_timestamp_limit(self, reader_ptr: int, time_limit: int) -> int:
        """
        Set timestamp upper limit.

        :return: accept_timestamp (microseconds)
        """
        accept_ts = ctypes.c_long(0)
        self._lib.setTimestampLimit(
            ctypes.c_long(reader_ptr),
            ctypes.c_long(time_limit),
            ctypes.byref(accept_ts),
        )
        return accept_ts.value

    def get_partition_status(self, reader_ptr: int) -> Tuple[int, int, int]:
        """
        Get partition status.

        :return: (refresh_time_us, max_message_id, max_message_timestamp_us)
        """
        refresh_time = ctypes.c_long(0)
        max_msg_id = ctypes.c_long(0)
        max_msg_ts = ctypes.c_long(0)
        self._lib.getPartitionStatus(
            ctypes.c_long(reader_ptr),
            ctypes.byref(refresh_time),
            ctypes.byref(max_msg_id),
            ctypes.byref(max_msg_ts),
        )
        return refresh_time.value, max_msg_id.value, max_msg_ts.value

    def delete_swift_reader(self, reader_ptr: int):
        self._lib.deleteSwiftReader(ctypes.c_long(reader_ptr))

    # ------------------------------------------------------------------ #
    #  Writer Interface                                                    #
    # ------------------------------------------------------------------ #

    def create_swift_writer(self, client_ptr: int, writer_config: str) -> Tuple[int, int]:
        """
        Create a Swift Writer.

        :return: (error_code, writer_ptr)
        """
        writer_ptr = ctypes.c_long(0)
        ec = self._lib.createSwiftWriter(
            ctypes.c_long(client_ptr),
            writer_config.encode("utf-8"),
            ctypes.byref(writer_ptr),
        )
        return ec, writer_ptr.value

    def write_message(self, writer_ptr: int, serialized_msg: bytes) -> int:
        return self._lib.writeMessage(
            ctypes.c_long(writer_ptr),
            serialized_msg,
            ctypes.c_int(len(serialized_msg)),
        )

    def write_messages(self, writer_ptr: int, serialized_msgs: bytes, wait_send: bool) -> int:
        return self._lib.writeMessages(
            ctypes.c_long(writer_ptr),
            serialized_msgs,
            ctypes.c_int(len(serialized_msgs)),
            ctypes.c_byte(1 if wait_send else 0),
        )

    def get_committed_checkpoint_id(self, writer_ptr: int) -> int:
        return self._lib.getCommittedCheckpointId(ctypes.c_long(writer_ptr))

    def wait_finished(self, writer_ptr: int, timeout: int) -> int:
        return self._lib.waitFinished(ctypes.c_long(writer_ptr), ctypes.c_long(timeout))

    def wait_sent(self, writer_ptr: int, timeout: int) -> int:
        return self._lib.waitSent(ctypes.c_long(writer_ptr), ctypes.c_long(timeout))

    def delete_swift_writer(self, writer_ptr: int):
        self._lib.deleteSwiftWriter(ctypes.c_long(writer_ptr))

    # ------------------------------------------------------------------ #
    #  Admin Interface                                                     #
    # ------------------------------------------------------------------ #

    def get_admin_adapter(self, client_ptr: int) -> int:
        return self._lib.getAdminAdapter(ctypes.c_long(client_ptr))

    def get_admin_adapter_by_zk(self, client_ptr: int, zk_path: str) -> int:
        return self._lib.getAdminAdapterByZk(ctypes.c_long(client_ptr), zk_path.encode("utf-8"))

    def get_broker_address(self, admin_ptr: int, topic_name: str, partition_id: int) -> Tuple[int, str]:
        """
        Get broker address.

        :return: (error_code, broker_address)
        """
        addr_ptr = ctypes.c_void_p(0)
        addr_len = ctypes.c_int(0)
        ec = self._lib.getBrokerAddress(
            ctypes.c_long(admin_ptr),
            topic_name.encode("utf-8"),
            ctypes.c_int(partition_id),
            ctypes.byref(addr_ptr),
            ctypes.byref(addr_len),
        )
        if ec != 0:
            return ec, ""
        data = self._read_output_string(addr_ptr, addr_len.value)
        return ec, data.decode("utf-8")

    def create_topic(self, admin_ptr: int, serialized_request: bytes) -> int:
        return self._lib.createTopic(
            ctypes.c_long(admin_ptr),
            serialized_request,
            ctypes.c_int(len(serialized_request)),
        )

    def delete_topic(self, admin_ptr: int, topic_name: str) -> int:
        return self._lib.deleteTopic(ctypes.c_long(admin_ptr), topic_name.encode("utf-8"))

    def get_topic_info(self, admin_ptr: int, topic_name: str) -> Tuple[int, bytes]:
        """
        :return: (error_code, serialized_TopicInfoResponse_bytes)
        """
        info_ptr = ctypes.c_void_p(0)
        info_len = ctypes.c_int(0)
        ec = self._lib.getTopicInfo(
            ctypes.c_long(admin_ptr),
            topic_name.encode("utf-8"),
            ctypes.byref(info_ptr),
            ctypes.byref(info_len),
        )
        data = self._read_output_string(info_ptr, info_len.value) if ec == 0 else b""
        return ec, data

    def get_partition_count(self, admin_ptr: int, topic_name: str) -> Tuple[int, int]:
        """
        :return: (error_code, partition_count)
        """
        part_count = ctypes.c_int(0)
        ec = self._lib.getPartitionCount(
            ctypes.c_long(admin_ptr),
            topic_name.encode("utf-8"),
            ctypes.byref(part_count),
        )
        return ec, part_count.value

    def get_partition_info(self, admin_ptr: int, topic_name: str, partition_id: int) -> Tuple[int, bytes]:
        """
        :return: (error_code, serialized_PartitionInfoResponse_bytes)
        """
        info_ptr = ctypes.c_void_p(0)
        info_len = ctypes.c_int(0)
        ec = self._lib.getPartitionInfo(
            ctypes.c_long(admin_ptr),
            topic_name.encode("utf-8"),
            ctypes.c_int(partition_id),
            ctypes.byref(info_ptr),
            ctypes.byref(info_len),
        )
        data = self._read_output_string(info_ptr, info_len.value) if ec == 0 else b""
        return ec, data

    def get_all_topic_info(self, admin_ptr: int) -> Tuple[int, bytes]:
        """
        :return: (error_code, serialized_AllTopicInfoResponse_bytes)
        """
        info_ptr = ctypes.c_void_p(0)
        info_len = ctypes.c_int(0)
        ec = self._lib.getAllTopicInfo(
            ctypes.c_long(admin_ptr),
            ctypes.byref(info_ptr),
            ctypes.byref(info_len),
        )
        data = self._read_output_string(info_ptr, info_len.value) if ec == 0 else b""
        return ec, data
