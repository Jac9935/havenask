"""
Swift Python Client

Python client for the Havenask Swift message queue, wrapping the native C++ library via ctypes.
Reference implementation: aios/apps/facility/swift/client/java_client

Quick start:
    from swift import SwiftClient

    with SwiftClient(lib_dir="/path/to/so") as client:
        client.init("zkPath=zfs://10.0.0.1:2181/swift/service")

        # Write messages
        writer = client.create_writer("topicName=my_topic")
        from swift.proto.swift_message_pb2 import WriteMessageInfo
        msg = WriteMessageInfo(data=b"hello swift")
        writer.write(msg)
        writer.wait_finished()

        # Read messages
        reader = client.create_reader("topicName=my_topic;partitionId=0")
        ts, message = reader.read()
        print(message.data)

        # Admin operations
        admin = client.get_admin_adapter()
        count = admin.get_partition_count("my_topic")
"""

from .client import SwiftClient
from .reader import SwiftReader
from .writer import SwiftWriter
from .admin import SwiftAdminAdaptor
from .exception import SwiftException, SwiftRetryException, ErrorCode

__all__ = [
    "SwiftClient",
    "SwiftReader",
    "SwiftWriter",
    "SwiftAdminAdaptor",
    "SwiftException",
    "SwiftRetryException",
    "ErrorCode",
]
