"""
Swift Python Client

对应 Havenask Swift 消息队列的 Python 客户端，基于 ctypes 封装原生 C++ 库。
参考实现：aios/apps/facility/swift/client/java_client

快速入门：
    from swift import SwiftClient

    with SwiftClient(lib_dir="/path/to/so") as client:
        client.init("zkPath=zfs://10.0.0.1:2181/swift/service")

        # 写消息
        writer = client.create_writer("topicName=my_topic")
        from swift.proto.swift_message_pb2 import WriteMessageInfo
        msg = WriteMessageInfo(data=b"hello swift")
        writer.write(msg)
        writer.wait_finished()

        # 读消息
        reader = client.create_reader("topicName=my_topic;partitionId=0")
        ts, message = reader.read()
        print(message.data)

        # 管理操作
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
