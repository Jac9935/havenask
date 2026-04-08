# Swift Python Client

Swift 消息队列的 Python 客户端，基于 `ctypes` 封装原生 C++ 库，实现与 Java 客户端等价的功能。

## 目录结构

```
python_client/
├── swift/
│   ├── __init__.py          # 包入口，导出主要类
│   ├── client.py            # SwiftClient  — 主客户端
│   ├── reader.py            # SwiftReader  — 消息读取器
│   ├── writer.py            # SwiftWriter  — 消息写入器
│   ├── admin.py             # SwiftAdminAdaptor — 管理操作
│   ├── api.py               # ctypes 原生库封装层
│   ├── exception.py         # 异常类 & 错误码
│   ├── proto/               # protobuf 生成文件（需执行 generate_proto.sh）
│   └── util/
│       ├── __init__.py
│       └── field_group.py   # FieldGroupWriter / FieldGroupReader
├── example.py               # 使用示例
├── generate_proto.sh        # 生成 proto Python 绑定的脚本
└── requirements.txt
```

## 依赖

```bash
pip install -r requirements.txt
# 依赖：protobuf >= 3.20.0
```

## Proto 文件生成

客户端使用 protobuf 进行消息序列化，需要先生成 Python 绑定：

```bash
# 确保 protoc 已安装（推荐 3.x）
which protoc

# 生成 Python proto 文件
bash generate_proto.sh
```

生成后 `swift/proto/` 目录下会出现以下文件：
- `common_pb2.py`
- `err_code_pb2.py`
- `swift_message_pb2.py`
- `admin_request_response_pb2.py`
- `heartbeat_pb2.py`

> **注意**：即使不生成 proto 文件，客户端也可工作，但读写方法返回原始 `bytes` 而非 protobuf 对象。

## 原生库配置

Python 客户端通过 ctypes 调用以下 `.so` 文件（与 Java 端相同）：

| 库文件 | 说明 |
|--------|------|
| `libprotobuf.so.7.0.0` | Protobuf 运行时 |
| `libzookeeper_mt.so.2.0.0` | ZooKeeper 客户端 |
| `libalog.so.13.2.2` | Alibaba 日志库 |
| `libanet.so.13.2.1` | Alibaba 网络库 |
| `libarpc.so.13.2.2` | Alibaba RPC 库 |
| `libautil.so.9.3` | Alibaba 工具库 |
| `libswift_client_minimal.so.107.2` | **Swift 客户端主库** |

有两种方式让 Python 找到这些库：

**方式 1：设置 LD_LIBRARY_PATH（推荐）**
```bash
export LD_LIBRARY_PATH=/path/to/so/files:$LD_LIBRARY_PATH
python example.py
```

**方式 2：通过 lib_dir 参数指定**
```python
client = SwiftClient(lib_dir="/path/to/so/files")
```

> 提示：这些 `.so` 文件与 Java JAR 包中的 `linux-x86-64/` 目录内容相同，可直接解压复用。

## 快速使用

### 初始化客户端

```python
from swift import SwiftClient

# 方式 1：with 语句（自动关闭）
with SwiftClient(lib_dir="/path/to/so") as client:
    client.init("zkPath=zfs://10.0.0.1:2181/swift/service;logConfigFile=./alog.conf")
    # ...

# 方式 2：手动管理
client = SwiftClient(lib_dir="/path/to/so")
client.init("zkPath=zfs://10.0.0.1:2181/swift/service")
# ...
client.close()
```

客户端配置字符串格式：
```
zkPath=zfs://host1:port,host2:port,host3:port/path;
logConfigFile=./swift_alog.conf;
useFollowerAdmin=true|false
```

### 写入消息

```python
from swift.proto.swift_message_pb2 import WriteMessageInfo, WriteMessageInfoVec

writer = client.create_writer("topicName=my_topic")

# 写入单条消息
msg = WriteMessageInfo(data=b"hello swift")
writer.write(msg)
writer.wait_finished()          # 等待持久化（默认 30s 超时）

# 批量写入
msgs = WriteMessageInfoVec()
for i in range(100):
    msgs.messageInfoVec.append(WriteMessageInfo(data=f"msg {i}".encode()))
writer.write_batch(msgs, wait_sent=True)

writer.close()
```

### 使用 FieldGroup 结构化消息

```python
from swift.util import FieldGroupWriter, FieldGroupReader

# 写入端：序列化字段
fw = FieldGroupWriter()
fw.add_field("title", "商品标题", is_updated=False)
fw.add_field("price", "99.9", is_updated=True)
data = fw.to_bytes()

msg = WriteMessageInfo(data=data)
writer.write(msg)

# 读取端：反序列化字段
fr = FieldGroupReader()
fr.from_production_string(raw_data)
for field in fr.fields:
    print(f"{field.name} = {field.value} (updated={field.is_updated})")
```

### 读取消息

```python
reader = client.create_reader("topicName=my_topic;partitionId=0")

# 从最新时间戳开始读（在 reader_config 中设置起始位置）
# reader_config 选项：
#   readFromOffset=readFromBeginning   从头开始
#   readFromOffset=readFromEnd         从末尾开始（默认）
#   readFromOffset=readFromTimestamp:1234567890  从指定时间戳开始

while True:
    try:
        timestamp, msg = reader.read(timeout_us=3_000_000)
        print(f"msgId={msg.msgId}, data={msg.data!r}")
    except SwiftException as e:
        if e.ec in (ErrorCode.ERROR_BROKER_NO_DATA,
                    ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT):
            break  # 无更多数据
        raise

reader.close()
```

### 管理操作

```python
admin = client.get_admin_adapter()

# 创建 Topic
from swift.proto.admin_request_response_pb2 import TopicCreationRequest
from swift.proto.common_pb2 import TOPIC_MODE_NORMAL

req = TopicCreationRequest(
    topicName="my_topic",
    partitionCount=4,
    topicMode=TOPIC_MODE_NORMAL,
    partitionMinBufferSize=8,   # MB
    partitionMaxBufferSize=256, # MB
)
admin.create_topic(req)
admin.wait_topic_ready("my_topic", timeout_sec=60)

# 查询 Topic 信息
count = admin.get_partition_count("my_topic")
info = admin.get_topic_info("my_topic")

# Broker 地址
addr = admin.get_broker_address("my_topic", partition_id=0)

# 删除 Topic
admin.delete_topic("my_topic")

admin.close()
```

## 与 Java 客户端的对应关系

| Java | Python | 说明 |
|------|--------|------|
| `SwiftClient.java` | `swift/client.py` | 主客户端入口 |
| `SwiftReader.java` | `swift/reader.py` | 消息读取器 |
| `SwiftWriter.java` | `swift/writer.py` | 消息写入器 |
| `SwiftAdminAdaptor.java` | `swift/admin.py` | 管理操作 |
| `SwiftClientApiLibrary.java` | `swift/api.py` | 原生库封装（JNA→ctypes） |
| `SwiftException.java` | `swift/exception.py` | 异常类 |
| `FieldGroupWriter.java` | `swift/util/field_group.py` | 字段序列化 |
| `FieldGroupReader.java` | `swift/util/field_group.py` | 字段反序列化 |

### 超时单位

与 Java 端一致，所有超时参数单位均为**微秒（μs）**：
- `read()` 默认超时：`3_000_000` μs = 3s
- `wait_finished()` 默认超时：`30_000_000` μs = 30s
- `wait_sent()` 默认超时：`3_000_000` μs = 3s

## 错误处理

所有操作失败时抛出 `SwiftException`，可通过 `.ec` 属性获取错误码：

```python
from swift import SwiftException, ErrorCode

try:
    admin.create_topic(req)
except SwiftException as e:
    if e.ec == ErrorCode.ERROR_ADMIN_TOPIC_HAS_EXISTED:
        print("Topic 已存在")
    else:
        raise
```

常见错误码：
- `ERROR_ADMIN_TOPIC_HAS_EXISTED` (21101) — Topic 已存在
- `ERROR_ADMIN_TOPIC_NOT_EXISTED` (21102) — Topic 不存在
- `ERROR_BROKER_NO_DATA` (12107) — 暂无新消息
- `ERROR_CLIENT_READ_MESSAGE_TIMEOUT` (13209) — 读取超时
- `ERROR_CLIENT_NO_MORE_MESSAGE` (13213) — 没有更多消息
