# Swift Python Client

Python client for the Swift message queue, wrapping the native C++ library via `ctypes` to provide functionality equivalent to the Java client.

## Directory Structure

```
python_client/
├── swift/
│   ├── __init__.py          # Package entry, exports main classes
│   ├── client.py            # SwiftClient  — Main client
│   ├── reader.py            # SwiftReader  — Message reader
│   ├── writer.py            # SwiftWriter  — Message writer
│   ├── admin.py             # SwiftAdminAdaptor — Admin operations
│   ├── api.py               # ctypes native library wrapper
│   ├── exception.py         # Exception classes & error codes
│   ├── proto/               # protobuf generated files (run generate_proto.sh)
│   └── util/
│       ├── __init__.py
│       └── field_group.py   # FieldGroupWriter / FieldGroupReader
├── example.py               # Usage examples
├── generate_proto.sh        # Script to generate proto Python bindings
└── requirements.txt
```

## Dependencies

```bash
pip install -r requirements.txt
# Requires: protobuf >= 3.20.0
```

## Proto File Generation

The client uses protobuf for message serialization. Python bindings must be generated first:

```bash
# Ensure protoc is installed (3.x recommended)
which protoc

# Generate Python proto files
bash generate_proto.sh
```

After generation, the following files will appear in `swift/proto/`:
- `common_pb2.py`
- `err_code_pb2.py`
- `swift_message_pb2.py`
- `admin_request_response_pb2.py`
- `heartbeat_pb2.py`

> **Note**: The client works even without generated proto files, but read/write methods will return raw `bytes` instead of protobuf objects.

## Native Library Configuration

The Python client calls the following `.so` files via ctypes (same as the Java client):

| Library | Description |
|---------|-------------|
| `libprotobuf.so.7.0.0` | Protobuf runtime |
| `libzookeeper_mt.so.2.0.0` | ZooKeeper client |
| `libalog.so.13.2.2` | Alibaba logging library |
| `libanet.so.13.2.1` | Alibaba networking library |
| `libarpc.so.13.2.2` | Alibaba RPC library |
| `libautil.so.9.3` | Alibaba utility library |
| `libswift_client_minimal.so.107.2` | **Swift client main library** |

There are two ways for Python to locate these libraries:

**Option 1: Set LD_LIBRARY_PATH (recommended)**
```bash
export LD_LIBRARY_PATH=/path/to/so/files:$LD_LIBRARY_PATH
python example.py
```

**Option 2: Specify via lib_dir parameter**
```python
client = SwiftClient(lib_dir="/path/to/so/files")
```

> Tip: These `.so` files are identical to those in the `linux-x86-64/` directory inside the Java JAR package and can be extracted for reuse.

## Quick Start

### Initialize the Client

```python
from swift import SwiftClient

# Option 1: with statement (auto-close)
with SwiftClient(lib_dir="/path/to/so") as client:
    client.init("zkPath=zfs://10.0.0.1:2181/swift/service;logConfigFile=./alog.conf")
    # ...

# Option 2: Manual management
client = SwiftClient(lib_dir="/path/to/so")
client.init("zkPath=zfs://10.0.0.1:2181/swift/service")
# ...
client.close()
```

Client configuration string format:
```
zkPath=zfs://host1:port,host2:port,host3:port/path;
logConfigFile=./swift_alog.conf;
useFollowerAdmin=true|false
```

### Write Messages

```python
from swift.proto.swift_message_pb2 import WriteMessageInfo, WriteMessageInfoVec

writer = client.create_writer("topicName=my_topic")

# Write a single message
msg = WriteMessageInfo(data=b"hello swift")
writer.write(msg)
writer.wait_finished()          # Wait for persistence (default 30s timeout)

# Batch write
msgs = WriteMessageInfoVec()
for i in range(100):
    msgs.messageInfoVec.append(WriteMessageInfo(data=f"msg {i}".encode()))
writer.write_batch(msgs, wait_sent=True)

writer.close()
```

### Using FieldGroup for Structured Messages

```python
from swift.util import FieldGroupWriter, FieldGroupReader

# Producer side: serialize fields
fw = FieldGroupWriter()
fw.add_field("title", "Product Title", is_updated=False)
fw.add_field("price", "99.9", is_updated=True)
data = fw.to_bytes()

msg = WriteMessageInfo(data=data)
writer.write(msg)

# Consumer side: deserialize fields
fr = FieldGroupReader()
fr.from_production_string(raw_data)
for field in fr.fields:
    print(f"{field.name} = {field.value} (updated={field.is_updated})")
```

### Read Messages

```python
reader = client.create_reader("topicName=my_topic;partitionId=0")

# Read from latest timestamp (set start position in reader_config)
# reader_config options:
#   readFromOffset=readFromBeginning   Read from the beginning
#   readFromOffset=readFromEnd         Read from the end (default)
#   readFromOffset=readFromTimestamp:1234567890  Read from a specific timestamp

while True:
    try:
        timestamp, msg = reader.read(timeout_us=3_000_000)
        print(f"msgId={msg.msgId}, data={msg.data!r}")
    except SwiftException as e:
        if e.ec in (ErrorCode.ERROR_BROKER_NO_DATA,
                    ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT):
            break  # No more data
        raise

reader.close()
```

### Admin Operations

```python
admin = client.get_admin_adapter()

# Create a Topic
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

# Query Topic info
count = admin.get_partition_count("my_topic")
info = admin.get_topic_info("my_topic")

# Broker address
addr = admin.get_broker_address("my_topic", partition_id=0)

# Delete a Topic
admin.delete_topic("my_topic")

admin.close()
```

## Java Client Mapping

| Java | Python | Description |
|------|--------|-------------|
| `SwiftClient.java` | `swift/client.py` | Main client entry point |
| `SwiftReader.java` | `swift/reader.py` | Message reader |
| `SwiftWriter.java` | `swift/writer.py` | Message writer |
| `SwiftAdminAdaptor.java` | `swift/admin.py` | Admin operations |
| `SwiftClientApiLibrary.java` | `swift/api.py` | Native library wrapper (JNA -> ctypes) |
| `SwiftException.java` | `swift/exception.py` | Exception classes |
| `FieldGroupWriter.java` | `swift/util/field_group.py` | Field serialization |
| `FieldGroupReader.java` | `swift/util/field_group.py` | Field deserialization |

### Timeout Units

Consistent with the Java client, all timeout parameters are in **microseconds (us)**:
- `read()` default timeout: `3_000_000` us = 3s
- `wait_finished()` default timeout: `30_000_000` us = 30s
- `wait_sent()` default timeout: `3_000_000` us = 3s

## Error Handling

All operations throw `SwiftException` on failure. Use the `.ec` attribute to get the error code:

```python
from swift import SwiftException, ErrorCode

try:
    admin.create_topic(req)
except SwiftException as e:
    if e.ec == ErrorCode.ERROR_ADMIN_TOPIC_HAS_EXISTED:
        print("Topic already exists")
    else:
        raise
```

Common error codes:
- `ERROR_ADMIN_TOPIC_HAS_EXISTED` (21101) — Topic already exists
- `ERROR_ADMIN_TOPIC_NOT_EXISTED` (21102) — Topic does not exist
- `ERROR_BROKER_NO_DATA` (12107) — No new messages available
- `ERROR_CLIENT_READ_MESSAGE_TIMEOUT` (13209) — Read timeout
- `ERROR_CLIENT_NO_MORE_MESSAGE` (13213) — No more messages
