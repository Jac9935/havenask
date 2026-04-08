"""
Swift Python Client Usage Examples

Prerequisites:
  1. Run ./generate_proto.sh to generate proto binding files
  2. Ensure .so file paths are correct (lib_dir parameter)
  3. Configure the correct ZooKeeper address

Usage:
  python example.py
"""

import sys
import os

# Add package path
sys.path.insert(0, os.path.dirname(__file__))

from swift import SwiftClient, SwiftException, ErrorCode
from swift.util import FieldGroupWriter, FieldGroupReader

# ---- Configuration ----
ZK_PATH = "zfs://x.x.x.x:2181/havenask/swift-local/appmaster"
TOPIC_NAME = "test"
LIB_DIR = os.path.join(os.path.dirname(__file__), "../java_client/src/main/resources/linux-x86-64")

_SO_DIR = os.path.abspath(LIB_DIR)
CLIENT_CONFIG = f"zkPath={ZK_PATH};logConfigFile={_SO_DIR}/swift_alog.conf;useFollowerAdmin=false"
WRITER_CONFIG = f"topicName={TOPIC_NAME}"
READER_CONFIG = f"topicName={TOPIC_NAME}"


def demo_admin(client: SwiftClient):
    """Demonstrate admin operations: create topic, query partition count, etc."""
    print("\n=== Admin Operations ===")
    admin = client.get_admin_adapter()

    # Query all topics
    try:
        resp = admin.get_all_topic_info()
        print(f"Total topic count: {len(resp.allTopicInfo)}")
    except SwiftException as e:
        print(f"Failed to query all topics: {e}")

    # Query partition count for a specific topic
    try:
        count = admin.get_partition_count(TOPIC_NAME)
        print(f"Topic [{TOPIC_NAME}] partition count: {count}")
    except SwiftException as e:
        if e.ec == ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED:
            print(f"Topic [{TOPIC_NAME}] does not exist, skipping query")
        else:
            print(f"Failed to query partition count: {e}")

    admin.close()


def demo_write(client: SwiftClient):
    """Demonstrate writing messages (using FieldGroupWriter to construct data)."""
    print("\n=== Write Messages ===")
    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo, WriteMessageInfoVec
    except ImportError:
        print("Warning: proto files not generated, skipping write demo. Please run ./generate_proto.sh first")
        return

    with client.create_writer(WRITER_CONFIG) as writer:
        # Use FieldGroupWriter to construct structured data
        fg_writer = FieldGroupWriter()
        fg_writer.add_field("title", "Hello Swift from Python!", is_updated=False)
        fg_writer.add_field("content", "This is a test message.", is_updated=False)
        fg_writer.add_field("id", "12345", is_updated=True)

        msg = WriteMessageInfo(
            data=fg_writer.to_bytes(),
            uint16Payload=0,
            compress=False,
        )
        writer.write(msg)
        print("Message written, waiting for completion...")
        writer.wait_finished()

        checkpoint = writer.get_committed_checkpoint_id()
        print(f"Committed checkpoint ID: {checkpoint}")

        # Batch write example
        msgs = WriteMessageInfoVec()
        for i in range(5):
            m = WriteMessageInfo(data=f"batch message {i}".encode())
            msgs.messageInfoVec.append(m)
        writer.write_batch(msgs, wait_sent=True)
        print("Batch write of 5 messages completed")


def demo_read(client: SwiftClient):
    """Demonstrate reading messages (read up to 10 messages, exit on timeout)."""
    print("\n=== Read Messages ===")
    try:
        from swift.proto.SwiftMessage_pb2 import Message
    except ImportError:
        print("Warning: proto files not generated, skipping read demo. Please run ./generate_proto.sh first")
        return

    with client.create_reader(READER_CONFIG) as reader:
        # Read from latest position (set readFromOffset in READER_CONFIG for actual use)
        count = 0
        while count < 10:
            try:
                ts, msg = reader.read(timeout_us=3_000_000)
                print(f"Message #{msg.msgId} | ts={ts} | data={msg.data[:50]!r}")

                # If data is in FieldGroup format, parse it
                try:
                    fg_reader = FieldGroupReader()
                    fg_reader.from_production_string(msg.data)
                    for f in fg_reader.fields:
                        print(f"  Field: {f.name} = {f.value} (updated={f.is_updated})")
                except Exception:
                    pass  # Not FieldGroup format, ignore

                count += 1
            except SwiftException as e:
                if e.ec in (ErrorCode.ERROR_BROKER_NO_DATA,
                             ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT,
                             ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE):
                    print("No new messages, exiting read loop")
                    break
                raise

        # Query partition status
        refresh_time, max_id, max_ts = reader.get_partition_status()
        print(f"\nPartition status: refreshTime={refresh_time}, maxMsgId={max_id}, maxMsgTs={max_ts}")


def demo_hash_shard(client: SwiftClient):
    """Demonstrate hash-based shard writing and per-shard read verification."""
    print("\n=== Hash Shard Write & Verification ===")
    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo
    except ImportError:
        print("Warning: proto files not generated, skipping demo.")
        return

    # Query partition count
    admin = client.get_admin_adapter()
    partition_count = admin.get_partition_count(TOPIC_NAME)
    admin.close()
    print(f"Topic [{TOPIC_NAME}] partition count: {partition_count}")

    # Test primary keys
    primary_keys = [
        "user_001", "user_002", "user_003",
        "order_100", "order_200", "order_300",
        "item_aaa", "item_bbb", "item_ccc",
        "doc_xyz",  "doc_abc",  "doc_123",
    ]

    # 1. Write with primary key hash routing via functionChain=HASH,hashId2partId
    writer_config = f"topicName={TOPIC_NAME};functionChain=HASH,hashId2partId"
    print(f"\n--- Writing {len(primary_keys)} messages ({writer_config}) ---")
    with client.create_writer(writer_config) as writer:
        for pk in primary_keys:
            msg = WriteMessageInfo()
            msg.data    = f"data_for_{pk}".encode("utf-8")
            msg.hashStr = pk.encode("utf-8")   # Primary key as hash shard key
            writer.write(msg)
            print(f"  Write: pk={pk!r:15s}  data={msg.data.decode()!r}")
        writer.wait_finished()
        print(f"Committed checkpoint ID: {writer.get_committed_checkpoint_id()}")

    # 2. Read per shard to verify message distribution
    print(f"\n--- Reading per shard ({partition_count} shards total) ---")
    shard_counts = {}
    for part_id in range(partition_count):
        reader_config = f"topicName={TOPIC_NAME};partitions={part_id}"
        msgs_in_shard = []
        try:
            with client.create_reader(reader_config) as reader:
                while True:
                    try:
                        ts, msg = reader.read(timeout_us=2_000_000)
                        msgs_in_shard.append(msg)
                    except SwiftException as e:
                        if e.ec in (ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE,
                                    ErrorCode.ERROR_BROKER_NO_DATA,
                                    ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT,
                                    ErrorCode.ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT):
                            break
                        raise
        except SwiftException as e:
            print(f"  shard {part_id}: failed to create reader -> {e}")
            continue

        shard_counts[part_id] = len(msgs_in_shard)
        print(f"\n  Shard {part_id} ({len(msgs_in_shard)} messages):")
        for msg in msgs_in_shard:
            print(f"    msgId={msg.msgId:<6d} data={msg.data.decode()!r}")

    print(f"\n--- Shard Distribution Summary ---")
    for part_id in range(partition_count):
        count = shard_counts.get(part_id, 0)
        print(f"  Shard {part_id}: {'█' * count} {count} messages")


def demo_perf(client: SwiftClient):
    """
    Performance test: measure write and read throughput for 100B / 1KB / 100KB messages.

    Method:
      - Write: use functionChain=HASH,hashId2partId for even shard distribution
      - Read: re-read all messages from the topic beginning
      - Metrics: messages/sec (msg/s), bandwidth (MB/s)
    """
    import time

    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo
    except ImportError:
        print("Warning: proto files not generated, skipping performance test.")
        return

    # Message sizes and counts (target ~10MB total data per case)
    test_cases = [
        ("100B",  100,        100_000),
        ("1KB",   1024,       10_000),
        ("100KB", 100 * 1024, 100),
    ]

    writer_config = f"topicName={TOPIC_NAME};functionChain=HASH,hashId2partId"
    reader_config = f"topicName={TOPIC_NAME}"

    print("\n=== Performance Test ===")
    print(f"{'Msg Size':>8}  {'Msg Count':>8}  "
          f"{'Write Time':>10}  {'Write Speed':>12}  {'Write BW':>12}  "
          f"{'Read Time':>10}  {'Read Speed':>12}  {'Read BW':>12}")
    print("-" * 100)

    for label, msg_size, msg_count in test_cases:
        payload = b"x" * msg_size  # Fixed content, focus on throughput

        # ---- Write test ----
        with client.create_writer(writer_config) as writer:
            t0 = time.time()
            for i in range(msg_count):
                m = WriteMessageInfo()
                m.data    = payload
                m.hashStr = str(i).encode()   # Use sequence number as key for even shard distribution
                writer.write(m)
            writer.wait_finished()
            write_elapsed = time.time() - t0

        write_mps = msg_count / write_elapsed
        write_mbps = (msg_count * msg_size) / write_elapsed / 1024 / 1024

        # ---- Read test (re-read all messages from the beginning) ----
        read_count = 0
        with client.create_reader(reader_config) as reader:
            # Simplified: read until timeout/no more messages, count this batch
            t0 = time.time()
            while read_count < msg_count:
                try:
                    reader.read(timeout_us=3_000_000)
                    read_count += 1
                except SwiftException as e:
                    if e.ec in (ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE,
                                ErrorCode.ERROR_BROKER_NO_DATA,
                                ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT,
                                ErrorCode.ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT):
                        break
                    raise
            read_elapsed = time.time() - t0

        read_mps  = read_count / read_elapsed if read_elapsed > 0 else 0
        read_mbps = (read_count * msg_size) / read_elapsed / 1024 / 1024 if read_elapsed > 0 else 0

        print(f"{label:>8}  {msg_count:>8,}  "
              f"{write_elapsed:>9.2f}s  {write_mps:>10,.0f}/s  {write_mbps:>10.2f}MB/s  "
              f"{read_elapsed:>9.2f}s  {read_mps:>10,.0f}/s  {read_mbps:>10.2f}MB/s")

    print("-" * 100)


def demo_field_group():
    """Demonstrate FieldGroupWriter/Reader serialization/deserialization."""
    print("\n=== FieldGroup Serialization Demo ===")

    # Write
    writer = FieldGroupWriter()
    writer.add_field("name", "John Doe", is_updated=False)
    writer.add_field("age", "30", is_updated=True)
    writer.add_field("city", "Beijing", is_updated=False)
    data = writer.to_bytes()
    print(f"Serialized byte count: {len(data)}")

    # Read
    reader = FieldGroupReader()
    reader.from_production_string(data)
    print(f"Field count: {reader.get_field_size()}")
    for i in range(reader.get_field_size()):
        f = reader.get_field(i)
        print(f"  [{i}] name={f.name!r}, value={f.value!r}, is_updated={f.is_updated}")


if __name__ == "__main__":
    # No connection required, demonstrate FieldGroup directly
    demo_field_group()

    # The following demos require a real Swift service
    print(f"\nConnecting to Swift service: {ZK_PATH}")
    with SwiftClient(lib_dir=os.path.abspath(LIB_DIR)) as client:
        client.init(CLIENT_CONFIG)
        demo_admin(client)
        demo_write(client)
        demo_read(client)
        demo_hash_shard(client)
        demo_perf(client)
