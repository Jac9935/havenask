"""
Swift Python Client 使用示例

运行前准备：
  1. 执行 ./generate_proto.sh 生成 proto 绑定文件
  2. 确保 .so 文件路径正确（lib_dir 参数）
  3. 配置正确的 ZooKeeper 地址

用法：
  python example.py
"""

import sys
import os

# 添加包路径
sys.path.insert(0, os.path.dirname(__file__))

from swift import SwiftClient, SwiftException, ErrorCode
from swift.util import FieldGroupWriter, FieldGroupReader

# ---- 配置区 ----
ZK_PATH = "zfs://x.x.x.x:2181/havenask/swift-local/appmaster"
TOPIC_NAME = "test"
LIB_DIR = os.path.join(os.path.dirname(__file__), "../java_client/src/main/resources/linux-x86-64")

_SO_DIR = os.path.abspath(LIB_DIR)
CLIENT_CONFIG = f"zkPath={ZK_PATH};logConfigFile={_SO_DIR}/swift_alog.conf;useFollowerAdmin=false"
WRITER_CONFIG = f"topicName={TOPIC_NAME}"
READER_CONFIG = f"topicName={TOPIC_NAME}"


def demo_admin(client: SwiftClient):
    """演示管理操作：创建 Topic、查询分区数等。"""
    print("\n=== Admin 操作 ===")
    admin = client.get_admin_adapter()

    # 查询所有 Topic
    try:
        resp = admin.get_all_topic_info()
        print(f"所有 Topic 数量: {len(resp.allTopicInfo)}")
    except SwiftException as e:
        print(f"查询所有 Topic 失败: {e}")

    # 查询指定 Topic 分区数
    try:
        count = admin.get_partition_count(TOPIC_NAME)
        print(f"Topic [{TOPIC_NAME}] 分区数: {count}")
    except SwiftException as e:
        if e.ec == ErrorCode.ERROR_ADMIN_TOPIC_NOT_EXISTED:
            print(f"Topic [{TOPIC_NAME}] 不存在，跳过查询")
        else:
            print(f"查询分区数失败: {e}")

    admin.close()


def demo_write(client: SwiftClient):
    """演示写入消息（使用 FieldGroupWriter 构造 data）。"""
    print("\n=== 写入消息 ===")
    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo, WriteMessageInfoVec
    except ImportError:
        print("警告: proto 文件未生成，跳过写入演示。请先执行 ./generate_proto.sh")
        return

    with client.create_writer(WRITER_CONFIG) as writer:
        # 使用 FieldGroupWriter 构造结构化 data
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
        print(f"消息已写入，等待完成...")
        writer.wait_finished()

        checkpoint = writer.get_committed_checkpoint_id()
        print(f"已提交 checkpoint ID: {checkpoint}")

        # 批量写入示例
        msgs = WriteMessageInfoVec()
        for i in range(5):
            m = WriteMessageInfo(data=f"batch message {i}".encode())
            msgs.messageInfoVec.append(m)
        writer.write_batch(msgs, wait_sent=True)
        print(f"批量写入 5 条消息完成")


def demo_read(client: SwiftClient):
    """演示读取消息（读取最多 10 条，超时后退出）。"""
    print("\n=== 读取消息 ===")
    try:
        from swift.proto.SwiftMessage_pb2 import Message
    except ImportError:
        print("警告: proto 文件未生成，跳过读取演示。请先执行 ./generate_proto.sh")
        return

    with client.create_reader(READER_CONFIG) as reader:
        # 从最新位置开始读取（实际需要在 READER_CONFIG 中设置 readFromOffset）
        count = 0
        while count < 10:
            try:
                ts, msg = reader.read(timeout_us=3_000_000)
                print(f"消息 #{msg.msgId} | ts={ts} | data={msg.data[:50]!r}")

                # 如果 data 是 FieldGroup 格式，可以解析
                try:
                    fg_reader = FieldGroupReader()
                    fg_reader.from_production_string(msg.data)
                    for f in fg_reader.fields:
                        print(f"  字段: {f.name} = {f.value} (updated={f.is_updated})")
                except Exception:
                    pass  # 非 FieldGroup 格式，忽略

                count += 1
            except SwiftException as e:
                if e.ec in (ErrorCode.ERROR_BROKER_NO_DATA,
                             ErrorCode.ERROR_CLIENT_READ_MESSAGE_TIMEOUT,
                             ErrorCode.ERROR_CLIENT_NO_MORE_MESSAGE):
                    print("暂无新消息，退出读取循环")
                    break
                raise

        # 查询分区状态
        refresh_time, max_id, max_ts = reader.get_partition_status()
        print(f"\n分区状态: refreshTime={refresh_time}, maxMsgId={max_id}, maxMsgTs={max_ts}")


def demo_hash_shard(client: SwiftClient):
    """演示按主键 hash 分片写入，并逐 shard 读取验证分布。"""
    print("\n=== Hash 分片写入与验证 ===")
    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo
    except ImportError:
        print("警告: proto 文件未生成，跳过演示。")
        return

    # 查询分区数
    admin = client.get_admin_adapter()
    partition_count = admin.get_partition_count(TOPIC_NAME)
    admin.close()
    print(f"Topic [{TOPIC_NAME}] 分区数: {partition_count}")

    # 测试主键列表
    primary_keys = [
        "user_001", "user_002", "user_003",
        "order_100", "order_200", "order_300",
        "item_aaa", "item_bbb", "item_ccc",
        "doc_xyz",  "doc_abc",  "doc_123",
    ]

    # 1. 按主键写入，functionChain=HASH,hashId2partId 自动路由 shard
    writer_config = f"topicName={TOPIC_NAME};functionChain=HASH,hashId2partId"
    print(f"\n--- 写入 {len(primary_keys)} 条消息（{writer_config}）---")
    with client.create_writer(writer_config) as writer:
        for pk in primary_keys:
            msg = WriteMessageInfo()
            msg.data    = f"data_for_{pk}".encode("utf-8")
            msg.hashStr = pk.encode("utf-8")   # 主键作为 hash 分片依据
            writer.write(msg)
            print(f"  写入: pk={pk!r:15s}  data={msg.data.decode()!r}")
        writer.wait_finished()
        print(f"已提交 checkpoint ID: {writer.get_committed_checkpoint_id()}")

    # 2. 逐 shard 读取，验证消息分布
    print(f"\n--- 逐 shard 读取（共 {partition_count} 个 shard）---")
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
            print(f"  shard {part_id}: reader 创建失败 -> {e}")
            continue

        shard_counts[part_id] = len(msgs_in_shard)
        print(f"\n  Shard {part_id}（{len(msgs_in_shard)} 条）:")
        for msg in msgs_in_shard:
            print(f"    msgId={msg.msgId:<6d} data={msg.data.decode()!r}")

    print(f"\n--- 分片分布汇总 ---")
    for part_id in range(partition_count):
        count = shard_counts.get(part_id, 0)
        print(f"  Shard {part_id}: {'█' * count} {count} 条")


def demo_perf(client: SwiftClient):
    """
    性能测试：分别测试 100B / 1KB / 100KB 消息的写入和读取吞吐量。

    测试方法：
      - 写入：使用 functionChain=HASH,hashId2partId 均匀分散到各 shard
      - 读取：从 topic 头部重新读取所有消息
      - 指标：消息数/秒（msg/s）、带宽（MB/s）
    """
    import time

    try:
        from swift.proto.SwiftMessage_pb2 import WriteMessageInfo
    except ImportError:
        print("警告: proto 文件未生成，跳过性能测试。")
        return

    # 各消息大小对应的发送条数（控制总数据量约 10MB）
    test_cases = [
        ("100B",  100,        100_000),
        ("1KB",   1024,       10_000),
        ("100KB", 100 * 1024, 100),
    ]

    writer_config = f"topicName={TOPIC_NAME};functionChain=HASH,hashId2partId"
    reader_config = f"topicName={TOPIC_NAME}"

    print("\n=== 性能测试 ===")
    print(f"{'消息大小':>8}  {'消息条数':>8}  "
          f"{'写入耗时':>10}  {'写入速度':>12}  {'写入带宽':>12}  "
          f"{'读取耗时':>10}  {'读取速度':>12}  {'读取带宽':>12}")
    print("-" * 100)

    for label, msg_size, msg_count in test_cases:
        payload = b"x" * msg_size  # 固定内容，聚焦于吞吐

        # ---- 写入测试 ----
        with client.create_writer(writer_config) as writer:
            t0 = time.time()
            for i in range(msg_count):
                m = WriteMessageInfo()
                m.data    = payload
                m.hashStr = str(i).encode()   # 以序号为主键，均匀分散到各 shard
                writer.write(m)
            writer.wait_finished()
            write_elapsed = time.time() - t0

        write_mps = msg_count / write_elapsed
        write_mbps = (msg_count * msg_size) / write_elapsed / 1024 / 1024

        # ---- 读取测试（从头读回全部消息）----
        read_count = 0
        with client.create_reader(reader_config) as reader:
            # seek 到本次写入前的位置（从当前 topic 末尾往前 msg_count 条）
            # 简化处理：直接读直到超时/无更多消息，计量本批次消息数
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
    """演示 FieldGroupWriter/Reader 的序列化/反序列化。"""
    print("\n=== FieldGroup 序列化演示 ===")

    # 写入
    writer = FieldGroupWriter()
    writer.add_field("name", "张三", is_updated=False)
    writer.add_field("age", "30", is_updated=True)
    writer.add_field("city", "北京", is_updated=False)
    data = writer.to_bytes()
    print(f"序列化后字节数: {len(data)}")

    # 读取
    reader = FieldGroupReader()
    reader.from_production_string(data)
    print(f"字段数量: {reader.get_field_size()}")
    for i in range(reader.get_field_size()):
        f = reader.get_field(i)
        print(f"  [{i}] name={f.name!r}, value={f.value!r}, is_updated={f.is_updated}")


if __name__ == "__main__":
    # 无需连接服务，直接演示 FieldGroup
    demo_field_group()

    # 以下演示需要真实的 Swift 服务
    print(f"\n连接 Swift 服务: {ZK_PATH}")
    with SwiftClient(lib_dir=os.path.abspath(LIB_DIR)) as client:
        client.init(CLIENT_CONFIG)
        demo_admin(client)
        demo_write(client)
        demo_read(client)
        demo_hash_shard(client)
        demo_perf(client)
