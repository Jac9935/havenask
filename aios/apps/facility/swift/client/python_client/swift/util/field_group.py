"""
FieldGroup 序列化/反序列化工具。

对应 Java 端的 FieldGroupWriter.java 和 FieldGroupReader.java。

Swift 使用自定义二进制格式对消息字段进行编码：
  - 生产消息格式（Production）：name_len(varint) + name + value_len(varint) + value + is_updated(1byte)
  - 消费消息格式（Consumption）：is_existed(1byte) + [value_len(varint) + value + is_updated(1byte)]

Varint 编码与 protobuf 相同（7 位一组，最高位为延续位）。
"""

from io import BytesIO
from typing import List, Optional


class Field:
    def __init__(self):
        self.name = ""
        self.value = ""
        self.is_updated = False
        self.is_existed = False

    def __repr__(self):
        return (
            f"Field(name={self.name!r}, value={self.value!r}, "
            f"is_updated={self.is_updated}, is_existed={self.is_existed})"
        )


# ------------------------------------------------------------------ #
#  Varint 编解码                                                       #
# ------------------------------------------------------------------ #

def _write_varint32(buf: BytesIO, value: int):
    """将无符号 int 以 varint 格式写入缓冲区（与 protobuf 编码一致）。"""
    value = value & 0xFFFFFFFF  # 视为无符号 32 位
    while True:
        if (value & ~0x7F) == 0:
            buf.write(bytes([value]))
            return
        buf.write(bytes([(value & 0x7F) | 0x80]))
        value >>= 7


def _read_varint32(data: bytes, pos: int) -> (int, int):
    """
    从 data[pos:] 读取一个 varint32，返回 (value, new_pos)。
    最多读取 5 个字节（32 位 varint）。
    """
    result = 0
    shift = 0
    for i in range(5):
        if pos >= len(data):
            raise ValueError(f"Unexpected end of data at position {pos}")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if (b & 0x80) == 0:
            return result, pos
    raise ValueError("Varint too long (> 5 bytes for int32)")


# ------------------------------------------------------------------ #
#  FieldGroupWriter                                                    #
# ------------------------------------------------------------------ #

class FieldGroupWriter:
    """
    将字段列表序列化为 Swift 生产消息格式（Production format）。

    使用示例：
        writer = FieldGroupWriter()
        writer.add_field("title", "hello world", is_updated=True)
        writer.add_field("id", "123")
        data = writer.to_bytes()
    """

    def __init__(self):
        self._buf = BytesIO()

    def add_field(self, name: str, value: str, is_updated: bool = False):
        """
        添加一个字段。

        :param name:       字段名（UTF-8 字符串）
        :param value:      字段值（UTF-8 字符串）
        :param is_updated: 是否为更新字段
        """
        name_bytes = name.encode("utf-8")
        value_bytes = value.encode("utf-8")
        _write_varint32(self._buf, len(name_bytes))
        self._buf.write(name_bytes)
        _write_varint32(self._buf, len(value_bytes))
        self._buf.write(value_bytes)
        self._buf.write(bytes([1 if is_updated else 0]))

    def add_field_bytes(self, name: bytes, value: bytes, is_updated: bool = False):
        """
        添加一个字段（字节数组版本，适用于非 UTF-8 值）。
        """
        _write_varint32(self._buf, len(name))
        self._buf.write(name)
        _write_varint32(self._buf, len(value))
        self._buf.write(value)
        self._buf.write(bytes([1 if is_updated else 0]))

    def to_bytes(self) -> bytes:
        """返回序列化后的字节数组。"""
        return self._buf.getvalue()

    def reset(self):
        """清空缓冲区，重新开始写入。"""
        self._buf = BytesIO()


# ------------------------------------------------------------------ #
#  FieldGroupReader                                                    #
# ------------------------------------------------------------------ #

class FieldGroupReader:
    """
    将 Swift 消息字节解析为字段列表。

    支持两种格式：
      - Production 格式：用于生产端写入的消息（from_production_string）
      - Consumption 格式：用于消费端读取的消息（from_consumption_string）

    使用示例：
        reader = FieldGroupReader()
        reader.from_production_string(data_bytes)
        for field in reader.fields:
            print(field.name, field.value, field.is_updated)
    """

    def __init__(self):
        self.fields: List[Field] = []

    def from_production_string(self, data: bytes) -> bool:
        """
        解析生产消息格式。

        格式：[name_len(varint) name value_len(varint) value is_updated(1byte)] * N

        :param data: 原始字节
        :return: True 表示解析成功
        :raises ValueError: 数据格式错误
        """
        self.fields = []
        pos = 0
        while pos < len(data):
            f = Field()
            name_len, pos = _read_varint32(data, pos)
            f.name = data[pos:pos + name_len].decode("utf-8")
            pos += name_len

            value_len, pos = _read_varint32(data, pos)
            f.value = data[pos:pos + value_len].decode("utf-8")
            pos += value_len

            if pos >= len(data):
                raise ValueError("Unexpected end of data while reading is_updated")
            f.is_updated = data[pos] != 0
            pos += 1

            self.fields.append(f)
        return True

    def from_consumption_string(self, data: bytes) -> bool:
        """
        解析消费消息格式。

        格式：[is_existed(1byte) [value_len(varint) value is_updated(1byte)]] * N

        :param data: 原始字节
        :return: True 表示解析成功
        :raises ValueError: 数据格式错误
        """
        self.fields = []
        pos = 0
        while pos < len(data):
            f = Field()
            if pos >= len(data):
                raise ValueError("Unexpected end of data while reading is_existed")
            f.is_existed = data[pos] != 0
            pos += 1

            if f.is_existed:
                value_len, pos = _read_varint32(data, pos)
                f.value = data[pos:pos + value_len].decode("utf-8")
                pos += value_len

                if pos >= len(data):
                    raise ValueError("Unexpected end of data while reading is_updated")
                f.is_updated = data[pos] != 0
                pos += 1

            self.fields.append(f)
        return True

    def get_field_size(self) -> int:
        return len(self.fields)

    def get_field(self, index: int) -> Field:
        return self.fields[index]
