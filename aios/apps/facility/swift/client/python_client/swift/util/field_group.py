"""
FieldGroup serialization/deserialization utilities.

Corresponds to FieldGroupWriter.java and FieldGroupReader.java on the Java side.

Swift uses a custom binary format to encode message fields:
  - Production format: name_len(varint) + name + value_len(varint) + value + is_updated(1byte)
  - Consumption format: is_existed(1byte) + [value_len(varint) + value + is_updated(1byte)]

Varint encoding is identical to protobuf (7 bits per group, MSB as continuation bit).
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
#  Varint Encoding/Decoding                                            #
# ------------------------------------------------------------------ #

def _write_varint32(buf: BytesIO, value: int):
    """Write an unsigned int to the buffer in varint format (consistent with protobuf encoding)."""
    value = value & 0xFFFFFFFF  # Treat as unsigned 32-bit
    while True:
        if (value & ~0x7F) == 0:
            buf.write(bytes([value]))
            return
        buf.write(bytes([(value & 0x7F) | 0x80]))
        value >>= 7


def _read_varint32(data: bytes, pos: int) -> (int, int):
    """
    Read a varint32 from data[pos:], returns (value, new_pos).
    Reads at most 5 bytes (32-bit varint).
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
    Serialize a list of fields into Swift production message format.

    Example:
        writer = FieldGroupWriter()
        writer.add_field("title", "hello world", is_updated=True)
        writer.add_field("id", "123")
        data = writer.to_bytes()
    """

    def __init__(self):
        self._buf = BytesIO()

    def add_field(self, name: str, value: str, is_updated: bool = False):
        """
        Add a field.

        :param name:       Field name (UTF-8 string)
        :param value:      Field value (UTF-8 string)
        :param is_updated: Whether this is an updated field
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
        Add a field (byte array version, suitable for non-UTF-8 values).
        """
        _write_varint32(self._buf, len(name))
        self._buf.write(name)
        _write_varint32(self._buf, len(value))
        self._buf.write(value)
        self._buf.write(bytes([1 if is_updated else 0]))

    def to_bytes(self) -> bytes:
        """Return the serialized byte array."""
        return self._buf.getvalue()

    def reset(self):
        """Clear the buffer and start writing from scratch."""
        self._buf = BytesIO()


# ------------------------------------------------------------------ #
#  FieldGroupReader                                                    #
# ------------------------------------------------------------------ #

class FieldGroupReader:
    """
    Parse Swift message bytes into a list of fields.

    Supports two formats:
      - Production format: messages written by the producer (from_production_string)
      - Consumption format: messages read by the consumer (from_consumption_string)

    Example:
        reader = FieldGroupReader()
        reader.from_production_string(data_bytes)
        for field in reader.fields:
            print(field.name, field.value, field.is_updated)
    """

    def __init__(self):
        self.fields: List[Field] = []

    def from_production_string(self, data: bytes) -> bool:
        """
        Parse production message format.

        Format: [name_len(varint) name value_len(varint) value is_updated(1byte)] * N

        :param data: Raw bytes
        :return: True on successful parsing
        :raises ValueError: On data format error
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
        Parse consumption message format.

        Format: [is_existed(1byte) [value_len(varint) value is_updated(1byte)]] * N

        :param data: Raw bytes
        :return: True on successful parsing
        :raises ValueError: On data format error
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
