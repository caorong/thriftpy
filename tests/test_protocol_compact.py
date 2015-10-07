# -*- coding: utf-8 -*-

from io import BytesIO

from thriftpy._compat import u
from thriftpy.thrift import TType, TPayload
from thriftpy.utils import hexlify
from thriftpy.protocol import compact


class TItem(TPayload):
    thrift_spec = {
        1: (TType.I32, "id", False),
        2: (TType.LIST, "phones", (TType.STRING), False),
    }
    default_spec = [("id", None), ("phones", None)]


def gen_proto(bytearray=b''):
    b = BytesIO(bytearray)
    proto = compact.TCompactProtocol(b)
    return (b, proto)


def test_pack_byte():
    b, proto = gen_proto()
    proto.write_val(TType.BYTE, 77)
    assert "4d" == hexlify(b.getvalue())


def test_unpack_byte():
    b, proto = gen_proto(b'\x4d')
    assert 77 == proto.read_val(TType.BYTE)


def test_pack_i16():
    b, proto = gen_proto()
    proto.write_val(TType.I16, 12345)
    assert "f2 c0 01" == hexlify(b.getvalue())


def test_unpack_i16():
    b, proto = gen_proto(b"\xf2\xc0\x01")
    assert 12345 == proto.read_val(TType.I16)


def test_pack_i32():
    b, proto = gen_proto()
    proto.write_val(TType.I32, 1234567890)
    assert "a4 8b b0 99 09" == hexlify(b.getvalue())


def test_unpack_i32():
    b, proto = gen_proto(b"\xa4\x8b\xb0\x99\x09")
    assert 1234567890 == proto.read_val(TType.I32)


def test_pack_i64():
    b, proto = gen_proto()
    proto.write_val(TType.I64, 1234567890123456789)
    assert "aa 84 cc de 8f bd 88 a2 22" == hexlify(b.getvalue())


def test_unpack_i64():
    b, proto = gen_proto(b"\xaa\x84\xcc\xde\x8f\xbd\x88\xa2\x22")
    assert 1234567890123456789 == proto.read_val(TType.I64)


def test_pack_double():
    b, proto = gen_proto()
    proto.write_val(TType.DOUBLE, 1234567890.1234567890)
    assert "b7 e6 87 b4 80 65 d2 41" == hexlify(b.getvalue())


def test_unpack_double():
    b, proto = gen_proto(b"\xb7\xe6\x87\xb4\x80\x65\xd2\x41")
    assert 1234567890.1234567890 == proto.read_val(TType.DOUBLE)


def test_pack_string():
    b, proto = gen_proto()
    proto.write_val(TType.STRING, "hello world!")
    assert "0c 68 65 6c 6c 6f 20 77 6f 72 6c 64 21" == \
           hexlify(b.getvalue())

    b1, proto1 = gen_proto()
    proto1.write_val(TType.STRING, "你好世界")
    assert "0c e4 bd a0 e5 a5 bd e4 b8 96 e7 95 8c" == \
           hexlify(b1.getvalue())


def test_unpack_string():
    b, proto = gen_proto(b'\x0c\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64\x21')
    assert u('hello world!') == proto.read_val(TType.STRING)

    b, proto = gen_proto(b'\x0c\xe4\xbd\xa0\xe5\xa5\xbd\xe4\xb8\x96\xe7\x95\x8c')
    assert u('你好世界') == proto.read_val(TType.STRING)


def test_write_message_begin():
    b, proto = gen_proto()
    proto.write_message_begin("test", 2, 1)
    assert "82 41 01 04 74 65 73 74" == \
           hexlify(b.getvalue())


def test_read_message_begin():
    b, proto = gen_proto(b"\x82\x41\x01\x04\x74\x65\x73\x74")
    res = proto.read_message_begin()
    assert res == ("test", 2, 1)


def test_write_struct():
    b, proto = gen_proto()
    item = TItem(id=123, phones=["123456", "abcdef"])
    proto.write_struct(item)
    assert ("15 f6 01 19 28 06 31 32 33 34 "
            "35 36 06 61 62 63 64 65 66 00" == hexlify(b.getvalue()))


def test_read_struct():
    b, proto = gen_proto(b"\x15\xf6\x01\x19\x28\x06\x31\x32\x33\x34\x35\x36\x06\x61\x62\x63\x64\x65\x66\x00")
    _item = TItem(id=123, phones=["123456", "abcdef"])
    _item2 = TItem()
    proto.read_struct(_item2)
    assert _item == _item2


def test_write_empty_struct():
    b, proto = gen_proto()
    item = TItem()
    proto.write_struct(item)
    assert "00" == hexlify(b.getvalue())


def test_read_empty_struct():
    b, proto = gen_proto(b"\x00")
    _item = TItem()
    _item2 = TItem()
    proto.read_struct(_item2)
    assert _item == _item2


def test_write_huge_struct():
    b, proto = gen_proto()
    item = TItem(id=12345, phones=["1234567890"] * 100000)
    proto.write_struct(item)


if __name__ == '__main__':
    test_pack_byte()
    test_unpack_byte()
    test_pack_i16()
    test_unpack_i16()
    test_pack_i32()
    test_unpack_i32()
    test_pack_i64()
    test_unpack_i64()
    test_pack_double()
    test_unpack_double()
    test_pack_string()
    test_unpack_string()
    test_write_message_begin()
    test_read_message_begin()
    test_write_struct()
    test_read_struct()
    test_write_empty_struct()
    test_read_empty_struct()
    test_write_huge_struct()
