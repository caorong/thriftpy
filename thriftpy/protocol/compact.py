# -*- coding: utf-8 -*-

from __future__ import absolute_import

import array
from struct import pack, unpack

from .exc import TProtocolException
from ..thrift import TException
from ..thrift import TType

from thriftpy._compat import PY3


CLEAR = 0
FIELD_WRITE = 1
VALUE_WRITE = 2
CONTAINER_WRITE = 3
BOOL_WRITE = 4
FIELD_READ = 5
CONTAINER_READ = 6
VALUE_READ = 7
BOOL_READ = 8


def check_integer_limits(i, bits):
    if bits == 8 and (i < -128 or i > 127):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i8 requires -128 <= number <= 127")
    elif bits == 16 and (i < -32768 or i > 32767):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i16 requires -32768 <= number <= 32767")
    elif bits == 32 and (i < -2147483648 or i > 2147483647):
        raise TProtocolException(
            TProtocolException.INVALID_DATA,
            "i32 requires -2147483648 <= number <= 2147483647")
    elif bits == 64 and (i < -9223372036854775808 or i > 9223372036854775807):
        raise TProtocolException(
            TProtocolException.INVALID_DATA,
            "i64 requires -9223372036854775808 <= number <= \
                    9223372036854775807")


def make_zig_zag(n, bits):
    check_integer_limits(n, bits)
    return (n << 1) ^ (n >> (bits - 1))


def from_zig_zag(n):
    return (n >> 1) ^ -(n & 1)


def write_varint(trans, n):
    out = []
    while True:
        if n & ~0x7f == 0:
            out.append(n)
            break
        else:
            out.append((n & 0xff) | 0x80)
            n = n >> 7
    data = array.array('B', out).tostring()
    if PY3:
        trans.write(data)
    else:
        trans.write(bytes(data))


def read_varint(trans):
    result = 0
    shift = 0

    while True:
        x = trans.read(1)
        byte = ord(x)
        result |= (byte & 0x7f) << shift
        if byte >> 7 == 0:
            return result
        shift += 7


class CompactType:
    STOP = 0x00
    TRUE = 0x01
    FALSE = 0x02
    BYTE = 0x03
    I16 = 0x04
    I32 = 0x05
    I64 = 0x06
    DOUBLE = 0x07
    BINARY = 0x08
    LIST = 0x09
    SET = 0x0A
    MAP = 0x0B
    STRUCT = 0x0C


CTYPES = {TType.STOP: CompactType.STOP,
          TType.BOOL: CompactType.TRUE,  # used for collection
          TType.BYTE: CompactType.BYTE,
          TType.I16: CompactType.I16,
          TType.I32: CompactType.I32,
          TType.I64: CompactType.I64,
          TType.DOUBLE: CompactType.DOUBLE,
          TType.STRING: CompactType.BINARY,
          TType.STRUCT: CompactType.STRUCT,
          TType.LIST: CompactType.LIST,
          TType.SET: CompactType.SET,
          TType.MAP: CompactType.MAP
          }

TTYPES = {}
for k, v in CTYPES.items():
    TTYPES[v] = k
TTYPES[CompactType.FALSE] = TType.BOOL
del k
del v


class TCompactProtocol(object):
    """Compact implementation of the Thrift protocol driver."""
    PROTOCOL_ID = 0x82
    VERSION = 1
    VERSION_MASK = 0x1f
    TYPE_MASK = 0xe0
    TYPE_BITS = 0x07
    TYPE_SHIFT_AMOUNT = 5

    def __init__(self, trans):
        self.trans = trans
        self.__last_fid = 0
        self.__bool_fid = None
        self.__bool_value = None
        self.__structs = []

    def __getTType(self, byte):
        return TTYPES[byte & 0x0f]

    def __read_size(self):
        result = read_varint(self.trans)
        if result < 0:
            raise TException("Length < 0")
        return result

    def read_message_begin(self):
        proto_id = self.read_ubyte()
        if proto_id != self.PROTOCOL_ID:
            raise TProtocolException(TProtocolException.BAD_VERSION,
                                     'Bad protocol id in the message: %d'
                                     % proto_id)

        ver_type = self.read_ubyte()
        type = (ver_type >> self.TYPE_SHIFT_AMOUNT) & self.TYPE_BITS
        version = ver_type & self.VERSION_MASK
        if version != self.VERSION:
            raise TProtocolException(TProtocolException.BAD_VERSION,
                                     'Bad version: %d (expect %d)'
                                     % (version, self.VERSION))
        seqid = read_varint(self.trans)
        name = self.read_string()
        return (name, type, seqid)

    def read_message_end(self):
        assert len(self.__structs) == 0

    def read_field_begin(self):
        type = self.read_ubyte()
        if type & 0x0f == TType.STOP:
            return (None, 0, 0)
        delta = type >> 4
        if delta == 0:
            fid = from_zig_zag(read_varint(self.trans))
        else:
            fid = self.__last_fid + delta
        self.__last_fid = fid
        type = type & 0x0f
        if type == CompactType.TRUE:
            self.__bool_value = True
        elif type == CompactType.FALSE:
            self.__bool_value = False
        else:
            pass
        return (None, self.__getTType(type), fid)

    def read_field_end(self):
        pass

    def read_struct_begin(self):
        self.__structs.append(self.__last_fid)
        self.__last_fid = 0

    def read_struct_end(self):
        self.__last_fid = self.__structs.pop()

    def read_map_begin(self):
        size = self.__read_size()
        types = 0
        if size > 0:
            types = self.read_ubyte()
        vtype = self.__getTType(types)
        ktype = self.__getTType(types >> 4)
        return (ktype, vtype, size)

    def read_collection_begin(self):
        size_type = self.read_ubyte()
        size = size_type >> 4
        type = self.__getTType(size_type)
        if size == 15:
            size = self.__read_size()
        return type, size

    def read_collection_end(self):
        pass

    def read_byte(self):
        result, = unpack('!b', self.trans.read(1))
        return result

    def read_ubyte(self):
        result, = unpack('!B', self.trans.read(1))
        return result

    def read_int(self):
        return from_zig_zag(read_varint(self.trans))

    def read_double(self):
        buff = self.trans.read(8)
        val, = unpack('<d', buff)
        return val

    def read_string(self):
        len = self.__read_size()

        byte_payload = self.trans.read(len)
        try:
            return byte_payload.decode('utf-8')
        except UnicodeDecodeError:
            return byte_payload

    def read_bool(self):
        if self.__bool_value is not None:
            result = self.__bool_value
            self.__bool_value = None
            return result
        return self.read_byte() == CompactType.TRUE

    def read_struct(self, obj):
        self.read_struct_begin()
        while True:
            (fname, ftype, fid) = self.read_field_begin()
            if ftype == TType.STOP:
                break

            if fid not in obj.thrift_spec:
                self.skip(ftype)
                continue

            try:
                field = obj.thrift_spec[fid]
            except IndexError:
                self.skip(ftype)
                raise
            else:
                if field is not None and ftype == field[0]:
                    fname = field[1]
                    fspec = field[2]
                    val = self.read_val(ftype, fspec)
                    setattr(obj, fname, val)
                else:
                    self.skip(ftype)
            self.read_field_end()
        self.read_struct_end()

    def read_val(self, ttype, spec=None):
        if ttype == TType.BOOL:
            return self.read_bool()

        elif ttype == TType.BYTE:
            return self.read_byte()

        elif ttype == TType.I16 or ttype == TType.I32 or ttype == TType.I64:
            return self.read_int()

        elif ttype == TType.DOUBLE:
            return self.read_double()

        elif ttype == TType.STRING:
            return self.read_string()

        elif ttype == TType.LIST or ttype == TType.SET:
            if isinstance(spec, tuple):
                v_type, v_spec = spec[0], spec[1]
            else:
                v_type, v_spec = spec, None
            result = []
            r_type, sz = self.read_collection_begin()

            for i in range(sz):
                result.append(self.read_val(v_type, v_spec))

            self.read_collection_end()
            return result

        elif ttype == TType.MAP:
            if isinstance(spec[0], int):
                k_type = spec[0]
                k_spec = None
            else:
                k_type, k_spec = spec[0]

            if isinstance(spec[1], int):
                v_type = spec[1]
                v_spec = None
            else:
                v_type, v_spec = spec[1]

            result = {}
            sk_type, sv_type, sz = self.read_map_begin()
            if sk_type != k_type or sv_type != v_type:
                for _ in range(sz):
                    self.skip(sk_type)
                    self.skip(sv_type)
                self.read_collection_end()
                return {}

            for i in range(sz):
                k_val = self.read_val(k_type, k_spec)
                v_val = self.read_val(v_type, v_spec)
                result[k_val] = v_val
            self.read_collection_end()
            return result

        elif ttype == TType.STRUCT:
            obj = spec()
            self.read_struct(obj)
            return obj

    def __write_size(self, i32):
        write_varint(self.trans, i32)

    def __write_field_header(self, type, fid):
        delta = fid - self.__last_fid
        if 0 < delta <= 15:
            self.write_ubyte(delta << 4 | type)
        else:
            self.write_byte(type)
            self.write_i16(fid)
        self.__last_fid = fid

    def write_message_begin(self, name, type, seqid):
        self.write_ubyte(self.PROTOCOL_ID)
        self.write_ubyte(self.VERSION | (type << self.TYPE_SHIFT_AMOUNT))
        write_varint(self.trans, seqid)
        self.write_string(name)

    def write_message_end(self):
        pass

    def write_field_stop(self):
        self.write_byte(0)

    def write_field_begin(self, name, type, fid):
        if type == TType.BOOL:
            self.__bool_fid = fid
        else:
            self.__write_field_header(CTYPES[type], fid)

    def write_field_end(self):
        pass

    def write_struct_begin(self, name):
        self.__structs.append(self.__last_fid)
        self.__last_fid = 0

    def write_struct_end(self):
        self.__last_fid = self.__structs.pop()

    def write_collection_begin(self, etype, size):
        if size <= 14:
            self.write_ubyte(size << 4 | CTYPES[etype])
        else:
            self.write_ubyte(0xf0 | CTYPES[etype])
            self.__write_size(size)

    def write_map_begin(self, ktype, vtype, size):
        if size == 0:
            self.write_byte(0)
        else:
            self.__write_size(size)
            self.write_ubyte(CTYPES[ktype] << 4 | CTYPES[vtype])

    def write_collection_end(self):
        pass

    def write_ubyte(self, byte):
        self.trans.write(pack('!B', byte))

    def write_byte(self, byte):
        self.trans.write(pack('!b', byte))

    def write_bool(self, bool):
        if self.__bool_fid and self.__bool_fid > self.__last_fid \
                and self.__bool_fid - self.__last_fid <= 15:
            if bool:
                ctype = CompactType.TRUE
            else:
                ctype = CompactType.FALSE
            self.__write_field_header(ctype, self.__bool_fid)
        else:
            if bool:
                self.write_byte(CompactType.TRUE)
            else:
                self.write_byte(CompactType.FALSE)

    def write_i16(self, i16):
        write_varint(self.trans, make_zig_zag(i16, 16))

    def write_i32(self, i32):
        write_varint(self.trans, make_zig_zag(i32, 32))

    def write_i64(self, i64):
        write_varint(self.trans, make_zig_zag(i64, 64))

    def write_double(self, dub):
        self.trans.write(pack('<d', dub))

    def write_string(self, s):
        if PY3:
            self.__write_size(len(bytearray(s, 'utf-8')))
        else:
            self.__write_size(len(s))
        if not isinstance(s, bytes):
            s = s.encode('utf-8')
        self.trans.write(s)

    def write_struct(self, obj):
        self.write_struct_begin(obj.__class__.__name__)

        for field in iter(obj.thrift_spec):
            if field is None:
                continue
            fspec = obj.thrift_spec[field]
            if len(fspec) == 3:
                ftype, fname, freq = fspec
                f_container_spec = None
            else:
                ftype, fname, f_container_spec, f_req = fspec
            val = getattr(obj, fname)
            if val is None:
                continue

            self.write_field_begin(fname, ftype, field)
            self.write_val(ftype, val, f_container_spec)
            self.write_field_end()
        self.write_field_stop()
        self.write_struct_end()

    def write_val(self, ttype, val, spec=None):

        if ttype == TType.BOOL:
            self.write_bool(val)

        elif ttype == TType.BYTE:
            self.write_byte(val)

        elif ttype == TType.I16:
            self.write_i16(val)

        elif ttype == TType.I32:
            self.write_i32(val)

        elif ttype == TType.I64:
            self.write_i64(val)

        elif ttype == TType.DOUBLE:
            self.write_double(val)

        elif ttype == TType.STRING:
            self.write_string(val)

        elif ttype == TType.LIST or ttype == TType.SET:
            if isinstance(spec, tuple):
                e_type, t_spec = spec[0], spec[1]
            else:
                e_type, t_spec = spec, None

            val_len = len(val)
            self.write_collection_begin(e_type, val_len)
            for e_val in val:
                self.write_val(e_type, e_val, t_spec)
            self.write_collection_end()

        elif ttype == TType.MAP:
            if isinstance(spec[0], int):
                k_type = spec[0]
                k_spec = None
            else:
                k_type, k_spec = spec[0]

            if isinstance(spec[1], int):
                v_type = spec[1]
                v_spec = None
            else:
                v_type, v_spec = spec[1]

            self.write_map_begin(k_type, v_type, len(val))
            for k in iter(val):
                self.write_val(k_type, k, k_spec)
                self.write_val(v_type, val[k], v_spec)
            self.write_collection_end()

        elif ttype == TType.STRUCT:
            self.write_struct(val)

    def skip(self, ttype):
        if ttype == TType.STOP:
            return

        elif ttype == TType.BOOL:
            self.read_bool()

        elif ttype == TType.BYTE:
            self.read_byte()

        elif ttype == TType.I16:
            from_zig_zag(read_varint(self.trans))

        elif ttype == TType.I32:
            from_zig_zag(read_varint(self.trans))

        elif ttype == TType.I64:
            from_zig_zag(read_varint(self.trans))

        elif ttype == TType.DOUBLE:
            self.read_double()

        elif ttype == TType.STRING:
            self.read_string()

        elif ttype == TType.STRUCT:
            name = self.read_struct_begin()
            while True:
                (name, ttype, id) = self.read_field_begin()
                if ttype == TType.STOP:
                    break
                self.skip(ttype)
                self.read_field_end()
            self.read_struct_end()

        elif ttype == TType.MAP:
            (ktype, vtype, size) = self.read_map_begin()
            for i in range(size):
                self.skip(ktype)
                self.skip(vtype)
            self.read_collection_end()

        elif ttype == TType.SET:
            (etype, size) = self.read_collection_begin()
            for i in range(size):
                self.skip(etype)
            self.read_collection_end()

        elif ttype == TType.LIST:
            (etype, size) = self.read_collection_begin()
            for i in range(size):
                self.skip(etype)
            self.read_collection_end()


class TCompactProtocolFactory:
    def __init__(self):
        pass

    def get_protocol(self, trans):
        return TCompactProtocol(trans)
