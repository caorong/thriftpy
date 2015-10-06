# -*- coding: utf-8 -*-

from __future__ import absolute_import

import array
from struct import pack, unpack

from .exc import TProtocolException
from ..thrift import TException
from thriftpy._compat import PY3

__all__ = ['TCompactProtocol', 'TCompactProtocolFactory']

CLEAR = 0
FIELD_WRITE = 1
VALUE_WRITE = 2
CONTAINER_WRITE = 3
BOOL_WRITE = 4
FIELD_READ = 5
CONTAINER_READ = 6
VALUE_READ = 7
BOOL_READ = 8


def make_helper(v_from, container):
    def helper(func):
        def nested(self, *args, **kwargs):
            # assert self.state in (v_from, container), \
            # (self.state, v_from, container)
            return func(self, *args, **kwargs)

        return nested

    return helper


writer = make_helper(VALUE_WRITE, CONTAINER_WRITE)
reader = make_helper(VALUE_READ, CONTAINER_READ)


def checkIntegerLimits(i, bits):
    if bits == 8 and (i < -128 or i > 127):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i8 requires -128 <= number <= 127")
    elif bits == 16 and (i < -32768 or i > 32767):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i16 requires -32768 <= number <= 32767")
    elif bits == 32 and (i < -2147483648 or i > 2147483647):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i32 requires -2147483648 <= number <= 2147483647")
    elif bits == 64 and (i < -9223372036854775808 or i > 9223372036854775807):
        raise TProtocolException(TProtocolException.INVALID_DATA,
                                 "i64 requires -9223372036854775808 <= number <= 9223372036854775807")


def makeZigZag(n, bits):
    checkIntegerLimits(n, bits)
    return (n << 1) ^ (n >> (bits - 1))


def fromZigZag(n):
    return (n >> 1) ^ -(n & 1)


def writeVarint(trans, n):
    out = []
    while True:
        if n & ~0x7f == 0:
            out.append(n)
            break
        else:
            out.append((n & 0xff) | 0x80)
            n = n >> 7
    # trans.write(bytes(''.join(map(chr, out)), encoding="UTF-8"))
    # print(bytes(''.join(map(chr, out))))
    # print(type(bytes(''.join(map(chr, out)))))
    # print('out  -------> ', out)
    # data = ''.join(map(chr, out))
    # data = str(bytearray(out))
    data = array.array('B', out).tostring()
    # print('data -------> ', data)
    # print('data -------> type ', type(data))
    # print('data  -=-=-= ->', data.encode('UTF-8'))
    # if isinstance(data, unicode):
    #     data = data.encode("utf-8")
    if PY3:
        # trans.write(data.encode('UTF-8'))
        trans.write(data)
    else:
        trans.write(bytes(data))
        # trans.write(bytes(''.join(map(chr, out))))


def readVarint(trans):
    result = 0
    shift = 0

    while True:
        # x = trans.readAll(1)
        # print('trans type', type(trans))
        x = trans.read(1)
        # print('readVarint', type(x))
        # print('readVarint', x)
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


class TType:
    STOP = 0
    VOID = 1
    BOOL = 2
    BYTE = 3
    I08 = 3
    DOUBLE = 4
    I16 = 6
    I32 = 8
    I64 = 10
    STRING = 11
    UTF7 = 11
    STRUCT = 12
    MAP = 13
    SET = 14
    LIST = 15
    UTF8 = 16
    UTF16 = 17

    _VALUES_TO_NAMES = ('STOP',
                        'VOID',
                        'BOOL',
                        'BYTE',
                        'DOUBLE',
                        None,
                        'I16',
                        None,
                        'I32',
                        None,
                        'I64',
                        'STRING',
                        'STRUCT',
                        'MAP',
                        'SET',
                        'LIST',
                        'UTF8',
                        'UTF16')


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


_TTYPE_HANDLERS = (
    (None, None, False),  # 0 TType.STOP
    (None, None, False),  # 1 TType.VOID # TODO: handle void?
    ('readBool', 'writeBool', False),  # 2 TType.BOOL
    ('readByte', 'writeByte', False),  # 3 TType.BYTE and I08
    ('readDouble', 'writeDouble', False),  # 4 TType.DOUBLE
    (None, None, False),  # 5 undefined
    ('readI16', 'writeI16', False),  # 6 TType.I16
    (None, None, False),  # 7 undefined
    ('readI32', 'writeI32', False),  # 8 TType.I32
    (None, None, False),  # 9 undefined
    ('readI64', 'writeI64', False),  # 10 TType.I64
    ('readString', 'writeString', False),  # 11 TType.STRING and UTF7
    ('readContainerStruct', 'writeContainerStruct', True),  # 12 *.STRUCT
    ('readContainerMap', 'writeContainerMap', True),  # 13 TType.MAP
    ('readContainerSet', 'writeContainerSet', True),  # 14 TType.SET
    ('readContainerList', 'writeContainerList', True),  # 15 TType.LIST
    (None, None, False),  # 16 TType.UTF8 # TODO: handle utf8 types?
    (None, None, False)  # 17 TType.UTF16 # TODO: handle utf16 types?
)


class TCompactProtocol(object):
    """Compact implementation of the Thrift protocol driver."""
    PROTOCOL_ID = 0x82
    VERSION = 1
    VERSION_MASK = 0x1f
    TYPE_MASK = 0xe0
    TYPE_BITS = 0x07
    TYPE_SHIFT_AMOUNT = 5

    def __init__(self, trans):
        # TProtocolBase.__init__(self, trans)
        self.trans = trans
        self.state = CLEAR
        self.__last_fid = 0
        self.__bool_fid = None
        self.__bool_value = None
        self.__structs = []
        self.__containers = []

    def __writeVarint(self, n):
        writeVarint(self.trans, n)

    def writeMessageBegin(self, name, type, seqid):
        assert self.state == CLEAR
        self.__writeUByte(self.PROTOCOL_ID)
        self.__writeUByte(self.VERSION | (type << self.TYPE_SHIFT_AMOUNT))
        self.__writeVarint(seqid)
        self.__writeString(name)
        self.state = VALUE_WRITE

    def writeMessageEnd(self):
        assert self.state == VALUE_WRITE
        self.state = CLEAR

    def writeStructBegin(self, name):
        assert self.state in (CLEAR, CONTAINER_WRITE, VALUE_WRITE), self.state
        self.__structs.append((self.state, self.__last_fid))
        self.state = FIELD_WRITE
        self.__last_fid = 0

    def writeStructEnd(self):
        assert self.state == FIELD_WRITE
        self.state, self.__last_fid = self.__structs.pop()

    def writeFieldStop(self):
        self.__writeByte(0)

    def __writeFieldHeader(self, type, fid):
        delta = fid - self.__last_fid
        if 0 < delta <= 15:
            self.__writeUByte(delta << 4 | type)
        else:
            self.__writeByte(type)
            self.__writeI16(fid)
        self.__last_fid = fid

    def writeFieldBegin(self, name, type, fid):
        # assert self.state == FIELD_WRITE, self.state
        if type == TType.BOOL:
            self.state = BOOL_WRITE
            self.__bool_fid = fid
        else:
            self.state = VALUE_WRITE
            self.__writeFieldHeader(CTYPES[type], fid)

    def writeFieldEnd(self):
        assert self.state in (VALUE_WRITE, BOOL_WRITE), self.state
        self.state = FIELD_WRITE

    def __writeUByte(self, byte):
        self.trans.write(pack('!B', byte))

    def __writeByte(self, byte):
        self.trans.write(pack('!b', byte))

    def __writeI16(self, i16):
        self.__writeVarint(makeZigZag(i16, 16))

    def __writeSize(self, i32):
        self.__writeVarint(i32)

    def writeCollectionBegin(self, etype, size):
        assert self.state in (VALUE_WRITE, CONTAINER_WRITE), self.state
        if size <= 14:
            self.__writeUByte(size << 4 | CTYPES[etype])
        else:
            self.__writeUByte(0xf0 | CTYPES[etype])
            self.__writeSize(size)
        self.__containers.append(self.state)
        self.state = CONTAINER_WRITE

    writeSetBegin = writeCollectionBegin
    writeListBegin = writeCollectionBegin

    def writeMapBegin(self, ktype, vtype, size):
        assert self.state in (VALUE_WRITE, CONTAINER_WRITE), self.state
        if size == 0:
            self.__writeByte(0)
        else:
            self.__writeSize(size)
            self.__writeUByte(CTYPES[ktype] << 4 | CTYPES[vtype])
        self.__containers.append(self.state)
        self.state = CONTAINER_WRITE

    def writeCollectionEnd(self):
        assert self.state == CONTAINER_WRITE, self.state
        self.state = self.__containers.pop()

    writeMapEnd = writeCollectionEnd
    writeSetEnd = writeCollectionEnd
    writeListEnd = writeCollectionEnd

    def writeBool(self, bool):
        if self.state == BOOL_WRITE:
            if bool:
                ctype = CompactType.TRUE
            else:
                ctype = CompactType.FALSE
            self.__writeFieldHeader(ctype, self.__bool_fid)
        elif self.state == CONTAINER_WRITE:
            if bool:
                self.__writeByte(CompactType.TRUE)
            else:
                self.__writeByte(CompactType.FALSE)
        else:
            raise AssertionError("Invalid state in compact protocol")

    writeByte = writer(__writeByte)
    writeI16 = writer(__writeI16)

    @writer
    def writeI32(self, i32):
        self.__writeVarint(makeZigZag(i32, 32))

    @writer
    def writeI64(self, i64):
        self.__writeVarint(makeZigZag(i64, 64))

    @writer
    def writeDouble(self, dub):
        self.trans.write(pack('<d', dub))

    def __writeString(self, s):
        if PY3:
            self.__writeSize(len(bytearray(s, 'utf-8')))
        else:
            self.__writeSize(len(s))
        print(type(s))
        # print(type(bytes(s, encoding="UTF-8")))
        print(s)
        print(len(s))
        # if PY3:
        #     pass
        # else:
        if not isinstance(s, bytes):
            s = s.encode('utf-8')
            # outbuf.write(pack_string(val))
        # self.trans.write(bytes(s, encoding="UTF-8"))
        self.trans.write(s)
        # self.trans.write(s.encode("utf-8"))

    writeString = writer(__writeString)

    def readFieldBegin(self):
        # print('state',self.state)
        assert self.state == FIELD_READ, self.state
        type = self.__readUByte()
        if type & 0x0f == TType.STOP:
            return (None, 0, 0)
        delta = type >> 4
        if delta == 0:
            fid = self.__readI16()
        else:
            fid = self.__last_fid + delta
        self.__last_fid = fid
        type = type & 0x0f
        if type == CompactType.TRUE:
            self.state = BOOL_READ
            self.__bool_value = True
        elif type == CompactType.FALSE:
            self.state = BOOL_READ
            self.__bool_value = False
        else:
            self.state = VALUE_READ
        return (None, self.__getTType(type), fid)

    def readFieldEnd(self):
        assert self.state in (VALUE_READ, BOOL_READ), self.state
        self.state = FIELD_READ

    def __readUByte(self):
        # result, = unpack('!B', self.trans.readAll(1))
        result, = unpack('!B', self.trans.read(1))
        return result

    def __readByte(self):
        # result, = unpack('!b', self.trans.readAll(1))
        result, = unpack('!b', self.trans.read(1))
        return result

    def __readVarint(self):
        # print(self.trans.getvalue())
        # print(self.trans.read(1))
        return readVarint(self.trans)

    def __readZigZag(self):
        return fromZigZag(self.__readVarint())

    def __readSize(self):
        result = self.__readVarint()
        if result < 0:
            raise TException("Length < 0")
        return result

    def readMessageBegin(self):
        assert self.state == CLEAR
        proto_id = self.__readUByte()
        # print('proto_id', proto_id)
        if proto_id != self.PROTOCOL_ID:
            raise TProtocolException(TProtocolException.BAD_VERSION,
                                     'Bad protocol id in the message: %d' % proto_id)
        ver_type = self.__readUByte()
        type = (ver_type >> self.TYPE_SHIFT_AMOUNT) & self.TYPE_BITS
        version = ver_type & self.VERSION_MASK
        if version != self.VERSION:
            raise TProtocolException(TProtocolException.BAD_VERSION,
                                     'Bad version: %d (expect %d)' % (version, self.VERSION))
        seqid = self.__readVarint()
        name = self.__readString()
        return (name, type, seqid)

    def readMessageEnd(self):
        assert self.state == CLEAR
        assert len(self.__structs) == 0

    def readStructBegin(self):
        assert self.state in (CLEAR, CONTAINER_READ, VALUE_READ), self.state
        self.__structs.append((self.state, self.__last_fid))
        self.state = FIELD_READ
        self.__last_fid = 0

    def readStructEnd(self):
        assert self.state == FIELD_READ
        self.state, self.__last_fid = self.__structs.pop()

    def readCollectionBegin(self):
        assert self.state in (VALUE_READ, CONTAINER_READ), self.state
        size_type = self.__readUByte()
        size = size_type >> 4
        type = self.__getTType(size_type)
        if size == 15:
            size = self.__readSize()
        self.__containers.append(self.state)
        self.state = CONTAINER_READ
        return type, size

    readSetBegin = readCollectionBegin
    readListBegin = readCollectionBegin

    def readMapBegin(self):
        assert self.state in (VALUE_READ, CONTAINER_READ), self.state
        size = self.__readSize()
        types = 0
        if size > 0:
            types = self.__readUByte()
        vtype = self.__getTType(types)
        ktype = self.__getTType(types >> 4)
        self.__containers.append(self.state)
        self.state = CONTAINER_READ
        return (ktype, vtype, size)

    def readCollectionEnd(self):
        assert self.state == CONTAINER_READ, self.state
        self.state = self.__containers.pop()

    readSetEnd = readCollectionEnd
    readListEnd = readCollectionEnd
    readMapEnd = readCollectionEnd

    def readBool(self):
        if self.state == BOOL_READ:
            return self.__bool_value == CompactType.TRUE
        elif self.state == CONTAINER_READ:
            return self.__readByte() == CompactType.TRUE
        else:
            raise AssertionError("Invalid state in compact protocol: %d" %
                                 self.state)

    readByte = reader(__readByte)
    __readI16 = __readZigZag
    readI16 = reader(__readZigZag)
    readI32 = reader(__readZigZag)
    readI64 = reader(__readZigZag)

    @reader
    def readDouble(self):
        # buff = self.trans.readAll(8)
        buff = self.trans.read(8)
        val, = unpack('<d', buff)
        return val

    def __readString(self):
        len = self.__readSize()
        # print('read string len', len)
        # return self.trans.readAll(len)

        byte_payload = self.trans.read(len)
        # print('byte_payload', byte_payload)
        # print('byte_payload type', type(byte_payload))
        try:
            return byte_payload.decode('utf-8')
        except UnicodeDecodeError:
            return byte_payload

        # return self.trans.read(len)

    readString = reader(__readString)

    def __getTType(self, byte):
        return TTYPES[byte & 0x0f]

    def skip(self, ttype):
        if ttype == TType.STOP:
            return
        elif ttype == TType.BOOL:
            self.readBool()
        elif ttype == TType.BYTE:
            self.readByte()
        elif ttype == TType.I16:
            self.readI16()
        elif ttype == TType.I32:
            self.readI32()
        elif ttype == TType.I64:
            self.readI64()
        elif ttype == TType.DOUBLE:
            self.readDouble()
        elif ttype == TType.STRING:
            self.readString()
        elif ttype == TType.STRUCT:
            name = self.readStructBegin()
            while True:
                (name, ttype, id) = self.readFieldBegin()
                if ttype == TType.STOP:
                    break
                self.skip(ttype)
                self.readFieldEnd()
            self.readStructEnd()
        elif ttype == TType.MAP:
            (ktype, vtype, size) = self.readMapBegin()
            for i in xrange(size):
                self.skip(ktype)
                self.skip(vtype)
            self.readMapEnd()
        elif ttype == TType.SET:
            (etype, size) = self.readSetBegin()
            for i in xrange(size):
                self.skip(etype)
            self.readSetEnd()
        elif ttype == TType.LIST:
            (etype, size) = self.readListBegin()
            for i in xrange(size):
                self.skip(etype)
            self.readListEnd()

    def read_message_begin(self):
        api, ttype, seqid = self.readMessageBegin()
        return api, ttype, seqid

    def read_message_end(self):
        self.readMessageEnd()

    def write_message_begin(self, name, ttype, seqid):
        self.writeMessageBegin(name, ttype, seqid)

    def write_message_end(self):
        self.writeMessageEnd()

    def read_struct(self, obj):
        return self.readStruct(obj)

    def readStruct(self, obj):
        self.readStructBegin()
        while True:
            # fname 没有用 是 None
            (fname, ftype, fid) = self.readFieldBegin()
            # print('readFieldBegin', fname, ftype, fid)
            if ftype == TType.STOP:
                break

            if fid not in obj.thrift_spec:
                self.skip(ftype)
                continue

            try:
                field = obj.thrift_spec[fid]
                print('field-', obj.thrift_spec)
                # print(type(obj.thrift_spec))
            except IndexError:
                self.skip(ftype)
                raise
            else:
                if field is not None and ftype == field[0]:
                    print('field', field)
                    fname = field[1]
                    fspec = field[2]
                    val = self.read_val(ftype, fspec)
                    setattr(obj, fname, val)
                else:
                    self.skip(ftype)
            self.readFieldEnd()
        self.readStructEnd()

        # def write_struct(self, obj):
        #   self.writeStruct(obj, obj.thrift_spec)
        # write_val(self.trans, TType.STRUCT, obj)

    def write_struct(self, obj):
        self.writeStructBegin(obj.__class__.__name__)
        # print('thrift_spec ', obj.thrift_spec)  # thrift_spec  {1: (11, 'a', False)}
        # print(obj)

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

            self.writeFieldBegin(fname, ftype, field)
            print('fspec -> ', fspec)
            # print('writeFieldBegin - ', fname, ftype, field)
            # self.write_val(ftype, val, fspec)
            self.write_val(ftype, val, f_container_spec)
            self.writeFieldEnd()
        self.writeFieldStop()
        self.writeStructEnd()

    def read_val(self, ttype, spec=None):
        try:
            (r_handler, w_handler, is_container) = _TTYPE_HANDLERS[ttype]
        except IndexError:
            raise TProtocolException(type=TProtocolException.INVALID_DATA,
                                     message='Invalid field type %d' % (ttype))
        if r_handler is None:
            raise TProtocolException(type=TProtocolException.INVALID_DATA,
                                     message='Invalid field type %d' % (ttype))
        reader = getattr(self, r_handler)
        print('reader', reader)

        print('read spec', spec)
        if ttype == TType.LIST or ttype == TType.SET:
            if isinstance(spec, tuple):
                v_type, v_spec = spec[0], spec[1]
            else:
                v_type, v_spec = spec, None
            print(spec)
            result = []
            r_type, sz = self.readListBegin()
            # the v_type is useless here since we already get it from spec
            # if r_type != v_type:
            #     for _ in range(sz):
            #         self.skip(r_type)
            #     self.readListEnd()
            #     return []

            for i in range(sz):
                result.append(self.read_val(v_type, v_spec))

            self.readListEnd()
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
            sk_type, sv_type, sz = self.readMapBegin()
            # if sk_type != k_type or sv_type != v_type:
            #     for _ in range(sz):
            #         skip(inbuf, sk_type)
            #         skip(inbuf, sv_type)
            #     self.readMapEnd()
            #     return {}

            for i in range(sz):
                k_val = self.read_val(k_type, k_spec)
                v_val = self.read_val(v_type, v_spec)
                result[k_val] = v_val
            self.readMapEnd()

            return result

        else:
            if not is_container:
                return reader()
            return reader(spec)

    def readContainerList(self, spec):
        results = []
        ttype, tspec = spec[0], spec[1]
        r_handler = _TTYPE_HANDLERS[ttype][0]
        reader = getattr(self, r_handler)
        (list_type, list_len) = self.readListBegin()
        if tspec is None:
            # list values are simple types
            for idx in xrange(list_len):
                results.append(reader())
        else:
            # this is like an inlined read_val
            container_reader = _TTYPE_HANDLERS[list_type][0]
            val_reader = getattr(self, container_reader)
            for idx in xrange(list_len):
                val = val_reader(tspec)
                results.append(val)
        self.readListEnd()
        return results

    def readContainerSet(self, spec):
        results = set()
        ttype, tspec = spec[0], spec[1]
        r_handler = _TTYPE_HANDLERS[ttype][0]
        reader = getattr(self, r_handler)
        (set_type, set_len) = self.readSetBegin()
        if tspec is None:
            # set members are simple types
            for idx in xrange(set_len):
                results.add(reader())
        else:
            container_reader = _TTYPE_HANDLERS[set_type][0]
            val_reader = getattr(self, container_reader)
            for idx in xrange(set_len):
                results.add(val_reader(tspec))
        self.readSetEnd()
        return results

    def readContainerMap(self, spec):
        results = dict()
        key_ttype, key_spec = spec[0], spec[1]
        val_ttype, val_spec = spec[2], spec[3]
        (map_ktype, map_vtype, map_len) = self.readMapBegin()
        # TODO: compare types we just decoded with thrift_spec and
        # abort/skip if types disagree
        key_reader = getattr(self, _TTYPE_HANDLERS[key_ttype][0])
        val_reader = getattr(self, _TTYPE_HANDLERS[val_ttype][0])
        # list values are simple types
        for idx in xrange(map_len):
            if key_spec is None:
                k_val = key_reader()
            else:
                k_val = self.read_val(key_ttype, key_spec)
            if val_spec is None:
                v_val = val_reader()
            else:
                v_val = self.read_val(val_ttype, val_spec)
            # this raises a TypeError with unhashable keys types
            # i.e. this fails: d=dict(); d[[0,1]] = 2
            results[k_val] = v_val
        self.readMapEnd()
        return results

    def writeContainerStruct(self, val, spec):
        val.write(self)

    def readContainerStruct(self, spec):
        obj_class = spec
        obj = obj_class()
        obj.read(self)
        return obj

    def write_val(self, ttype, val, spec=None):
        # print('write_val', ttype, val, spec)
        r_handler, w_handler, is_container = _TTYPE_HANDLERS[ttype]
        # print('write_val - get_type_handler', r_handler, w_handler, is_container)

        # list, map, set 自己处理
        if ttype == TType.LIST or ttype == TType.SET:
            # ftype, fname, f_container_spec, f_req = fspec
            # print('list --> ', ttype, val, spec)  # (15, [111, 222, 333], (15, 'phones', 10, False))
            # self.writeFieldBegin('phones', TType.LIST, 2)
            if isinstance(spec, tuple):
                # e_type_, t_spec, e_type  = spec[0], spec[2], spec[2]
                e_type, t_spec = spec[0], spec[1]
            else:
                e_type, t_spec = spec, None

            # ele_type = e_type
            val_len = len(val)
            # self.writeListBegin(TType.I64, len(self.phones))
            self.writeListBegin(e_type, val_len)
            for e_val in val:
                # for list<struct>  =>   (14, 'lp', (12, <class 'pp_thrift.Person'>), False)
                # if isinstance(e_type, tuple):
                #     self.write_val(e_type[0], e_val, t_spec)
                # else:
                self.write_val(e_type, e_val, t_spec)
            self.writeListEnd()
            # oprot.writeFieldEnd()
        elif ttype == TType.MAP:
            # print('map -> ', ttype, val, spec)  # (13, {'kkk': 5}, (13, 'm', (11, 6), False))
            print('type=map => ', ttype, val, spec)

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

            self.writeMapBegin(k_type, v_type, len(val))
            for k in iter(val):
                self.write_val(k_type, k, k_spec)
                self.write_val(v_type, val[k], v_spec)
            self.writeMapEnd()

        else:
            writer = getattr(self, w_handler)
            if is_container:
                print('writeField - val, spec', val, spec)
                writer(val, spec)
            else:
                writer(val)


class TCompactProtocolFactory:
    def __init__(self):
        pass

    def get_protocol(self, trans):
        return TCompactProtocol(trans)

    def getProtocol(self, trans):
        return TCompactProtocol(trans)
