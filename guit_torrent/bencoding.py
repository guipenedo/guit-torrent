from collections import OrderedDict


class _BencodingReader:
    def __init__(self, data: bytes):
        if not isinstance(data, bytes):
            raise ValueError("data to decode must be bytes")
        self.i = 0
        self.data = data

    def decode_next(self):
        if self.i == len(self.data):
            return None
        match self.data[self.i: self.i + 1]:
            # int
            case b'i':
                end = self.data.index(b'e', self.i)
                value = int(self.data[self.i + 1: end].decode('utf-8'))
                self.i = end + 1
            # list
            case b'l':
                self.i += 1
                value = []
                while self.data[self.i: self.i + 1] != b'e':
                    value.append(self.decode_next())
                self.i += 1
            # dict
            case b'd':
                self.i += 1
                value = OrderedDict()
                while self.data[self.i: self.i + 1] != b'e':
                    key = self.decode_next()
                    value[key] = self.decode_next()
                self.i += 1
            # str
            case _:
                sep = self.data.index(b':', self.i)
                wlen = int(self.data[self.i: sep].decode('utf-8'))
                value = self.data[sep + 1: sep + 1 + wlen]
                try:
                    value = value.decode('utf-8')
                except UnicodeDecodeError:
                    # keep it as bytes
                    pass
                self.i = sep + 1 + wlen
        return value


def bendecode(data):
    return _BencodingReader(data).decode_next()


def benencode(data) -> bytes:
    # dict
    if isinstance(data, dict):
        return b"d" + b"".join([
            b"".join((benencode(key), benencode(value))) for key, value in data.items()
        ]) + b"e"
    # bool
    if isinstance(data, bool):
        data = 1 if data else 0
    # int
    if isinstance(data, int):
        return str.encode(f"i{data}e", "utf-8")
    # str
    if isinstance(data, str):
        return str.encode(f"{len(data)}:{data}", "utf-8")
    # bytes
    if isinstance(data, bytes):
        return str.encode(str(len(data)), 'utf-8') + b":" + data
    # list
    try:
        iterator = iter(data)
    except TypeError:
        raise ValueError(f"Unsupported type {type(data)}")
    else:
        # iterable
        return b"l" + b"".join(map(benencode, iterator)) + b"e"
