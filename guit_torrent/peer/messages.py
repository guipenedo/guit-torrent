import struct
from abc import ABC
from asyncio import StreamReader
from dataclasses import dataclass, field
from typing import ClassVar

PSTR = b"BitTorrent protocol"
PSTR_PREFIX = struct.pack("!B", len(PSTR)) + PSTR


async def read_msg(reader: StreamReader) -> "PeerMessage":
    length = struct.unpack("!I", await reader.readexactly(4))[0]
    if length:
        data = await reader.readexactly(length)
        msg_type_id, data = struct.unpack("!B", data[:1])[0], data[1:]
        msg_type = id_to_msg_class(msg_type_id)
        msg = msg_type.decode(data)
        return msg
    return KeepAliveMessage()


def id_to_msg_class(id: int):
    match id:
        case ChokeMessage.id:
            return ChokeMessage
        case UnchokeMessage.id:
            return UnchokeMessage
        case InterestedMessage.id:
            return InterestedMessage
        case NotInterestedMessage.id:
            return NotInterestedMessage
        case HaveMessage.id:
            return HaveMessage
        case BitfieldMessage.id:
            return BitfieldMessage
        case RequestMessage.id:
            return RequestMessage
        case PieceMessage.id:
            return PieceMessage
        case CancelMessage.id:
            return CancelMessage
        case PortMessage.id:
            return PortMessage
        case _:
            return KeepAliveMessage


@dataclass
class PeerMessage(ABC):
    id: ClassVar[int]

    def encoded_data(self) -> bytes:
        return b""

    def encode(self) -> bytes:
        data = (struct.pack("!B", self.id) if self.id else b"") + self.encoded_data()
        return struct.pack("!I", len(data)) + data

    @classmethod
    def decode(cls, data: bytes):
        return cls()


@dataclass
class HandshakeMessage(PeerMessage):
    info_hash: bytes = field(repr=False)
    peer_id: str | bytes

    def encode(self) -> bytes:
        return (PSTR_PREFIX + bytes([0] * 8) + self.info_hash +
                (str.encode(self.peer_id) if type(self.peer_id) != bytes else self.peer_id))

    @classmethod
    def decode(cls, data: bytes):
        try:
            peer_id = data[-20:].decode('ascii')
        except UnicodeDecodeError:
            peer_id = data[-20:]
        return cls(info_hash=data[-40: -20], peer_id=peer_id)

    @classmethod
    def len(cls):
        return len(PSTR_PREFIX) + 8 + 20 + 20


@dataclass
class KeepAliveMessage(PeerMessage):
    id = None


@dataclass
class ChokeMessage(PeerMessage):
    id = 0


@dataclass
class UnchokeMessage(PeerMessage):
    id = 1


@dataclass
class InterestedMessage(PeerMessage):
    id = 2


@dataclass
class NotInterestedMessage(PeerMessage):
    id = 3


@dataclass
class HaveMessage(PeerMessage):
    piece_index: int
    id = 4

    def encoded_data(self) -> bytes:
        return struct.pack("!I", self.piece_index)

    @classmethod
    def decode(cls, data: bytes):
        return cls(piece_index=struct.unpack("!I", data)[0])


@dataclass
class BitfieldMessage(PeerMessage):
    pieces: set[int] = field(repr=False)
    nr_bytes: int
    id = 5

    @classmethod
    def decode(cls, data: bytes):
        return cls(pieces={
            bi * 8 + bit
            for bi, byte in enumerate(struct.unpack(f"!{len(data)}B", data))
            for bit in range(8)
            if (byte & (1 << (7 - bit))) != 0
        }, nr_bytes=len(data))

    def encoded_data(self) -> bytes:
        return struct.pack(f"!{self.nr_bytes}B", *[
            sum([
                1 << (7 - bit)
                for bit in range(8) if bi * 8 + bit in self.pieces
            ]) for bi in range(self.nr_bytes)
        ])


@dataclass
class RequestMessage(PeerMessage):
    index: int  # piece index
    begin: int
    length: int
    id = 6

    @classmethod
    def decode(cls, data: bytes):
        index, begin, length = struct.unpack("!3I", data)
        return cls(index=index, begin=begin, length=length)

    def encoded_data(self) -> bytes:
        return struct.pack("!3I", self.index, self.begin, self.length)


@dataclass
class PieceMessage(PeerMessage):
    index: int
    begin: int
    block: bytes = field(repr=False)
    id = 7

    @classmethod
    def decode(cls, data: bytes):
        index, begin = struct.unpack("!2I", data[:8])
        return cls(index=index, begin=begin, block=data[8:])

    def encoded_data(self) -> bytes:
        return struct.pack("!2I", self.index, self.begin) + self.block


@dataclass
class CancelMessage(RequestMessage):
    id = 8


@dataclass
class PortMessage(PeerMessage):
    port: int
    id = 9

    @classmethod
    def decode(cls, data: bytes):
        return cls(port=struct.unpack("!H", data)[0])

    def encoded_data(self) -> bytes:
        return struct.pack("!H", self.port)
