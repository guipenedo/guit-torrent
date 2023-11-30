import secrets

import pytest

from guit_torrent.peer.messages import read_msg, KeepAliveMessage, ChokeMessage, UnchokeMessage, InterestedMessage, \
    NotInterestedMessage, HaveMessage, BitfieldMessage, RequestMessage, PieceMessage, CancelMessage, PortMessage


class FakeStream:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    async def readexactly(self, size):
        assert self.pos + size <= len(self.data)
        self.pos += size
        return self.data[self.pos - size: self.pos]


@pytest.mark.asyncio
async def test_encode_decode():
    for msg in (
            KeepAliveMessage(),
            ChokeMessage(),
            UnchokeMessage(),
            InterestedMessage(),
            NotInterestedMessage(),
            HaveMessage(23),
            BitfieldMessage({1, 3, 13}, 2),
            RequestMessage(12, 123, 19999),
            PieceMessage(123, 333, b"\x00" + secrets.token_bytes(30) + b"\x00"),
            CancelMessage(12, 123, 19999),
            PortMessage(1237)
    ):
        encoded = msg.encode()
        parsed = await read_msg(FakeStream(encoded))
        assert encoded == parsed.encode()
