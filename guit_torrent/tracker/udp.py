import asyncio
import random
import struct
import urllib.parse
from asyncio import DatagramProtocol, Event

from guit_torrent.metainfo import TorrentMetaInfo
from guit_torrent.tracker.base import BaseTracker, TrackerResponse, TrackerRequestParameters, TrackerError

MAGIC_CONNECT_CONSTANT = 0x41727101980


def get_random_transaction_id():
    return random.randint(0, 2 ** 32 - 1)


class TrackerUDP(BaseTracker):
    def __init__(self, address: urllib.parse.ParseResult, torrent_metainfo: TorrentMetaInfo, peer_id,
                 get_downloaded_cb, update_data_cb=None):
        super().__init__(address, torrent_metainfo, peer_id, get_downloaded_cb, update_data_cb)
        self.host, self.port = address.hostname, address.port
        self._session = None
        self.transport = None
        self.protocol: TrackerUDPProtocol = None
        self.connection_id = None
        self._n = 0

    async def close(self):
        if self.transport:
            self.transport.close()
        await super().close()

    async def _connect(self):
        # console.log(f"Sending connect to UDP tracker {self}...")
        sent_transaction_id = get_random_transaction_id()
        connect_msg = struct.pack("!QII", MAGIC_CONNECT_CONSTANT, 0, sent_transaction_id)
        res = await self.protocol.send(connect_msg)
        if len(res) < 16:
            raise TrackerError("Expected at least 16 bytes from connect response")
        action, transaction_id, self.connection_id = struct.unpack("!IIQ", res)
        if action != 0 or transaction_id != sent_transaction_id:
            raise TrackerError(f"Invalid connect response {action=}, {transaction_id=} for {sent_transaction_id=}")
        # console.log("Successfully connected to UDP tracker.")
        self.connected = True

    async def _send_announce(self, params: TrackerRequestParameters) -> "TrackerResponse":
        if not self.protocol:
            loop = asyncio.get_running_loop()

            self.transport, self.protocol = await loop.create_datagram_endpoint(
                lambda: TrackerUDPProtocol(), remote_addr=(self.host, self.port))
        if not self.connection_id:
            await self._connect()
        # console.log("Sending announce to UDP tracker...")
        """
        Offset  Size    Name    Value
        0       64-bit integer  connection_id
        8       32-bit integer  action          1 // announce
        12      32-bit integer  transaction_id
        16      20-byte string  info_hash
        36      20-byte string  peer_id
        56      64-bit integer  downloaded
        64      64-bit integer  left
        72      64-bit integer  uploaded
        80      32-bit integer  event           0 // 0: none; 1: completed; 2: started; 3: stopped
        84      32-bit integer  IP address      0 // default
        88      32-bit integer  key
        92      32-bit integer  num_want        -1 // default
        96      16-bit integer  port
        98
        """
        sent_transaction_id = get_random_transaction_id()
        announce_msg = struct.pack(
            "!QII20B20BQQQIIIiH",
            self.connection_id,
            1,
            sent_transaction_id,
            *params.info_hash,
            *str.encode(params.peer_id),
            params.downloaded,
            params.left,
            params.uploaded,
            params.event_numeral,
            0,
            params.key,
            params.numwant,
            params.port
        )
        announce_res = await self.protocol.send(announce_msg)
        """
        Offset      Size            Name            Value
        0           32-bit integer  action          1 // announce
        4           32-bit integer  transaction_id
        8           32-bit integer  interval
        12          32-bit integer  leechers
        16          32-bit integer  seeders
        20 + 6 * n  32-bit integer  IP address
        24 + 6 * n  16-bit integer  TCP port
        20 + 6 * N
        """
        if len(announce_res) < 20:
            raise TrackerError("Expected at least 20 bytes from announce response")
        action, transaction_id, interval, leechers, seeders = struct.unpack("!5I", announce_res[:20])
        if action != 1 or transaction_id != sent_transaction_id:
            raise TrackerError(f"Invalid announce response {action=}, {transaction_id=} for {sent_transaction_id=}")
        return TrackerResponse(
            interval=interval,
            incomplete=leechers,
            complete=seeders,
            peers=bytes(announce_res[20:])
        )


class TrackerUDPProtocol(DatagramProtocol):
    def __init__(self):
        self._buffer = bytearray()
        self._transport = None
        self._data_received: Event = asyncio.Event()
        self._exc = None
        self._connection_lost: bool = False
        self._connection_made: Event = asyncio.Event()

    def connection_made(self, transport):
        self._transport = transport
        self._connection_made.set()

    async def send(self, data) -> bytes:
        if not self._transport:
            # wait for connection to be established
            try:
                await self._connection_made.wait()
            finally:
                self._connection_made.clear()
        n = 0
        while n <= 8:
            self._transport.sendto(data)
            # wait for response
            if not self._buffer and not self._exc and not self._connection_lost:
                try:
                    await asyncio.wait_for(self._data_received.wait(), 15 * (2 ** n))
                except asyncio.TimeoutError:
                    n += 1
                    continue
                finally:
                    self._data_received.clear()

            if self._exc:
                exc = self._exc
                self._exc = None
                raise exc

            if self._connection_lost:
                raise ConnectionResetError('Connection lost')

            buffer = self._buffer
            self._buffer = bytearray()
            return buffer
        raise TimeoutError

    def _notify(self):
        self._data_received.set()

    def datagram_received(self, data, addr):
        self._buffer.extend(data)
        self._notify()

    def error_received(self, exc):
        self._exc = exc
        self._notify()

    def connection_lost(self, exc):
        self._exc = exc
        self._connection_lost = True
        self._notify()
