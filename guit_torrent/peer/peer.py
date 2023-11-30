import asyncio
import time

from guit_torrent.metainfo import TorrentMetaInfo
from guit_torrent.peer.messages import ChokeMessage, UnchokeMessage, InterestedMessage, NotInterestedMessage, \
    HaveMessage, BitfieldMessage, RequestMessage, PieceMessage, CancelMessage, PortMessage, KeepAliveMessage, \
    HandshakeMessage, read_msg
from guit_torrent.ui import console

KEEP_ALIVE_INTERVAL = 2 * 60
BLOCKS_TO_QUEUE = 50

class PeerConnectError(Exception):
    pass


class Peer:
    def __init__(self, torrent: TorrentMetaInfo, our_peer_id: str, host: tuple[str, int], block_received_cb):
        self.torrent = torrent
        self.our_peer_id = our_peer_id

        self.address, self.port = host

        self.block_received_cb = block_received_cb

        self.alive = False
        self.starting = True

        self.am_not_choking = asyncio.Event()
        self.am_interested = False
        self.peer_choking = True
        self.peer_interested = False

        self.available_pieces = set()
        self.blocks_to_request = asyncio.Queue()
        self.blocks_requested = asyncio.Semaphore(BLOCKS_TO_QUEUE)

        self.writer = None
        self.reader = None

        self.main_task = None
        self.request_future = None
        self.keep_alive_future = None

    @property
    def host(self):
        return self.address, self.port

    async def close(self):
        self.starting = False
        self.alive = False
        if self.request_future:
            self.request_future.cancel()
        if self.keep_alive_future:
            self.keep_alive_future.cancel()
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
        if self.main_task:
            self.main_task.cancel()

    def start(self):
        self.main_task = asyncio.create_task(self._start())

    def __str__(self):
        return f"{self.address}:{self.port}"

    async def send_message(self, msg):
        console.log(f"{msg} -> {self}")
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def request_blocks(self):
        await self.send_message(UnchokeMessage())
        self.peer_choking = False
        while True:
            block = await self.blocks_to_request.get()
            block.last_requested = time.time()
            # console.log(f"Block ready to be requested to {self}")
            if not self.am_interested:
                self.am_interested = True
                # console.log(f"Sending interested to {self}")
                await self.send_message(InterestedMessage())
            # wait for unchoke
            await self.am_not_choking.wait()
            # console.log(f"{self} unchoked! proceeding")
            # can we request it? - acquire space in requested queue
            # console.log(f"waiting for queue space in {self}")
            await self.blocks_requested.acquire()
            # send the message!
            await self.send_message(RequestMessage(
                index=block.piece_id,
                begin=block.begin,
                length=block.length
            ))
            # console.log(f"Requesting block [{block.piece_id}, {block.begin}] from {self}")

    async def connect(self):
        self.starting = True
        self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.address, self.port), 15)
        # handshake
        handshake_msg = HandshakeMessage(info_hash=self.torrent.info_hash, peer_id=self.our_peer_id)
        await self.send_message(handshake_msg)

        handshake_res = HandshakeMessage.decode(await self.reader.read(HandshakeMessage.len()))
        # check if info_hash matches
        if handshake_res.info_hash != self.torrent.info_hash:
            raise PeerConnectError("Handshake mismatch")
        self.starting = False
        self.alive = True
        console.log(f"Connected to peer {self} {self.alive=}")

    async def handle_message(self):
        msg = await read_msg(self.reader)
        console.log(f"{self} -> {msg}")
        match msg:
            case ChokeMessage():
                # console.log(f"{self} choked us!")
                self.am_not_choking.clear()
            case UnchokeMessage():
                # console.log(f"{self} unchoked us!")
                self.am_not_choking.set()
            case InterestedMessage():
                self.peer_interested = True
            case NotInterestedMessage():
                self.peer_interested = False
            case HaveMessage():
                self.available_pieces.add(msg.piece_index)
            case BitfieldMessage():
                self.available_pieces |= msg.pieces
            case RequestMessage():
                raise NotImplementedError
            case PieceMessage():
                console.log(f"[red]{self} sent us block [{msg.index}, {msg.begin}]")
                if self.block_received_cb:
                    await self.block_received_cb(msg)
                self.blocks_requested.release()
            case CancelMessage():
                raise NotImplementedError
            case PortMessage():
                raise NotImplementedError
            case KeepAliveMessage():
                pass
            case _ as a:
                console.log(f"[yellow] unknown message: {a}")

    async def keep_alive(self):
        while self.alive:
            await self.send_message(KeepAliveMessage())
            await asyncio.sleep(KEEP_ALIVE_INTERVAL)

    async def _start(self):
        try:
            await self.connect()
            self.keep_alive_future = asyncio.create_task(self.keep_alive())
            self.request_future = asyncio.create_task(self.request_blocks())
            # self.request_future.add_done_callback(lambda future: future.exception())
            while self.alive:
                await self.handle_message()
        except (PeerConnectError, ConnectionResetError, ConnectionRefusedError,
                asyncio.TimeoutError, OSError, asyncio.IncompleteReadError):
            if not self.starting:
                console.log(f"[red]Connection with {self} dropped.")
        except Exception as e:
            raise e
        finally:
            await self.close()
