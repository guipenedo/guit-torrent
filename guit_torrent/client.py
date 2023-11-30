import asyncio
import os
from asyncio import InvalidStateError
from collections import deque

from guit_torrent.metainfo import load_torrent_metadata
from guit_torrent.peer.messages import PieceMessage
from guit_torrent.peer.peer import Peer, BLOCKS_TO_QUEUE
from guit_torrent.torrentdata import TorrentBlock, TorrentPiece, get_torrentdata_from_metainfo
from guit_torrent.tracker.manager import TrackerManager
from guit_torrent.ui import ui_update_files_progress, console, ui_view, ui_update_overall

MAX_PEERS = 50
CLIENT_UPDATES_INTERVAL = 5


class TorrentClient:
    def __init__(self, torrent_path, base_output_folder):
        self.torrent_path = torrent_path
        self.torrent_metadata = load_torrent_metadata(torrent_path)
        console.log(f"Loaded torrent \"{torrent_path}\"")
        console.print(self.torrent_metadata)
        self.output_folder = os.path.join(base_output_folder, self.torrent_metadata.info.name)
        # peers
        self.peers = []
        self.dead_peers = set()
        # tracker
        self.tracker_updates = asyncio.Event()
        self.tracker_manager = TrackerManager(self.torrent_metadata, self.get_downloaded_bytes, self.tracker_updates)
        # torrent data
        self.torrent = None
        self.running = False

    def get_downloaded_bytes(self):
        return self.torrent.confirmed_downloaded_bytes if self.torrent else 0

    async def close(self):
        self.running = False
        await self.tracker_manager.close()
        for peer in self.peers:
            await peer.close()
        if self.torrent:
            self.torrent.close()
        ui_view.close()

    async def block_received(self, msg: PieceMessage):
        block: TorrentBlock = self.torrent.pieces[msg.index].get_block(msg.begin)
        if block and len(msg.block) == block.length:
            await self.torrent.write_block(block, msg.block)
            block.downloaded = True
            block.data = msg.block
            piece: TorrentPiece = self.torrent.pieces[block.piece_id]
            piece.bytes_downloaded += block.length
            if piece.downloaded:
                verified = await self.torrent.verify_piece(block.piece_id)
                if verified:
                    piece.confirmed = True
                    if all([piece.confirmed for piece in self.torrent.pieces]):
                        self.torrent.downloaded = True
                        self.running = False
                else:
                    for block in piece.blocks:
                        block.downloaded = False
                        piece.bytes_downloaded -= block.length
            ui_update_files_progress(self.torrent)
        else:
            raise RuntimeError("SIZE MISMATCH ON BLOCK")

    async def stop(self):
        await self.tracker_manager.close()
        for peer in self.peers:
            await peer.close()

    async def start(self):
        await self._init_torrent()
        self.tracker_manager.start()
        ui_view.start(refresh=True)
        self.running = True
        while self.running:
            # update active peers
            for peer in self.peers:
                if not peer.alive and not peer.starting:
                    try:
                        peer.main_task.result()
                    except (asyncio.CancelledError, InvalidStateError):
                        pass
                    self.dead_peers.add(peer.host)
            self.peers = [peer for peer in self.peers if peer.alive or peer.starting]
            active_peer_hosts = set([peer.host for peer in self.peers])
            # try the ones we haven't just removed first
            peers_to_try = deque(
                list(self.tracker_manager.peers - self.dead_peers - active_peer_hosts) +
                list(self.tracker_manager.peers & self.dead_peers)
            )
            while len(self.peers) < MAX_PEERS and len(peers_to_try) > 0:
                # create new peer
                peer = Peer(self.torrent_metadata, self.tracker_manager.peer_id, peers_to_try.popleft(),
                            self.block_received)
                peer.start()
                self.peers.append(peer)
            # decide what to request next
            pieces_and_availability = [(piece, []) for piece in self.torrent.pieces]
            for peer in self.peers:
                for available_piece in peer.available_pieces:
                    pieces_and_availability[available_piece][1].append(peer)
            pieces_and_availability.sort(key=lambda x: len(x[1]), reverse=True)  # sort by rarity (rarest one first)
            available_pieces = sum(len(peers) > 0 for piece, peers in pieces_and_availability)
            ui_update_overall(self, available_pieces)
            # request them!
            for piece, peers in pieces_and_availability:
                if piece.confirmed:
                    continue
                blocks_left = deque(
                    [block for block in piece.blocks if not block.downloaded and block.request_timedout])
                peers.sort(key=lambda peer: peer.blocks_to_request.qsize(), reverse=True)
                for peer in peers:
                    while blocks_left and peer.blocks_to_request.qsize() < BLOCKS_TO_QUEUE:
                        peer.blocks_to_request.put_nowait(blocks_left.popleft())
            await asyncio.sleep(CLIENT_UPDATES_INTERVAL)
        await self.close()

    async def _init_torrent(self):
        self.torrent = get_torrentdata_from_metainfo(self.torrent_metadata, self.output_folder)
        if await self.torrent.check_existing_data():
            self.torrent.downloaded = True
