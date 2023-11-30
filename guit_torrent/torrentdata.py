import asyncio
import os
import time
from dataclasses import dataclass, field
from hashlib import sha1
from math import ceil

from tqdm import tqdm

from guit_torrent.ui import get_progress_task_for_file

REQUEST_TIMEOUT = 2 * 60
BLOCK_SIZE = 2 ** 14


def get_torrentdata_from_metainfo(torrent_metadata, output_folder):
    # for file in self.torrent_metadata.info.get_files():
    # length/size calculations
    nr_pieces = ceil(torrent_metadata.info.total_length / torrent_metadata.info.piece_length)
    last_piece_length = torrent_metadata.info.total_length % torrent_metadata.info.piece_length

    # init all pieces
    pieces = []
    for piece_id in range(nr_pieces):
        piece_length = torrent_metadata.info.piece_length if piece_id != nr_pieces - 1 else last_piece_length
        last_block_length = piece_length % BLOCK_SIZE
        # add piece
        pieces.append(
            TorrentPiece(
                piece_id=piece_id,
                begin=torrent_metadata.info.piece_length * piece_id,
                length=piece_length,
                sha1_hash=torrent_metadata.info.pieces[piece_id * 20: (piece_id + 1) * 20],
                blocks=[
                    TorrentBlock(
                        piece_id=piece_id,
                        block_id=block_id,
                        begin=begin,
                        length=BLOCK_SIZE if begin + BLOCK_SIZE <= piece_length else last_block_length,
                        absolute_begin=torrent_metadata.info.piece_length * piece_id + begin
                    )
                    for block_id, begin in enumerate(range(0, piece_length, BLOCK_SIZE))
                ]
            )
        )

    # init files
    files = []
    file_begin = 0
    run_pre_check = False
    for file in torrent_metadata.info.get_files():
        file_path = os.path.join(output_folder, file.name)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        file = TorrentFile(
            name=file.name,
            length=file.length,
            begin=file_begin,
            fd=os.open(file_path, os.O_RDWR | os.O_CREAT)
        )
        file.progress_task = get_progress_task_for_file(file)
        files.append(file)
        file_begin += file.length

    return Torrent(
        name=torrent_metadata.info.name,
        length=torrent_metadata.info.total_length,
        piece_length=torrent_metadata.info.piece_length,
        pieces=pieces,
        files=files
    )


@dataclass
class TorrentBlock:
    piece_id: int
    block_id: int
    begin: int
    length: int
    absolute_begin: int
    downloaded: bool = False
    last_requested: int = None
    data: bytes = None

    @property
    def request_timedout(self):
        return not self.last_requested or time.time() - self.last_requested > REQUEST_TIMEOUT


@dataclass
class TorrentPiece:
    piece_id: int
    begin: int
    length: int
    sha1_hash: bytes
    bytes_downloaded: int = 0

    confirmed: bool = False

    blocks: list[TorrentBlock] = field(default_factory=list)

    @property
    def downloaded(self):
        return all([block.downloaded for block in self.blocks])

    @property
    def end(self):
        return self.begin + self.length

    def verify(self, data):
        return sha1(data).digest() == self.sha1_hash

    def get_block(self, block_begin):
        blocks = [block for block in self.blocks if block.begin == block_begin]
        if blocks:
            return blocks[0]


@dataclass
class TorrentFile:
    name: str
    length: int
    begin: int  # start position in the "continuous stream" of data

    downloaded: bool = False

    fd: int = None
    file_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    progress_task: int = None

    async def read_section(self, begin, length):
        async with self.file_lock:
            os.fsync(self.fd)
            os.lseek(self.fd, begin, os.SEEK_SET)
            return os.read(self.fd, length)

    async def write_section(self, begin, data):
        async with self.file_lock:
            os.lseek(self.fd, begin, os.SEEK_SET)
            os.write(self.fd, data)

    def get_pieces(self, pieces):
        return [
            piece for piece in pieces
            if self.includes_piece(piece)
        ]

    def downloaded_bytes(self, pieces):
        total = 0
        for piece in pieces:
            if piece.confirmed:
                intersect = get_intersections(piece.begin, piece.end, self.begin, self.begin + self.length)
                if intersect is not None:
                    total += intersect[0][1] - intersect[0][0]
        return total

    def includes_piece(self, piece: TorrentPiece) -> bool:
        return get_intersections(self.begin, self.begin + self.length, piece.begin, piece.end) is not None

    def includes_block(self, block: TorrentBlock) -> bool:
        return get_intersections(self.begin, self.begin + self.length,
                                 block.absolute_begin, block.absolute_begin + block.length) is not None

    def close(self):
        if self.fd:
            os.close(self.fd)
            self.fd = None


@dataclass
class Torrent:
    name: str
    length: int
    piece_length: int

    pieces: list[TorrentPiece] = field(default_factory=list)
    files: list[TorrentFile] = field(default_factory=list)

    downloaded: bool = False

    def get_piece_files(self, piece_id) -> list[tuple[TorrentFile, int, int]]:
        files = []
        piece = self.pieces[piece_id]
        for file in self.files:
            # check intersection
            intersects = get_intersections(piece.begin, piece.end, file.begin, file.begin + file.length)
            if intersects is not None:
                start, end = intersects[2]  # relative to the file itself
                files.append((file, start, end - start))
        return files

    def get_block_files(self, block: TorrentBlock) -> list[tuple[TorrentFile, int, int]]:
        files = []
        for file in self.files:
            # check intersection
            intersects = get_intersections(block.absolute_begin, block.absolute_begin + block.length,
                                           file.begin, file.begin + file.length)
            if intersects is not None:
                start, end = intersects[2]  # relative to the file itself
                files.append((file, start, end - start))
        return files

    @property
    def confirmed_downloaded_bytes(self):
        # only count confirmed pieces
        return sum([piece.length for piece in self.pieces if piece.confirmed])

    @property
    def downloaded_bytes(self):
        return sum([piece.bytes_downloaded for piece in self.pieces])

    async def read_piece(self, piece_id):
        assert piece_id < len(self.pieces)
        data = bytearray()
        for file, begin, length in self.get_piece_files(piece_id):
            data.extend(await file.read_section(begin, length))
        return data

    async def write_block(self, block: TorrentBlock, data: bytes):
        data_begin = 0
        for file, begin, length in self.get_block_files(block):
            await file.write_section(begin, data[data_begin: data_begin + length])
            data_begin += length

    async def write_piece(self, piece: TorrentPiece):
        data = bytearray()
        for block in piece.blocks:
            assert block.data is not None
            data.extend(block.data)
            block.data = None
        data_begin = 0
        for file, begin, length in self.get_piece_files(piece.piece_id):
            await file.write_section(begin, data[data_begin: data_begin + length])
            data_begin += length

    async def verify_piece(self, piece_id) -> bool:
        return self.pieces[piece_id].verify(await self.read_piece(piece_id))

    async def check_existing_data(self):
        pieces_check = True
        for piece in tqdm(self.pieces, unit="piece", desc="verified"):
            piece.confirmed = await self.verify_piece(piece.piece_id)
            if not piece.confirmed:
                pieces_check = False
            if piece.downloaded and not piece.confirmed:
                # marked as downloaded but invalid. mark for redownload
                for block in piece.blocks:
                    block.downloaded = False
            elif not piece.downloaded and piece.confirmed:
                # marked as not downloaded but actually correct. mark as downloaded
                for block in piece.blocks:
                    block.downloaded = True
                    piece.bytes_downloaded += block.length
        for file in self.files:
            if all([piece.confirmed for piece in file.get_pieces(self.pieces)]):
                file.downloaded = True
        return pieces_check

    def close(self):
        for file in self.files:
            file.close()


def get_intersections(a: int, b: int, c: int, d: int) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
    """
        Get intersection of [a,b[ and [c,d[ in:
            - absolute interval
            - relative to starting point a
            - relative to starting point c
        returns None if no intersection
    """
    if a < d and c < b:
        start = max(a, c)
        end = min(b, d)
        return (start, end), (start - a, end - a), (start - c, end - c)
