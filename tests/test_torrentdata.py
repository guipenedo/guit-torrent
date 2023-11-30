import os
from hashlib import sha1
from tempfile import TemporaryDirectory

import pytest

from guit_torrent.metainfo import MultiFileInfo, IndividualFileInfo, TorrentMetaInfo, load_torrent_metadata
from guit_torrent.torrentdata import get_torrentdata_from_metainfo


def load_block(data, piece_length, piece, begin, length):
    piece_data = data[piece * piece_length: (piece + 1) * piece_length]
    assert begin + length <= len(piece_data)
    return piece_data[begin: begin + length]


@pytest.mark.asyncio
async def test_pieces_creation():
    metadata = load_torrent_metadata("assets/some_files.torrent")
    with TemporaryDirectory() as tmpdir:
        torrent = get_torrentdata_from_metainfo(metadata, tmpdir)
        piece_length = torrent.piece_length

        files_raw_data = bytearray()
        for file in os.listdir("assets/torrent_files"):
            with open(os.path.join("assets/torrent_files", file), "rb") as f:
                files_raw_data.extend(f.read())

        for piece in torrent.pieces:
            for block in piece.blocks:
                block_data = load_block(files_raw_data, piece_length, block.piece_id, block.begin, block.length)
                await torrent.write_block(block, block_data)
            assert (await torrent.read_piece(piece.piece_id) ==
                    files_raw_data[piece.piece_id * piece_length: (piece.piece_id + 1) * piece_length])
            assert await torrent.verify_piece(piece.piece_id)
        for file in torrent.files:
            os.fsync(file.fd)

        written_data = bytearray()
        for file in os.listdir(tmpdir):
            with open(os.path.join(tmpdir, file), "rb") as f:
                written_data.extend(f.read())
        assert written_data == files_raw_data
