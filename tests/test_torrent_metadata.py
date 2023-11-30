import os
from hashlib import sha1

import pytest

from guit_torrent.metainfo import MultiFileInfo, IndividualFileInfo, TorrentMetaInfo, load_torrent_metadata


@pytest.mark.asyncio
async def test_torrent_file_creation():
    files_raw_data = bytearray()
    files = []
    for file in os.listdir("assets/torrent_files"):
        with open(os.path.join("assets/torrent_files", file), "rb") as f:
            file_data = f.read()
            files_raw_data.extend(file_data)
            files.append(IndividualFileInfo(
                name=file,
                length=len(file_data)
            ))
    piece_length = 3 * (2 ** 14) + 50
    piece_hashes = bytearray()
    for piece_beginning in range(0, len(files_raw_data), piece_length):
        piece_hashes.extend(sha1(files_raw_data[piece_beginning:piece_beginning + piece_length]).digest())
    info = MultiFileInfo(name="some_files", files=files, piece_length=piece_length, pieces=bytes(piece_hashes))
    meta_info = TorrentMetaInfo(
        info=info,
        info_hash=None,
        announce_list=None,
        announce="udp://someplace:80/announce",
        comment="just a nice torrent"
    )
    with open("assets/some_files.torrent", "wb") as f:
        f.write(meta_info.encode())

    loaded = load_torrent_metadata("assets/some_files.torrent")
    loaded.info_hash = None
    assert loaded == meta_info

