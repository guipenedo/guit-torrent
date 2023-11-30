import dataclasses
import itertools
import os.path
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from hashlib import sha1

from guit_torrent.bencoding import benencode, bendecode
from guit_torrent.utils import _format_keys


@dataclass
class IndividualFileInfo:
    name: str = ""
    """length of the file in bytes"""
    length: int = 0
    """(optional) a 32-character hexadecimal string corresponding to the MD5 sum of the file. This is not used by 
    BitTorrent at all, but it is included by some programs for greater compatibility."""
    md5sum: str | None = None


@dataclass
class FileInfo(ABC):
    """number of bytes in each piece"""
    piece_length: int
    """string consisting of the concatenation of all 20-byte SHA1 hash values, one per piece (byte string, 
    i.e. not urlencoded)"""
    pieces: bytes = field(repr=False)
    """(optional) this field is an integer. If it is set to "1", the client MUST publish its presence to get other 
    peers ONLY via the trackers explicitly described in the metainfo file. If this field is set to "0" or is not 
    present, the client may obtain peer from other means, e.g. PEX peer exchange, dht. Here, "private" may be read as 
    "no external peer source"."""
    private: bool | None = False

    @property
    def total_length(self) -> int:
        return 0

    @abstractmethod
    def get_files(self) -> list["SingleFileInfo"]:
        return []

    def to_dict(self):
        info = {
            key.replace("_", " "): val for key, val in dataclasses.asdict(self).items() if val is not None
        }
        if "private" in info:
            info["private"] = 1 if info["private"] else 0
        if "files" in info:
            info["files"] = [
                {
                    "path": file["name"].split("/"),
                    "length": file["length"]
                }
                for file in info["files"]
            ]
        return info


@dataclass
class SingleFileInfo(IndividualFileInfo, FileInfo):
    @property
    def total_length(self) -> int:
        return self.length

    def get_files(self) -> list["IndividualFileInfo"]:
        return [self]


@dataclass
class MultiFileInfo(FileInfo):
    """the name of the directory in which to store all the files. This is purely advisory."""
    name: str = ""
    """a list of dictionaries, one for each file"""
    files: list[IndividualFileInfo] = None

    @property
    def total_length(self) -> int:
        return sum([file.length for file in self.files])

    def get_files(self) -> list["IndividualFileInfo"]:
        return self.files


@dataclass
class TorrentMetaInfo:
    """Data extracted from a .torrent file"""
    """a dictionary that describes the file(s) of the torrent. There are two possible forms: one for the case of a
    'single-file' torrent with no directory structure, and one for the case of a 'multi-file' torrent"""
    info: FileInfo
    """sha1 hash of info"""
    info_hash: bytes
    """The announce URL of the tracker"""
    announce: str
    """"(optional) this is an extention to the official specification, offering 
    backwards-compatibility. (list of lists of strings)."""
    announce_list: list[list[str]] | None = field(repr=True, default=None)
    """(optional) the creation time of the torrent, in standard UNIX epoch format (integer, seconds since 1-Jan-1970 
    00:00:00 UTC)"""
    creation_date: int | None = None
    """(optional) free-form textual comments of the author"""
    comment: str | None = None
    """(optional) name and version of the program used to create the .torrent"""
    created_by: str | None = None
    """(optional) the string encoding format used to generate the pieces part of the info dictionary in the .torrent 
    metafile"""
    encoding: str | None = None

    def get_announce_urls(self):
        if self.announce_list:
            return list(itertools.chain(*self.announce_list))
        return [self.announce]

    @classmethod
    def from_dict(cls, data):
        info_hash = sha1(benencode(data.get("info"))).digest()
        info = _format_keys(data.pop("info"))
        if "private" in info:
            info["private"] = info["private"] == 1
        if "files" in info:
            info["files"] = [IndividualFileInfo(
                name=os.path.join(*file.pop("path")),
                **file
            ) for file in info["files"]]
            info = MultiFileInfo(**info)
        else:
            info = SingleFileInfo(**info)
        return cls(info=info, info_hash=info_hash, **_format_keys(data, cls))

    def encode(self):
        data = dataclasses.asdict(self)
        to_encode = {
            key.replace("_", " "): val for key, val in data.items() if val is not None
        }
        to_encode["info"] = self.info.to_dict()
        return benencode(to_encode)


def load_torrent_metadata(torrent_path):
    with open(torrent_path, "rb") as f:
        return TorrentMetaInfo.from_dict(bendecode(f.read()))
