import asyncio
import random
import string

from guit_torrent.metainfo import TorrentMetaInfo
from guit_torrent.tracker.base import BaseTracker

DEFAULT_ANNOUNCE_INTERVAL = 2 * 60
TRACKERS_TO_TRY = 20


class TrackerManager:
    def __init__(self, torrent_metadata: TorrentMetaInfo, get_downloaded_cb=None, updates=None):
        self.peer_id = _generate_peer_id()
        self.trackers = [
            BaseTracker.from_url(announce_url, torrent_metadata, self.peer_id, get_downloaded_cb, self.update_data_cb)
            for announce_url in torrent_metadata.get_announce_urls()
        ]
        self.nr_leechers = 0
        self.nr_seeders = 0
        self.peers = set()

        self.updates = updates

    def update_data_cb(self, leechers, seeders, peers):
        self.nr_leechers = leechers
        self.nr_seeders = seeders
        self.peers.update(set(peers))
        if self.updates:
            self.updates.set()

    async def close(self):
        await asyncio.gather(*(tracker.close() for tracker in self.trackers), return_exceptions=False)

    def start(self):
        for tracker in self.trackers:
            tracker.future = asyncio.ensure_future(tracker.start())

    async def announce(self, event: str = None):
        await asyncio.gather(*[tracker.announce(event) for tracker in self.trackers])


def _generate_peer_id():
    return "-GT0001-" + "".join(random.choices(string.digits, k=12))
