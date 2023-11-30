import urllib.parse

import aiohttp
import async_timeout

from guit_torrent.bencoding import bendecode
from guit_torrent.metainfo import TorrentMetaInfo
from guit_torrent.tracker.base import BaseTracker, TrackerResponse, TrackerRequestParameters
from guit_torrent.utils import _format_keys


class TrackerHTTP(BaseTracker):
    def __init__(self, address: urllib.parse.ParseResult, torrent_metainfo: TorrentMetaInfo, peer_id,
                 get_downloaded_cb, update_data_cb=None):
        super().__init__(address, torrent_metainfo, peer_id, get_downloaded_cb, update_data_cb)
        self._session = None

    async def close(self):
        if self._session:
            await self._session.close()
        await super().close()

    async def _send_announce(self, params: TrackerRequestParameters) -> "TrackerResponse":
        if not self._session:
            self._session = aiohttp.ClientSession()
        url = self.address.geturl() + "?" + params.get_url_query()
        # console.log(f"Sending connect to HTTP tracker {self}...")
        with async_timeout.timeout(5):
            async with self._session.get(url) as response:
                res = TrackerResponse(**_format_keys(bendecode(await response.read()), TrackerResponse))
                self.connected = True
                return res
