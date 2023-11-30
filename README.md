# guitTorrent

Python BitTorrent client implemented from scratch, built with asyncio.

[Unofficial BitTorrent specification](https://wiki.theory.org/BitTorrentSpecification)

Supports:
- multi tracker
- udp tracker
- multi file torrents
- resuming from existing files (after verifying the data)

Launch with:
```
python launcher.py (path to .torrent file) -o outputfolder
```