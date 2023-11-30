import asyncio
from argparse import ArgumentParser

from guit_torrent.client import TorrentClient

argparser = ArgumentParser("Launch download of a torrent file")
argparser.add_argument("torrent", help="Path to a .torrent file", type=str)
argparser.add_argument("-o", "--output", help="Main output folder. Defaults to downloads/", type=str,
                       default="downloads")


if __name__ == "__main__":
    args = argparser.parse_args()
    client = TorrentClient(args.torrent, args.output)

    loop = asyncio.get_event_loop()
    main_task = asyncio.ensure_future(client.start())
    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(client.stop())
