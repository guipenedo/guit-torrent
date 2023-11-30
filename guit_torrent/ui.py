from typing import Optional

from rich.live import Live
from rich.progress import Progress, TextColumn, BarColumn, DownloadColumn, TimeRemainingColumn, TransferSpeedColumn, \
    SpinnerColumn, MofNCompleteColumn, Task
from rich.table import Table, Column
from rich.text import Text


class IntCompletionColumn(MofNCompleteColumn):

    def __init__(self, title: str = None, field_names: tuple = ("completed",),
                 caption: str = None, separator: str = "/", table_column: Optional[Column] = None,
                 color: str = "blue"):
        self.separator = separator
        self.field_names = field_names
        self.title = title
        self.caption = caption
        self.color = color
        super().__init__(table_column=table_column)

    def render(self, task: "Task") -> Text:
        values = self.separator.join([str(task.fields.get(column, "?")) for column in self.field_names])

        return Text.from_markup(
            (f"[{self.color}]{self.title} " if self.title else "") +
            (f"[{self.color}]({self.caption})" if self.caption else "") +
            (f":[reset] " if self.title or self.caption else "") +
            values
        )


torrent_progress = Progress(
    TextColumn("[bold]{task.description}", justify="center"),
    SpinnerColumn(),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    IntCompletionColumn("peers", caption="alive/conn/total", field_names=("peers_alive", "peers_conn", "peers_total")),
    IntCompletionColumn("pieces", caption="dl/avail/total", field_names=("pieces_downloaded", "pieces_available",
                                                                         "pieces_total"), color="yellow"),
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
    IntCompletionColumn("trackers", field_names=("trackers_conn", "trackers_total"), color="orange"),
)

torrent_task = torrent_progress.add_task("Fetching metadata...")

file_progress = Progress(
    TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
    BarColumn(bar_width=None),
    "[progress.percentage]{task.percentage:>3.1f}%",
    "•",
    DownloadColumn(),
    "•",
    TransferSpeedColumn(),
    "•",
    TimeRemainingColumn(),
)

progress_table = Table(show_header=False, show_edge=False)
progress_table.add_row(torrent_progress)
progress_table.add_row(file_progress)

ui_view = Live(progress_table)
console = ui_view.console


def get_progress_task_for_file(file) -> int:
    return file_progress.add_task(file.name, filename=file.name, total=file.length)


def ui_update_files_progress(torrent):
    for file in torrent.files:
        if file.progress_task:
            file_progress.update(file.progress_task, completed=file.downloaded_bytes(torrent.pieces),
                                 filename=file.name)


def ui_update_overall(client, available_pieces):
    torrent_progress.update(
        torrent_task, description=client.torrent.name, completed=client.torrent.downloaded_bytes,
        total=client.torrent.length, peers_alive=len([peer for peer in client.peers if peer.alive]),
        peers_conn=len(client.peers), peers_total=len(client.tracker_manager.peers),
        pieces_downloaded=sum(piece.confirmed for piece in client.torrent.pieces),
        pieces_available=available_pieces, pieces_total=len(client.torrent.pieces),
        trackers_conn=sum(tracker.connected and not tracker.error for tracker in client.tracker_manager.trackers),
        trackers_total=len(client.tracker_manager.trackers)
    )
