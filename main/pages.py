from pathlib import Path

from .event import Database, Session, Speaker


def generate_pages(database: Database, output_dir: Path) -> None:
    for speaker in database:
        ...  # TODO
