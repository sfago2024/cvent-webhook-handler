import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Self

from pydantic import BaseModel

logger = logging.getLogger(__name__)


def camel_case(name: str) -> str:
    first, *rest = name.split("_")
    return first + "".join(map(str.capitalize, rest))


class SessionData(BaseModel):
    session_description: str
    session_end_date_time: datetime
    session_name: str
    session_start_date_time: datetime
    session_stub: str
    speaker_category: list[str]
    speakers: list[str]
    timezone_name: str
    updated_date: date

    class Config:
        alias_generator = camel_case
        allow_mutation = False


class SpeakerData(BaseModel):
    presenter_at: list[str]
    speaker_biography: str
    speaker_display_name: str
    speaker_first_name: str
    speaker_last_name: str
    speaker_stub: str
    speaker_title: str
    updated_date: date

    class Config:
        alias_generator = camel_case
        allow_mutation = False


@dataclass
class Session:
    data: SessionData
    updated: bool = True
    deleted: bool = False

    @property
    def filename(self) -> str:
        return f"{self.stub}.json"

    @property
    def stub(self) -> str:
        return self.data.session_stub

    @property
    def url(self) -> str:
        return f"/sessions/{slugify(self.data.session_name)}.html"

    @property
    def link(self) -> str:
        return f'<a href="/sessions/{self.url}">{self.data.session_name}</a>'


@dataclass
class Speaker:
    data: SpeakerData
    updated: bool = True
    deleted: bool = False

    @property
    def filename(self) -> str:
        return f"{self.stub}.json"

    @property
    def stub(self) -> str:
        return self.data.speaker_stub

    @property
    def url(self) -> str:
        return f"/speakers/{slugify(self.data.speaker_display_name)}.html"

    @property
    def link(self) -> str:
        return f'<a href="/speakers/{self.url}">{self.data.speaker_display_name}</a>'


@dataclass
class Database:
    sessions: dict[str, Session]
    speakers: dict[str, Speaker]

    @classmethod
    def load(cls, data_dir: Path) -> Self:
        self = cls({}, {})
        try:
            for path in (data_dir / "sessions").iterdir():
                session = Session(SessionData.parse_file(path), updated=False)
                self.sessions[session.stub] = session
        except FileNotFoundError:
            pass
        try:
            for path in (data_dir / "speakers").iterdir():
                speaker = Speaker(SpeakerData.parse_file(path), updated=False)
                self.speakers[speaker.stub] = speaker
        except FileNotFoundError:
            pass
        return self

    def save(self, data_dir: Path) -> None:
        data_dir.mkdir(exist_ok=True)
        (data_dir / "sessions").mkdir(exist_ok=True)
        for session in self.sessions.values():
            if session.updated:
                path = data_dir / "sessions" / session.filename
                path.write_text(session.data.json())
                logger.info("Wrote %s", path)
        (data_dir / "speakers").mkdir(exist_ok=True)
        for speaker in self.speakers.values():
            if speaker.updated:
                path = data_dir / "speakers" / speaker.filename
                path.write_text(speaker.data.json())
                logger.info("Wrote %s", path)

    def delete_session(self, stub: str) -> bool:
        if (existing := self.sessions.get(stub)) is not None:
            existing.deleted = True
            return True
        return False

    def delete_speaker(self, stub: str) -> bool:
        if (existing := self.speakers.get(stub)) is not None:
            existing.deleted = True
            return True
        return False

    def update_session(self, data: SessionData) -> bool:
        if (existing := self.sessions.get(data.session_stub)) is not None:
            if existing.data == data:
                return False
            else:
                existing.data = data
                return True
        else:
            session = Session(data)
            self.sessions[session.stub] = session
            return True

    def update_speaker(self, data: SpeakerData) -> bool:
        if (existing := self.speakers.get(data.speaker_stub)) is not None:
            if existing.data == data:
                return False
            else:
                existing.data = data
                return True
        else:
            speaker = Speaker(data)
            self.speakers[speaker.stub] = speaker
            return True


def handle_event(event: dict, output_dir: Path, database: Database) -> bool:
    event_type = event["eventType"]
    message, *others = event["message"]
    if others:
        logger.warning("Request contained additional messages")
        for line in json.dumps(others, indent=4).splitlines():
            logger.debug("others: %s", line)

    logger.info("Handling event of type %r", event_type)
    if event_type == "SessionCreated":
        session = SessionData(**message)
        changed = database.update_session(session)
    elif event_type == "SessionUpdated":
        session = SessionData(**message)
        changed = database.update_session(session)
    elif event_type == "SessionDeleted":
        session_stub = message["sessionStub"]
        changed = database.delete_session(session_stub)
    elif event_type == "SpeakerCreated":
        speaker = SpeakerData(**message)
        changed = database.update_speaker(speaker)
    elif event_type == "SpeakerUpdated":
        speaker = SpeakerData(**message)
        changed = database.update_speaker(speaker)
    elif event_type == "SpeakerDeleted":
        speaker_stub = message["speakerStub"]
        changed = database.delete_speaker(speaker_stub)
    else:
        raise ValueError(f"Unrecognized event type {event_type!r}")
    return changed


def slugify(s: str) -> str:
    # TODO: Make this better
    return s.lower().replace(" ", "-")
