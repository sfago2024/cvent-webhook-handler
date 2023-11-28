import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum, auto
from pathlib import Path
from textwrap import dedent
from typing import Any, Self

import requests
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth

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
    speaker_category: list[str] = []
    speakers: list[str] = []
    timezone_name: str
    updated_date: date

    class Config:
        alias_generator = camel_case
        frozen = True


class SpeakerData(BaseModel):
    presenter_at: list[str] = []
    speaker_biography: str
    speaker_display_name: str
    speaker_first_name: str
    speaker_last_name: str
    speaker_stub: str
    speaker_title: str
    updated_date: date

    class Config:
        alias_generator = camel_case
        frozen = True


class SpeakerCategory(Enum):
    COMPOSER = auto()
    PERFORMER = auto()
    PRESENTER = auto()

    def __repr__(self) -> str:
        return self.name


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
    def slugified_name(self) -> str:
        return slugify(self.data.session_name)

    @property
    def url_relpath(self) -> str:
        return f"sessions/{self.slugified_name}/"

    def link(self, base_url: str) -> str:
        return f'<a href="{base_url}{self.url_relpath}">{self.data.session_name}</a>'


@dataclass
class Speaker:
    data: SpeakerData
    categories: list[SpeakerCategory]
    updated: bool = True
    deleted: bool = False

    @property
    def filename(self) -> str:
        return f"{self.stub}.json"

    @property
    def stub(self) -> str:
        return self.data.speaker_stub

    @property
    def slugified_name(self) -> str:
        return slugify(self.data.speaker_display_name)

    @property
    def url_relpath(self) -> str:
        if SpeakerCategory.COMPOSER in self.categories:
            return f"composers/{self.slugified_name}/"
        if SpeakerCategory.PERFORMER in self.categories:
            return f"performers/{self.slugified_name}/"
        else:
            return f"speakers/{self.slugified_name}/"

    def link(self, base_url: str) -> str:
        return f'<a href="{base_url}{self.url_relpath}">{self.data.speaker_display_name}</a>'


@dataclass
class Database:
    sessions: dict[str, Session]
    speakers: dict[str, Speaker]
    speaker_categories: dict[str, list[SpeakerCategory]]

    @classmethod
    def load(cls, data_dir: Path) -> Self:
        self = cls({}, {}, {})
        try:
            for path in (data_dir / "sessions").iterdir():
                session = Session(SessionData.parse_file(path), updated=False)
                if session.stub in self.sessions:
                    logger.warn(
                        "Duplicate session stub: %s (%s)",
                        session.stub,
                        session.slugified_name,
                    )
                self.sessions[session.stub] = session

                for speaker_stub, category in zip(
                    session.data.speakers, session.data.speaker_category
                ):
                    if category in {"Organist", "Performer"}:
                        self.speaker_categories.setdefault(speaker_stub, []).append(
                            SpeakerCategory.PERFORMER
                        )
                    elif category in {"New Music Composer"}:
                        self.speaker_categories.setdefault(speaker_stub, []).append(
                            SpeakerCategory.COMPOSER
                        )
                    elif category in {
                        "Speaker",
                        "Panelist",
                        "Presenter",
                        "Workshop Presenter",
                        "Moderator",
                    }:
                        self.speaker_categories.setdefault(speaker_stub, []).append(
                            SpeakerCategory.PRESENTER
                        )
                    else:
                        logger.warning(
                            "Unknown speaker category %s in %s",
                            category,
                            session.slugified_name,
                        )
        except FileNotFoundError:
            logger.warn("FileNotFound while loading sessions", exc_info=True)
        try:
            for path in (data_dir / "speakers").iterdir():
                data = SpeakerData.parse_file(path)
                categories = self.speaker_categories.get(data.speaker_stub, [])
                speaker = Speaker(data, categories, updated=False)
                if speaker.stub in self.speakers:
                    logger.warn(
                        "Duplicate speaker stub: %s (%s)",
                        speaker.stub,
                        speaker.slugified_name,
                    )
                self.speakers[speaker.stub] = speaker
        except FileNotFoundError:
            logger.warn("FileNotFound while loading speakers", exc_info=True)
        return self

    def save(self, data_dir: Path) -> None:
        data_dir.mkdir(exist_ok=True)
        (data_dir / "sessions").mkdir(exist_ok=True)
        for session in self.sessions.values():
            if session.updated:
                path = data_dir / "sessions" / session.filename
                path.write_text(session.data.json(by_alias=True))
                logger.info("Wrote %s", path)
        (data_dir / "speakers").mkdir(exist_ok=True)
        for speaker in self.speakers.values():
            if speaker.updated:
                path = data_dir / "speakers" / speaker.filename
                path.write_text(speaker.data.json(by_alias=True))
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
                existing.updated = True
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
                existing.updated = True
                return True
        else:
            categories = self.speaker_categories.get(data.speaker_stub, [])
            speaker = Speaker(data, categories)
            self.speakers[speaker.stub] = speaker
            return True


def handle_event(event: dict, database: Database, mailgun_api_key: str) -> bool:
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
    elif event_type == "InviteeOrGuestAccepted":
        if message["admissionItem"] == "Convention Registration – SF Select Circle":
            notify_about_circle_registration(message, mailgun_api_key)
        else:
            logger.warning(
                "Invitee/Guest accepted with admission item %r",
                message["admissionItem"],
            )
            for line in json.dumps(message, indent=4).splitlines():
                logger.debug("full message: %s", line)
    else:
        raise ValueError(f"Unrecognized event type {event_type!r}")
    return changed


def notify_about_circle_registration(
    message: dict[str, Any], mailgun_api_key: str
) -> None:
    subject = f"SF Select Circle registration: {message.get('fullName')}"
    body = dedent(
        f"""
        Full Name: {message.get("fullName")}
        First Name: {message.get("firstName")}
        Last Name: {message.get("lastName")}
        Email: {message.get("email")}
        Home Phone: {message.get("homePhone")}
        Mobile Phone: {message.get("mobilePhone")}
        Work Phone: {message.get("workPhone")}
        """
    )

    data = {
        "from": "sfago2024 Notifications <notifications@mg.sfago2024.org>",
        # "to": "Brian Larsen <donate@sfago2024.org>",
        # "cc": "Matthew Burt <matthewburt@gmail.com>",
        # "cc": "Colin Chan <colin@sfago2024.org>",
        "to": "Colin Chan <colin@sfago2024.org>",
        "subject": subject,
        "text": body,
    }
    requests.post(
        "https://api.mailgun.net/v3/mg.sfago2024.org/messages",
        data=data,
        auth=HTTPBasicAuth("api", mailgun_api_key),
    )


SLUG_REPLACE_PATTERN: re.Pattern[str] = re.compile(r"[^\w\d]+")


def slugify(s: str) -> str:
    return SLUG_REPLACE_PATTERN.sub("-", s.casefold()).strip("-")
