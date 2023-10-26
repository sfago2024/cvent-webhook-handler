import logging
import shutil
from asyncio.subprocess import PIPE, create_subprocess_exec
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from pathlib import Path
from textwrap import dedent
from typing import AsyncIterator

from .event import Database, Session, Speaker, SpeakerCategory

logger = logging.getLogger(__name__)


def render_page(path: str, title: str, content: str):
    date = datetime.now()
    return dedent(
        """\
        +++
        title = '''{title}'''
        path = '''{path}'''
        template = "future.html"
        +++

        <p class="todo">
        <strong>NOTE:</strong> This page is automatically generated based on data from Cvent.
        But, I'm aware of several issues with the generated pages at the moment:
        many dates & times are wrong, and some sessions & speakers are missing altogether!
        </p>

        {content}
        """
    ).format(**locals())


def speaker_page(path: str, speaker: Speaker, base_url: str, database: Database) -> str:
    if stubs := speaker.data.presenter_at:
        sessions = "<ul>"
        for stub in stubs:
            if session := database.sessions.get(stub):
                sessions += f"<li>{session.link(base_url)}</li>"
            else:
                sessions += f"<li>(unknown session with identifier {stub})</li>"
    else:
        sessions = "<p>None yet</p>"
    content = dedent(
        """\
        <h1>{speaker.data.speaker_display_name}</h1>
        <h2>Biography</h2>
        <p>{speaker.data.speaker_biography}</p>
        <h2>Sessions</h2>
        {sessions}
        """
    ).format(**locals())
    return render_page(
        path=path, title=f"{speaker.data.speaker_display_name}", content=content
    )


def session_page(path: str, session: Session, base_url: str, database: Database) -> str:
    if stubs := session.data.speakers:
        speakers = "<ul>"
        speaker_types = set()
        for stub in stubs:
            if speaker := database.speakers.get(stub):
                speaker_types.update(database.speaker_categories.get(speaker.stub, []))
                speakers += f"<li>{speaker.link(base_url)}</li>"
            else:
                speakers += f"<li>(unknown speaker with identifier {stub})</li>"
        speakers += "</ul>"
        match speaker_types:
            case [SpeakerCategory.PERFORMER]:
                speaker_label = "Performers"
            case [SpeakerCategory.PRESENTER]:
                speaker_label = "Presenters"
            case _:
                speaker_label = "People"
    else:
        speaker_label = None
        speakers = None
    content = dedent(
        """\
        <h1>{session.data.session_name}</h1>
        <h2>Date/Time</h2>
        <p>{session.data.session_start_date_time:%A, %B %d, %Y}<br>
        {session.data.session_start_date_time:%I:%M %p} – {session.data.session_end_date_time:%I:%M %p} ({session.data.timezone_name})</p>
        <h2>Description</h2>
        {session.data.session_description}
        """
    ).format(**locals())
    if speaker_label:
        content += dedent(
            """\
            <h2>{speaker_label}</h2>
            {speakers}
            """
        ).format(**locals())
    return render_page(path=path, title=f"{session.data.session_name}", content=content)


def index_page(path: str, title: str, links: list[str]) -> str:
    items = "\n".join(f"<li>{link}</li>" for link in sorted(links))
    content = dedent(
        """
        <h1>{title}</h1>
        <ul>
        {items}
        </ul>
        """
    ).format(**locals())
    return render_page(path, title, content)


def schedule_page(path: str, title: str, base_url: str, database: Database) -> str:
    days: dict[date, dict[time, list[str]]] = {}
    for session in database.sessions.values():
        start = session.data.session_start_date_time
        times = days.setdefault(start.date(), {})
        links = times.setdefault(start.time(), [])
        links.append(session.link(base_url))

    lines = []
    for date, times in sorted(days.items()):
        lines.append(f"<h2>{date:%A, %B %d, %Y}</h2>")
        for time, links in sorted(times.items()):
            lines.append(f"<h3>{time:%I:%M %p}</h3>")
            lines.append(f"<ul>")
            for link in links:
                lines.append(f"<li>{link}</li>")
            lines.append(f"</ul>")

    content = "\n".join(lines)
    return render_page(path, title, content)


@asynccontextmanager
async def manage_repo(repo_dir: Path, commit: bool) -> AsyncIterator[None]:
    proc = await create_subprocess_exec(
        "git", "status", "--porcelain", cwd=repo_dir, stdout=PIPE
    )
    stdout, _ = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"'git status' exited {proc.returncode}")
    if stdout:
        items = stdout.decode("utf-8", errors="replace").splitlines()
        if commit:
            raise RuntimeError(f"repo is not clean ({items})")
        else:
            logger.warning("repo is not clean (%s)", items)
    if commit:
        proc = await create_subprocess_exec("git", "pull", "--ff-only", cwd=repo_dir)
        if (returncode := await proc.wait()) != 0:
            raise RuntimeError(f"'git pull' exited {returncode}")
    try:
        yield
    except:
        # If we're in commit mode, clean up any mess
        if commit:
            proc = await create_subprocess_exec(
                "git", "status", "--porcelain", cwd=repo_dir, stdout=PIPE
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"'git status' exited {proc.returncode}")
            if stdout:
                logger.warn("Repo is dirty, cleaning it...")
                proc = await create_subprocess_exec("git", "stash", "-u", cwd=repo_dir)
                if (returncode := await proc.wait()) != 0:
                    raise RuntimeError(f"'git stash' exited {returncode}")
                proc = await create_subprocess_exec(
                    "git", "stash", "drop", cwd=repo_dir
                )
                await proc.wait()
        raise
    else:
        # If we're in commit mode, build and make a commit!
        if commit:
            proc = await create_subprocess_exec("zola", "build", cwd=repo_dir)
            if (returncode := await proc.wait()) != 0:
                raise RuntimeError(f"'zola build' exited {returncode}")
            proc = await create_subprocess_exec("git", "add", ".", cwd=repo_dir)
            if (returncode := await proc.wait()) != 0:
                raise RuntimeError(f"'git add' exited {returncode}")
            proc = await create_subprocess_exec(
                "git",
                "commit",
                "--author=cvent-webhook-handler <colin+cventwebhookhandler@lumeh.org>",
                "--message=cvent-webhook-handler update",
                cwd=repo_dir,
            )
            if (returncode := await proc.wait()) != 0:
                raise RuntimeError(f"'git commit' exited {returncode}")
            proc = await create_subprocess_exec("git", "push", cwd=repo_dir)
            if (returncode := await proc.wait()) != 0:
                raise RuntimeError(f"'git push' exited {returncode}")


async def generate_pages(
    database: Database, base_url: str, repo_dir: Path, commit: bool
) -> None:
    async with manage_repo(repo_dir, commit):
        output_dir = repo_dir / "content/_generated"
        if output_dir.is_dir():
            shutil.rmtree(output_dir)
        output_dir.mkdir()

        (output_dir / "schedule.md").write_text(
            schedule_page(base_url + "schedule", "Schedule", base_url, database)
        )

        links = []
        for session in database.sessions.values():
            page = session_page(
                base_url + session.url_relpath, session, base_url, database
            )
            path = output_dir / f"session-{session.slugified_name}.md"
            if path.exists():
                logger.warn("Overwriting duplicate session %s", session.slugified_name)
            path.write_text(page)
            links.append(session.link(base_url))
        (output_dir / "sessions.md").write_text(
            index_page(base_url + "sessions", "Sessions", links)
        )

        composer_links = []
        performer_links = []
        for speaker in database.speakers.values():
            page = speaker_page(
                base_url + speaker.url_relpath, speaker, base_url, database
            )
            path = output_dir / f"speaker-{speaker.slugified_name}.md"
            if path.exists():
                logger.warn("Overwriting duplicate speaker %s", speaker.slugified_name)
            path.write_text(page)
            if SpeakerCategory.COMPOSER in database.speaker_categories.get(
                speaker.stub, []
            ):
                composer_links.append(speaker.link(base_url))
            if SpeakerCategory.PERFORMER in database.speaker_categories.get(
                speaker.stub, []
            ):
                performer_links.append(speaker.link(base_url))
        (output_dir / "composers.md").write_text(
            index_page(base_url + "composers", "Composers", composer_links)
        )
        (output_dir / "performers.md").write_text(
            index_page(base_url + "performers", "Performers", performer_links)
        )
