import shutil
from datetime import datetime
from pathlib import Path
from textwrap import dedent

from .event import Database, Session, Speaker


def render_page(title: str, content: str):
    date = datetime.now()
    return dedent(
        """\
        <!DOCTYPE html>
        <html lang="en-us">
        <head>
          <meta charset="utf-8">
          <title>{title} — AGO 2024 San Francisco</title>
          <meta name="viewport" content="width=device-width,initial-scale=1">
          <link rel="stylesheet" href="/default.css">
        </head>
        <body>
          <div class="main-flex">
            <header>
              <div>
                <img src="/img/logo.png" alt="AGO 2024 San Francisco logo">
                <div class="line"></div>
                <p class="dates"><time datetime="2024-06-30">June 30</time> – <time datetime="2024-07-04">July 4, 2024</time></p>
              </div>
            </header>
            <nav>
              <a href="/schedule.html">Schedule</a>
              <a href="/sessions.html">Sessions</a>
              <a href="/speakers.html">Speakers</a>
            </nav>
            <div class="content">

        {content}

            </div>
            <div class="spacer"></div>
            <footer>
              <p>This page was last updated on {date:%A, %B %d, %Y, at %I:%M %p}.</p>
            </footer>
          </div>
        </body>
        </html>
        """
    ).format(**locals())


def speaker_page(speaker: Speaker, database: Database) -> str:
    if stubs := speaker.data.presenter_at:
        sessions = "<ul>"
        for stub in stubs:
            if session := database.sessions.get(stub):
                sessions += f"<li>{session.link}</li>"
            else:
                sessions += f"<li>(unknown session with identifier {stub})</li>"
    else:
        sessions = "<p>None yet</p>"
    content = dedent(
        """\
        <h1>{speaker.data.speaker_display_name}</h1>
        <h2>Biography<\h2>
        <p>{speaker.data.speaker_biography}</p>
        <h2>Sessions<\h2>
        {sessions}
        """
    ).format(**locals())
    return render_page(title=f"{speaker.data.speaker_display_name}", content=content)


def session_page(session: Session, database: Database) -> str:
    if stubs := session.data.speakers:
        speakers = "<ul>"
        for stub in stubs:
            if speaker := database.speakers.get(stub):
                speakers += f"<li>{speaker.link}</li>"
            else:
                speakers += f"<li>(unknown speaker with identifier {stub})</li>"
    else:
        speakers = "<p>None yet</p>"
    content = dedent(
        """\
        <h1>{session.data.session_name}</h1>
        <h2>Date/Time</h2>
        <p>{session.data.session_start_date_time:%A, %B %d, %Y}<br>
        {session.data.session_start_date_time:%I:%M %p} – {session.data.session_end_date_time:%I:%M %p} ({session.data.timezone_name})</p>
        <h2>Location</h2>
        (not implemented yet)
        <h2>Description<\h2>
        {session.data.session_description}
        <h2>Speakers<\h2>
        {speakers}
        """
    ).format(**locals())
    return render_page(title=f"{session.data.session_name}", content=content)


def generate_pages(database: Database, static_dir: Path, output_dir: Path) -> None:
    shutil.rmtree(output_dir)
    (output_dir).mkdir(exist_ok=True)
    shutil.copytree(static_dir, output_dir, dirs_exist_ok=True)

    (output_dir / "schedule.html").write_text(
        render_page("Schedule", "(not implemented yet)")
    )

    (output_dir / "sessions").mkdir()
    for session in database.sessions.values():
        page = session_page(session, database)
        path = Path(session.url).relative_to("/").write_text(page)

    (output_dir / "speakers").mkdir()
    for speaker in database.speakers.values():
        page = speaker_page(speaker, database)
        path = Path(speaker.url).relative_to("/").write_text(page)
