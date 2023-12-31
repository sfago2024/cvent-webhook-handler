import asyncio
import json
import logging
import os
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import Annotated, cast

import uvicorn
from fastapi import Body, FastAPI, Header, HTTPException, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logger = logging.getLogger(__name__)


from .event import Database, handle_event


def make_app(*, auth_token: str, data_dir: Path, mailgun_api_key: str):
    app = FastAPI()

    @app.get("/cvent-event")
    async def auth(
        authorization: Annotated[str | None, Header()] = None,
    ):
        if authorization != auth_token:
            logger.warning("Incorrect auth: {authorization!r}")
            raise HTTPException(
                status_code=401, detail=f"Incorrect authorization: {authorization!r}"
            )
        return {"message": "Correct auth!"}

    @app.post("/cvent-event")
    async def cvent_event(
        event: Annotated[dict, Body()],
        authorization: Annotated[str | None, Header()] = None,
    ):
        if authorization != auth_token:
            logger.warning("Incorrect auth: {authorization!r}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Incorrect auth: {authorization!r}",
            )
        database = Database.load(data_dir)
        try:
            changed = handle_event(event, database, mailgun_api_key)
        except Exception as e:
            logger.warning("Failed to process request", exc_info=True)
            for line in json.dumps(event, indent=4).splitlines():
                logger.debug("event: %s", line)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{type(e).__name__}: {e}",
            )
        if not changed:
            return
        try:
            database.save(data_dir)
        except Exception:
            logger.error("Failed to save database", exc_info=True)
            return

    return app


def directory(s: str) -> Path:
    p = Path(s)
    if p.is_dir():
        return p
    else:
        raise ValueError(f"Not a directory: {p!r}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--data-dir", type=directory, required=True)
    args = parser.parse_args()

    try:
        auth_token = os.environ["CVENT_AUTH_TOKEN"]
    except KeyError:
        raise RuntimeError(f"Missing environment variable CVENT_AUTH_TOKEN")
    try:
        mailgun_api_key = os.environ["MAILGUN_API_KEY"]
    except KeyError:
        raise RuntimeError(f"Missing environment variable MAILGUN_API_KEY")

    uvicorn.run(
        make_app(
            auth_token=auth_token,
            data_dir=args.data_dir,
            mailgun_api_key=mailgun_api_key,
        ),
        port=args.port,
    )
