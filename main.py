import json
import logging
import os
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import Annotated, cast

import uvicorn
from fastapi import Body, FastAPI, Header, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def make_app(*, auth_token: str, output_dir: Path):
    app = FastAPI()

    @app.get("/")
    async def root():
        return {"message": "It is working (probably)"}

    @app.get("/cvent-event")
    async def auth(
        authorization: Annotated[str | None, Header()] = None,
    ):
        if authorization != auth_token:
            raise HTTPException(
                status_code=401, detail=f"Incorrect authorization: {authorization!r}"
            )
        return {"message": "Correct auth!"}

    @app.post("/cvent-event")
    async def handle_event(
        event: Annotated[dict, Body()],
        authorization: Annotated[str | None, Header()] = None,
    ):
        if authorization != auth_token:
            raise HTTPException(
                status_code=401, detail=f"Incorrect authorization: {authorization!r}"
            )
        now = datetime.now()
        path = output_dir / f"{now:%Y%m%d-%H%M%S.%f}.json"
        path.write_text(json.dumps(event) + "\n")
        return {"message": f"Wrote data to {path.name}"}

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
    parser.add_argument("--output-dir", type=directory, required=True)
    args = parser.parse_args()

    try:
        auth_token = os.environ["CVENT_AUTH_TOKEN"]
    except KeyError:
        raise RuntimeError(f"Missing environment variable CVENT_AUTH_TOKEN")
    uvicorn.run(
        make_app(auth_token=auth_token, output_dir=args.output_dir),
        port=args.port,
    )
