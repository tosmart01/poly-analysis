from __future__ import annotations

import argparse

from .logging_config import configure_logging
from .web import app


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run analysis-poly web server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host for web server")
    parser.add_argument("--port", type=int, default=8000, help="Bind port for web server")
    return parser


def main() -> None:
    import uvicorn

    args = _build_arg_parser().parse_args()
    configure_logging()
    uvicorn.run(app, host=args.host, port=args.port)

