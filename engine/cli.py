"""Command-line interface for engine inspection and operations."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
from dataclasses import asdict
from typing import cast
from uuid import UUID

from engine.persistence import create_pool
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.replay_check import replay_check


def _load_definition(reference: str) -> WorkflowDefinition:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("definition must use module:attribute syntax")
    value = getattr(importlib.import_module(module_name), attribute)
    if not isinstance(value, WorkflowDefinition):
        raise TypeError(f"{reference} is not a workflow definition")
    return value


async def _run_replay_check(args: argparse.Namespace) -> int:
    definition = _load_definition(cast(str, args.definition))
    against_version = cast(int, args.against_version)
    if definition.version != against_version:
        raise ValueError(
            f"loaded definition version {definition.version} does not match "
            f"--against-version {against_version}"
        )
    pool = await create_pool(cast(str, args.database_url))
    try:
        report = await replay_check(
            pool,
            workflow_id=UUID(cast(str, args.workflow_id)),
            definition=definition,
        )
    finally:
        await pool.close()
    print(json.dumps(asdict(report), default=str, sort_keys=True))
    return 0 if report.compatible else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="engine")
    subparsers = parser.add_subparsers(dest="command", required=True)
    replay_parser = subparsers.add_parser(
        "replay-check", help="check a persisted history against candidate workflow code"
    )
    replay_parser.add_argument("workflow_id")
    replay_parser.add_argument("--against-version", type=int, required=True)
    replay_parser.add_argument("--definition", required=True, help="module:attribute")
    replay_parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        required=os.environ.get("DATABASE_URL") is None,
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "replay-check":
        raise SystemExit(asyncio.run(_run_replay_check(args)))


if __name__ == "__main__":
    main()
