"""Command-line interface for engine inspection and operations."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import secrets
import signal
from contextlib import suppress
from dataclasses import asdict
from typing import cast
from uuid import UUID

from engine.config import DatabaseConfig, secret_value
from engine.observability import configure_logging
from engine.persistence import (
    Pool,
    create_configured_pool,
    register_workflow_definition,
    start_workflow,
)
from engine.persistence.migrations import migrate
from engine.runtime.definitions import DefinitionRegistry, WorkflowDefinition
from engine.runtime.replay_check import replay_check
from engine.runtime.serialization import JSONValue
from engine.workers import WorkerRole, run_worker


def _load_definition(reference: str) -> WorkflowDefinition:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("definition must use module:attribute syntax")
    value = getattr(importlib.import_module(module_name), attribute)
    if not isinstance(value, WorkflowDefinition):
        raise TypeError(f"{reference} is not a workflow definition")
    return value


def _load_registry(reference: str) -> DefinitionRegistry:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("definitions must use module:attribute syntax")
    value = getattr(importlib.import_module(module_name), attribute)
    if not isinstance(value, DefinitionRegistry):
        raise TypeError(f"{reference} is not a definition registry")
    return value


def _json_input(raw: str) -> JSONValue:
    return cast(JSONValue, json.loads(raw))


async def _configured_pool(database_url: str, application_name: str) -> Pool:
    config = DatabaseConfig.from_env(url=database_url, application_name=application_name)
    return await create_configured_pool(config)


async def _registered_pool(database_url: str, application_name: str) -> Pool:
    await migrate(database_url)
    return await _configured_pool(database_url, application_name)


async def _run_replay_check(args: argparse.Namespace) -> int:
    definition = _load_definition(cast(str, args.definition))
    against_version = cast(int, args.against_version)
    if definition.version != against_version:
        raise ValueError(
            f"loaded definition version {definition.version} does not match "
            f"--against-version {against_version}"
        )
    pool = await _configured_pool(cast(str, args.database_url), "dwe-replay-check")
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


async def _run_register(args: argparse.Namespace) -> int:
    database_url = cast(str, args.database_url)
    registry = _load_registry(cast(str, args.definitions))
    pool = await _registered_pool(database_url, "dwe-register")
    try:
        for definition in registry.workflows:
            await register_workflow_definition(pool, definition)
    finally:
        await pool.close()
    print(
        json.dumps(
            {
                "registered": [
                    {"workflow_type": item.name, "version": item.version}
                    for item in registry.workflows
                ]
            },
            sort_keys=True,
        )
    )
    return 0


async def _run_start(args: argparse.Namespace) -> int:
    pool = await _registered_pool(cast(str, args.database_url), "dwe-start")
    try:
        started = await start_workflow(
            pool,
            workflow_type=cast(str, args.workflow_type),
            definition_version=cast(int, args.version),
            workflow_input=_json_input(cast(str, args.input)),
            queue_name=cast(str, args.queue),
        )
    finally:
        await pool.close()
    print(json.dumps(asdict(started), default=str, sort_keys=True))
    return 0


async def _run_worker(args: argparse.Namespace) -> int:
    database_url = cast(str, args.database_url)
    registry = _load_registry(cast(str, args.definitions))
    pool = await _registered_pool(database_url, "dwe-worker")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(shutdown_signal, stop.set)
    try:
        for definition in registry.workflows:
            await register_workflow_definition(pool, definition)
        roles = cast(list[WorkerRole] | None, args.role)
        await run_worker(
            pool,
            registry,
            queue_name=cast(str, args.queue),
            roles=roles or ("workflow", "activity", "maintenance"),
            idle_delay=cast(float, args.poll_interval),
            heartbeat_interval=cast(float, args.heartbeat_interval),
            stop=stop,
        )
    finally:
        await pool.close()
    return 0


def _database_argument(parser: argparse.ArgumentParser) -> None:
    default_url = secret_value("DATABASE_URL")
    parser.add_argument(
        "--database-url",
        default=default_url,
        required=default_url is None,
    )


def _run_auth_key(args: argparse.Namespace) -> int:
    token = secrets.token_urlsafe(32)
    key_id = cast(str, args.key_id)
    role = cast(str, args.role)
    digest = hashlib.sha256(token.encode()).hexdigest()
    print(
        json.dumps(
            {
                "key_id": key_id,
                "role": role,
                "token": token,
                "configuration": f"{key_id}:{role}:{digest}",
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="engine")
    subparsers = parser.add_subparsers(dest="command", required=True)
    auth_parser = subparsers.add_parser(
        "auth-key", help="generate a bearer token and its SHA-256 configuration entry"
    )
    auth_parser.add_argument("--key-id", required=True)
    auth_parser.add_argument("--role", choices=("viewer", "operator", "admin"), required=True)
    replay_parser = subparsers.add_parser(
        "replay-check", help="check a persisted history against candidate workflow code"
    )
    replay_parser.add_argument("workflow_id")
    replay_parser.add_argument("--against-version", type=int, required=True)
    replay_parser.add_argument("--definition", required=True, help="module:attribute")
    _database_argument(replay_parser)

    register_parser = subparsers.add_parser(
        "register", help="persist immutable workflow definitions from a registry"
    )
    register_parser.add_argument("--definitions", required=True, help="module:registry")
    _database_argument(register_parser)

    start_parser = subparsers.add_parser("start", help="start a registered workflow")
    start_parser.add_argument("workflow_type")
    start_parser.add_argument("--version", type=int, required=True)
    start_parser.add_argument("--input", default="null", help="JSON workflow input")
    start_parser.add_argument("--queue", default="default")
    _database_argument(start_parser)

    worker_parser = subparsers.add_parser("worker", help="run continuous worker loops")
    worker_parser.add_argument("--definitions", required=True, help="module:registry")
    worker_parser.add_argument("--queue", default="default")
    worker_parser.add_argument(
        "--role",
        action="append",
        choices=("workflow", "activity", "maintenance"),
        help="worker role to run; repeat it, or omit for all roles",
    )
    worker_parser.add_argument("--poll-interval", type=float, default=0.05)
    worker_parser.add_argument("--heartbeat-interval", type=float, default=10.0)
    _database_argument(worker_parser)
    return parser


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "auth-key":
        raise SystemExit(_run_auth_key(args))
    if args.command == "replay-check":
        raise SystemExit(asyncio.run(_run_replay_check(args)))
    if args.command == "register":
        raise SystemExit(asyncio.run(_run_register(args)))
    if args.command == "start":
        raise SystemExit(asyncio.run(_run_start(args)))
    if args.command == "worker":
        try:
            raise SystemExit(asyncio.run(_run_worker(args)))
        except KeyboardInterrupt:
            raise SystemExit(130) from None


if __name__ == "__main__":
    main()
