"""Fusion CLI — snapshot / check / migrate / serve."""

import argparse
import importlib
import inspect
import subprocess
import sys
from pathlib import Path

import msgspec.yaml

from fusion.orm.migration.apply import to_ddl
from fusion.orm.migration.diff import diff
from fusion.orm.migration.snapshot import serialize
from fusion.orm.model import Model


def discover_models(module_path: str) -> list[type[Model]]:
    mod = importlib.import_module(module_path)
    return [
        obj
        for _, obj in inspect.getmembers(mod, inspect.isclass)
        if issubclass(obj, Model) and obj is not Model and obj.__module__ == mod.__name__
    ]


def cmd_snapshot(args: argparse.Namespace) -> None:
    models = discover_models(args.module)
    snapshot = serialize(models)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(msgspec.yaml.encode(snapshot, order="sorted"))
    print(f"Snapshot written to {output}")


def cmd_check(args: argparse.Namespace) -> None:
    snapshot_path = Path(args.snapshot)
    if not snapshot_path.exists():
        print("No snapshot found. Run `fusion snapshot <module>` first.")
        return

    models = discover_models(args.module)
    current = serialize(models)
    stored = msgspec.yaml.decode(snapshot_path.read_bytes())
    changes = diff(stored, current)

    if not changes:
        print("Up to date.")
        return

    print("Pending schema changes:")
    for change in changes:
        op = change["op"]
        table = change.get("table", "")
        col = change.get("column", "")
        print(f"  {op}: {table}" + (f".{col}" if col else ""))
    print("Run `fusion snapshot <module>` to update the snapshot.")
    sys.exit(1)


def cmd_migrate(args: argparse.Namespace) -> None:
    import asyncio
    import os

    import asyncpg

    dsn = args.dsn or os.environ.get("POSTGRES_DSN")
    if not dsn:
        print(
            "Error: DSN required. Pass --dsn or set the POSTGRES_DSN env var.",
            file=sys.stderr,
        )
        sys.exit(1)

    models = discover_models(args.module)
    current = serialize(models)
    snapshot_path = Path(args.snapshot)
    stored = msgspec.yaml.decode(snapshot_path.read_bytes()) if snapshot_path.exists() else {}
    changes = diff(stored, current, allow_drop=args.drop)

    if not changes:
        print("Nothing to migrate.")
        return

    statements = to_ddl(changes)

    async def _run() -> None:
        conn = await asyncpg.connect(dsn)
        try:
            async with conn.transaction():
                for stmt in statements:
                    await conn.execute(stmt)
        finally:
            await conn.close()

    asyncio.run(_run())

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_bytes(msgspec.yaml.encode(current, order="sorted"))
    print(f"Applied {len(statements)} statement(s). Snapshot updated.")


def cmd_serve(args: argparse.Namespace) -> None:
    cmd = ["uvicorn", args.app, "--host", args.host, "--port", str(args.port)]
    if args.reload:
        cmd.append("--reload")
    try:
        subprocess.run(cmd, check=True)  # noqa: S603
    except FileNotFoundError:
        print(
            "Error: uvicorn is not installed. Run: pip install uvicorn",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(prog="fusion", description="Fusion framework CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_snap = sub.add_parser("snapshot", help="Write model snapshot to YAML")
    p_snap.add_argument("module", help="Python module containing Model subclasses")
    p_snap.add_argument("--output", default="migrations/snapshot.yaml", metavar="FILE")
    p_snap.set_defaults(func=cmd_snapshot)

    p_check = sub.add_parser("check", help="Show pending schema changes")
    p_check.add_argument("module", help="Python module containing Model subclasses")
    p_check.add_argument("--snapshot", default="migrations/snapshot.yaml", metavar="FILE")
    p_check.set_defaults(func=cmd_check)

    p_migrate = sub.add_parser("migrate", help="Apply pending migrations to the database")
    p_migrate.add_argument("module", help="Python module containing Model subclasses")
    p_migrate.add_argument("--dsn", default=None, help="PostgreSQL connection string")
    p_migrate.add_argument("--snapshot", default="migrations/snapshot.yaml", metavar="FILE")
    p_migrate.add_argument("--drop", action="store_true", help="Allow destructive operations")
    p_migrate.set_defaults(func=cmd_migrate)

    p_serve = sub.add_parser("serve", help="Run the application with uvicorn")
    p_serve.add_argument("app", help="ASGI app path (e.g. myapp:app)")
    p_serve.add_argument("--host", default="0.0.0.0", metavar="HOST")  # noqa: S104
    p_serve.add_argument("--port", default=8000, type=int, metavar="PORT")
    p_serve.add_argument("--reload", action="store_true", help="Enable auto-reload")
    p_serve.set_defaults(func=cmd_serve)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
