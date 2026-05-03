"""Fusion CLI — draft / shift / history / serve."""

import argparse
import importlib
import inspect
import pkgutil
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from fusion.orm.model import Model


def _sanitize_slug(slug: str) -> str:
    slug = slug.lower()
    slug = slug.replace(" ", "_")
    slug = re.sub(r"[^a-z0-9_]", "", slug)
    slug = re.sub(r"_+", "_", slug)
    return slug.strip("_")


def _infer_modules_from_cwd() -> list[str]:
    """Return top-level package names found in CWD when pyproject.toml is present."""
    cwd = Path.cwd()
    if not (cwd / "pyproject.toml").exists():
        return []
    packages = sorted(p.name for p in cwd.iterdir() if p.is_dir() and (p / "__init__.py").exists())
    if packages:
        cwd_str = str(cwd)
        if cwd_str not in sys.path:
            sys.path.insert(0, cwd_str)
    return packages


def _resolve_modules(args: argparse.Namespace) -> list[str]:
    modules: list[str] = args.module or []
    if not modules:
        modules = _infer_modules_from_cwd()
    if not modules:
        print(
            "Error: no module specified and no pyproject.toml found in current directory.",
            file=sys.stderr,
        )
        sys.exit(1)
    return modules


def discover_models(module_paths: list[str]) -> list[type[Model]]:
    seen: set[int] = set()
    models: list[type[Model]] = []

    def _collect(mod: object) -> None:
        for _, obj in inspect.getmembers(mod, inspect.isclass):
            if (
                issubclass(obj, Model)
                and obj is not Model
                and obj.__module__ == getattr(mod, "__name__", None)
                and id(obj) not in seen
            ):
                seen.add(id(obj))
                models.append(obj)

    for module_path in module_paths:
        mod = importlib.import_module(module_path)
        _collect(mod)

        if hasattr(mod, "__path__"):
            for _finder, name, _ispkg in pkgutil.walk_packages(
                mod.__path__, prefix=mod.__name__ + "."
            ):
                try:
                    submod = importlib.import_module(name)
                    _collect(submod)
                except ImportError:
                    pass

    return models


def _generate_filename(slug: str, migrations_dir: Path) -> str:
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    base = f"{ts}_{slug}.py"
    if not (migrations_dir / base).exists():
        return base
    counter = 1
    while (migrations_dir / f"{ts}_{slug}_{counter}.py").exists():
        counter += 1
    return f"{ts}_{slug}_{counter}.py"


def _render_shift_file(operations: list, class_name: str) -> str:
    op_class_names = sorted({type(op).__name__ for op in operations})
    imports = ", ".join(["Shift", *op_class_names])
    import_line = f"from fusion.orm.shift import {imports}"

    op_lines = [f"        {op!r}," for op in operations]
    ops_block = "\n".join(op_lines)

    return (
        f"{import_line}\n\n\nclass {class_name}(Shift):\n    operations = [\n{ops_block}\n    ]\n"
    )


def _slug_to_class_name(slug: str) -> str:
    return "".join(part.capitalize() for part in slug.split("_") if part)


def cmd_draft(args: argparse.Namespace) -> None:
    from fusion.orm.shift.draft import diff_states, models_to_schema_state
    from fusion.orm.shift.replay import replay_shifts

    slug = _sanitize_slug(args.slug)
    migrations_dir = Path(args.migrations_dir)

    shift_files = sorted(migrations_dir.glob("*.py")) if migrations_dir.exists() else []
    current_state = replay_shifts(shift_files)

    models = discover_models(_resolve_modules(args))
    target_state = models_to_schema_state(models)

    ops = diff_states(current_state, target_state)
    if not ops:
        print("No changes detected. Nothing to draft.")
        return

    migrations_dir.mkdir(parents=True, exist_ok=True)
    filename = _generate_filename(slug, migrations_dir)
    class_name = _slug_to_class_name(slug)
    content = _render_shift_file(ops, class_name)
    (migrations_dir / filename).write_text(content)
    print(f"Wrote {migrations_dir / filename}")


def cmd_shift(args: argparse.Namespace) -> None:
    import asyncio
    import os

    from fusion.orm.shift.apply import apply_shifts

    dsn = args.dsn or os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        print(
            "Error: DSN required. Pass --dsn or set POSTGRES_DSN / DATABASE_URL.",
            file=sys.stderr,
        )
        sys.exit(1)

    migrations_dir = Path(args.migrations_dir)
    shift_files = sorted(migrations_dir.glob("*.py")) if migrations_dir.exists() else []

    names = ", ".join(f.stem for f in shift_files) if shift_files else "(none)"
    try:
        asyncio.run(apply_shifts(dsn, shift_files))
    except Exception as exc:
        print(f"Error applying shifts [{names}]: {exc}", file=sys.stderr)
        sys.exit(1)


def cmd_history(args: argparse.Namespace) -> None:
    import asyncio
    import os

    from fusion.orm.shift.history import get_history

    dsn = args.dsn or os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        print(
            "Error: DSN required. Pass --dsn or set POSTGRES_DSN / DATABASE_URL.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        result = asyncio.run(get_history(dsn, as_json=args.json))
        print(result, end="")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


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

    p_draft = sub.add_parser("draft", help="Generate a shift file from detected model changes")
    p_draft.add_argument("slug", help="Human-readable suffix for the migration file")
    p_draft.add_argument(
        "module", nargs="*", help="Python module(s) or package(s) containing Model subclasses"
    )
    p_draft.add_argument(
        "--migrations-dir", default="migrations", metavar="DIR", dest="migrations_dir"
    )
    p_draft.set_defaults(func=cmd_draft)

    p_shift = sub.add_parser("shift", help="Apply unapplied shift files to the database")
    p_shift.add_argument("--dsn", default=None, help="PostgreSQL connection string")
    p_shift.add_argument(
        "--migrations-dir", default="migrations", metavar="DIR", dest="migrations_dir"
    )
    p_shift.set_defaults(func=cmd_shift)

    p_history = sub.add_parser("history", help="Show applied shifts with timestamps")
    p_history.add_argument("--dsn", default=None, help="PostgreSQL connection string")
    p_history.add_argument("--json", action="store_true", help="Output as JSON array")
    p_history.set_defaults(func=cmd_history)

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
