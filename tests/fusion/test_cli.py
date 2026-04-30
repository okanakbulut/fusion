"""Tests for fusion.cli — snapshot / check / migrate / serve commands."""

import argparse
import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import msgspec.yaml
import pytest

from fusion.orm.model import Model

# ---------------------------------------------------------------------------
# Fixture: a temp importable module with two Model subclasses
# ---------------------------------------------------------------------------


@pytest.fixture
def models_module(monkeypatch):
    module_name = "_fusion_test_cli_models"

    class User(Model):
        id: int | None = None
        email: str

    class Post(Model):
        id: int | None = None
        title: str

    User.__module__ = module_name
    Post.__module__ = module_name

    mod = types.ModuleType(module_name)
    mod.User = User
    mod.Post = Post

    monkeypatch.setitem(sys.modules, module_name, mod)
    return module_name


# ---------------------------------------------------------------------------
# Slice 1: fusion snapshot
# ---------------------------------------------------------------------------


def test_snapshot_writes_yaml_file(tmp_path, models_module):
    from fusion.cli import cmd_snapshot

    output = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(output)))

    assert output.exists()
    data = msgspec.yaml.decode(output.read_bytes())
    assert "tables" in data
    assert "users" in data["tables"]
    assert "posts" in data["tables"]


def test_snapshot_creates_parent_directories(tmp_path, models_module):
    from fusion.cli import cmd_snapshot

    output = tmp_path / "migrations" / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(output)))

    assert output.exists()


def test_snapshot_yaml_is_sorted_deterministically(tmp_path, models_module):
    from fusion.cli import cmd_snapshot

    output = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(output)))

    raw = output.read_text()
    assert raw.index("posts") < raw.index("users")


# ---------------------------------------------------------------------------
# Slice 2: fusion check
# ---------------------------------------------------------------------------


def test_check_reports_up_to_date_when_no_drift(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(snap)))
    cmd_check(argparse.Namespace(module=[models_module], snapshot=str(snap)))

    assert "Up to date" in capsys.readouterr().out


def test_check_reports_pending_changes_when_drift(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(snap)))

    class Comment(Model):
        id: int | None = None
        body: str

    Comment.__module__ = models_module
    sys.modules[models_module].Comment = Comment

    with pytest.raises(SystemExit):
        cmd_check(argparse.Namespace(module=[models_module], snapshot=str(snap)))

    assert "create_table" in capsys.readouterr().out


def test_check_warns_when_no_snapshot_exists(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check

    cmd_check(
        argparse.Namespace(
            module=[models_module],
            snapshot=str(tmp_path / "nonexistent.yaml"),
        )
    )

    assert "No snapshot found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Slice 3: fusion migrate (unit — asyncpg mocked)
# ---------------------------------------------------------------------------


def test_migrate_errors_without_dsn(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_migrate

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    with pytest.raises(SystemExit):
        cmd_migrate(
            argparse.Namespace(
                module=[models_module],
                dsn=None,
                snapshot=str(tmp_path / "snapshot.yaml"),
                drop=False,
            )
        )

    assert "DSN" in capsys.readouterr().err


def test_migrate_nothing_to_do_when_snapshot_matches(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_migrate, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(snap)))

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    cmd_migrate(
        argparse.Namespace(
            module=[models_module],
            dsn="postgresql://localhost/test",
            snapshot=str(snap),
            drop=False,
        )
    )

    assert "Nothing to migrate" in capsys.readouterr().out


def _mock_asyncpg_conn():
    mock_tx = MagicMock()
    mock_tx.__aenter__ = AsyncMock(return_value=mock_tx)
    mock_tx.__aexit__ = AsyncMock(return_value=None)

    mock_conn = MagicMock()
    mock_conn.execute = AsyncMock()
    mock_conn.close = AsyncMock()
    mock_conn.transaction = MagicMock(return_value=mock_tx)

    return mock_conn


def test_migrate_applies_ddl_and_writes_snapshot(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_migrate

    snap = tmp_path / "snapshot.yaml"
    mock_conn = _mock_asyncpg_conn()

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    with patch("asyncpg.connect", AsyncMock(return_value=mock_conn)):
        cmd_migrate(
            argparse.Namespace(
                module=[models_module],
                dsn="postgresql://localhost/test",
                snapshot=str(snap),
                drop=False,
            )
        )

    assert snap.exists()
    assert "Applied" in capsys.readouterr().out


def test_migrate_uses_env_dsn_when_no_arg(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_migrate

    snap = tmp_path / "snapshot.yaml"
    mock_conn = _mock_asyncpg_conn()

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://envhost/envdb")
    with patch("asyncpg.connect", AsyncMock(return_value=mock_conn)) as mock_connect:
        cmd_migrate(
            argparse.Namespace(
                module=[models_module],
                dsn=None,
                snapshot=str(snap),
                drop=False,
            )
        )

    mock_connect.assert_awaited_once_with("postgresql://envhost/envdb")


# ---------------------------------------------------------------------------
# Slice 4: fusion serve
# ---------------------------------------------------------------------------


def test_serve_invokes_uvicorn_with_host_and_port(capsys):
    from fusion.cli import cmd_serve

    calls = []
    with patch("subprocess.run", lambda cmd, check: calls.append(cmd)):
        cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=False))

    assert calls[0] == ["uvicorn", "myapp:app", "--host", "0.0.0.0", "--port", "8000"]


def test_serve_passes_reload_flag(capsys):
    from fusion.cli import cmd_serve

    calls = []
    with patch("subprocess.run", lambda cmd, check: calls.append(cmd)):
        cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=True))

    assert "--reload" in calls[0]


def test_serve_exits_gracefully_when_uvicorn_missing(capsys):
    from fusion.cli import cmd_serve

    with patch("subprocess.run", side_effect=FileNotFoundError):
        with pytest.raises(SystemExit):
            cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=False))

    assert "uvicorn" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Slice 5: multiple modules and package discovery
# ---------------------------------------------------------------------------


def test_snapshot_merges_multiple_modules(tmp_path, monkeypatch):
    from fusion.cli import cmd_snapshot

    mod_a = "_fusion_test_mod_a"
    mod_b = "_fusion_test_mod_b"

    class Alpha(Model):
        id: int | None = None
        name: str

    class Beta(Model):
        id: int | None = None
        value: str

    Alpha.__module__ = mod_a
    Beta.__module__ = mod_b

    a = types.ModuleType(mod_a)
    a.Alpha = Alpha
    b = types.ModuleType(mod_b)
    b.Beta = Beta

    monkeypatch.setitem(sys.modules, mod_a, a)
    monkeypatch.setitem(sys.modules, mod_b, b)

    output = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[mod_a, mod_b], output=str(output)))

    data = msgspec.yaml.decode(output.read_bytes())
    assert "alphas" in data["tables"]
    assert "betas" in data["tables"]


def test_discover_models_deduplicates_across_modules(monkeypatch):
    from fusion.cli import discover_models

    mod_a = "_fusion_test_dedup_a"
    mod_b = "_fusion_test_dedup_b"

    class Shared(Model):
        id: int | None = None
        x: str

    Shared.__module__ = mod_a

    a = types.ModuleType(mod_a)
    a.Shared = Shared
    b = types.ModuleType(mod_b)
    b.Shared = Shared  # same class object imported into b

    monkeypatch.setitem(sys.modules, mod_a, a)
    monkeypatch.setitem(sys.modules, mod_b, b)

    models = discover_models([mod_a, mod_b])
    assert models.count(Shared) == 1


def test_discover_models_from_package(tmp_path, monkeypatch):
    from fusion.cli import discover_models

    pkg_dir = tmp_path / "mypkg"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")
    (pkg_dir / "users.py").write_text(
        "from fusion.orm.model import Model\n\n"
        "class User(Model):\n"
        "    id: int | None = None\n"
        "    email: str\n"
    )
    (pkg_dir / "posts.py").write_text(
        "from fusion.orm.model import Model\n\n"
        "class Post(Model):\n"
        "    id: int | None = None\n"
        "    title: str\n"
    )

    monkeypatch.syspath_prepend(str(tmp_path))

    models = discover_models(["mypkg"])
    names = {m.__name__ for m in models}
    assert "User" in names
    assert "Post" in names


# ---------------------------------------------------------------------------
# Slice 6: schema-qualified tables
# ---------------------------------------------------------------------------


def _schema_module(monkeypatch, module_name: str) -> str:
    class Metric(Model):
        __schema__ = "analytics"
        id: int | None = None
        name: str

    Metric.__module__ = module_name
    mod = types.ModuleType(module_name)
    mod.Metric = Metric
    monkeypatch.setitem(sys.modules, module_name, mod)
    return module_name


def test_snapshot_captures_schema(tmp_path, monkeypatch):
    from fusion.cli import cmd_snapshot

    module_name = _schema_module(monkeypatch, "_fusion_test_schema_snap")
    output = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=[module_name], output=str(output)))

    data = msgspec.yaml.decode(output.read_bytes())
    assert data["tables"]["metrics"]["schema"] == "analytics"


def test_schema_table_ddl_is_qualified():
    from fusion.orm.migration.apply import to_ddl

    changes = [
        {
            "op": "create_table",
            "table": "metrics",
            "schema": "analytics",
            "columns": {
                "id": {"type": "SERIAL", "nullable": False, "primary_key": True},
                "name": {"type": "TEXT", "nullable": False},
            },
        }
    ]
    stmts = to_ddl(changes)
    assert stmts[0].startswith('CREATE TABLE "analytics"."metrics"')


def test_migrate_schema_table_applies_qualified_ddl(tmp_path, monkeypatch, capsys):
    from fusion.cli import cmd_migrate

    module_name = _schema_module(monkeypatch, "_fusion_test_schema_migrate")
    snap = tmp_path / "snapshot.yaml"
    mock_conn = _mock_asyncpg_conn()

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    with patch("asyncpg.connect", AsyncMock(return_value=mock_conn)):
        cmd_migrate(
            argparse.Namespace(
                module=[module_name],
                dsn="postgresql://localhost/test",
                snapshot=str(snap),
                drop=False,
            )
        )

    executed = [call.args[0] for call in mock_conn.execute.await_args_list]
    assert any('"analytics"."metrics"' in stmt for stmt in executed)


# ---------------------------------------------------------------------------
# main() — argument parser wiring
# ---------------------------------------------------------------------------


def test_main_snapshot_subcommand(tmp_path, models_module, monkeypatch):
    from fusion.cli import main

    monkeypatch.setattr(
        sys, "argv", ["fusion", "snapshot", models_module, "--output", str(tmp_path / "s.yaml")]
    )
    main()
    assert (tmp_path / "s.yaml").exists()


def test_main_check_subcommand(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_snapshot, main

    snap = tmp_path / "s.yaml"
    cmd_snapshot(argparse.Namespace(module=[models_module], output=str(snap)))

    monkeypatch.setattr(sys, "argv", ["fusion", "check", models_module, "--snapshot", str(snap)])
    main()
    assert "Up to date" in capsys.readouterr().out


def test_main_migrate_subcommand(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import main

    snap = tmp_path / "s.yaml"
    mock_conn = _mock_asyncpg_conn()

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fusion",
            "migrate",
            models_module,
            "--dsn",
            "postgresql://localhost/test",
            "--snapshot",
            str(snap),
        ],
    )
    with patch("asyncpg.connect", AsyncMock(return_value=mock_conn)):
        main()

    assert "Applied" in capsys.readouterr().out


def test_main_serve_subcommand(monkeypatch):
    from fusion.cli import main

    monkeypatch.setattr(sys, "argv", ["fusion", "serve", "myapp:app", "--port", "9000"])
    with patch("subprocess.run", lambda cmd, check: None):
        main()
