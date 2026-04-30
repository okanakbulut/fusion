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
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(output)))

    assert output.exists()
    data = msgspec.yaml.decode(output.read_bytes())
    assert "tables" in data
    assert "users" in data["tables"]
    assert "posts" in data["tables"]


def test_snapshot_creates_parent_directories(tmp_path, models_module):
    from fusion.cli import cmd_snapshot

    output = tmp_path / "migrations" / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(output)))

    assert output.exists()


def test_snapshot_yaml_is_sorted_deterministically(tmp_path, models_module):
    from fusion.cli import cmd_snapshot

    output = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(output)))

    raw = output.read_text()
    assert raw.index("posts") < raw.index("users")


# ---------------------------------------------------------------------------
# Slice 2: fusion check
# ---------------------------------------------------------------------------


def test_check_reports_up_to_date_when_no_drift(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(snap)))
    cmd_check(argparse.Namespace(module=models_module, snapshot=str(snap)))

    assert "Up to date" in capsys.readouterr().out


def test_check_reports_pending_changes_when_drift(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(snap)))

    class Comment(Model):
        id: int | None = None
        body: str

    Comment.__module__ = models_module
    sys.modules[models_module].Comment = Comment

    with pytest.raises(SystemExit):
        cmd_check(argparse.Namespace(module=models_module, snapshot=str(snap)))

    assert "create_table" in capsys.readouterr().out


def test_check_warns_when_no_snapshot_exists(tmp_path, models_module, capsys):
    from fusion.cli import cmd_check

    cmd_check(
        argparse.Namespace(
            module=models_module,
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
                module=models_module,
                dsn=None,
                snapshot=str(tmp_path / "snapshot.yaml"),
                drop=False,
            )
        )

    assert "DSN" in capsys.readouterr().err


def test_migrate_nothing_to_do_when_snapshot_matches(tmp_path, models_module, monkeypatch, capsys):
    from fusion.cli import cmd_migrate, cmd_snapshot

    snap = tmp_path / "snapshot.yaml"
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(snap)))

    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    cmd_migrate(
        argparse.Namespace(
            module=models_module,
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
                module=models_module,
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
                module=models_module,
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
    cmd_snapshot(argparse.Namespace(module=models_module, output=str(snap)))

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
