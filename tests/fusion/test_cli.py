"""Tests for fusion.cli — discover_models / serve commands."""

import sys
import types
from unittest.mock import patch

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
# fusion serve
# ---------------------------------------------------------------------------


def test_serve_invokes_uvicorn_with_host_and_port(capsys):
    import argparse

    from fusion.cli import cmd_serve

    calls = []
    with patch("subprocess.run", lambda cmd, check: calls.append(cmd)):
        cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=False))

    assert calls[0] == ["uvicorn", "myapp:app", "--host", "0.0.0.0", "--port", "8000"]


def test_serve_passes_reload_flag(capsys):
    import argparse

    from fusion.cli import cmd_serve

    calls = []
    with patch("subprocess.run", lambda cmd, check: calls.append(cmd)):
        cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=True))

    assert "--reload" in calls[0]


def test_serve_exits_gracefully_when_uvicorn_missing(capsys):
    import argparse

    from fusion.cli import cmd_serve

    with patch("subprocess.run", side_effect=FileNotFoundError):
        with pytest.raises(SystemExit):
            cmd_serve(argparse.Namespace(app="myapp:app", host="0.0.0.0", port=8000, reload=False))

    assert "uvicorn" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# discover_models
# ---------------------------------------------------------------------------


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
# main() — argument parser wiring
# ---------------------------------------------------------------------------


def test_main_serve_subcommand(monkeypatch):
    from fusion.cli import main

    monkeypatch.setattr(sys, "argv", ["fusion", "serve", "myapp:app", "--port", "9000"])
    with patch("subprocess.run", lambda cmd, check: None):
        main()
