"""Tests for fusion.cli — serve command."""

import sys
from unittest.mock import patch

import pytest


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


def test_main_serve_subcommand(monkeypatch):
    from fusion.cli import main

    monkeypatch.setattr(sys, "argv", ["fusion", "serve", "myapp:app", "--port", "9000"])
    with patch("subprocess.run", lambda cmd, check: None):
        main()
