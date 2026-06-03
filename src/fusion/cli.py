"""Fusion CLI — serve."""

import argparse
import subprocess
import sys


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
