# fusion

ASGI web framework with built-in DI, OpenAPI generation, ORM, and MCP support.

## Stack

- Python 3.14, [msgspec](https://github.com/jcrist/msgspec), [pypika](https://github.com/kayak/pypika), asyncpg
- Tests: pytest + pytest-cov (coverage must stay ≥ 98%)
- Lint: ruff, pyright

## Commands

```bash
uv sync --extra dev          # install all deps

uv run pytest                # tests + doctests + coverage
uv run pytest --cov=src/fusion --cov-report=term-missing  # with missing lines

.venv/bin/python -m ruff check src/                  # lint
.venv/bin/python -m ruff format src/ tests/          # format (auto-fix)
.venv/bin/python -m ruff format --check src/ tests/  # format (check only, what CI runs)
.venv/bin/python -m pyright src/                     # type-check
```

## Before pushing to GitHub

Run pre-commit checks:

```bash
uv run pre-commit run --all-files
```

Or install the hooks once so they run automatically on every commit:

```bash
uv run pre-commit install --hook-type commit-msg --hook-type pre-commit
```

## Conventions

- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `chore:`, `test:`, `bump:`).
- ORM queries are lazy — `.build()` returns `(sql, params)`, nothing hits the DB until `.fetch()`.
- `Exp` is an escape hatch for raw SQL — never interpolate user input into it.
