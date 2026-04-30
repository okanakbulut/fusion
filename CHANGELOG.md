## v0.9.0 (2026-04-30)

### Feat

- support multi-module/package discovery and schema-qualified migrations
- add fusion CLI with snapshot, check, migrate, and serve commands

## v0.8.4 (2026-04-28)

### Fix

- filterin on joined column

## v0.8.3 (2026-04-28)

### Feat

- fix prefetch duplicate join, add integration tests with DSN-based Postgres

## v0.8.2 (2026-04-28)

### Feat

- add prefecth support for relationships
- add alias-based join filtering and startswith lookup

## v0.8.1 (2026-04-27)

### Feat

- add explicit on= join columns using field descriptors

### Fix

- apply ruff format and add format-check pre-commit hook

## v0.8.0 (2026-04-27)

### Feat

- add fusion.orm — Postgres ORM submodule

### Fix

- resolve pyright type errors in query.py
- resolve all ruff lint issues in fusion.orm

## v0.7.0 (2026-04-22)

### Feat

- structured RFC-9457 validation errors with per-field details

### Fix

- swap except order in RequestBodyResolver and add two-pass coverage tests

## v0.6.1 (2026-04-20)

### Fix

- include branch coverage in badge and use explicit tag push
- use absolute raw GitHub URL for coverage badge in README

## v0.6.0 (2026-04-20)

### Feat

- generate and commit coverage badge on release

### Fix

- skip --changelog when previous tag is missing
- replace coverage-badge with inline script for Python 3.14 compatibility

## v0.5.0 (2026-04-20)

### Feat

- add dry_run checkbox to release workflow for safe testing

### Fix

- use RELEASE_TOKEN to bypass branch protection on push
- replace invalid cz --push flag with explicit git push --follow-tags
- resolve pyright type errors without type: ignore suppression

## v0.4.1 (2026-04-19)

## v0.4.0 (2026-04-19)

### Feat

- implement RFC-9457 Problem Details for HTTP APIs

### Refactor

- migrate core routing and DI to typed injectable architecture

## v0.3.2 (2025-07-21)

## v0.3.1 (2025-07-21)

## v0.3.0 (2025-07-21)

## v0.2.2 (2025-05-31)

## v0.2.1 (2025-05-31)

### Feat

- implement dependency injection

## v0.1.0 (2024-02-16)
