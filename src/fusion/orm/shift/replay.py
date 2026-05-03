from __future__ import annotations

import importlib.util
import inspect
from collections.abc import Iterable
from pathlib import Path

from fusion.orm.shift.operations import Shift
from fusion.orm.shift.state import SchemaState


def replay_shifts(paths: Iterable[Path]) -> SchemaState:
    state = SchemaState()
    for path in sorted(paths, key=lambda p: p.name):
        shift_cls = _load_shift(path)
        for op in shift_cls.operations:
            op.apply(state)
    return state


def _load_shift(path: Path) -> type[Shift]:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except SyntaxError as exc:
        raise ImportError(f"Failed to import shift file '{path.name}': {exc}") from exc
    for _, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, Shift) and obj is not Shift:
            return obj
    raise ImportError(f"No Shift subclass found in '{path.name}'")
