import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not os.environ.get("POSTGRES_DSN"):
        skip = pytest.mark.skip(reason="set POSTGRES_DSN to run integration tests")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
