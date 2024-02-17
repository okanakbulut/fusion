import typing


class QueryParams(dict[str, typing.Any]):
    def __init__(self, mapping: list[tuple[str, str]] = []):
        super().__init__()
        for key, value in mapping:
            self.__setitem__(key, value)

    def __setitem__(self, key: str, value: typing.Any) -> None:
        k, *rest = key.split(".", maxsplit=1)
        if rest:
            rest = rest[0]
            if k not in self:
                self[k] = QueryParams()
            self[k][rest] = value
        else:
            if key.endswith("[]"):
                key = key[:-2]
                if key not in self:
                    self[key] = []
                self[key].append(value)
            else:
                super().__setitem__(key, value)
