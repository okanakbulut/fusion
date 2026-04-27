import re
import typing

from fusion.object import MetaObject, Object

from .column import Column


def _camel_to_snake(name: str) -> str:
    s1 = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s1).lower()


def _pluralize(name: str) -> str:
    if name.endswith(("s", "x", "z", "ch", "sh")):
        return name + "es"
    if name.endswith("y") and len(name) > 1 and name[-2] not in "aeiou":
        return name[:-1] + "ies"
    return name + "s"


def _derive_table_name(class_name: str) -> str:
    return _pluralize(_camel_to_snake(class_name))


class MetaModel(MetaObject):
    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, typing.Any],
        **kwargs: typing.Any,
    ) -> type:
        table_override: str | None = namespace.pop("__table__", None)
        schema_override: str | None = namespace.pop("__schema__", None)
        db_constraints: list[typing.Any] = list(namespace.pop("__constraints__", []))
        db_indexes: list[typing.Any] = list(namespace.pop("__indexes__", []))

        kwargs.setdefault("kw_only", True)
        kwargs.setdefault("frozen", True)
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)

        new_cls.__table_name__ = table_override or _derive_table_name(name)  # type: ignore[attr-defined]
        new_cls.__schema__ = schema_override  # type: ignore[attr-defined]
        new_cls.__db_constraints__ = db_constraints  # type: ignore[attr-defined]
        new_cls.__db_indexes__ = db_indexes  # type: ignore[attr-defined]

        return new_cls

    def __getattribute__(cls, name: str) -> typing.Any:
        try:
            fields: dict[str, typing.Any] = super().__getattribute__("__fields__")
            if name in fields:
                table: str = super().__getattribute__("__table_name__")
                return Column(name, table=table)
        except AttributeError:  # pragma: no cover
            pass
        return super().__getattribute__(name)


class Model(Object, metaclass=MetaModel):
    __table_name__: typing.ClassVar[str]
    __schema__: typing.ClassVar[str | None]
    __db_constraints__: typing.ClassVar[list[typing.Any]]
    __db_indexes__: typing.ClassVar[list[typing.Any]]

    @classmethod
    def select(cls, *columns: str) -> SelectQuery:
        from .query import SelectQuery

        return SelectQuery(cls, columns)

    @classmethod
    def insert(cls) -> InsertQuery:
        from .query import InsertQuery

        return InsertQuery(cls)

    @classmethod
    def update(cls) -> UpdateQuery:
        from .query import UpdateQuery

        return UpdateQuery(cls)

    @classmethod
    def delete(cls) -> DeleteQuery:
        from .query import DeleteQuery

        return DeleteQuery(cls)


if typing.TYPE_CHECKING:  # pragma: no cover
    from .query import DeleteQuery, InsertQuery, SelectQuery, UpdateQuery
