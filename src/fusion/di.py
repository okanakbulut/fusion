import inspect
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from graphlib import TopologicalSorter
from typing import Annotated, Any, Callable, ClassVar, Self, Union

import msgspec

type Inject[T] = Annotated[T, "Inject"]
type Provider = Callable[..., AsyncIterator[Any]]
type WrappedProvider = Callable[..., AbstractAsyncContextManager[Any]]
type Constructor = Union[type["Injectable"], WrappedProvider]


class ExecutionContext(AsyncExitStack):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.instances: dict[type, Any] = {}

    async def enter_async_context(self, cm: AbstractAsyncContextManager[Any]) -> Any:
        instance = await super().enter_async_context(cm)
        self.instances[instance.__class__] = instance
        return instance

    def __setitem__(self, key: type, value: Any) -> None:
        self.instances[key] = value

    def __getitem__(self, key: type) -> Any:
        return self.instances[key]

    def __contains__(self, key: type) -> bool:
        return key in self.instances


class Injectable(msgspec.Struct):
    __dependencies__: ClassVar[dict[Constructor, list[Constructor]]] = defaultdict(list)

    @classmethod
    def _dependencies(cls):
        "Return the topologically sorted dependencies of the class."
        graph: dict[Constructor, list[Constructor]] = {}
        dependencies = deque(Injectable.__dependencies__[cls])
        while dependencies:
            dependency = dependencies.popleft()
            if dependency in graph:
                continue
            graph[dependency] = Injectable.__dependencies__[dependency]
            for dep in graph[dependency]:
                if dep not in graph:
                    dependencies.append(dep)
        # topological sort the graph
        # t: TopologicalSorter[Constructor] = TopologicalSorter(graph)
        return TopologicalSorter(graph).static_order()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        "Register the class as an injectable."
        super().__init_subclass__(**kwargs)
        for _, annotation in cls.__annotations__.items():
            if origin := getattr(annotation, "__origin__", None):
                if origin is ClassVar:
                    continue
                if origin is Inject:
                    typ = annotation.__args__[0]
                    Injectable.__dependencies__[cls].append(typ)

    @classmethod
    def register(cls, fn: Provider) -> None:
        sig = inspect.signature(fn)
        return_type = sig.return_annotation.__args__[0]
        parameters: list[tuple[str, type]] = []
        dependencies: list[type] = []
        for name, param in sig.parameters.items():
            origin = getattr(param.annotation, "__origin__", None)
            if origin is Inject:
                typ = param.annotation.__args__[0]
                parameters.append((name, typ))
                dependencies.append(typ)

        # wrap the provider function in an async context manager
        fn_acm = asynccontextmanager(fn)

        @asynccontextmanager
        async def provider(ctx: ExecutionContext) -> AsyncIterator[Any]:
            if return_type in ctx:
                yield ctx[return_type]
                return
            args = {name: ctx[typ] for name, typ in parameters}
            async with fn_acm(**args) as result:
                ctx[return_type] = result
                yield result

        Injectable.__dependencies__[return_type].append(provider)
        Injectable.__dependencies__[provider] = dependencies

    @classmethod
    async def instance(cls, ctx: ExecutionContext) -> Self:
        """
        Instantiate an instance of the class if it's not already exists in the stack.
        """
        if cls in ctx:
            return ctx[cls]

        for dependency in cls._dependencies():
            match dependency:
                case type():
                    if dependency not in ctx:
                        ctx[dependency] = await dependency.instance(ctx)
                case _ as provider:
                    await ctx.enter_async_context(provider(ctx))

        args = {}
        for name, annotation in cls.__annotations__.items():
            if origin := getattr(annotation, "__origin__", None):
                if origin is Inject:
                    typ = annotation.__args__[0]
                    args[name] = ctx[typ]
        # instantiate the class
        return cls(**args)
