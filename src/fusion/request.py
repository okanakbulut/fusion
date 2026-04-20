from .annotations import Cookie, Header, PathParam, QueryParam, RequestBody
from .context import context
from .injectable import Injectable
from .types import Receive, Scope, Send


class Request(Injectable):
    """HTTP Request object.
    Provides access to request data such as path parameters, query parameters,
    headers, cookies, and body through annotations.
    Annotations supported:
    - PathParam
    - QueryParam
    - Header
    - Cookie
    - RequestBody

    Example:
    --------
    ```python
    from fusion.annotations import QueryParam, Header, RequestBody
    from fusion.request import Request

    class MyRequest(Request):
        user_id: QueryParam[int]
        auth_token: Header[str]
        data: RequestBody[MyDataModel]
    ```
    """

    __allowed_annotations__ = {Cookie, Header, PathParam, QueryParam, RequestBody}

    @property
    def scope(self) -> Scope:
        return context.get().scope

    @property
    def receive(self) -> Receive:
        return context.get().receive

    @property
    def send(self) -> Send:
        return context.get().send

    @property
    def headers(self) -> dict[str, str]:
        return context.get().headers

    @property
    def cookies(self) -> dict[str, str]:
        return context.get().cookies

    @property
    def query_params(self) -> dict[str, object | list[object]]:
        return context.get().query_params

    @property
    def path_params(self) -> dict[str, object]:
        return context.get().path_params

    async def body(self) -> bytes:
        return await context.get().body()
