"""Credentials in a signature, and the roles an operation requires.

A credential is a parameter like any other: a marker names the scheme that
carries it, the binder extracts it, and that same declaration is what the
OpenAPI document describes.  Nothing is configured - the scheme is implied by
the marker, and an API key's wire name by the parameter's own name.

Verification is deliberately absent.  Checking a token's signature or looking up
a role needs your keys, your store and your cache, so the framework only asks:
markers extract and document, ``@requires`` declares, and the ``Authorizer`` you
hand the application decides.
"""

import base64
import re
import typing

from .context import context
from .exceptions import ProblemException
from .object import Object
from .resolvers import Resolver
from .responses import Forbidden, Unauthorized

ROLES_ATTRIBUTE = "__fusion_roles__"
"""Where ``requires`` records its roles.  ``Signature.of`` reads it, so the
declaration travels with the function rather than living in a side registry."""


class Credentials(Object, frozen=True):
    """The two halves of an HTTP Basic credential, already decoded."""

    username: str
    password: str


def _camel(value: str) -> str:
    """``x-api-key`` -> ``xApiKey``, for naming a scheme after its header."""
    head, *rest = re.split(r"[-_]", value)
    return head.lower() + "".join(part.title() for part in rest)


class SecurityResolver(Resolver):
    """Base for resolvers that carry a credential.

    ``location`` is ``security`` rather than a parameter location, which is what
    keeps a credential out of an operation's ``parameters``: OpenAPI describes it
    under ``securitySchemes`` instead, and a header parameter named
    ``Authorization`` is ignored by the specification outright.

    A missing or malformed credential raises rather than returning ``MISSING``.
    Falling through to the binder would report it as a validation error - a 422
    listing a field - when the honest answer is 401.
    """

    location: typing.ClassVar[str] = "security"

    @property
    def scheme_name(self) -> str:
        """Key this scheme is registered under in ``components``."""
        raise NotImplementedError  # pragma: no cover

    def scheme(self) -> dict[str, typing.Any]:
        """The OpenAPI Security Scheme Object for this credential."""
        raise NotImplementedError  # pragma: no cover

    def _authorization(self, expected: str) -> str:
        scheme, _, rest = self.context.headers.get("authorization", "").partition(" ")
        if scheme.lower() != expected or not rest.strip():
            raise ProblemException(Unauthorized(detail=f"Expected a {expected.title()} credential"))
        return rest.strip()


class BearerResolver(SecurityResolver):
    """``Authorization: Bearer <token>``, handed over with the prefix removed."""

    @property
    def scheme_name(self) -> str:
        return "bearerAuth"

    def scheme(self) -> dict[str, typing.Any]:
        return {"type": "http", "scheme": "bearer"}

    async def resolve(self) -> tuple[str, typing.Any]:
        return self.name, self._authorization("bearer")


class BasicResolver(SecurityResolver):
    """``Authorization: Basic <base64>``, decoded into ``Credentials``."""

    @property
    def scheme_name(self) -> str:
        return "basicAuth"

    def scheme(self) -> dict[str, typing.Any]:
        return {"type": "http", "scheme": "basic"}

    async def resolve(self) -> tuple[str, typing.Any]:
        encoded = self._authorization("basic")
        try:
            decoded = base64.b64decode(encoded, validate=True).decode()
        except ValueError, UnicodeDecodeError:
            raise ProblemException(Unauthorized(detail="Malformed Basic credential")) from None

        username, separator, password = decoded.partition(":")
        if not separator:
            raise ProblemException(Unauthorized(detail="Malformed Basic credential"))
        return self.name, Credentials(username=username, password=password)


class ApiKeyResolver(SecurityResolver):
    """An API key, named by the parameter that declares it."""

    where: typing.ClassVar[str] = "header"

    @property
    def wire_name(self) -> str:
        """The name a client actually sends.

        Header names are conventionally dashed and the context maps ``-`` to
        ``_`` on the way in, so the document has to spell it back out; query and
        cookie names are passed through untouched.
        """
        return self.name.replace("_", "-") if self.where == "header" else self.name

    @property
    def scheme_name(self) -> str:
        return f"{_camel(self.wire_name)}Auth"

    def scheme(self) -> dict[str, typing.Any]:
        return {"type": "apiKey", "in": self.where, "name": self.wire_name}

    def _source(self) -> typing.Mapping[str, typing.Any]:
        return self.context.headers

    async def resolve(self) -> tuple[str, typing.Any]:
        value = self._source().get(self.name)
        if not value:
            raise ProblemException(Unauthorized(detail=f"Missing {self.wire_name}"))
        return self.name, str(value)


class ApiKeyQueryResolver(ApiKeyResolver):
    where: typing.ClassVar[str] = "query"

    def _source(self) -> typing.Mapping[str, typing.Any]:
        return self.context.query_params


class ApiKeyCookieResolver(ApiKeyResolver):
    where: typing.ClassVar[str] = "cookie"

    def _source(self) -> typing.Mapping[str, typing.Any]:
        return self.context.cookies


def requires[F: typing.Callable[..., typing.Any]](*roles: str) -> typing.Callable[[F], F]:
    """Declare the roles an operation needs before it runs.

    Roles are AND-ed, and stacking the decorator unions them.  What a role means
    is never the framework's business: it hands the set to the application's
    ``Authorizer`` and does as it is told.
    """
    if not roles or any(not role.strip() for role in roles):
        raise ValueError(
            "requires() needs at least one non-empty role; an empty requirement reads as "
            "protection and provides none."
        )

    def decorate(func: F) -> F:
        declared: frozenset[str] = getattr(func, ROLES_ATTRIBUTE, frozenset())
        setattr(func, ROLES_ATTRIBUTE, declared | frozenset(roles))
        return func

    return decorate


async def authorize(roles: frozenset[str]) -> Forbidden | None:
    """Ask the application's authorizer whether this request may proceed.

    Returns the problem to answer with, or ``None`` to continue - the same
    protocol a guard middleware follows.
    """
    if not roles:
        return None
    authorizer = context.get().scope["app"].authorizer
    if await authorizer.authorize(roles):
        return None
    return Forbidden(detail=f"Requires {', '.join(sorted(roles))}")
