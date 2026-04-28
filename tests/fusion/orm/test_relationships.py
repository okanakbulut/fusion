"""Tests for ORM relationship mechanics: FK injection, prefetch joins, hydration, replace() sync, and constraint deduplication."""

import uuid

import pytest

from fusion.orm.model import Model


class Author(Model):
    id: int | None = None
    name: str


class Article(Model):
    id: int | None = None
    title: str
    author: Author | None = None


def test_relationship_field_injects_fk_column():
    article = Article(title="foo", author_id=1)
    assert article.author_id == 1
    assert article.author is None


def test_prefetch_generates_left_join():
    sql, _ = Article.select().prefetch(Author).build()
    assert sql == (
        'SELECT "articles"."id","articles"."title","articles"."author_id",'
        '"authors"."id" "author__id","authors"."name" "author__name"'
        ' FROM "articles" LEFT JOIN "authors" ON "articles"."author_id"="authors"."id"'
    )


def test_join_and_prefetch_same_model_no_duplicate_join():
    """Explicit .join() + .prefetch() on the same model must not emit the table twice."""
    sql, _ = Article.select().join(Author).prefetch(Author).build()
    assert sql == (
        'SELECT "articles"."id","articles"."title","articles"."author_id",'
        '"authors"."id" "author__id","authors"."name" "author__name"'
        ' FROM "articles" JOIN "authors" ON "articles"."author_id"="authors"."id"'
    )


@pytest.mark.asyncio
async def test_prefetch_hydrates_relationship():
    from unittest.mock import AsyncMock, MagicMock

    conn = AsyncMock()
    row = {"id": 1, "title": "hello", "author_id": 42, "author__id": 42, "author__name": "Alice"}
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetch = AsyncMock(return_value=[record])

    results = await Article.select().prefetch(Author).fetch(conn)
    assert len(results) == 1
    assert isinstance(results[0].author, Author)
    assert results[0].author.name == "Alice"


class Tag(Model):
    id: int | None = None
    name: str


class TaggedArticle(Model):
    id: int | None = None
    title: str
    author: Author | None = None
    tag: Tag | None = None


def test_multi_prefetch_generates_two_joins():
    sql, _ = TaggedArticle.select().prefetch(Author, Tag).build()
    assert sql == (
        'SELECT "tagged_articles"."id","tagged_articles"."title","tagged_articles"."author_id","tagged_articles"."tag_id",'
        '"authors"."id" "author__id","authors"."name" "author__name",'
        '"tags"."id" "tag__id","tags"."name" "tag__name"'
        ' FROM "tagged_articles"'
        ' LEFT JOIN "authors" ON "tagged_articles"."author_id"="authors"."id"'
        ' LEFT JOIN "tags" ON "tagged_articles"."tag_id"="tags"."id"'
    )


@pytest.mark.asyncio
async def test_multi_prefetch_hydrates_both_relationships():
    from unittest.mock import AsyncMock, MagicMock

    conn = AsyncMock()
    row = {
        "id": 1,
        "title": "hello",
        "author_id": 42,
        "tag_id": 7,
        "author__id": 42,
        "author__name": "Alice",
        "tag__id": 7,
        "tag__name": "python",
    }
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetch = AsyncMock(return_value=[record])

    results = await TaggedArticle.select().prefetch(Author, Tag).fetch(conn)
    assert len(results) == 1
    assert isinstance(results[0].author, Author)
    assert results[0].author.name == "Alice"
    assert isinstance(results[0].tag, Tag)
    assert results[0].tag.name == "python"


class RequiredAuthorPost(Model):
    id: int | None = None
    title: str
    author: Author  # required — not Optional


def test_required_relationship_injects_required_fk():
    import msgspec

    fields = {f.name: f for f in msgspec.structs.fields(RequiredAuthorPost)}
    assert "author_id" in fields
    assert fields["author_id"].required


class UUIDAuthor(Model):
    id: uuid.UUID | None = None
    name: str


class UUIDArticle(Model):
    id: int | None = None
    title: str
    author: UUIDAuthor | None = None


def test_fk_constraint_auto_registered():
    from fusion.orm.constraints import ForeignKey

    fk_constraints = [c for c in Article.__db_constraints__ if isinstance(c, ForeignKey)]
    assert len(fk_constraints) == 1
    assert fk_constraints[0].column == "author_id"
    assert fk_constraints[0].target is Author


def test_relationship_field_excluded_from_insert():
    sql, _ = Article.insert().values(Article(title="hello", author_id=1)).build()
    assert sql == 'INSERT INTO "articles" ("title","author_id") VALUES ($1,$2) RETURNING *'


def test_replace_author_syncs_author_id():
    article = Article(title="hello", author_id=1)
    new_author = Author(id=42, name="Alice")
    updated = article.replace(author=new_author)
    assert updated.author is new_author
    assert updated.author_id == 42


def test_replace_author_id_clears_author():
    new_author = Author(id=42, name="Alice")
    article = Article(title="hello", author=new_author, author_id=42)
    updated = article.replace(author_id=99)
    assert updated.author_id == 99
    assert updated.author is None


def test_fk_column_type_matches_target_id_type():
    import msgspec

    fields = {f.name: f for f in msgspec.structs.fields(UUIDArticle)}
    assert "author_id" in fields
    assert fields["author_id"].type == uuid.UUID | None


# ---------------------------------------------------------------------------
# Coverage for new code
# ---------------------------------------------------------------------------


def test_unwrap_optional_returns_none_for_multi_type_union():
    from fusion.orm.model import _unwrap_optional

    # int | str | None has two non-None types → should return None
    result = _unwrap_optional(int | str | None)
    assert result is None


def test_resolve_namespace_annotations_fallback():
    from fusion.orm.model import _resolve_namespace_annotations

    ns = {"__annotations__": {"x": int}}
    result = _resolve_namespace_annotations(ns)
    assert result == {"x": int}


class ManualFKOptionalArticle(Model):
    id: int | None = None
    title: str
    author: Author | None = None
    author_id: int | None = None  # manually declared — injection should be skipped


def test_manual_fk_not_doubled():
    import msgspec

    fields = {f.name: f for f in msgspec.structs.fields(ManualFKOptionalArticle)}
    assert fields.get("author_id") is not None
    # No duplicate constraints
    from fusion.orm.constraints import ForeignKey

    fk_count = sum(
        1
        for c in ManualFKOptionalArticle.__db_constraints__
        if isinstance(c, ForeignKey) and c.column == "author_id"
    )
    assert fk_count == 1


class ManualFKRequiredPost(Model):
    id: int | None = None
    title: str
    author: Author  # required
    author_id: int  # manually declared


def test_manual_fk_required_not_doubled():
    from fusion.orm.constraints import ForeignKey

    fk_count = sum(
        1
        for c in ManualFKRequiredPost.__db_constraints__
        if isinstance(c, ForeignKey) and c.column == "author_id"
    )
    assert fk_count == 1


def test_find_prefetch_relation_raises_for_missing_fk():
    from fusion.orm.query import _find_prefetch_relation

    with pytest.raises(ValueError, match="No ForeignKey"):
        _find_prefetch_relation(Author, Article)


@pytest.mark.asyncio
async def test_prefetch_null_row_leaves_relationship_none():
    from unittest.mock import AsyncMock, MagicMock

    conn = AsyncMock()
    # LEFT JOIN returned NULLs — author was not set
    row = {
        "id": 1,
        "title": "orphan",
        "author_id": None,
        "author__id": None,
        "author__name": None,
    }
    record = MagicMock()
    record.items = MagicMock(return_value=list(row.items()))
    conn.fetch = AsyncMock(return_value=[record])

    results = await Article.select().prefetch(Author).fetch(conn)
    assert len(results) == 1
    assert results[0].author is None
