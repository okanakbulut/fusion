# fusion.orm User Guide

`fusion.orm` is the persistence layer for the fusion framework. It has two responsibilities:

1. **Query building** — composable, injection-proof Postgres queries via a fluent API.
2. **Schema management** — Python model classes are the single source of truth; `fusion-orm` snapshots, diffs, and migrates forward automatically.

Queries are built on top of [pypika](https://github.com/kayak/pypika) and executed via [asyncpg](https://github.com/MagicStack/asyncpg). pypika is an implementation detail — it never appears in public APIs.

---

## Quick start

```python
>>> from fusion.orm import Model, field, ForeignKey, Q, db_now

>>> class User(Model):
...     id: int | None = None
...     email: str
...     username: str

>>> class Post(Model):
...     id: int | None = None
...     user_id: int
...     title: str
...     body: str | None = None
...     created_at: str | None = field(db_type="TIMESTAMPTZ", default=db_now())
...     __constraints__ = [ForeignKey("user_id", User)]

>>> sql, params = Post.select().where(user_id=1).build()
>>> sql
'SELECT * FROM "posts" WHERE "user_id"=$1'
>>> params
[1]

```

---

## Defining models

### Table names

Class names are automatically converted to snake_case and pluralized:

```python
>>> from fusion.orm import Model

>>> class UserProfile(Model):
...     id: int | None = None
...     bio: str

>>> UserProfile.__table_name__
'user_profiles'

>>> class Category(Model):
...     id: int | None = None
...     name: str

>>> Category.__table_name__
'categories'

```

Override with `__table__` when you need a non-standard name:

```python
>>> class LegacyAuditLog(Model):
...     __table__ = "tbl_audit"
...     id: int | None = None
...     action: str

>>> LegacyAuditLog.__table_name__
'tbl_audit'

```

### Schema-qualified tables

Set `__schema__` to place the table in a Postgres schema other than the default:

```python
>>> class Metric(Model):
...     __schema__ = "analytics"
...     id: int | None = None
...     name: str

>>> sql, params = Metric.select().build()
>>> sql
'SELECT * FROM "analytics"."metrics"'

```

### Primary key

`id: int | None = None` is the primary key convention. `fusion.orm` omits it from `INSERT` statements and populates it from `RETURNING *`:

```python
>>> from fusion.orm import Model

>>> class Tag(Model):
...     id: int | None = None
...     label: str

>>> tag = Tag(label="python")
>>> sql, params = Tag.insert().values(tag).build()
>>> sql
'INSERT INTO "tags" ("label") VALUES ($1) RETURNING *'

```

### Nullability

Nullability is derived from the Python type annotation — no separate `nullable` parameter:

```python
>>> from fusion.orm import Model

>>> class Article(Model):
...     id: int | None = None
...     title: str          # NOT NULL
...     summary: str | None = None  # NULL

```

`str` maps to `TEXT NOT NULL`, `str | None` maps to `TEXT NULL`.

### Field options and db_type

`field()` extends the base fusion field with a `db_type` override for Postgres-specific column types:

```python
>>> from fusion.orm import Model, field

>>> class Document(Model):
...     id: int | None = None
...     tags: list[str] = field(db_type="TEXT[]", default_factory=list)
...     meta: dict = field(db_type="JSONB", default_factory=dict)
...     title: str = field(min_length=1, max_length=255)

```

All validation constraints (`min_length`, `max_length`, `ge`, `gt`, `le`, `lt`, `pattern`) from the base `field()` apply at construction time.

Default Python → Postgres type mapping:

| Python | Postgres |
|--------|----------|
| `str` | `TEXT` |
| `int` | `INTEGER` |
| `float` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `datetime` | `TIMESTAMPTZ` |

Anything else requires an explicit `db_type`.

### DB-level defaults

`db_now()` and `db_uuid()` are sentinel values. When `INSERT` encounters one, it omits the column so Postgres applies the `DEFAULT`:

```python
>>> from fusion.orm import Model, field, db_now, db_uuid

>>> class Event(Model):
...     id: int | None = None
...     name: str
...     created_at: str | None = field(db_type="TIMESTAMPTZ", default=db_now())

>>> e = Event(name="signup")
>>> sql, params = Event.insert().values(e).build()
>>> sql
'INSERT INTO "events" ("name") VALUES ($1) RETURNING *'
>>> params
['signup']

```

`db_uuid()` works the same way for `UUID` columns:

```python
>>> class Token(Model):
...     id: int | None = None
...     user_id: int
...     value: str | None = field(db_type="UUID", default=db_uuid())

>>> t = Token(user_id=42)
>>> sql, params = Token.insert().values(t).build()
>>> sql
'INSERT INTO "tokens" ("user_id") VALUES ($1) RETURNING *'

```

### Constraints and indexes

Constraints and indexes are declared at the class level, separate from `field()`:

```python
>>> from fusion.orm import Model, field, ForeignKey, UniqueConstraint, Index, db_now

>>> class Organisation(Model):
...     id: int | None = None
...     slug: str
...     __constraints__ = [UniqueConstraint("slug")]

>>> class Member(Model):
...     id: int | None = None
...     user_id: int
...     org_id: int
...     role: str
...     joined_at: str | None = field(db_type="TIMESTAMPTZ", default=db_now())
...     __constraints__ = [
...         ForeignKey("user_id", User),
...         ForeignKey("org_id", Organisation),
...         UniqueConstraint("user_id", "org_id"),
...     ]
...     __indexes__ = [Index("org_id", "joined_at")]

```

`ForeignKey` also drives automatic JOIN ON clause inference — see [JOIN](#join) below.

---

## Query API

All queries are **lazy**: nothing executes until `.fetch()` or `.fetch_one()` is called with an asyncpg connection. `.build()` returns `(sql, params)` without touching the database, which is how the doctests in this guide verify SQL.

### SELECT

```python
>>> from fusion.orm import Model, ForeignKey

>>> class Author(Model):
...     id: int | None = None
...     email: str

>>> class Post(Model):
...     id: int | None = None
...     author_id: int
...     title: str
...     body: str | None = None
...     __constraints__ = [ForeignKey("author_id", Author)]

```

Select all columns:

```python
>>> sql, params = Post.select().build()
>>> sql
'SELECT * FROM "posts"'
>>> params
[]

```

Select specific columns:

```python
>>> sql, params = Post.select("id", "title").build()
>>> sql
'SELECT "id","title" FROM "posts"'

```

Pagination:

```python
>>> sql, params = (
...     Post.select()
...     .where(author_id=1)
...     .order_by("title")
...     .limit(10)
...     .offset(20)
...     .build()
... )
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 ORDER BY "title" ASC LIMIT 10 OFFSET 20'
>>> params
[1]

```

Descending order:

```python
>>> sql, params = Post.select().order_by("title", desc=True).build()
>>> sql
'SELECT * FROM "posts" ORDER BY "title" DESC'

```

### WHERE

Simple equality:

```python
>>> sql, params = Post.select().where(author_id=1).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1'
>>> params
[1]

```

Multiple keyword conditions are combined with AND:

```python
>>> sql, params = Post.select().where(author_id=1, body=None).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 AND "body"=$2'
>>> params
[1, None]

```

Chaining `.where()` also combines with AND:

```python
>>> sql, params = Post.select().where(author_id=1).where(body__is_not_null=True).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 AND "body" IS NOT NULL'

```

Supported lookup suffixes:

| Suffix | SQL |
|--------|-----|
| *(none)* | `= $n` |
| `__ne` | `<> $n` |
| `__gt` | `> $n` |
| `__gte` | `>= $n` |
| `__lt` | `< $n` |
| `__lte` | `<= $n` |
| `__like` | `LIKE $n` |
| `__ilike` | `ILIKE $n` |
| `__in` | `IN $n` |
| `__is_null` | `IS NULL` |
| `__is_not_null` | `IS NOT NULL` |

```python
>>> sql, params = Post.select().where(title__ilike="%python%").build()
>>> sql
'SELECT * FROM "posts" WHERE "title" ILIKE $1'
>>> params
['%python%']

>>> sql, params = Post.select().where(author_id__in=[1, 2, 3]).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id" IN $1'
>>> params
[[1, 2, 3]]

>>> sql, params = Post.select().where(body__is_null=True).build()
>>> sql
'SELECT * FROM "posts" WHERE "body" IS NULL'

```

Runtime-constructed filter dicts unpack naturally:

```python
>>> filters = {"author_id": 1, "title__ilike": "%orm%"}
>>> sql, params = Post.select().where(**filters).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 AND "title" ILIKE $2'

```

### Q objects

`Q` enables `AND`, `OR`, and `NOT` composition across conditions. Use `&`, `|`, and `~`:

```python
>>> from fusion.orm import Q

>>> sql, params = Post.select().where(Q(author_id=1) | Q(author_id=2)).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 OR "author_id"=$2'
>>> params
[1, 2]

>>> sql, params = Post.select().where(Q(author_id=1) & Q(body__is_null=True)).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id"=$1 AND "body" IS NULL'
>>> params
[1]

>>> sql, params = Post.select().where(~Q(author_id=1)).build()
>>> sql
'SELECT * FROM "posts" WHERE NOT "author_id"=$1'
>>> params
[1]

```

`Q` accepts the same double-underscore lookups as `.where()`.

### JOIN

When a `ForeignKey` is in `__constraints__`, `.join()` infers the ON clause automatically:

```python
>>> sql, params = Post.select().join(Author).build()
>>> sql
'SELECT * FROM "posts" JOIN "authors" ON "posts"."author_id"="authors"."id"'

```

Join types via the `how` parameter:

```python
>>> sql, params = Post.select().join(Author, how="left").build()
>>> sql
'SELECT * FROM "posts" LEFT JOIN "authors" ON "posts"."author_id"="authors"."id"'

>>> sql, params = Post.select().join(Author, how="right").build()
>>> sql
'SELECT * FROM "posts" RIGHT JOIN "authors" ON "posts"."author_id"="authors"."id"'

>>> sql, params = Post.select().join(Author, how="outer").build()
>>> sql
'SELECT * FROM "posts" FULL OUTER JOIN "authors" ON "posts"."author_id"="authors"."id"'

```

After a join, WHERE conditions on joined table columns are automatically qualified:

```python
>>> sql, params = Post.select().join(Author).where(author_id=1).build()
>>> sql
'SELECT * FROM "posts" JOIN "authors" ON "posts"."author_id"="authors"."id" WHERE "posts"."author_id"=$1'

```

#### Explicit join columns

When no `ForeignKey` exists between two models, pass `on=(LeftModel.col, RightModel.col)` using the field descriptors directly:

```python
>>> class Employee(Model):
...     id: int | None = None
...     department_id: int
...     name: str

>>> class Department(Model):
...     id: int | None = None
...     name: str

>>> sql, params = Employee.select().join(Department, on=(Employee.department_id, Department.id)).build()
>>> sql
'SELECT * FROM "employees" JOIN "departments" ON "employees"."department_id"="departments"."id"'

```

`how` works the same way:

```python
>>> sql, params = Employee.select().join(Department, on=(Employee.department_id, Department.id), how="left").build()
>>> sql
'SELECT * FROM "employees" LEFT JOIN "departments" ON "employees"."department_id"="departments"."id"'

```

For joins on multiple columns, pass a list of pairs — each pair is AND'd in the ON clause:

```python
>>> class Product(Model):
...     id: int | None = None
...     sku: str
...     region: str

>>> class Price(Model):
...     id: int | None = None
...     sku: str
...     region: str
...     amount: int

>>> sql, params = Product.select().join(
...     Price, on=[(Product.sku, Price.sku), (Product.region, Price.region)]
... ).build()
>>> sql
'SELECT * FROM "products" JOIN "prices" ON "products"."sku"="prices"."sku" AND "products"."region"="prices"."region"'

```

Explicit `on` also overrides FK inference when you need a non-standard join column. Here `Post` has a `ForeignKey("author_id", Author)`, but we join on a different column pair instead:

```python
>>> sql, params = Post.select().join(Author, on=(Post.title, Author.email)).build()
>>> sql
'SELECT * FROM "posts" JOIN "authors" ON "posts"."title"="authors"."email"'

```

### Relationships and prefetch

Declare a relationship field by annotating a model field with another `Model` type. The metaclass automatically injects the corresponding FK column (`author_id` for `author: Author | None`) and registers a `ForeignKey` constraint:

```python
>>> class Author(Model):
...     id: int | None = None
...     name: str

>>> class Article(Model):
...     id: int | None = None
...     title: str
...     author: Author | None = None   # injects author_id: int | None = None

```

The FK column is available at construction time:

```python
>>> a = Article(title="hello", author_id=1)
>>> a.author_id
1
>>> a.author is None
True

```

The relationship field is excluded from `INSERT` — only the FK column is written:

```python
>>> sql, params = Article.insert().values(Article(title="hello", author_id=1)).build()
>>> sql
'INSERT INTO "articles" ("title","author_id") VALUES ($1,$2) RETURNING *'

```

Required (non-optional) relationships work the same way but the FK column is non-nullable:

```python
>>> class RequiredArticle(Model):
...     id: int | None = None
...     title: str
...     author: Author   # required → author_id: int (required, not Optional)

```

#### .prefetch()

`.prefetch(*models)` generates a `LEFT JOIN` for each relationship and instructs `.fetch()` to hydrate the related objects in a single query:

```python
>>> sql, _ = Article.select().prefetch(Author).build()
>>> sql
'SELECT * FROM "articles" LEFT JOIN "authors" ON "articles"."author_id"="authors"."id"'

```

After `.fetch(conn)` each returned `Article` has `.author` populated. When the LEFT JOIN finds no matching row, `.author` stays `None`:

```python
# async with pool.acquire() as conn:
#     articles = await Article.select().prefetch(Author).fetch(conn)
#     articles[0].author        # Author instance, or None if no match
#     articles[0].author.name   # "Alice"
```

When the related model uses a custom table name via `__table__`, `.prefetch()` uses that name in the JOIN automatically:

```python
>>> class LegacyUser(Model):
...     __table__ = "tbl_users"
...     id: int | None = None
...     name: str

>>> class Comment(Model):
...     id: int | None = None
...     body: str
...     author: LegacyUser | None = None

>>> sql, _ = Comment.select().prefetch(LegacyUser).build()
>>> sql
'SELECT * FROM "comments" LEFT JOIN "tbl_users" ON "comments"."author_id"="tbl_users"."id"'

```

Multiple relationships are prefetched in a single query:

```python
>>> class Tag(Model):
...     id: int | None = None
...     name: str

>>> class TaggedArticle(Model):
...     id: int | None = None
...     title: str
...     author: Author | None = None
...     tag: Tag | None = None

>>> sql, _ = TaggedArticle.select().prefetch(Author, Tag).build()
>>> sql
'SELECT * FROM "tagged_articles" LEFT JOIN "authors" ON "tagged_articles"."author_id"="authors"."id" LEFT JOIN "tags" ON "tagged_articles"."tag_id"="tags"."id"'

```

`.prefetch()` composes with `.where()`, `.order_by()`, `.limit()`, and `.offset()` like any other query modifier.

#### replace() and relationship sync

`Model.replace()` keeps the FK column and the relationship field in sync:

```python
# Setting the relationship object syncs the FK column:
# updated = article.replace(author=Author(id=42, name="Alice"))
# updated.author_id   # 42

# Setting the FK column directly clears the stale relationship field:
# updated = article.replace(author_id=99)
# updated.author      # None
```

---

### INSERT

`INSERT` always appends `RETURNING *`. The database fills in `id` and any DB-sentinel defaults; the returned rows are fully populated model instances.

Single row:

```python
>>> post = Post(author_id=1, title="Hello")
>>> sql, params = Post.insert().values(post).build()
>>> sql
'INSERT INTO "posts" ("author_id","title","body") VALUES ($1,$2,$3) RETURNING *'
>>> params
[1, 'Hello', None]

```

Bulk insert — one round-trip:

```python
>>> p1 = Post(author_id=1, title="First")
>>> p2 = Post(author_id=2, title="Second")
>>> sql, params = Post.insert().values([p1, p2]).build()
>>> sql
'INSERT INTO "posts" ("author_id","title","body") VALUES ($1,$2,$3),($4,$5,$6) RETURNING *'
>>> params
[1, 'First', None, 2, 'Second', None]

```

### UPDATE

`UPDATE` also appends `RETURNING *` and returns the updated rows.

```python
>>> sql, params = Post.update().set(title="New title").where(author_id=1).build()
>>> sql
'UPDATE "posts" SET "title"=$1 WHERE "author_id"=$2 RETURNING *'
>>> params
['New title', 1]

```

Multiple fields:

```python
>>> sql, params = Post.update().set(title="t", body="b").where(author_id=1).build()
>>> sql
'UPDATE "posts" SET "title"=$1,"body"=$2 WHERE "author_id"=$3 RETURNING *'
>>> params
['t', 'b', 1]

```

### DELETE

`DELETE` appends `RETURNING *` and returns the deleted rows.

```python
>>> sql, params = Post.delete().where(author_id=1).build()
>>> sql
'DELETE FROM "posts" WHERE "author_id"=$1 RETURNING *'
>>> params
[1]

```

### Executing queries

All terminal methods are `async` and take an asyncpg connection explicitly:

```python
# async with pool.acquire() as conn:
#     posts = await Post.select().where(author_id=1).fetch(conn)      # list[Post]
#     post  = await Post.select().where(id=5).fetch_one(conn)         # Post | None
#     raws  = await Post.select().fetch(conn, raw=True)               # list[dict]
#
#     saved   = await Post.insert().values(post).fetch(conn)          # list[Post]
#     updated = await Post.update().set(title="x").where(id=1).fetch(conn)
#     deleted = await Post.delete().where(id=1).fetch(conn)
```

`fetch_one` returns `None` when no row matches. `fetch(conn, raw=True)` returns `list[dict]` instead of model instances.

INSERT, UPDATE, and DELETE all use `.fetch()` — there is no separate `.execute()`.

### Transactions

Pass the transactional connection to every `.fetch()` call — standard asyncpg patterns apply:

```python
# async with pool.acquire() as conn:
#     async with conn.transaction():
#         users = await User.insert().values(User(email="a@b.com")).fetch(conn)
#         await Post.insert().values(Post(author_id=users[0].id, title="first")).fetch(conn)
```

### Modifying model instances

`Model` is frozen. Use `msgspec.replace` to produce a modified copy:

```python
# import msgspec
# updated_post = msgspec.replace(post, title="new title")
```

---

## UNION

```python
>>> from fusion.orm import union

>>> q = union(
...     Post.select("id", "title").where(author_id=1),
...     Post.select("id", "title").where(author_id=2),
... )
>>> sql, params = q.build()
>>> sql
'(SELECT "id","title" FROM "posts" WHERE "author_id"=$1) UNION (SELECT "id","title" FROM "posts" WHERE "author_id"=$2)'
>>> params
[1, 2]

```

`UNION ALL` (preserves duplicates):

```python
>>> q = union(
...     Post.select("id", "title").where(author_id=1),
...     Post.select("id", "title").where(author_id=2),
...     all=True,
... )
>>> sql, params = q.build()
>>> sql
'(SELECT "id","title" FROM "posts" WHERE "author_id"=$1) UNION ALL (SELECT "id","title" FROM "posts" WHERE "author_id"=$2)'

```

`union(...).fetch(conn)` returns `list[dict]` because a UNION may span multiple models.

---

## CTE

The `cte()` function takes the main query and each CTE as a named keyword argument. The kwarg name becomes the SQL alias:

```python
>>> from fusion.orm import cte

>>> q = cte(
...     main=Post.select("id", "title"),
...     recent=Post.select().where(author_id=1),
... )
>>> sql, params = q.build()
>>> sql
'WITH recent AS (SELECT * FROM "posts" WHERE "author_id"=$1) SELECT "id","title" FROM "posts"'
>>> params
[1]

```

Multiple CTEs:

```python
>>> q = cte(
...     main=Post.select("id", "title"),
...     first=Post.select().where(author_id=1),
...     second=Post.select().where(author_id=2),
... )
>>> sql, params = q.build()
>>> sql
'WITH first AS (SELECT * FROM "posts" WHERE "author_id"=$1), second AS (SELECT * FROM "posts" WHERE "author_id"=$2) SELECT "id","title" FROM "posts"'
>>> params
[1, 2]

```

### Recursive CTE

`recursive_cte()` emits `WITH RECURSIVE`. Compose the base and recursive terms with `union(..., all=True)`:

```python
>>> from fusion.orm import recursive_cte

>>> class Node(Model):
...     id: int | None = None
...     name: str
...     parent_id: int | None = None

>>> q = recursive_cte(
...     main=Node.select("id", "name"),
...     tree=union(
...         Node.select("id", "name", "parent_id").where(parent_id__is_null=True),
...         Node.select("id", "name", "parent_id"),
...         all=True,
...     ),
... )
>>> sql, params = q.build()
>>> sql
'WITH RECURSIVE tree AS ((SELECT "id","name","parent_id" FROM "nodes" WHERE "parent_id" IS NULL) UNION ALL (SELECT "id","name","parent_id" FROM "nodes")) SELECT "id","name" FROM "nodes"'

```

---

## Raw SQL fragments

`Exp` wraps a raw SQL fragment and passes it through without escaping. Use it for Postgres operators or expressions that the query builder doesn't cover:

```python
>>> from fusion.orm import Exp

>>> sql, params = Post.update().set(body=Exp('body || \' (edited)\'')
... ).where(author_id=1).build()
>>> sql
'UPDATE "posts" SET "body"=body || \' (edited)\' WHERE "author_id"=$1 RETURNING *'

```

`Exp` is an escape hatch. Never interpolate user-supplied values into `Exp` — those must be passed as query parameters.

`.where_raw()` accepts an `Exp` for conditions that can't be expressed with the lookup DSL:

```python
>>> sql, params = Post.select().where_raw(Exp('"author_id" IS NOT NULL')).build()
>>> sql
'SELECT * FROM "posts" WHERE "author_id" IS NOT NULL'

```

---

## Schema management

### Commands

```bash
# Serialize current models to migrations/NNNN_<timestamp>.yaml
fusion-orm snapshot

# Verify models match the latest snapshot (use as a pre-commit hook)
fusion-orm check

# Apply pending snapshots to the database
fusion-orm migrate --db postgresql://user:pass@host/db
```

### Workflow

1. Change model classes in Python.
2. Run `fusion-orm snapshot` — a new YAML file is written to `migrations/`.
3. Commit the model change and the YAML snapshot together. A git pre-commit hook runs `fusion-orm check` and blocks the commit if they are out of sync.
4. On deploy: `fusion-orm migrate` reads the `_orm_migrations` table, diffs consecutive snapshots, generates DDL, applies it, and records the new version.

### Snapshot format

Each YAML file is a full schema snapshot:

```yaml
version: 1
tables:
  users:
    columns:
      id:         { type: SERIAL,      nullable: false, primary_key: true }
      email:      { type: TEXT,        nullable: false }
      username:   { type: TEXT,        nullable: false }
      created_at: { type: TIMESTAMPTZ, nullable: true,  default: NOW() }
    constraints:
      - { type: unique, columns: [email] }
      - { type: unique, columns: [username] }
    indexes:
      - { columns: [email] }
```

### Migration safety rules

All migrations are forward-only. DROP operations require an explicit `--drop` flag. RENAME is blocked — treat it as add + data migration + drop in separate steps.

| Change | Safe? |
|--------|-------|
| Add nullable column | Yes |
| Add NOT NULL column (with `db_default`) | Yes |
| Add table | Yes |
| Add index or constraint | Yes |
| Drop column / table | Requires `--drop` |
| Rename column | Blocked |

---

## Public API reference

```python
from fusion.orm import (
    Model,           # base class for all models
    field,           # field DSL with db_type support
    db_now,          # DB sentinel: DEFAULT NOW()
    db_uuid,         # DB sentinel: DEFAULT gen_random_uuid()
    Q,               # boolean condition composition
    Exp,             # raw SQL fragment
    union,           # UNION / UNION ALL
    cte,             # WITH ... SELECT
    recursive_cte,   # WITH RECURSIVE ... SELECT
    ForeignKey,      # FK constraint + JOIN inference
    UniqueConstraint,
    Index,
)
```
