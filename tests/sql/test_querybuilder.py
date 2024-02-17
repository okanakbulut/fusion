import pytest

from fusion.sql.querybuilder import Exp, Join, Q, RightJoin, cte, query, recursive_cte, union

class Model:
    pass
@pytest.fixture
def user() -> type[Model]:
    class User(Model):
        id: int
        org_id: int
        name: str

    return User


@pytest.fixture
def profile() -> type[Model]:
    class Profile(Model):
        id: int
        org_id: int
        user_id: int
        manager_id: int
        title: str

    return Profile


@pytest.fixture
def custom_user() -> type[Model]:
    "to test custom schema"

    class User(Model):
        __schema__ = "custom"
        id: int
        name: str

    return User


class TestQuery:
    def test_selection(self, user):
        q, _ = query(u="user").sql()
        assert q == 'SELECT *\nFROM user AS "u"'

        q, _ = query(u="user").select("u.id", "u.name").sql()
        assert q == 'SELECT u.id, u.name\nFROM user AS "u"'

        with pytest.raises(ValueError, match=r"Invalid selection:*"):
            query(u=user).select(user).sql()

        with pytest.raises(ValueError, match="At least one data source is required"):
            query().select("u.id").sql()

    def test_selection_with_model(self, user):
        q, _ = query(u=user).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"'

        q, _ = query(u=user).select("u.id", "u.name").sql()
        assert q == 'SELECT u.id, u.name\nFROM public.user AS "u"'

    def test_selection_with_subquery(self, user):
        q, _ = query(u=query(u=user)).sql()
        assert q == 'SELECT *\nFROM (\n  SELECT *\n  FROM public.user AS "u"\n) AS "u"'

        q, _ = query(sub=query(u=user)).select("sub.id", "sub.name").sql()
        assert (
            q
            == 'SELECT sub.id, sub.name\nFROM (\n  SELECT *\n  FROM public.user AS "u"\n) AS "sub"'
        )

    def test_selection_with_custom_schema(self, custom_user):
        q, _ = query(u=custom_user).sql()
        assert q == 'SELECT *\nFROM custom.user AS "u"'

        q, _ = query(u=custom_user).select("u.id", "u.name").sql()
        assert q == 'SELECT u.id, u.name\nFROM custom.user AS "u"'

    def test_where_condition(self, user):
        q, args = query(u=user).where(id=101).where(name__startswith="John").sql()
        assert (
            q == """SELECT *\nFROM public.user AS "u"\nWHERE "id" = $1 AND "name" LIKE $2 || '%'"""
        )
        assert args == [101, "John"]

        q, args = query(u=user).where(u__id=101).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" = $1'
        assert args == [101]

        q, args = query(u=user).where(u__id=101).where(u__name="John").sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" = $1 AND "u"."name" = $2'
        assert args == [101, "John"]

        # startswith
        q, args = query(u=user).where(u__name__startswith="John").sql()
        assert q == """SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" LIKE $1 || '%'"""
        assert args == ["John"]

        # endswith
        q, args = query(u=user).where(u__name__endswith="John").sql()
        assert q == """SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" LIKE '%' || $1"""
        assert args == ["John"]

        # range
        q, args = query(u=user).where(u__id__range=[1, 100]).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" BETWEEN $1 AND $2'
        assert args == [1, 100]

        # contains
        q, args = query(u=user).where(u__name__contains="John").sql()
        assert q == """SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" LIKE '%' || $1 || '%'"""
        assert args == ["John"]

        # in
        q, args = query(u=user).where(u__id__in=[1, 2, 3]).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" = any($1::int[])'
        assert args == [[1, 2, 3]]

        q, args = query(u=user).where(u__score__in=[1.0, 2.0, 3.0]).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."score" = any($1::float[])'
        assert args == [[1.0, 2.0, 3.0]]

        q, args = query(u=user).where(u__name__in=["John", "Micheal"]).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" = any($1::text[])'
        assert args == [["John", "Micheal"]]
        from datetime import date, datetime

        q, args = query(u=user).where(u__created_on__in=[date(2024, 2, 1), date(2024, 2, 2)]).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."created_on" = any($1::date[])'
        assert args == [[date(2024, 2, 1), date(2024, 2, 2)]]

        q, args = (
            query(u=user)
            .where(u__created_on__in=[datetime(2024, 2, 1), datetime(2024, 2, 2)])
            .sql()
        )
        assert (
            q
            == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."created_on" = any($1::timestamptz[])'
        )
        assert args == [[datetime(2024, 2, 1), datetime(2024, 2, 2)]]

        with pytest.raises(ValueError):
            query(u=user).where(u__id__in=[None]).sql()

        with pytest.raises(ValueError):
            query(u=user).where(u__id__in=dict()).sql()
        # is null
        q, args = query(u=user).where(u__name__isnull=True).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" IS NULL'
        assert args == []

        # is not null
        q, args = query(u=user).where(u__name__isnull=False).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."name" IS NOT NULL'
        assert args == []

        # lte
        q, args = query(u=user).where(u__id__lte=101).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" <= $1'
        assert args == [101]

        # lt
        q, args = query(u=user).where(u__id__lt=101).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" < $1'
        assert args == [101]

        # gte
        q, args = query(u=user).where(u__id__gte=101).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" >= $1'
        assert args == [101]

        # gt
        q, args = query(u=user).where(u__id__gt=101).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" > $1'
        assert args == [101]

        with pytest.raises(ValueError, match=r"Invalid lookup:*"):
            query(u=user).where(u__id__invalid__lookup=101).sql()

        with pytest.raises(ValueError, match=r"Unsupported lookup:*"):
            query(u=user).where(u__id__qte=101).sql()

        with pytest.raises(ValueError, match=r"Invalid argument:*"):
            query(u=user).where(Q("u.id=1")).sql()

        with pytest.raises(
            ValueError, match="Cannot specify both 'on' and 'using' for a join condition"
        ):
            query(u=user).source(p=Join(user, on=Q(p__id=Exp("u.id")), using=["id"])).sql()

        with pytest.raises(ValueError, match="Missing join condition"):
            query(u=user).source(p=Join(user)).sql()

    def test_where_condition_with_q(self, user):
        q, args = query(u=user).where(Q(u__id__in=[1, 2, 3])).sql()
        assert q == 'SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" = any($1::int[])'
        assert args == [[1, 2, 3]]

        # or
        q, args = query(u=user).where(Q(u__id__in=[1, 2, 3]) | Q(u__name__startswith="John")).sql()
        assert (
            q
            == """SELECT *\nFROM public.user AS "u"\nWHERE ("u"."id" = any($1::int[]) OR "u"."name" LIKE $2 || '%')"""
        )
        assert args == [[1, 2, 3], "John"]

        john_or_jane = Q(u__name__startswith="John") | Q(u__name__startswith="Jane")
        q, args = query(u=user).where(john_or_jane).sql()
        assert (
            q
            == """SELECT *\nFROM public.user AS "u"\nWHERE ("u"."name" LIKE $1 || '%' OR "u"."name" LIKE $2 || '%')"""
        )

        q, args = query(u=user).where(~john_or_jane).sql()
        assert (
            q
            == """SELECT *\nFROM public.user AS "u"\nWHERE NOT (("u"."name" LIKE $1 || '%' OR "u"."name" LIKE $2 || '%'))"""
        )

    def test_where_condition_with_subquery(self, user):
        q, args = (
            query(u=user)
            .where(u__id__in=query(u=user).select("id").where(u__name__startswith="John"))
            .sql()
        )
        assert (
            q
            == """SELECT *\nFROM public.user AS "u"\nWHERE "u"."id" IN (\n  SELECT id\n  FROM public.user AS "u"\n  WHERE "u"."name" LIKE $1 || '%'\n)"""
        )
        assert args == ["John"]

    def test_joins(self, user, profile):
        q, _ = query(u=user).source(p=Join(profile, on=Q(p__user_id=Exp('"u"."id"')))).sql()
        assert (
            q
            == 'SELECT *\nFROM public.user AS "u"\n  INNER JOIN public.profile AS "p" ON ("p"."user_id" = "u"."id")'
        )

        q, _ = (
            query(u=user).source(p=Join("public.profile", on=Q(p__user_id=Exp('"u"."id"')))).sql()
        )
        assert (
            q
            == 'SELECT *\nFROM public.user AS "u"\n  INNER JOIN public.profile AS "p" ON ("p"."user_id" = "u"."id")'
        )

    def test_group_by(self, profile):
        q, _ = (
            query(p=profile)
            .group_by("p.org_id")
            .select("p.org_id", user_count="count(p.user_id)")
            .sql()
        )
        assert (
            q
            == 'SELECT p.org_id, count(p.user_id) "user_count"\nFROM public.profile AS "p"\nGROUP BY p.org_id'
        )

    def test_order_by(self, profile):
        q, _ = (
            query(p=profile)
            .group_by("p.org_id")
            .order_by("p.org_id")
            .select("p.org_id", user_count="count(p.user_id)")
            .sql()
        )
        assert (
            q
            == 'SELECT p.org_id, count(p.user_id) "user_count"\nFROM public.profile AS "p"\nGROUP BY p.org_id\nORDER BY p.org_id'
        )

        q, _ = (
            query(p=profile)
            .group_by("p.org_id")
            .order_by("-p.org_id")
            .select("p.org_id", "p.title", user_count="count(p.user_id)")
            .sql()
        )
        assert (
            q
            == 'SELECT p.org_id, p.title, count(p.user_id) "user_count"\nFROM public.profile AS "p"\nGROUP BY p.org_id\nORDER BY p.org_id DESC'
        )

    def test_joins_with_subquery(self, user, profile):
        manager_id = 520
        direct_reports_profile = query(p=profile).where(p__manager_id=manager_id)
        q, args = (
            query(u=user)
            .source(p=Join(direct_reports_profile, on=Q(p__user_id=Exp('"u"."id"'))))
            .select("u.id", "u.name")
            .sql()
        )
        assert (
            q
            == 'SELECT u.id, u.name\nFROM public.user AS "u"\n  INNER JOIN (\n    SELECT *\n    FROM public.profile AS "p"\n    WHERE "p"."manager_id" = $1\n  ) AS "p" ON ("p"."user_id" = "u"."id")'
        )
        assert args == [520]


class TestUnion:
    def test_union(self, user):
        q1 = query(u=user).where(u__name__startswith="John")
        q2 = query(u=user).where(u__name__startswith="Jane")
        q, args = union(q1, q2).sql()
        assert (
            q
            == """  SELECT *\n  FROM public.user AS "u"\n  WHERE "u"."name" LIKE $1 || '%'\nUNION\n  SELECT *\n  FROM public.user AS "u"\n  WHERE "u"."name" LIKE $2 || '%'"""
        )
        assert args == ["John", "Jane"]

        with pytest.raises(ValueError, match="At least two queries must be provided"):
            union(q1).sql()

    def test_union_all(self, user):
        q1 = query(u=user).where(u__name__startswith="John")
        q2 = query(u=user).where(u__name__startswith="Jane")
        q, args = union(q1, q2, all=True).sql()
        assert (
            q
            == """  SELECT *\n  FROM public.user AS "u"\n  WHERE "u"."name" LIKE $1 || '%'\nUNION ALL\n  SELECT *\n  FROM public.user AS "u"\n  WHERE "u"."name" LIKE $2 || '%'"""
        )
        assert args == ["John", "Jane"]


class TestCTE:
    def test_cte(self, user, profile):
        q, args = cte(
            main=query(dr="direct_reports")
            .source(
                u=Join(user, on=Q(u__id=Exp('"dr"."user_id"')) & Q(u__org_id=Exp('"dr"."org_id"')))
            )
            .select("u.id", "u.name"),
            direct_reports=query(p=profile).where(p__manager_id=520),
        ).sql()
        assert (
            q
            == """WITH "direct_reports" AS (
  SELECT *
  FROM public.profile AS "p"
  WHERE "p"."manager_id" = $1
)
SELECT u.id, u.name
FROM direct_reports AS "dr"
  INNER JOIN public.user AS "u" ON (("u"."id" = "dr"."user_id" AND "u"."org_id" = "dr"."org_id"))"""
        )
        assert args == [520]

        with pytest.raises(ValueError, match="At least one CTE must be provided"):
            cte(main=query(cte="cte_tree")).sql()

    def test_recursive_cte(self, user, profile):
        q, args = recursive_cte(
            main=query(dr="direct_reports"),
            direct_reports=union(
                # direct reports
                query(p=profile).select("p.id", "p.user_id", "p.title").where(p__manager_id=520),
                # indirect reports
                query(p=profile, dr="direct_reports")
                .select("p.id", "p.user_id", "p.title")
                .where(p__manager_id=Exp('"dr"."user_id"')),
                all=True,
            ),
        ).sql()
        expected = '''WITH RECURSIVE "direct_reports" AS (
    SELECT p.id, p.user_id, p.title
    FROM public.profile AS "p"
    WHERE "p"."manager_id" = $1
  UNION ALL
    SELECT p.id, p.user_id, p.title
    FROM public.profile AS "p", direct_reports AS "dr"
    WHERE "p"."manager_id" = "dr"."user_id"
)
SELECT *
FROM direct_reports AS "dr"'''
        assert q == expected
        assert args == [520]

        with pytest.raises(ValueError, match="Exactly one union query must be provided"):
            recursive_cte(main=query(cte="cte_tree")).sql()
