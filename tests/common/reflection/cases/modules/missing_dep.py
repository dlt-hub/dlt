from n import x, make_type  # a regular missing import
from aleph import Zero  # a type

# module level code calling into the missing module, like psycopg2 type registration
interval_type = make_type((1186,), "INTERVAL", lambda value, cur: value)
int_type = x.types.Integer


class One(Zero):
    pass


def f() -> Zero:
    pass
