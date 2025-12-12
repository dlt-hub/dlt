import sqlalchemy as sa

from dlt.common.configuration.resolve import resolve_configuration
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)


def test_sqlalchemy_owned_engine_ref_counting_disposes_on_last_return(mocker) -> None:
    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials("sqlite:///:memory:")
        )._bind_dataset_name(dataset_name="test_dataset")
    )

    engine = c.credentials.engine
    dispose_spy = mocker.spy(engine, "dispose")

    assert c.credentials._conn_borrows == 0
    assert c.credentials._conn_owner is True

    conn = c.credentials.borrow_conn()
    assert c.credentials._conn_borrows == 1

    c.credentials.return_conn(conn)

    assert c.credentials._conn_borrows == 0
    dispose_spy.assert_called_once()


def test_sqlalchemy_external_engine_ref_counting_does_not_dispose(mocker) -> None:
    engine = sa.create_engine("sqlite:///:memory:")

    c = resolve_configuration(
        SqlalchemyClientConfiguration(
            credentials=SqlalchemyCredentials(engine)
        )._bind_dataset_name(dataset_name="test_dataset")
    )

    dispose_spy = mocker.spy(engine, "dispose")

    assert c.credentials._conn_owner is False
    assert c.credentials._conn_borrows == 0

    conn = c.credentials.borrow_conn()
    assert c.credentials._conn_borrows == 0

    c.credentials.return_conn(conn)
    assert c.credentials._conn_borrows == 0

    dispose_spy.assert_not_called()

    with engine.connect() as conn:
        conn.execute(sa.text("SELECT 1"))

    engine.dispose()