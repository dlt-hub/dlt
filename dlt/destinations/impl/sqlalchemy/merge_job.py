from typing import Sequence, Tuple, Optional, List, Union
import operator

import sqlalchemy as sa

from dlt.destinations.sql_jobs import SqlMergeFollowupJob
from dlt.common.destination import PreparedTableSchema, DestinationCapabilitiesContext
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    get_first_column_name_with_prop,
    is_nested_table,
    get_validity_column_names,
    get_active_record_timestamp,
)
from dlt.common.time import ensure_pendulum_datetime
from dlt.common.storages.load_package import load_package as current_load_package


class SqlalchemyMergeFollowupJob(SqlMergeFollowupJob):
    """Uses SQLAlchemy to generate merge SQL statements.
    Result is equivalent to the SQL generated by `SqlMergeFollowupJob`
    except for delete-insert we use concrete tables instead of temporary tables.
    """

    @classmethod
    def gen_merge_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlalchemyClient,  # type: ignore[override]
    ) -> List[str]:
        root_table = table_chain[0]

        root_table_obj = sql_client.get_existing_table(root_table["name"])
        staging_root_table_obj = root_table_obj.to_metadata(
            sql_client.metadata, schema=sql_client.staging_dataset_name
        )

        primary_key_names = get_columns_names_with_prop(root_table, "primary_key")
        merge_key_names = get_columns_names_with_prop(root_table, "merge_key")

        temp_metadata = sa.MetaData()

        append_fallback = (len(primary_key_names) + len(merge_key_names)) == 0

        sqla_statements = []
        tables_to_drop: List[sa.Table] = (
            []
        )  # Keep track of temp tables to drop at the end of the job

        if not append_fallback:
            key_clause = cls._generate_key_table_clauses(
                primary_key_names, merge_key_names, root_table_obj, staging_root_table_obj
            )

            # Generate the delete statements
            if len(table_chain) == 1 and not cls.requires_temp_table_for_delete():
                delete_statement = root_table_obj.delete().where(
                    sa.exists(
                        sa.select(sa.literal(1))
                        .where(key_clause)
                        .select_from(staging_root_table_obj)
                    )
                )
                sqla_statements.append(delete_statement)
            else:
                row_key_col_name = cls._get_row_key_col(table_chain, sql_client, root_table)
                row_key_col = root_table_obj.c[row_key_col_name]
                # Use a real table cause sqlalchemy doesn't have TEMPORARY TABLE abstractions
                delete_temp_table = sa.Table(
                    "delete_" + root_table_obj.name,
                    temp_metadata,
                    # Give this column a fixed name to be able to reference it later
                    sa.Column("_dlt_id", row_key_col.type),
                    schema=staging_root_table_obj.schema,
                )
                tables_to_drop.append(delete_temp_table)
                # Add the CREATE TABLE statement
                sqla_statements.append(sa.sql.ddl.CreateTable(delete_temp_table))
                # Insert data into the "temporary" table
                insert_statement = delete_temp_table.insert().from_select(
                    [row_key_col],
                    sa.select(row_key_col).where(
                        sa.exists(
                            sa.select(sa.literal(1))
                            .where(key_clause)
                            .select_from(staging_root_table_obj)
                        )
                    ),
                )
                sqla_statements.append(insert_statement)

                for table in table_chain[1:]:
                    chain_table_obj = sql_client.get_existing_table(table["name"])
                    root_key_name = cls._get_root_key_col(table_chain, sql_client, table)
                    root_key_col = chain_table_obj.c[root_key_name]

                    delete_statement = chain_table_obj.delete().where(
                        root_key_col.in_(sa.select(delete_temp_table.c._dlt_id))
                    )

                    sqla_statements.append(delete_statement)

                # Delete from root table
                delete_statement = root_table_obj.delete().where(
                    row_key_col.in_(sa.select(delete_temp_table.c._dlt_id))
                )
                sqla_statements.append(delete_statement)

        hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
            root_table,
            root_table_obj,
            invert=True,
        )

        dedup_sort = get_dedup_sort_tuple(root_table)  # column_name, 'asc' | 'desc'

        if len(table_chain) > 1 and (primary_key_names or hard_delete_col_name is not None):
            condition_column_names = (
                None if hard_delete_col_name is None else [hard_delete_col_name]
            )
            condition_columns = (
                [staging_root_table_obj.c[col_name] for col_name in condition_column_names]
                if condition_column_names is not None
                else []
            )

            staging_row_key_col = staging_root_table_obj.c[row_key_col_name]

            # Create the insert "temporary" table (but use a concrete table)
            insert_temp_table = sa.Table(
                "insert_" + root_table_obj.name,
                temp_metadata,
                sa.Column(row_key_col_name, staging_row_key_col.type),
                schema=staging_root_table_obj.schema,
            )
            tables_to_drop.append(insert_temp_table)
            create_insert_temp_table_statement = sa.sql.ddl.CreateTable(insert_temp_table)
            sqla_statements.append(create_insert_temp_table_statement)
            staging_primary_key_cols = [
                staging_root_table_obj.c[col_name] for col_name in primary_key_names
            ]

            inner_cols = [staging_row_key_col]

            if primary_key_names:
                if dedup_sort is not None:
                    order_by_col = staging_root_table_obj.c[dedup_sort[0]]
                    order_dir_func = sa.asc if dedup_sort[1] == "asc" else sa.desc
                else:
                    order_by_col = sa.select(sa.literal(None))
                    order_dir_func = sa.asc
                if condition_columns:
                    inner_cols += condition_columns

                inner_select = sa.select(
                    sa.func.row_number()
                    .over(
                        partition_by=set(staging_primary_key_cols),
                        order_by=order_dir_func(order_by_col),
                    )
                    .label("_dlt_dedup_rn"),
                    *inner_cols,
                ).subquery()

                select_for_temp_insert = sa.select(inner_select.c[row_key_col_name]).where(
                    inner_select.c._dlt_dedup_rn == 1
                )
                hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
                    root_table,
                    inner_select,
                    invert=True,
                )

                if not_delete_cond is not None:
                    select_for_temp_insert = select_for_temp_insert.where(not_delete_cond)
            else:
                hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
                    root_table,
                    staging_root_table_obj,
                    invert=True,
                )
                select_for_temp_insert = sa.select(staging_row_key_col).where(not_delete_cond)

            insert_into_temp_table = insert_temp_table.insert().from_select(
                [row_key_col_name], select_for_temp_insert
            )
            sqla_statements.append(insert_into_temp_table)

        # Insert from staging to dataset
        for table in table_chain:
            table_obj = sql_client.get_existing_table(table["name"])
            staging_table_obj = table_obj.to_metadata(
                sql_client.metadata, schema=sql_client.staging_dataset_name
            )
            select_sql = staging_table_obj.select()

            if (primary_key_names and len(table_chain) > 1) or (
                not primary_key_names
                and is_nested_table(table)
                and hard_delete_col_name is not None
            ):
                uniq_column_name = root_key_name if is_nested_table(table) else row_key_col_name
                uniq_column = staging_table_obj.c[uniq_column_name]
                select_sql = select_sql.where(
                    uniq_column.in_(
                        sa.select(
                            insert_temp_table.c[row_key_col_name].label(uniq_column_name)
                        ).subquery()
                    )
                )
            elif primary_key_names and len(table_chain) == 1:
                staging_primary_key_cols = [
                    staging_table_obj.c[col_name] for col_name in primary_key_names
                ]
                if dedup_sort is not None:
                    order_by_col = staging_table_obj.c[dedup_sort[0]]
                    order_dir_func = sa.asc if dedup_sort[1] == "asc" else sa.desc
                else:
                    order_by_col = sa.select(sa.literal(None))
                    order_dir_func = sa.asc

                inner_select = sa.select(
                    staging_table_obj,
                    sa.func.row_number()
                    .over(
                        partition_by=set(staging_primary_key_cols),
                        order_by=order_dir_func(order_by_col),
                    )
                    .label("_dlt_dedup_rn"),
                ).subquery()

                select_sql = sa.select(
                    *[c for c in inner_select.c if c.name != "_dlt_dedup_rn"]
                ).where(inner_select.c._dlt_dedup_rn == 1)

                hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
                    root_table, inner_select, invert=True
                )

                if hard_delete_col_name is not None:
                    select_sql = select_sql.where(not_delete_cond)
            else:
                hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
                    root_table, staging_root_table_obj, invert=True
                )

                if hard_delete_col_name is not None:
                    select_sql = select_sql.where(not_delete_cond)

            insert_statement = table_obj.insert().from_select(
                [col.name for col in table_obj.columns], select_sql
            )
            sqla_statements.append(insert_statement)

        # Drop all "temp" tables at the end
        for table_obj in tables_to_drop:
            sqla_statements.append(sa.sql.ddl.DropTable(table_obj))

        return [
            x + ";" if not x.endswith(";") else x
            for x in (
                str(stmt.compile(sql_client.engine, compile_kwargs={"literal_binds": True}))
                for stmt in sqla_statements
            )
        ]

    @classmethod
    def _get_hard_delete_col_and_cond(  # type: ignore[override]
        cls,
        table: PreparedTableSchema,
        table_obj: sa.Table,
        invert: bool = False,
    ) -> Tuple[Optional[str], Optional[sa.sql.elements.BinaryExpression]]:
        col_name = get_first_column_name_with_prop(table, "hard_delete")
        if col_name is None:
            return None, None
        col = table_obj.c[col_name]
        if invert:
            cond = col.is_(None)
        else:
            cond = col.isnot(None)
        if table["columns"][col_name]["data_type"] == "bool":
            if invert:
                cond = sa.or_(cond, col.is_(False))
            else:
                cond = col.is_(True)
        return col_name, cond

    @classmethod
    def _generate_key_table_clauses(
        cls,
        primary_keys: Sequence[str],
        merge_keys: Sequence[str],
        root_table_obj: sa.Table,
        staging_root_table_obj: sa.Table,
    ) -> sa.sql.ClauseElement:
        # Returns an sqlalchemy or_ clause
        clauses = []
        if primary_keys or merge_keys:
            for key in primary_keys:
                clauses.append(
                    sa.and_(
                        *[
                            root_table_obj.c[key] == staging_root_table_obj.c[key]
                            for key in primary_keys
                        ]
                    )
                )
            for key in merge_keys:
                clauses.append(
                    sa.and_(
                        *[
                            root_table_obj.c[key] == staging_root_table_obj.c[key]
                            for key in merge_keys
                        ]
                    )
                )
            return sa.or_(*clauses)  # type: ignore[no-any-return]
        else:
            return sa.true()  # type: ignore[no-any-return]

    @classmethod
    def _gen_concat_sqla(
        cls, columns: Sequence[sa.Column]
    ) -> Union[sa.sql.elements.BinaryExpression, sa.Column]:
        # Use col1 + col2 + col3 ... to generate a dialect specific concat expression
        result = columns[0]
        if len(columns) == 1:
            return result
        # Cast because CONCAT is only generated for string columns
        result = sa.cast(result, sa.String)
        for col in columns[1:]:
            result = operator.add(result, sa.cast(col, sa.String))
        return result

    @classmethod
    def gen_scd2_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlalchemyClient,  # type: ignore[override]
    ) -> List[str]:
        sqla_statements = []
        root_table = table_chain[0]
        root_table_obj = sql_client.get_existing_table(root_table["name"])
        staging_root_table_obj = root_table_obj.to_metadata(
            sql_client.metadata, schema=sql_client.staging_dataset_name
        )

        from_, to = get_validity_column_names(root_table)
        hash_ = get_first_column_name_with_prop(root_table, "x-row-version")

        caps = sql_client.capabilities

        format_datetime_literal = caps.format_datetime_literal
        if format_datetime_literal is None:
            format_datetime_literal = (
                DestinationCapabilitiesContext.generic_capabilities().format_datetime_literal
            )

        boundary_ts = ensure_pendulum_datetime(
            root_table.get("x-boundary-timestamp", current_load_package()["state"]["created_at"])  # type: ignore[arg-type]
        )

        boundary_literal = format_datetime_literal(boundary_ts, caps.timestamp_precision)

        active_record_timestamp = get_active_record_timestamp(root_table)

        update_statement = (
            root_table_obj.update()
            .values({to: sa.text(boundary_literal)})
            .where(root_table_obj.c[hash_].notin_(sa.select(staging_root_table_obj.c[hash_])))
        )

        if active_record_timestamp is None:
            active_record_literal = None
            root_is_active_clause = root_table_obj.c[to].is_(None)
        else:
            active_record_literal = format_datetime_literal(
                active_record_timestamp, caps.timestamp_precision
            )
            root_is_active_clause = root_table_obj.c[to] == sa.text(active_record_literal)

        update_statement = update_statement.where(root_is_active_clause)

        merge_keys = get_columns_names_with_prop(root_table, "merge_key")
        if merge_keys:
            root_merge_key_cols = [root_table_obj.c[key] for key in merge_keys]
            staging_merge_key_cols = [staging_root_table_obj.c[key] for key in merge_keys]

            update_statement = update_statement.where(
                cls._gen_concat_sqla(root_merge_key_cols).in_(
                    sa.select(cls._gen_concat_sqla(staging_merge_key_cols))
                )
            )

        sqla_statements.append(update_statement)

        insert_statement = root_table_obj.insert().from_select(
            [col.name for col in root_table_obj.columns],
            sa.select(
                sa.literal(boundary_literal.strip("'")).label(from_),
                sa.literal(
                    active_record_literal.strip("'") if active_record_literal is not None else None
                ).label(to),
                *[c for c in staging_root_table_obj.columns if c.name not in [from_, to]],
            ).where(
                staging_root_table_obj.c[hash_].notin_(
                    sa.select(root_table_obj.c[hash_]).where(root_is_active_clause)
                )
            ),
        )
        sqla_statements.append(insert_statement)

        nested_tables = table_chain[1:]
        for table in nested_tables:
            row_key_column = cls._get_root_key_col(table_chain, sql_client, table)

            table_obj = sql_client.get_existing_table(table["name"])
            staging_table_obj = table_obj.to_metadata(
                sql_client.metadata, schema=sql_client.staging_dataset_name
            )

            insert_statement = table_obj.insert().from_select(
                [col.name for col in table_obj.columns],
                staging_table_obj.select().where(
                    staging_table_obj.c[row_key_column].notin_(
                        sa.select(table_obj.c[row_key_column])
                    )
                ),
            )
            sqla_statements.append(insert_statement)

        return [
            x + ";" if not x.endswith(";") else x
            for x in (
                str(stmt.compile(sql_client.engine, compile_kwargs={"literal_binds": True}))
                for stmt in sqla_statements
            )
        ]
