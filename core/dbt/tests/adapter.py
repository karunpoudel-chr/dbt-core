from itertools import chain, repeat
from dbt.context import providers
from unittest.mock import patch


class TestProcessingException(Exception):
    pass


def relation_from_name(adapter, name: str):
    """reverse-engineer a relation (including quoting) from a given name and
    the adapter. Assumes that relations are split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()
    kwargs = {}  # This will contain database, schema, identifier

    parts = name.split(".")
    names = ["database", "schema", "identifier"]
    defaults = [credentials.database, credentials.schema, None]
    values = chain(repeat(None, 3 - len(parts)), parts)
    for name, value, default in zip(names, values, defaults):
        # no quote policy -> use the default
        if value is None:
            if default is None:
                include_policy[name] = False
            value = default
        else:
            include_policy[name] = True
            # if we have a value, we can figure out the quote policy.
            trimmed = value[1:-1]
            if adapter.quote(trimmed) == value:
                quote_policy[name] = True
                value = trimmed
            else:
                quote_policy[name] = False
        kwargs[name] = value

    relation = cls.create(
        include_policy=include_policy,
        quote_policy=quote_policy,
        **kwargs,
    )
    return relation


def check_relation_types(adapter, relation_to_type):

    expected_relation_values = {}
    found_relations = []
    schemas = set()

    for key, value in relation_to_type.items():
        relation = relation_from_name(adapter, key)
        expected_relation_values[relation] = value
        schemas.add(relation.without_identifier())

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("__test"):
            for schema in schemas:
                found_relations.extend(adapter.list_relations_without_caching(schema))

    for key, value in relation_to_type.items():
        for relation in found_relations:
            # this might be too broad
            if relation.identifier == key:
                assert relation.type == value, (
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


def check_relations_equal(adapter, relation_names):
    if len(relation_names) < 2:
        raise TestProcessingException(
            "Not enough relations to compare",
        )
    relations = [relation_from_name(adapter, name) for name in relation_names]

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("_test"):
            basis, compares = relations[0], relations[1:]
            columns = [c.name for c in adapter.get_columns_in_relation(basis)]

            for relation in compares:
                sql = adapter.get_rows_different_sql(basis, relation, column_names=columns)
                _, tbl = adapter.execute(sql, fetch=True)
                num_rows = len(tbl)
                assert (
                    num_rows == 1
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of rows ({num_rows})"
                num_cols = len(tbl[0])
                assert (
                    num_cols == 2
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of cols ({num_cols})"
                row_count_difference = tbl[0][0]
                assert (
                    row_count_difference == 0
                ), f"Got {row_count_difference} difference in row count betwen {basis} and {relation}"
                rows_mismatched = tbl[0][1]
                assert (
                    rows_mismatched == 0
                ), f"Got {rows_mismatched} different rows between {basis} and {relation}"
