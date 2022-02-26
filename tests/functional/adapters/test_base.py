import pytest
from dbt.tests.util import run_dbt, get_manifest, update_config_file
from dbt.tests.adapter import relation_from_name, check_relation_types, check_relations_equal
from tests.functional.adapters.files import (
    seeds_base_csv,
    base_view,
    base_table,
    base_materialized_var,
    schema_base_yml,
)


@pytest.fixture
def models():
    return {
        "view_model.sql": base_view,
        "table_model.sql": base_table,
        "swappable.sql": base_materialized_var,
        "schema.yml": schema_base_yml,
    }


@pytest.fixture
def seeds():
    return {
        "base.csv": seeds_base_csv,
    }


@pytest.fixture
def project_config_update():
    return {
        "name": "base",
    }


def test_base(project):

    # seed command
    results = run_dbt(["seed"])
    # seed result length
    assert len(results) == 1
    manifest = get_manifest(project.project_root)

    # seed node exists
    assert "seed.base.base" in manifest.nodes

    # run command
    results = run_dbt()
    # run result length
    assert len(results) == 3
    manifest = get_manifest(project.project_root)

    # names exist in nodes
    assert "model.base.view_model" in manifest.nodes
    assert "model.base.table_model" in manifest.nodes
    assert "model.base.swappable" in manifest.nodes

    # length of nodes and sources
    assert len(manifest.nodes) == 4
    assert len(manifest.sources) == 1

    # relations_equal
    check_relations_equal(project.adapter, ["base", "view_model", "table_model", "swappable"])

    # base table rowcount
    relation = relation_from_name(project.adapter, "base")
    result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
    assert result[0] == 10

    # check relation types
    expected = {
        "base": "table",
        "view_model": "view",
        "table_model": "table",
        "swappable": "table",
    }
    check_relation_types(project.adapter, expected)

    # Change var and check relation types
    # "swappable" should have changed to "view"
    update_config_file(
        {"vars": {"materialized_var": "view"}}, project.project_root, "dbt_project.yml"
    )
    run_dbt()
    # check relation types
    expected = {
        "base": "table",
        "view_model": "view",
        "table_model": "table",
        "swappable": "view",
    }
    check_relation_types(project.adapter, expected)
