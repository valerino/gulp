import pytest

from gulp.config import GulpConfig


def _config_with(overrides: dict) -> GulpConfig:
    cfg = object.__new__(GulpConfig)
    cfg._config = overrides
    cfg._concurrency_num_tasks = None
    return cfg


@pytest.mark.unit
def test_adaptive_concurrency_respects_small_cap():
    cfg = _config_with(
        {
            "concurrency_adaptive_num_tasks": True,
            "concurrency_num_tasks": 2,
            "concurrency_opensearch_num_nodes": 1,
            "concurrency_postgres_num_nodes": 1,
            "concurrency_tasks_cap_per_process": 2,
        }
    )

    assert cfg.concurrency_num_tasks() == 2


@pytest.mark.unit
def test_adaptive_concurrency_clamps_invalid_counts():
    cfg = _config_with(
        {
            "concurrency_adaptive_num_tasks": True,
            "concurrency_num_tasks": 2,
            "concurrency_opensearch_num_nodes": 0,
            "concurrency_postgres_num_nodes": "bad",
            "concurrency_tasks_cap_per_process": 0,
        }
    )

    assert cfg.concurrency_opensearch_num_nodes() == 1
    assert cfg.concurrency_postgres_num_nodes() == 1
    assert cfg.concurrency_tasks_cap_per_process() == 1
    assert cfg.concurrency_num_tasks() == 1


@pytest.mark.unit
def test_opensearch_pool_config_defaults_and_clamps():
    cfg = _config_with({})

    assert cfg.opensearch_pool_maxsize() == 10

    cfg = _config_with(
        {
            "opensearch_pool_maxsize": 0,
        }
    )

    assert cfg.opensearch_pool_maxsize() == 1
