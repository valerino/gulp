"""Unit checks for sigma_zip_big_harness helpers."""

import sys
from types import SimpleNamespace

import pytest

from tests.integration import test_stress
from tests.integration.test_stress import (
    SigmaZipHarnessError,
    _count_operation_notes,
    _round_robin_instance_url,
    _should_print_received_count,
    _wait_request_stats_progress,
)
from test_scripts import sigma_zip_big_harness
from test_scripts.sigma_zip_big_harness import _debug, _instance_urls, _parse_args, _split_urls


class _FakeCollab:
    def __init__(self, pages: list[list[object]]) -> None:
        self._pages = pages
        self.calls: list[dict[str, object]] = []

    async def note_list(self, **kwargs) -> list[object]:
        self.calls.append(kwargs)
        return self._pages.pop(0)


class _FakeClient:
    def __init__(self, pages: list[list[object]]) -> None:
        self.collab = _FakeCollab(pages)


class _FakePlugins:
    def __init__(self, stats: list[dict[str, object]]) -> None:
        self._stats = stats

    async def request_get(self, _req_id: str) -> dict[str, object]:
        return self._stats.pop(0)


class _FakeStatsClient:
    def __init__(self, stats: list[dict[str, object]]) -> None:
        self.plugins = _FakePlugins(stats)


@pytest.mark.unit
def test_received_count_debug_sampling() -> None:
    assert _should_print_received_count(1, 1149)
    assert _should_print_received_count(1000, 1149)
    assert _should_print_received_count(1149, 1149)
    assert not _should_print_received_count(999, 1149)


@pytest.mark.unit
def test_debug_prints_message_and_fields(capsys) -> None:
    _debug("worker start", worker=1, url="http://localhost:8080")

    assert capsys.readouterr().out == (
        "[sigma-zip-big][debug] worker start worker=1 "
        "url=http://localhost:8080 "
        "operation=? "
        "records_ingested=? "
        "completed_queries=? "
        "total_hits=? "
        "expected_records_ingested=98633 "
        "expected_completed_queries=1149 "
        "expected_total_hits=73464\n"
    )


@pytest.mark.unit
def test_instance_urls_use_base_plus_additional_urls() -> None:
    urls = _instance_urls(
        "http://localhost:8080",
        _split_urls("http://localhost:8100, http://localhost:8101"),
        3,
    )

    assert urls == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
    ]


@pytest.mark.unit
def test_instance_urls_requires_declared_count() -> None:
    with pytest.raises(ValueError, match="additional URL"):
        _instance_urls("http://localhost:8080", ["http://localhost:8100"], 3)


@pytest.mark.unit
def test_parse_args_defaults_to_all_users_concurrent(monkeypatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sigma_zip_big_harness.py",
            "--users",
            "4",
            "--instances",
            "1",
        ],
    )

    args = _parse_args()

    assert args.concurrency == 4


@pytest.mark.unit
def test_ingest_requests_are_split_by_flattened_operation_file_index() -> None:
    urls = ["http://localhost:8080", "http://localhost:8100", "http://localhost:8101"]

    selected = [
        _round_robin_instance_url(urls, operation_index, file_index, 24)
        for operation_index in range(10)
        for file_index in range(24)
    ]

    assert selected[:6] == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
    ]
    assert {
        url: selected.count(url)
        for url in urls
    } == {
        "http://localhost:8080": 80,
        "http://localhost:8100": 80,
        "http://localhost:8101": 80,
    }


@pytest.mark.unit
def test_query_requests_are_split_by_operation_index() -> None:
    urls = ["http://localhost:8080", "http://localhost:8100", "http://localhost:8101"]

    selected = [
        _round_robin_instance_url(urls, operation_index, 0, 1)
        for operation_index in range(10)
    ]

    assert selected == [
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
        "http://localhost:8100",
        "http://localhost:8101",
        "http://localhost:8080",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_defers_cleanup_to_harness_finally(monkeypatch) -> None:
    observed: dict[str, object] = {}

    async def _fake_run(*args, **kwargs):
        observed.update(kwargs)
        return {"operation_id": kwargs["operation_id"]}

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "user-a",
        "pw",
        7,
        "op-a",
        ["http://localhost:8080", "http://localhost:8100"],
    )

    assert result["success"] is True
    assert observed["cleanup"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_partial_totals_can_succeed(monkeypatch) -> None:
    async def _fake_run(*args, **kwargs):
        return {
            "operation_id": kwargs["operation_id"],
            "status": "done",
            "records_ingested": 12,
            "completed_queries": 3,
            "total_hits": 7,
            "notes": 7,
        }

    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run,
    )

    result = await sigma_zip_big_harness._worker(
        "http://localhost:8080",
        "user-a",
        "pw",
        7,
        "op-a",
        ["http://localhost:8080", "http://localhost:8100"],
    )

    assert result["success"] is True
    assert result["status"] == "done"
    assert result["records_ingested"] == 12
    assert result["completed_queries"] == 3
    assert result["total_hits"] == 7
    assert result["notes"] == 7
    assert "error" not in result


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_deletes_operations_after_final_status_output(monkeypatch, capsys) -> None:
    class _FakeGulpClient:
        def __init__(self, base_url: str, token: str | None = None) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    async def _fake_run_flow(*args, **kwargs):
        return {
            "operation_id": kwargs["operation_id"],
            "status": "done",
            "completed_queries": 1,
        }

    async def _fake_delete_op(*args, **kwargs) -> None:
        return None

    monkeypatch.setitem(sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient))
    monkeypatch.setattr(sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop())
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_worker_users",
        lambda *args, **kwargs: _return(["user-a"]),
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_setup_operation", lambda *args: _return("op-a"))
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", lambda *args: _noop()
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=True,
            instance_urls=["http://localhost:8080"],
        )
    )

    out = capsys.readouterr().out
    assert rc == 0
    assert "expected_total_hits=73464" in out
    assert out.index("status=done") < out.index("cleanup start")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_always_deletes_operations_but_cleanup_controls_users(monkeypatch) -> None:
    class _FakeGulpClient:
        def __init__(self, base_url: str, token: str | None = None) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    deleted_operations: list[str] = []
    cleaned_users: list[str] = []

    async def _fake_run_flow(*args, **kwargs):
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    async def _fake_delete_op(_client, operation_id: str) -> None:
        deleted_operations.append(operation_id)

    async def _fake_cleanup_users(_client, users: list[str]) -> None:
        cleaned_users.extend(users)

    monkeypatch.setitem(sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient))
    monkeypatch.setattr(sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop())
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_worker_users",
        lambda *args, **kwargs: _return(["user-a"]),
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_setup_operation", lambda *args: _return("op-a"))
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", _fake_delete_op)
    monkeypatch.setattr(sigma_zip_big_harness, "_cleanup_worker_users", _fake_cleanup_users)

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=1,
            worker_password="pw",
            cleanup=False,
            instance_urls=["http://localhost:8080"],
        )
    )

    assert rc == 0
    assert deleted_operations == ["op-a"]
    assert cleaned_users == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_limits_worker_concurrency(monkeypatch) -> None:
    class _FakeGulpClient:
        def __init__(self, base_url: str, token: str | None = None) -> None:
            self.base_url = base_url.rstrip("/")
            self.token = token or "token"

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args) -> None:
            return None

    active = 0
    max_active = 0

    async def _fake_run_flow(*args, **kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await sigma_zip_big_harness.asyncio.sleep(0)
        active -= 1
        return {"operation_id": kwargs["operation_id"], "status": "done"}

    monkeypatch.setitem(sys.modules, "gulp_sdk", SimpleNamespace(GulpClient=_FakeGulpClient))
    monkeypatch.setattr(sigma_zip_big_harness, "_login_ready", lambda *args, **kwargs: _noop())
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_create_worker_users",
        lambda *args, **kwargs: _return(["user-a", "user-b", "user-c"]),
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_setup_operation", lambda *args: _return("op-a"))
    monkeypatch.setattr(
        sigma_zip_big_harness,
        "_run_query_sigma_zip_big_matches_and_notes",
        _fake_run_flow,
    )
    monkeypatch.setattr(sigma_zip_big_harness, "_delete_op", lambda *args: _noop())
    monkeypatch.setattr(
        sigma_zip_big_harness, "_cleanup_worker_users", lambda *args: _noop()
    )

    rc = await sigma_zip_big_harness._run(
        SimpleNamespace(
            base_url="http://localhost:8080",
            admin_user="admin",
            admin_password="admin",
            users=3,
            worker_password="pw",
            cleanup=True,
            instance_urls=["http://localhost:8080", "http://localhost:8100"],
            concurrency=1,
        )
    )

    assert rc == 0
    assert max_active == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_wait_request_stats_progress_uses_polled_stats(monkeypatch) -> None:
    async def _no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(test_stress.asyncio, "sleep", _no_sleep)
    client = _FakeStatsClient(
        [
            {"status": "ongoing", "data": {"completed_queries": 1}},
            {"status": "done", "data": {"completed_queries": 2}},
        ]
    )

    stats = await _wait_request_stats_progress(
        client,
        "req-a",
        lambda item: int((item.get("data") or {}).get("completed_queries", 0)) >= 2,
        timeout_sec=1,
        label="query",
    )

    assert stats["status"] == "done"


async def _noop() -> None:
    return None


async def _return(value: object) -> object:
    return value


@pytest.mark.unit
@pytest.mark.asyncio
async def test_count_operation_notes_paginates_until_short_page() -> None:
    client = _FakeClient([[object(), object()], [object()], []])

    assert await _count_operation_notes(client, "op-a", page_size=2) == 3
    assert client.collab.calls == [
        {
            "operation_id": "op-a",
            "flt": {"operation_ids": ["op-a"], "limit": 2, "offset": 0},
        },
        {
            "operation_id": "op-a",
            "flt": {"operation_ids": ["op-a"], "limit": 2, "offset": 2},
        },
    ]
