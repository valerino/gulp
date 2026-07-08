#!/usr/bin/env python3
"""Run the BIG_SIGMAS sigma-zip stress flow as N created users."""

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.integration.test_stress import (  # noqa: E402
    SigmaZipHarnessError,
    _cleanup_worker_users,
    _create_worker_users,
    _delete_op,
    _login_ready,
    _run_query_sigma_zip_big_matches_and_notes,
    _short_error,
    _setup_operation,
)


_EXPECTED_RECORDS_INGESTED = 98633
_EXPECTED_COMPLETED_QUERIES = 1149
_EXPECTED_TOTAL_HITS = 73464


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _split_urls(value: str | None) -> list[str]:
    if not value:
        return []
    return [url.strip() for url in value.split(",") if url.strip()]


def _instance_urls(base_url: str, extra_urls: list[str] | None, instances: int) -> list[str]:
    urls = [base_url, *(extra_urls or [])]
    if instances < 1:
        raise ValueError("--instances must be >= 1")
    if len(urls) != instances:
        raise ValueError(
            f"--instances={instances} requires {instances - 1} additional URL(s), got {len(urls) - 1}"
        )
    return urls


def _debug(message: str, **fields: object) -> None:
    fields.setdefault("worker", "?")
    fields.setdefault("operation", "?")
    fields.setdefault("records_ingested", "?")
    fields.setdefault("completed_queries", "?")
    fields.setdefault("total_hits", "?")
    fields.setdefault("expected_records_ingested", _EXPECTED_RECORDS_INGESTED)
    fields.setdefault("expected_completed_queries", _EXPECTED_COMPLETED_QUERIES)
    fields.setdefault("expected_total_hits", _EXPECTED_TOTAL_HITS)
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    print(f"[sigma-zip-big][debug] {message}{' ' + details if details else ''}", flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create N users and run test_stress.py::test_query_sigma_zip_big_matches_and_notes "
            "with BIG_SIGMAS semantics, one operation per user."
        )
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("GULP_BASE_URL", "http://localhost:8080"),
        help="Gulp server URL (env: GULP_BASE_URL)",
    )
    parser.add_argument(
        "--instances",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_INSTANCES", "1")),
        help="Number of Gulp instances to spread workers across (env: GULP_SIGMA_ZIP_INSTANCES)",
    )
    parser.add_argument(
        "--instance-url",
        action="append",
        default=[],
        help=(
            "Additional Gulp instance URL. Repeat it or pass comma-separated URLs "
            "(env: GULP_SIGMA_ZIP_INSTANCE_URLS)."
        ),
    )
    parser.add_argument(
        "--admin-user",
        default=os.getenv("GULP_TEST_USER", "admin"),
        help="Admin user used to create/delete workers (env: GULP_TEST_USER)",
    )
    parser.add_argument(
        "--admin-password",
        default=os.getenv("GULP_TEST_PASSWORD", "admin"),
        help="Admin password (env: GULP_TEST_PASSWORD)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_USERS", "10")),
        help="Number of created users/concurrent flows (env: GULP_SIGMA_ZIP_USERS, default: 10)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("GULP_SIGMA_ZIP_CONCURRENCY", "0")),
        help=(
            "Maximum worker flows to run at once. Default 0 means all users "
            "(env: GULP_SIGMA_ZIP_CONCURRENCY)."
        ),
    )
    parser.add_argument(
        "--worker-password",
        default=os.getenv("GULP_SIGMA_ZIP_USER_PASSWORD", "TestPass!123"),
        help="Password assigned to created users (env: GULP_SIGMA_ZIP_USER_PASSWORD)",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("GULP_SIGMA_ZIP_CLEANUP", True),
        help="Delete created users at the end; operations are always deleted at the end (env: GULP_SIGMA_ZIP_CLEANUP, default: true)",
    )
    args = parser.parse_args()
    if args.users < 1:
        parser.error("--users must be >= 1")
    env_urls = _split_urls(os.getenv("GULP_SIGMA_ZIP_INSTANCE_URLS"))
    cli_urls = [url for value in args.instance_url for url in _split_urls(value)]
    try:
        args.instance_urls = _instance_urls(args.base_url, cli_urls or env_urls, args.instances)
    except ValueError as exc:
        parser.error(str(exc))
    if args.concurrency == 0:
        args.concurrency = args.users
    if args.concurrency < 1:
        parser.error("--concurrency must be >= 1, or 0 for all users")
    return args


async def _worker(
    base_url: str,
    user: str,
    password: str,
    worker_id: int,
    operation_id: str,
    instance_urls: list[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {"worker_id": worker_id, "user_id": user, "success": False}
    started = time.monotonic()
    _debug(
        "worker start",
        worker=worker_id,
        user=user,
        url=base_url,
        operation=operation_id,
    )
    try:
        result.update(
            await _run_query_sigma_zip_big_matches_and_notes(
                base_url,
                user,
                password,
                big=True,
                cleanup=False,
                operation_id=operation_id,
                instance_urls=instance_urls,
                operation_index=worker_id,
            )
        )
        result["status"] = result.get("status", "done")
        result["success"] = result["status"] == "done"
    except SigmaZipHarnessError as exc:
        result.update(exc.totals)
        result["success"] = False
        result["status"] = result.get("status", "failed")
        result["error"] = _short_error(exc)
    except Exception as exc:
        result["status"] = "error"
        result["error"] = _short_error(exc)
    result["total_secs"] = round(time.monotonic() - started, 2)
    _debug(
        "worker finish",
        worker=worker_id,
        success=result["success"],
        status=result.get("status", "?"),
        operation=operation_id,
        query_url=result.get("query_url", "?"),
        records_ingested=result.get("records_ingested", "?"),
        completed_queries=result.get("completed_queries", "?"),
        total_hits=result.get("total_hits", "?"),
        notes=result.get("notes", "?"),
        ws_stats_update_count=result.get("ws_stats_update_count", "?"),
        ws_query_done_count=result.get("ws_query_done_count", "?"),
        ws_collab_create_count=result.get("ws_collab_create_count", "?"),
        total_secs=result["total_secs"],
        error=result.get("error", ""),
    )
    return result


async def _create_worker_operation(base_url: str, user: str, password: str) -> str:
    from gulp_sdk import GulpClient

    async with GulpClient(base_url) as client:
        await _login_ready(client, user, password)
        return await _setup_operation(client)


async def _run(args: argparse.Namespace) -> int:
    from gulp_sdk import GulpClient

    worker_users: list[str] = []
    operation_ids: list[str] = []
    worker_urls = [args.instance_urls[i % len(args.instance_urls)] for i in range(args.users)]
    worker_concurrency = getattr(args, "concurrency", args.users)
    _debug(
        "start",
        users=args.users,
        instances=len(args.instance_urls),
        concurrency=worker_concurrency,
        base_url=args.base_url,
        cleanup=args.cleanup,
        urls=",".join(args.instance_urls),
    )
    async with GulpClient(args.base_url) as admin_client:
        await _login_ready(admin_client, args.admin_user, args.admin_password)
        _debug("admin ready", user=args.admin_user, url=args.base_url)
        worker_users = await _create_worker_users(
            admin_client,
            args.users,
            args.worker_password,
            permission=["read", "edit", "ingest", "delete"],
        )
        _debug("worker users created", count=len(worker_users), users=",".join(worker_users))
        try:
            operation_ids = await asyncio.gather(
                *[
                    _create_worker_operation(worker_urls[i], user, args.worker_password)
                    for i, user in enumerate(worker_users)
                ]
            )
            for i, operation_id in enumerate(operation_ids):
                _debug(
                    "operation ready",
                    worker=i,
                    user=worker_users[i],
                    url=worker_urls[i],
                    operation=operation_id,
                )
            _debug("workers launching", count=len(worker_users))
            semaphore = asyncio.Semaphore(worker_concurrency)

            async def _run_worker(i: int) -> dict[str, Any]:
                async with semaphore:
                    return await _worker(
                        worker_urls[i],
                        worker_users[i],
                        args.worker_password,
                        i,
                        operation_ids[i],
                        args.instance_urls,
                    )

            tasks = [
                asyncio.create_task(_run_worker(i))
                for i in range(args.users)
            ]
            results = await asyncio.gather(*tasks)
            passed = [r for r in results if r.get("success")]
            failed = [r for r in results if not r.get("success")]
            total_records_ingested = sum(int(r.get("records_ingested", 0) or 0) for r in results)
            total_completed_queries = sum(int(r.get("completed_queries", 0) or 0) for r in results)
            total_hits = sum(int(r.get("total_hits", 0) or 0) for r in results)
            print(
                f"[sigma-zip-big] done passed={len(passed)} failed={len(failed)} "
                f"users={args.users} instances={len(args.instance_urls)} cleanup={args.cleanup} "
                f"records_ingested={total_records_ingested} "
                f"expected_records_ingested={_EXPECTED_RECORDS_INGESTED * args.users} "
                f"completed_queries={total_completed_queries} "
                f"expected_completed_queries={_EXPECTED_COMPLETED_QUERIES * args.users} "
                f"total_hits={total_hits} "
                f"expected_total_hits={_EXPECTED_TOTAL_HITS * args.users}"
            )
            for result in results:
                status = "PASS" if result.get("success") else "FAIL"
                print(
                    f"  {status} worker={result['worker_id']} user={result['user_id']} "
                    f"status={result.get('status', '?')} "
                    f"url={worker_urls[result['worker_id']]} "
                    f"query_url={result.get('query_url', '?')} "
                    f"operation={result.get('operation_id', '?')} total={result.get('total_secs')}s "
                    f"records_ingested={result.get('records_ingested', '?')} "
                    f"expected_records_ingested={result.get('expected_records_ingested', _EXPECTED_RECORDS_INGESTED)} "
                    f"completed_queries={result.get('completed_queries', '?')} "
                    f"expected_completed_queries={result.get('expected_completed_queries', _EXPECTED_COMPLETED_QUERIES)} "
                    f"total_hits={result.get('total_hits', '?')} "
                    f"expected_total_hits={result.get('expected_total_hits', _EXPECTED_TOTAL_HITS)} "
                    f"notes={result.get('notes', '?')} "
                    f"ws_stats={result.get('ws_stats_update_count', '?')} "
                    f"ws_query_done={result.get('ws_query_done_count', '?')} "
                    f"ws_collab_create={result.get('ws_collab_create_count', '?')} "
                    f"error={result.get('error', '')}"
                )
            return 1 if failed else 0
        finally:
            _debug(
                "cleanup start",
                operations=len(operation_ids),
                users=len(worker_users),
                cleanup_users=args.cleanup,
            )
            for operation_id in operation_ids:
                try:
                    await _delete_op(admin_client, operation_id)
                    _debug("operation deleted", operation=operation_id)
                except Exception as exc:
                    _debug(
                        "operation cleanup failed",
                        operation=operation_id,
                        error=_short_error(exc),
                    )
                    pass
            if args.cleanup:
                await _cleanup_worker_users(admin_client, worker_users)
                _debug("worker users deleted", count=len(worker_users))


def main() -> int:
    return asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
