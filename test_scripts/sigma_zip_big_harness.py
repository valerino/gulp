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
    _cleanup_worker_users,
    _create_worker_users,
    _delete_op,
    _login_ready,
    _run_query_sigma_zip_big_matches_and_notes,
    _short_error,
    _setup_operation,
)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


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
        "--worker-password",
        default=os.getenv("GULP_SIGMA_ZIP_USER_PASSWORD", "TestPass!123"),
        help="Password assigned to created users (env: GULP_SIGMA_ZIP_USER_PASSWORD)",
    )
    parser.add_argument(
        "--cleanup",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("GULP_SIGMA_ZIP_CLEANUP", True),
        help="Delete created operations/data and users at the end (env: GULP_SIGMA_ZIP_CLEANUP, default: true)",
    )
    args = parser.parse_args()
    if args.users < 1:
        parser.error("--users must be >= 1")
    return args


async def _worker(
    base_url: str,
    user: str,
    password: str,
    worker_id: int,
    cleanup: bool,
    operation_id: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {"worker_id": worker_id, "user_id": user, "success": False}
    started = time.monotonic()
    try:
        result.update(
            await _run_query_sigma_zip_big_matches_and_notes(
                base_url,
                user,
                password,
                big=True,
                cleanup=cleanup,
                operation_id=operation_id,
            )
        )
        result["success"] = True
    except Exception as exc:
        result["error"] = _short_error(exc)
    result["total_secs"] = round(time.monotonic() - started, 2)
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
    async with GulpClient(args.base_url) as admin_client:
        await _login_ready(admin_client, args.admin_user, args.admin_password)
        worker_users = await _create_worker_users(
            admin_client,
            args.users,
            args.worker_password,
            permission=["read", "edit", "ingest", "delete"],
        )
        try:
            operation_ids = await asyncio.gather(
                *[
                    _create_worker_operation(args.base_url, user, args.worker_password)
                    for user in worker_users
                ]
            )
            tasks = [
                asyncio.create_task(
                    _worker(
                        args.base_url,
                        worker_users[i],
                        args.worker_password,
                        i,
                        args.cleanup,
                        operation_ids[i],
                    )
                )
                for i in range(args.users)
            ]
            results = await asyncio.gather(*tasks)
        finally:
            if args.cleanup:
                for operation_id in operation_ids:
                    try:
                        await _delete_op(admin_client, operation_id)
                    except Exception:
                        pass
                await _cleanup_worker_users(admin_client, worker_users)

    passed = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    print(
        f"[sigma-zip-big] done passed={len(passed)} failed={len(failed)} "
        f"users={args.users} cleanup={args.cleanup}"
    )
    for result in results:
        status = "PASS" if result.get("success") else "FAIL"
        print(
            f"  {status} worker={result['worker_id']} user={result['user_id']} "
            f"operation={result.get('operation_id', '?')} total={result.get('total_secs')}s "
            f"notes={result.get('notes', '?')} error={result.get('error', '')}"
        )
    return 1 if failed else 0


def main() -> int:
    return asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
