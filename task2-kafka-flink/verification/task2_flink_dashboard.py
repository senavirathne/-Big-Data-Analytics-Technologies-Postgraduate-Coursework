#!/usr/bin/env python3
"""Deploy the coursework JAR through Flink's Web Dashboard and save evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from playwright.sync_api import Page, sync_playwright


JOB_NAME = "Austin traffic telemetry: 10-minute sensor totals"
PROGRAM_ARGUMENTS = "--bootstrap-servers kafka:9092 --topic traffic-telemetry"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--base-url",
        default=os.getenv("FLINK_BASE_URL", "http://flink-jobmanager:8081"),
    )
    parser.add_argument(
        "--jar",
        type=Path,
        default=Path("/workspace/artifacts/traffic-window-job.jar"),
    )
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=Path("/workspace/evidence"),
    )
    return parser.parse_args()


def read_json(url: str, timeout: float = 10) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_for_dashboard(base_url: str, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    last_error = "dashboard not queried"
    while time.monotonic() < deadline:
        try:
            overview = read_json(f"{base_url}/overview")
            if int(overview.get("taskmanagers", 0)) == 1:
                return
            last_error = f"unexpected overview: {overview}"
        except Exception as error:  # Dashboard may still be starting.
            last_error = str(error)
        time.sleep(2)
    raise TimeoutError(f"Flink dashboard did not become ready: {last_error}")


def write_page_snapshot(page: Page, target: Path) -> None:
    target.write_text(page.content(), encoding="utf-8")


def required_source_commit() -> str:
    source_commit = os.getenv("SOURCE_COMMIT_SHA", "")
    if re.fullmatch(r"[0-9a-f]{40}", source_commit) is None:
        raise ValueError(
            "SOURCE_COMMIT_SHA must be the full lowercase 40-hex Git commit"
        )
    return source_commit


def main() -> None:
    args = arguments()
    source_commit = required_source_commit()
    jar = args.jar.resolve(strict=True)
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    base_url = args.base_url.rstrip("/")
    wait_for_dashboard(base_url)

    audit: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_commit": source_commit,
        "deployment_method": "Flink Web Dashboard controlled by Playwright in Docker",
        "jar": jar.name,
        "parallelism": 3,
        "program_arguments": PROGRAM_ARGUMENTS,
        "dashboard_url": f"{base_url}/#/submit",
    }
    console_messages: list[str] = []
    failed_requests: list[dict[str, str]] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1600, "height": 1000})
        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        page = context.new_page()
        page.set_default_timeout(60_000)
        page.on(
            "console",
            lambda message: console_messages.append(
                f"{message.type}: {message.text}"
            ),
        )
        page.on(
            "requestfailed",
            lambda request: failed_requests.append(
                {
                    "method": request.method,
                    "url": request.url,
                    "failure": request.failure or "unknown failure",
                }
            ),
        )

        try:
            page.goto(f"{base_url}/#/submit", wait_until="networkidle")
            page.get_by_text("Uploaded Jars", exact=True).wait_for()
            page.screenshot(
                path=evidence / "flink-submit-before-upload.png", full_page=True
            )

            upload = page.locator("input#upload-file")
            upload.wait_for(state="attached")
            upload.set_input_files(str(jar))

            jar_row = page.locator("tr.clickable", has_text=jar.name)
            jar_row.wait_for(state="visible")
            page.screenshot(
                path=evidence / "flink-dashboard-jar-uploaded.png", full_page=True
            )
            jar_row.click()

            entry_class = page.locator('input[formcontrolname="entryClass"]')
            parallelism = page.locator('input[formcontrolname="parallelism"]')
            program_arguments = page.locator('input[formcontrolname="programArgs"]')
            entry_class.wait_for(state="visible")
            if not entry_class.input_value().strip():
                entry_class.fill("com.coursework.TrafficWindowJob")
            parallelism.fill("3")
            program_arguments.fill(PROGRAM_ARGUMENTS)
            audit["entry_class"] = entry_class.input_value()
            page.screenshot(
                path=evidence / "flink-dashboard-submit-configured.png",
                full_page=True,
            )

            run_response: dict[str, Any] = {}

            def record_run_response(response: Any) -> None:
                if response.request.method == "POST" and re.search(
                    r"/jars/[^/]+/run(?:\?|$)", response.url
                ):
                    run_response.update(
                        {
                            "url": response.url,
                            "status": response.status,
                        }
                    )

            page.on("response", record_run_response)
            page.get_by_role("button", name="Submit", exact=True).click()
            page.wait_for_url(re.compile(r"/#/job/running/[0-9a-f]{32}"))
            match = re.search(r"/job/running/([0-9a-f]{32})", page.url)
            if not match:
                raise RuntimeError(f"Could not extract a Flink job ID from {page.url}")
            job_id = match.group(1)
            if run_response.get("status") != 200:
                raise RuntimeError(
                    "Dashboard JAR run request did not return HTTP 200: "
                    f"{run_response}"
                )
            audit.update(
                {
                    "job_id": job_id,
                    "post_submit_url": page.url,
                    "run_request": run_response,
                }
            )

            deadline = time.monotonic() + 120
            last_state = "UNKNOWN"
            while time.monotonic() < deadline:
                jobs = read_json(f"{base_url}/jobs/overview").get("jobs", [])
                matching = [job for job in jobs if job.get("jid") == job_id]
                if matching:
                    last_state = str(matching[0].get("state", "UNKNOWN"))
                    if last_state == "RUNNING":
                        audit["job_overview"] = matching[0]
                        break
                    if last_state in {"FAILED", "CANCELED", "FINISHED"}:
                        raise RuntimeError(
                            f"Submitted Flink job entered terminal state {last_state}"
                        )
                time.sleep(2)
            else:
                raise TimeoutError(
                    f"Flink job {job_id} did not reach RUNNING; last state={last_state}"
                )

            page.goto(f"{base_url}/#/job/running/{job_id}/overview")
            page.get_by_text(JOB_NAME, exact=True).first.wait_for()
            page.screenshot(
                path=evidence / "flink-job-running-overview.png", full_page=True
            )
            write_page_snapshot(page, evidence / "flink-job-running-overview.html")
        finally:
            audit["console_messages"] = console_messages
            audit["failed_browser_requests"] = failed_requests
            (evidence / "flink-dashboard-audit.json").write_text(
                json.dumps(audit, indent=2) + "\n", encoding="utf-8"
            )
            context.tracing.stop(path=evidence / "flink-dashboard-trace.zip")
            context.close()
            browser.close()

    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
