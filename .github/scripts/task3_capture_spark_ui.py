#!/usr/bin/env python3
"""Capture Task 3 Spark master and completed History Server UI evidence."""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from playwright.sync_api import Locator, Page, sync_playwright


APPLICATION_NAME = "WebBerkStanInDegreeAnalysis"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-url", default="http://127.0.0.1:8080")
    parser.add_argument("--history-url", default="http://127.0.0.1:18080")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    return parser.parse_args()


def read_json(url: str, timeout: float = 10) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def wait_for_master(master_url: str, timeout: float = 180) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = "master not queried"
    while time.monotonic() < deadline:
        try:
            status = read_json(f"{master_url}/json/")
            live_workers = [
                worker
                for worker in status.get("workers", [])
                if worker.get("state") == "ALIVE"
            ]
            profiles = sorted(
                (worker.get("cores"), worker.get("memory"))
                for worker in live_workers
            )
            if profiles == [(2, 2048), (2, 2048)]:
                return status
            last_error = f"worker profiles are {profiles}"
        except Exception as error:  # Master may still be starting.
            last_error = str(error)
        time.sleep(2)
    raise TimeoutError(f"Spark master was not ready: {last_error}")


def wait_for_history_application(
    history_url: str, timeout: float = 240
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = "History API not queried"
    while time.monotonic() < deadline:
        try:
            applications = read_json(f"{history_url}/api/v1/applications")
            matches = [
                application
                for application in applications
                if application.get("name") == APPLICATION_NAME
            ]
            if matches:
                application = matches[0]
                attempts = application.get("attempts", [])
                if attempts and attempts[0].get("completed") is True:
                    return application
                last_error = "application is present but not marked complete"
            else:
                last_error = f"{APPLICATION_NAME!r} is not listed"
        except Exception as error:  # History Server may be refreshing event logs.
            last_error = str(error)
        time.sleep(3)
    raise TimeoutError(f"Completed Spark history application not found: {last_error}")


def first_visible(locator: Locator) -> Locator | None:
    for index in range(locator.count()):
        candidate = locator.nth(index)
        if candidate.is_visible():
            return candidate
    return None


def screenshot(page: Page, target: Path) -> None:
    page.screenshot(path=target, full_page=True)


def main() -> None:
    args = arguments()
    master_url = args.master_url.rstrip("/")
    history_url = args.history_url.rstrip("/")
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)

    master_status = wait_for_master(master_url)
    history_application = wait_for_history_application(history_url)
    (evidence / "spark-master-status.json").write_text(
        json.dumps(master_status, indent=2), encoding="utf-8"
    )
    (evidence / "spark-history-application.json").write_text(
        json.dumps(history_application, indent=2), encoding="utf-8"
    )

    audit: dict[str, Any] = {
        "application_name": APPLICATION_NAME,
        "application_id": history_application.get("id"),
        "master_url": master_url,
        "history_url": history_url,
        "captured_pages": [],
    }
    console_messages: list[str] = []

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

        try:
            page.goto(master_url, wait_until="networkidle")
            page.get_by_text("Workers (2)", exact=False).first.wait_for()
            screenshot(page, evidence / "spark-master-two-workers.png")
            audit["captured_pages"].append(page.url)

            page.goto(history_url, wait_until="networkidle")
            application_id = history_application.get("id")
            if not isinstance(application_id, str) or not application_id:
                raise RuntimeError("History API application has no valid id")
            application_row = page.locator("#history-summary tbody tr").filter(
                has_text=APPLICATION_NAME
            ).first
            application_row.wait_for(state="visible")
            application_link = application_row.get_by_role(
                "link", name=application_id, exact=True
            ).first
            application_link.wait_for()
            app_href = application_link.get_attribute("href")
            if not app_href:
                raise RuntimeError("History Server application link has no href")
            app_url = urljoin(f"{history_url}/", app_href)
            screenshot(page, evidence / "spark-history-applications.png")
            audit["captured_pages"].append(page.url)

            page.goto(app_url, wait_until="networkidle")
            page.get_by_text(APPLICATION_NAME, exact=False).first.wait_for()
            screenshot(page, evidence / "spark-history-application-overview.png")
            audit["captured_pages"].append(page.url)

            jobs_link = first_visible(page.get_by_role("link", name="Jobs", exact=True))
            if jobs_link is None:
                raise RuntimeError("Spark History application has no visible Jobs link")
            jobs_link.click()
            page.wait_for_load_state("networkidle")
            screenshot(page, evidence / "spark-history-jobs.png")
            audit["captured_pages"].append(page.url)

            job_detail = first_visible(page.locator('a[href*="job/?id="]'))
            if job_detail is not None:
                job_detail.click()
                page.wait_for_load_state("networkidle")
                dag_toggle = first_visible(page.get_by_text("DAG Visualization", exact=False))
                if dag_toggle is not None:
                    try:
                        dag_toggle.click()
                        page.wait_for_timeout(750)
                    except Exception:
                        # Some Spark themes render the DAG expanded and make its heading inert.
                        pass
                screenshot(page, evidence / "spark-history-job-dag.png")
                audit["captured_pages"].append(page.url)

            page.goto(app_url, wait_until="networkidle")
            stages_link = first_visible(
                page.get_by_role("link", name="Stages", exact=True)
            )
            if stages_link is None:
                raise RuntimeError("Spark History application has no visible Stages link")
            stages_link.click()
            page.wait_for_load_state("networkidle")
            screenshot(page, evidence / "spark-history-stages.png")
            audit["captured_pages"].append(page.url)

            stage_detail = first_visible(page.locator('a[href*="stage/?id="]'))
            if stage_detail is not None:
                stage_detail.click()
                page.wait_for_load_state("networkidle")
                screenshot(page, evidence / "spark-history-stage-metrics.png")
                audit["captured_pages"].append(page.url)

            (evidence / "spark-history-last-page.html").write_text(
                page.content(), encoding="utf-8"
            )
        finally:
            audit["console_messages"] = console_messages
            (evidence / "spark-ui-capture-audit.json").write_text(
                json.dumps(audit, indent=2), encoding="utf-8"
            )
            context.tracing.stop(path=evidence / "spark-ui-trace.zip")
            context.close()
            browser.close()

    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
