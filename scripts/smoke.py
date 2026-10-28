#!/usr/bin/env python3
"""Exercise an already running isolated docker-compose.test.yml stack.

Only analytics-service in the explicitly named test project is stopped/restarted.
No database volumes, user data, or external HTTP APIs are accessed.
"""

import argparse
import concurrent.futures
import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="isolated test Compose project")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    compose = ["docker", "compose", "-p", args.project, "-f", str(root / "docker-compose.test.yml")]
    base = f"http://127.0.0.1:{args.port}"
    opener = urllib.request.build_opener(NoRedirect)

    def request(path, data=None):
        payload = json.dumps(data).encode() if data is not None else None
        req = urllib.request.Request(base + path, data=payload, headers={"Content-Type": "application/json"})
        try:
            response = opener.open(req, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers, response.read()

    def compose_command(*command):
        return subprocess.run(compose + list(command), cwd=root, check=True, capture_output=True, text=True, timeout=60).stdout

    def eventually(check, description):
        deadline = time.monotonic() + 30
        last = None
        while time.monotonic() < deadline:
            try:
                last = check()
                if last:
                    return last
            except (OSError, ValueError) as error:
                last = error
            time.sleep(0.2)
        raise AssertionError(f"Timed out waiting for {description}: {last}")

    def stats(expected):
        status, _, body = request(f"/links/{code}/stats")
        return status == 200 and json.loads(body)["total_clicks"] == expected

    status, _, body = request("/links", {"url": "https://example.org/smoke-test"})
    assert status == 201, (status, body)
    code = json.loads(body)["code"]
    assert re.fullmatch(r"[A-Za-z0-9]{6}", code), code
    status, headers, _ = request(f"/{code}")
    assert status == 302 and headers["Location"] == "https://example.org/smoke-test", (status, headers)
    eventually(lambda: stats(1), "initial analytics delivery")

    def pending():
        sql = f"SELECT count(*) FROM click_outbox o JOIN links l ON l.id=o.link_id WHERE l.code='{code}'"
        return int(compose_command("exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "linkpulse_test", "-Atc", sql).strip())

    # This file is fixed to the test Compose, whose databases use tmpfs.
    compose_command("stop", "analytics-service")
    count = 8
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as executor:
            results = list(executor.map(lambda _: request(f"/{code}")[0], range(count)))
        assert results == [302] * count, results
        assert request("/health")[0] == 200
        assert request("/ready")[0] == 200
        assert request(f"/links/{code}/stats")[0] == 503
        assert pending() == count, "redirects must remain durable during outage"
    finally:
        compose_command("start", "analytics-service")

    eventually(lambda: stats(count + 1), "analytics recovery with exact count")
    eventually(lambda: pending() == 0, "outbox drain")
    print(json.dumps({"project": args.project, "code": code, "initial_clicks": 1,
                      "queued_during_outage": count, "total_after_recovery": count + 1,
                      "pending_after_recovery": 0, "result": "passed"}))


if __name__ == "__main__":
    main()
