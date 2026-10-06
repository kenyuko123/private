#!/usr/bin/env python3
# ============================================================
# GitHub Actions launcher
# Chỉ cần điền TOKEN bên dưới rồi chạy:
#     python main.py
#
# Tool sẽ:
# 1. Kiểm tra token.
# 2. Tự lấy tài khoản GitHub của token.
# 3. Dùng repo "rdp" của tài khoản đó.
# 4. Chạy .github/workflows/main.yml.
# 5. Chờ đến khi GitHub xác nhận workflow đã QUEUED/RUNNING.
# 6. Thấy workflow bắt đầu chạy => báo COMPLETE và thoát.
#
# Không chờ workflow kết thúc 90 phút.
# ============================================================

import json
import sys
import time
import urllib.error
import urllib.request

# ===================== ĐIỀN TOKEN Ở ĐÂY =====================
TOKEN = "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"

# Repo mặc định: kenyuko123/rdp
REPO_NAME = "rdp"

# Workflow cố định
WORKFLOW_FILE = "main.yml"

# Thời gian tối đa chờ GitHub nhận workflow
START_TIMEOUT = 30

API = "https://api.github.com"


def request(url, token, method="GET", payload=None):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "rdp-launcher/1.0",
    }

    data = None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers=headers,
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            raw = response.read().decode("utf-8", errors="replace")
            return response.status, (json.loads(raw) if raw else None)

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            msg = json.loads(raw).get("message", raw)
        except Exception:
            msg = raw

        raise RuntimeError(f"GitHub HTTP {e.code}: {msg}") from e

    except urllib.error.URLError as e:
        raise RuntimeError(f"Lỗi mạng: {e.reason}") from e


def main():
    token = TOKEN.strip()

    if not token or token == "PASTE_GITHUB_TOKEN_HERE":
        print("ERROR: Hãy điền GitHub token vào biến TOKEN ở đầu file main.py.")
        return 1

    print("[*] Kiểm tra GitHub token...")

    try:
        _, me = request(f"{API}/user", token)
        username = me["login"]

        full_repo = f"{username}/{REPO_NAME}"
        repo_api = f"{API}/repos/{full_repo}"

        print(f"[*] Repo: {full_repo}")

        _, repo = request(repo_api, token)
        branch = repo.get("default_branch")

        if not branch:
            raise RuntimeError("Không lấy được default branch.")

        workflow_api = (
            f"{repo_api}/actions/workflows/{WORKFLOW_FILE}"
        )

        _, workflow = request(workflow_api, token)

        if workflow.get("state") != "active":
            raise RuntimeError(
                f"{WORKFLOW_FILE} không active "
                f"(state={workflow.get('state')})"
            )

        # Lấy các run hiện tại trước khi dispatch để phân biệt
        # run mới với run cũ.
        runs_api = (
            f"{repo_api}/actions/workflows/"
            f"{WORKFLOW_FILE}/runs?per_page=10"
        )
        _, before = request(runs_api, token)

        old_ids = {
            run["id"]
            for run in (before.get("workflow_runs") or [])
        }

        print(f"[*] Branch: {branch}")
        print(f"[*] Workflow: {WORKFLOW_FILE}")
        print("[*] Đang chạy workflow...")

        dispatch_api = f"{workflow_api}/dispatches"

        request(
            dispatch_api,
            token,
            method="POST",
            payload={"ref": branch},
        )

        # GitHub API dispatch thường trả 204 và không trả run_id.
        # Vì vậy dò run mới cho tới khi thấy queued/in_progress.
        deadline = time.time() + START_TIMEOUT

        while time.time() < deadline:
            time.sleep(1)

            _, result = request(runs_api, token)
            runs = result.get("workflow_runs") or []

            for run in runs:
                if run.get("id") not in old_ids:
                    status = run.get("status")
                    run_id = run.get("id")

                    if status in ("queued", "in_progress"):
                        print()
                        print("==========================================")
                        print("       WORKFLOW STARTED")
                        print("==========================================")
                        print(f"REPO    : {full_repo}")
                        print(f"WORKFLOW: {WORKFLOW_FILE}")
                        print(f"JOB     : rdp")
                        print(f"RUN ID  : {run_id}")
                        print(f"STATUS  : {status}")
                        print()
                        print("COMPLETE: GitHub đã bắt đầu chạy main.yml.")
                        print(f"URL: {run.get('html_url', '')}")
                        print("==========================================")
                        return 0

        # Trường hợp GitHub đã nhận dispatch nhưng API chưa kịp hiện run.
        print()
        print("==========================================")
        print("       WORKFLOW DISPATCHED")
        print("==========================================")
        print(f"REPO    : {full_repo}")
        print(f"WORKFLOW: {WORKFLOW_FILE}")
        print("COMPLETE: Đã gửi lệnh chạy main.yml.")
        print(f"Actions : https://github.com/{full_repo}/actions")
        print("==========================================")
        return 0

    except (RuntimeError, KeyError) as e:
        print()
        print(f"[ERROR] {e}")
        print()
        print("Các quyền token cần thiết thường gồm:")
        print("- Actions: write")
        print("- Contents: read")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
    
