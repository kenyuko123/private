#!/usr/bin/env python3

import json
import time
import urllib.request
import urllib.error

FIREBASE = "https://realtime-database-bee52-default-rtdb.asia-southeast1.firebasedatabase.app"
TOKEN = "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"
REPO = "kenyuko123/rdp"
WORKFLOW = "main.yml"
BRANCH = "main"

POLL = 2
RETRY = 3


def http(url, method="GET", headers=None, data=None):
    body = None

    if data is not None:
        body = json.dumps(data).encode()
        headers = {
            **(headers or {}),
            "Content-Type": "application/json"
        }

    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers=headers or {}
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode("utf-8", "replace")
            try:
                raw = json.loads(raw) if raw else None
            except Exception:
                pass
            return r.status, raw

    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {e.code}: {raw}")

    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error: {e.reason}")


def fb(method="GET", data=None):
    return http(
        FIREBASE + "/rdp/command.json",
        method,
        {"Accept": "application/json"},
        data
    )


def gh(path, method="GET", data=None):
    if not TOKEN or TOKEN == "YOUR_NEW_TOKEN":
        raise RuntimeError("Chưa nhập GitHub token mới")

    return http(
        "https://api.github.com/" + path,
        method,
        {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {TOKEN}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "firebase-rdp-worker"
        },
        data
    )


def dispatch():
    path = (
        f"repos/{REPO}/actions/workflows/"
        f"{WORKFLOW}/dispatches"
    )

    last = None

    for i in range(1, RETRY + 1):
        try:
            status, _ = gh(
                path,
                "POST",
                {"ref": BRANCH}
            )

            if 200 <= status < 300:
                print(f"[OK] main.yml dispatch HTTP {status}")
                return True

            raise RuntimeError(
                f"GitHub dispatch HTTP {status}"
            )

        except Exception as e:
            last = e
            print(f"[!] Dispatch lỗi ({i}/{RETRY}): {e}")

            if i < RETRY:
                time.sleep(2)

    raise RuntimeError(last)


def process(cmd, done):
    if not isinstance(cmd, dict):
        print("[!] Command lỗi -> xoá")
        fb("DELETE")
        return

    action = str(cmd.get("action", "")).lower().strip()
    rid = str(cmd.get("request_id", "unknown"))

    print(f"\n[*] {action} | request_id={rid}")

    if action != "create":
        print("[!] Action không hợp lệ -> xoá")
        fb("DELETE")
        return

    # Đã dispatch trước đó nhưng DELETE Firebase thất bại
    if rid in done:
        try:
            fb("DELETE")
            print("[OK] Đã xoá command còn sót")
        except Exception as e:
            print("[!] DELETE:", e)
        return

    # --------------------------------------------------------
    # QUAN TRỌNG:
    # DISPATCH TRƯỚC
    # DELETE SAU
    # --------------------------------------------------------

    try:
        dispatch()
    except Exception as e:
        print("[ERROR] Không dispatch được:", e)
        print("[*] Giữ nguyên Firebase để thử lại")
        return

    # Đánh dấu ngay sau khi GitHub chấp nhận dispatch
    done.add(rid)

    try:
        fb("DELETE")
        print("[OK] Dispatch thành công -> đã xoá Firebase")
    except Exception as e:
        print("[!] Dispatch OK nhưng DELETE Firebase lỗi:", e)
        print("[*] Không dispatch lại request này")


def main():
    print("=" * 45)
    print("   RDP FIREBASE → GITHUB ACTIONS WORKER")
    print("=" * 45)
    print(f"Repo: {REPO}")
    print(f"Workflow: {WORKFLOW}")
    print(f"Branch: {BRANCH}")
    print("[*] Waiting for /rdp/command ...")
    print()

    done = set()
    last_error = None

    while True:
        try:
            _, cmd = fb()

            if cmd:
                process(cmd, done)
                last_error = None

        except KeyboardInterrupt:
            print("\n[OK] Stopped")
            break

        except Exception as e:
            msg = str(e)

            if msg != last_error:
                print("[ERROR]", msg)
                last_error = msg

        time.sleep(POLL)


if __name__ == "__main__":
    main()
