from datetime import datetime, timedelta
import email
from email.header import decode_header, make_header
import getpass
import imaplib
import os
import re
import time
from bs4 import BeautifulSoup

# ============================================================
# CONFIG
# ============================================================

GMAIL_USER = "bobaobao2013@gmail.com"
APP_PASS = os.getenv("rhxxnrvmwnrobuko")

IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993
MAILBOX = "INBOX"

CHECK_INTERVAL = 10

# ============================================================
# START TIME
# ============================================================

START_TIME = datetime.now().astimezone()
START_TIMESTAMP = START_TIME.timestamp()

print("=" * 60)
print("GMAIL MONITOR")
print("=" * 60)
print(f"⏱️ Bắt đầu: {START_TIME:%d/%m/%Y %H:%M:%S %Z}")
print("=" * 60)


# ============================================================
# DECODE
# ============================================================

def decode_str(value):
    if not value:
        return ""

    try:
        return str(make_header(decode_header(value)))
    except Exception:
        return str(value)


def decode_payload(part):
    payload = part.get_payload(decode=True)

    if payload is None:
        return ""

    charset = part.get_content_charset() or "utf-8"

    try:
        return payload.decode(charset, errors="replace")
    except (LookupError, UnicodeError):
        return payload.decode("utf-8", errors="replace")


# ============================================================
# EXTRACT BODY + LINKS
# ============================================================

def extract_links_and_clean_body(msg):

    plain_parts = []
    html_parts = []
    links = []

    def add_link(link):
        if not link:
            return

        link = link.strip().rstrip(".,;:!?)\"]}'")

        if link and link not in links:
            links.append(link)

    for part in msg.walk():

        if part.is_multipart():
            continue

        if part.get_content_disposition() == "attachment":
            continue

        content_type = part.get_content_type()

        if content_type not in ("text/plain", "text/html"):
            continue

        content = decode_payload(part)

        if not content:
            continue

        if content_type == "text/plain":
            plain_parts.append(content)

        elif content_type == "text/html":
            html_parts.append(content)

    html_text_parts = []

    for html_content in html_parts:

        soup = BeautifulSoup(html_content, "html.parser")

        for a in soup.find_all("a", href=True):
            add_link(a["href"])

        html_text_parts.append(
            soup.get_text(separator="\n", strip=True)
        )

    if plain_parts:
        body = "\n".join(plain_parts)
    else:
        body = "\n".join(html_text_parts)

    found_urls = re.findall(
        r"https?://[^\s<>\"']+|www\.[^\s<>\"']+",
        body
    )

    for url in found_urls:
        add_link(url)

    clean_body = "\n".join(
        line.strip()
        for line in body.splitlines()
        if line.strip()
    )

    return clean_body, links


# ============================================================
# GMAIL INTERNAL DATE
# ============================================================

def get_internal_date(fetch_data):

    for item in fetch_data:

        if not isinstance(item, tuple):
            continue

        header = item[0]

        if not isinstance(header, bytes):
            continue

        try:
            date_tuple = imaplib.Internaldate2tuple(header)

            if date_tuple:
                timestamp = time.mktime(date_tuple)

                return datetime.fromtimestamp(
                    timestamp
                ).astimezone()

        except Exception:
            continue

    return None


# ============================================================
# PROCESS EMAIL
# ============================================================

def process_email(mail, uid):

    status, data = mail.uid(
        "fetch",
        str(uid),
        "(INTERNALDATE RFC822)"
    )

    if status != "OK":
        raise RuntimeError(f"Fetch UID {uid} thất bại")

    received_time = get_internal_date(data)

    if received_time is None:
        raise RuntimeError(
            f"Không đọc được INTERNALDATE UID {uid}"
        )

    received_timestamp = received_time.timestamp()

    # Chỉ nhận email sau thời điểm tool bắt đầu
    if received_timestamp <= START_TIMESTAMP:

        print(
            f"⏭️ Bỏ email cũ | UID {uid} | "
            f"{received_time:%d/%m/%Y %H:%M:%S}"
        )

        return

    raw_email = None

    for item in data:
        if isinstance(item, tuple):
            raw_email = item[1]
            break

    if raw_email is None:
        raise RuntimeError(
            f"Không lấy được email UID {uid}"
        )

    msg = email.message_from_bytes(raw_email)

    subject = decode_str(msg.get("Subject", ""))
    sender = decode_str(msg.get("From", ""))
    date_header = msg.get("Date", "")

    body_text, links = extract_links_and_clean_body(msg)

    print("\n" + "=" * 60)
    print("📩 EMAIL MỚI")
    print("=" * 60)

    print(f"🆔 UID: {uid}")
    print(f"⏱️ Gmail nhận: {received_time:%d/%m/%Y %H:%M:%S %Z}")
    print(f"📨 Date: {date_header}")
    print(f"👤 Người gửi: {sender}")
    print(f"📌 Tiêu đề: {subject}")

    print("-" * 60)
    print("📝 Nội dung:")
    print(body_text if body_text else "[Không có nội dung]")

    print("-" * 60)

    if links:
        print(f"🔗 Link ({len(links)}):")

        for i, link in enumerate(links, 1):
            print(f"{i}. {link}")
    else:
        print("🔗 Không tìm thấy link")

    print("=" * 60 + "\n")


# ============================================================
# MONITOR
# ============================================================

def monitor_inbox():

    app_pass = APP_PASS

    if not app_pass:
        app_pass = getpass.getpass(
            "🔐 Gmail App Password: "
        ).strip().replace(" ", "")

    if not app_pass:
        print("❌ App Password trống!")
        return

    mail = None
    last_seen_uid = 0
    reconnect_delay = 5

    search_date = (
        START_TIME - timedelta(days=1)
    ).strftime("%d-%b-%Y")

    try:

        while True:

            try:

                # ------------------------------------------------
                # CONNECT
                # ------------------------------------------------

                if mail is None:

                    print("📡 Đang kết nối Gmail...")

                    mail = imaplib.IMAP4_SSL(
                        IMAP_SERVER,
                        IMAP_PORT,
                        timeout=30
                    )

                    mail.login(GMAIL_USER, app_pass)

                    status, _ = mail.select(MAILBOX)

                    if status != "OK":
                        raise RuntimeError(
                            "Không thể mở mailbox"
                        )

                    print("✅ Kết nối thành công!")
                    print(
                        f"⏱️ Lọc mail sau: "
                        f"{START_TIME:%d/%m/%Y %H:%M:%S}"
                    )
                    print("👀 Đang theo dõi...\n")

                    reconnect_delay = 5

                # ------------------------------------------------
                # SEARCH
                # ------------------------------------------------

                status, _ = mail.select(MAILBOX)

                if status != "OK":
                    raise RuntimeError(
                        "Select mailbox thất bại"
                    )

                uid_range = f"{last_seen_uid + 1}:*"

                criteria = (
                    f'(SINCE "{search_date}" UID {uid_range})'
                )

                status, data = mail.uid(
                    "search",
                    None,
                    criteria
                )

                if status != "OK":
                    raise RuntimeError(
                        "UID SEARCH thất bại"
                    )

                # ------------------------------------------------
                # PROCESS
                # ------------------------------------------------

                if data and data[0]:

                    uid_list = sorted(
                        set(
                            int(uid)
                            for uid in data[0].split()
                        )
                    )

                    for uid in uid_list:

                        if uid <= last_seen_uid:
                            continue

                        # Nếu fetch lỗi, giữ UID để thử lại
                        process_email(mail, uid)

                        # Chỉ cập nhật sau khi xử lý xong
                        last_seen_uid = uid

                time.sleep(CHECK_INTERVAL)

            except KeyboardInterrupt:
                # Ctrl+C không được chuyển thành reconnect
                print("\n🛑 Đang dừng toàn bộ tool...")
                break

            except Exception as e:

                print(f"\n❌ Lỗi: {e}")

                if mail is not None:

                    try:
                        mail.logout()
                    except Exception:
                        pass

                    mail = None

                print(
                    f"🔄 Kết nối lại sau {reconnect_delay}s..."
                )

                time.sleep(reconnect_delay)

                reconnect_delay = min(
                    reconnect_delay * 2,
                    60
                )

    finally:

        # Luôn thực hiện khi thoát, kể cả Ctrl+C
        if mail is not None:

            try:
                mail.logout()
                print("🔌 Đã đóng kết nối IMAP.")
            except Exception:
                pass

        print("👋 Tool đã kết thúc hoàn toàn.")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    try:
        monitor_inbox()

    except KeyboardInterrupt:
        print("\n🛑 Ctrl+C — hủy toàn bộ chương trình.")

    finally:
        print("✅ Đã thoát.")
