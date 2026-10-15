from datetime import datetime
import email
from email.header import decode_header
import imaplib
import re
import time
from bs4 import BeautifulSoup

# ============================================================
# CẤU HÌNH TÀI KHOẢN CỦA BẠN
# ============================================================
GMAIL_USER = "bobaobao2000@gmail.com"
APP_PASS = "icqhtafrozhrvkat"  # App Password 16 ký tự

IMAP_SERVER = "imap.gmail.com"
MAILBOX = "INBOX"
CHECK_INTERVAL = 10  # Thời gian quét lại (giây)


def decode_str(s):
  if not s:
    return ""
  decoded, encoding = decode_header(s)[0]
  if isinstance(decoded, bytes):
    return decoded.decode(encoding or "utf-8", errors="ignore")
  return decoded


def extract_links_and_clean_body(msg):
  body = ""
  links = []

  if msg.is_multipart():
    for part in msg.walk():
      content_type = part.get_content_type()
      content_disposition = str(part.get("Content-Disposition"))
      if content_type == "text/plain" and "attachment" not in content_disposition:
        payload = part.get_payload(decode=True)
        if payload:
          charset = part.get_content_charset() or "utf-8"
          body = payload.decode(charset, errors="ignore")
          break
      elif (
          content_type == "text/html" and "attachment" not in content_disposition
      ):
        payload = part.get_payload(decode=True)
        if payload:
          charset = part.get_content_charset() or "utf-8"
          html_content = payload.decode(charset, errors="ignore")
          soup = BeautifulSoup(html_content, "html.parser")
          for a in soup.find_all("a", href=True):
            href = a["href"]
            if href and href not in links:
              links.append(href)
          body = soup.get_text()
          break
  else:
    payload = msg.get_payload(decode=True)
    if payload:
      charset = msg.get_content_charset() or "utf-8"
      content = payload.decode(charset, errors="ignore")
      if "<html" in content.lower() or "<body" in content.lower():
        soup = BeautifulSoup(content, "html.parser")
        for a in soup.find_all("a", href=True):
          href = a["href"]
          if href and href not in links:
            links.append(href)
        body = soup.get_text()
      else:
        body = content

  # Tìm thêm link dạng text thuần trong nội dung
  url_pattern = re.findall(r"https?://[^\s<>\"']+|www\.[^\s<>\"']+", body)
  for url in url_pattern:
    if url not in links:
      links.append(url)

  clean_body = "\n".join(
      [line.strip() for line in body.splitlines() if line.strip()]
  )
  return clean_body, links


def get_current_max_uid(mail):
  """Lấy UID lớn nhất hiện có trong hộp thư NGAY TẠI THỜI ĐIỂM BẬT TOOL.

  Mọi email đến sau khi bật tool chắc chắn sẽ có UID lớn hơn mốc này.
  """
  mail.select(MAILBOX)
  status, data = mail.search(None, "ALL")
  if status == "OK" and data[0]:
    msg_nums = data[0].split()
    if msg_nums:
      latest_num = msg_nums[-1]
      res, uid_data = mail.fetch(latest_num, "(UID)")
      if res == "OK":
        for part in uid_data:
          if isinstance(part, bytes):
            try:
              uid_str = part.decode("utf-8")
              if "UID" in uid_str:
                return int(uid_str.split("UID")[1].strip().split()[0])
            except Exception:
              pass
  return 0


def monitor_inbox():
  print(f"Đang kết nối tới {IMAP_SERVER}...")
  try:
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(GMAIL_USER, APP_PASS)
    mail.select(MAILBOX)

    # 1. Ghi nhận mốc UID tại thời điểm vừa bật tool
    last_seen_uid = get_current_max_uid(mail)
    print("✅ Kết nối thành công!")
    print(
        "🛡️ Đã thiết lập bộ lọc: TUYỆT ĐỐI BỎ QUA toàn bộ email cũ trước thời"
        " điểm này."
    )
    print("👀 Đang trực chiến lắng nghe email mới đến...\n")

    while True:
      mail.select(MAILBOX)

      # 2. Chỉ tìm kiếm các email có UID lớn hơn mốc khởi điểm
      # Lệnh UID SEARCH UNSEEN hoặc lọc qua UID > last_seen_uid giúp tối ưu hiệu năng tối đa
      status, messages = mail.uid("search", None, f"UID {last_seen_uid + 1}:*")

      if status == "OK" and messages[0]:
        uid_list = messages[0].split()
        for uid_bytes in uid_list:
          uid_int = int(uid_bytes.decode("utf-8"))

          # Đảm bảo chỉ xử lý các UID thực sự mới sinh ra sau mốc bật tool
          if uid_int > last_seen_uid:
            last_seen_uid = uid_int  # Cập nhật mốc mới nhất đã đọc

            res, msg_data = mail.uid("fetch", uid_bytes, "(RFC822)")
            if res != "OK":
              continue

            for response_part in msg_data:
              if isinstance(response_part, tuple):
                msg_obj = email.message_from_bytes(response_part[1])

                subject = decode_str(msg_obj["Subject"])
                sender = decode_str(msg_obj["From"])
                date_str = msg_obj.get("Date", "")

                body_text, links = extract_links_and_clean_body(msg_obj)

                print("=" * 60)
                print(" 📩 PHÁT HIỆN EMAIL MỚI (Sau khi bật tool)")
                print("=" * 60)
                print(f"🕒 Thời gian: {date_str}")
                print(f"👤 Người gửi: {sender}")
                print(f"📌 Tiêu đề:   {subject}")
                print("-" * 60)
                print("📝 Nội dung:")
                print(body_text if body_text else "[Không có nội dung]")
                print("-" * 60)

                if links:
                  print("🔗 Các đường dẫn được tách:")
                  for link in links:
                    print(f"   [{link}]")
                else:
                  print("🔗 Không tìm thấy đường dẫn nào.")
                print("=" * 60 + "\n")

      time.sleep(CHECK_INTERVAL)

  except Exception as e:
    print(f"❌ Lỗi kết nối hoặc xác thực: {e}")


if __name__ == "__main__":
  monitor_inbox()
  
