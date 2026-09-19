"""Aff Forwarder Webhook — extension ĐỘC LẬP, không đụng vào code Nexus.

Chạy thành server riêng (mặc định http://127.0.0.1:5001), dùng Nexus.exe làm
server API (mặc định http://127.0.0.1:5000):

- ``GET  /api/accounts``                → danh sách tài khoản Zalo.
- ``GET  /api/groups/personal``         → danh sách nhóm của tài khoản.
- ``GET  /api/messages/group-since``    → lấy TẤT CẢ tin nhóm mới kể từ mốc msgId.
- ``POST /api/send-group-message``      → gửi text (+ ảnh) vào nhóm kết quả.

Logic: theo chu kỳ cấu hình, kiểm tra từng NHÓM NGUỒN; tin mới có link
Shopee/Lazada thì chuyển thành link affiliate (Shopee bằng aff id, Lazada bằng
cookie dán trên dashboard) rồi gửi NGUYÊN nội dung + ảnh (chỉ thay link) vào
NHÓM KẾT QUẢ. Lần kiểm tra đầu tiên của một nhóm chỉ ghi mốc (baseline) —
không chuyển tiếp tin cũ.

Chạy:  python webhook/aff_webhook.py  (tùy chọn: --port 5001)
"""

import json
import os
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from link_converter import (  # noqa: E402
    BROWSER_UA,
    NO_PROXY,
    convert_product_link,
    extract_product_links,
    normalize_lazada_cookie,
)

DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "aff_webhook.json")

# Địa chỉ Nexus fix cứng (đổi được qua biến môi trường NEXUS_URL nếu cần).
NEXUS_URL = os.environ.get("NEXUS_URL", "http://127.0.0.1:5000").rstrip("/")

_lock = threading.RLock()

_MAX_LOGS = 200
# Tin gửi lỗi làm MỐC dừng lại ở tin đó (lượt sau thử lại đúng từ đó, không bỏ
# sót tin ở giữa); thử quá _MAX_RETRY lần vẫn lỗi thì bỏ qua để không nghẽn.
_MAX_RETRY = 3
# Tin CÓ ẢNH nhưng URL ảnh CHƯA SẴN SÀNG (ảnh nặng, CDN chưa xử lý xong): CHỜ và
# thử lại nhiều lần hơn (tới ~15 lượt ≈ 15 phút nếu chu kỳ 60s) để không gửi
# thiếu ảnh; quá hạn này mới đành gửi TEXT (không để mất tin).
_IMG_MAX_RETRY = 15
# Nhóm mới thêm: vẫn chuyển tiếp tin đăng trong khoảng này (mặc định 30 phút);
# tin cũ hơn coi là lịch sử -> chỉ ghi mốc, không chuyển.
_BASELINE_RECENT_MS = 30 * 60 * 1000
# Số msgId nguồn tối đa giữ trong bản đồ quote (msgId nguồn -> tin đích) để nhóm
# đích trả lời đúng tin tương ứng; vượt thì bỏ các mốc cũ nhất.
_FORWARD_MAP_MAX = 1000
DEFAULT_SHOPEE_AFF_ID = "17340820046"
# subId CỐ ĐỊNH gắn vào link Shopee (&sub_id=) và Lazada (subId1=) để biết
# click/đơn nào đến qua hệ thống này khi đối soát ở webhook affiliate.
DEFAULT_SUB_ID = "nexus"
# Lazada Affiliate Open API (LiteApp) — thay cho cookie.
DEFAULT_LAZADA_APP_KEY = "105827"
DEFAULT_LAZADA_APP_SECRET = "r8ZMKhPxu1JZUCwTUBVMJiJnZKjhWeQF"
DEFAULT_LAZADA_USER_TOKEN = "d29e97bd88054544b021d23b430fe751"

# Nhãn loại tin Nexus thêm vào đầu nội dung ("[Hình ảnh] ...") — bỏ khi
# chuyển tiếp để giữ nguyên nội dung gốc.
_CONTENT_LABELS = ("[Hình ảnh]", "[Ảnh GIF]", "[Video]")

app = Flask(__name__, template_folder=os.path.join(BASE_DIR, "templates"))


# ─── Lưu trữ cấu hình + trạng thái + log ─────────────────────────────────────

def _default_settings() -> dict:
    return {
        "enabled": False,
        # profileId (uid Zalo — ổn định) của tài khoản thực hiện; webhook chỉ
        # truyền cái này, Nexus tự tra cookies/zpwEnk/imei. account_id giữ lại
        # để tương thích cấu hình cũ.
        "profile_id": "",
        "account_id": "",
        # Mỗi luồng: 1 NHÓM KẾT QUẢ nhận tin từ NHIỀU nhóm nguồn.
        # [{"id", "dest_group_id", "source_group_ids": [...]}]
        "routes": [],
        # Chu kỳ kiểm tra tính bằng GIÂY. Mặc định 60s: mỗi 60s quét lại nhóm
        # nguồn, thấy tin mới thì chuyển đổi link và gửi nhóm đích.
        "interval_seconds": 60,
        # Độ trễ giữa các lần gửi tin (giây) — tránh gửi dồn dập bị Zalo chặn.
        "send_delay_seconds": 3,
        "shopee_aff_id": DEFAULT_SHOPEE_AFF_ID,
        # Shopee Affiliate Open API (generateShortLink) — CÁCH DUY NHẤT Shopee thống
        # kê SubID. Thiếu 2 trường này thì fallback link an_redir (không đối soát SubID).
        "shopee_app_id": "",
        "shopee_app_secret": "",
        # subId cố định (đối soát click/đơn qua hệ thống) — dùng cho cả Shopee & Lazada.
        "sub_id": DEFAULT_SUB_ID,
        # Lazada Open API (LiteApp) — ưu tiên; cookie chỉ là fallback.
        "lazada_app_key": DEFAULT_LAZADA_APP_KEY,
        "lazada_app_secret": DEFAULT_LAZADA_APP_SECRET,
        "lazada_user_token": DEFAULT_LAZADA_USER_TOKEN,
        "lazada_cookie": "",
    }


def _sanitize_routes(raw) -> list:
    """Chuẩn hóa danh sách luồng: bỏ nguồn trùng nhóm kết quả, khử trùng lặp."""
    routes = []
    for i, r in enumerate(raw if isinstance(raw, list) else []):
        if not isinstance(r, dict):
            continue
        dest = str(r.get("dest_group_id") or "").strip()
        sources = sorted({
            str(x or "").strip() for x in (r.get("source_group_ids") or [])
            if str(x or "").strip() and str(x or "").strip() != dest
        })
        routes.append({
            "id": str(r.get("id") or f"r{i + 1}").strip(),
            "dest_group_id": dest,
            "source_group_ids": sources,
        })
    return routes


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_db() -> dict:
    db = {}
    if os.path.exists(DB_PATH):
        try:
            with open(DB_PATH, "r", encoding="utf-8") as f:
                db = json.load(f)
        except Exception:
            db = {}
    if not isinstance(db, dict):
        db = {}
    settings = db.get("settings") if isinstance(db.get("settings"), dict) else {}
    # Migration bản cũ (1 cặp nguồn/kết quả toàn cục) → 1 luồng.
    if settings.get("dest_group_id") and not settings.get("routes"):
        settings["routes"] = [{
            "id": "r1",
            "dest_group_id": settings.get("dest_group_id"),
            "source_group_ids": settings.get("source_group_ids") or [],
        }]
    # Migration chu kỳ bản cũ (phút) → giây.
    if "interval_seconds" not in settings and settings.get("interval_minutes"):
        try:
            settings["interval_seconds"] = max(1, int(settings["interval_minutes"]) * 60)
        except Exception:
            settings["interval_seconds"] = 1
    merged = _default_settings()
    merged.update({k: v for k, v in settings.items() if k in merged})
    merged["routes"] = _sanitize_routes(merged.get("routes"))
    db["settings"] = merged
    db.setdefault("state", {})
    db["state"].setdefault("lastMsgByGroup", {})
    db.setdefault("logs", [])
    db.setdefault("stats", {})
    db["stats"].setdefault("forwarded", 0)
    db["stats"].setdefault("errors", 0)
    db["stats"].setdefault("lastRunAt", "")
    db["stats"].setdefault("lastRunMessage", "Chưa chạy lần nào")
    return db


def _save_db(db: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = DB_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, DB_PATH)


def _append_log(db: dict, entry: dict) -> None:
    entry["at"] = _now_str()
    db["logs"].insert(0, entry)
    del db["logs"][_MAX_LOGS:]


# ─── Gọi API Nexus ───────────────────────────────────────────────────────────

class NexusError(RuntimeError):
    pass


def _nexus_base() -> str:
    return NEXUS_URL


def _check_nexus_payload(response) -> dict:
    try:
        payload = response.json()
    except Exception:
        raise NexusError(f"Nexus trả về không phải JSON (HTTP {response.status_code}).")
    if isinstance(payload, dict) and payload.get("policy_required"):
        raise NexusError("Nexus yêu cầu đồng ý chính sách: mở giao diện Nexus, "
                         "tick đồng ý ở trang /policy rồi chạy lại.")
    return payload


def nexus_get(path: str, params: dict = None, timeout: int = 60) -> dict:
    try:
        response = requests.get(_nexus_base() + path, params=params or {},
                                timeout=timeout, proxies=NO_PROXY)
    except Exception as exc:
        raise NexusError(f"Không kết nối được Nexus ({_nexus_base()}): {exc}. "
                         "Hãy chắc chắn Nexus.exe đang chạy.")
    return _check_nexus_payload(response)


def nexus_post_form(path: str, data: dict, files: dict = None, timeout: int = 120) -> dict:
    try:
        response = requests.post(_nexus_base() + path, data=data, files=files,
                                 timeout=timeout, proxies=NO_PROXY)
    except Exception as exc:
        raise NexusError(f"Không kết nối được Nexus ({_nexus_base()}): {exc}. "
                         "Hãy chắc chắn Nexus.exe đang chạy.")
    return _check_nexus_payload(response)


# ─── Logic chuyển tiếp ───────────────────────────────────────────────────────

def _strip_content_label(text: str) -> str:
    text = str(text or "").strip()
    for label in _CONTENT_LABELS:
        if text.startswith(label):
            return text[len(label):].strip()
    return text


def _download_image(url: str, timeout: int = 30, retries: int = 2) -> bytes:
    """Tải ảnh từ CDN Zalo. Thử lại vài lần (ảnh nặng / CDN chậm) trước khi bỏ.

    Vòng ngoài đã chờ nhiều lượt (_IMG_MAX_RETRY) nên ở đây chỉ thử nhanh 2 lần
    để không làm 1 lượt quét treo quá lâu khi ảnh chưa sẵn."""
    url = str(url or "").strip()
    if not url:
        return b""
    headers = {"User-Agent": BROWSER_UA, "Referer": "https://chat.zalo.me/"}
    for attempt in range(max(1, retries)):
        try:
            response = requests.get(url, headers=headers, timeout=timeout, proxies=NO_PROXY)
            if response.status_code == 200 and response.content:
                return response.content
            print(f"[aff_webhook] Ảnh HTTP {response.status_code} (lần {attempt + 1})", flush=True)
        except Exception as exc:
            print(f"[aff_webhook] Không tải được ảnh (lần {attempt + 1}): {exc}", flush=True)
        if attempt < retries - 1:
            time.sleep(2)
    return b""


def _send_to_dest(settings: dict, dest_group_id: str, text: str, thumb: str,
                  force_text: bool = False, quote: dict = None) -> dict:
    """Gửi nội dung (kèm ảnh nếu tải được) vào nhóm kết quả qua API Nexus.

    Tin CÓ ẢNH (``thumb``) mà CHƯA tải được URL ảnh: mặc định KHÔNG gửi thiếu ảnh
    mà trả ``imageNotReady`` để phía trên CHỜ và thử lại (đợi tới khi có URL ảnh).
    Chỉ khi ``force_text=True`` (đã chờ quá lâu) mới gửi phần text, bỏ ảnh.

    ``quote`` (tùy chọn): thông tin tin ĐÍCH cần TRẢ LỜI để nhóm đích phản hồi đúng
    tin tương ứng — {owner, msgId, cliMsgId, type, ts, text}.
    Trả thêm ``sentMsgId`` + ``sentCliMsgId`` của tin vừa gửi (để map dựng quote sau).
    """
    data = {
        # Nexus tự tra cookies/zpwEnk/imei theo profileId.
        "profileId": settings.get("profile_id") or settings.get("account_id") or "",
        "group_id": dest_group_id,
        "message": text,
    }
    if quote and quote.get("msgId") and quote.get("cliMsgId"):
        data["qmsgOwner"] = str(quote.get("owner") or "")
        data["qmsgId"] = str(quote.get("msgId"))
        data["qmsgCliId"] = str(quote.get("cliMsgId"))
        data["qmsgType"] = str(quote.get("type") or "webchat")
        data["qmsgTs"] = str(quote.get("ts") or "")
        data["qmsg"] = str(quote.get("text") or "")
    files = None
    with_photo = False
    if thumb:
        image_bytes = _download_image(thumb)
        if image_bytes:
            files = {"photo": (f"aff_{int(time.time() * 1000)}.jpg", image_bytes, "image/jpeg")}
            with_photo = True
        elif not force_text:
            # Ảnh chưa tải được (URL chưa sẵn sàng) -> BÁO CHỜ, không gửi thiếu ảnh.
            return {"ok": False, "withPhoto": False, "imageNotReady": True,
                    "message": "Ảnh chưa sẵn sàng (chưa tải được URL) — sẽ đợi & thử lại."}
    if not text.strip() and not files:
        return {"ok": False, "withPhoto": False,
                "message": "Tin không có nội dung chữ và ảnh không tải được."}
    try:
        payload = nexus_post_form("/api/send-group-message", data, files=files)
    except NexusError as exc:
        return {"ok": False, "withPhoto": with_photo, "message": str(exc)}
    if not payload.get("success"):
        return {"ok": False, "withPhoto": with_photo,
                "message": str(payload.get("error") or "Nexus báo gửi thất bại.")}
    return {"ok": True, "withPhoto": with_photo,
            "sentMsgId": str(payload.get("sentMsgId") or ""),
            "sentCliMsgId": str(payload.get("sentCliMsgId") or "")}


def _send_delay(settings: dict) -> int:
    """Độ trễ (giây) giữa các lần gửi tin — tránh gửi dồn dập. Mặc định 3s."""
    try:
        return max(0, int(settings.get("send_delay_seconds", 3)))
    except Exception:
        return 3


def _is_own_forwarded(msg: dict, settings: dict) -> bool:
    """Tin CÓ PHẢI do chính webhook đã chuyển tiếp không (đã gắn subId của mình).

    Dùng để tránh lặp vô hạn khi một nhóm vừa là nguồn vừa là đích: tin webhook
    gửi ra luôn chứa ``sub_id=<subid>`` (Shopee) hoặc ``subId1=<subid>`` (Lazada).
    Tin thường do người dùng tự gõ KHÔNG bị coi là của webhook -> vẫn chuyển tiếp.
    """
    sub = str(settings.get("sub_id") or "").strip()
    if not sub:
        return False
    raw = str(msg.get("title") or "") + " " + str(msg.get("href") or "")
    # Shopee: sub_id1= (mới) / sub_id= (link cũ); Lazada: subId1=.
    return (("sub_id1=" + sub) in raw or ("sub_id=" + sub) in raw
            or ("subId1=" + sub) in raw)


def _convert_message_text(settings: dict, msg: dict):
    """Chuyển toàn bộ link Shopee/Lazada trong 1 tin thành link aff.

    Trả (raw_text, new_text, conversions, ok_count, links). Dùng chung cho lượt
    quét mới lẫn lượt retry để logic chuyển link nhất quán.
    """
    raw_text = _strip_content_label(msg.get("title"))
    scan_text = " ".join(filter(None, [raw_text, str(msg.get("href") or "")]))
    # resolve_unknown=True: link rút gọn bên thứ ba (dealgiare.com...) dẫn tới
    # Shopee/Lazada cũng được nhận để chuyển sang mã của mình.
    links = extract_product_links(scan_text, resolve_unknown=True)
    conversions = []
    new_text = raw_text
    ok_count = 0
    for item in links:
        # Link lạ đã resolve -> dựng link aff từ URL đích; vẫn thay đúng URL gốc
        # trong nội dung tin.
        src = item.get("resolved") or item["url"]
        conv = convert_product_link(src, item["platform"], settings)
        conversions.append(conv)
        if conv.get("ok"):
            ok_count += 1
            if item["url"] in new_text:
                new_text = new_text.replace(item["url"], conv["link"])
            else:
                # Link nằm ở href (link card) — nối vào cuối nội dung.
                new_text = (new_text + "\n" + conv["link"]).strip()
    return raw_text, new_text, conversions, ok_count, links


def _link_entries(conversions: list) -> list:
    """Tóm tắt kết quả chuyển link để ghi vào nhật ký."""
    return [
        {"platform": c.get("platform"), "original": c.get("original"),
         "aff": c.get("link", ""), "ok": bool(c.get("ok")),
         "converted": bool(c.get("converted", True)),
         "error": "" if c.get("ok") else str(c.get("message") or "")}
        for c in conversions
    ]


def _forward_message(settings: dict, gid: str, dest_ids: list, msg: dict,
                     force_text: bool = False, fmap: dict = None) -> dict:
    """Chuyển link trong 1 tin rồi gửi vào từng nhóm đích trong ``dest_ids``.

    Trả dict gồm: entries (log), sent_dests, failed_dests (các nhóm chưa gửi
    được -> cần thử lại), converted_ok (có chuyển được link nào không), no_link,
    image_pending (có nhóm đích chưa gửi được vì ẢNH chưa sẵn sàng -> nên CHỜ).
    ``force_text=True`` -> gửi phần text dù ảnh chưa tải được (fallback sau khi chờ).

    ``fmap`` = bản đồ ``msgId nguồn -> {destGroupId: {msgId, cliMsgId}}`` của các tin
    ĐÃ chuyển tiếp. Nếu tin nguồn là TRẢ LỜI (``msg['quote']``) một tin đã map thì gửi
    kèm quote để nhóm đích phản hồi đúng tin tương ứng; sau khi gửi, ghi map cho tin này.
    """
    raw_text, new_text, conversions, ok_count, links = _convert_message_text(settings, msg)
    has_photo = bool(str(msg.get("thumb") or "").strip())
    has_text = bool((raw_text or "").strip())
    msg_type = str(msg.get("msgType") or "")
    # Tin nguồn TRẢ LỜI tin nào (globalMsgId = msgId của tin gốc trong group-since).
    src_quote = msg.get("quote") if isinstance(msg.get("quote"), dict) else None
    quoted_src_id = str((src_quote or {}).get("globalMsgId") or "").strip()
    owner_uid = str(settings.get("profile_id") or settings.get("account_id") or "").strip()
    src_msg_id = str(msg.get("msgId") or "").strip()
    # Chuyển tiếp MỌI tin có nội dung: chữ / ảnh / link (không nhất thiết phải có link).
    # Chỉ bỏ qua tin RỖNG hoàn toàn hoặc tin hệ thống (thu hồi/xóa) — không có giá trị.
    is_system_noise = msg_type in ("chat.undo", "chat.delete")
    nothing_to_forward = (not links and not has_photo and not has_text) or is_system_noise
    out = {"entries": [], "sent_dests": [], "failed_dests": [],
           "converted_ok": ok_count > 0, "no_link": nothing_to_forward,
           "image_pending": False}
    if nothing_to_forward:
        return out
    base = {
        "groupId": gid, "groupName": gid,
        "sender": str(msg.get("senderName") or msg.get("senderUid") or ""),
        "textPreview": (new_text or raw_text)[:300],
        "thumbUrl": str(msg.get("thumb") or ""),
        "links": _link_entries(conversions),
        "msgId": str(msg.get("msgId") or ""),
        "msgTs": int(msg.get("createTime") or 0),
    }
    # CÓ link nhưng chuyển KHÔNG được cái nào -> KHÔNG chặn/retry nữa (vòng retry cũ
    # làm nghẽn mốc, delay lâu). Chuyển tiếp LUÔN tin với link gốc (new_text == raw_text
    # vì không thay được link nào); log các link ghi rõ ok=false để biết chưa gắn subId.
    delay = _send_delay(settings)
    for i, dest in enumerate(dest_ids):
        if i > 0 and delay:
            time.sleep(delay)  # giãn giữa các nhóm đích, tránh gửi dồn dập
        # Nếu tin nguồn trả lời 1 tin đã chuyển tiếp vào nhóm đích này -> gửi kèm quote.
        quote = None
        if quoted_src_id and isinstance(fmap, dict):
            ref = (fmap.get(quoted_src_id) or {}).get(dest)
            if ref and ref.get("msgId") and ref.get("cliMsgId"):
                quote = {"owner": owner_uid, "msgId": ref["msgId"], "cliMsgId": ref["cliMsgId"],
                         "type": ref.get("type") or "webchat", "ts": "",
                         "text": str((src_quote or {}).get("text") or "")}
        sent = _send_to_dest(settings, dest, new_text, str(msg.get("thumb") or ""),
                             force_text=force_text, quote=quote)
        entry = dict(base)
        entry["destGroupId"] = dest
        if quote:
            entry["repliedTo"] = quoted_src_id
        if sent.get("ok"):
            # Ghi map để tin sau trả lời đúng tin này ở nhóm đích.
            if isinstance(fmap, dict) and src_msg_id and sent.get("sentMsgId"):
                fmap.setdefault(src_msg_id, {})[dest] = {
                    "msgId": str(sent.get("sentMsgId")),
                    "cliMsgId": str(sent.get("sentCliMsgId") or ""),
                    # Loại tin đích để dựng đúng quote: có text -> webchat, chỉ ảnh -> photo.
                    "type": "webchat" if new_text.strip() else "chat.photo",
                }
            # links rỗng (tin chỉ có ảnh) hoặc đổi được hết -> "sent"; đổi được 1 phần
            # -> "sent_partial"; CÓ link mà không đổi được cái nào -> "sent_raw"
            # (gửi nguyên link gốc, chưa gắn subId) để nhật ký nêu rõ.
            if not conversions or ok_count == len(conversions):
                status = "sent"
            elif ok_count > 0:
                status = "sent_partial"
            else:
                status = "sent_raw"
            entry.update({
                "status": status,
                "withPhoto": bool(sent.get("withPhoto")), "error": "",
            })
            out["sent_dests"].append(dest)
        else:
            if sent.get("imageNotReady"):
                out["image_pending"] = True
                entry["status"] = "waiting_image"
            else:
                entry["status"] = "error"
            entry.update({"withPhoto": False,
                          "error": str(sent.get("message") or "Gửi vào nhóm kết quả thất bại")})
            out["failed_dests"].append(dest)
        out["entries"].append(entry)
    return out


def _force_forward_message(settings: dict, gid: str, dest_ids: list, msg: dict) -> dict:
    """GỬI LẠI THỦ CÔNG 1 tin vào các nhóm đích — DÙ KHÔNG có link.

    Dùng cho nút "Gửi lại": tin có link -> chuyển link như thường; tin KHÔNG link
    (thường là ẢNH) -> vẫn gửi nguyên nội dung + ảnh. Ảnh chưa sẵn sàng thì vẫn
    gửi (``force_text=True``) để bấm là có kết quả ngay.
    Trả {entries, sent_dests, failed_dests}.
    """
    raw_text, new_text, conversions, ok_count, links = _convert_message_text(settings, msg)
    text = new_text if (links and ok_count) else raw_text
    thumb = str(msg.get("thumb") or "")
    out = {"entries": [], "sent_dests": [], "failed_dests": []}
    if not text.strip() and not thumb:
        return out  # tin rỗng (không chữ, không ảnh) -> bỏ qua
    base = {
        "groupId": gid, "groupName": gid,
        "sender": str(msg.get("senderName") or msg.get("senderUid") or ""),
        "textPreview": (text or raw_text)[:300],
        "thumbUrl": thumb,
        "links": _link_entries(conversions),
        "msgId": str(msg.get("msgId") or ""),
        "msgTs": int(msg.get("createTime") or 0),
        "manual": True,
    }
    delay = _send_delay(settings)
    for i, dest in enumerate(dest_ids):
        if i > 0 and delay:
            time.sleep(delay)
        sent = _send_to_dest(settings, dest, text, thumb, force_text=True)
        entry = dict(base)
        entry["destGroupId"] = dest
        if sent.get("ok"):
            entry.update({"status": "sent", "withPhoto": bool(sent.get("withPhoto")), "error": ""})
            out["sent_dests"].append(dest)
        else:
            entry.update({"status": "error", "withPhoto": False,
                          "error": str(sent.get("message") or "Gửi lại thất bại")})
            out["failed_dests"].append(dest)
        out["entries"].append(entry)
    return out


def _log_forward_result(res: dict) -> None:
    """Ghi nhật ký + thống kê cho 1 lần chuyển tiếp 1 tin (nhiều nhóm đích)."""
    with _lock:
        d = _load_db()
        for entry in res["entries"]:
            _append_log(d, entry)
            if entry["status"].startswith("sent"):
                d["stats"]["forwarded"] = int(d["stats"]["forwarded"]) + 1
            elif entry["status"] != "waiting_image":
                d["stats"]["errors"] = int(d["stats"]["errors"]) + 1
        _save_db(d)


def _log_group_error(gid: str, message: str) -> None:
    """Ghi nhật ký 1 lỗi ở mức NHÓM NGUỒN (không chặn các nhóm khác)."""
    with _lock:
        d = _load_db()
        _append_log(d, {"groupId": gid, "groupName": gid, "status": "error",
                        "error": str(message), "links": [], "textPreview": ""})
        d["stats"]["errors"] = int(d["stats"]["errors"]) + 1
        _save_db(d)


def _scan_source_group(settings: dict, gid: str, group_routes: list,
                       last_by_group: dict, forward_map: dict) -> dict:
    """Quét 1 NHÓM NGUỒN: chuyển link tin mới rồi gửi tới các nhóm đích của nhóm đó.

    Cập nhật ``last_by_group[gid]`` (mốc msgId) và ``forward_map`` (quote).
    Trả thống kê riêng nhóm này: {"checked","newMessages","forwarded","errors","baseline"}.
    Mọi lỗi được ghi log và trả về — KHÔNG raise ra ngoài để không chặn nhóm khác.
    """
    prev = last_by_group.get(gid) or {}
    since_id = str(prev.get("msgId") or "0") or "0"
    is_baseline = since_id in ("", "0")
    acc_ref = str(settings.get("profile_id") or settings.get("account_id") or "").strip()
    stats = {"checked": 0, "newMessages": 0, "forwarded": 0, "errors": 0, "baseline": 0}

    try:
        # Lấy TẤT CẢ tin có msgId > mốc (Nexus dùng getrecentv2, phân trang lùi
        # tới mốc) -> không bỏ sót khi có nhiều tin giữa 2 lần quét.
        payload = nexus_get("/api/messages/group-since",
                            {"groupId": gid, "profileId": acc_ref,
                             "sinceMsgId": since_id})
    except NexusError as exc:
        _log_group_error(gid, str(exc))
        stats["errors"] += 1
        return stats
    stats["checked"] = 1
    if not payload.get("success"):
        _log_group_error(gid, str(payload.get("error") or "group-since lỗi"))
        stats["errors"] += 1
        return stats

    # items: đã sắp CŨ -> MỚI, chỉ gồm tin có msgId > mốc (Nexus lọc sẵn).
    items = payload.get("items") or []
    latest_seen = str(payload.get("latestMsgId") or since_id or "0")
    # stuck: tin đang bị kẹt (gửi lỗi) của nhóm này, kèm số lần đã thử.
    stuck = dict(prev.get("stuck") or {})
    all_dests = [r["dest_group_id"] for r in group_routes]

    if is_baseline:
        # Nhóm MỚI (chưa có mốc): chỉ chuyển tin RẤT MỚI (trong _BASELINE_RECENT_MS),
        # bỏ lịch sử cũ; ghi mốc = tin mới nhất, các lần sau lấy tiếp từ đó.
        last_by_group[gid] = {
            "msgId": latest_seen,
            "ts": int(items[-1].get("createTime") or 0) if items else int(prev.get("ts") or 0),
        }
        recent_cutoff = int(time.time() * 1000) - _BASELINE_RECENT_MS
        new_items = [it for it in items if int(it.get("createTime") or 0) >= recent_cutoff]
        if not new_items:
            stats["baseline"] = 1
            return stats
        for it in new_items:
            stats["newMessages"] += 1
            if _is_own_forwarded(it, settings):
                continue
            # Baseline chỉ chạy 1 lần (mốc đã tiến) nên không chờ lại được ảnh:
            # gửi luôn phần text nếu ảnh chưa sẵn sàng, tránh mất tin.
            res = _forward_message(settings, gid, all_dests, it,
                                   force_text=True, fmap=forward_map)
            if res["no_link"]:
                continue
            _log_forward_result(res)
            stats["forwarded"] += len(res["sent_dests"])
            stats["errors"] += len(res["failed_dests"])
            time.sleep(_send_delay(settings))
        return stats

    # KHÔNG baseline: xử lý CŨ->MỚI, MỐC CHỈ TIẾN QUA TIN ĐÃ XONG. Gặp tin lỗi
    # thì DỪNG (mốc giữ ở tin thành công trước đó) để lượt sau tiếp tục ĐÚNG từ
    # tin lỗi, KHÔNG nhảy lên tin mới nhất -> không bỏ sót tin ở giữa.
    mark_msg = since_id
    mark_ts = int(prev.get("ts") or 0)
    for it in items:
        mid = str(it.get("msgId") or "")
        mts = int(it.get("createTime") or 0)
        stats["newMessages"] += 1
        # Bỏ qua tin do CHÍNH webhook đã chuyển (đã gắn subId của mình) để tránh
        # lặp vô hạn khi một nhóm vừa là nguồn vừa là đích. Tin thường — kể cả do
        # chính tài khoản tự gõ — vẫn được chuyển tiếp bình thường.
        if _is_own_forwarded(it, settings):
            mark_msg, mark_ts = mid, mts
            if stuck.get("msgId") == mid:
                stuck = {}
            continue
        # Nếu là tin đang kẹt -> chỉ gửi các nhóm đích CÒN THIẾU (tránh gửi trùng).
        is_stuck_here = stuck.get("msgId") == mid
        dest_ids = (stuck.get("pendingDests") or all_dests) if is_stuck_here else all_dests
        prev_attempts = int(stuck.get("attempts") or 0) if is_stuck_here else 0
        img_wait = bool(stuck.get("imageWait")) if is_stuck_here else False
        # Tin CÓ ẢNH: đã chờ đủ _IMG_MAX_RETRY lượt mà vẫn chưa có URL ảnh -> lượt
        # này gửi TEXT (bỏ ảnh) để không mất tin; còn lại thì vẫn CHỜ ảnh.
        force_text = img_wait and (prev_attempts + 1 >= _IMG_MAX_RETRY)
        res = _forward_message(settings, gid, dest_ids, it, force_text=force_text,
                               fmap=forward_map)
        if res["no_link"]:
            mark_msg, mark_ts = mid, mts
            if stuck.get("msgId") == mid:
                stuck = {}
            continue
        _log_forward_result(res)
        stats["forwarded"] += len(res["sent_dests"])

        if not res["failed_dests"]:
            # Gửi thành công hết (kể cả tin chỉ có ảnh) -> mốc tiến qua tin này.
            mark_msg, mark_ts = mid, mts
            if stuck.get("msgId") == mid:
                stuck = {}
            time.sleep(_send_delay(settings))
            continue

        # Tin CHƯA XONG. Phân biệt: (a) CHỜ ẢNH (URL chưa sẵn sàng) -> chờ lâu
        # hơn, KHÔNG tính là lỗi; (b) lỗi thật (gửi fail / không chuyển được link).
        image_wait = img_wait or bool(res.get("image_pending"))
        if not image_wait:
            stats["errors"] += len(res["failed_dests"])
        budget = _IMG_MAX_RETRY if image_wait else _MAX_RETRY
        attempts = prev_attempts + 1
        if attempts >= budget:
            # Quá hạn: với tin ảnh, lượt này đã force_text (gửi text bỏ ảnh) — nếu
            # vẫn tới đây nghĩa là ngay text cũng lỗi -> bỏ qua để không nghẽn.
            reason = ("Đã chờ ảnh quá lâu vẫn chưa có URL -> bỏ qua." if image_wait
                      else f"Đã bỏ qua sau {attempts} lần thử (không gửi được).")
            with _lock:
                d = _load_db()
                _append_log(d, {
                    "groupId": gid, "groupName": gid, "status": "error",
                    "error": reason,
                    "links": [], "textPreview": str(it.get("title") or "")[:220],
                    "msgId": mid, "skipped": True,
                })
                _save_db(d)
            mark_msg, mark_ts = mid, mts
            stuck = {}
            time.sleep(_send_delay(settings))
            continue
        # Chưa tới ngưỡng -> DỪNG tại đây; lượt sau bắt đầu lại đúng từ tin này
        # (nếu chờ ảnh: đợi tới khi CDN có URL ảnh rồi mới gửi kèm ảnh).
        stuck = {"msgId": mid, "attempts": attempts,
                 "pendingDests": res["failed_dests"], "imageWait": image_wait}
        break

    new_mark = {"msgId": mark_msg, "ts": mark_ts}
    if stuck:
        new_mark["stuck"] = stuck
    last_by_group[gid] = new_mark
    return stats


def run_forward_once(triggered_by: str = "worker") -> dict:
    """Kiểm tra mọi nhóm nguồn 1 lượt, chuyển link và gửi vào nhóm kết quả.

    Mỗi nhóm nguồn xử lý ĐỘC LẬP: lỗi ở 1 nhóm không làm dừng các nhóm khác, và
    mốc msgId được lưu ngay sau mỗi nhóm nên không mất tiến độ khi có lỗi.
    Một nhóm nguồn thuộc nhiều luồng -> quét 1 lần, gửi cho từng nhóm đích.
    """
    with _lock:
        db = _load_db()
        settings = dict(db["settings"])
        last_by_group = dict(db["state"]["lastMsgByGroup"])
        # Bản đồ msgId nguồn -> {destGroupId: {msgId, cliMsgId}} để dựng lại quote
        # (nhóm đích trả lời đúng tin tương ứng đã chuyển tiếp).
        forward_map = dict(db["state"].get("forwardMap") or {})

    def _persist_state() -> None:
        with _lock:
            d = _load_db()
            d["state"]["lastMsgByGroup"] = last_by_group
            d["state"]["forwardMap"] = forward_map
            d["state"].pop("retryQueue", None)  # cơ chế hàng đợi cũ — không dùng nữa
            _save_db(d)

    def _finish(ok: bool, message: str, **extra) -> dict:
        _persist_state()
        with _lock:
            d = _load_db()
            d["stats"]["lastRunAt"] = _now_str()
            d["stats"]["lastRunMessage"] = message
            _save_db(d)
        result = {"ok": ok, "checkedGroups": 0, "newMessages": 0,
                  "forwarded": 0, "errors": 0, "message": message}
        result.update(extra)
        return result

    # profileId (uid) của tài khoản thực hiện — Nexus tự tra phiên theo id này.
    acc_ref = str(settings.get("profile_id") or settings.get("account_id") or "").strip()
    if not acc_ref:
        return _finish(False, "Chưa chọn tài khoản Zalo trên dashboard.")
    # Luồng đủ cấu hình = có nhóm kết quả + ít nhất 1 nhóm nguồn.
    routes = [r for r in settings["routes"] if r["dest_group_id"] and r["source_group_ids"]]
    if not routes:
        return _finish(False, "Chưa có luồng nào đủ cấu hình — chọn nhóm kết quả trước, "
                              "rồi tick các nhóm nguồn cho luồng đó.")

    # Một nhóm nguồn có thể thuộc nhiều luồng: quét MỘT lần, gửi cho từng luồng.
    routes_by_source: dict = {}
    for route in routes:
        for gid in route["source_group_ids"]:
            routes_by_source.setdefault(gid, []).append(route)
    source_ids = sorted(routes_by_source)

    checked = 0
    new_messages = 0
    forwarded = 0
    errors = 0
    baseline_count = 0

    for gid in source_ids:
        try:
            st = _scan_source_group(settings, gid, routes_by_source[gid],
                                    last_by_group, forward_map)
        except Exception as exc:  # noqa: BLE001 — 1 nhóm lỗi không được chặn nhóm khác
            _log_group_error(gid, f"Lỗi xử lý nhóm: {exc}")
            st = {"checked": 0, "newMessages": 0, "forwarded": 0, "errors": 1, "baseline": 0}
        checked += st["checked"]
        new_messages += st["newMessages"]
        forwarded += st["forwarded"]
        errors += st["errors"]
        baseline_count += st["baseline"]
        _persist_state()  # lưu mốc ngay -> lỗi về sau không mất tiến độ nhóm này

    # Giới hạn kích thước bản đồ quote (giữ các msgId mới nhất theo thứ tự chèn).
    if len(forward_map) > _FORWARD_MAP_MAX:
        for k in list(forward_map.keys())[:len(forward_map) - _FORWARD_MAP_MAX]:
            forward_map.pop(k, None)

    _persist_state()

    baseline_note = f", {baseline_count} nhóm ghi mốc lần đầu" if baseline_count else ""
    stuck_groups = sum(1 for m in last_by_group.values()
                       if isinstance(m, dict) and m.get("stuck"))
    stuck_note = f" | {stuck_groups} nhóm đang chờ gửi lại tin lỗi" if stuck_groups else ""
    message = (f"Đã kiểm tra {checked}/{len(source_ids)} nhóm nguồn của {len(routes)} luồng: "
               f"{new_messages} tin mới, chuyển tiếp {forwarded}, lỗi {errors}{baseline_note}{stuck_note}.")
    return _finish(True, message, checkedGroups=checked, newMessages=new_messages,
                   forwarded=forwarded, errors=errors)


# ─── Worker chạy nền theo chu kỳ ─────────────────────────────────────────────

_last_run = 0.0
_run_lock = threading.Lock()  # tránh chạy chồng (worker + bấm tay cùng lúc)


def _worker_loop() -> None:
    global _last_run
    while True:
        # Ngủ ngắn để phục vụ được chu kỳ nhỏ (tới 1s). Mỗi vòng chỉ đọc cấu
        # hình nhẹ rồi kiểm tra mốc thời gian; lượt quét Nexus vẫn do _run_lock
        # chống chạy chồng.
        time.sleep(1)
        try:
            with _lock:
                settings = _load_db()["settings"]
            if not settings.get("enabled"):
                continue
            interval_seconds = max(1, int(settings.get("interval_seconds") or 1))
            if time.time() - _last_run < interval_seconds:
                continue
            _last_run = time.time()
            with _run_lock:
                result = run_forward_once(triggered_by="worker")
            print(f"[aff_webhook] {result.get('message', '')}", flush=True)
        except Exception as exc:
            print(f"[aff_webhook] Loop error: {exc}", flush=True)


# ─── HTTP API + dashboard ────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "aff-webhook"})


@app.route("/api/config", methods=["GET", "PATCH"])
def api_config():
    with _lock:
        db = _load_db()
        if request.method == "GET":
            return jsonify({"success": True, "settings": db["settings"], "stats": db["stats"],
                            "nexusUrl": NEXUS_URL})
        patch = request.get_json(silent=True) or {}
        settings = db["settings"]
        if "enabled" in patch:
            settings["enabled"] = bool(patch["enabled"])
        if "profile_id" in patch:
            settings["profile_id"] = str(patch["profile_id"] or "").strip()
        if "account_id" in patch:
            settings["account_id"] = str(patch["account_id"] or "").strip()
        if "routes" in patch:
            settings["routes"] = _sanitize_routes(patch["routes"])
        if "interval_seconds" in patch:
            try:
                settings["interval_seconds"] = max(1, int(patch["interval_seconds"]))
            except Exception:
                settings["interval_seconds"] = 1
        if "send_delay_seconds" in patch:
            try:
                settings["send_delay_seconds"] = max(0, int(patch["send_delay_seconds"]))
            except Exception:
                settings["send_delay_seconds"] = 3
        if "shopee_aff_id" in patch:
            settings["shopee_aff_id"] = str(patch["shopee_aff_id"] or "").strip() or DEFAULT_SHOPEE_AFF_ID
        if "shopee_app_id" in patch:
            settings["shopee_app_id"] = str(patch["shopee_app_id"] or "").strip()
        if "shopee_app_secret" in patch:
            settings["shopee_app_secret"] = str(patch["shopee_app_secret"] or "").strip()
        if "sub_id" in patch:
            settings["sub_id"] = str(patch["sub_id"] or "").strip()
        if "lazada_app_key" in patch:
            settings["lazada_app_key"] = str(patch["lazada_app_key"] or "").strip()
        if "lazada_app_secret" in patch:
            settings["lazada_app_secret"] = str(patch["lazada_app_secret"] or "").strip()
        if "lazada_user_token" in patch:
            settings["lazada_user_token"] = str(patch["lazada_user_token"] or "").strip()
        if "lazada_cookie" in patch:
            # Chấp nhận cả chuỗi header lẫn bảng cookies copy từ DevTools.
            settings["lazada_cookie"] = normalize_lazada_cookie(patch["lazada_cookie"])
        _save_db(db)
        return jsonify({"success": True, "settings": settings, "stats": db["stats"]})


@app.route("/api/logs", methods=["GET"])
def api_logs():
    with _lock:
        db = _load_db()
        return jsonify({"success": True, "logs": db["logs"][:100], "stats": db["stats"]})


@app.route("/api/logs/clear", methods=["POST"])
def api_logs_clear():
    """Xóa nhật ký theo loại: which = 'success' | 'fail' | 'all' (mặc định all)."""
    data = request.get_json(silent=True) or {}
    which = str(data.get("which") or "all").strip().lower()

    def _is_fail(l):
        return str(l.get("status")) == "error" or bool(l.get("skipped"))

    with _lock:
        db = _load_db()
        if which == "fail":
            db["logs"] = [l for l in db["logs"] if not _is_fail(l)]
            db["stats"]["errors"] = 0
        elif which == "success":
            db["logs"] = [l for l in db["logs"] if _is_fail(l)]
            db["stats"]["forwarded"] = 0
        else:
            db["logs"] = []
            db["stats"]["forwarded"] = 0
            db["stats"]["errors"] = 0
        db["stats"]["lastRunMessage"] = f"Đã xóa lịch sử ({which})."
        _save_db(db)
    return jsonify({"success": True, "which": which, "remaining": len(db["logs"])})


@app.route("/api/run", methods=["POST"])
@app.route("/webhook/run", methods=["POST", "GET"])
def api_run():
    """Chạy 1 lượt kiểm tra + chuyển tiếp ngay (nút "Chạy ngay" hoặc webhook ngoài)."""
    global _last_run
    if not _run_lock.acquire(blocking=False):
        return jsonify({"success": False, "error": "Đang có lượt chạy khác, thử lại sau."}), 409
    try:
        _last_run = time.time()  # reset chu kỳ worker sau khi chạy tay
        result = run_forward_once(triggered_by="manual")
    finally:
        _run_lock.release()
    return jsonify({"success": result.get("ok", False), **result})


@app.route("/api/rerun-from", methods=["POST"])
def api_rerun_from():
    """GỬI LẠI đúng (các) tin đã chọn KÈM tin ngay TRƯỚC nó, cho nhóm đó thôi.

    KHÔNG lùi mốc, KHÔNG gửi lại các tin sau. Tin trước thường là ẢNH (không link)
    -> vẫn gửi ảnh. Dùng khi tin đã chuyển thiếu ảnh (ảnh nằm ở tin trước)."""
    data = request.get_json(silent=True) or {}
    items = data.get("items") or []
    # Gom các msgId được chọn theo từng nhóm.
    sel_by_group = {}
    for it in items:
        gid = str(it.get("groupId") or "").strip()
        mid = str(it.get("msgId") or "").strip()
        if gid and mid.isdigit():
            sel_by_group.setdefault(gid, set()).add(int(mid))
    if not sel_by_group:
        return jsonify({"success": False, "error": "Chưa chọn tin hợp lệ để gửi lại."}), 400

    with _lock:
        settings = dict(_load_db()["settings"])
    acc_ref = str(settings.get("profile_id") or settings.get("account_id") or "").strip()
    if not acc_ref:
        return jsonify({"success": False, "error": "Chưa chọn tài khoản Zalo trên dashboard."}), 400
    # Nhóm nguồn -> các nhóm đích tương ứng.
    routes_by_source = {}
    for r in settings.get("routes") or []:
        if not (r.get("dest_group_id") and r.get("source_group_ids")):
            continue
        for gid in r["source_group_ids"]:
            routes_by_source.setdefault(gid, []).append(r)

    if not _run_lock.acquire(blocking=False):
        return jsonify({"success": False, "error": "Đang có lượt chạy khác, thử lại sau."}), 409
    sent = 0
    errors = 0
    pairs = 0
    not_found = 0
    try:
        for gid, sel in sel_by_group.items():
            dests = [r["dest_group_id"] for r in routes_by_source.get(gid, [])]
            if not dests:
                errors += 1
                continue
            # Lấy 1 cửa sổ tin gần đây của nhóm để tìm tin đã chọn + tin ngay trước.
            try:
                payload = nexus_get("/api/messages/group-since",
                                    {"groupId": gid, "profileId": acc_ref,
                                     "sinceMsgId": "0", "count": 50})
            except NexusError:
                errors += 1
                continue
            if not payload.get("success"):
                errors += 1
                continue
            win = payload.get("items") or []  # cũ -> mới
            pos = {str(m.get("msgId")): i for i, m in enumerate(win)}
            idxs = set()
            for mid in sel:
                i = pos.get(str(mid))
                if i is None:
                    not_found += 1
                    continue
                if i - 1 >= 0:
                    idxs.add(i - 1)  # tin ngay TRƯỚC (thường là ảnh)
                idxs.add(i)          # tin đã chọn
            if not idxs:
                continue
            pairs += 1
            for i in sorted(idxs):  # cũ -> mới: gửi ảnh trước, tin có link sau
                res = _force_forward_message(settings, gid, dests, win[i])
                with _lock:
                    d = _load_db()
                    for entry in res["entries"]:
                        _append_log(d, entry)
                        if entry["status"] == "sent":
                            d["stats"]["forwarded"] = int(d["stats"]["forwarded"]) + 1
                        else:
                            d["stats"]["errors"] = int(d["stats"]["errors"]) + 1
                    _save_db(d)
                sent += len(res["sent_dests"])
                errors += len(res["failed_dests"])
                time.sleep(_send_delay(settings))
    finally:
        _run_lock.release()

    if not pairs:
        return jsonify({"success": False,
                        "error": "Không tìm thấy tin đã chọn trong 50 tin gần đây của nhóm "
                                 "(tin có thể quá cũ)."}), 400
    message = (f"Đã gửi lại {pairs} nhóm (mỗi tin kèm tin ngay trước): "
               f"gửi {sent}, lỗi {errors}"
               + (f", {not_found} tin không tìm thấy." if not_found else "."))
    return jsonify({"success": True, "message": message,
                    "forwarded": sent, "errors": errors, "pairs": pairs})


@app.route("/api/test-convert", methods=["POST"])
def api_test_convert():
    """Thử chuyển 1 URL theo cấu hình hiện tại (kiểm tra aff id / cookie Lazada)."""
    data = request.get_json(silent=True) or {}
    url = str(data.get("url") or "").strip()
    if not url:
        return jsonify({"success": False, "error": "Chưa nhập URL sản phẩm."}), 400
    links = extract_product_links(url, resolve_unknown=True)
    if not links:
        return jsonify({"success": False,
                        "error": "URL không phải link Shopee/Lazada (và không dẫn tới hai sàn này)."}), 400
    with _lock:
        settings = _load_db()["settings"]
    result = convert_product_link(links[0].get("resolved") or links[0]["url"],
                                  links[0]["platform"], settings)
    status = 200 if result.get("ok") else 400
    return jsonify({"success": bool(result.get("ok")), **result}), status


@app.route("/api/send-manual", methods=["POST"])
def api_send_manual():
    """Gửi thủ công: chuyển link (nếu có) sang link aff, gộp với nội dung tin
    nhắn rồi gửi vào (các) NHÓM ĐÍCH cấu hình trong luồng qua Nexus."""
    data = request.get_json(silent=True) or {}
    url = str(data.get("url") or "").strip()
    message = str(data.get("message") or "").strip()

    with _lock:
        settings = _load_db()["settings"]
    if not (settings.get("profile_id") or settings.get("account_id")):
        return jsonify({"success": False, "error": "Chưa chọn tài khoản Zalo trên dashboard."}), 400

    # Nhóm đích = các nhóm kết quả của luồng (khử trùng lặp).
    dests = []
    for r in settings.get("routes", []):
        d = str(r.get("dest_group_id") or "").strip()
        if d and d not in dests:
            dests.append(d)
    if not dests:
        return jsonify({"success": False,
                        "error": "Chưa có nhóm kết quả nào trong luồng — hãy chọn nhóm kết quả trước."}), 400

    aff_link = ""
    if url:
        links = extract_product_links(url, resolve_unknown=True)
        if not links:
            return jsonify({"success": False,
                            "error": "URL không phải link Shopee/Lazada (và không dẫn tới hai sàn này)."}), 400
        conv = convert_product_link(links[0].get("resolved") or links[0]["url"],
                                    links[0]["platform"], settings)
        if not conv.get("ok"):
            return jsonify({"success": False, "error": conv.get("message") or "Không chuyển được link."}), 400
        aff_link = str(conv.get("link") or "").strip()

    # Gộp nội dung tin nhắn + link aff (mỗi phần 1 dòng).
    final_text = "\n".join(p for p in (message, aff_link) if p).strip()
    if not final_text:
        return jsonify({"success": False, "error": "Chưa có nội dung hoặc link để gửi."}), 400

    sent_ok, errors = 0, []
    delay = _send_delay(settings)
    for i, d in enumerate(dests):
        if i > 0 and delay:
            time.sleep(delay)  # giãn giữa các nhóm đích, tránh gửi dồn dập
        sent = _send_to_dest(settings, d, final_text, "")
        if sent.get("ok"):
            sent_ok += 1
        else:
            errors.append(str(sent.get("message") or "lỗi"))
    if not sent_ok:
        return jsonify({"success": False, "error": "Gửi thất bại: " + "; ".join(errors)}), 502
    return jsonify({"success": True, "link": aff_link, "text": final_text,
                    "sentTo": sent_ok, "totalDest": len(dests)})


@app.route("/api/nexus/accounts", methods=["GET"])
def api_nexus_accounts():
    """Proxy: danh sách tài khoản từ Nexus (tránh CORS cho dashboard)."""
    try:
        payload = nexus_get("/api/accounts")
    except NexusError as exc:
        return jsonify({"success": False, "error": str(exc), "accounts": []}), 502
    accounts = [
        {"accountId": str(a.get("accountId") or ""),
         # profileId = uid Zalo (ổn định) — webhook chỉ cần truyền cái này.
         "profileId": str(a.get("uid") or ""),
         "name": str(a.get("name") or ""),
         "avatarUrl": str(a.get("avatarUrl") or "")}
        for a in (payload.get("accounts") or [])
    ]
    return jsonify({"success": True, "accounts": accounts})


@app.route("/api/nexus/groups", methods=["GET"])
def api_nexus_groups():
    """Proxy: danh sách nhóm của tài khoản từ Nexus."""
    # Nhận profileId (uid) — ưu tiên; vẫn nhận accountId để tương thích.
    profile_ref = str(request.args.get("profileId") or request.args.get("accountId") or "").strip()
    if not profile_ref:
        return jsonify({"success": False, "error": "Thiếu profileId", "groups": []}), 400
    try:
        payload = nexus_get("/api/groups/personal", {"profileId": profile_ref})
    except NexusError as exc:
        return jsonify({"success": False, "error": str(exc), "groups": []}), 502
    groups = [
        {"groupId": str(g.get("groupId") or ""), "name": str(g.get("name") or ""),
         "avatar": str(g.get("avatar") or g.get("fullAvt") or "")}
        for g in (payload.get("groups") or [])
        if str(g.get("groupId") or "")
    ]
    return jsonify({"success": bool(payload.get("success", True)), "groups": groups})


def _open_dashboard_in_edge(url: str) -> None:
    """Tự mở dashboard bằng Microsoft Edge (fallback: trình duyệt mặc định)."""
    try:
        subprocess.Popen(["cmd", "/c", "start", "", "msedge", url])
    except Exception:
        try:
            webbrowser.open(url)
        except Exception as exc:
            print(f"[aff_webhook] Không tự mở được trình duyệt: {exc}", flush=True)


def main() -> None:
    port = 5001
    open_browser = "--no-browser" not in sys.argv
    for i, arg in enumerate(sys.argv):
        if arg == "--port" and i + 1 < len(sys.argv):
            try:
                port = int(sys.argv[i + 1])
            except ValueError:
                pass
    threading.Thread(target=_worker_loop, daemon=True, name="aff-webhook-worker").start()
    url = f"http://127.0.0.1:{port}/"
    print(f"[aff_webhook] Dashboard: {url} (Nexus API: {NEXUS_URL})", flush=True)
    if open_browser:
        threading.Timer(1.0, _open_dashboard_in_edge, args=(url,)).start()
    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    main()
