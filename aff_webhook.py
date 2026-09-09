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
# Nhóm mới thêm: vẫn chuyển tiếp tin đăng trong khoảng này (mặc định 30 phút);
# tin cũ hơn coi là lịch sử -> chỉ ghi mốc, không chuyển.
_BASELINE_RECENT_MS = 30 * 60 * 1000
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
        "shopee_aff_id": DEFAULT_SHOPEE_AFF_ID,
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


def _download_image(url: str, timeout: int = 30) -> bytes:
    """Tải ảnh từ CDN Zalo (link có thời hạn — lỗi thì trả b'')."""
    url = str(url or "").strip()
    if not url:
        return b""
    try:
        response = requests.get(url, headers={"User-Agent": BROWSER_UA},
                                timeout=timeout, proxies=NO_PROXY)
        if response.status_code == 200 and response.content:
            return response.content
    except Exception as exc:
        print(f"[aff_webhook] Không tải được ảnh: {exc}", flush=True)
    return b""


def _send_to_dest(settings: dict, dest_group_id: str, text: str, thumb: str) -> dict:
    """Gửi nội dung (kèm ảnh nếu tải được) vào nhóm kết quả qua API Nexus."""
    data = {
        # Nexus tự tra cookies/zpwEnk/imei theo profileId.
        "profileId": settings.get("profile_id") or settings.get("account_id") or "",
        "group_id": dest_group_id,
        "message": text,
    }
    files = None
    with_photo = False
    if thumb:
        image_bytes = _download_image(thumb)
        if image_bytes:
            files = {"photo": (f"aff_{int(time.time() * 1000)}.jpg", image_bytes, "image/jpeg")}
            with_photo = True
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
    return {"ok": True, "withPhoto": with_photo}


def run_forward_once(triggered_by: str = "worker") -> dict:
    """Kiểm tra mọi nhóm nguồn 1 lượt, chuyển link và gửi vào nhóm kết quả."""
    with _lock:
        db = _load_db()
        settings = dict(db["settings"])
        last_by_group = dict(db["state"]["lastMsgByGroup"])

    def _finish(ok: bool, message: str, **extra) -> dict:
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
        # MỐC đã lưu (msgId). Lần đầu = "0" -> baseline (chỉ tin rất mới).
        prev = last_by_group.get(gid) or {}
        since_id = str(prev.get("msgId") or "0") or "0"
        is_baseline = since_id in ("", "0")
        try:
            # Lấy TẤT CẢ tin có msgId > mốc (Nexus dùng getrecentv2, phân trang lùi
            # tới mốc) -> không bỏ sót khi có nhiều tin giữa 2 lần quét.
            payload = nexus_get("/api/messages/group-since",
                                {"groupId": gid, "profileId": acc_ref,
                                 "sinceMsgId": since_id})
        except NexusError as exc:
            return _finish(False, str(exc), checkedGroups=checked)
        checked += 1
        if not payload.get("success"):
            errors += 1
            with _lock:
                d = _load_db()
                _append_log(d, {"groupId": gid, "groupName": gid, "status": "error",
                                "error": str(payload.get("error") or "group-since lỗi"),
                                "links": [], "textPreview": ""})
                d["stats"]["errors"] = int(d["stats"]["errors"]) + 1
                _save_db(d)
            continue

        # items: đã sắp CŨ -> MỚI, chỉ gồm tin có msgId > mốc (Nexus lọc sẵn).
        items = payload.get("items") or []
        # Cập nhật MỐC = msgId lớn nhất Nexus thấy (kể cả khi không có tin mới)
        # để lần sau chỉ lấy tin MỚI HƠN, không lặp lại.
        latest_mark = str(payload.get("latestMsgId") or since_id or "0")
        last_by_group[gid] = {
            "msgId": latest_mark,
            "ts": int(items[-1].get("createTime") or 0) if items else int(prev.get("ts") or 0),
        }

        if is_baseline:
            # Nhóm MỚI (chưa có mốc): chỉ chuyển tin RẤT MỚI (trong _BASELINE_RECENT_MS),
            # bỏ qua lịch sử cũ; các lần sau lấy TẤT CẢ tin mới kể từ mốc này.
            recent_cutoff = int(time.time() * 1000) - _BASELINE_RECENT_MS
            new_items = [it for it in items if int(it.get("createTime") or 0) >= recent_cutoff]
            if not new_items:
                baseline_count += 1
                continue  # chỉ có tin cũ -> ghi mốc, không chuyển
        else:
            new_items = list(items)  # Nexus đã lọc msgId > mốc
        if not new_items:
            continue

        for latest in new_items:
            msg_id = str(latest.get("msgId") or "")
            msg_ts = int(latest.get("createTime") or 0)
            new_messages += 1
            if str(latest.get("senderUid") or "") in ("", "0"):
                continue  # tin của chính tài khoản — bỏ qua để không tự chuyển tiếp

            raw_text = _strip_content_label(latest.get("title"))
            scan_text = " ".join(filter(None, [raw_text, str(latest.get("href") or "")]))
            links = extract_product_links(scan_text)
            if not links:
                continue  # tin không có link Shopee/Lazada

            conversions = []
            new_text = raw_text
            ok_count = 0
            for item in links:
                conv = convert_product_link(item["url"], item["platform"], settings)
                conversions.append(conv)
                if conv.get("ok"):
                    ok_count += 1
                    if item["url"] in new_text:
                        new_text = new_text.replace(item["url"], conv["link"])
                    else:
                        # Link nằm ở href (link card) — nối vào cuối nội dung.
                        new_text = (new_text + "\n" + conv["link"]).strip()

            def _base_entry() -> dict:
                return {
                    "groupId": gid,
                    "groupName": gid,
                    "sender": str(latest.get("senderName") or latest.get("senderUid") or ""),
                    "textPreview": (new_text or raw_text)[:220],
                    "links": [
                        {"platform": c.get("platform"), "original": c.get("original"),
                         "aff": c.get("link", ""), "ok": bool(c.get("ok")),
                         "error": "" if c.get("ok") else str(c.get("message") or "")}
                        for c in conversions
                    ],
                    "msgId": msg_id,
                    "msgTs": msg_ts,
                }

            entries = []
            if ok_count == 0:
                errors += 1
                entry = _base_entry()
                entry.update({"status": "error",
                              "error": "Không chuyển được link nào: "
                                       + "; ".join(str(c.get("message") or "") for c in conversions)})
                entries.append(entry)
            else:
                # Tin thuộc bao nhiêu luồng thì gửi vào bấy nhiêu nhóm kết quả.
                for route in routes_by_source[gid]:
                    sent = _send_to_dest(settings, route["dest_group_id"], new_text,
                                         str(latest.get("thumb") or ""))
                    entry = _base_entry()
                    entry["destGroupId"] = route["dest_group_id"]
                    if sent.get("ok"):
                        forwarded += 1
                        entry.update({
                            "status": "sent" if ok_count == len(conversions) else "sent_partial",
                            "withPhoto": bool(sent.get("withPhoto")),
                            "error": "",
                        })
                    else:
                        errors += 1
                        entry.update({"status": "error", "withPhoto": False,
                                      "error": str(sent.get("message") or "Gửi vào nhóm kết quả thất bại")})
                    entries.append(entry)

            with _lock:
                d = _load_db()
                for entry in entries:
                    _append_log(d, entry)
                    if entry["status"].startswith("sent"):
                        d["stats"]["forwarded"] = int(d["stats"]["forwarded"]) + 1
                    else:
                        d["stats"]["errors"] = int(d["stats"]["errors"]) + 1
                _save_db(d)

            time.sleep(0.3)  # giãn nhẹ giữa các tin cho API Zalo

    with _lock:
        d = _load_db()
        d["state"]["lastMsgByGroup"] = last_by_group
        _save_db(d)

    baseline_note = f", {baseline_count} nhóm ghi mốc lần đầu" if baseline_count else ""
    message = (f"Đã kiểm tra {checked}/{len(source_ids)} nhóm nguồn của {len(routes)} luồng: "
               f"{new_messages} tin mới, chuyển tiếp {forwarded}, lỗi {errors}{baseline_note}.")
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
        if "shopee_aff_id" in patch:
            settings["shopee_aff_id"] = str(patch["shopee_aff_id"] or "").strip() or DEFAULT_SHOPEE_AFF_ID
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


@app.route("/api/test-convert", methods=["POST"])
def api_test_convert():
    """Thử chuyển 1 URL theo cấu hình hiện tại (kiểm tra aff id / cookie Lazada)."""
    data = request.get_json(silent=True) or {}
    url = str(data.get("url") or "").strip()
    if not url:
        return jsonify({"success": False, "error": "Chưa nhập URL sản phẩm."}), 400
    links = extract_product_links(url)
    if not links:
        return jsonify({"success": False, "error": "URL không phải link Shopee/Lazada."}), 400
    with _lock:
        settings = _load_db()["settings"]
    result = convert_product_link(links[0]["url"], links[0]["platform"], settings)
    status = 200 if result.get("ok") else 400
    return jsonify({"success": bool(result.get("ok")), **result}), status


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
