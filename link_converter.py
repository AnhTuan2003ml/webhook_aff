"""Chuyển link sản phẩm Shopee/Lazada thành link affiliate (module độc lập).

- Shopee: dựng link redirect https://s.shopee.vn/an_redir?origin_link=...
  gắn ``affiliate_id`` — không cần đăng nhập, chỉ cần aff id.
- Lazada: gọi API link-convert-v2.json của adsense.lazada.vn bằng COOKIE
  người dùng dán trực tiếp trên dashboard (không đối soát, không subId);
  trả về link rút gọn https://s.lazada.vn/...
"""

import hashlib
import hmac
import json
import re
import time
from urllib.parse import quote, urlparse

import requests

# Bỏ qua system proxy (tool bắt gói đặt proxy toàn hệ thống làm SSL fail).
NO_PROXY = {"http": None, "https": None}

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36")

URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)

# Host sản phẩm + host link rút gọn của từng sàn.
_SHOPEE_HOST_RE = re.compile(r"(?:^|\.)(?:shopee\.vn|shope\.ee|shp\.ee)$", re.IGNORECASE)
_LAZADA_HOST_RE = re.compile(r"(?:^|\.)(?:lazada\.vn|lazada\.com\.vn)$", re.IGNORECASE)
_SHORT_HOSTS = {"s.shopee.vn", "shope.ee", "vn.shp.ee", "shp.ee", "s.lazada.vn", "s.lazada.com.vn"}

LAZADA_CONVERT_URL = "https://adsense.lazada.vn/newOffer/link-convert-v2.json"

# ─── Lazada Affiliate Open API (LiteApp) ─────────────────────────────────────
# Ký HMAC-SHA256 theo chuẩn Lazada Open Platform (lazop). Dùng App Key/Secret +
# User Token thay cho cookie -> ổn định, không hết hạn phiên.
LAZADA_API_BASE = "https://api.lazada.vn/rest"
LAZADA_GETLINK_PATH = "/marketing/getlink"


def _lazop_sign(secret: str, api_path: str, params: dict) -> str:
    """Chữ ký lazop: HMAC-SHA256(secret, apiPath + concat(sort(key+value))) -> HEX hoa."""
    base = api_path + "".join("{}{}".format(k, params[k]) for k in sorted(params))
    return hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest().upper()


def _deep_find(obj, key):
    """Tìm giá trị không rỗng đầu tiên của `key` trong dict/list lồng nhau."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key and v not in (None, "", "null"):
                return v
            found = _deep_find(v, key)
            if found not in (None, "", "null"):
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _deep_find(item, key)
            if found not in (None, "", "null"):
                return found
    return None


def lazada_aff_link_api(url: str, app_key: str, app_secret: str, user_token: str,
                        sub_id: str = "", timeout: int = 20) -> dict:
    """Chuyển URL sản phẩm Lazada sang link aff bằng Open API (LiteApp).

    Gọi /marketing/getlink với inputType=url; gắn subId1 để đối soát click/đơn.
    """
    app_key = str(app_key or "").strip()
    app_secret = str(app_secret or "").strip()
    user_token = str(user_token or "").strip()
    if not (app_key and app_secret and user_token):
        return {"ok": False, "message": "Chưa cấu hình Lazada App Key / Secret / User Token."}

    product_url = resolve_short_link(url)
    if classify_product_link(product_url) != "lazada":
        return {"ok": False, "message": f"Không phải link Lazada: {product_url[:120]}"}

    params = {
        "app_key": app_key,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "sha256",
        "userToken": user_token,
        "inputType": "url",
        "inputValue": product_url,
    }
    if sub_id:
        params["subId1"] = str(sub_id)
    params["sign"] = _lazop_sign(app_secret, LAZADA_GETLINK_PATH, params)

    try:
        response = requests.get(
            LAZADA_API_BASE + LAZADA_GETLINK_PATH,
            params=params, timeout=timeout, proxies=NO_PROXY,
            headers={"User-Agent": BROWSER_UA, "Accept": "application/json"},
        )
        payload = response.json()
    except Exception as exc:
        return {"ok": False, "message": f"Không gọi được Lazada Open API: {exc}"}

    # data đôi khi là chuỗi JSON -> parse lại để dò link.
    if isinstance(payload, dict) and isinstance(payload.get("data"), str):
        try:
            payload["data"] = json.loads(payload["data"])
        except Exception:
            pass

    code = str((payload or {}).get("code") or "0")
    if code not in ("0", ""):
        msg = str(payload.get("message") or payload.get("type") or payload.get("request_id") or "")
        return {"ok": False, "message": f"Lazada Open API lỗi (code={code}) {msg}".strip()}

    link = (_deep_find(payload, "regularPromotionLink")
            or _deep_find(payload, "mmPromotionLink")
            or _deep_find(payload, "dmPromotionLink"))
    if not link:
        err = _deep_find(payload, "errorMsg") or _deep_find(payload, "error_msg")
        return {"ok": False, "message": f"Lazada không trả về link: {err or json.dumps(payload)[:160]}"}

    link = str(link).strip()
    # Bảo đảm subId1 có trong link để đối soát (nếu API chưa gắn sẵn).
    if sub_id and "subId1=" not in link:
        link += ("&" if "?" in link else "?") + "subId1=" + quote(str(sub_id), safe="")
    return {"ok": True, "link": link, "productUrl": product_url}


def _clean_url(url: str) -> str:
    """Cắt dấu câu dính cuối URL khi tách từ văn bản tin nhắn."""
    return str(url or "").strip().rstrip(".,;:!?)]}>»\"'")


def normalize_lazada_cookie(raw: str) -> str:
    """Chuẩn hóa cookie Lazada người dùng dán vào thành chuỗi header ``a=b; c=d``.

    Chấp nhận 2 kiểu dán:
    - Chuỗi header Cookie sẵn: ``name=value; name2=value2`` → giữ nguyên.
    - Bảng cookies copy từ DevTools (mỗi dòng: name, value, domain, path,
      expires...) phân tách bằng tab hoặc nhiều khoảng trắng → tự ghép lại,
      chỉ giữ cookie thuộc domain lazada (bỏ .mmstat.com...), khử trùng tên.
    """
    raw = str(raw or "").strip()
    if not raw:
        return ""
    # Một dòng và đã có dạng k=v → coi là header sẵn.
    if "\n" not in raw and "=" in raw and "\t" not in raw:
        return raw

    pairs = []
    seen = set()
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # Dòng header sẵn lẫn trong nội dung dán.
        if "=" in line.split("\t")[0].split(" ")[0] and ";" in line:
            for item in line.split(";"):
                item = item.strip()
                if "=" in item:
                    name = item.split("=", 1)[0].strip()
                    if name and name not in seen:
                        seen.add(name)
                        pairs.append(item)
            continue
        tokens = line.split("\t") if "\t" in line else line.split(None, 2)
        tokens = [t.strip() for t in tokens if t.strip() != ""]
        if len(tokens) < 2:
            continue
        name, value = tokens[0], tokens[1]
        if not name or "=" in name:
            continue
        # Cột domain (nếu có): chỉ giữ cookie của lazada (bỏ .mmstat.com...).
        domain = tokens[2].split()[0].lower() if len(tokens) >= 3 and tokens[2] else ""
        if "." in domain and "lazada" not in domain:
            continue
        if name in seen:
            continue
        seen.add(name)
        pairs.append(f"{name}={value}")
    return "; ".join(pairs)


def classify_product_link(url: str) -> str:
    """Trả "shopee" / "lazada" / "" theo host của URL."""
    try:
        host = (urlparse(_clean_url(url)).hostname or "").lower()
    except Exception:
        return ""
    if not host:
        return ""
    if _SHOPEE_HOST_RE.search(host):
        return "shopee"
    if _LAZADA_HOST_RE.search(host):
        return "lazada"
    return ""


def extract_product_links(text: str) -> list:
    """Tách các link Shopee/Lazada trong nội dung tin nhắn (giữ thứ tự, khử trùng lặp).

    Returns: [{"url": ..., "platform": "shopee"|"lazada"}]
    """
    found = []
    seen = set()
    for raw in URL_RE.findall(str(text or "")):
        url = _clean_url(raw)
        platform = classify_product_link(url)
        if not platform or url in seen:
            continue
        seen.add(url)
        found.append({"url": url, "platform": platform})
    return found


def resolve_short_link(url: str, timeout: int = 15) -> str:
    """Đi theo redirect của link rút gọn (shope.ee, s.lazada.vn...) để lấy URL
    sản phẩm thật. Lỗi thì trả lại URL ban đầu."""
    url = _clean_url(url)
    try:
        host = (urlparse(url).hostname or "").lower()
        if host not in _SHORT_HOSTS:
            return url
        response = requests.get(
            url,
            headers={"User-Agent": BROWSER_UA},
            timeout=timeout,
            allow_redirects=True,
            proxies=NO_PROXY,
            stream=True,  # chỉ cần URL cuối, không tải body
        )
        final_url = _clean_url(response.url)
        response.close()
        return final_url or url
    except Exception:
        return url


def shopee_aff_link(url: str, aff_id: str, sub_id: str = "") -> dict:
    """Dựng link affiliate Shopee dạng an_redir từ URL sản phẩm.

    Link rút gọn (shope.ee, s.shopee.vn — thường là link aff của người khác)
    được resolve về URL sản phẩm thật trước để hoa hồng ghi nhận đúng aff_id.
    """
    aff_id = str(aff_id or "").strip()
    if not aff_id:
        return {"ok": False, "message": "Chưa cấu hình Shopee affiliate id."}
    product_url = resolve_short_link(url)
    if classify_product_link(product_url) != "shopee":
        return {"ok": False, "message": f"Không phải link Shopee: {product_url[:120]}"}
    # Bỏ query/fragment (utm, sp_atk... của người đăng) — giữ đường dẫn sản phẩm.
    parsed = urlparse(product_url)
    origin = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    link = (
        "https://s.shopee.vn/an_redir?origin_link=" + quote(origin, safe="")
        + "&affiliate_id=" + quote(aff_id, safe="")
        + ("&sub_id=" + quote(str(sub_id), safe="") if sub_id else "")
    )
    return {"ok": True, "link": link, "productUrl": origin}


def lazada_aff_link(url: str, cookie: str, timeout: int = 20) -> dict:
    """Chuyển URL sản phẩm Lazada sang link aff bằng cookie adsense.lazada.vn.

    Gọi đúng 1 API POST /newOffer/link-convert-v2.json với header Cookie dán
    trực tiếp; trả về data.shortLink (https://s.lazada.vn/...).
    """
    cookie = str(cookie or "").strip()
    if not cookie:
        return {"ok": False, "message": "Chưa dán cookie Lazada (adsense.lazada.vn) trên dashboard."}
    product_url = resolve_short_link(url)
    if classify_product_link(product_url) != "lazada":
        return {"ok": False, "message": f"Không phải link Lazada: {product_url[:120]}"}

    try:
        response = requests.post(
            LAZADA_CONVERT_URL,
            headers={
                "content-type": "application/json",
                "accept": "application/json, text/plain, */*",
                "origin": "https://adsense.lazada.vn",
                "referer": "https://adsense.lazada.vn/index.htm",
                "user-agent": BROWSER_UA,
                "cookie": cookie,
            },
            data=json.dumps({"jumpUrl": product_url, "subIdTemplateKey": ""}),
            timeout=timeout,
            proxies=NO_PROXY,
        )
    except Exception as exc:
        return {"ok": False, "message": f"Không gọi được API Lazada: {exc}"}

    try:
        payload = response.json()
    except Exception:
        # Trả HTML trang login/captcha = phiên hết hạn.
        return {"ok": False, "message": "Cookie Lazada đã hết hạn hoặc không hợp lệ — hãy dán cookie mới.",
                "cookieExpired": True}

    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    if payload.get("success") is not True or payload.get("resultCode") != 1:
        message = str(data.get("message") or payload.get("message")
                      or payload.get("errorMsg") or f"resultCode={payload.get('resultCode')}")
        return {"ok": False, "message": f"Lazada từ chối chuyển link: {message}"}

    short_link = str(data.get("shortLink") or "").strip()
    if not short_link.startswith("https://s.lazada.vn/"):
        return {"ok": False, "message": f"shortLink trả về không hợp lệ: {short_link[:120]}"}
    return {"ok": True, "link": short_link, "productUrl": product_url}


def convert_product_link(url: str, platform: str, settings: dict) -> dict:
    """Chuyển 1 link theo sàn tương ứng, dùng cấu hình trong ``settings``.

    - Shopee: gắn ``sub_id`` CỐ ĐỊNH để đối soát click/đơn qua hệ thống.
    - Lazada: ƯU TIÊN Open API (LiteApp: App Key/Secret/User Token) + subId1;
      nếu thiếu cấu hình API hoặc API lỗi mà còn cookie thì dùng cookie làm fallback.
    """
    settings = settings or {}
    sub_id = str(settings.get("sub_id") or "").strip()
    if platform == "shopee":
        result = shopee_aff_link(url, settings.get("shopee_aff_id", ""), sub_id)
    elif platform == "lazada":
        app_key = str(settings.get("lazada_app_key") or "").strip()
        app_secret = str(settings.get("lazada_app_secret") or "").strip()
        user_token = str(settings.get("lazada_user_token") or "").strip()
        cookie = str(settings.get("lazada_cookie") or "").strip()
        if app_key and app_secret and user_token:
            result = lazada_aff_link_api(url, app_key, app_secret, user_token, sub_id)
            if not result.get("ok") and cookie:
                fallback = lazada_aff_link(url, cookie)  # dự phòng khi API lỗi
                if fallback.get("ok"):
                    result = fallback
        else:
            result = lazada_aff_link(url, cookie)
    else:
        result = {"ok": False, "message": "Sàn không được hỗ trợ."}
    result["platform"] = platform
    result["original"] = _clean_url(url)
    return result
