# Aff Forwarder Webhook (extension độc lập)

Extension chạy RIÊNG, không đụng vào code Nexus — dùng **Nexus.exe làm server API**.

## Cách hoạt động

0. Cấu hình theo **luồng**: chọn **nhóm kết quả trước**, rồi tick các **nhóm
   nguồn** cho luồng đó — mỗi nhóm kết quả nhận tin từ nhiều nhóm nguồn, và
   tạo được nhiều luồng (một nhóm nguồn có thể thuộc nhiều luồng).
1. Theo chu kỳ cấu hình, gọi `GET /api/messages/group-latest` của Nexus để kiểm
   tra tin nhắn mới nhất của từng nhóm nguồn (mỗi nhóm chỉ quét MỘT lần dù
   thuộc nhiều luồng).
2. Tin mới có link **Shopee/Lazada** thì chuyển thành link affiliate:
   - Shopee: dựng link `s.shopee.vn/an_redir` gắn affiliate id (mặc định `17340820046`).
   - Lazada: gọi `adsense.lazada.vn/newOffer/link-convert-v2.json` bằng **cookie
     dán trên dashboard** → nhận shortLink `https://s.lazada.vn/...`.
3. Gửi **nguyên nội dung + ảnh gốc, chỉ thay link** vào **nhóm kết quả của
   từng luồng** chứa nhóm nguồn đó, qua `POST /api/send-group-message` của Nexus.
4. Lần kiểm tra **đầu tiên** của mỗi nhóm chỉ ghi mốc tin mới nhất (baseline),
   KHÔNG chuyển tiếp tin cũ. Tin do chính tài khoản gửi bị bỏ qua (tránh vòng lặp).

## Chạy

```bat
:: 0. Cài thư viện (chỉ lần đầu):
pip install -r webhook\requirements.txt
:: 1. Mở Nexus.exe (server API tại http://127.0.0.1:5000), vào trang /policy đồng ý chính sách.
:: 2. Chạy webhook:
python webhook\aff_webhook.py
:: Tùy chọn:  --port 5001 (đổi cổng), --no-browser (không tự mở trình duyệt)
```

Dashboard **tự mở bằng Microsoft Edge** tại http://127.0.0.1:5001/ (không có Edge
thì mở bằng trình duyệt mặc định). Địa chỉ Nexus API fix cứng `http://127.0.0.1:5000`
— muốn đổi thì đặt biến môi trường `NEXUS_URL` trước khi chạy.

## Dashboard

- Công tắc bật/tắt theo dõi (mặc định tắt), nút "Chạy ngay".
- Chọn tài khoản Zalo, chu kỳ kiểm tra; mục "Luồng chuyển tiếp": thêm/xóa
  luồng, mỗi luồng chọn 1 nhóm kết quả + nhiều nhóm nguồn (có tìm kiếm).
- Ô nhập Shopee affiliate id; ô dán cookie Lazada; nút "Thử" chuyển 1 link để
  kiểm tra aff id / cookie.
- Thống kê (đã chuyển tiếp / lỗi / lần chạy cuối) + nhật ký chi tiết từng tin.

## API của webhook

| Endpoint | Mô tả |
|---|---|
| `GET  /health` | kiểm tra sống |
| `GET/PATCH /api/config` | đọc/sửa cấu hình |
| `POST /api/run` hoặc `POST /webhook/run` | chạy 1 lượt kiểm tra ngay (trigger ngoài) |
| `GET  /api/logs` | nhật ký + thống kê |
| `POST /api/test-convert {url}` | thử chuyển 1 link |

## Lưu ý

- Dữ liệu (cấu hình, mốc msgId, nhật ký) lưu ở `webhook/data/aff_webhook.json`.
- API `group-latest` chỉ trả **tin mới nhất** của nhóm: nếu giữa 2 lần kiểm tra
  có nhiều tin, chỉ tin cuối được xử lý — nhóm chạy nhanh nên đặt chu kỳ 1 phút.
- Ô cookie Lazada nhận cả 2 kiểu dán: chuỗi header `name=value; name2=value2`
  HOẶC copy nguyên bảng cookies từ DevTools (Application → Cookies → chọn hết
  → copy) — hệ thống tự ghép và bỏ cookie không thuộc domain lazada.
- Cookie Lazada hết hạn → log báo "dán cookie mới"; dán lại trên dashboard.
- Nexus phải được đồng ý chính sách (trang `/policy`) sau mỗi lần mở lại exe,
  nếu không webhook sẽ báo lỗi "Nexus yêu cầu đồng ý chính sách".
