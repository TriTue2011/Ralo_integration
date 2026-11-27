# 🏠 Ralo (Zalo Call) — tích hợp Home Assistant

> **⚠️ MIỄN TRỪ TRÁCH NHIỆM — ĐỌC TRƯỚC KHI DÙNG**
>
> Kho này **chỉ phục vụ mục đích học tập và nghiên cứu cá nhân, phi thương mại**. Đây là dự án **không chính thức, không
> liên kết, không được Zalo / VNG tài trợ hay xác nhận** dưới bất kỳ hình thức nào.
>
> Khi tải, dựng hoặc sử dụng mã nguồn trong kho này, **người dùng tự chịu trách nhiệm** tuân thủ **Điều khoản dịch vụ của
> Zalo** (zalo.vn) và **pháp luật hiện hành tại nơi cư trú**. Điều khoản dịch vụ của Zalo, mục *Hành vi vi phạm*, cấm người
> dùng (không giới hạn): sao chép/sửa đổi/tạo bản phái sinh của Dịch Vụ khi chưa được đồng ý; thiết kế lại, biên dịch,
> tháo gỡ, đảo ngược thiết kế Dịch Vụ; can thiệp, thay đổi, làm gián đoạn cơ chế hoạt động nguyên bản của Dịch Vụ. **Việc
> dùng kho này để làm những việc Điều khoản dịch vụ của Zalo cấm là lựa chọn và rủi ro của riêng người dùng** — tác giả
> không khuyến khích, không chịu trách nhiệm cho hậu quả đó.
>
> Tác giả **không chịu trách nhiệm** với bất kỳ thiệt hại, tranh chấp, hay hậu quả pháp lý nào phát sinh từ việc bên thứ ba
> tải về, sửa đổi, triển khai hoặc dùng mã nguồn này sai mục đích, kể cả khi dùng để vi phạm Điều khoản dịch vụ của Zalo
> hoặc pháp luật. Kho này được cung cấp **"nguyên trạng" (as-is)**, không bảo đảm dưới bất kỳ hình thức nào.

Gọi **thoại và video Zalo** từ Home Assistant: điện thoại đổ chuông, nghe máy xong tiếng bên kia ra **loa trong nhà**
(R1, loa camera, `media_player`), mic loa/camera gửi sang điện thoại, video phát hình từ **camera nhà**. Nhắn tin, gửi ảnh/tệp/ghi âm,
quy tắc trả lời tự động, blueprint.

> ⚠️ App không chính thức, đăng nhập Zalo bằng mã QR. **Dùng tài khoản Zalo phụ.**

Kho này chỉ chứa **phần tích hợp Home Assistant** (`custom_components/zalo_call`), cài được bằng HACS. Tích hợp cần **máy chủ Zalo Call**
(app đăng nhập QR + gọi thoại/video) chạy cạnh Home Assistant. Mã máy chủ để riêng; bạn cài máy chủ bằng add-on hoặc Docker như bên dưới.

## 1. Cài máy chủ Zalo Call (làm trước)

| Cách | Dùng khi | Hướng dẫn |
|---|---|---|
| **Add-on Home Assistant** | Home Assistant OS / Supervised | Thêm kho add-on `https://github.com/TriTue2011/has-addons`, cài **Zalo Calling**: [hướng dẫn](https://github.com/TriTue2011/has-addons/blob/main/zalo_calling/DOCS.md) |
| **Docker / Portainer** | Docker, Container, Core | Image khởi động công khai `ghcr.io/tritue2011/zalo-calling-loader:latest` (có giao diện) hoặc `:noui` |

Máy chủ tải mã khi khởi động bằng **token GitHub chỉ đọc** (do chủ kho cấp: quyền *Contents: Read-only* trên kho mã máy chủ). Lần đầu cần token;
hết hạn token thì máy chủ vẫn chạy bản đã tải kèm cảnh báo. Cổng mặc định **8080**, dữ liệu ở `/data`.

## 2. Thêm tích hợp (HACS)

[![Mở trong HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=TriTue2011&repository=zalo_call&category=integration)

1. **HACS → ⋮ → Custom repositories** → dán `https://github.com/TriTue2011/zalo_call`, **Type = Integration** → **Add**.
2. Tìm **Ralo** trong HACS → **Download** → **khởi động lại Home Assistant**.
3. Không dùng HACS: tải `zalo_call.zip` ở [Releases](https://github.com/TriTue2011/zalo_call/releases/latest), giải nén vào `custom_components/zalo_call` của HA, khởi động lại.
4. Cập nhật: HACS báo khi có bản mới. Cần Home Assistant ≥ 2024.11.

## 3. Kết nối với máy chủ, đăng nhập Zalo

[![Thêm tích hợp](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=zalo_call)

**Cài đặt → Thiết bị & Dịch vụ → Thêm tích hợp → Zalo Call**, điền:

| Ô | Giá trị |
|---|---|
| Địa chỉ app | `http://<IP máy chạy app>:8080`. Cùng mạng Docker thì dùng tên container: `http://zalo-calling:8080` |
| Tên đăng nhập / Mật khẩu | Cặp đặt ở biến môi trường của máy chủ, hoặc cặp máy chủ **tự sinh** (xem nhật ký container, tìm `TU SINH`) |

Báo «không gọi được tới app» → kiểm tra địa chỉ, cổng 8080 và container đang chạy; «sai tên đăng nhập hoặc mật khẩu» → xem lại cặp ở trên.
Tích hợp tự lo webhook sang Home Assistant. Sau khi kết nối có sẵn: cảm biến **Trạng thái Zalo**, ảnh **Mã QR đăng nhập**, nút đăng nhập/đăng xuất, cảm biến **Cuộc gọi** và **Sức khoẻ tài khoản**.

**Đăng nhập Zalo (quét QR):** mở `http://<IP>:8080` → chọn **Qua PC** (gọi thoại/video) hoặc **Qua Web** (chỉ nhắn tin) → quét QR bằng Zalo trên điện thoại.
Hoặc từ Home Assistant: dịch vụ `zalo_call.login_qr` (`type: pc` / `web`) rồi mở ảnh **Mã QR đăng nhập** và quét. Đăng xuất: `zalo_call.logout`.

**Chọn người nhận:** Cấu hình của tích hợp → «Người và nhóm nhận»: chọn từ danh bạ Zalo, mỗi người thành thực thể `notify.zalo_<tên>`.

## 4. Dịch vụ và blueprint

Mọi dịch vụ `zalo_call.*` (gọi, nhắn tin, ảnh/tệp, quy tắc, AI, cài đặt app…) hiện đủ mô tả và ô nhập ở **Công cụ nhà phát triển → Hành động**.
Blueprint mẫu ở kho [Blueprint](https://github.com/TriTue2011/Blueprint) của cùng tác giả.

## Báo lỗi

[Issues](https://github.com/TriTue2011/zalo_call/issues). Khi báo lỗi kèm phiên bản tích hợp, phiên bản máy chủ và đoạn nhật ký (che mật khẩu/token).

## Giấy phép

Phần tích hợp Home Assistant trong kho này (`custom_components/zalo_call`) phát hành theo **GNU AGPL-3.0** (xem [LICENSE](LICENSE)). Bản quyền © 2026 TriTue2011.
Ai sửa và phân phối, hoặc cho người khác dùng qua mạng, phải công khai mã đã sửa theo cùng giấy phép.
**Máy chủ Zalo Call** (app đăng nhập QR, gọi thoại/video) là phần mềm riêng, không thuộc giấy phép này.
