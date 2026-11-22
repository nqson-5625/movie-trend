# Module 08 - Kibana

Module 08 triển khai Kibana **8.15.3** trong namespace Kubernetes `bigdata`
(có thể đổi bằng `NAMESPACE=...`), cùng Elasticsearch 8.15.3 trong chính
namespace đó. Service chỉ là `ClusterIP`; truy cập dev qua
`kubectl port-forward`, không công khai endpoint trên host/network.

Phạm vi hạ tầng ở đây là deployment, health probes, Spaces và bảy Data Views
canonical theo [runtime contract](../../contracts/phase2/common/phase2_runtime_config.json).
Dashboard D1-D5, saved objects, Lens panels và metric thuộc các owner lane, chưa
được tạo bởi module này.

## 1. Bí mật và quyền truy cập

Trước `make up`, cần có Secret `kibana-credentials` trong namespace `bigdata`
với các key:

| Key | Dùng cho |
|---|---|
| `kibana-system-password` | Mật khẩu reserved user `kibana_system` trong Elasticsearch. |
| `security-encryption-key` | `xpack.security.encryptionKey`, ít nhất 32 ký tự. |
| `saved-objects-encryption-key` | Mã hóa saved objects, ít nhất 32 ký tự. |
| `reporting-encryption-key` | Mã hóa reporting, ít nhất 32 ký tự. |

Tạo/đồng bộ password `kibana_system` bằng quy trình quản lý secret của cluster,
rồi nạp cùng giá trị vào Secret. Tạo ba encryption key độc lập và lưu chúng trong
secret manager/Kubernetes Secret. Không commit Secret, giá trị thật hoặc file
local chứa credentials. `make check-secret` kiểm tra đủ key, độ dài và việc ba
encryption key khác nhau mà không in giá trị. Deployment sẽ không hoạt động nếu
password này chưa được đồng bộ với reserved user trong Elasticsearch.

`make -C modules/elasticsearch init-roles` cài hai role liên quan:

* `movie_trend_kibana_analytics_read`: chỉ đọc metadata/document trên sáu
  analytics aliases và chỉ có quyền Kibana read trong Space `integration`.
* `movie_trend_kibana_integrator`: không có Elasticsearch index quyền; có thể
  sửa Dashboard/Visualize và quản lý saved objects trong Space `integration`;
  không nhận base privilege `all` cho các tính năng Kibana khác.

Gán role tương ứng cho user qua quy trình cấp quyền hiện có. User P1 cần cả hai
role để vừa quản lý saved objects vừa đọc analytics. Không dùng `kibana_system`
để đăng nhập UI. Lệnh bootstrap cần tài khoản quản trị ES/Kibana chỉ trong lúc
tạo Spaces/Data Views.

## 2. Triển khai

Cài dependency Python, tạo namespace `bigdata` và Elasticsearch trước, sau đó:

```bash
python -m pip install -r modules/kibana/requirements.txt
make -C modules/kibana validate
make -C modules/kibana check-secret
make -C modules/kibana up
make -C modules/kibana status
make -C modules/kibana forward
```

Port-forward chạy ở terminal riêng; mở `http://127.0.0.1:5601`. Kiểm tra log
bằng `make -C modules/kibana logs`. `make up` luôn rollout pod sau khi apply để
Kibana nhận thay đổi từ ConfigMap được mount bằng `subPath`. `make down` chỉ xóa
ConfigMap, Service và Deployment Kibana; không đụng Elasticsearch hay dữ liệu.

## 3. Khởi tạo Spaces và Data Views

Sau khi port-forward ES 9200 và Kibana 5601, cấu hình `.env` local với
`KIBANA_HTTP_URL=http://127.0.0.1:5601` cùng
`KIBANA_USERNAME`/`KIBANA_PASSWORD` của tài khoản quản trị. Nếu hai biến
Kibana không có, bootstrap fallback sang `ES_USERNAME`/`ES_PASSWORD` để tương
thích môi trường dev hiện tại. Sau đó chạy:

```bash
make -C modules/kibana bootstrap
```

Bootstrap tạo `dev` và `integration`, sau đó tạo bảy Data Views trong
`integration`. ID, alias và time field lấy trực tiếp từ runtime contract; time
field null được giữ ở trạng thái không chọn. Bootstrap kiểm Kibana đang
`available` và đúng version trong contract trước khi ghi. Data Views bật
`allowNoIndex` để đội dashboard có thể dựng saved objects trước lần publish
analytics đầu tiên. Lệnh có thể chạy lại; nếu Space hay Data View hiện có khác
contract, lệnh fail và không tự ghi đè. Data Views luôn trỏ exact canonical
aliases, không dùng dev/fixture/physical index patterns.

## 4. Kiểm thử và trạng thái

```bash
make -C modules/kibana test
```

Các test offline kiểm tra inventory/time fields, chặn alias ngoài contract và
quyền role theo Space. Chạy được test không phải bằng chứng rằng Kubernetes,
Kibana API, Elasticsearch aliases hay dashboard đã được nghiệm thu. Cần xác minh
deployment Ready, login bằng user tương ứng, sáu aliases searchable và
Data Views hiển thị đúng trước khi nhận gate G0.
