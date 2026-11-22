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

### 1.1. Điều kiện trước khi tạo Secret

Elasticsearch phải đang chạy và tài khoản quản trị trong `.env` phải đăng nhập
được. Từ thư mục gốc repository, thiết lập kubeconfig và kiểm tra pod:

```bash
cd /home/fpt/movie-trend
export KUBECONFIG=/home/fpt/.kube/config (tùy từng máy local)
kubectl get pod -n bigdata elasticsearch-0
```

Nếu chưa có port-forward Elasticsearch, giữ lệnh sau chạy ở một terminal riêng:

```bash
make -C modules/elasticsearch forward
```

Ở terminal thao tác, nạp `.env` và kiểm tra ba biến bắt buộc. Các lệnh kiểm tra
chỉ báo biến thiếu, không in credential:

```bash
set -a
source .env
set +a
: "${ES_HTTP_URL:?Thiếu ES_HTTP_URL trong .env}"
: "${ES_USERNAME:?Thiếu ES_USERNAME trong .env}"
: "${ES_PASSWORD:?Thiếu ES_PASSWORD trong .env}"

curl --fail --silent --show-error \
  --user "$ES_USERNAME:$ES_PASSWORD" \
  "$ES_HTTP_URL/_security/_authenticate"
```

`ES_USERNAME` ở bước này phải là tài khoản có quyền đổi mật khẩu reserved user,
ví dụ tài khoản quản trị `elastic`. Không dùng `kibana_system` làm tài khoản quản
trị hoặc tài khoản đăng nhập giao diện Kibana.

### 1.2. Sinh password và ba encryption key

Đặt `umask` chặt, sau đó sinh bốn giá trị độc lập. Dùng chuỗi hex để các giá trị
không chứa newline hoặc ký tự cần escape khi gọi API:

```bash
umask 077
export KIBANA_SYSTEM_PASSWORD="$(openssl rand -hex 24)"
export KIBANA_SECURITY_ENCRYPTION_KEY="$(openssl rand -hex 32)"
export KIBANA_SAVED_OBJECTS_ENCRYPTION_KEY="$(openssl rand -hex 32)"
export KIBANA_REPORTING_ENCRYPTION_KEY="$(openssl rand -hex 32)"
```

Không dùng cùng một giá trị cho ba encryption key. Không chạy `echo` các biến,
không đưa giá trị thật vào README, manifest Kubernetes hoặc Git.

### 1.3. Đồng bộ password với user `kibana_system` trong Elasticsearch

Đổi password reserved user bằng Security API. Vì password ở trên chỉ gồm ký tự
hex nên có thể đưa an toàn vào JSON mà không cần escape bổ sung:

```bash
set -a
. ./.env
set +a
printf '%s\n' "$ES_HTTP_URL"

curl --fail --silent --show-error \
  --user "$ES_USERNAME:$ES_PASSWORD" \
  --request POST \
  --header 'Content-Type: application/json' \
  --data-binary "{\"password\":\"$KIBANA_SYSTEM_PASSWORD\"}" \
  "$ES_HTTP_URL/_security/user/kibana_system/_password"
```

API thành công trả về `{}`. Xác minh ngay bằng chính credential vừa đặt:

```bash
curl --fail --silent --show-error \
  --user "kibana_system:$KIBANA_SYSTEM_PASSWORD" \
  "$ES_HTTP_URL/_security/_authenticate"
```

Kết quả phải có `username` là `kibana_system`. Chỉ tiếp tục tạo Kubernetes
Secret khi cả lệnh đổi password và lệnh xác minh đều thành công.

### 1.4. Tạo hoặc cập nhật Secret `kibana-credentials`

Lệnh dưới đây idempotent: lần đầu sẽ tạo Secret, các lần sau sẽ cập nhật Secret
hiện có. Giá trị được lấy từ biến môi trường và không được ghi vào file YAML:

```bash
kubectl create secret generic kibana-credentials \
  --namespace bigdata \
  --from-literal=kibana-system-password="$KIBANA_SYSTEM_PASSWORD" \
  --from-literal=security-encryption-key="$KIBANA_SECURITY_ENCRYPTION_KEY" \
  --from-literal=saved-objects-encryption-key="$KIBANA_SAVED_OBJECTS_ENCRYPTION_KEY" \
  --from-literal=reporting-encryption-key="$KIBANA_REPORTING_ENCRYPTION_KEY" \
  --dry-run=client \
  --output yaml | kubectl apply -f -
```

Không dùng `kubectl get secret ... -o yaml` trong log CI vì trường `data` dù được
base64 vẫn là credential có thể giải mã. Kiểm tra cấu trúc bằng validator của
module; validator không in giá trị:

```bash
make -C modules/kibana check-secret
```

Kết quả hợp lệ liệt kê bốn tên key:

```text
kibana-system-password
security-encryption-key
saved-objects-encryption-key
reporting-encryption-key
```

Sau khi Secret hợp lệ, triển khai Kibana:

```bash
make -C modules/kibana up
make -C modules/kibana status
```

`make up` thực hiện rollout lại Deployment, nên pod mới sẽ nhận các giá trị mới
từ Secret. Khi hoàn tất, xóa credential tạm khỏi môi trường shell:

```bash
unset KIBANA_SYSTEM_PASSWORD
unset KIBANA_SECURITY_ENCRYPTION_KEY
unset KIBANA_SAVED_OBJECTS_ENCRYPTION_KEY
unset KIBANA_REPORTING_ENCRYPTION_KEY
```

### 1.5. Xoay vòng credential

Khi đổi `kibana_system` password, phải thực hiện theo đúng thứ tự: sinh password
mới, gọi API Elasticsearch, cập nhật `kibana-credentials`, chạy `make up` để
rollout Kibana, rồi kiểm tra log. Nếu chỉ đổi một phía, Kibana sẽ báo lỗi xác
thực với Elasticsearch và không đạt readiness.

Ba encryption key cần được giữ ổn định qua các lần restart. Thay
`saved-objects-encryption-key` hoặc `reporting-encryption-key` có thể khiến dữ
liệu đã mã hóa bằng key cũ không đọc được; chỉ xoay các key này theo kế hoạch
migration/rotation có lưu giữ key cũ, không sinh lại chúng trong mỗi lần `make
up`.

Nếu Kibana không Ready sau khi cập nhật Secret, kiểm tra:

```bash
make -C modules/kibana check-secret
make -C modules/kibana logs
kubectl describe pod -n bigdata -l app=kibana
```

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
