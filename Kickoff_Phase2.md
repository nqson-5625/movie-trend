# KICKOFF PHASE 2 - Elasticsearch, Spark SQL và Kibana

**Dự án:** Movie Trend - phân tích xu hướng tương tác với phim  
**Ngày cập nhật kiến trúc:** 21/11/2026

**Trạng thái:** Contract và phân công đã chuyển sang Kibana; code Module 06-08 chưa được xem là đã triển khai

**Hợp đồng kỹ thuật đi kèm:** [DATA_CONTRACT_Phase2.md](DATA_CONTRACT_Phase2.md), bản Phase2-3.0

**Website tham khảo:** [What's Seattle Reading?](https://www.whats-seattle-reading.com/)

## 1. Mục tiêu, kiến trúc và trạng thái đầu vào

Phase 2 hoàn thiện **Module 06 Elasticsearch**, **Module 07 Spark SQL** và **Module 08 Kibana** theo luồng:

~~~text
upstream outputs/Kafka
  -> 6 Elasticsearch source aliases
  -> Spark SQL chuẩn hóa metric
  -> 6 analytics alias families
  -> Kibana Data Views
  -> Lens panels/controls
  -> D1-D5
~~~

Kibana chỉ đọc analytics aliases bằng role read-only. Lens được phép lọc, sắp xếp và dùng phép tổng hợp đã ghi trong contract; không tính lại weighted average, `trending_score`, latest/previous hoặc quy tắc cutoff. Auto-refresh chỉ làm Kibana truy vấn lại Elasticsearch sau mỗi khoảng cấu hình; đây không phải cơ chế push và không cam kết độ trễ bằng 0.

Repository hiện có Module 01-05, dữ liệu/profile và code Spark Streaming. Chưa thấy thư mục triển khai `modules/elasticsearch/`, `modules/spark_sql/`, `modules/kibana/` hoặc bằng chứng end-to-end của các analytics aliases. Vì vậy mọi gate Module 06-08 vẫn là **chưa nghiệm thu** cho đến khi có artefact, test và biên bản tương ứng.

| Nhánh | Output đã đối chiếu | Ý nghĩa và giới hạn |
|---|---|---|
| Batch | `movie_stats.tsv` 9.724 dòng; `movie_daily_stats.tsv` 99.538; `genre_daily_stats.tsv` 32.894 | Run `20261024T104145Z`, cutoff T = `2018-10-01T00:00:00Z`; 100.836 rating duy nhất và 274.480 lượt gắn genre. |
| MovieLens Streaming | `movie_realtime_stats.jsonl` 201.627; `genre_realtime.jsonl` 127.770 | Cửa sổ 10m/1h theo event time; dữ liệu hiện có đều trước T, là replay lịch sử chứ không phải mức quan tâm hiện tại. |
| TMDB Streaming | `tmdb_realtime.jsonl` 20 dòng | Một lần quan sát tại `2026-11-12T06:02:43Z`; title không có trong output, delta null, 20 ID không khớp reference đính kèm. Đây chỉ là baseline. |

Một rating MovieLens được gọi là **một lượt tương tác/rating**, không phải lượt xem. Không cộng hai kích thước cửa sổ, không cộng Batch với replay đang trùng timeline, không cộng popularity TMDB với rating MovieLens. Chín truy vấn nghiệp vụ tối thiểu của Module 07 vẫn được giữ nguyên theo mục 8.1 của contract.

## 2. Năm dashboard và quyết định phục vụ dữ liệu

| ID, owner | Câu hỏi nghiệp vụ và panel bắt buộc | Analytics/Data View | Quy tắc không được vi phạm |
|---|---|---|---|
| **D1 - Tổng quan lịch sử**; P1 Quang Sơn | KPI tổng rating, số phim, điểm TB; timeline; Top 10 all/year/month; run/T | `analytics_timeseries`, `analytics_top_movies`; `dv_d1_timeseries`, `dv_d1_top_movies`, đều không chọn time field mặc định | Hàng all có `period_start=null` không bị global time filter loại; Top 10 dùng đúng một `period_type/period_start`. |
| **D2 - Xu hướng từng phim**; P2 Đỗ Sơn | Chọn 1-3 phim; line day/month/year; tooltip count/avg | `analytics_timeseries`; `dv_d2_movie_timeseries`, time field `period_start` | Luôn `entity_type=movie`, đúng một `period_type`; điểm TB đã do Spark SQL tính từ sum/count, không average-of-averages. |
| **D3 - Thể loại qua thời gian**; P3 Giang | Timeline, Top, heatmap, filter genre/kỳ | `analytics_genre_trends`; `dv_d3_genre_trends`, time field `period_start` | Đúng một `period_type`; phim đa thể loại được tính ở mỗi genre nên tổng genre không phải tổng hệ thống. |
| **D4 - Biến động cửa sổ ngắn**; P4 Hiếu | Hai vùng movie/genre; current, previous, score; chọn 10m hoặc 1h; liên kết tra cứu lịch sử | `analytics_growth_current` cho KPI/rank và `analytics_growth` cho lịch sử; `dv_d4_growth_current` không có time field, `dv_d4_growth_history` dùng `window_start` | Current alias chỉ chứa cửa sổ hoàn chỉnh mới nhất của từng size; filter đúng một `window_size`. Không dùng “Last 15 minutes” trên replay. |
| **D5 - Tín hiệu TMDB**; P5 Thắng | Số movie states hiện có; Top popularity; scatter popularity × vote_average, size vote_count; event/update time; delta khi đủ dữ liệu | `analytics_tmdb_signal`; `dv_d5_tmdb_signal`, không có time field mặc định | Đây là **latest-per-tmdbId**, không phải một crawl snapshot toàn cục. Không nối `tmdbId` với `movieId`; title null hiện ID; delta null không đổi thành 0. |

Website mẫu chỉ truyền cảm hứng về tìm kiếm, so sánh, xếp hạng và thể loại. Không sao chép dữ liệu/ý nghĩa sách. “Phim tăng trở lại” ở D4 chỉ là biến động hai cửa sổ liền kề, không phải kết luận phim cũ nổi lại nhiều năm.

### 2.1. D4 - current snapshot xác định được và lịch sử riêng

`analytics_growth` tiếp tục là lịch sử, grain `(window_size, window_start, entity_type, entity_key)`. P4 bổ sung materialization `analytics_growth_current-v<snapshot_version>` và alias read-only `analytics_growth_current`:

1. Với mỗi `window_size`, chọn `selected_window_start = max(window_start)` thỏa `window_end <= source_complete_through`; mốc complete phải lấy từ replay EOF hoặc checkpoint/watermark đã công bố, không lấy `updated_at` hay thời gian máy.
2. Cùng một lần build phải lấy **toàn bộ** movie và genre rows của đúng `selected_window_start` cho cả 10m và 1h. Grain current là `(snapshot_version, window_size, entity_type, entity_key)`; `_id` trong physical index là `window_size|entity_type|entity_key`.
3. P4 ghi physical index mới, kiểm schema/count/khóa, exact previous window và marker; P5 chỉ chạy lệnh atomic alias swap sau READY. Một size lỗi thì không đổi alias. Rollback trỏ alias về physical version trước.
4. Auto-refresh 10 giây được đề xuất cho môi trường thử nghiệm, không phải SLA. Vì Data View current không có time field, replay cũ vẫn hiển thị; Data View history dùng absolute time range khi tra cứu.

Không dùng field `is_latest_window`: cách đó đòi cập nhật bỏ cờ trên tài liệu cũ và có thể để lại hai trạng thái latest khi ghi dở.

### 2.2. D5 - latest theo từng phim, không giả snapshot crawl hoàn chỉnh

`tmdb_realtime` và `analytics_tmdb_signal` có grain `tmdbId`; `event_time` quyết định bản mới nhất của riêng ID đó. D5 gọi KPI là **số phim có trạng thái latest đang lưu**, đồng thời hiển thị `min(event_time)`, `max(event_time)` và `max(published_at)` để người xem biết độ lệch quan sát. Không lọc cả tập theo `max(event_time)` và không gọi nó là “số phim trong snapshot mới nhất”.

Muốn nâng cấp thành snapshot toàn cục phải có change request riêng với `crawl_run_id`, expected/actual count, điều kiện crawl complete, physical snapshot bất biến và atomic publish. Phase2-3.0 không giả định các cơ chế đó đã tồn tại.

## 3. Phân công theo luồng

| Người | Luồng sở hữu | Source aliases | Analytics aliases | Dashboard và phần dùng chung |
|---|---|---|---|---|
| **P1 - D1** | Đọc analytics Batch phim của P2 | Không ghi source | Không ghi analytics | Chuẩn giao diện, D1, điều hướng, review và tích hợp saved objects D1-D5. |
| **P2 - D2** | Batch phim → SQL phim | `movie_batch_stats`, `movie_daily_stats` | `analytics_timeseries`, `analytics_top_movies` | D2; cấp dữ liệu D1; query 1,2,4-6; phối hợp query 8. |
| **P3 - D3** | Batch genre → SQL genre | `genre_trends` | `analytics_genre_trends` | D3 và query 3. |
| **P4 - D4** | MovieLens RT/replay → growth/history/current | `movie_realtime_stats`, `genre_realtime` | `analytics_growth`, `analytics_growth_current` | D4, query 7/8, current snapshot publisher input và restart tests. |
| **P5 - D5 + platform** | TMDB periodic → latest-per-movie | `tmdb_realtime` | `analytics_tmdb_signal` | D5/query 9; ES + Spark SQL + Kibana tương thích, roles, spaces, Data Views dùng chung và alias publisher. |

Có **6 source alias families** và **6 analytics alias families**. `analytics_growth_current` là serving projection của lịch sử `analytics_growth`, nhưng vẫn là một index family/alias riêng cần mapping, quyền, fixture và gate. Mỗi output có một writer logic; P5 vận hành hạ tầng/publisher nhưng không thay metric của P2-P4.

### 3.1. Giao diện dùng chung P5 phải bàn giao sớm

| Nền | P5 cung cấp | Domain owner tự làm |
|---|---|---|
| Elasticsearch | Version, endpoint dev/test, templates, role theo least privilege, health/log/Bulk checks, namespace fixture và alias publish/rollback | Mapping/loader/query của lane, `_id`, đối soát và READY report. |
| Spark SQL | Runtime/connector tương thích ES, UTC, sáu temp views, writer/helper, retry và logs | SQL/DataFrame, metric, fixture và tests của lane. |
| Kibana | Version tương thích ES, Space dev/integration, role read-only analytics, API import/export, Data View IDs ổn định, refresh setting thử nghiệm | P1 chuẩn UI; P2-P5 Lens panels/dashboard của mình; không đổi contract bằng formula Lens. |
| Publish | Atomic publish cho sáu Batch aliases và riêng `analytics_growth_current`; publication marker có version/timestamps | Owner giao physical index hoàn chỉnh; không trỏ Data View vào physical version. |

P1 không thể dựng dashboard có dữ liệu trước khi P5 có ES/Kibana dev và P2-P5 nạp fixture vào dev indices. Trước thời điểm đó P1 chỉ soạn style guide, wireframe và checklist field.

### 3.2. Cây thư mục Phase 2 đã chốt

Các thư mục dưới đây là cấu trúc phải tạo khi triển khai; việc ghi vào tài liệu không chứng minh chúng đã tồn tại.

~~~text
movie-trend/
├── Kickoff_Phase2.md
├── DATA_CONTRACT_Phase2.md
├── contracts/phase2/
│   ├── common/                              [P5: aliases, publication, shared fixtures]
│   ├── movie_batch/                         [P2]
│   ├── genre_batch/                         [P3]
│   ├── movielens_realtime/                  [P4: growth history + current]
│   └── tmdb_realtime/                       [P5]
├── modules/
│   ├── elasticsearch/                       [Module 06]
│   │   ├── common/{config,templates,roles,publication}/
│   │   ├── lanes/{movie_batch,genre_batch,movielens_realtime,tmdb_realtime}/
│   │   └── tests/{common,lanes}/
│   ├── spark_sql/                           [Module 07]
│   │   ├── common/{runtime,readers,writers}/
│   │   ├── lanes/{movie_batch,genre_batch,movielens_realtime,tmdb_realtime}/
│   │   └── tests/{common,lanes}/
│   └── kibana/                              [Module 08]
│       ├── shared/                          [P1 UI; P5 platform/space/roles]
│       ├── data_views/                      [P5 IDs/config; owners xác nhận fields]
│       ├── dashboards/
│       │   ├── d01_overview/                [P1]
│       │   ├── d02_movie_trends/            [P2]
│       │   ├── d03_genres/                  [P3]
│       │   ├── d04_movielens_realtime/      [P4]
│       │   └── d05_tmdb_signal/             [P5]
│       ├── saved_objects/                   [exports theo owner + integration bundle]
│       └── qa/                              [P1]
├── docs/phase2/
│   ├── platform/                            [P5]
│   ├── lanes/{movie_batch,genre_batch,movielens_realtime,tmdb_realtime}/
│   └── integration/                         [P1]
└── data/local/phase2/                       [không commit]
    ├── es_reports/
    ├── kibana_import_reports/
    └── checkpoints/                         [chỉ local dev]
~~~

Đã bỏ thành phần xuất file trung gian chỉ để phục vụ công cụ BI cũ. Kibana đọc trực tiếp alias; báo cáo import/export saved objects chỉ là artefact cấu hình, không phải bản sao dữ liệu.

### 3.3. Quyền sửa và tích hợp saved objects

| Owner | Vùng sửa chính | Quy tắc giao nhau |
|---|---|---|
| P1 | `modules/kibana/shared/`, D1, `qa/`, integration bundle/docs; quản lý hai tài liệu root | Import bundle của từng owner vào Space integration, kiểm ID/reference rồi export bundle tổng; không sửa Lens metric của owner để che lỗi. |
| P2 | Contract/lane `movie_batch`, D2 và saved objects D2 | Giao D1 fixtures/aliases; export chỉ objects do P2 sở hữu, dùng stable IDs. |
| P3 | Contract/lane `genre_batch`, D3 và saved objects D3 | Không publish Batch alias riêng. |
| P4 | Contract/lane `movielens_realtime`, D4 và saved objects D4 | Sở hữu schema/build của growth history/current; P5 vận hành alias command sau READY. |
| P5 | Common ES/SQL/Kibana deployment, Data Views/roles; TMDB lane, D5 | Cấp môi trường và stable Data View IDs; không ghi đè dashboard của P1-P4 khi import. |

Mỗi owner export NDJSON có manifest (`owner`, object IDs, Data View IDs, contract version, exported_at). P1 import vào Space integration với chế độ không overwrite ngoài allowlist của owner; collision ngoài allowlist làm fail. Bundle tổng được export lại và smoke-import vào Space sạch trước G4. Secrets, credentials và local state không commit.

## 4. Nhiệm vụ chi tiết - P1: Kibana chung và D1

| Mã | Việc cần làm | Phối hợp | Bàn giao/tiêu chí |
|---|---|---|---|
| P1.1 | Quy chuẩn bố cục, màu, nhãn nguồn, UTC, tooltip, trạng thái dữ liệu, điều hướng và template Kibana cho D1-D5. | Soạn wireframe từ contract; chỉ dựng object có dữ liệu sau khi P5 giao dev Space và fixture indices. | Style guide + template dashboard; nhãn Batch/replay/latest-per-movie nhất quán. |
| P1.2 | Quy chuẩn Data Views, field types/time field, controls/filter, quyền read-only và auto-refresh. | P5 tạo stable IDs/roles; owners ký field list. | Không global time filter D1/D4-current/D5; đề xuất 10 giây ghi rõ không phải SLA; Lens không tính lại metric. |
| P1.3 | Dựng D1 từ hai aliases P2, tách panel all-time khỏi timeline và Top theo kỳ. | P2 giao dev fixture sau K0 và analytics thật K2. | All-time = 100.836, 9.724; `period_start=null` vẫn hiện; Top cùng kỳ. |
| P1.4 | Review D2-D5 về giao diện, filter, null, provenance; lỗi metric trả về owner. | Owners giao draft trước K3. | Review log và ảnh sau sửa. |
| P1.5 | Tích hợp 5 dashboard, saved objects, liên kết điều hướng, thay fixture aliases bằng canonical aliases và nghiệm thu. | Theo manifest/allowlist mục 3.3. | Integration bundle import được vào Space sạch; không còn dev Data View/index pattern. |
| P1.6 | Chủ trì trace một KPI/panel mỗi dashboard từ source → ES → SQL → Kibana. | Mỗi owner cung cấp query đối chứng. | Biên bản K4, trạng thái gate và giới hạn hiển thị đúng. |

## 5. Nhiệm vụ chi tiết - P2: Batch phim, Spark SQL và D2

| Mã | Việc cần làm | Phối hợp | Bàn giao/tiêu chí |
|---|---|---|---|
| P2.1 | Mapping `movie_stats.tsv`/`movie_daily_stats.tsv`, khóa theo run + movie/date. | P5 templates/endpoint. | Schema + fixture K0. |
| P2.2 | Loader hai physical source, kiểm từng Bulk item, idempotent rerun. | Nạp dev khi nền sẵn. | 9.724/99.538 dòng; mỗi bảng tổng 100.836. |
| P2.3 | Giao physical source/READY; chỉ publish cùng ba Batch analytics. | P3/P5. | Không tạo run nửa cũ nửa mới. |
| P2.4 | Tạo `analytics_timeseries` và `analytics_top_movies`, rank ổn định, provenance. | Runtime P5. | D1 all và Top mẫu đúng contract. |
| P2.5 | Query/test 7d, 30d, WoW và fixture Batch-vs-RT. | P4. | Không thêm metric ngoài D2. |
| P2.6 | Dựng D2 bằng `dv_d2_movie_timeseries`: filter cố định `entity_type=movie`, control tối đa 3 phim, đúng một period type. | Template P1, Data View P5. | Một phim/kỳ khớp SQL; avg không bị tính lại; saved objects có manifest. |
| P2.7 | Runbook loader/SQL/Kibana object và rollback. | P1/P5. | Người khác tái chạy được. |

## 6. Nhiệm vụ chi tiết - P3: Batch genre, Spark SQL và D3

| Mã | Việc cần làm | Phối hợp | Bàn giao/tiêu chí |
|---|---|---|---|
| P3.1 | Mapping `genre_daily_stats.tsv`, run/T/ref_version, khóa run/date/genre. | P5. | Schema + fixture K0. |
| P3.2 | Loader physical source, Bulk/idempotency checks. | P2 chạy song song. | 32.894 dòng, 274.480 lượt gắn genre. |
| P3.3 | Giao physical source + READY cùng run Batch. | P2/P5. | Alias giữ run cũ nếu bất kỳ phần nào lỗi. |
| P3.4 | Tạo `analytics_genre_trends` day/month/year và query tuần ISO. | Runtime P5. | Rank/avg và test biên đúng. |
| P3.5 | Dựng D3 bằng `dv_d3_genre_trends`, đúng một period type; tooltip nói rõ đa nhãn. | Template P1. | Không trình bày tổng genre như tổng hệ thống; saved objects có manifest. |
| P3.6 | Runbook và smoke sau publish. | P1/P5. | Có count/lỗi/lệnh chạy lại. |

## 7. Nhiệm vụ chi tiết - P4: MovieLens replay, growth history/current và D4

| Mã | Việc cần làm | Phối hợp | Bàn giao/tiêu chí |
|---|---|---|---|
| P4.1 | Mapping hai RT source, khóa entity/window/size. | P5 template. | Fixture và đối chứng 201.627/127.770. |
| P4.2 | Kiểm canonical Spark Streaming writer, Bulk item/network/upsert. | Owner Module 05. | Snapshot pass tách live/restart pass. |
| P4.3 | Test dedup/state qua restart; state hiện trong RAM là rủi ro phải sửa. | P5 platform. | Hai micro-batch trước/sau restart không giảm/nhân count. |
| P4.4 | Tạo lịch sử `analytics_growth`, exact previous window và provenance. | Runtime P5. | Hai size không trộn; missing previous giữ null. |
| P4.5 | Query top growth và Batch-vs-RT sau cutoff trên fixture. | P2. | Query 7/8 pass; output hiện tại sau T vẫn rỗng. |
| P4.6 | Materialize/publish input `analytics_growth_current`; dựng D4 current + liên kết history. Không dùng relative “Last 15 minutes” cho replay. | P5 chạy atomic alias swap; P1 review. | Một size mỗi lần; current alias không chứa window cũ; rollback/restart/replay tests pass; saved objects có manifest. |
| P4.7 | Runbook complete watermark/EOF, snapshot version, rollback và trạng thái replay/live. | P1/P5. | Người xem biết event time khác processing/publish/view time. |

## 8. Nhiệm vụ chi tiết - P5: ES/Spark SQL/Kibana, TMDB và D5

| Mã | Việc cần làm | Phối hợp | Bàn giao/tiêu chí |
|---|---|---|---|
| P5.1 | Dựng Elasticsearch và Kibana cùng version tương thích; endpoint, templates, roles, Spaces, logs. | Giao nền ngay K0. | Mỗi owner có dev namespace và quyền tối thiểu. |
| P5.2 | Dựng Spark SQL runner UTC, ES connector, readers/writers, `_id`, retry/log. | Owners viết metric trên fixture. | Job mẫu đọc/ghi dev index; không có thành phần xuất dữ liệu trung gian cho dashboard. |
| P5.3 | Publisher sáu Batch aliases và publisher riêng growth-current, kiểm response từng action/rollback. | P2-P4 READY. | Không có alias trỏ index dở. |
| P5.4 | TMDB periodic end-to-end: producer → Kafka → Spark → `tmdb_realtime`, dedup/late/restart/Bulk. | Owner Module 05 khi sửa upstream. | Hai crawl thật cùng ID trước live_pass; baseline 20 dòng chỉ snapshot_pass. |
| P5.5 | Tạo `analytics_tmdb_signal` latest-per-tmdbId; chặn ghi lùi theo event_time. | Fixture baseline + edge cases. | Delta nullable đúng; không giả global snapshot. |
| P5.6 | Dựng D5 bằng Data View không time field; KPI latest states, Top/scatter, min/max event time, published time; fallback ID. | Template P1. | Null delta không thành 0; vote_average /10; saved objects có manifest. |
| P5.7 | Kiểm tổng thể 6 source + 6 analytics families, roles, Data Views, freshness, restart và runbook. | P1-P4 giao báo cáo lane. | Bảng trạng thái G0-G4; không tự đánh dấu pass từ tài liệu. |

## 9. Mốc bàn giao K0-K4

| Mốc | Công việc song song | Điều kiện qua mốc |
|---|---|---|
| **K0 - Contract, nền dev, fixture** | P5 giao ES/Spark SQL/Kibana dev, roles, stable Data View IDs và smoke; P1 giao style/wireframe; P2-P5 nạp fixture vào dev indices. | Dashboard có dữ liệu chỉ bắt đầu sau khi nền dev và fixture index tồn tại; 6 source/6 analytics families, grain/null/time field/owner thống nhất. |
| **K1 - 6 source index families** | P2/P3 dựng ba physical Batch chưa publish; P4 kiểm hai MovieLens canonical sources; P5 kiểm TMDB canonical source. | Counts/mapping/Bulk/idempotency đúng; snapshot_pass và live_pass tách biệt; ba Batch source aliases chỉ đổi cùng ba Batch analytics tại K2/G2P. |
| **K2 - 6 analytics aliases + 9 query tests** | P2 hai Batch movie, P3 genre, P4 growth history/current, P5 TMDB. | Batch publish sáu aliases nguyên tử; growth-current publish riêng nguyên tử; query tests pass; Data Views đọc canonical aliases. |
| **K3 - 5 dashboard đơn lẻ** | Mỗi owner giao dashboard/panels NDJSON + manifest; P1 review. | Filter, null, provenance và dữ liệu mẫu khớp analytics; không còn dev fixture trong object giao tích hợp. |
| **K4 - Integration và demo** | P1 import theo allowlist, liên kết D1-D5, smoke-import bundle tổng vào Space sạch. | Trace đủ 5 lane, auto-refresh được mô tả đúng, mọi giới hạn replay/latest-per-movie hiển thị rõ. |

Nếu nền P5 chưa có, owners vẫn có thể viết schema/SQL/unit fixture và P1 làm wireframe, nhưng không tuyên bố dashboard Kibana đã chạy. Nếu một lane trễ, P1 không đổi metric để bù.

## 10. Freshness đầu cuối và điều kiện nghiệm thu

~~~text
event_time/crawl event_time
  -> Kafka ingest + Spark Streaming processing (`updated_at` ở source)
  -> source window/state đủ điều kiện
  -> Spark SQL build physical analytics
  -> alias publish thành công (`published_at`)
  -> Elasticsearch refresh/searchable
  -> lần Kibana query kế tiếp
  -> thời điểm người dùng nhìn thấy
~~~

`window_start/event_time` là thời gian nghiệp vụ; `updated_at` là thời gian xử lý source; `published_at` là lúc phiên analytics được công bố. Kibana refresh interval thử nghiệm là 10 giây, nhưng độ trễ đầu cuối chưa là SLA cho đến khi đo từng đoạn và ghi p50/p95/max. MovieLens replay trước T không được gọi là realtime hiện tại. TMDB chỉ được gọi periodic/live sau test hai crawl thật, duplicate, late event, restart và Bulk item failure.

| Nhóm | Điều kiện pass |
|---|---|
| Batch | 9.724/99.538/32.894; 100.836 rating; 274.480 genre assignments; weighted avg đúng. |
| D1-D3 | Null/time filter/period type không làm sai grain; Top và KPI trace được. |
| D4 | Current alias đúng một complete window mỗi size, không lẫn window cũ; history giữ đủ; restart/replay/rollback pass. |
| D5 | Latest-per-ID, không global snapshot; baseline 20 dòng không đủ live_pass; null/title/ID đúng. |
| Kibana | Role read-only analytics; stable Data Views; saved objects import sạch; controls/links/refresh đúng contract. |
| Tái chạy | Runbook, logs, Bulk item checks, publication marker và rollback có bằng chứng. |

## 11. Các rủi ro phải báo cáo đúng

1. Code `streaming_job.py` đang giữ dedup/window/TMDB previous state trong RAM; checkpoint hiện không chứng minh phục hồi các Python states đó.
2. `es_sink.py` hiện warning rồi tiếp tục khi Bulk có item lỗi hoặc mất kết nối; P4/P5 phải làm lỗi thất bại/quarantine/retry an toàn trước live_pass.
3. `analytics_growth_current`, sáu analytics aliases, Data Views và saved objects mới là contract thiết kế, chưa có bằng chứng triển khai trong repo.
4. TMDB producer lưu một lượt fetch để phát lại an toàn nhưng event schema chưa có `crawl_run_id`/expected count/publication protocol; do đó D5 vẫn latest-per-movie.
5. Tài liệu `P1_1_Tableau_Style_Guide.md` nếu tồn tại về sau là artefact quyết định cũ cần thay thế; ngoài hai file contract này không có file nào được sửa trong đợt tài liệu hiện tại.
