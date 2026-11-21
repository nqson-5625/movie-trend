# DATA CONTRACT PHASE 2 - Elasticsearch, Spark SQL và Kibana

**Dự án:** Movie Trend

**Phiên bản:** Phase2-3.0

**Ngày hiệu lực:** 21/11/2026

**Owner tài liệu:** P1 điều phối; P2-P5 ký phần dữ liệu/lane của mình

**Tài liệu phân công:** [Kickoff_Phase2.md](Kickoff_Phase2.md)

## 0. Hiệu lực, phạm vi và thứ tự ưu tiên

Tài liệu này là hợp đồng triển khai Phase 2 cho Module 06-08. Khi xung đột, thứ tự ưu tiên là: change request đã ký → tài liệu này → `Kickoff_Phase2.md` → contract nền `DATA_CONTRACT.md` → README/code cũ. Không được âm thầm đổi index, field, grain, metric, cutoff, null semantics hoặc writer.

Phase2-3.0 thay kiến trúc tiêu thụ dashboard bằng Kibana đọc trực tiếp Elasticsearch analytics aliases. Việc sửa tài liệu **không** chứng minh Elasticsearch, Spark SQL, Kibana, Data Views, analytics indices hoặc dashboard đã chạy. Repository tại thời điểm rà soát mới có bằng chứng Module 01-05 và code upstream.

## 1. Kiến trúc và ownership

~~~text
Batch files / Kafka events
  -> source physical indices + 6 canonical source aliases
  -> 6 Spark SQL temp views
  -> analytics physical indices + 6 analytics aliases
  -> 7 Kibana Data Views
  -> Lens panels + controls
  -> D1-D5 saved dashboards
~~~

| Lane | Source alias / temp view | Canonical writer | Analytics alias / writer | Dashboard owner |
|---|---|---|---|---|
| Batch movie aggregate | `movie_batch_stats` / `batch_movie` | P2 loader | `analytics_timeseries`, `analytics_top_movies` / P2 Spark SQL | D1 P1, D2 P2 |
| Batch movie daily | `movie_daily_stats` / `batch_movie_daily` | P2 loader | Hai aliases phía trên / P2 | D1/D2 |
| Batch genre daily | `genre_trends` / `batch_genre` | P3 loader | `analytics_genre_trends` / P3 | D3 P3 |
| MovieLens RT movie | `movie_realtime_stats` / `rt_movie` | Spark Streaming; P4 nghiệm thu | `analytics_growth` và `analytics_growth_current` / P4 Spark SQL | D4 P4 |
| MovieLens RT genre | `genre_realtime` / `rt_genre` | Spark Streaming; P4 nghiệm thu | Hai aliases phía trên / P4 | D4 P4 |
| TMDB RT | `tmdb_realtime` / `tmdb_rt` | Spark Streaming; P5 nghiệm thu | `analytics_tmdb_signal` / P5 Spark SQL | D5 P5 |

P5 sở hữu nền tảng Elasticsearch, Spark SQL, Kibana, roles, Spaces, Data View IDs và lệnh publish; P5 không sở hữu metric P2-P4. P1 sở hữu chuẩn giao diện, D1, review và integration bundle; P1 không ghi analytics. Kibana dùng role chỉ đọc `analytics_*`, không đọc source aliases trong dashboard và không ghi index.

**Đếm thống nhất:** 6 source alias families, 6 analytics alias families và 7 Data Views. `analytics_growth_current` là index family thứ sáu; nó không thay lịch sử `analytics_growth`.

## 2. Bằng chứng dữ liệu hiện có và trạng thái triển khai

| Artefact | Số dòng/khóa | Đối chứng đã ghi nhận |
|---|---:|---|
| `movie_stats.tsv` | 9.724 movie | Tổng 100.836 interaction/rating. |
| `movie_daily_stats.tsv` | 99.538 movie-day | Tổng 100.836; cộng theo movie khớp movie aggregate. |
| `genre_daily_stats.tsv` | 32.894 genre-day | Tổng 274.480 lượt gắn genre; phim đa genre được đếm ở từng nhóm. |
| `movie_realtime_stats.jsonl` | 201.627 windows | 100.826 docs 10m, 100.801 docs 1h; mỗi size cộng 100.836 rating. |
| `genre_realtime.jsonl` | 127.770 windows | 77.500 docs 10m, 50.270 docs 1h; mỗi size cộng 274.480 genre assignments. |
| `tmdb_realtime.jsonl` | 20 latest states | Cùng `event_time=2026-11-12T06:02:43Z`; prev/delta null; chỉ baseline. |

Batch run là `20261024T104145Z`, cutoff T là `2018-10-01T00:00:00Z`. MovieLens windows hiện có đều trước T và là replay lịch sử. 20 `tmdbId` không khớp movie reference đính kèm; không suy diễn join. Các count trên là đối chiếu file/tài liệu hiện có, không phải bằng chứng ES/SQL/Kibana end-to-end.

## 3. Ngữ nghĩa thời gian và freshness

| Field/mốc | Nghĩa bắt buộc |
|---|---|
| `event_time` | Thời gian nghiệp vụ của TMDB event; quyết định latest/previous theo từng `tmdbId`. |
| `window_start`, `window_end` | Cửa sổ MovieLens nửa mở `[start,end)`, align UTC; không phải thời gian xử lý. |
| `period_start` | Đầu kỳ UTC cho day/month/year; null cho all. |
| `updated_at` | Lúc Spark Streaming/source xử lý hoặc cập nhật record; không quyết định latest nghiệp vụ. |
| `analytics_run_id` | ID một lần build analytics; keyword, không dùng thay timestamp. |
| `published_at` | Lúc physical analytics version được công bố qua alias; date UTC. |
| `source_complete_through` | Mốc event-time mà P4 đã chứng minh source windows kết thúc trước/đúng mốc là hoàn chỉnh. |
| Kibana query/view time | Lần Kibana truy vấn lại và lúc trình duyệt render; không được lưu như event time. |

Freshness end-to-end phải được đo theo chuỗi:

~~~text
upstream event/crawl
 -> Kafka ingest
 -> Spark Streaming source write (`updated_at`)
 -> source complete condition
 -> Spark SQL physical analytics build
 -> alias publish (`published_at`)
 -> Elasticsearch searchable
 -> Kibana query kế tiếp
 -> người dùng nhìn thấy
~~~

Đề xuất auto-refresh cho môi trường thử nghiệm là **10 giây**. Đây là cấu hình để test, không phải SLA. Khi chưa đo, báo `freshness_status=unmeasured`; khi đo phải ghi p50/p95/max cho từng đoạn, dataset, thời gian và tải thử. Không gọi MovieLens replay là “hiện tại”. TMDB chỉ gắn `live_pass` sau hai crawl thật, duplicate/late/restart/Bulk tests.

## 4. Hợp đồng 6 source aliases

Tất cả date dùng Elasticsearch `date` UTC; ID số dùng `integer` hoặc `long` như bảng; nhãn/filter dùng `keyword`; title dùng `text` kèm subfield `title.keyword` kiểu `keyword`. Các field bắt buộc không được dynamic-map thành text.

| Alias | Grain và `_id` | Mapping tối thiểu | Writer/quy tắc |
|---|---|---|---|
| `movie_batch_stats` | `(batch_run_id,movieId)`; `_id=run\|movieId` | `batch_run_id keyword`, `batch_cutoff_time date`, `generated_at date`, `movieId integer`, `title text+keyword nullable`, `total_count long`, `rating_sum double`, `avg_rating double` | P2 loader; physical theo run; không đổi `total_count` thành interaction ở source. |
| `movie_daily_stats` | `(batch_run_id,date,movieId)`; `_id=run\|date\|movieId` | metadata Batch như trên; `date date`, `movieId integer`, title nullable, `interaction_count/rating_count long`, `rating_sum/avg_rating double` | P2 loader; idempotent. |
| `genre_trends` | `(batch_run_id,date,genre)`; `_id=run\|date\|genre` | metadata Batch; `date date`, `genre keyword`, counts long, sum/avg double | P3 loader; giữ `UNKNOWN`. |
| `movie_realtime_stats` | `(movieId,window_start,window_size)`; `_id=movieId\|start\|size` | `window_start/end date`, `window_size keyword`, `movieId integer`, counts long, sum/avg double, `updated_at date` | Spark Streaming canonical; cùng key upsert, 10m/1h riêng. |
| `genre_realtime` | `(genre,window_start,window_size)`; `_id=genre\|start\|size` | dates/size như trên, `genre keyword`, counts long, sum/avg double, `updated_at date` | Spark Streaming canonical; giữ UNKNOWN. |
| `tmdb_realtime` | `tmdbId`; `_id=tmdbId` | `tmdbId integer`, `event_time date`, `popularity double`, `vote_count long`, `vote_average double`, `prev_popularity/popularity_delta double nullable`, `updated_at date` | Spark Streaming canonical; latest-per-ID, event cũ không overwrite. Title hiện không có trong output source. |

Ba Batch physical sources cùng `batch_run_id/T/ref_version/manifest_hash`. Manifest cung cấp ref version; không tự thêm field không có vào TSV. Writer source không phải Spark SQL. Fixture tĩnh chỉ vào namespace dev, không nạp canonical RT aliases.

## 5. Metric chuẩn và quy tắc null

| Metric | Công thức/aggregation chuẩn | Cấm |
|---|---|---|
| `interaction_count`, `rating_count` | Số rating trong đúng grain; với dữ liệu này hai field bằng nhau | Gọi là view count. |
| `avg_rating` | `rating_sum / rating_count` do Spark SQL tính; null nếu denominator không hợp lệ | Average-of-averages trong Lens. |
| `rank_in_period` | `row_number` giảm dần count, tie-break `movieId` hoặc `genre` tăng dần | Lens tự rank trên nhiều period. |
| `trending_score` | `(current_window_count-previous_window_count)/(previous_window_count+1)` | So hai size hoặc window không liền kề. |
| `previous_window_*` | Exact `window_start - window_size` cùng entity/size | Lấy document gần nhất nhưng không liền kề. |
| `popularity_delta` | latest popularity - previous hợp lệ của cùng `tmdbId` theo event_time | Đổi null thành 0; cộng với MovieLens metric. |

Nếu previous MovieLens document không tồn tại, `previous_window_present=false`, `previous_window_count=null`, `trending_score=null`. Nếu previous tồn tại và count thật bằng 0 thì present=true và score được tính. TMDB snapshot đầu tiên của từng ID giữ prev/delta null.

## 6. Hợp đồng 6 analytics aliases

Mọi analytics document có `schema_version keyword`, `analytics_run_id keyword` và `published_at date`. Batch giữ `batch_run_id keyword`, `batch_cutoff_time date`, `ref_version keyword` và `manifest_hash keyword`. Physical index bất biến trước khi alias publish, trừ `analytics_growth` history được upsert theo full window key như quy định dưới đây.

| Alias | Grain / `_id` | Mapping nghiệp vụ tối thiểu | Owner/consumer |
|---|---|---|---|
| `analytics_timeseries` | `(batch_run_id,period_type,period_start,entity_type,movieId)`; null token cố định trong `_id` | `period_type keyword` day/month/year/all; `period_start date nullable`; `entity_type keyword` movie/all; `movieId integer nullable`; title text+keyword nullable; counts long; sum/avg double; `distinct_movie_count long nullable` | P2; D1/D2 |
| `analytics_top_movies` | `(batch_run_id,period_type,period_start,movieId)` | `period_type keyword` all/year/month; `period_start date nullable`; movie/title; counts/sum/avg; `rank_in_period integer` | P2; D1 |
| `analytics_genre_trends` | `(batch_run_id,period_type,period_start,genre)` | period keyword; `period_start date`; `genre keyword`; counts/sum/avg; rank integer | P3; D3 |
| `analytics_growth` | `(window_size,window_start,entity_type,entity_key)`; `_id=size\|start\|type\|key` | `window_start/end date`, `window_size keyword`, `entity_type keyword`, `entity_key keyword`, `movieId integer nullable`, `genre keyword nullable`, title text+keyword nullable, `title_source keyword nullable`, current/previous counts long nullable as appropriate, `previous_window_present boolean`, `trending_score double nullable`, rating fields, `updated_at date`, `timeline_mode keyword`, common metadata | P4; D4 history |
| `analytics_growth_current` | `(snapshot_version,window_size,entity_type,entity_key)`; physical `_id=size\|type\|key` | Toàn bộ business fields của growth; thêm `snapshot_version keyword`, `selected_window_start date` bằng `window_start`, `source_complete_through date`, `history_analytics_run_id keyword`, common metadata | P4; D4 current |
| `analytics_tmdb_signal` | `tmdbId`; `_id=tmdbId` | `tmdbId integer`, `event_time date`, popularity double, vote_count long, vote_average double, prev/delta double nullable, `updated_at date`, title text+keyword nullable, `source keyword=tmdb`, common metadata | P5; D5 |

### 6.1. Period semantics D1-D3

`period_start` của day/month/year là đầu kỳ UTC; all là null. `analytics_timeseries` có entity `all` cho D1 và `movie` cho D2. D1 all dùng đúng một record `(period_type=all, entity_type=all)` để có 100.836 rating, 9.724 phim và weighted average. `analytics_top_movies` lưu đủ mọi phim trong kỳ; panel chỉ lọc `rank_in_period <= 10`.

D2/D3 phải chọn đúng một `period_type`; không cộng day + month + year. D2 luôn `entity_type=movie`, tối đa ba `movieId`. Kỳ không có rating có thể hiển thị 0 bằng date histogram gap policy cho count, nhưng không tự đặt `avg_rating=0`. D3 giữ từng genre độc lập; tổng genre không đại diện tổng rating hệ thống.

### 6.2. D4 history và current snapshot protocol

`analytics_growth` giữ lịch sử đầy đủ để tra cứu và audit. P4 chỉ upsert một history document khi source window key đã ổn định theo cùng khóa; retry không nhân đôi. `updated_at` không dùng để chọn latest.

P4 tạo `analytics_growth_current-v<snapshot_version>` theo protocol:

1. **Complete boundary:** lấy `source_complete_through` từ replay EOF đã đối soát hoặc watermark/checkpoint bền vững đã công bố cho cả movie và genre. Với mỗi size `s`, chọn `W_s=max(window_start)` sao cho `window_end <= source_complete_through`. Không chọn max document đơn lẻ nếu window chưa complete.
2. **Consistent read:** đọc một PIT/pinned history generation; lấy mọi row có `window_size=s AND window_start=W_s` cho cả `entity_type=movie,genre`. Exact previous đã được tính từ `W_s-s` trong history.
3. **Physical build:** ghi index mới với `snapshot_version`, `history_analytics_run_id`, `source_complete_through`, `published_at`; reject key trùng, sai size, row có start khác `W_s`, thiếu một entity type hoặc Bulk item failure.
4. **READY marker:** ghi count/hash theo size/entity, selected starts, history generation và previous-coverage report. Hai size phải cùng READY; không publish riêng nửa bộ.
5. **Atomic publish:** P5 thực hiện một aliases request remove old/add new cho alias `analytics_growth_current`, kiểm response và đọc lại alias/count. Kibana không bao giờ trỏ physical name.
6. **Rollback:** nếu publish/verification lỗi, alias giữ hoặc được trả về version trước; physical lỗi bị đánh `REJECTED`, không tái sử dụng. History không bị xóa/rollback theo current.

Current physical index chỉ chứa đúng một selected window cho mỗi size; do đó auto-refresh không thể trộn cửa sổ cũ. Không thêm `is_latest_window` vào history.

MovieLens source hiện không có title. P4 chỉ được enrich **nhãn title** bằng reference hoặc Batch title có `ref_version/batch_run_id` truy vết qua `title_source`; không mang count/rating Batch vào growth và phải giữ fallback `entity_key` khi title thiếu.

### 6.3. D5 latest-per-movie protocol

`tmdb_realtime` và `analytics_tmdb_signal` là latest theo từng `tmdbId`. Các ID có thể có `event_time` khác nhau; toàn bộ alias không phải một global crawl snapshot. D5 không filter `event_time=max(event_time)` và KPI không được đặt tên “phim trong snapshot mới nhất”.

P5 build physical `analytics_tmdb_signal-v<analytics_run_id>` từ một consistent read của `tmdb_realtime`, chọn tối đa một row mỗi ID, kiểm event-time monotonic so với version đang công bố, rồi atomic swap alias sau khi mọi Bulk item và count checks pass. Event cũ đến muộn không ghi đè. Event cùng timestamp nhưng payload khác phải quarantine hoặc dùng tie-break đã ghi contract/test; không chọn ngẫu nhiên.

KPI hợp lệ: cardinality `tmdbId` của latest-state set, min/max `event_time`, max `updated_at`, `published_at`. Title null hiển thị `TMDB <tmdbId>`. `vote_average` hiển thị thang /10. Chỉ có delta sau previous event thật của chính ID.

Snapshot toàn cục là mở rộng riêng, bắt buộc có `crawl_run_id`, expected/actual count, event identity, complete marker, immutable physical snapshot và atomic alias publish; không nằm trong contract hiện tại.

## 7. Công bố, quyền và trạng thái tìm kiếm được

### 7.1. Batch publication

P2/P3 tạo sáu physical indices cùng run: 3 source + `analytics_timeseries`, `analytics_top_movies`, `analytics_genre_trends`. P5 chỉ chuyển **sáu Batch aliases trong một request** sau READY đầy đủ. Kiểm từng alias action, mapping, count, run/T/ref/manifest và search smoke. Lỗi bất kỳ phần nào giữ/rollback cả sáu aliases; không có run nửa cũ nửa mới.

### 7.2. RT publication

- Source RT canonical do Spark Streaming ghi; static fixture không cạnh tranh writer.
- `analytics_growth` history dùng full-key idempotent upsert và chỉ đánh complete theo boundary đã chứng minh.
- `analytics_growth_current` và `analytics_tmdb_signal` dùng immutable physical version + atomic read-alias swap.
- `published_at` chỉ được đặt cho version READY ngay trước publish; sau alias swap phải gọi refresh/search smoke hoặc đợi ES trở nên searchable rồi mới báo thành công.

### 7.3. Roles

| Principal | Quyền tối thiểu |
|---|---|
| Batch loaders P2/P3 | create/write physical source trong lane; không publish alias. |
| Spark Streaming | create/write ba canonical RT source; không ghi analytics. |
| Spark SQL P2-P5 | read source lane; create/write physical analytics lane; không sửa dashboard. |
| Publisher P5 | manage aliases đúng allowlist; không đổi document metric. |
| Kibana dashboard role | read/view_index_metadata trên 6 analytics aliases; không đọc source, không write/delete/manage alias. |
| P1 integrator | quản lý saved objects trong Space integration; không có data write. |

Secrets chỉ qua môi trường/secret store. Data Views luôn dùng canonical alias, không dùng wildcard chứa dev/fixture/physical versions.

## 8. Kibana dashboard consumption contract

| Dashboard | Owner | Alias và Data View | Time field | Controls bắt buộc | Panels/KPI và aggregation hợp lệ | Trạng thái/refresh | Acceptance |
|---|---|---|---|---|---|---|---|
| D1 | P1 | `analytics_timeseries` → `dv_d1_timeseries`; `analytics_top_movies` → `dv_d1_top_movies` | **Không chọn** ở cả hai để giữ all rows có `period_start=null` | `period_type` all/year/month; period selector đồng bộ Top; batch run pin | KPI dùng filtered single all row: sum counts, max avg; timeline date histogram trên month rows; Top filter rank<=10, sort rank | Batch/static; refresh off mặc định hoặc manual | All-time 100.836/9.724 vẫn hiện dù dashboard khác có time filters; Top cùng một kỳ. |
| D2 | P2 | `analytics_timeseries` → `dv_d2_movie_timeseries` | `period_start` | Locked `entity_type=movie`; đúng một period type; OR selection 1-3 movie IDs | line sum count; max precomputed avg trên unique grain; tooltip sum/rating; gap count=0, avg=null | Batch/static; manual | Không chấp nhận phim thứ tư; không trộn period; một movie-month khớp SQL. |
| D3 | P3 | `analytics_genre_trends` → `dv_d3_genre_trends` | `period_start` | đúng một period type; genre include/exclude | line/heatmap sum count trong từng genre; max avg/rank trên unique grain; Top cùng period | Batch/static; manual | Tooltip ghi multi-label; tổng genre không gọi là tổng hệ thống. |
| D4 | P4 | `analytics_growth_current` → `dv_d4_growth_current`; `analytics_growth` → `dv_d4_growth_history` | current: **không chọn**; history: `window_start` | exactly one `window_size`; entity type; history dùng absolute range | rank theo current count/score từ current alias; show previous/present/raw count; history date histogram không cấp KPI current | Replay: auto-refresh 10s chỉ để test publish; không SLA | Current có đúng W10/W1 theo size, không window cũ; “Last 15 minutes” không làm biến mất replay. |
| D5 | P5 | `analytics_tmdb_signal` → `dv_d5_tmdb_signal` | **Không chọn** vì set latest-per-ID có event times khác nhau | popularity/vote ranges; delta-present optional; không global max-time filter | cardinality tmdbId; Top popularity; scatter popularity×vote_average size vote_count; min/max event_time; max published/update | Baseline/snapshot_pass/live_pass; 10s sau live test, không SLA | Không gọi global snapshot; /10; title fallback ID; null delta vẫn null. |

Data View IDs là hằng số cấu hình do P5 quản lý trong `modules/kibana/data_views/`. D2 control phải giới hạn cardinality lựa chọn 1-3 bằng cơ chế tương thích phiên bản Kibana đã chốt; G3 fail nếu người dùng thêm phim thứ tư. Không dùng Lens Formula để tạo lại `trending_score`, `popularity_delta` hoặc weighted average.

### 8.1. Chín truy vấn tối thiểu Module 07

| # | Query | Điều kiện bắt buộc | Owner/artefact |
|---:|---|---|---|
| 1 | Top phim | Đúng period; đủ rank/tie-break | P2; top analytics/test |
| 2 | Rating trung bình phim | sum/count; không avg-of-avg | P2; timeseries/test |
| 3 | Genre theo thời gian | day/month/year và tuần ISO; multi-label | P3 |
| 4 | Xu hướng 7 ngày | Hai khoảng nửa mở bằng nhau, end<=T | P2 |
| 5 | Xu hướng 30 ngày | 30 ngày liên tiếp, không thay bằng tháng | P2 |
| 6 | WoW | Hai tuần ISO Monday 00:00Z | P2 |
| 7 | Top phim tăng | Exact adjacent RT windows, một size, missing previous null | P4; growth/test |
| 8 | Batch vs RT | Batch `<T`; RT `window_start>=T`; một size; output replay hiện tại RT sau T rỗng | P4 với P2 |
| 9 | Current TMDB signal | Latest per tmdbId; delta nullable; không ghép MovieLens | P5 |

Query 4-6 có `coverage_status`; thiếu coverage trả null/unverified. Query 8 dùng fixture có T nằm giữa hai nguồn để chứng minh không cộng trùng. Chỉ sáu analytics families mục 6 bắt buộc materialize cho dashboard.

## 9. Fixture và phát triển song song

Fixture đặt trong `contracts/phase2/<lane>/fixtures/`, schema trong `schemas/`, README ghi schema/contract version, owner, generated_at và nhãn fixture. Dev indices dùng prefix/namespace cách ly. P1 chỉ dựng dashboard có dữ liệu sau khi P5 giao dev ES/Kibana và fixture đã được owner nạp.

| Owner | Fixture tối thiểu | Tests bắt buộc |
|---|---|---|
| P2 | all row có null period; day/month/year; movie/all; Top hai kỳ và tie; kỳ trống | D1 null không mất; D2 một period, max 3; 100.836/9.724 khi đổi dữ liệu thật. |
| P3 | Hai genre/two periods, UNKNOWN, multi-label, tie rank | Không trộn period; không cộng genre thành system total; 32.894/274.480 thật. |
| P4 | Hai size; mỗi size có window cũ, selected complete window và window chưa complete; movie+genre; previous present/missing/zero; hai physical current versions | Chọn W theo complete boundary; partial Bulk hoặc thiếu một size không swap; rollback; restart/replay không giảm/nhân count; history còn nguyên. |
| P5 | Baseline 20; >=2 events cùng ID; hai ID có latest time khác; duplicate, older late, equal-time conflict; build physical analytics lỗi một item | Latest-per-ID; incomplete crawl không bị gọi snapshot; null/delta; restart state; alias chỉ swap sau full success. |
| P1 | Stable Data View IDs, owner NDJSON manifests và Space sạch | Import không overwrite ngoài allowlist; links/controls/labels đúng; không còn dev index reference. |

## 10. Gate nghiệm thu G0-G4

| Gate | Bài kiểm tra | Trạng thái khi chỉ sửa tài liệu |
|---|---|---|
| **G0 - Contract + dev foundation** | 6 source/6 analytics/7 Data Views; ES/Spark SQL/Kibana versions; roles/Spaces; fixture index smoke; style guide | **Chưa PASS** |
| **G1A - Batch source** | 9.724/99.538/32.894 cùng run/T/ref; sums; zero Bulk errors; idempotent | **Chưa PASS** |
| **G1B - MovieLens RT** | Canonical writer, 10m/1h, snapshot pass riêng; Kafka restart/replay/Bulk before live pass | **Chưa PASS** |
| **G1C - TMDB RT** | Hai crawl thật cùng ID, duplicate/late/equal-time/restart/Bulk; baseline chỉ snapshot_pass | **Chưa PASS** |
| **G2 - SQL analytics** | 6 analytics mappings/keys/metrics; 9 query tests; D4 current protocol; D5 versioned publish | **Chưa PASS** |
| **G2P - Publication** | Atomic six Batch aliases; independent atomic growth-current and TMDB analytics swaps; search smoke/rollback | **Chưa PASS** |
| **G3 - Kibana** | D1-D5 saved objects; field/time/control/aggregation contract; owner manifests; canonical aliases only | **Chưa PASS** |
| **G4 - Integration/demo** | Bundle imports into clean Space; trace five dashboards; freshness/replay/latest labels; runbooks | **Chưa PASS** |

K0-K4 trong Kickoff là mốc bàn giao dự án; G0-G4 ở đây là gate kỹ thuật. K0 tương ứng chuẩn bị G0, K1 bao phủ G1, K2 bao phủ G2/G2P, K3 là G3, K4 là G4.

## 11. Luồng kiểm thử D4 và D5 end-to-end

### 11.1. D4

1. Movie event hợp lệ/trùng/đến muộn đi Kafka; Spark Streaming dedup và upsert hai size vào source với full window key.
2. Bulk `errors=true` hoặc item failure làm micro-batch fail/quarantine và retry an toàn; không advance checkpoint như đã thành công.
3. Sau replay EOF hoặc watermark/checkpoint bền vững, P4 công bố `source_complete_through` cho cả movie/genre.
4. Spark SQL upsert history, tính exact previous, rồi build current physical từ pinned history generation.
5. Fixture chứa một incomplete newer window để chứng minh nó không được chọn; restart/replay cho cùng source không đổi final counts.
6. Một size thiếu/entity thiếu/Bulk partial làm READY fail và alias giữ version cũ. Full pass mới atomic swap; ES searchable smoke xong Kibana query lại.

### 11.2. D5

1. Producer fetch và phát events; schema hiện không có global crawl ID nên mỗi event chỉ đại diện quan sát của một movie.
2. Spark Streaming dedup event, so `event_time` theo ID; older late không overwrite, equal-time conflict quarantine/tie-break; state phải phục hồi qua restart.
3. Bulk item/network failure làm batch fail/quarantine/retry; không chỉ warning rồi checkpoint.
4. P5 build full physical analytics latest-state set từ consistent source read, giữ null delta khi chưa có previous.
5. Partial analytics write không swap alias. Full validation + searchability mới publish; Kibana auto-refresh sau đó chỉ truy vấn lại alias.
6. Hai crawl thật có cùng ID chỉ chứng minh delta/live behavior; chưa biến latest-state set thành global snapshot.

## 12. Đặc tả lỗi và rollback

| Lỗi | Hành vi bắt buộc |
|---|---|
| Bulk HTTP thành công nhưng item lỗi | Ghi ID/reason, fail hoặc quarantine và retry; không báo complete. |
| Source restart mất Python in-memory state | Không live_pass; phục hồi từ state bền vững/raw log và chạy test count/latest/delta. |
| Batch physical/manifest lệch | Giữ cả sáu aliases run cũ. |
| Growth history có window mới hơn nhưng chưa complete | Không đưa vào current physical. |
| Growth current build/publish lỗi | Alias giữ version trước; history vẫn giữ; physical lỗi REJECTED. |
| TMDB event cũ/trùng/conflict | Dedup/chặn/quarantine; không ghi lùi theo processing time. |
| TMDB analytics partial | Không swap alias; D5 tiếp tục version cũ và hiển thị published time cũ. |
| Data View trỏ physical/dev wildcard | G3 fail; sửa về exact canonical alias. |
| Saved object ID collision ngoài owner allowlist | Import fail; không overwrite tự động. |
| Filter trộn period/window size | Dashboard/control phải chặn; acceptance fail. |

## 13. Saved objects, thư mục và ranh giới sửa

Cấu trúc chuẩn nằm tại Kickoff mục 3.2. Không còn component tạo bản sao analytics cho dashboard; Kibana đọc trực tiếp aliases. `modules/kibana/` có:

- `shared/`: theme/navigation/status badges do P1; deployment/Space config do P5.
- `data_views/`: định nghĩa stable IDs, aliases, time fields và field format do P5 quản lý, domain owner ký.
- `dashboards/d01...d05/`: mỗi owner chỉ sửa dashboard/panels lane mình.
- `saved_objects/owners/<owner>/`: NDJSON + manifest theo owner.
- `saved_objects/integration/`: bundle tổng do P1 tạo từ Space integration.
- `qa/`: import smoke, references scan, screenshots và KPI trace.

P1 import owner bundle với overwrite allowlist đúng owner, kiểm references, rồi export integration bundle. Bundle phải smoke-import vào Space sạch. NDJSON không chứa secrets và không được coi là dữ liệu analytics.

## 14. Nguồn tham khảo và giới hạn tái sử dụng

- [What's Seattle Reading?](https://www.whats-seattle-reading.com/): chỉ tham khảo interaction/layout, không sao chép dữ liệu sách.
- [Elasticsearch Aliases API](https://www.elastic.co/docs/manage-data/data-store/aliases): dùng atomic alias actions và phải kiểm response/read-back.
- Tài liệu chính thức Elastic cho phiên bản Elasticsearch/Kibana được P5 chốt: Data Views, dashboard controls, Lens, saved object import/export và security roles; version cụ thể phải ghi trong runbook khi triển khai.
- `DATA_CONTRACT.md`, manifest/output Batch, `modules/mapreduce/src`, `modules/kafka_ingestion/src` và `modules/spark_streaming/src` là nguồn đối chiếu upstream.

Không viện dẫn một tính năng Kibana khác phiên bản đã chốt mà chưa smoke test. Không suy ra môi trường đã tồn tại từ sơ đồ contract.

## 15. Checklist trước công bố

- [ ] G0: ES/Spark SQL/Kibana dev có version tương thích, roles/Spaces/Data Views và fixture smoke.
- [ ] G1A: Batch counts 9.724/99.538/32.894; sums 100.836 và 274.480; no Bulk item errors.
- [ ] G1B: MovieLens snapshot_pass; live_pass chỉ sau persistent state/restart/replay/Bulk tests.
- [ ] G1C: TMDB baseline 20 chỉ snapshot_pass; hai crawl thật + late/duplicate/restart/Bulk trước live_pass.
- [ ] G2: 6 analytics families và 9 queries pass; mappings/null/provenance đúng.
- [ ] G2P: Batch six-alias atomic publish; D4 current/D5 analytics version publish và rollback pass.
- [ ] G3: D1 all null record còn hiển thị; D2/D3 một period; D4 một size/current window; D5 null delta/latest-per-ID.
- [ ] G4: clean-Space import, five traces, freshness wording và runbooks ký nhận.

Rủi ro code đang mở: `streaming_job.py` giữ state Python trong RAM; `es_sink.py` chỉ warning ở Bulk/network error; Module 06-08 và current-snapshot publisher chưa có bằng chứng triển khai. Không đánh dấu gate pass chỉ vì contract này đã được sửa.

## 16. Quản lý thay đổi và nhật ký

Mọi change request ghi: vấn đề; cũ → mới; owner; ảnh hưởng P1-P5; migration schema/fixture/index/Data View/saved objects; rollback; tests; ngày ký. Owner dữ liệu và consumer cùng ký trước code merge.

### Phase2-3.0 - Tableau → Kibana (21/11/2026)

1. Thay Module 08 và toàn bộ đường tiêu thụ bằng Kibana đọc trực tiếp analytics aliases; loại bỏ yêu cầu workbook, file BI trung gian và exporter phục vụ Tableau. Các nhắc đến Tableau trong các phiên bản 2.x bên dưới là **lịch sử quyết định đã bị thay thế**, không phải yêu cầu hiện hành.
2. Thêm `analytics_growth_current` làm analytics family thứ sáu, physical version theo snapshot và atomic alias swap; giữ `analytics_growth` làm lịch sử. Đây là thay đổi data contract cần P4 triển khai SQL/materializer/tests và P5 triển khai publisher/role/template.
3. Chốt D5 là latest-per-`tmdbId`, không phải global crawl snapshot. Không thêm `crawl_run_id` ngầm; muốn snapshot toàn cục phải đổi upstream/schema/publication bằng change request mới.
4. Thêm `analytics_run_id`, `published_at`, D4 `snapshot_version/source_complete_through/history_analytics_run_id`, Data View/time-field contract, roles, saved-object ownership và freshness stages.
5. Code còn phải cập nhật sau bước tài liệu: P4/P5 sửa persistent state và Bulk failure behavior ở Module 05; P4 tạo growth history/current; P5 tạo ES/SQL/Kibana nền, versioned TMDB analytics publish và Data Views; P1-P5 tạo saved dashboards. Không phần nào tự động hoàn tất bởi thay đổi Markdown.

### Phase2-2.2 - cấu trúc cũ (15/11/2026, đã bị thay thế một phần)

Chốt ownership theo lane và fixture trong `contracts/phase2/<lane>/`; quyết định này vẫn giữ. Cấu trúc `modules/tableau/`, `tableau_export/` và `tableau_exports/` đã bị Phase2-3.0 thay thế.

### Phase2-2.1 - serving cũ (15/11/2026, đã bị thay thế một phần)

Giữ quyết định atomic publish sáu Batch aliases, restart/Bulk/late-event tests và chín query. Quyết định Tableau Desktop, CSV export, workbook và refresh file đã bị thay thế bởi direct aliases + Kibana query refresh.

### Phase2-2.0 và Phase2-1.1 (15/11/2026, lịch sử)

Giữ ownership dọc, số liệu đối chứng, period all/null, distinct movie count, trạng thái snapshot/live và writer canonical. Mọi yêu cầu trình bày bằng Tableau/TWB/TWBX hoặc workbook trong tài liệu/mẫu cũ không còn hiệu lực; nếu có `P1_1_Tableau_Style_Guide.md` thì phải thay bằng Kibana style guide, nhưng file ngoài hai contract không được sửa trong change này.
