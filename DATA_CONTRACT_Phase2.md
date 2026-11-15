# DATA CONTRACT PHASE 2 - Serving, Query và Tableau

**Dự án:** Movie Trend  
**Phiên bản tài liệu:** Phase2-2.2, chốt đường dẫn triển khai Phase 2 ngày 15/11/2026  
**Trạng thái:** baseline kỹ thuật để 5 owner ký nhận ở G0; các quyết định đã ghi có hiệu lực triển khai, thay đổi phải theo mục 14  
**Owner ký nhận G0:** cả 5 thành viên; người 1 quản lý bản tích hợp  
**Kế hoạch phân công:** [Kickoff_Phase2.md](Kickoff_Phase2.md)  
**Hợp đồng nền:** DATA_CONTRACT.md trong mã nguồn movie-trend  
**Website tham khảo cách trình bày:** [What's Seattle Reading?](https://www.whats-seattle-reading.com/)

## 0. Hiệu lực và thứ tự ưu tiên

Tài liệu này **bổ sung** hợp đồng nền cho Module 06/07/08 và bộ dữ liệu đã bàn giao. Những quy tắc bất biến của hợp đồng nền (identity, event_time UTC, cutoff, writer ownership, công thức rating, tránh double count) tiếp tục có hiệu lực. Phase 2 quyết định rõ tên index analytics, grain, field và mốc bàn giao cho năm dashboard.

**Bổ sung chính:** index analytics_tmdb_signal dành riêng cho D5, thuộc writer duy nhất Spark SQL và pattern analytics_*. Đây là tên mới so với danh sách bốn index analytics ở hợp đồng nền; nhóm dùng chính tài liệu Phase 2 làm quyết định mở rộng, không để mỗi thành viên đặt tên khác nhau. Nếu thay đổi một field/index/grain sau thời điểm chốt, lập change request như mục 14.

Năm index analytics ở mục 7 phục vụ **5 dashboard**. Chúng không thay thế chín truy vấn nghiệp vụ tối thiểu ở hợp đồng nền mục 13.3; các truy vấn 7/30 ngày, WoW và Batch vs Realtime vẫn phải có SQL và kiểm thử theo mục 8.1, dù chưa có dashboard riêng.

Không lấy website sách làm nguồn dữ liệu phim. Website chỉ là tham khảo cách xem tổng quan, so sánh tựa, xếp hạng, thể loại và phát hiện thay đổi; dữ liệu và định nghĩa metric của Movie Trend vẫn do các file dự án quyết định.

## 1. Kiến trúc và quyền sở hữu theo luồng

~~~text
MovieLens Batch phim (P2) --------> 2 ES Batch indices ----> 2 analytics phim ----> D1 (P1), D2 (P2)
MovieLens Batch genre (P3) -------> 1 ES Batch index ------> analytics genre -----> D3 (P3)
MovieLens Streaming replay (P4) --> 2 ES RT indices -------> analytics growth ----> D4 (P4)
TMDB crawl định kỳ (P5) ----------> 1 ES RT index --------> analytics TMDB -----> D5 (P5)
                                      |                         |
                              Hạ tầng ES chung P5        Spark SQL runner chung P5
                                                                         |
                                                               Tableau workbook chung P1
~~~

**Một lane = một owner cho schema nguồn, phép biến đổi, kết quả phân tích, test và dashboard.** P5 cung cấp hạ tầng ES và runtime Spark SQL dùng chung, không trở thành người viết mọi loader/job. P1 cung cấp chuẩn Tableau và đóng gói workbook, không tự tính lại metric. D1/D2 cùng đọc analytics phim do P2 viết; P1 không tạo index hoặc bản analytics riêng cho D1.

| Lane | Input thực tế | Index nguồn canonical | Writer logic và owner Phase 2 | Temp view Spark SQL | Analytics output / owner | Tableau owner |
|---|---|---|---|---|---|---|
| Phim Batch | HDFS /batch/movie_stats/run_id=.../part-*; export movie_stats.tsv | movie_batch_stats | Batch Loader phim P2 | batch_movie | analytics_top_movies và phần phim analytics_timeseries / P2 | D1 P1; D2 P2 |
| Phim-ngày Batch | HDFS /batch/movie_daily_stats/run_id=.../part-*; export movie_daily_stats.tsv | movie_daily_stats | Batch Loader phim P2 | batch_movie_daily | analytics_timeseries, analytics_top_movies / P2 | D1 P1; D2 P2 |
| Genre Batch | HDFS /batch/genre_daily_stats/run_id=.../part-*; export genre_daily_stats.tsv | genre_trends | Batch Loader genre P3 | batch_genre | analytics_genre_trends / P3 | D3 P3 |
| Phim Streaming | movie_realtime_stats.jsonl hoặc writer Spark Streaming | movie_realtime_stats | Spark Streaming canonical; P4 kiểm tra, tích hợp và phối hợp sửa Module 05 | rt_movie | analytics_growth phần movie / P4 | D4 P4 |
| Genre Streaming | genre_realtime.jsonl hoặc writer Spark Streaming | genre_realtime | Spark Streaming canonical; P4 kiểm tra, tích hợp và phối hợp sửa Module 05 | rt_genre | analytics_growth phần genre / P4 | D4 P4 |
| TMDB Streaming | tmdb_realtime.jsonl và các snapshot crawl tiếp theo | tmdb_realtime | Nhánh Spark Streaming TMDB canonical; P5 chịu trách nhiệm Phase 2 end-to-end, phối hợp Module 05 | tmdb_rt | analytics_tmdb_signal / P5 | D5 P5 |

P5 **không** tự ghi trực tiếp vào canonical movie_realtime_stats/genre_realtime/tmdb_realtime bằng script snapshot: writer của ba index vẫn là Spark Streaming. P4/P5 có thể sửa hoặc yêu cầu owner Module 05 sửa nhánh streaming theo cùng một PR có test, không tạo writer cạnh tranh. P5 quản trị template, endpoint, credentials, alias và runner; P2-P5 mỗi người là **writer duy nhất của analytics thuộc lane mình** thông qua job Spark SQL. Tableau chỉ đọc analytics_*.

Ba file export Batch có header; file part-* HDFS gốc không có header. Loader phải dùng manifest để biết cột, tránh nạp header thành record. HDFS raw/reference là nguồn chuẩn; ES giữ các view đã tính. P2 và P3 nạp physical index nguồn và tính physical analytics theo cùng run song song; P5 chỉ công bố **ba source alias cùng ba analytics alias** sau khi đủ sáu index pass, theo mục 7.1. Không có trạng thái phục vụ trộn hai run.

### 1.1. Hạ tầng dùng chung - giới hạn trách nhiệm của P5 và P1

| Hạng mục | P5 công bố và duy trì | Domain owner P2/P3/P4/P5 tự chịu trách nhiệm | P1 chịu trách nhiệm |
|---|---|---|---|
| Elasticsearch | Cluster, version, index templates, quyền tối thiểu, endpoint dev/test, health, logs, Bulk response, script alias publish/rollback | Mapping field/khóa và nạp hoặc xác minh sink của index thuộc lane; đối soát count, retry và truy vấn mẫu | Chỉ đọc analytics qua data source Tableau |
| Spark SQL | Runtime/phiên bản, UTC, connector/adapter ES, temp view loader, helper ID/publish, lệnh chạy, logs/retry và smoke job | SQL/DataFrame, metrics, tests, fixture, output analytics và runbook của lane | Kiểm tra kết quả hiển thị so với analytics, không định nghĩa lại metric |
| Tableau | P5 cấp endpoint/permissions analytics read-only | P2-P5 dựng từng dashboard theo template và ký nhận số liệu | P1 dựng template, D1, điều hướng, ghép 5 trang, QA và bản trình bày |

**Điểm bàn giao chéo cố định:** P2 → P1: hai analytics phim; P2 + P3 → P5: ba physical source, ba physical analytics Batch và manifest; P5 → P1-P4: nền ES/Spark SQL và adapter Tableau; P2 → P4: Batch fixture cho query Batch vs RT; P4/P5 → P1: analytics RT và TMDB. Các bên trao schema/fixture ngay K0, không đợi backend hoàn thành.

**Vị trí mã và quyền sửa:** Cây thư mục đã chốt tại mục 3.1-3.2 của [Kickoff_Phase2.md](Kickoff_Phase2.md). `modules/elasticsearch/common/` và `modules/spark_sql/common/` do P5 sở hữu; các thư mục `lanes/<lane>/` do owner của lane viết; `modules/tableau/final_workbook/` chỉ P1 tích hợp. Vị trí này không làm thay đổi quyền writer duy nhất đối với sáu source indices và năm analytics indices nêu ở mục 5.

## 2. Snapshot đầu vào và đối soát ban đầu

| Output | Số record | Khóa logic / phạm vi | Đặc điểm phải giữ |
|---|---:|---|---|
| movie_stats.tsv | 9.724 | (batch_run_id, movieId) | 100.836 total_count trên toàn bảng; có title. |
| movie_daily_stats.tsv | 99.538 | (batch_run_id, date, movieId) | 100.836 interaction_count trên toàn bảng; date 1996-03-29 đến 2018-09-24. |
| genre_daily_stats.tsv | 32.894 | (batch_run_id, date, genre) | 274.480 interaction_count gắn thể loại; một rating có thể thuộc nhiều genre. |
| movie_realtime_stats.jsonl | 201.627 | (movieId, window_start, window_size) | 100.826 cửa sổ 10m và 100.801 cửa sổ 1h; mỗi loại bao phủ 100.836 rating. |
| genre_realtime.jsonl | 127.770 | (genre, window_start, window_size) | 77.500 cửa sổ 10m và 50.270 cửa sổ 1h; mỗi loại bao phủ 274.480 lượt gắn thể loại. |
| tmdb_realtime.jsonl | 20 | tmdbId | Một event_time = 2026-11-12T06:02:43Z; prev_popularity/popularity_delta hiện đều null. |
| manifest.json | 1 | run_id = 20261024T104145Z | batch_cutoff_time T = 2018-10-01T00:00:00Z; reference ref_version = 20261017-5f26995f. |

**Lưu ý xác minh:** Bộ reference đóng gói trong mã nguồn có ref_version khác với reference khai báo trong manifest Batch. Không dùng bản reference trong repo để âm thầm thay nhãn/genre của một run đã công bố; khi cần enrich phải chỉ rõ ref_version và đối soát. 20 tmdbId của output TMDB không khớp movie_reference đi kèm.

**Kết quả kiểm tra trực tiếp sáu output:** Không thấy khóa logic trùng trong từng file; 9.724 phim có tổng count và rating_sum khớp tổng các dòng phim-ngày tương ứng; avg_rating của ba bảng Batch khớp sum/count; các cửa sổ 10m/1h có thời lượng và mốc UTC đúng; hai view phim theo mỗi window_size đều cộng ra 100.836 rating. Khi gom view 10m theo ngày, **99.538 khóa phim-ngày và 32.894 khóa thể loại-ngày** có count và rating_sum khớp tương ứng với Batch. Cả 9.724 movieId trong Realtime có title ở Batch. Đây là đối soát **file đã giao**, chưa chứng minh Elasticsearch hoặc pipeline live đã chạy end-to-end, cũng không bỏ qua khác biệt ref_version.

Các số record trong bảng là **mốc đối chứng khi nạp đúng bộ snapshot đính kèm vào môi trường trống/cô lập**. Index Kafka live có thể tăng hoặc đổi nội dung theo thời gian; không dùng số snapshot để kết luận một index live bất kỳ bị lỗi nếu không xác định source offsets và thời điểm chụp.

## 3. Ngữ nghĩa thời gian và tách view

| Trường | Quy tắc |
|---|---|
| event_time MovieLens | Timestamp gốc của rating, UTC. Replay vào Kafka không biến nó thành ngày hiện tại. |
| date Batch | Ngày UTC từ event_time, không lấy từ ingest_time hay generated_at. |
| batch_cutoff_time T | UTC tròn giờ; Batch chỉ có event_time < T. Cả ba bảng Batch của một run dùng cùng T. |
| window_start/window_end | Cửa sổ nửa mở [start, end), align UTC; window_size chỉ 10m hoặc 1h; cần cùng window_size khi so sánh. |
| updated_at | Thời điểm Spark ghi/cập nhật, không phải mốc để vẽ xu hướng nghiệp vụ. |
| event_time TMDB | Thời điểm snapshot TMDB, ở timeline riêng so với MovieLens lịch sử. |
| Tableau display | Ưu tiên ghi rõ UTC trên axis/tooltip; nếu đổi timezone, phải ghi nhãn và không đổi mốc tính nhóm. |

**Quy tắc tách cho Phase 2:** D1-D3 chỉ đọc Batch analytics; D4 chỉ đọc MovieLens Streaming analytics; D5 chỉ đọc TMDB analytics. Người xem có thể so sánh các dashboard bằng mắt nhưng hệ thống không cộng tổng của chúng. Các window trong output RT MovieLens hiện kết thúc trước T; D4 mang nhãn **MovieLens replay**, không mang nhãn “hôm nay”.

Một kỳ không có dòng aggregate có thể được vẽ là “0 rating trong tập đã nạp” để giữ trục thời gian liên tục, nhưng không được diễn giải thành “0 rating trong thực tế” khi chưa chứng minh đủ độ bao phủ nguồn. Không tạo sẵn hàng chục triệu dòng 0 cho mọi phim × ngày: dựng trục thời gian cho khoảng/phim được chọn hoặc dùng bảng lịch có kiểm soát.

Nếu sau này tạo dashboard tổng hợp lịch sử + khoảng mới, SQL phải chọn một run Batch và chỉ lấy cửa sổ Realtime có window_start >= T, chọn **một** window_size, tránh cộng trùng. Mốc T phải nằm trên timeline nguồn replay và align 1 giờ. Không dùng UNION ALL toàn bộ các view. **Query Batch vs Realtime vẫn phải được viết và kiểm thử trong Module 07 ở Phase 2** theo mục 8.1; chỉ việc đưa kết quả ghép lên một dashboard thứ sáu mới là mở rộng.

## 4. Hợp đồng schema cho sáu index nguồn

| Index | Field và type nghiệp vụ | Khóa document / index | Metadata và kiểm tra |
|---|---|---|---|
| movie_batch_stats | movieId integer; title string; total_count long; rating_count long; rating_sum double; avg_rating double | _id từ (batch_run_id, movieId) | batch_run_id string, batch_cutoff_time timestamp, generated_at timestamp; count > 0. |
| movie_daily_stats | date date; movieId integer; interaction_count/rating_count long; rating_sum/avg_rating double | _id từ (batch_run_id, date, movieId) | Cùng run/T; date < ngày T (nếu T là 00:00), hoặc lọc event_time < T ở upstream. |
| genre_trends | date date; genre string; interaction_count/rating_count long; rating_sum/avg_rating double | _id từ (batch_run_id, date, genre) | Giữ UNKNOWN; không suy ra tổng rating hệ thống bằng tổng genre. |
| movie_realtime_stats | window_start/end timestamp; window_size keyword; movieId integer; interaction_count/rating_count long; rating_sum/avg_rating double | _id ghép movieId, window_start, window_size theo thứ tự, dùng dấu gạch đứng như Spark hiện tại | updated_at timestamp; cùng một _id được upsert, không nhân đôi. |
| genre_realtime | window_start/end timestamp; window_size keyword; genre string; interaction_count/rating_count long; rating_sum/avg_rating double | _id ghép genre, window_start, window_size theo thứ tự, dùng dấu gạch đứng như Spark hiện tại | updated_at timestamp; genre UNKNOWN giữ nguyên. |
| tmdb_realtime | tmdbId integer; event_time timestamp; popularity/vote_average double; vote_count long; prev_popularity/popularity_delta nullable double | _id = chuỗi tmdbId như Spark hiện tại | updated_at timestamp; bản snapshot mới nhất theo event_time; title không có trong output hiện hành. |

Sơ đồ field chi tiết và giá trị required lấy từ hợp đồng nền mục 10-11. Mọi số đếm không âm; rating_sum hữu hạn; avg_rating là dẫn xuất từ sum/count. Ở Batch, với MovieLens, rating_count = interaction_count (hoặc total_count). Ở TMDB, vote_average dùng thang 0-10, không tương đương avg_rating MovieLens (0,5-5).

**Bản đồ field bắt buộc khi nạp:** Cả ba Batch source đều có `batch_run_id` string, `batch_cutoff_time` timestamp UTC và `generated_at` timestamp UTC, không null; manifest cung cấp `reference.ref_version` và `jobs.<name>.columns` đúng thứ tự, không tự thêm cột `ref_version` vào TSV. `movie_batch_stats.title` nullable; `date`, `movieId` hoặc `genre` bắt buộc ở các bảng tương ứng. Hai RT MovieLens có `updated_at` bắt buộc; TMDB source có `updated_at`, `event_time`, `tmdbId`, `popularity`, `vote_count`, `vote_average` bắt buộc, `prev_popularity` và `popularity_delta` nullable; `title` **không có** trong TMDB view hiện hành dù có ở event gốc. Không đổi tên `total_count` của movie_batch_stats thành `interaction_count` trong index nguồn; chỉ map nó khi tạo analytics. Index nguồn không được Spark SQL ghi lại.

P2/P3 đọc số cột từ manifest; TSV export có một header phải khớp chính xác danh sách này, HDFS `part-*` không header. Chọn **một** nguồn nạp cho mỗi run, ghi rõ HDFS path hoặc SHA-256 của bộ export đã dùng; không trộn HDFS và TSV của hai run. Mọi hàng cần parse được int/long/double hữu hạn, count không âm, `rating_count > 0` khi có `avg_rating`, `batch_run_id/T/generated_at` nhất quán với manifest. Bản lỗi được cách ly và job thất bại, không âm thầm bỏ hàng để vẫn đánh dấu READY. Kiểm tra `abs(avg_rating - rating_sum/rating_count) <= 1e-4` cho field đã làm tròn; khi gộp luôn lấy sum/count gốc. Mỗi physical index phải có số `_id` duy nhất đúng số dòng mong đợi và report Bulk item thành công/thất bại.

**Phiên bản Batch:** P2 tạo physical movie_batch_stats_<run>, movie_daily_stats_<run>, analytics_timeseries_<run>, analytics_top_movies_<run>; P3 tạo genre_trends_<run>, analytics_genre_trends_<run>. Hai job SQL đọc physical source của **cùng run** qua tham số runner, không đọc alias đang phục vụ. P2/P3 chỉ đánh dấu `READY` sau khi đủ nguồn và analytics của mình, kiểm tra manifest, schema, count, sum, khóa, lỗi Bulk và run/T/ref_version. P5 kiểm tra cả sáu physical index rồi dùng **một yêu cầu cập nhật aliases của Elasticsearch** gỡ/gắn sáu alias ổn định sang cùng run. Trước yêu cầu, P5 đọc ánh xạ alias cũ và chỉ tạo `remove` cho alias thực sự tồn tại, đánh dấu `must_exist` cho thao tác cần fail nếu đối tượng không có. Sau yêu cầu, P5 kiểm tra cả `errors`/`action_results` **dù HTTP 200**, đọc lại ánh xạ thực tế và xác nhận cả sáu cùng trỏ đúng một physical index/run; nếu sai, khóa publication marker, rollback về sáu ánh xạ cũ và kiểm tra lại. Chỉ sau khi xác nhận P1 mới refresh workbook. Nếu thiếu/lỗi bất kỳ index, giữ nguyên bộ cũ; lần đầu chưa có alias thì để workbook ở fixture/chưa công bố. Lưu alias trước/sau, manifest, response và lệnh rollback. Không cho P2/P3 chuyển alias lẻ. Một yêu cầu đổi alias là nguyên tử ở mức ánh xạ thành công; lỗi từng action có thể tạo kết quả một phần nếu không kiểm tra. Các request đọc độc lập qua nhiều index phải pin run và kiểm tra publication marker để phát hiện thay phiên giữa các request.

**Khóa Batch:** P5 cung cấp hàm mã hóa tuple xác định gồm run_id, phân biệt thứ tự/thành phần/null/kiểu; P2/P3 gọi cùng hàm trong loader và kiểm thử lặp lại. Cùng tuple → cùng _id, tuple khác → _id khác. Không dùng số thứ tự dòng hay ID ngẫu nhiên.

**Trạng thái RT sau khởi động lại:** Code Spark hiện giữ seen_movie_event_ids, movie_window_state, genre_window_state và tmdb_prev_state trong RAM. Kafka checkpoint một mình không tái tạo các biến đó. Restart có thể làm mất dedup/cumulative window hoặc snapshot TMDB trước. P4 nghiệm thu phục hồi nhánh MovieLens cùng owner Module 05; P5 nghiệm thu riêng nhánh TMDB cùng owner producer/Module 05. Hai bên phải kiểm thử restart giữa micro-batch, lỗi Bulk từng item, replay trùng và đến sai thứ tự; nếu chưa sửa state bền vững hoặc tái tính được từ raw, chỉ báo **snapshot tĩnh/demo có giới hạn**, không tuyên bố live đã pass.

### 4.1. Bảng bàn giao vật lý để các luồng chạy độc lập

| Luồng | Đầu vào duy nhất và người ghi nguồn | Định danh, điều kiện ghi | Đối soát trước bàn giao | Người nhận |
|---|---|---|---|---|
| P2 phim Batch | movie_stats.tsv và movie_daily_stats.tsv + manifest; P2 ghi hai physical index theo run. | (run,movieId) và (run,date,movieId); date UTC < T; không nạp header; null/kiểu theo mục 4. | 9.724/99.538 dòng, cả hai tổng 100.836; với từng movieId cộng daily count/sum khớp summary, không có duplicate _id. | P5 xét publish alias, P2 SQL, P1 nhận analytics sau K2. |
| P3 genre Batch | genre_daily_stats.tsv cùng manifest; P3 ghi physical genre_trends. | (run,date,genre); giữ UNKNOWN và genre đa nhãn; cùng run/T/ref_version P2. | 32.894 dòng, 274.480 lượt gắn thể loại, các sum/count hữu hạn và số lỗi Bulk bằng 0. | P5 xét publish alias, P3 SQL. |
| P4 MovieLens RT | Hai JSONL để đối chứng; canonical writer là Spark Streaming movie-events. | (movieId hoặc genre,window_start,window_size); cùng key upsert; 10m và 1h tách hẳn. | Snapshot 201.627/127.770 docs; count mỗi window_size lần lượt 100.836/274.480; restart/replay là bài test riêng. | P4 SQL và P5 health; không nạp fixture lên canonical. |
| P5 TMDB live | Producer crawl định kỳ → Kafka tmdb-snapshots → Spark Streaming; JSONL 20 dòng chỉ là fixture/baseline. | tmdbId cho bản mới nhất, event_time quyết định thứ tự; cùng event dedup, bản cũ không overwrite. | Ít nhất hai lần crawl thật trên cùng tmdbId để tính prev/delta; test retry, thứ tự, restart, Bulk item, freshness. | P5 SQL và P1 nhận analytics sau K2. |

Mỗi bàn giao có schema version, run_id/T hoặc window/snapshot, số dòng, query đối chứng, lệnh tái chạy, trạng thái `fixture`, `snapshot_pass` hoặc `live_pass`, và người ký nhận. Không nâng `snapshot_pass` thành `live_pass` chỉ vì index đã có tài liệu.

## 5. Quyền ghi, giao diện module và quá trình công bố

| Tài sản | Writer duy nhất | Người nhận/đọc | Gói bàn giao bắt buộc |
|---|---|---|---|
| Physical movie_batch_stats và movie_daily_stats theo run | P2 Batch Loader | P5 publish, P2 Spark SQL | Mapping, _id, run_id/T/ref_version, số dòng, sum/count, lỗi Bulk, 2-3 query đối chứng |
| Physical genre_trends theo cùng run | P3 Batch Loader | P5 publish, P3 Spark SQL | Cùng metadata của P2, UNKNOWN, count 32.894 và lượt gắn thể loại 274.480 |
| Sáu Batch aliases ổn định (3 nguồn + 3 analytics) | P5 công bố cùng một yêu cầu sau READY của P2/P3 | P1/P2/P3/P4 | Sáu physical cùng run/T/ref_version; báo cáo alias trước/sau, rollback và marker |
| movie_realtime_stats, genre_realtime | Writer Spark Streaming hiện hữu, P4 chịu trách nhiệm nghiệm thu/integration Phase 2 | P4 Spark SQL | Mapping, idempotent _id, source offsets/checkpoint, static/live status, lỗi từng Bulk item |
| tmdb_realtime | Writer nhánh TMDB Spark Streaming hiện hữu, P5 chịu trách nhiệm nguồn định kỳ và nghiệm thu Phase 2 | P5 Spark SQL | Snapshot event_time, latest/previous, dedup, retry, out-of-order, restart, lỗi Bulk |
| analytics_timeseries và analytics_top_movies | P2 qua Spark SQL runner chung | P1, P2 Tableau | Schema/fixture, physical version/alias, run/T, count/avg, Top sample, code/test |
| analytics_genre_trends | P3 qua Spark SQL runner chung | P3 Tableau, P1 | Schema/fixture, run/T, genre đa nhãn, query tuần, code/test |
| analytics_growth | P4 qua Spark SQL runner chung | P4 Tableau, P1 | Schema/fixture, window_size, previous present/null, timeline_mode, code/test |
| analytics_tmdb_signal | P5 qua Spark SQL runner chung | P5 Tableau, P1 | Schema/fixture, event_time, delta nullable, refresh, code/test |
| Một workbook 5 dashboard | P1 tích hợp; P2-P5 viết riêng trang mình | Người dùng/mentor | Dashboard, số liệu đối chứng, label nguồn, bản xuất và hướng dẫn mở |

Spark SQL dùng sáu temp view chuẩn batch_movie, batch_movie_daily, batch_genre, rt_movie, rt_genre, tmdb_rt. Temp view là đối tượng trong phiên job; **không phải output bền vững của Tableau**. P5 duy trì runtime/connector và contract adapter; P2/P3/P4/P5 tạo analytics riêng, công bố physical index/alias hoặc snapshot index theo mục 7.1. Dashboard chỉ đọc năm analytics outputs.

Để chạy song song, P2/P3/P4/P5 phát hành schema và fixture analytics ngay từ K0. Dùng file snapshot tĩnh vào **index dev tách biệt** để smoke; không ghi đè ba canonical RT indices. Kết quả static snapshot pass và Kafka live/restart pass là hai mức nghiệm thu khác nhau. Số dòng từ output đính kèm chỉ là mốc đối chứng cho snapshot cô lập.

### 5.1. Gói bàn giao chuẩn và ranh giới sửa mã

| Bên giao → bên nhận | Artifact bắt buộc tại K0/K1/K2 | Điều kiện để bên nhận tự làm tiếp |
|---|---|---|
| P5 → P1-P4, K0 | `phase2_runtime_config` (ES URL, Spark/connector version, UTC, namespace dev, cách cấp quyền), hàm đọc index với tham số physical/alias, helper tạo `_id`, schema/format export Tableau, lệnh smoke. Giá trị bí mật chỉ qua môi trường, không commit. | P2/P3 viết SQL từ physical index fixture; P4 viết SQL từ RT fixture; P1 dựng workbook từ CSV fixture đúng schema. |
| P1 → P2-P5, K0 | Template Tableau 5 trang, quy tắc field type, filter period/window, timezone, null, title fallback, trạng thái nguồn. | Mỗi người dựng một trang riêng mà không định nghĩa metric mới. |
| P2 → P5 và P1/P4, K1/K2 | Hai physical source + hai physical analytics theo run, manifest hash, READY report; CSV fixture D1/D2, query 1/2/4-6; fixture Batch đối chiếu cutoff cho P4. | P5 công bố sau READY của P3; P1 dựng D1; P4 kiểm query 8 không chờ hệ thống live. |
| P3 → P5 và P1, K1/K2 | Một physical source + một physical analytics cùng run với P2, READY report; CSV fixture D3 và query tuần ISO. | P5 đủ sáu Batch physical để publish; P1 dựng D3 vào workbook. |
| P4 → P1/P5, K1/K2 | Mapping/writer audit hai canonical RT, fixture dev, analytics_growth, query 7/8, hai trạng thái snapshot/live và test restart. | P1 kiểm D4; P5 chỉ hỗ trợ nền, không đổi metric P4. |
| P5 → P1, K1/K2 | tmdb_realtime canonical theo crawl, analytics_tmdb_signal, query 9, status snapshot/live, source timestamps; publication marker sáu Batch aliases, tài liệu kết nối Tableau. | P1 thay fixture bằng output thật khi trạng thái được ghi rõ. |

`READY report` tối thiểu gồm owner, schema_version, manifest SHA-256, run_id, T, ref_version, sáu tên physical index dự kiến (owner điền index thuộc mình), row_count và sum_count theo index, số duplicate `_id`, số Bulk item lỗi, SQL test result, thời điểm hoàn tất và trạng thái READY/FAILED. P5 không chấp nhận READY chỉ dựa vào log “job thành công”; phải query lại ES. Thay schema/helper/metric buộc cập nhật fixture, test và mọi bên nhận theo mục 14. Mỗi owner chỉ sửa job/loader/dashboard của lane mình; P5 duy trì module chung qua giao diện ổn định và review thay đổi nền.

## 6. Định nghĩa metric chuẩn

| Metric | Công thức / ý nghĩa | Owner thực thi | Tránh sai |
|---|---|---|---|
| interaction_count MovieLens | Số rating event hợp lệ, dedup theo event_id ở upstream | Upstream tạo số đếm; P2/P3/P4 đối soát theo luồng | Không gọi view_count hoặc số lượt xem phim. |
| rating_count | Số rating dùng để tính điểm trung bình; trong nguồn hiện tại bằng interaction_count | Upstream; P2/P3/P4 giữ nguyên | Không dùng vote_count TMDB thay thế. |
| avg_rating khi gộp kỳ | SUM(rating_sum) / SUM(rating_count) khi mẫu số > 0; null nếu = 0 | P2/P3/P4 theo luồng | Không lấy AVG(avg_rating) của các aggregate. |
| top phim lịch sử | Xếp hạng giảm dần theo SUM(interaction_count) trong **đúng period_type/period_start** | P2 | Không cộng thêm movie_stats khi đã tổng hợp daily. |
| genre count | Mỗi event được tính ở từng genre gắn với phim | P3/P4 | SUM qua mọi genre có thể > tổng rating MovieLens; không dùng để tính tỷ lệ phần trăm loại trừ nhau. |
| trending_score | (current_window_count - previous_window_count) / (previous_window_count + 1) | P4 | Chỉ với hai cửa sổ liền kề, cùng window_size; thiếu kỳ trước → null, không ngầm coi có số 0 quan sát được. |
| growth_7d / 30d / WoW | Hai khoảng liền kề, cùng độ dài trên event_time; growth_7d theo contract nền dùng (current_7d-previous_7d)/(previous_7d+1); growth_30d/WoW dùng cùng dạng chênh lệch trên hai kỳ tương ứng | P2 viết query và test trên daily aggregate | Thuộc **truy vấn tối thiểu của Module 07**, dù chưa lên D2; ghi rõ hai mốc kỳ, không lấy trung bình các avg_rating. |
| TMDB popularity_delta | popularity mới - prev_popularity của cùng tmdbId, có thứ tự event_time | P5 nghiệm thu upstream và trình bày | Một snapshot → null; không bịa 0 và không ghép popularity với interaction_count. |

Mức tăng 0→1 trong một cửa sổ có score dương nhưng chỉ một rating: dashboard phải hiển thị current_count và previous_count, có thể có bộ lọc ngưỡng tối thiểu **do người xem chọn** và ghi rõ ngưỡng. Không tự thay công thức score.

## 7. Query outputs Phase 2 - hợp đồng vật lý cho Tableau

Năm analytics index có owner duy nhất: P2 sở hữu hai bảng phim Batch, P3 thể loại Batch, P4 tăng trưởng MovieLens, P5 tín hiệu TMDB. Mỗi owner tính bằng runtime Spark SQL chung của P5; P2/P3 đọc physical source theo run trước khi công bố đồng thời sáu Batch aliases, P4/P5 đọc nguồn RT canonical đã được nghiệm thu. Các bảng chứa số đếm và tổng điểm để audit; Tableau có thể lọc/sắp xếp và cộng các field cộng được **trong một grain**, nhưng **không tự viết lại công thức trending_score/weighted average** hoặc cộng đồng thời các period_type.

| Index analytics | Grain/khóa duy nhất | Cột bắt buộc | Dashboard / owner |
|---|---|---|---|
| analytics_timeseries | (batch_run_id, period_type, period_start, entity_type, movieId). entity_type = movie hoặc all; movieId null khi all. period_type = day/month/year cho phim; day/month/year/all cho tổng hệ thống. Kỳ all có period_start null. | batch_run_id, batch_cutoff_time, period_type, period_start nullable, entity_type, movieId nullable, title nullable, interaction_count, rating_count, rating_sum, avg_rating, distinct_movie_count nullable (chỉ entity_type=all) | D1/D2; writer P2, reader P1/P2 |
| analytics_top_movies | (batch_run_id, period_type, period_start, movieId). period_type = all/year/month; all có period_start null. | batch_run_id, batch_cutoff_time, period_type, period_start nullable, movieId, title, interaction_count, rating_count, rating_sum, avg_rating, rank_in_period | D1; writer P2, reader P1 |
| analytics_genre_trends | (batch_run_id, period_type, period_start, genre). period_type = day/month/year. | batch_run_id, batch_cutoff_time, period_type, period_start, genre, interaction_count, rating_count, rating_sum, avg_rating, rank_in_period | D3; writer P3, reader P1/P3 |
| analytics_growth | (window_size, window_start, entity_type, entity_key). entity_type = movie hoặc genre; entity_key là **chuỗi** movieId hoặc tên genre, giữ thêm movieId/genre nullable. | window_start, window_end, window_size, entity_type, entity_key, movieId nullable, genre nullable, title nullable, title_source nullable, current_window_count, previous_window_count nullable, previous_window_present boolean, trending_score nullable, rating_count, rating_sum, avg_rating, updated_at, timeline_mode | D4, P4 |
| analytics_tmdb_signal | tmdbId (MVP giữ snapshot mới nhất); trong tương lai nếu giữ lịch sử thì phải version theo (tmdbId,event_time) | tmdbId, event_time, popularity, vote_count, vote_average, prev_popularity nullable, popularity_delta nullable, updated_at, title nullable, source = tmdb | D5; writer P5, reader P1/P5 |

**Quy tắc period_type:** Kỳ month là tháng UTC lịch; year là năm UTC lịch; day là ngày UTC. period_start của day/month/year là mốc đầu kỳ UTC (kiểu date), all là null. Một record day không được cộng thêm vào month/year trong cùng biểu đồ: mỗi dashboard lọc đúng **một period_type**. Với analytics_timeseries entity_type=all được tính từ các dòng phim-ngày cùng kỳ, không cộng song song với các dòng entity_type=movie. D1 all dùng **một** hàng (period_type=all, entity_type=all) để có 100.836 interaction, 9.724 distinct_movie_count và avg_rating; không suy số phim từ Top 10. analytics_top_movies lưu **đủ mọi phim có rating trong từng kỳ**, không chỉ 10 phim đầu; Tableau chỉ hiển thị 10 hàng có rank nhỏ nhất. Bộ lọc khoảng ngày tùy ý không align kỳ là yêu cầu mới, cần query khác hoặc mở rộng contract.

**Quy tắc xếp hạng:** rank_in_period do Spark SQL tính theo interaction_count giảm dần; khi hòa thì movieId tăng dần hoặc genre tăng dần theo chữ cái. Dùng ROW_NUMBER để có thứ tự ổn định. Giữ tổng count/sum trước khi xếp hạng; không áp LIMIT 10 trước khi tính KPI số phim.

**Quy tắc window:** current_window_count = interaction_count của cửa sổ hiện tại. previous_window_start = window_start - window_size; tìm theo cùng entity_key và size. Nếu không có tài liệu kỳ trước, previous_window_present=false, previous_window_count/trending_score=null; Tableau ghi “chưa có kỳ trước”. Không dùng updated_at để sắp thứ tự cửa sổ. timeline_mode = replay_history cho bộ dữ liệu MovieLens hiện hành; chuyển sang live_requires_cutoff_verified chỉ sau khi có dữ liệu mới và kiểm tra T.

**Tên phim ở D4:** Realtime output hiện chỉ có movieId. P4 được gắn title để hiển thị từ canonical reference có version truy vết được; với bộ replay hiện tại có thể LEFT JOIN batch_movie theo movieId để lấy **title mà thôi**, đặt title_source = batch_movie/<batch_run_id>. Không lấy total_count, avg_rating hay các metric Batch vào analytics_growth. Nếu một movieId RT không có title hoặc version chưa được xác minh, giữ movieId và title null, Tableau hiển thị “MovieLens ID <movieId>”. Không dùng title từ TMDB bằng phép nối ID số.

**Quy tắc snapshot:** Không biến một file tmdb_realtime chỉ chứa trạng thái hiện tại thành chuỗi thời gian. Có thể sắp xếp 20 phim theo popularity/vote_count/vote_average. title null được hiện bằng “TMDB ID <tmdbId>”; không dùng nhan đề của phim MovieLens có ID số tình cờ giống. Nếu nhiều snapshot đến, chỉ công bố dòng có event_time lớn nhất của từng tmdbId sau khi kiểm tra logic phiên bản/trạng thái của Module 05.

### 7.1. Kiểu dữ liệu, khóa và công bố analytics

| Nhóm field | Kiểu bắt buộc trong Spark SQL / ES | Quy tắc null và hiển thị |
|---|---|---|
| batch_run_id, period_type, entity_type, entity_key, window_size, timeline_mode, source, genre, title_source | string / keyword (genre có thể thêm text subfield nếu cần tìm kiếm) | title_source null khi không có enrichment; các field khóa bắt buộc không null trừ trường nullable được nêu ở mục 7. |
| title | string / text + keyword | Nullable khi thiếu reference; chỉ là nhãn, không thuộc khóa/metric. |
| movieId, tmdbId, rank_in_period, distinct_movie_count | integer / integer | movieId null với entity_type=all/genre; distinct_movie_count chỉ có trên dòng all; rank_in_period không null trên bảng có rank. |
| interaction_count, rating_count, current_window_count, previous_window_count, vote_count | long / long | Các count bắt buộc không âm; previous_window_count null khi previous_window_present=false. |
| rating_sum, avg_rating, trending_score, popularity, vote_average, prev_popularity, popularity_delta | double / double | Giữ null theo schema; Tableau không biến null thành 0 đối với score, avg thiếu mẫu hoặc delta thiếu snapshot. |
| period_start, batch_cutoff_time, window_start, window_end, event_time, updated_at | date hoặc timestamp UTC / ES date | period_start null cho all; date gốc chỉ tới ngày, timestamp có Z; không dùng chuỗi locale làm khóa thời gian. |
| previous_window_present | boolean / boolean | false khi không quan sát được cửa sổ trước; trường hợp này trending_score null. |

Mỗi analytics index dùng _id xác định từ **toàn bộ tuple khóa** ở bảng mục 7. P5 cung cấp một cách mã hóa tuple dùng chung và P2-P5 áp dụng có phân biệt null, kiểu dữ liệu và thứ tự thành phần (ví dụ JSON array chuẩn UTF-8 rồi băm SHA-256); cung cấp hàm chung và test cùng khóa → cùng ID. Không để Spark SQL sinh ID ngẫu nhiên hay upsert của hai kỳ vào cùng ID. Mỗi document analytics mang metadata nguồn tương ứng: Batch giữ batch_run_id/T; Realtime giữ window_size/window_start và timeline_mode; TMDB giữ event_time/source.

P2/P3 ghi physical analytics theo run bằng cách đọc **physical source indices theo run**, không đọc alias đang công bố. P5 kiểm tra đủ ba source và ba analytics physical, tất cả cùng run/T/ref_version, rồi đổi **sáu alias** trong một yêu cầu như mục 4. P1 nhận publication marker và chỉ refresh sau khi đối chiếu sáu alias. Không tạo quãng phục vụ có source run mới nhưng analytics run cũ; nếu một phần lỗi, giữ trọn bộ cũ. Khi Tableau đọc nhiều nguồn qua nhiều request, mỗi workbook/extract phải pin cùng run; nếu marker thay đổi giữa chừng thì làm lại refresh toàn bộ, không ghép kết quả khác run. Với analytics_growth và analytics_tmdb_signal, P4/P5 cập nhật theo _id ổn định với kiểm tra freshness/phiên hoặc công bố snapshot hoàn chỉnh; không để Tableau đọc một đợt ghi dở. P4/P5 ghi tần suất cập nhật và timestamp nguồn vào runbook; không suy ra SLA từ trigger Spark.

P2-P5 công bố schema fixture trước; sau khi ES sẵn sàng, kiểm tra đầu ra thật đúng schema và các ví dụ đối chứng. Cách Spark SQL đọc/ghi Elasticsearch (connector hoặc adapter) là lựa chọn triển khai của Module 07, nhưng phải được ghi version, endpoint, lệnh chạy, quyền đọc/ghi, chế độ retry và kiểm tra count trong runbook; không được giả định chỉ tạo temp view trong bộ nhớ là Tableau đã có dữ liệu.

### 7.2. Giao diện hạ tầng dùng chung, trạng thái công bố và tính mới

| Thành phần P5 cung cấp | Quy ước cố định cho P2-P5 | Bài kiểm tra giao diện |
|---|---|---|
| ES môi trường chung | Endpoint, version, health, template/mapping, quyền writer theo index và reader, namespace dev tách canonical; thông tin truy cập ngoài repo. | Mỗi owner tạo/đọc dev index của mình, Bulk item lỗi bị bắt, không ghi index của người khác. |
| Spark SQL runner | Cùng Spark/connector version, SparkSession UTC, tên temp view mục 5, API đọc index, xuất physical analytics, logging/retry. | Một fixture input → temp view → dev analytics và đối chiếu schema/count; test chạy lặp không đổi _id. |
| Key serializer | Serialize tuple có thứ tự, type-tag và null, UTC ISO, canonical UTF-8; tên và version hàm được đóng băng tại K0. | Test cross-lane cùng tuple/khác tuple, diacritics genre, movieId số khác chuỗi, null khác chuỗi 'null'. |
| Batch publisher | READY P2/P3 cho sáu physical index; manifest bất biến; đổi sáu alias cùng một yêu cầu, kiểm `errors/action_results` và ánh xạ thực tế; marker run/T/ref_version/manifest checksum và rollback. | Thiếu 1 index, lỗi Bulk, sai T, một alias action lỗi hay Analytics chưa xong đều không phát hành marker mới; nếu chuyển một phần thì rollback/khóa refresh; nhiều request pin một run. |
| Analytics publisher | Owner P2-P5 tạo dữ liệu; P5 cấp hàm publish/rollback và trạng thái; P1 chỉ đọc bản hoàn chỉnh. | Alias/schema/row count và metadata đúng; không xuất bản index đang ghi dở. |

Mỗi analytics record cần chỉ ra nguồn phiên bản: Batch có batch_run_id và batch_cutoff_time; MovieLens RT có window_start/end, window_size, updated_at, timeline_mode; TMDB có tmdbId, event_time, updated_at, source. Không nhập lại snapshot cũ để thay bản mới theo thời gian xử lý. `updated_at` là lúc pipeline xử lý, **không thay** event_time làm mốc nghiệp vụ. P4/P5 đo và ghi độ trễ quan sát được từ source event_time tới cập nhật index và tới Tableau; chưa đo thì ghi “chưa xác định”, không cam kết realtime theo phút.

**TMDB live:** P5 xác minh producer crawl định kỳ thực sự phát bản ghi vào Kafka `tmdb-snapshots`, với cùng tmdbId ở tối thiểu hai lần fetch khác event_time; `prev_popularity` là popularity của snapshot hợp lệ liền trước theo event_time, `popularity_delta = popularity - prev_popularity`. Bản đầu giữ cả hai null. Event trùng ID được bỏ qua; bản cũ đến muộn không được ghi đè latest. Trường hợp timestamp bằng nhau nhưng nội dung khác phải từ chối/cách ly hoặc có tie-break xác định được ghi test; không nhận một thứ tự ngẫu nhiên. Sau restart phải khôi phục latest/previous từ state bền vững hoặc raw event log và kiểm tra delta; nếu chưa đáp ứng thì công bố `snapshot_pass`, không đánh dấu `live_pass`. `tmdb_realtime` chỉ lưu trạng thái mới nhất từng tmdbId; muốn chuỗi lịch sử phải đọc Kafka/raw snapshots hoặc đổi contract. D5 dùng dấu thời gian snapshot thực và trạng thái nguồn. Không đặt ngưỡng freshness/SLA giả khi producer chưa được đo.

**Chặn ghi lùi và lỗi âm thầm:** `updated_at` không chứng minh một bản ghi mới hơn về nghiệp vụ. P4 phải chứng minh update cùng window dùng state/phần dữ liệu lũy kế bền vững và thứ tự phiên tính được kiểm tra; P5 phải so `event_time` của TMDB mới với bản đã lưu **tại thời điểm ghi ES** (hoặc publish snapshot version bất biến), không chỉ kiểm tra bản sao trong RAM trước Bulk. Retry một micro-batch không được làm count giảm hoặc ghi `prev_popularity` thành null. Kết quả Bulk `errors=true`, item failure hay mất kết nối phải **làm job thất bại/quarantine rồi retry an toàn**, không chỉ ghi warning và tiếp tục checkpoint; nếu còn cơ chế cũ thì G1B/G1C live chưa pass. Hai owner ghi trạng thái live theo từng nhánh riêng.

### 7.3. Giao diện Tableau xác định từ K0

Đường bàn giao **tối thiểu** là P5 cung cấp hàm export read-only từng analytics alias sang **CSV UTF-8 có header, quote theo RFC 4180**, kèm một manifest cho mỗi bản export; P1 dùng năm CSV này làm data source Tableau Desktop và refresh sau khi có phiên hoàn chỉnh. Export là bản sao 1:1 của năm analytics index, **không phải một bộ số liệu tính lại**. P5 phải đọc đủ mọi trang kết quả ES (không giới hạn ở trang đầu), pin physical index/run khi export Batch, dùng snapshot đọc nhất quán cho RT, so document count trước/sau, kiểm `_id` trùng và đối chiếu hash/count. P2-P5 không tự tạo CSV riêng cho trang của mình. Nếu nhóm đã thử và chốt connector đọc thẳng ES tương đương, chỉ đổi phương thức ở G0 bằng change request với smoke test năm nguồn; schema và provenance vẫn giữ nguyên.

| File/manifest | Contract cho P5 và P1 | Kiểm tra |
|---|---|---|
| `analytics_<name>.csv` | Tên cột, null và kiểu theo mục 7/7.1; timestamp UTC ISO 8601, date `YYYY-MM-DD`, số theo dấu chấm thập phân; null xuất ô trống, 0 xuất `0`; CSV chứa đúng một phiên bản index và không nhân dòng. | P1 so field/type, số dòng, tổng `interaction_count` đúng grain với query ES; không lấy NULL thành 0 cho score/delta. |
| `tableau_export_manifest.json` | Với mỗi file: analytics alias, physical index hoặc source checkpoint, schema_version, row_count, SHA-256, `exported_at`; ba Batch exports cùng `batch_run_id/T/ref_version/manifest_hash`; RT có `max_updated_at` và snapshot/window cuối. | P1 từ chối ghép D1-D3 khi khác run hoặc file/checksum chưa khớp; D4/D5 ghi rõ mốc nguồn và mốc refresh. |
| Publish và refresh | P5 xuất ba Batch CSV vào thư mục bất biến `batch/<run_id>/`, kiểm count/hash, rồi công bố **một manifest pointer** tới thư mục đó; P1 pin đúng thư mục/run trước khi refresh cả ba nguồn. D4/D5 xuất version riêng sau khi analytics của lane đã publish. | Không ghi đè file đang được đọc; không ghép CSV cũ/mới; giữ thư mục run trước để rollback; demo cuối không dùng fixture. |

TMDB producer, Kafka, Spark và analytics có thể đang chạy định kỳ trong khi Tableau Desktop chưa refresh. D5 phải hiện **event_time của snapshot và thời điểm Tableau refresh**; chỉ gắn nhãn “dữ liệu từ luồng crawl định kỳ” khi G1C live_pass, không quảng cáo màn hình tự cập nhật theo phút nếu chưa có job export/refresh được kiểm thử. P1 và P5 kiểm tra một lần refresh qua hai snapshot thật; nếu chưa có cơ chế refresh tự động, công bố rõ thao tác refresh thủ công trong runbook.

## 8. Dashboard consumption contract

| Dashboard | Analytics được phép đọc | Bộ lọc/tương tác | KPI hoặc so sánh đối chứng |
|---|---|---|---|
| D1 | analytics_timeseries với entity_type=all; analytics_top_movies | period_type và period_start theo kỳ được công bố; chọn năm/tháng tác động Top | all: 100.836 rating, 9.724 phim; avg_rating ≈ 3,50156/5. |
| D2 | analytics_timeseries với entity_type=movie | chọn 1-3 movieId, day/month/year, timeline | title từ Batch; các kỳ thiếu hiển thị 0 lượt và điểm TB null. |
| D3 | analytics_genre_trends | day/month/year, genre, highlight | Drama có 41.928 lượt gắn thể loại toàn kỳ; tổng nhóm có thể vượt 100.836. |
| D4 | analytics_growth | một window_size, một khoảng event_time, entity_type | Với cửa sổ 2017-05-03T21:00:00Z kích thước 1h: 566 rating trên 566 phim; không gọi là “hiện tại”. |
| D5 | analytics_tmdb_signal | sắp xếp popularity/vote_count/vote_average | 20 phim tại một snapshot; prev/delta null; nhãn ID thay title. |

Nếu Dashboard không có index analytics tương ứng, dùng fixture **chỉ để phát triển**. Không trình bày kết quả fixture thành dữ liệu chạy end-to-end trên ES. Mỗi worksheet/tooltip cần chỉ rõ chỉ số, đơn vị, mốc thời gian và nguồn.

### 8.1. Chín truy vấn tối thiểu của hợp đồng nền

Đây là **giao diện Module 07**, kể cả khi 5 dashboard Phase 2 không hiển thị hết. Query có thể là hàm/job Spark SQL trả DataFrame có test và dữ liệu đối chứng; chỉ các output ở mục 7 mới bắt buộc materialize vào ES cho Tableau. Không làm các query này bằng calculated field riêng trong Tableau.

| # | Truy vấn bắt buộc | Input và quy tắc | Owner / đầu ra kiểm tra |
|---|---|---|---|
| 1 | Top phim theo interaction | batch_movie_daily trong kỳ, tie-break movieId; đối chiếu batch_movie kỳ all | P2; analytics_top_movies và test |
| 2 | Rating TB theo phim | SUM(rating_sum)/SUM(rating_count), từ batch_movie_daily hoặc batch_movie kỳ all | P2; analytics_timeseries/top_movies và test |
| 3 | Genre phổ biến theo ngày/tuần/tháng | batch_genre, group UTC, đa nhãn; tuần dùng chuẩn ISO week bắt đầu thứ Hai UTC và ghi rõ khoảng | P3; analytics_genre_trends có day/month/year cho dashboard và query riêng theo week |
| 4 | Xu hướng movie 7 ngày | Hai hoặc nhiều khoảng 7 ngày liền kề trên timeline Batch, chỉ các ngày nằm trước T; gap được ghi rõ | P2; hàm/query + test kỳ mẫu |
| 5 | Xu hướng movie 30 ngày | Hai hoặc nhiều khoảng 30 ngày liền kề, dùng **30 ngày liên tiếp**, không thay bằng tháng lịch | P2; hàm/query + test kỳ mẫu |
| 6 | Week-over-week growth | Hai tuần ISO liền kề có mốc đầu/cuối và cùng độ dài 7 ngày; (current-previous)/(previous+1) | P2; hàm/query + test kỳ mẫu |
| 7 | Top phim đang tăng | Hai window RT liền kề, cùng 10m hoặc 1h; score và count gốc, không xếp phim thiếu kỳ trước như có growth | P4; analytics_growth và test |
| 8 | Batch vs Realtime | Hai nhánh giữ nhãn source/T; query ghép **chỉ khi** RT window_start >= T và một window_size; cũng hỗ trợ bảng đối chiếu tách nguồn không cộng | P4 với P2; test fixture hai phía T. Với output hiện tại RT sau T rỗng: không tạo tổng giả. |
| 9 | Current TMDB signal | Snapshot mới nhất từng tmdbId theo event_time; popularity/votes và delta nullable, không gộp vào MovieLens | P5; analytics_tmdb_signal và test |

Các query 4-6 là một phần nghiệm thu Module 07 dù không thêm vào D2, để D2 giữ nhiệm vụ so sánh xu hướng từng phim. Query 8 phải có test T nằm giữa hai tập dữ liệu fixture, vì bộ output hiện tại không có cửa sổ MovieLens sau T.

### 8.2. Tham số và đầu ra chuẩn cho query chưa materialize

| Query | Tham số bắt buộc | Cột kết quả tối thiểu | Cách xử lý biên |
|---|---|---|---|
| P2 7d/30d | `batch_run_id`, `movieId`, `period_end_utc` là 00:00Z, `duration_days` = 7 hoặc 30. | `movieId`, `period_start`, `period_end`, `previous_start`, `previous_end`, `current_count`, `previous_count`, `growth`, `batch_run_id`, `batch_cutoff_time`, `coverage_status`. | Current `[end-Nd,end)`, previous `[end-2Nd,end-Nd)`; `end <= T`; không thay 30 ngày bằng tháng lịch; chưa chứng minh nguồn đủ kỳ thì `coverage_status=unverified` và không tuyên bố đây là xu hướng đầy đủ. |
| P2 WoW | `batch_run_id`, `movieId`, `iso_week_start_utc` là thứ Hai 00:00Z. | Các cột như trên, `duration_days=7`; start/end của tuần hiện và tuần trước. | Dữ liệu đúng hai tuần ISO liền kề, `current_end <= T`; không dùng tuần trong Tableau do locale quyết định. |
| P3 genre week | `batch_run_id`, `iso_week_start_utc` hoặc phạm vi tuần, `genre` tùy chọn. | `genre`, `period_start`, `period_end`, `interaction_count`, `rating_count`, `rating_sum`, `avg_rating`, `batch_run_id`. | Mỗi tuần nửa mở thứ Hai → thứ Hai UTC; cộng từng genre độc lập, không coi tổng genre là tổng ratings. |
| P4 Batch vs RT | `batch_run_id`, `T`, `window_size` = 10m/1h, entity tùy chọn. | `source`, `entity_key`, `period_start`, `period_end`, `interaction_count`, `rating_sum`, `rating_count`, `batch_run_id`/`window_size`, `timeline_mode`. | Batch `<T`; chỉ RT `window_start >= T` và cùng size. Bộ replay hiện tại trả RT sau T rỗng; fixture phải có sau T để test cộng không trùng. |

Với query growth của P2, `growth=(current_count-previous_count)/(previous_count+1)`. Nếu kỳ trước không có rating của phim nhưng nguồn kỳ đó **đã xác minh đầy đủ**, `previous_count=0` là 0 quan sát được; khi thiếu nguồn, trả null và `coverage_status=unverified`. D4 giữ quy tắc khác trong mục 7: thiếu hẳn document window trước thì `previous_window_present=false`, score null. Mọi query có test đúng mốc nửa mở, số 0, dữ liệu thiếu, year boundary và chạy lặp cùng run.

## 9. Fixture và phát triển song song

Mọi người bắt đầu K0, không đợi index thật. Fixture có đúng schema và khóa của index thật, đặt trong `contracts/phase2/<lane>/fixtures/` (`movie_batch`, `genre_batch`, `movielens_realtime`, `tmdb_realtime`); schema tương ứng ở `contracts/phase2/<lane>/schemas/`. P5 giữ fixture cho helper chung ở `contracts/phase2/common/fixtures/`. Mỗi lane có README ghi schema version, người tạo, thời gian và nhãn **fixture, không phải dữ liệu production**. P1 thiết kế Tableau bằng fixture; trước demo cuối phải đổi sang analytics thật và đối chiếu field/type cùng một KPI.

| Owner tạo | Gói fixture tối thiểu và edge case bắt buộc | Giao cho / dùng ngay | Bằng chứng khi đổi sang dữ liệu thật |
|---|---|---|---|
| P2 | Hai index nguồn Batch phim (mỗi index 2-3 record cùng run/T); analytics_timeseries đủ day/month/year/all, movie/all, movieId null đúng chỗ, distinct_movie_count; analytics_top_movies hai kỳ hòa điểm cần tie-break; hai phim, kỳ trống; hai cặp 7d/30d và tuần ISO. | P1 dựng D1; P2 dựng D2 và query 1,2,4-6; P4 nhận một cặp test Batch trước T. | Loader 9.724/99.538, mỗi bảng 100.836 interaction; SQL all 100.836 và 9.724 phim; query kỳ mẫu khớp TSV. |
| P3 | genre_trends 2-3 record cùng run/T P2, UNKNOWN; analytics_genre_trends ít nhất hai genre, hai kỳ, hòa count, genre đa nhãn, ranh giới tuần ISO. | P3 dựng D3 và query 3; P1 đặt layout; P5 thử publisher sáu alias. | Loader 32.894 record/274.480 lượt gắn; một genre/kỳ truy ngược TSV; không giả sử tổng genre bằng tổng phim. |
| P4 | Hai source RT dev index cho 10m và 1h; analytics_growth có previous hiện diện, thiếu previous, previous=0 thật; fixture T nằm giữa Batch và RT, cùng entity ở hai window liền kề, title thiếu. | P4 dựng D4 và query 7/8; P2 cung cấp phía Batch; P1 bố trí nhãn replay. | Source 201.627/127.770 docs trong snapshot cô lập; đối soát theo size, score và raw count; test restart riêng. |
| P5 | tmdb_realtime 20 record baseline; thêm tối thiểu hai event cùng tmdbId khác event_time, event trùng và cũ đến muộn; analytics_tmdb_signal null delta trước, delta đúng sau; ES/Spark SQL smoke fixture. | P5 dựng D5, query 9; P1 đặt trạng thái snapshot/live; P2-P4 thử runtime. | Theo dõi hai crawl thật qua Kafka → Streaming → ES → SQL → D5; chứng minh dedup, latest, prev, restart hoặc đánh dấu snapshot_pass. |
| P1 | Template và một workbook 5 trang dùng fixture của P2-P5; bộ kiểm tra format UTC, null và nhãn nguồn. | P2-P5 dùng để làm trang của mình ngay K0. | Thay cả năm data source bằng index analytics đã publish, có biên bản không còn nguồn fixture. |

Không nạp fixture tĩnh vào canonical `movie_realtime_stats`, `genre_realtime` hoặc `tmdb_realtime`; dùng namespace dev cách ly để không tranh writer với Spark Streaming. Các fixture và schema được duyệt tại G0; sửa schema phải cập nhật tất cả fixture và worksheet liên quan.

## 10. Kiểm tra nghiệm thu tại từng giao diện

| Gate | Bài kiểm tra bắt buộc | Chủ trì / người ký nhận |
|---|---|---|
| G0 - schema và nền | Sáu source + năm analytics, grain, quyền writer, _id, field nullable, fixture, API runner, template Tableau; bốn owner bắt đầu song song. | P1 điều phối; P5 giao nền; P2-P5 ký phần của mình. |
| G1A - Batch physical | P2 phim 9.724/99.538, P3 genre 32.894 cùng run/T/ref_version; sum phim 100.836, sum genre 274.480, Bulk item lỗi bằng 0, idempotent rerun; giữ cả ba source ở physical, chưa đổi alias. | P2/P3 giao physical, P5 kiểm tra; công bố tại G2P. |
| G1B - MovieLens RT | Hai canonical indices do Spark Streaming ghi; P4 kiểm tra _id/upsert, 10m và 1h riêng, source count; static snapshot pass tách Kafka live/restart pass, không tuyên bố hiện tại với replay trước T. | P4 giao và ký; owner M05 phối hợp sửa state. |
| G1C - TMDB | P5 kiểm tra producer periodic → Kafka → Spark → ES, >=2 crawl thực có chung tmdbId, latest/prev/delta, duplicate, old event, restart, Bulk item; baseline 20 record chỉ snapshot_pass. | P5 giao và ký; owner producer/M05 phối hợp. |
| G2 - ES → Spark SQL | P5 kiểm tra runner/temp views; P2/P3 giao ba physical analytics Batch tính từ ba physical source cùng run; P4/P5 giao hai RT analytics; chín query mục 8.1 có test, count/weighted average/window/null đúng. | P5 ký nền; P2 ký 1,2,4-6; P3 ký 3; P4 ký 7/8 với P2; P5 ký 9. |
| G2P - công bố Batch | P5 kiểm tra sáu physical Batch index cùng manifest/run/T/ref_version, query đối chứng, không có item Bulk lỗi; đổi sáu alias trong một yêu cầu, kiểm từng action và đọc lại mapping; lỗi một phần thì khóa marker/rollback; P1 pin run cho toàn bộ refresh. | P2/P3 ký số liệu; P5 ký công bố; P1 nhận marker. |
| G3 - Tableau | Mỗi người dựng D1-D5 theo template, chỉ đọc analytics, filter/tooltip/UTC/null/provenance đúng, không còn fixture ở workbook cuối. | P2-P5 giao trang; P1 kiểm và tích hợp. |
| G4 - demo tổng | Một workbook 5 dashboard, truy vết KPI qua file/Kafka → ES → SQL → Tableau, hiển thị giới hạn replay/snapshot/live; có cấu hình, runbook, lỗi và trạng thái nghiệm thu từng luồng. | P1 chủ trì, cả năm ký. |

**Trace đối chứng:** Forrest Gump (movieId 356) có 329 rating all trên Batch; P2 chọn thêm một tháng thực từ movie_daily_stats và truy tới analytics_timeseries → D2. P3 chọn một kỳ của Drama (41.928 lượt gắn toàn kỳ) → analytics_genre_trends → D3. P4 chọn một window 1h → analytics_growth → D4. P5 chọn một tmdbId qua hai crawl thực → tmdb_realtime → analytics_tmdb_signal → D5. P1 đối chiếu D1 all 100.836, 9.724 phim. Trace minh họa không thay kiểm tra toàn bộ dữ liệu.

## 11. Đặc tả lỗi và hành vi an toàn

| Trường hợp | Hành vi bắt buộc |
|---|---|
| Thiếu manifest, sai count hoặc bất kỳ source/analytics Batch job thất bại | P2/P3 không đánh dấu READY; P5 giữ nguyên **sáu alias** của run cũ (nếu có), log nguyên nhân và không refresh Tableau. |
| ES Bulk trả HTTP success nhưng từng item có lỗi | Thống kê failed item, lưu ID/lý do, retry có kiểm soát; không thông báo nạp xong chỉ dựa vào status HTTP. |
| RT upsert lại cùng window | Cập nhật cùng _id; không nhân đôi document hay cộng lặp analytics. Idempotent _id không tự phục hồi state cộng dồn bị mất sau restart; phải kiểm thử cả count. |
| Kafka/Spark restart giữa hai micro-batch | State hiện còn trong RAM; G1 live MovieLens cần P4/owner M05 chứng minh phục hồi dedup/cumulative window; G1 live TMDB cần P5/owner M05 chứng minh latest/delta; snapshot JSONL chỉ được ghi snapshot_pass. |
| SQL thấy 2 batch runs | Khi build analytics chỉ đọc ba physical source của run mục tiêu; khi phục vụ chỉ đọc sáu alias của cùng run đã công bố; lệch marker hoặc metadata thì từ chối, không trộn run_id. |
| Thiếu previous window | trending_score null và previous_window_present=false; không nhầm với kỳ có 0 rating được quan sát. Dùng exact previous_window_start, không lấy kỳ có dữ liệu gần nhất nhưng cách nhiều giờ/ngày. |
| Viết analytics đứt giữa chừng | Batch: không đổi sáu alias, giữ run cũ; RT: không publish snapshot dở hoặc stale update. Ghi run/version, lỗi và chạy lại bằng cùng khóa. |
| Thiếu title TMDB hoặc thiếu snapshot trước | Hiển thị ID và “chưa đủ dữ liệu”; không gọi API ngoài hoặc ghép ngẫu nhiên trong Tableau. |
| TMDB event trùng, cũ đến muộn hoặc bằng event_time nhưng khác nội dung | P5 dedup theo event identity, chặn bản cũ overwrite; đồng thời gian khác payload phải được cách ly hoặc tie-break đã kiểm thử; không tính lại delta trên trạng thái sai. |
| Có ít nhất hai TMDB snapshots nhưng restart làm mất previous | P5 không công bố live_pass và không hiện delta chưa chứng minh được; phục hồi từ state bền vững hoặc tái dựng từ raw trước khi refresh analytics. |
| Filter trộn period_type hay window_size | Từ chối/giới hạn filter để chỉ một độ chi tiết được tính mỗi lần. |
| Reference version khác giữa các nhánh | Ghi rõ version, xác minh khả năng so sánh genre trước khi kết luận xuyên hai view. |

## 12. Nguồn tham khảo và ranh giới tái sử dụng

- [Website What's Seattle Reading?](https://www.whats-seattle-reading.com/): tham khảo tương tác Overview, Checkout Trends, Most Borrowed, Genres và Backlist Boomerangs; không lấy dữ liệu sách làm dữ liệu phim.
- [Elasticsearch Aliases API](https://www.elastic.co/docs/manage-data/data-store/aliases): một request hỗ trợ nhiều hành động; response nhiều action cần kiểm tra lỗi riêng.
- [Tableau Text File](https://help.tableau.com/current/pro/desktop/en-us/examples_text.htm) và [Refresh Data Sources](https://help.tableau.com/current/pro/desktop/en-us/refreshing_data.htm): CSV dùng được trong Desktop; file phẳng đã xuất không tự bảo đảm refresh trên Tableau Cloud/Server.
- Hợp đồng nền DATA_CONTRACT.md của repo movie-trend, đặc biệt mục 5.4, 10-14, 18-21.
- Batch_layer_output/README.txt và manifest.json: run, cutoff, schema export, row count.
- Mã nguồn modules/mapreduce/src, modules/spark_streaming/src và sáu file output: phép tổng hợp thực tế và các giới hạn được quan sát.

## 13. Checklist trước khi công bố Phase 2

**Trạng thái kiểm tra tại ngày chốt:** sáu file output và mã nguồn upstream đã được đối chiếu tĩnh; **chưa có bằng chứng** rằng Elasticsearch, Spark SQL analytics, exporter CSV hoặc workbook đã chạy end-to-end theo hợp đồng này. Không đánh dấu các ô dưới đây pass chỉ vì đã viết tài liệu.

| Rủi ro quan sát từ mã nguồn/bộ file hiện có | Owner giải quyết | Điều kiện đóng |
|---|---|---|
| `modules/spark_streaming/src/es_sink.py` hiện chỉ warning với Bulk item lỗi/mất kết nối; job có thể tiếp tục. | P4 cho MovieLens, P5 cho TMDB, phối hợp owner Module 05. | Lỗi từng item làm batch fail/retry hoặc quarantine được đối soát, không mất dữ liệu sau checkpoint. |
| `streaming_job.py` giữ seen event IDs, window states và TMDB previous trong RAM. | P4 sửa/test movie/genre; P5 sửa/test TMDB, phối hợp Module 05. | Restart giữa micro-batch và replay event không đổi count/latest/delta; nêu nơi lưu state, cách recovery. |
| `tmdb_realtime.jsonl` chỉ có một snapshot 20 dòng, prev/delta null. | P5. | Hai lần crawl thật cùng ID đi qua Kafka → ES → analytics → D5, với bản cũ/trùng đến muộn và restart test. |
| ref_version của reference trong mã nguồn khác manifest Batch. | P2/P3 xác minh run; P4 chỉ enrich title khi xác định provenance. | READY report và publication marker giữ ref_version của manifest; không join/reference ngầm sai version. |
| Chưa có publisher sáu alias, Spark SQL runner và exporter analytics → Tableau. | P5; P1 ký nhận giao diện Tableau. | G2P và G3 có lệnh chạy, test trường hợp thiếu index/partial alias action, CSV đủ dòng/kiểu và refresh đúng phiên. |

- [ ] G0: P1 duyệt template và fixture; P5 giao ES/Spark SQL/ID helper; P2-P5 ký schema, grain, writer, timeline và tests của mình.
- [ ] G1A: P2 hai physical Batch phim; P3 một physical genre cùng run/T/ref_version; P5 kiểm tra, giữ staging, chưa publish.
- [ ] G1B: P4 xác nhận MovieLens RT writer canonical, _id, Bulk item, 10m/1h; công bố riêng snapshot_pass và live_pass có restart test.
- [ ] G1C: P5 xác nhận TMDB producer crawl định kỳ, hai fetch thật, Kafka/Streaming/ES, dedup/latest/previous/delta, restart; nếu chưa đạt chỉ ghi snapshot_pass.
- [ ] G2: P2 hai physical analytics phim và query 1/2/4-6; P3 physical genre analytics và query 3; P4 growth và query 7/8 với P2; P5 TMDB và query 9; năm analytics đúng schema.
- [ ] G2P: P5 đổi sáu Batch aliases bằng một yêu cầu sau READY đầy đủ; P1 chỉ refresh khi sáu alias và publication marker cùng run.
- [ ] G3: P1 nhận bốn trang P2-P5, dựng D1 và kiểm tra một workbook chỉ đọc analytics thật, filter, đơn vị, UTC, nhãn replay/live và null.
- [ ] G4: cả năm ký runbook, trace, giới hạn, export và bản demo cuối.

## 14. Quản lý thay đổi

Mọi yêu cầu đổi index, field, grain, công thức, cutoff, cách dùng reference hoặc tên dashboard phải có một dòng change request gồm: **vấn đề; đề xuất cũ → mới; owner; tác động P1-P5; kế hoạch migrate fixture/index/worksheet; bài kiểm tra; ngày chốt**. Owner phần dữ liệu và người nhận cùng xác nhận trước khi đổi code. Không tự sửa một dashboard để che chênh lệch Query Layer.

Các mở rộng có thể làm sau Phase 2: bổ sung title TMDB vào output upstream; dữ liệu MovieLens sau T để hiển thị “hiện tại” đúng nghĩa; dashboard ghép Batch + Realtime theo cutoff; phát hiện phim cũ nổi lại nhiều năm với định nghĩa tuổi phim và ngưỡng mẫu rõ ràng.

### Nhật ký cập nhật Phase2-2.0 (15/11/2026)

1. Đổi sang ownership dọc P2 phim Batch, P3 genre Batch, P4 MovieLens Streaming, P5 TMDB live; P1 chỉ dựng D1 trên analytics P2 và tích hợp Tableau.
2. P5 đảm nhận hạ tầng Elasticsearch và Spark SQL dùng chung, serializer và publisher; không viết thay loader/SQL/dashboard của P2-P4. P2/P3 độc lập dựng sáu physical Batch; P5 chuyển sáu alias nguồn + analytics cùng run/T.
3. Bổ sung giao diện từng luồng, fixture song song, trạng thái nghiệm thu snapshot/live riêng, test restart và điều kiện có hai lần crawl thật TMDB để tính delta.

### Nhật ký rà soát Phase2-2.1 (15/11/2026)

1. Phát hiện khoảng lệch phiên giữa source Batch và analytics khi công bố theo hai bước; đổi sang P2/P3 dựng sáu physical index theo cùng run và P5 công bố sáu alias cùng một lần sau G2. P1 refresh theo publication marker đã kiểm tra.
2. Bổ sung kiểm lỗi từng action của Elasticsearch Aliases API, rollback nếu kết quả một phần, kiểm nguồn vật lý, schema, READY report và phiên bản ref/manifest.
3. Chốt đường bàn giao Tableau Desktop bằng export CSV có phiên bản, manifest và refresh tường minh; không xem CSV là live connection. Bổ sung định nghĩa query 7d/30d/WoW/ISO week, kiểm state/Bulk/late TMDB và danh sách điểm chưa đạt cần triển khai.

### Nhật ký cấu trúc Phase2-2.2 (15/11/2026)

1. Dẫn chiếu cây thư mục và owner tại Kickoff mục 3.1-3.2; mỗi lane sở hữu schema/fixture, mã ES/SQL, dashboard và runbook của mình.
2. Chuyển đường dẫn fixture sang `contracts/phase2/<lane>/fixtures/`; phần dùng chung của P5 ở `contracts/phase2/common/`. Không thay tên index, khóa, field hoặc metric.

### Nhật ký rà soát Phase2-1.1 (15/11/2026)

1. Đã xác minh sáu output: khóa, tổng count, sum theo movie, avg_rating, căn cửa sổ và null TMDB. Kết quả file pass, chưa suy ra hệ thống ES/SQL đã chạy.
2. Bổ sung period_type=all và distinct_movie_count cho D1; yêu cầu analytics_top_movies lưu đủ hạng trước khi Tableau chỉ hiển thị Top 10.
3. Bảo toàn chín truy vấn tối thiểu của contract nền, thêm test 7d/30d/WoW và Batch vs RT với cutoff fixture; chỉ năm output analytics phải materialize cho dashboard.
4. Làm rõ giới hạn state trong RAM của Spark hiện tại; G1 snapshot và G1 Kafka live là hai mức nghiệm thu khác nhau.
5. Chốt kiểu, khóa, quy tắc rank, publication và provenance cho analytics để P1-P5 không tự diễn giải.
