# KICKOFF PHASE 2 - Elasticsearch, Spark SQL và Tableau

**Dự án:** Movie Trend - phân tích xu hướng tương tác với phim  
**Ngày chốt phân công:** 15/11/2026  
**Trạng thái:** Phân công theo luồng dữ liệu đã chốt cho nhóm 5 người  
**Hợp đồng kỹ thuật đi kèm:** [DATA_CONTRACT_Phase2.md](DATA_CONTRACT_Phase2.md), bản Phase2-2.2  
**Website tham khảo:** [What's Seattle Reading?](https://www.whats-seattle-reading.com/)  

## 1. Mục tiêu, phạm vi và thực trạng đầu vào

Phase 2 hoàn thiện ba module còn lại: **Module 06 Elasticsearch**, **Module 07 Spark SQL**, **Module 08 Tableau**. Luồng đích là Batch View/Realtime View → Elasticsearch → Spark SQL tạo analytics_* → Tableau chỉ đọc. Cả 5 dashboard thuộc **một workbook Tableau**. Không thay MapReduce, Spark Streaming hay dữ liệu nguồn bằng tính toán riêng trong Tableau.

Năm index analytics ở Phase 2 phục vụ năm dashboard. Ngoài chúng, Module 07 vẫn phải triển khai và kiểm thử **chín truy vấn nghiệp vụ tối thiểu** của contract nền: Top phim, rating TB, genre theo thời gian, xu hướng 7/30 ngày, WoW, top phim tăng, đối chiếu Batch–Realtime theo cutoff và tín hiệu TMDB. Các truy vấn không có trang Tableau riêng được bàn giao dưới dạng Spark SQL job/hàm cùng test, theo mục 8.1 của DATA_CONTRACT_Phase2.md.

| Nhánh | Output có sẵn | Ý nghĩa và phạm vi |
|---|---|---|
| Batch | movie_stats.tsv (9.724 dòng), movie_daily_stats.tsv (99.538), genre_daily_stats.tsv (32.894), manifest.json | Lịch sử MovieLens trước T = 2018-10-01T00:00:00Z; run_id = 20261024T104145Z; 100.836 rating duy nhất. |
| MovieLens Streaming | movie_realtime_stats.jsonl (201.627), genre_realtime.jsonl (127.770) | Hai kích thước cửa sổ 10m và 1h theo event_time. Trong bản đính kèm, tất cả cửa sổ đều nằm trước T: đây là **replay lịch sử**, không phải mức quan tâm hôm nay. |
| TMDB Streaming | tmdb_realtime.jsonl (20) | Một snapshot tại 2026-11-12T06:02:43Z. Chưa có title trong output; prev_popularity và popularity_delta đều null; 20 ID không khớp movie_reference đính kèm. |

Khi trình bày, gọi một rating MovieLens là **một lượt tương tác/rating**, không phải một lượt xem phim. Không cộng hai kích thước cửa sổ streaming, không cộng tổng Batch và Realtime đang trùng timeline, không cộng popularity TMDB với rating MovieLens.

Website mẫu có các mục Overview, Checkout Trends, Most Borrowed, Genres và Backlist Boomerangs. Nhóm chuyển các ý tưởng tìm kiếm, so sánh, xếp hạng và theo dõi thể loại sang dữ liệu phim. Bản Phase 2 không sao chép nội dung sách/tác giả hoặc phân loại sách in/ebook/audiobook vì dataset không có trường tương ứng. “Phim tăng trở lại” được thể hiện bằng biến động cửa sổ ngắn; không gọi đó là phim cũ nổi lại sau nhiều năm khi dữ liệu chưa chứng minh điều này.

## 2. Năm dashboard đã chốt

| ID, owner | Mục đích / câu hỏi nghiệp vụ | Bố cục và tương tác bắt buộc | Nguồn Tableau và quy tắc kiểm tra | File output gốc |
|---|---|---|---|---|
| **D1 - Tổng quan lịch sử**; người 1 | Có bao nhiêu rating và phim được rating? Lượng tương tác theo tháng thay đổi ra sao? Top phim trong kỳ được chọn? Tương ứng Overview và phần Most Borrowed của website mẫu. | KPI tổng lượt, số phim, điểm trung bình; đường lượt rating theo thời gian; Top 10 phim; chọn kỳ all/year/month, nhấn kỳ để cập nhật xếp hạng; hiển thị batch run và T. | analytics_timeseries + analytics_top_movies; kiểm tra tổng toàn kỳ 100.836; tổng điểm/tổng rating; không cộng movie_stats với movie_daily_stats. | movie_stats.tsv; movie_daily_stats.tsv; manifest.json |
| **D2 - Xu hướng từng phim**; người 2 | Phim được chọn thay đổi lượng rating thế nào theo tháng/năm? So sánh tối đa ba phim như Checkout Trends. | Tìm phim theo title; chọn 1-3 phim; biểu đồ đường và tooltip lượt rating/điểm trung bình; đổi độ chi tiết day/month/year; điền 0 cho kỳ không có rating khi vẽ chuỗi. **Không thêm Top hoặc phim trở lại vào trang này.** | analytics_timeseries; join title theo movieId ở Spark SQL; kiểm tra một phim mẫu qua nhiều kỳ, điểm TB theo kỳ = tổng điểm / tổng rating. | movie_daily_stats.tsv; movie_stats.tsv |
| **D3 - Thể loại qua thời gian**; người 3 | Thể loại nào phổ biến theo ngày/tháng/năm? Thứ hạng và điểm trung bình thể loại thay đổi ra sao? Tương ứng Genres. | Đường nhiều thể loại, Top thể loại, heatmap thời gian × thể loại; bộ lọc thể loại/kỳ; nhấp vào thể loại để làm nổi bật. | analytics_genre_trends; note phim đa thể loại được tính ở từng nhóm; tổng các thể loại không phải tổng lượt hệ thống. | genre_daily_stats.tsv |
| **D4 - Biến động cửa sổ ngắn**; người 4 | Cửa sổ 10m/1h có bao nhiêu rating theo phim/thể loại? Mục nào tăng so với cửa sổ liền trước? Phỏng theo ý tưởng phát hiện sự quan tâm tăng trở lại. | Hai vùng phim và thể loại, xếp hạng số lượt, biến động so với kỳ trước; chỉ chọn **một** window_size; bộ lọc event_time; nhãn cố định “MovieLens replay” với dữ liệu hiện có. Khi phim quá thưa, ưu tiên giải thích phân bố và thể loại. | analytics_growth; chỉ so sánh cùng window_size và cửa sổ liền kề; hiển thị current_count, previous_count và trending_score, đánh dấu thiếu kỳ trước. | movie_realtime_stats.jsonl; genre_realtime.jsonl |
| **D5 - Tín hiệu TMDB live**; người 5 | Các phim TMDB hiện được quan tâm theo popularity, vote_count và vote_average ra sao? Popularity thay đổi thế nào giữa các lần crawl định kỳ? | KPI số phim trong snapshot mới nhất, Top popularity, scatter popularity × vote_average (kích thước vote_count), bảng tmdbId, event_time và lần cập nhật; delta chỉ hiện khi có ít nhất hai snapshot thật cùng tmdbId, nếu không ghi “chưa đủ snapshot”. | analytics_tmdb_signal; thang vote_average /10, kiểm tra latest/previous theo event_time; không gán movieId hoặc title khi không có ánh xạ xác thực. Bản 20 dòng chỉ là baseline. | tmdb_realtime.jsonl và các snapshot TMDB crawl định kỳ |

**Giới hạn tương tác:** Tableau được lọc, sắp xếp, hiển thị KPI và dùng thao tác chọn điểm trên các bảng analytics. Công thức nghiệp vụ, chuẩn hóa thời gian và phép so sánh cửa sổ do Spark SQL sở hữu. Các kỳ Top 10 trong D1 là kỳ all/year/month đã được Query Layer công bố; yêu cầu khoảng ngày tùy ý là mở rộng riêng nếu chưa có truy vấn hỗ trợ.

## 3. Nguyên tắc chia theo luồng và bảng phân công chung

Một người sở hữu đường đi của dữ liệu từ output upstream qua index nguồn, job Spark SQL, index analytics đến dashboard của mình. **D1 dùng chung dữ liệu với D2**: P2 là writer duy nhất của hai index Batch phim và hai analytics phim; P1 chỉ đọc để dựng D1. P5 dựng nền tảng Elasticsearch và bộ chạy Spark SQL dùng chung, nhưng không viết thay metric hay loader thuộc luồng P2-P4. P1 giữ chuẩn Tableau và là người tích hợp workbook.

| Người | Luồng và trách nhiệm chính | Index nguồn người đó triển khai/kiểm tra | Query output sở hữu | Dashboard và trách nhiệm Tableau | Phần dùng chung |
|---|---|---|---|---|---|
| **P1 - D1** | Đọc dữ liệu phân tích phim lịch sử P2 công bố; chịu trách nhiệm trải nghiệm và bản cuối. | Không ghi index nguồn. | Không ghi analytics. | D1 Tổng quan lịch sử; mẫu 5 trang, data source read-only, điều hướng, ghép, QA và bản demo. | Chuẩn Tableau, duyệt biểu đồ cuối của P2-P5. |
| **P2 - D2** | movie_stats.tsv + movie_daily_stats.tsv → loader Batch phim → Spark SQL phim → D2; cấp dữ liệu D1. | movie_batch_stats, movie_daily_stats; physical index theo run, giao P5 công bố alias. | analytics_timeseries, analytics_top_movies; query 7d/30d/WoW. | D2 Xu hướng từng phim; giải thích số liệu D1-D2. | Phối hợp P4 làm query Batch vs RT. |
| **P3 - D3** | genre_daily_stats.tsv → loader Batch genre → Spark SQL genre → D3. | genre_trends; physical index theo cùng run Batch của P2, giao P5 công bố alias. | analytics_genre_trends; query genre theo tuần ISO. | D3 Thể loại qua thời gian. | Đối soát chung run/cutoff với P2. |
| **P4 - D4** | movie_realtime_stats.jsonl + genre_realtime.jsonl → xác minh sink/upsert → Spark SQL cửa sổ → D4. | movie_realtime_stats, genre_realtime; writer canonical vẫn là Spark Streaming. | analytics_growth; query top đang tăng và Batch vs RT với P2. | D4 Biến động cửa sổ ngắn, nhãn MovieLens replay. | Phối hợp owner Module 05 khắc phục state sau restart. |
| **P5 - D5** | tmdb_realtime.jsonl và các lần crawl định kỳ tiếp theo → xác minh Kafka/Spark/ES → Spark SQL TMDB → D5. | tmdb_realtime; writer canonical là nhánh TMDB Spark Streaming. | analytics_tmdb_signal; query current TMDB. | D5 Tín hiệu TMDB, snapshot và delta thực khi đủ kỳ. | Dựng ES cluster, index template/quyền, Spark SQL runner/connector, quản lý cấu hình và công bố đồng thời ba source + ba analytics alias Batch cùng run. |

Mỗi output có **một writer logic**. Người khác được phép review, kiểm thử và dùng fixture; không ghi đè index canonical. P5 quản lý hạ tầng và lệnh publish, không đồng nghĩa với quyền tự đổi schema hoặc metric của P2-P4. Mỗi người dựng dashboard mình theo template của P1; P1 là người duy nhất ghép workbook cuối.

### Giao diện dùng chung do P5 bàn giao sớm

| Hạ tầng | P5 phải cung cấp | Điều P2-P4 tự làm trên nền đó |
|---|---|---|
| Elasticsearch | Endpoint dev/test; version, cấu hình tài nguyên, index templates, quyền đọc/ghi theo owner, script tạo index, quy ước physical index/alias, lệnh kiểm tra sức khỏe, Bulk errors và log. | Mapping cụ thể, loader hoặc xác minh sink, _id, đối soát và truy vấn smoke của index mình; không tự dựng cluster khác để tích hợp. |
| Spark SQL | Version/runtime, dependency kết nối ES, SparkSession cấu hình UTC, module khởi chạy, API đọc sáu temp view, hàm xuất analytics, cấu hình retry và job logs. | SQL/DataFrame và tests theo metric của luồng, fixture, analytics output và runbook của mình; không tự đổi thư viện/phiên bản chung. |
| Publish | P5 cung cấp lệnh công bố alias, khóa phiên bản và manifest trạng thái chung. | P2/P3 giao ba physical nguồn và ba physical analytics Batch cùng run đã kiểm tra; P4/P5 chứng minh RT upsert/cập nhật; mỗi query owner giao index analytics hoàn chỉnh. |

### 3.1. Cây thư mục Phase 2 đã chốt

Cấu trúc giữ ba module 06/07/08 ở cấp trên, sau đó tách mã theo lane. Ký hiệu `[P1]` đến `[P5]` là owner chịu trách nhiệm viết và nghiệm thu. Các thư mục chưa có sẽ được tạo khi bắt đầu triển khai; không chuyển mã Module 01-05 sang nơi mới chỉ để khớp cây.

~~~text
movie-trend/
├── Kickoff_Phase2.md                         [P1 quản lý; cả nhóm duyệt]
├── DATA_CONTRACT_Phase2.md                   [P1 quản lý; P2-P5 ký phần mình]
├── contracts/
│   └── phase2/
│       ├── common/                           [P5: chuẩn index, khóa, publication]
│       ├── movie_batch/                      [P2: schema, fixtures D1/D2]
│       ├── genre_batch/                      [P3: schema, fixtures D3]
│       ├── movielens_realtime/               [P4: schema, fixtures D4]
│       └── tmdb_realtime/                    [P5: schema, fixtures D5]
├── modules/
│   ├── elasticsearch/                        [Module 06]
│   │   ├── common/                           [P5]
│   │   │   ├── config/                       Cluster, quyền, .env.example
│   │   │   ├── templates/                    Template và mapping chung
│   │   │   └── publication/                  READY, sáu alias Batch, rollback
│   │   ├── lanes/
│   │   │   ├── movie_batch/                  [P2: mapping, hai Batch loaders]
│   │   │   ├── genre_batch/                  [P3: mapping, genre loader]
│   │   │   ├── movielens_realtime/           [P4: kiểm sink/upsert; không tạo writer mới]
│   │   │   └── tmdb_realtime/                [P5: kiểm sink/latest; không tạo writer mới]
│   │   └── tests/
│   │       ├── common/                       [P5]
│   │       └── lanes/                        [P2-P5 theo lane]
│   ├── spark_sql/                            [Module 07]
│   │   ├── common/                           [P5]
│   │   │   ├── runtime/                      SparkSession, UTC, ES connector
│   │   │   ├── readers/                      Sáu temp views, physical/alias
│   │   │   ├── writers/                      Analytics writer và ID helper
│   │   │   └── tableau_export/               CSV/manifest từ analytics ES
│   │   ├── lanes/
│   │   │   ├── movie_batch/                  [P2: timeseries, top, 7d/30d/WoW]
│   │   │   ├── genre_batch/                  [P3: genre trends, tuần ISO]
│   │   │   ├── movielens_realtime/           [P4: growth, Batch vs RT]
│   │   │   └── tmdb_realtime/                [P5: latest, delta]
│   │   └── tests/
│   │       ├── common/                       [P5]
│   │       └── lanes/                        [P2-P5 theo lane]
│   └── tableau/                              [Module 08]
│       ├── template/                         [P1: giao diện và điều hướng]
│       ├── data_sources/                     [P1: năm nguồn và refresh]
│       ├── dashboards/
│       │   ├── d01_overview/                 [P1]
│       │   ├── d02_movie_trends/             [P2]
│       │   ├── d03_genres/                   [P3]
│       │   ├── d04_movielens_realtime/       [P4]
│       │   └── d05_tmdb_signal/              [P5]
│       ├── final_workbook/                   [P1: workbook năm trang]
│       └── qa/                               [P1: checklist và số đối chứng]
├── docs/
│   └── phase2/
│       ├── platform/                         [P5: ES, SQL, publish, rollback]
│       ├── lanes/
│       │   ├── movie_batch/                  [P2: runbook]
│       │   ├── genre_batch/                  [P3: runbook]
│       │   ├── movielens_realtime/           [P4: runbook]
│       │   └── tmdb_realtime/                [P5: runbook]
│       └── integration/                      [P1: nghiệm thu và demo]
└── data/
    └── local/
        └── phase2/                           [file sinh ra, không commit]
            ├── es_reports/                   [P5 tập hợp báo cáo các lane]
            ├── tableau_exports/              [P5 tạo; P1 đọc]
            │   ├── batch/<run_id>/
            │   ├── movielens_realtime/
            │   └── tmdb_realtime/
            └── checkpoints/                  [P4/P5: chỉ checkpoint local dev]
~~~

### 3.2. Quyền sửa thư mục và điểm giao nhau

| Owner | Vùng được sửa trực tiếp | Giao diện với người khác / quy tắc tránh xung đột |
|---|---|---|
| P1 | `modules/tableau/template/`, `data_sources/`, `dashboards/d01_overview/`, `final_workbook/`, `qa/`, `docs/phase2/integration/`; quản lý hai tài liệu chốt ở root. | P2-P5 giao từng dashboard; **chỉ P1** cập nhật workbook cuối. Đổi contract root phải có change request và owner liên quan ký nhận, không tự đổi schema. |
| P2 | `contracts/phase2/movie_batch/`, hai lane `movie_batch/` trong ES/SQL và tests, `dashboards/d02_movie_trends/`, runbook `docs/phase2/lanes/movie_batch/`. | Giao hai physical source + hai analytics Batch cho P5, fixture D1 cho P1, fixture Batch vs RT cho P4. Không publish alias riêng. |
| P3 | `contracts/phase2/genre_batch/`, hai lane `genre_batch/` trong ES/SQL và tests, `dashboards/d03_genres/`, runbook genre. | Giao một physical source + một analytics Batch cùng run/T P2 cho P5. Không publish alias riêng. |
| P4 | `contracts/phase2/movielens_realtime/`, hai lane RT MovieLens trong ES/SQL và tests, `dashboards/d04_movielens_realtime/`, runbook RT. | Kiểm hai canonical source do Module 05 Spark Streaming ghi; không tạo loader thứ hai. Phối hợp P2 ở query Batch vs RT. |
| P5 | `contracts/phase2/common/` và `tmdb_realtime/`, `common/` của ES/SQL, hai lane TMDB và tests, `dashboards/d05_tmdb_signal/`, `docs/phase2/platform/` và TMDB runbook. | Bàn giao hạ tầng và CSV exporter ở K0/K2; điều phối sáu alias Batch sau READY P2/P3; tự đảm nhận TMDB live. Không viết thay metric P2-P4. |

`contracts/phase2/<lane>/` chứa schema, fixtures nhỏ và README cho đúng lane; không tạo thêm một thư mục `contracts/phase2/fixtures/` chung để lưu bản sao dễ lệch. File chạy sinh ra như CSV thật, ES reports và checkpoint local dev nằm ở `data/local/phase2/`, được ignore trong Git. Checkpoint production dùng vị trí bền vững do cấu hình môi trường của P5 chỉ định, không lấy thư mục local làm bằng chứng phục hồi live.

**Ngoại lệ đối với Module 02/05 hiện hữu:** P5 chỉnh `modules/kafka_ingestion/src/tmdb_producer.py` khi cần bảo đảm crawl định kỳ. P4 sở hữu logic/test MovieLens RT và P5 sở hữu logic/test TMDB, nhưng `modules/spark_streaming/src/streaming_job.py`, `es_sink.py` và `transforms.py` hiện chứa code của cả hai nhánh. P5 giữ vai trò tích hợp file dùng chung, P4 gửi thay đổi kèm test hoặc tách helper riêng trước khi P5 ghép; không để hai PR đồng thời ghi vào cùng khối hàm. Writer canonical của ba RT index vẫn là Spark Streaming.

## 4. Nhiệm vụ chi tiết - P1: Tableau chung và D1

| Mã | Việc cần làm | Đầu vào hoặc phối hợp để làm song song | Bàn giao và tiêu chí nghiệm thu |
|---|---|---|---|
| P1.1 | Dựng workbook khung gồm D1-D5: cỡ màn, màu, font, bố cục KPI, nhãn đơn vị, tooltip, menu, filter và cách ghi trạng thái dữ liệu. | Bắt đầu bằng fixture analytics đúng schema do P2-P5 bàn giao ở K0; không chờ ES. | Template và quy định 1 trang gửi mọi owner; năm dashboard có giao diện nhất quán. |
| P1.2 | Thiết lập data source Tableau từ năm analytics aliases qua export CSV read-only có manifest phiên bản do P5 cung cấp (hoặc connector đã được kiểm thử và chốt theo change request); quy định UTC, định dạng 0/null, kỳ all/year/month, phân biệt Batch, MovieLens replay và TMDB live. | P2-P5 cung cấp CSV fixture đúng schema; P5 cung cấp exporter/manifest và cách refresh; domain owner cung cấp field contract. | Danh sách field, filter và quyền đọc; Tableau không tự tính lại trending_score hoặc weighted average. |
| P1.3 | Dựng D1 từ analytics_timeseries và analytics_top_movies của P2: KPI tổng 100.836 lượt, 9.724 phim, avg_rating, đường thời gian, Top 10 kỳ all/year/month, run_id/T. | P2 giao fixture analytics ngay K0, kết quả thật sau K2. | KPI kỳ all khớp P2; chọn một kỳ tác động Top cùng kỳ; không cộng tổng ở hai period_type. |
| P1.4 | Review bản nháp D2-D5 về nhãn, tương tác, tooltip và khả năng đọc. Gửi lỗi số liệu về đúng domain owner thay vì sửa công thức trong Tableau. | Mỗi owner giao bản nháp trước K3. | Nhật ký nhận xét theo dashboard, mỗi lỗi có người xử lý và ảnh kiểm tra sau sửa. |
| P1.5 | Ghép dashboard của P2-P5 vào một workbook, đổi fixture sang analytics thực, thử điều hướng/lọc và xuất bản nội bộ. | K2 cung cấp analytics hoàn chỉnh và provenance. | Một workbook 5 trang, nguồn không lỗi, bản PDF/ảnh trình bày, checklist QA, hướng dẫn mở. |
| P1.6 | Chủ trì truy vết ít nhất một KPI mỗi dashboard từ output → ES → SQL → Tableau; ghi giới hạn dữ liệu tại đúng trang. | Domain owner cung cấp query đối chứng. | Biên bản K4 và bản demo có nhãn replay, snapshot và lần cập nhật cuối. |

## 5. Nhiệm vụ chi tiết - P2: Batch phim, Spark SQL phim và D2

| Mã | Việc cần làm | Đầu vào hoặc phối hợp để làm song song | Bàn giao và tiêu chí nghiệm thu |
|---|---|---|---|
| P2.1 | Kiểm tra manifest, thứ tự cột, run_id/T; dựng mapping movie_stats → movie_batch_stats và movie_daily_stats → movie_daily_stats cùng _id xác định theo run và movieId/date. | Hai TSV và manifest có sẵn; dùng ES template/endpoint do P5 cung cấp. | Schema và 2-3 document mẫu gửi P5/P1 ngay K0; không tải nhầm header HDFS như record. |
| P2.2 | Viết loader hai Batch tables vào physical indices theo run, kiểm tra Bulk từng item, retry có kiểm soát, re-run idempotent. | Phát triển với TSV local, tích hợp ES khi P5 báo nền tảng sẵn sàng. | 9.724 và 99.538 bản ghi; tổng mỗi bảng 100.836; mọi phim có count và rating_sum khớp cộng phim-ngày. |
| P2.3 | Giao hai physical source ở staging, báo cáo count và T cho P5; SQL bước P2.4 đọc physical source cùng run. Chỉ dùng alias sau khi đủ ba analytics Batch và P5 công bố cả sáu. | P3 làm genre song song. | Sau P2.4, giao thêm hai physical analytics và READY report; không tự chuyển alias riêng. |
| P2.4 | Viết query analytics_timeseries (day/month/year/all, dòng tổng có distinct_movie_count) và analytics_top_movies (all/year/month, đủ phim và rank ổn định); giữ title từ movie_batch_stats. | Fixture có thể tạo ngay trên TSV; Spark SQL runner của P5 đến sau. | Hai physical analytics cùng run theo contract; D1 all = 100.836, 9.724 và điểm TB ≈3,50156; Top all 329/317/307. |
| P2.5 | Viết và test query 7 ngày, 30 ngày liên tiếp và WoW theo event_time UTC, cùng độ dài kỳ, ISO week; hợp tác P4 làm fixture hai phía T cho Batch vs RT. | Batch TSV/fixture; không chờ D4. | Mã query, tests, kỳ và giá trị đối chứng; không nhét thêm chức năng vào D2. |
| P2.6 | Dựng D2 tìm/so sánh tối đa ba phim, line day/month/year, tooltip count/avg; gửi nguồn D1 cho P1. | Template P1 và analytics của chính P2. | Không có Top hay “phim trở lại” trong D2; một phim/tháng đối chiếu đúng output. |
| P2.7 | Viết runbook: cấu hình, lệnh nạp, reset dev, đối soát, tên physical indices, rollback và nguồn/provenance; bàn giao P1/P5. | P5 quy định môi trường chung. | Người khác tái chạy được trên cùng schema mà không cần đoán cột hoặc run. |

## 6. Nhiệm vụ chi tiết - P3: Batch genre, Spark SQL genre và D3

| Mã | Việc cần làm | Đầu vào hoặc phối hợp để làm song song | Bàn giao và tiêu chí nghiệm thu |
|---|---|---|---|
| P3.1 | Kiểm tra manifest và schema genre_daily_stats.tsv; dùng đúng run/T/ref_version với P2; mapping index genre_trends, khóa (run,date,genre). | TSV có sẵn; P5 cung cấp ES template. | Schema và fixture index giao P5/P1 tại K0; UNKNOWN giữ nguyên. |
| P3.2 | Viết loader genre vào physical index theo run; kiểm tra header, kiểu, lỗi Bulk, _id ổn định, re-run idempotent. | Nạp song song với hai bảng phim của P2. | 32.894 record; tổng 274.480 lượt gắn thể loại; không gọi đây là tổng rating hệ thống. |
| P3.3 | Giao physical source genre ở staging và báo cáo đối soát cho P5; SQL bước P3.4 đọc đúng physical index cùng run/T. | P2 giao hai source phim cùng run. | Sau P3.4, giao thêm physical analytics genre và READY report; P5 chuyển sáu alias trong một thao tác; nếu một bên lỗi giữ run cũ. |
| P3.4 | Tạo analytics_genre_trends day/month/year; điểm TB theo sum/count, rank theo count và tie-break; query genre theo tuần ISO cho contract nền. | Viết fixture từ TSV ngay; P5 Spark SQL runner dùng chung. | Physical analytics genre đúng top, line và heatmap; query tuần có test, không lẫn week với month. |
| P3.5 | Dựng D3 theo template P1: chọn genre, timeline, heatmap, Top, tooltip đa nhãn và UNKNOWN. | Analytics của chính P3. | Tổng các genre không bị trình bày như tổng rating hệ thống; một genre/một kỳ có query đối chứng. |
| P3.6 | Runbook loader/SQL và dữ liệu smoke, kiểm tra sau publish alias và analytics, gửi P1/P5. | P5 giữ cấu hình chung. | Có count, lỗi, lệnh chạy lại và owner nhận rõ. |

## 7. Nhiệm vụ chi tiết - P4: MovieLens Streaming replay, Spark SQL và D4

| Mã | Việc cần làm | Đầu vào hoặc phối hợp để làm song song | Bàn giao và tiêu chí nghiệm thu |
|---|---|---|---|
| P4.1 | Mapping movie_realtime_stats và genre_realtime: window_size 10m/1h, UTC, định danh theo movieId/genre + window_start + size; kiểm tra schema JSONL. | Hai file output có sẵn; P5 cung cấp ES template. | Fixture hai index, truy vấn mẫu, số record 201.627/127.770 trong snapshot cô lập. |
| P4.2 | Kiểm tra writer Spark Streaming duy nhất vào hai canonical indices; đọc response Bulk từng item, upsert lặp, lỗi network; demo tĩnh chỉ vào dev indices cách ly. | Phối hợp owner Module 05 cho mọi sửa upstream. | Nghiệm thu tách snapshot tĩnh và Kafka/restart; không ghi đè canonical bằng script tĩnh. |
| P4.3 | Kiểm tra dedup và state cửa sổ khi restart giữa micro-batch: không mất count, không nhân đôi, không ghi đè ES bằng tổng phần sau restart; nếu chưa có fix thì chỉ tuyên bố snapshot pass. | Mã Module 05 hiện giữ state trong RAM; phối hợp P5 về nền ES/Kafka. | Bài test hai micro-batch trước/sau restart, count cuối cùng khớp raw; log lỗi nếu chưa đạt. |
| P4.4 | Tạo analytics_growth: current/previous ở hai cửa sổ liền kề cùng size; thiếu previous → null; title chỉ là nhãn từ reference có version hoặc Batch title, không lấy metric Batch. | SQL chạy trên JSONL fixture trước; runner P5 dùng chung. | count, sum và trending_score đúng; các cửa sổ 10m/1h không cộng; title_source truy vết được. |
| P4.5 | Viết query top phim tăng và Batch vs RT theo cutoff trên fixture có dữ liệu sau T; với output hiện tại nói rõ RT sau T rỗng. | P2 giao batch test fixture; không đợi index thật. | Query 7 và 8 của contract nền có test; không tạo tổng Batch+replay giả. |
| P4.6 | Dựng D4: top phim/thể loại, biểu đồ cửa sổ và chênh lệch, filter size/time, số đếm gốc, nhãn “MovieLens replay”. | Template P1 và analytics của P4. | Không gọi 0→1 là bùng nổ đáng tin khi count nhỏ; thiếu previous không hiện tăng trưởng. |
| P4.7 | Runbook mode snapshot vs Kafka, source offsets, checkpoint, trạng thái, lần cập nhật cuối và giới hạn demo; giao P1/P5. | P5 công bố runtime/ES. | Người xem biết đang xem lịch sử replay hay stream đã nghiệm thu. |

## 8. Nhiệm vụ chi tiết - P5: hạ tầng ES/Spark SQL, TMDB realtime thật và D5

| Mã | Việc cần làm | Đầu vào hoặc phối hợp để làm song song | Bàn giao và tiêu chí nghiệm thu |
|---|---|---|---|
| P5.1 | Dựng Elasticsearch chung: phiên bản, cấu hình tài nguyên, endpoint, health check, templates, quyền cho Batch Loader/Spark Streaming/Spark SQL, log và script tạo môi trường. | Công bố interface ngay K0; P2-P4 phát triển bằng fixture khi cluster chưa chạy. | Mỗi owner được endpoint/credentials phù hợp và quy trình nạp dev; không phải tự dựng cluster riêng. |
| P5.2 | Dựng Spark SQL runner chung: phiên bản, dependency/connector ES, UTC, temp view loader, common _id serializer, output publisher, exporter read-only analytics → CSV/manifest cho Tableau, kiểm tra lỗi/cấu hình và lệnh tái chạy. | P2-P4 viết logic trên fixture; P5 đưa bộ chạy sớm. | Một job mẫu đọc một physical index và ghi một analytics dev index; export đủ dòng thành CSV/manifest đúng schema; log count/retry; mọi lane dùng cùng runtime. |
| P5.3 | Thiết lập quy trình nạp Batch theo version: nhận 2 source + 2 analytics phim P2 và 1 source + 1 analytics genre P3 cùng run/T, kiểm tra count và manifest, chuyển sáu alias trong cùng một thao tác; rollback khi thiếu một index. | P2/P3 làm loader song song. | Sáu alias cùng run/T; P2/P3 nhận biên bản publish; không có run nửa cũ nửa mới. |
| P5.4 | Sở hữu nhánh TMDB định kỳ end-to-end trong Phase 2: scheduler/producer theo cấu hình, Kafka tmdb-snapshots, Spark xử lý latest theo event_time, ES tmdb_realtime, kiểm tra retries/đến muộn/dedup/restart. | Tôn trọng writer canonical hiện có của Module 05; phối hợp owner upstream nếu phải sửa. | Có ít nhất 2 snapshot thật cùng tmdbId để kiểm chứng prev_popularity/delta; snapshot chỉ 20 dòng hiện tại chỉ là baseline. |
| P5.5 | Thiết kế analytics_tmdb_signal mới nhất từng tmdbId; không ghép tmdbId với movieId, popularity khác thang MovieLens; title null hiển thị ID. | Fixture từ JSONL có sẵn, chuyển sang index live khi nghiệm thu. | SQL latest/delta nullable đúng; snapshot mới hơn thay thế cũ, bản đến muộn không ghi đè. |
| P5.6 | Dựng D5: Top popularity, scatter popularity × vote_average với vote_count, thời điểm snapshot/lần cập nhật, delta khi đủ dữ liệu; phân biệt snapshot baseline và live. | Template P1, analytics của P5. | Với một snapshot không hiện growth giả; với hai snapshot hiển thị delta tính đúng và source timestamp. |
| P5.7 | Kiểm tra ES/Spark SQL tổng thể: sáu source indices, năm analytics indices, schema, publication, health, quyền, log, restart và runbook tích hợp; hỗ trợ các lane không sửa logic thay họ. | P2-P4 giao báo cáo riêng; P1 kiểm tra Tableau. | Môi trường tái chạy được; có bảng trạng thái G1/G2 và người phụ trách từng lỗi. |

**Cân bằng công việc:** P5 chịu nền tảng và TMDB live liên tục, vì vậy P2-P4 tự viết và test loader/query/dashboard của mình. P5 chỉ sở hữu các adapter/chức năng chung và nhánh TMDB; không trở thành người code thay ba lane khác.

## 9. Mốc bàn giao - phát triển đồng thời

Mốc là điều kiện nghiệm thu, không phải lịch chờ tuần tự. Từ K0, P1 dựng template; P2 làm phim Batch; P3 làm genre Batch; P4 làm MovieLens replay; P5 dựng ES/Spark SQL skeleton và TMDB fixture. Mọi người dùng cùng schema trong DATA_CONTRACT_Phase2.md.

| Mốc | Công việc đang chạy song song | Gói bàn giao và người nhận | Điều kiện qua mốc |
|---|---|---|---|
| **K0 - Contract và fixture**; P1 điều phối | P1 template D1; P2 hai Batch movie loaders/SQL fixture; P3 genre loader/SQL fixture; P4 RT movie/genre mapping/SQL fixture; P5 ES/Spark SQL skeleton + TMDB fixture. | P5 → mọi người: endpoint, runtime, lệnh smoke; P2 → P1: fixture D1/D2; P3/P4/P5 → P1: fixture D3/D4/D5. | Tên index, grain, keys, quyền writer, field/null, T và metric không bị hiểu khác; fixture đúng schema. |
| **K1 - Sáu index nguồn**; từng lane giao, P5 kiểm tra chung | P2/P3 nạp physical Batch độc lập; P4 kiểm tra RT MovieLens; P5 kiểm tra TMDB live hoặc snapshot cô lập; P1 tiếp tục workbook trên fixture. | P2 + P3 → P5: ba physical source Batch cùng run/T; P4 → P5: hai RT MovieLens; P5 → P1/P4: runtime và trạng thái TMDB. | P5 giữ nguồn Batch ở staging K1, công bố sáu aliases sau khi ba analytics Batch cũng sẵn sàng tại K2; RT tách static snapshot pass và Kafka/restart pass. |
| **K2 - Năm analytics outputs + query nền**; P2-P5 | Bốn lane xuất analytics trên Spark SQL runner P5; P1 dựng D1 và template từ fixture. | P2 → P1: analytics_timeseries/top_movies; P3 → P1: analytics_genre_trends; P4 → P1: analytics_growth; P5 → P1: analytics_tmdb_signal. P2/P3/P4/P5 giao 9 query tests. | Năm output thật đúng schema/khóa/metadata; P5 công bố cùng lúc sáu Batch aliases sau READY, P1 pin run trước khi refresh; 9 truy vấn tối thiểu qua test. |
| **K3 - Năm dashboard đơn lẻ**; P1-P5 | Mỗi owner dựng một trang, P1 review và sửa chuẩn giao diện; backend tiếp tục harden/restart test. | P2-P5 → P1: D2-D5, ảnh, query kiểm chứng, hạn chế và runbook. | Filter, tooltip, nhãn nguồn, giá trị mẫu khớp analytics; không dùng fixture giả trong demo thật. |
| **K4 - Một workbook và demo**; P1 | P5 kiểm tra platform, P2-P4 kiểm tra lane, P1 tích hợp và xuất bản. | P1 → cả nhóm: workbook 5 trang, bản export, trace KPI, checklist và hướng dẫn mở. | Cả 5 người tái trình bày được luồng của mình và biết phần nào static/replay/live. |

Nếu hạ tầng P5 chậm, domain owner tiếp tục code/kiểm thử trên fixture; nếu một lane trễ, P1 không tự bịa số liệu hoặc đổi metric. Trước khi công bố thật, mọi lane phải thay fixture bằng dữ liệu index đã qua K1/K2 và được người nhận ký nhận.

## 10. Điều kiện nghiệm thu toàn Phase 2

| Nhóm kiểm tra | Điều kiện pass |
|---|---|
| Truy xuất nguồn | Mỗi KPI xác định được file → index gốc → analytics output → worksheet; biết run_id hoặc window/snapshot. |
| Số liệu Batch | Ba index có 9.724/99.538/32.894 bản ghi; tổng theo phim và phim-ngày đều 100.836; avg_rating = tổng điểm / tổng count. |
| Số liệu Streaming | Có cả 10m và 1h nhưng không cộng hai view; với dataset hiện tại D4 ghi “replay”; số phim cửa sổ thưa được hiển thị trung thực. Phân biệt static snapshot pass với Kafka live/restart pass. |
| TMDB | 20 bản ghi một snapshot là baseline; P5 chứng minh ít nhất hai crawl thật có chung tmdbId, dedup/latest/previous/delta và restart trước khi nhận live_pass. Title/delta thiếu được hiển thị trung thực; TMDB không ghép tùy tiện với MovieLens. |
| Tableau | Một workbook 5 dashboard, filter/tooltip/điều hướng hoạt động, không phát minh metric riêng; bản xuất để mentor review. |
| Tái chạy | Có cấu hình ES/Spark SQL chung của P5, script/query/runbook từng lane, log đối soát, chín query tests, người nhận và biên bản K1-K4. |

## 11. Vấn đề cần báo cáo đúng, không che giấu

1. Dữ liệu MovieLens Streaming hiện trùng timeline Batch. Phase 2 trình bày D4 như một nhánh replay riêng, chưa ghép Batch + Realtime để công bố “tổng mới nhất”.
2. File TMDB hiện có không có title và chỉ có một snapshot; P5 phải kiểm chứng nhiều lần crawl thật trước khi hiện popularity_delta. Muốn tên phim phải bổ sung title vào output upstream theo quy trình đổi contract; không tự suy ra từ movie_reference vì 0/20 ID khớp.
3. Với 100.836 rating rải trên 9.724 phim, từng phim trong nhiều cửa sổ 10m/1h có rất ít rating. D4 nên hiển thị số đếm gốc, quy mô mẫu và thể loại để tránh diễn giải tăng trưởng trên 0→1 là hiện tượng mạnh.
4. Các index analytics ở Phase 2 được định nghĩa trong [hợp đồng kỹ thuật](DATA_CONTRACT_Phase2.md); Tableau chỉ đọc. Nếu thay tên, grain hoặc công thức, ghi change request và thông báo tất cả owner trước khi merge.
