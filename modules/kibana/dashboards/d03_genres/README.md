# Dashboard D3 — Thể loại nào đang được quan tâm? (`d03_genres`)

- **Owner:** P3 — Giang
- **Contract:** `DATA_CONTRACT_Phase2.md` (Phase2-3.0, mục 6.1 & 8)
- **Data View:** `dv_d3_genre_trends` -> alias `analytics_genre_trends` (`timeFieldName = period_start`)
- **Kibana version:** 8.15.3 (bố cục 48 cột khớp `MovieTrend_Kibana_8.15.3_Live_Layout_Preview.html`)

## Cấu trúc Panels (10 panels)

| Panel ID | Loại | Kích thước (`x,y,w,h`) | Nội dung |
|---|---|---|---|
| `mt-p2-8153-d3-nav` | Markdown | `0, 0, 48, 5` | Điều hướng D1–D5 |
| `mt-p2-8153-d3-header` | Markdown | `0, 5, 48, 10` | Tiêu đề D3 (`BATCH · UTC · Kibana 8.15.3`) |
| `mt-p2-8153-d3-total` | Lens Metric | `0, 15, 12, 12` | `sum(rating_count)` — Lượt gắn genre ở grain lọc |
| `mt-p2-8153-d3-genres` | Lens Metric | `12, 15, 12, 12` | `unique_count(genre)` — Số genre trong phạm vi lọc |
| `mt-p2-8153-d3-peak` | Lens Metric | `24, 15, 12, 12` | `max(rating_count)` — Rating_count kỳ lớn nhất |
| `mt-p2-8153-d3-avg` | Lens Metric | `36, 15, 12, 12` | `max(avg_rating)` — Điểm TB grain tối đa |
| `mt-p2-8153-d3-line` | Lens XY Line | `0, 27, 32, 28` | Xu hướng thể loại theo thời gian (`period_start` × `genre`) |
| `mt-p2-8153-d3-top` | Lens XY Horizontal Bar | `32, 27, 16, 28` | Top thể loại (`sum(rating_count)`) |
| `mt-p2-8153-d3-heat` | Lens Heatmap | `0, 55, 48, 30` | Bản đồ nhiệt thể loại × thời gian (`period_start` × `genre`) |
| `mt-p2-8153-d3-note` | Markdown | `0, 85, 48, 8` | Chú thích **GENRE MULTI-LABEL** (`274.480` lượt gắn, không cộng trộn `period_type`) |

## Controls bắt buộc

1. **Độ chi tiết (`period_type`):** `singleSelect: true`, mặc định `month` (chỉ chọn **một** `period_type` tại mỗi thời điểm).
2. **Thể loại (`genre`):** Lọc include/exclude theo thể loại (giữ `UNKNOWN`).

## Lệnh sinh & kiểm tra

```bash
.venv/bin/python modules/kibana/dashboards/d03_genres/build_d03_saved_objects.py
.venv/bin/python modules/kibana/dashboards/d03_genres/build_d03_saved_objects.py --check
.venv/bin/python -m pytest modules/kibana/tests/test_d03_genres_saved_objects.py -q
```
