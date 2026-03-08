# Unsplash Lite 数据集使用教程（中文）

> 适用目录：`unsplash-research-dataset-lite-latest/`

## 0. 先回答你最关心的两个问题

### Q1：当前是不是只有图片元数据和链接？还需要原始图像文件吗？

是的，Lite 数据集本体主要是结构化表数据（如 `photos/keywords/...`）和图片 URL，不会把全部原始图片二进制直接打包进来。

- 如果你做的是**统计分析、检索、标签处理、关键词实验**，只用表数据通常就够了。
- 如果你做的是**CV 训练（分类/检测/多模态）**，就需要把你筛选出的图片 URL 再下载到本地。

本仓库新增了 `download-from-csv` 命令：

```bash
python scripts/unsplash_lite_tool.py download-from-csv \
  --input-csv outputs/sampled_photos.csv \
  --output-dir outputs/images \
  --delay 0.2
```

> 建议不要一次性全量下载，先筛选 + 采样，再下载，能明显节省存储和时间。

### Q2：如何做关键词筛选 + 随机采样，得到一定数量结果图片？

本仓库新增了 `filter-sample` 命令，可直接从 `keywords` + `photos` 联合生成结果 CSV：

```bash
python scripts/unsplash_lite_tool.py \
  --dataset-dir unsplash-research-dataset-lite-latest \
  filter-sample \
  --keywords forest,mountain,snow \
  --sample-size 200 \
  --seed 42 \
  --output-csv outputs/sampled_photos.csv
```

然后再下载：

```bash
python scripts/unsplash_lite_tool.py download-from-csv \
  --input-csv outputs/sampled_photos.csv \
  --output-dir outputs/images \
  --limit 200
```

---

## 1. 数据集内容概览

根据官方文档，Lite 数据集通常包含 5 张主表（可能是 `.tsv` 或 `.csv`，也可能按分片方式存储）：

1. `photos`：图片主信息（作者、尺寸、EXIF、下载统计等）
2. `keywords`：图片与关键词的映射
3. `collections`：图片与合集的映射
4. `conversions`：图片下载转化相关统计
5. `colors`：图片颜色分布信息

你的目录截图显示为 `*.csv000`，这属于分片文件命名方式（例如 `photos.csv000`、`photos.csv001` ...），脚本会自动拼接读取。

---

## 2. 使用边界（务必先读）

请先阅读并遵守：

- `unsplash-research-dataset-lite-latest/README.md`
- `unsplash-research-dataset-lite-latest/TERMS.md`

特别注意（简化版）：

- Lite 数据集可用于商业和非商业场景下的内部模型训练用途。
- 不可对 Licensed Data 进行转售、再授权、再分发。
- 若收到争议内容通知，需要按条款删除相应数据。

> 以上为文档理解，不构成法律建议；上线前请让法务或负责人复核条款。

---

## 3. 工具脚本说明

文件：`scripts/unsplash_lite_tool.py`

功能：

- 自动识别普通文件和分片文件（如 `photos.csv000`）
- 统计各表行数、字段列表、前几行样例
- 输出 “photo_id + keyword” 样例
- 按关键词筛选 + 随机采样，导出带 URL 的 CSV
- 根据 CSV 批量下载图片

### 3.1 运行环境

- Python 3.9+（仅标准库，无第三方依赖）

### 3.2 查看帮助

```bash
python scripts/unsplash_lite_tool.py --help
```

### 3.3 输出全表摘要

```bash
python scripts/unsplash_lite_tool.py --dataset-dir unsplash-research-dataset-lite-latest summary --pretty
```

### 3.4 抽样查看关键词映射

```bash
python scripts/unsplash_lite_tool.py --dataset-dir unsplash-research-dataset-lite-latest keyword-samples --limit 30
```

### 3.5 关键词筛选 + 随机采样

默认是“命中任一关键词即可”：

```bash
python scripts/unsplash_lite_tool.py \
  --dataset-dir unsplash-research-dataset-lite-latest \
  filter-sample \
  --keywords beach,sunset \
  --sample-size 100 \
  --seed 7 \
  --output-csv outputs/beach_sunset_sample.csv
```

如果你希望“必须同时包含全部关键词”，加 `--require-all`：

```bash
python scripts/unsplash_lite_tool.py \
  --dataset-dir unsplash-research-dataset-lite-latest \
  filter-sample \
  --keywords forest,mountain \
  --require-all \
  --sample-size 80 \
  --seed 123 \
  --output-csv outputs/forest_mountain_all.csv
```

### 3.6 批量下载采样结果中的图片

```bash
python scripts/unsplash_lite_tool.py download-from-csv \
  --input-csv outputs/forest_mountain_all.csv \
  --output-dir outputs/images_forest_mountain \
  --delay 0.3
```
如需记录下载过程中的元数据（支持中断恢复后保留已完成记录），可追加两个参数：

```bash
python scripts/unsplash_lite_tool.py download-from-csv \
  --input-csv outputs/forest_mountain_all.csv \
  --output-dir outputs/images_forest_mountain \
  --metadata-jsonl outputs/images_forest_mountain/metadata.jsonl \
  --manifest-json outputs/images_forest_mountain/manifest.json
```

- `--metadata-jsonl`：每张图下载完成后立刻追加 1 行 JSON（append + flush）。
- `--manifest-json`：任务结束后输出最终汇总（含 `summary` 与 `records`）。

#### CSV 列名约定（对应元数据字段）

脚本会优先从输入 CSV 读取可用字段，推荐列名如下（有别名也可自动识别）：

- 基础字段：`photo_id`, `photo_image_url`, `search_keyword`（或 `keyword` / `matched_keywords`）
- 分辨率字段：`width`, `height`（或 `photo_width`, `photo_height`, `W`, `H`）
- Header 尺寸：`header_W`, `header_H`（或小写变体）
- EXIF 字段：`focal_length`, `aperture`, `exposure_time`, `iso`（或 `exif_*` 前缀）
- 统计字段：`laplacian_var`, `subject_saliency_ratio`

如果 CSV 没有这些列，脚本会在下载成功后补充可直接获取的值（例如文件大小、图片宽高、宽高比、最短边）。
---

## 4. 推荐工作流（从 0 到可分析）

1. 放好数据分片与文档。
2. 跑 `summary`，确认表都能识别。
3. 跑 `filter-sample`，先拿一个小样本 CSV。
4. 确认无误后，再跑 `download-from-csv` 下载图片。
5. 对下载结果做模型训练或可视化分析。

---

## 5. 常见问题

### Q1：为什么脚本提示“未找到 keywords 数据”？

- 目录下不存在 `keywords.csv / keywords.tsv / keywords.csv000...`
- 或者你传错了 `--dataset-dir`

### Q2：官方文档写的是 TSV，我这里是 CSV，怎么办？

- 正常情况。脚本会依据文件后缀自动选择分隔符。

### Q3：如果之后补充 `csv001/csv002` 文件怎么办？

- 无需改代码，脚本会按序号自动拼接读取。

### Q4：`photo_image_url` 能直接加尺寸参数吗？

- 可以，Unsplash 图片 URL 支持动态参数（例如宽高、质量）。你可在下载前自行拼接参数，做统一分辨率数据集。

