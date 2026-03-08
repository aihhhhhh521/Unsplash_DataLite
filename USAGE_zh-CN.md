# Unsplash Lite 数据集使用教程（中文）

> 适用目录：`unsplash-research-dataset-lite-latest/`

## 1. 数据集内容概览

根据官方文档，Lite 数据集通常包含 5 张主表（可能是 `.tsv` 或 `.csv`，也可能按分片方式存储）：

1. `photos`：图片主信息（作者、尺寸、EXIF、下载统计等）
2. `keywords`：图片与关键词的映射
3. `collections`：图片与合集的映射
4. `conversions`：图片下载转化相关统计
5. `colors`：图片颜色分布信息

你的目录截图显示为 `*.csv000`，这属于分片文件命名方式（例如 `photos.csv000`、`photos.csv001` ...），脚本需要支持自动拼接读取。

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

## 3. 我为你准备的工具脚本

文件：`scripts/unsplash_lite_tool.py`

功能：

- 自动识别普通文件和分片文件（如 `photos.csv000`）
- 统计各表行数、字段列表、前几行样例
- 输出 “photo_id + keyword” 样例，快速验证数据可用性

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

你会得到一个 JSON，包含：

- 表是否找到
- 使用了哪些分片文件
- 行数（rows）
- 字段名（columns）
- 前 3 条示例（preview）

### 3.4 抽样查看关键词映射

```bash
python scripts/unsplash_lite_tool.py --dataset-dir unsplash-research-dataset-lite-latest keyword-samples --limit 30
```

输出示例：

```text
001. abc123	forest
002. xyz456	mountain
```

---

## 4. 推荐工作流（从 0 到可分析）

1. 把数据分片和 `README.md / DOCS.md / TERMS.md` 放在同一目录（你当前就是这个结构）。
2. 先跑 `summary`，确认每张表都能被识别。
3. 再跑 `keyword-samples`，快速检查核心关联字段是否可用。
4. 如果你后续做模型训练，建议先导出一个 1% 子集进行迭代开发（节省 I/O 和算力）。

---

## 5. 常见问题

### Q1：为什么脚本提示“未找到 keywords 数据”？

- 目录下不存在 `keywords.csv / keywords.tsv / keywords.csv000...`
- 或者你传错了 `--dataset-dir`

### Q2：官方文档写的是 TSV，我这里是 CSV，怎么办？

- 正常情况。脚本会依据文件后缀自动选择分隔符。

### Q3：如果之后补充 `csv001/csv002` 文件怎么办？

- 无需改代码，脚本会按序号自动拼接读取。

---

## 6. 下一步可以继续做什么

如果你愿意，我可以继续给你补这三部分代码：

1. `to_parquet.py`：把原始 CSV/TSV 分片转为 Parquet（加速分析）
2. `build_training_subset.py`：按关键词/作者/时间分层抽样
3. `basic_eda.ipynb`：直接可运行的数据探索 Notebook（分布、热词、作者活跃度）

