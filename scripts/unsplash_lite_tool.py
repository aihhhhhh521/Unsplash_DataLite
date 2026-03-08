#!/usr/bin/env python3
"""Unsplash Lite 数据集辅助工具。

功能：
1) 自动识别按序号拆分的 CSV/TSV 分片（如 photos.csv000, photos.csv001 ...）。
2) 统计每张表的记录数、字段列表和示例行。
3) 输出“照片 + 关键词”的轻量级关联样例，便于快速验证数据可用性。
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

TABLE_BASENAMES = ["photos", "keywords", "collections", "conversions", "colors"]


def detect_delimiter(path: Path) -> str:
    if path.suffix.lower().startswith(".tsv"):
        return "\t"
    return ","


def find_table_parts(dataset_dir: Path, basename: str) -> List[Path]:
    patterns = [
        f"{basename}.csv", f"{basename}.tsv",
        f"{basename}.csv*", f"{basename}.tsv*",
    ]
    matches: List[Path] = []
    for pattern in patterns:
        matches.extend(Path(p) for p in glob.glob(str(dataset_dir / pattern)))
    if not matches:
        return []

    unique = sorted(set(matches), key=lambda p: p.name)

    plain = [p for p in unique if p.name in {f"{basename}.csv", f"{basename}.tsv"}]
    if plain:
        return sorted(plain)

    split = []
    for p in unique:
        suffix = p.name.split(".")[-1]
        if suffix.isdigit():
            split.append(p)
    return sorted(split, key=lambda p: int(p.name.split(".")[-1]))


def iter_rows(paths: Sequence[Path], delimiter: str) -> Iterator[Dict[str, str]]:
    fieldnames: List[str] | None = None
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames is None:
                continue
            if fieldnames is None:
                fieldnames = reader.fieldnames
            for row in reader:
                yield row


def summarize_table(paths: Sequence[Path]) -> Dict[str, object]:
    delimiter = detect_delimiter(paths[0])
    total = 0
    preview: List[Dict[str, str]] = []
    fieldnames: List[str] = []

    for idx, row in enumerate(iter_rows(paths, delimiter)):
        if not fieldnames:
            fieldnames = list(row.keys())
        total += 1
        if idx < 3:
            preview.append(row)

    return {
        "parts": [str(p.name) for p in paths],
        "rows": total,
        "columns": fieldnames,
        "preview": preview,
    }


def cmd_summary(dataset_dir: Path, pretty: bool) -> int:
    result = {}
    for table in TABLE_BASENAMES:
        parts = find_table_parts(dataset_dir, table)
        if not parts:
            result[table] = {"found": False}
            continue
        info = summarize_table(parts)
        info["found"] = True
        result[table] = info

    if pretty:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(result, ensure_ascii=False))
    return 0


def take_keywords(dataset_dir: Path, limit: int) -> List[Tuple[str, str]]:
    parts = find_table_parts(dataset_dir, "keywords")
    if not parts:
        return []
    delimiter = detect_delimiter(parts[0])

    out: List[Tuple[str, str]] = []
    for row in iter_rows(parts, delimiter):
        photo_id = row.get("photo_id", "")
        keyword = row.get("keyword", "")
        if photo_id and keyword:
            out.append((photo_id, keyword))
        if len(out) >= limit:
            break
    return out


def cmd_keyword_samples(dataset_dir: Path, limit: int) -> int:
    pairs = take_keywords(dataset_dir, limit)
    if not pairs:
        print("未找到 keywords 数据，请确认目录中存在 keywords.csv/tsv 或其分片文件。")
        return 1
    for i, (photo_id, keyword) in enumerate(pairs, start=1):
        print(f"{i:03d}. {photo_id}\t{keyword}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unsplash Lite 数据集辅助工具")
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=Path("unsplash-research-dataset-lite-latest"),
        help="数据集目录，默认: unsplash-research-dataset-lite-latest",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    summary = sub.add_parser("summary", help="输出各表统计信息")
    summary.add_argument("--pretty", action="store_true", help="美化 JSON 输出")

    keyword_samples = sub.add_parser("keyword-samples", help="输出照片-关键词样例")
    keyword_samples.add_argument("--limit", type=int, default=20, help="样例数量")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    dataset_dir: Path = args.dataset_dir

    if not dataset_dir.exists():
        print(f"数据目录不存在: {dataset_dir}")
        return 2

    if args.command == "summary":
        return cmd_summary(dataset_dir, pretty=args.pretty)
    if args.command == "keyword-samples":
        return cmd_keyword_samples(dataset_dir, limit=args.limit)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
