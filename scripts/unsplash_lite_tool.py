#!/usr/bin/env python3
"""Unsplash Lite 数据集辅助工具。

功能：
1) 自动识别按序号拆分的 CSV/TSV 分片（如 photos.csv000, photos.csv001 ...）。
2) 统计每张表的记录数、字段列表和示例行。
3) 输出“照片 + 关键词”的轻量级关联样例。
4) 按关键词筛选并随机采样，导出结果（含图片 URL）。
5) 可选：把采样结果中的图片 URL 下载到本地。
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterator, List, Sequence, Tuple

TABLE_BASENAMES = ["photos", "keywords", "collections", "conversions", "colors"]

def configure_csv_field_limit() -> None:
    """放宽 csv 单字段默认长度限制，避免超长字段触发 _csv.Error。"""
    limit = None
    for candidate in (sys.maxsize, 2**31 - 1):
        try:
            csv.field_size_limit(candidate)
            limit = candidate
            break
        except OverflowError:
            continue
    if limit is None:
        csv.field_size_limit(131072)


def parse_split_index(filename: str, basename: str) -> int | None:
    """解析分片序号，兼容 photos.csv000 / photos.csv.000 / photos.tsv001 等命名。"""
    m = re.fullmatch(rf"{re.escape(basename)}\.(csv|tsv)(?:\.)?(\d+)", filename)
    if not m:
        return None
    return int(m.group(2))



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

    split: List[Tuple[int, Path]] = []
    for p in unique:
        idx = parse_split_index(p.name, basename)
        if idx is not None:
            split.append((idx, p))
    return [p for _, p in sorted(split, key=lambda x: x[0])]


def iter_rows(paths: Sequence[Path], delimiter: str) -> Iterator[Dict[str, str]]:
    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            if reader.fieldnames is None:
                continue
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


def load_photo_urls(dataset_dir: Path) -> Dict[str, Dict[str, str]]:
    parts = find_table_parts(dataset_dir, "photos")
    if not parts:
        return {}
    delimiter = detect_delimiter(parts[0])

    url_map: Dict[str, Dict[str, str]] = {}
    for row in iter_rows(parts, delimiter):
        photo_id = row.get("photo_id", "")
        if not photo_id:
            continue
        url_map[photo_id] = {
            "photo_url": row.get("photo_url", ""),
            "photo_image_url": row.get("photo_image_url", ""),
            "photographer_username": row.get("photographer_username", ""),
        }
    return url_map


def normalize_keywords(raw: str) -> List[str]:
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


def cmd_filter_sample(
    dataset_dir: Path,
    include_keywords: List[str],
    require_all: bool,
    sample_size: int,
    seed: int,
    output_csv: Path,
) -> int:
    parts = find_table_parts(dataset_dir, "keywords")
    if not parts:
        print("未找到 keywords 数据。")
        return 1

    delimiter = detect_delimiter(parts[0])
    photo_to_keywords: Dict[str, set[str]] = {}

    for row in iter_rows(parts, delimiter):
        photo_id = row.get("photo_id", "")
        keyword = row.get("keyword", "").strip().lower()
        if not photo_id or not keyword:
            continue
        photo_to_keywords.setdefault(photo_id, set()).add(keyword)

    if not include_keywords:
        candidates = list(photo_to_keywords.keys())
    else:
        wanted = set(include_keywords)
        candidates = []
        for photo_id, kws in photo_to_keywords.items():
            if require_all:
                ok = wanted.issubset(kws)
            else:
                ok = bool(wanted.intersection(kws))
            if ok:
                candidates.append(photo_id)

    if not candidates:
        print("没有匹配到任何图片，请调整关键词或匹配模式。")
        return 1

    rng = random.Random(seed)
    n = min(sample_size, len(candidates))
    sampled_ids = rng.sample(candidates, n)
    photo_urls = load_photo_urls(dataset_dir)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "photo_id",
                "matched_keywords",
                "photo_url",
                "photo_image_url",
                "photographer_username",
            ],
        )
        writer.writeheader()
        for photo_id in sampled_ids:
            kws = sorted(photo_to_keywords.get(photo_id, []))
            meta = photo_urls.get(photo_id, {})
            writer.writerow(
                {
                    "photo_id": photo_id,
                    "matched_keywords": "|".join(kws),
                    "photo_url": meta.get("photo_url", ""),
                    "photo_image_url": meta.get("photo_image_url", ""),
                    "photographer_username": meta.get("photographer_username", ""),
                }
            )

    print(f"已写出 {n} 条样本到: {output_csv}")
    print(f"候选总数: {len(candidates)}")
    return 0


def choose_image_filename(photo_id: str, url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    suffix = Path(parsed.path).suffix.lower() or ".jpg"
    return f"{photo_id}{suffix}"


def cmd_download_from_csv(input_csv: Path, output_dir: Path, delay_s: float, limit: int) -> int:
    if not input_csv.exists():
        print(f"输入文件不存在: {input_csv}")
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)
    ok = 0
    skip = 0

    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if limit > 0:
        rows = rows[:limit]

    for idx, row in enumerate(rows, start=1):
        photo_id = row.get("photo_id", "")
        url = row.get("photo_image_url", "")
        if not photo_id or not url:
            skip += 1
            continue

        filename = choose_image_filename(photo_id, url)
        target = output_dir / filename
        if target.exists():
            skip += 1
            continue

        try:
            urllib.request.urlretrieve(url, target)
            ok += 1
            print(f"[{idx}/{len(rows)}] 下载成功: {target.name}")
            if delay_s > 0:
                time.sleep(delay_s)
        except Exception as e:
            skip += 1
            print(f"[{idx}/{len(rows)}] 下载失败: {photo_id} ({e})")

    print(f"完成：成功 {ok}，跳过/失败 {skip}，目录 {output_dir}")
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

    filter_sample = sub.add_parser(
        "filter-sample",
        help="按关键词筛选并随机采样，导出 CSV（包含图片 URL）",
    )
    filter_sample.add_argument(
        "--keywords",
        type=str,
        default="",
        help="英文关键词，逗号分隔。如: forest,mountain,snow",
    )
    filter_sample.add_argument(
        "--require-all",
        action="store_true",
        help="默认是“任一关键词匹配”；加此参数改为“必须包含全部关键词”",
    )
    filter_sample.add_argument("--sample-size", type=int, default=100, help="采样数量")
    filter_sample.add_argument("--seed", type=int, default=42, help="随机种子")
    filter_sample.add_argument(
        "--output-csv",
        type=Path,
        default=Path("outputs/sampled_photos.csv"),
        help="导出 CSV 路径",
    )

    dl = sub.add_parser("download-from-csv", help="根据 CSV 的 photo_image_url 下载图片")
    dl.add_argument("--input-csv", type=Path, required=True, help="输入 CSV（需含 photo_id/photo_image_url）")
    dl.add_argument("--output-dir", type=Path, default=Path("outputs/images"), help="下载目录")
    dl.add_argument("--delay", type=float, default=0.2, help="每次下载后的间隔秒数")
    dl.add_argument("--limit", type=int, default=0, help="最多下载多少条，0 表示不限制")

    return parser


def main() -> int:
    configure_csv_field_limit()
    parser = build_parser()
    args = parser.parse_args()
    dataset_dir: Path = args.dataset_dir

    if args.command in {"summary", "keyword-samples", "filter-sample"} and not dataset_dir.exists():
        print(f"数据目录不存在: {dataset_dir}")
        return 2

    if args.command == "summary":
        return cmd_summary(dataset_dir, pretty=args.pretty)
    if args.command == "keyword-samples":
        return cmd_keyword_samples(dataset_dir, limit=args.limit)
    if args.command == "filter-sample":
        return cmd_filter_sample(
            dataset_dir=dataset_dir,
            include_keywords=normalize_keywords(args.keywords),
            require_all=args.require_all,
            sample_size=args.sample_size,
            seed=args.seed,
            output_csv=args.output_csv,
        )
    if args.command == "download-from-csv":
        return cmd_download_from_csv(
            input_csv=args.input_csv,
            output_dir=args.output_dir,
            delay_s=args.delay,
            limit=args.limit,
        )

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
