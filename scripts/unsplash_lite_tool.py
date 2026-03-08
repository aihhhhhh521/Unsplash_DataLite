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
import concurrent.futures
import csv
import glob
import json
import math
import random
import re
import ssl
import struct
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Sequence, Tuple

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
    """检测分隔符。

    Unsplash Lite 官方示例按 `sep='\t'` 读取，即使文件名是 `.csv`。
    因此这里优先根据文件内容判断：首行若 Tab 数不少于逗号数，则按 TSV 处理。
    """
    with path.open("r", encoding="utf-8", newline="") as f:
        first_line = f.readline()

    if first_line:
        tab_count = first_line.count("\t")
        comma_count = first_line.count(",")
        if tab_count >= comma_count:
            return "\t"

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

def download_with_retry(
    url: str,
    target: Path,
    timeout: float,
    retries: int,
    backoff: float,
    user_agent: str,
) -> None:
    context = ssl.create_default_context()
    retry_status_codes = {408, 429, 500, 502, 503, 504}
    attempts = max(retries, 0) + 1

    for attempt in range(attempts):
        part_path = target.with_name(f"{target.name}.part")
        should_retry = False
        err_summary = ""

        try:
            req = urllib.request.Request(url, headers={"User-Agent": user_agent})
            with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
                status = getattr(resp, "status", 200)
                if not (200 <= status < 300):
                    if status in retry_status_codes:
                        raise urllib.error.HTTPError(url, status, f"HTTP {status}", resp.headers, None)
                    raise RuntimeError(f"HTTP {status} 不可重试")

                with part_path.open("wb") as out_f:
                    while True:
                        chunk = resp.read(1024 * 64)
                        if not chunk:
                            break
                        out_f.write(chunk)
            part_path.replace(target)
            return
        except urllib.error.HTTPError as e:
            if e.code in retry_status_codes:
                should_retry = True
                err_summary = f"HTTP {e.code}"
            else:
                raise RuntimeError(f"HTTP {e.code} 不可重试") from e
        except (ssl.SSLError, urllib.error.URLError, TimeoutError, ConnectionResetError) as e:
            should_retry = True
            err_summary = f"{type(e).__name__}: {e}"
        finally:
            if part_path.exists():
                part_path.unlink(missing_ok=True)

        if should_retry and attempt < attempts - 1:
            wait_s = backoff * (2**attempt)
            time.sleep(wait_s)
            continue

        if should_retry:
            raise RuntimeError(f"下载失败，已重试 {attempt} 次: {err_summary}")

    raise RuntimeError("下载失败：未知错误")

def build_download_tasks(rows: Sequence[Dict[str, str]], output_dir: Path) -> Tuple[List[Dict[str, str]], int]:
    tasks: List[Dict[str, str]] = []
    skipped = 0

    for row in rows:
        photo_id = row.get("photo_id", "")
        url = row.get("photo_image_url", "")
        if not photo_id or not url:
            skipped += 1
            continue

        filename = choose_image_filename(photo_id, url)
        target = output_dir / filename
        if target.exists():
            skipped += 1
            continue

        tasks.append(
            {
                "photo_id": photo_id,
                "url": url,
                "target": str(target),
            }
        )

    return tasks, skipped

def run_download_task(
    task: Dict[str, str],
    timeout: float,
    retries: int,
    backoff: float,
    delay_s: float,
) -> Dict[str, object]:
    photo_id = task["photo_id"]
    url = task["url"]
    target = Path(task["target"])
    started = time.perf_counter()

    result: Dict[str, object] = {
        "photo_id": photo_id,
        "status": "failed",
        "error": "",
        "bytes": 0,
        "elapsed": 0.0,
    }

    try:
        download_with_retry(
            url=url,
            target=target,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            user_agent="UnsplashLiteTool/1.0",
        )
        result["status"] = "ok"
        result["bytes"] = target.stat().st_size if target.exists() else 0
    except Exception as e:
        result["error"] = str(e)
    finally:
        result["elapsed"] = time.perf_counter() - started
        if delay_s > 0:
            time.sleep(delay_s)

    return result

def _clean_text(value: str | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def pick_first_value(row: Dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in row:
            value = _clean_text(row.get(key))
            if value:
                return value
    return ""


def to_float_or_none(value: str | None) -> float | None:
    raw = _clean_text(value)
    if not raw:
        return None
    try:
        val = float(raw)
    except ValueError:
        return None
    if not math.isfinite(val):
        return None
    return val


def to_int_or_none(value: str | None) -> int | None:
    val = to_float_or_none(value)
    if val is None:
        return None
    return int(round(val))


def read_image_size(path: Path) -> Tuple[int | None, int | None]:
    try:
        with path.open("rb") as f:
            head = f.read(32)
            if len(head) < 10:
                return None, None

            if head.startswith(b"\x89PNG\r\n\x1a\n"):
                f.seek(16)
                chunk = f.read(8)
                if len(chunk) == 8:
                    return struct.unpack(">II", chunk)

            if head[:3] == b"GIF":
                f.seek(6)
                chunk = f.read(4)
                if len(chunk) == 4:
                    width, height = struct.unpack("<HH", chunk)
                    return int(width), int(height)

            if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
                f.seek(12)
                vp8_header = f.read(4)
                if vp8_header == b"VP8 ":
                    f.seek(26)
                    chunk = f.read(4)
                    if len(chunk) == 4:
                        width, height = struct.unpack("<HH", chunk)
                        return int(width & 0x3FFF), int(height & 0x3FFF)
                if vp8_header == b"VP8L":
                    f.seek(21)
                    chunk = f.read(4)
                    if len(chunk) == 4:
                        b0, b1, b2, b3 = chunk
                        width = 1 + (((b1 & 0x3F) << 8) | b0)
                        height = 1 + (((b3 & 0x0F) << 10) | (b2 << 2) | ((b1 & 0xC0) >> 6))
                        return int(width), int(height)
                if vp8_header == b"VP8X":
                    f.seek(24)
                    chunk = f.read(6)
                    if len(chunk) == 6:
                        width = 1 + int.from_bytes(chunk[0:3], "little")
                        height = 1 + int.from_bytes(chunk[3:6], "little")
                        return width, height

            if head.startswith(b"\xff\xd8"):
                f.seek(2)
                while True:
                    marker_start = f.read(1)
                    if not marker_start:
                        break
                    if marker_start != b"\xff":
                        continue
                    marker = f.read(1)
                    while marker == b"\xff":
                        marker = f.read(1)
                    if not marker:
                        break
                    marker_byte = marker[0]
                    if marker_byte in {0xD8, 0xD9}:
                        continue
                    length_bytes = f.read(2)
                    if len(length_bytes) != 2:
                        break
                    seg_len = struct.unpack(">H", length_bytes)[0]
                    if seg_len < 2:
                        break
                    if marker_byte in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
                        data = f.read(seg_len - 2)
                        if len(data) >= 5:
                            height, width = struct.unpack(">HH", data[1:5])
                            return int(width), int(height)
                        break
                    f.seek(seg_len - 2, 1)
    except OSError:
        return None, None

    return None, None


def make_metadata_record(task: Dict[str, Any], result: Dict[str, object]) -> Dict[str, Any]:
    row: Dict[str, str] = task["row"]
    local_path = task["target"]
    status = str(result.get("status", "failed"))
    error = _clean_text(str(result.get("error", "")))
    downloaded_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    csv_w = to_int_or_none(pick_first_value(row, "width", "photo_width", "W", "w"))
    csv_h = to_int_or_none(pick_first_value(row, "height", "photo_height", "H", "h"))

    header_w = to_int_or_none(pick_first_value(row, "header_W", "header_w"))
    header_h = to_int_or_none(pick_first_value(row, "header_H", "header_h"))

    if status == "ok":
        file_path = Path(local_path)
        if header_w is None or header_h is None or csv_w is None or csv_h is None:
            detected_w, detected_h = read_image_size(file_path)
            if header_w is None:
                header_w = detected_w
            if header_h is None:
                header_h = detected_h
            if csv_w is None:
                csv_w = detected_w
            if csv_h is None:
                csv_h = detected_h

    width = csv_w
    height = csv_h
    min_side = min(width, height) if width and height else None
    aspect_ratio = (round(width / height, 6) if width and height and height != 0 else None)
    header_pixels = (header_w * header_h) if header_w and header_h else None

    filesize_kb = None
    if status == "ok":
        bytes_size = result.get("bytes", 0)
        if isinstance(bytes_size, (int, float)):
            filesize_kb = round(float(bytes_size) / 1024, 3)

    exif_data = {
        "focal_length": pick_first_value(row, "focal_length", "exif_focal_length") or None,
        "aperture": pick_first_value(row, "aperture", "exif_aperture") or None,
        "exposure_time": pick_first_value(row, "exposure_time", "exif_exposure_time") or None,
        "iso": to_int_or_none(pick_first_value(row, "iso", "exif_iso")),
    }

    pipeline_metrics = {
        "filesize_kb": filesize_kb,
        "header_W": header_w,
        "header_H": header_h,
        "header_pixels": header_pixels,
        "W": width,
        "H": height,
        "min_side": min_side,
        "aspect_ratio": aspect_ratio,
        "laplacian_var": to_float_or_none(pick_first_value(row, "laplacian_var")),
        "subject_saliency_ratio": to_float_or_none(pick_first_value(row, "subject_saliency_ratio")),
    }

    return {
        "photo_id": task["photo_id"],
        "photo_image_url": task["url"],
        "local_path": local_path,
        "status": status,
        "error": error or None,
        "downloaded_at": downloaded_at,
        "search_keyword": pick_first_value(row, "search_keyword", "keyword", "matched_keywords") or None,
        "resolution": [width, height] if width and height else None,
        "exif_data": exif_data,
        "pipeline_metrics": pipeline_metrics,
    }

def cmd_download_from_csv(
    input_csv: Path,
    output_dir: Path,
    delay_s: float,
    limit: int,
    workers: int,
    timeout: float,
    retries: int,
    backoff: float,
    metadata_jsonl: Path | None = None,
    manifest_json: Path | None = None,
) -> int:
    if not input_csv.exists():
        print(f"输入文件不存在: {input_csv}")
        return 2

    output_dir.mkdir(parents=True, exist_ok=True)
    with input_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if limit > 0:
        rows = rows[:limit]

    tasks, skipped = build_download_tasks(rows, output_dir)
    row_index: Dict[str, Dict[str, str]] = {row.get("photo_id", ""): row for row in rows if row.get("photo_id")}
    for task in tasks:
        task["row"] = row_index.get(task["photo_id"], {})
    total = len(tasks)
    success = 0
    failed = 0
    metadata_records: List[Dict[str, Any]] = []
    started_at = time.perf_counter()

    metadata_handle = None
    if metadata_jsonl is not None:
        metadata_jsonl.parent.mkdir(parents=True, exist_ok=True)
        metadata_handle = metadata_jsonl.open("a", encoding="utf-8")

    if total == 0:
        print(f"没有可下载任务（跳过 {skipped} 条）。")
        if manifest_json is not None:
            manifest_json.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "summary": {
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "skipped": skipped,
                    "elapsed_seconds": 0.0,
                },
                "records": [],
            }
            manifest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(workers, 1)) as executor:
            futures = {
                executor.submit(
                    run_download_task,
                    task,
                    timeout,
                    retries,
                    backoff,
                    delay_s,
                ): task
                for task in tasks
            }

            for done, future in enumerate(concurrent.futures.as_completed(futures), start=1):
                result = future.result()
                task = futures[future]
                photo_id = result["photo_id"]
                status = result["status"]
                elapsed = result["elapsed"]
                if status == "ok":
                    success += 1
                    size = result["bytes"]
                    print(f"[{done}/{total}] 下载成功: {photo_id} ({size} bytes, {elapsed:.2f}s)")
                else:
                    failed += 1
                    print(f"[{done}/{total}] 下载失败: {photo_id} ({result['error']}, {elapsed:.2f}s)")

                metadata_record = make_metadata_record(task, result)
                metadata_records.append(metadata_record)
                if metadata_handle is not None:
                    metadata_handle.write(json.dumps(metadata_record, ensure_ascii=False) + "\n")
                    metadata_handle.flush()
    finally:
        if metadata_handle is not None:
            metadata_handle.close()

    total_elapsed = time.perf_counter() - started_at

    if manifest_json is not None:
        manifest_json.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": {
                "total": total,
                "success": success,
                "failed": failed,
                "skipped": skipped,
                "elapsed_seconds": round(total_elapsed, 3),
            },
            "records": metadata_records,
        }
        manifest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"完成：成功 {success}，失败 {failed}，跳过 {skipped}，目录 {output_dir}")
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
    dl.add_argument("--workers", type=int, default=8, help="并发下载 worker 数，默认: 8")
    dl.add_argument("--timeout", type=float, default=20.0, help="单次请求超时秒数，默认: 20")
    dl.add_argument("--retries", type=int, default=3, help="失败重试次数，默认: 3")
    dl.add_argument("--backoff", type=float, default=1.0, help="指数退避基数秒数，默认: 1.0")
    dl.add_argument("--delay", type=float, default=0.0, help="可选限速：每个任务结束后的等待秒数")
    dl.add_argument("--limit", type=int, default=0, help="最多下载多少条，0 表示不限制")
    dl.add_argument(
        "--metadata-jsonl",
        type=Path,
        default=None,
        help="可选：将每条下载结果以 JSONL 追加写入该文件",
    )
    dl.add_argument(
        "--manifest-json",
        type=Path,
        default=None,
        help="可选：将本次下载汇总与明细写入 JSON 文件",
    )

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
            workers=args.workers,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            metadata_jsonl=args.metadata_jsonl,
            manifest_json=args.manifest_json,
        )

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
