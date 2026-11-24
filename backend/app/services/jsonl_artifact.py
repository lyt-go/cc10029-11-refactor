"""传输与暂存共用的 JSONL 产物读写与提交约定。

本模块收拢传输批次（batches 目录）和暂存落地（staging 目录）两侧对 JSONL
产物文件的读取、分片追加写入与提交确认逻辑，保证两侧遵循同一套约定：

- 读取约定：产物文件按 ``missing``（缺文件）/ ``empty``（存在但零条记录）/
  ``partial``（有记录但未提交）/ ``committed``（已提交）四种状态返回，
  缺文件与空文件的旧返回（空记录列表）和落盘值由调用方按各自语义单独对照。
- 写入约定：按分片追加写入 JSONL，分片写完后先做本侧数据确认（重盘回读 +
  条数/校验值核对），确认通过再原子落盘提交清单（``*.commit.json``），
  只有提交清单存在才表示该侧的完整批次信号。
- 恢复约定：再次执行时按产物状态决定恢复范围——已提交则幂等复用，
  未提交则只追加缺失分片，缺文件则从头写入。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.services.base import DATA_ROOT

# 提交清单后缀：与 .jsonl 产物同目录、同前缀，glob("*.jsonl") 不会命中。
COMMIT_SUFFIX = ".commit.json"


class ArtifactWriteInterrupted(RuntimeError):
    """后端写入中断：首片（或指定分片）已落盘，但本侧数据确认与提交尚未完成。

    抛出时产物文件处于 partial 状态，再次执行将从未提交位置恢复。
    """

    def __init__(self, filepath: Path, flushed_shards: int, flushed_records: int):
        self.filepath = filepath
        self.flushed_shards = flushed_shards
        self.flushed_records = flushed_records
        super().__init__(
            f"产物写入中断: {filepath} 已落盘 {flushed_shards} 个分片/"
            f"{flushed_records} 条记录，尚未提交确认"
        )


class BatchNotCommitted(RuntimeError):
    """上游批次产物存在但未提交（partial），下游不能据此继续处理。"""

    def __init__(self, artifact: Path, records_on_disk: int):
        self.artifact = artifact
        self.records_on_disk = records_on_disk
        super().__init__(
            f"上游批次未完成提交: {artifact} 仅有 {records_on_disk} 条落盘记录，"
            "缺少完整批次信号"
        )


@dataclass
class ArtifactRead:
    """一次产物读取的结果，保留缺文件/空文件/部分写入/已提交四种旧行为对照。"""

    filepath: Path
    records: list[dict[str, Any]]
    state: str  # missing | empty | partial | committed
    manifest: dict[str, Any] | None

    @property
    def exists(self) -> bool:
        return self.state != "missing"

    @property
    def committed(self) -> bool:
        return self.state == "committed"


def artifact_path(group: str, key: str) -> Path:
    """返回某一侧（batches/staging/...）产物 JSONL 文件路径。"""
    return DATA_ROOT / group / f"{key}.jsonl"


def commit_path_for(filepath: Path) -> Path:
    """返回产物文件对应的提交清单路径（同目录、同前缀）。"""
    return filepath.with_name(filepath.name + COMMIT_SUFFIX)


def _parse_jsonl(filepath: Path) -> list[dict[str, Any]]:
    """解析 JSONL 文件，行为与 BaseModuleService._read_jsonl 完全一致。

    文件不存在返回空列表；空文件返回空列表；坏行跳过。
    """
    if not filepath.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _load_manifest(filepath: Path) -> dict[str, Any] | None:
    """读取提交清单，文件缺失或内容损坏一律视为未提交。"""
    commit_file = commit_path_for(filepath)
    if not commit_file.exists():
        return None
    try:
        with open(commit_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _manifest_matches(manifest: dict[str, Any] | None, records: list[dict[str, Any]]) -> bool:
    """核对提交清单与当前落盘数据是否一致（条数 + 校验值）。"""
    if not manifest:
        return False
    if int(manifest.get("record_count", -1)) != len(records):
        return False
    return manifest.get("checksum") == _checksum_of(records)


def _checksum_of(records: list[dict[str, Any]]) -> str:
    """校验值算法沿用 BaseModuleService._compute_checksum(str(records)) 前 16 位。"""
    import hashlib

    return hashlib.sha256(str(records).encode("utf-8")).hexdigest()[:16]


def _inspect(filepath: Path) -> ArtifactRead:
    """按路径直接检查产物状态，与 read_artifact 使用同一套判定规则。"""
    if not filepath.exists():
        return ArtifactRead(filepath, [], "missing", None)
    records = _parse_jsonl(filepath)
    if not records:
        return ArtifactRead(filepath, [], "empty", None)
    manifest = _load_manifest(filepath)
    state = "committed" if _manifest_matches(manifest, records) else "partial"
    return ArtifactRead(filepath, records, state, manifest)


def read_artifact(group: str, key: str) -> ArtifactRead:
    """按共用约定读取一侧产物，区分缺文件、空文件、部分写入与已提交。

    旧行为对照：缺文件与空文件都返回空记录列表（``records == []``），
    调用方可通过 ``state`` 区分两者旧的落盘值差异。
    """
    return _inspect(artifact_path(group, key))


def require_committed(group: str, key: str) -> ArtifactRead:
    """下游读取入口：仅当上游批次已完整提交才放行，partial 直接拒绝。

    缺文件/空文件不在此拦截，保留两侧各自原有的模拟/空数据兜底行为。
    """
    result = read_artifact(group, key)
    if result.state == "partial":
        raise BatchNotCommitted(result.filepath, len(result.records))
    return result


# -- 测试用后端写入中断注入（一次性，触发后自动清除） --

_write_faults: dict[str, int] = {}


def arm_write_fault(filepath: Path, fail_after_shards: int = 1) -> None:
    """在指定产物写入 N 个分片后模拟后端写入中断（一次性触发）。"""
    _write_faults[str(filepath)] = fail_after_shards


def clear_write_fault(filepath: Path) -> None:
    """清除指定产物上的写入中断注入。"""
    _write_faults.pop(str(filepath), None)


def clear_all_write_faults() -> None:
    """清除全部写入中断注入。"""
    _write_faults.clear()


def _consume_fault(filepath: Path, flushed_shards: int) -> int | None:
    threshold = _write_faults.get(str(filepath))
    if threshold is not None and flushed_shards >= threshold:
        # 一次性触发：恢复执行时不再重复中断
        _write_faults.pop(str(filepath), None)
        return threshold
    return None


def write_artifact(
    filepath: Path,
    records: list[dict[str, Any]],
    *,
    shard_size: int,
    allow_empty_file: bool,
    extra_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """按共用约定分片写入一侧产物并完成本侧数据确认与提交。

    流程：
    1. 读取产物现状（missing/empty/partial/committed）；
    2. 已提交且数据一致 -> 幂等复用，不再写盘；
    3. 未提交 -> 按已有落盘条数定位恢复点，只追加缺失分片，首片后中断会
       抛出 :class:`ArtifactWriteInterrupted`，此时不写提交清单；
    4. 全部分片落盘后重盘回读，核对条数与校验值，通过才原子写入提交清单。

    ``allow_empty_file`` 保留两侧旧差异：暂存侧空数据也建空文件，
    传输侧零条数据不建文件。
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    current = _inspect(filepath)
    already = len(current.records)

    if current.state == "committed":
        return {
            "filepath": filepath,
            "state": "committed",
            "resumed": False,
            "reused": True,
            "shards_flushed": int(current.manifest.get("shard_count", 0)),
            "records_written": 0,
            "record_count": already,
            "total_bytes": int(current.manifest.get("total_bytes", 0)),
            "checksum": current.manifest.get("checksum", ""),
            "manifest": current.manifest,
        }

    if already > len(records):
        # 落盘数据比本次应写还多，属于异常状态，不做静默截断
        raise ArtifactWriteInterrupted(filepath, 0, already)

    # 缺文件但允许空文件（暂存旧行为）：先建空文件
    if not records and allow_empty_file and not filepath.exists():
        filepath.touch()

    shards_flushed = 0
    appended = 0
    # partial 恢复时已落盘的分片视为已完成，从下一片开始
    start_index = already
    for i in range(start_index, len(records), shard_size):
        chunk = records[i:i + shard_size]
        with open(filepath, "a", encoding="utf-8") as f:
            for rec in chunk:
                f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        shards_flushed += 1
        appended += len(chunk)
        fault_at = _consume_fault(filepath, shards_flushed)
        if fault_at is not None:
            # 首片已落盘但尚未交付：不做数据确认、不写提交清单
            raise ArtifactWriteInterrupted(filepath, shards_flushed, already + appended)

    # 全部分片落盘，执行本侧数据确认：重盘回读并核对条数、校验值
    on_disk = _parse_jsonl(filepath)
    if len(on_disk) != len(records):
        raise ArtifactWriteInterrupted(filepath, shards_flushed, len(on_disk))
    checksum = _checksum_of(on_disk)
    total_bytes = sum(len(str(r).encode("utf-8")) for r in on_disk)
    total_shards = (len(records) + shard_size - 1) // shard_size if records else 0

    # 传输侧零条数据不建产物文件（旧行为）：没有产物就没有提交清单，
    # 下游读到的仍是 missing，保留各自旧的缺文件兜底语义。
    if not filepath.exists():
        return {
            "filepath": filepath,
            "state": "missing",
            "resumed": False,
            "reused": False,
            "shards_flushed": 0,
            "records_written": 0,
            "record_count": 0,
            "total_bytes": 0,
            "checksum": checksum,
            "manifest": None,
        }

    manifest = {
        "artifact": filepath.name,
        "record_count": len(records),
        "shard_count": total_shards,
        "shard_size": shard_size,
        "total_bytes": total_bytes,
        "checksum": checksum,
        "committed_at": _now_ts(),
    }
    if extra_manifest:
        manifest.update(extra_manifest)

    # 原子提交：临时文件 + os.replace，提交清单是完整批次的唯一信号
    commit_file = commit_path_for(filepath)
    tmp_file = commit_file.with_name(commit_file.name + ".tmp")
    with open(tmp_file, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp_file, commit_file)

    return {
        "filepath": filepath,
        "state": "committed",
        "resumed": already > 0,
        "reused": False,
        "shards_flushed": shards_flushed,
        "records_written": appended,
        "record_count": len(records),
        "total_bytes": total_bytes,
        "checksum": checksum,
        "manifest": manifest,
    }


def _now_ts() -> float:
    import time

    return time.time()
