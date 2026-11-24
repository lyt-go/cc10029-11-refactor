"""暂存落地领域服务：维护传输数据的暂存分区落地路径、文件组织与驻留时限控制。

本模块从传输批次产物文件读取数据，执行分区落地写入暂存产物文件，
落地完成后记录落地水位并支持超时清空。

JSONL 产物的读取、分片写入与提交确认统一走
:mod:`app.services.jsonl_artifact` 的共用约定，与传输批次共用同一套
读写与提交语义；本服务保留暂存侧收到批次交付数据后生成文件、条数与
交付结果的原有约定。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.services.jsonl_artifact import (
    ArtifactWriteInterrupted,
    BatchNotCommitted,
    read_artifact,
    require_committed,
    write_artifact,
)
from app.store import store


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["暂存编号", "批次编号", "落地路径", "文件数量", "记录条数", "占用字节", "驻留时限", "落地水位"]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "已落地", "已清空"]
    ACTION_RULES = {"启动落地": "落地中", "确认落地": "已落地", "执行清空": "已清空"}
    NEGATIVE_ACTIONS = ["执行清空"]

    # 暂存侧分片写入大小：仅用于制造/恢复中断点，正常一次调用即全部落盘
    STAGING_SHARD_SIZE = 3

    def _find_staging_artifact(self, batch_code: str) -> tuple[Path, Any] | tuple[None, None]:
        """查找本批次已有的暂存产物及其读取状态（含提交清单），没有则返回 (None, None)。

        暂存编号固定为 ``STAG-{批次编号}-{时间戳}``，匹配时带尾部 ``-`` 边界，
        避免把更长批次编号（如 BATCH-1 与 BATCH-10）的产物误判为本批次产物。
        """
        staging_dir = self._data_dir("staging")
        prefix = f"STAG-{batch_code}-"
        matches = sorted(p for p in staging_dir.glob("STAG-*.jsonl") if p.name.startswith(prefix))
        if not matches:
            return None, None
        artifact_file = matches[0]
        return artifact_file, read_artifact("staging", artifact_file.stem)

    def land_batch(self, batch_code: str, record_count: int) -> dict[str, Any]:
        """将传输批次数据落地到暂存分区，读取传输产物文件并写入暂存产物文件。

        暂存分区是传输链路中数据暂存的关键环节：它从传输产物文件读取数据，
        计算字节量，写入落地路径下的暂存产物文件，并推进分区水位。

        恢复约定（同一批次可重复进入本方法）：
        - 上游批次缺文件：沿用旧兜底生成模拟数据；空文件：读到零条；
        - 上游批次 partial（写入中断、未提交）：拒绝落地，等待传输侧重传；
        - 暂存产物已提交：幂等复用既有分区，不重复建分区；
        - 暂存产物 partial：复用原暂存编号与分区条目，只追加缺失分片，
          完成本侧数据确认与提交后才返回交付结果。
        """
        # 读取上游传输批次产物文件（共用读取约定）
        source = require_committed("batches", batch_code)
        if source.state == "missing":
            # 缺文件：沿用旧兜底生成模拟数据
            records = self._simulate_records(batch_code, count=record_count, field_prefix="staging")
        else:
            # 存在（含空文件、已提交）：以产物为准，空文件读到零条
            records = [dict(r) for r in source.records]

        # 查找本批次已有的暂存产物，决定新建、恢复还是幂等复用
        existing_file, existing_read = self._find_staging_artifact(batch_code)
        resumed = False
        if existing_file is not None and existing_read.state == "committed":
            manifest = existing_read.manifest or {}
            existing_entry = next(
                (r for r in store.rows(self.MODULE) if r.get("暂存编号") == existing_file.stem),
                None,
            )
            return {
                "staging_code": existing_file.stem,
                "batch_code": batch_code,
                "records_landed": int(manifest.get("record_count", len(existing_read.records))),
                "total_bytes": int(manifest.get("total_bytes", 0)),
                "artifact_path": str(existing_file),
                "already_committed": True,
                "staging_id": existing_entry["id"] if existing_entry else None,
            }

        if existing_file is not None:
            # partial：复用原暂存编号与分区条目，从落盘位置恢复
            staging_code = existing_file.stem
            output_file = existing_file
            entry = next(
                (r for r in store.rows(self.MODULE) if r.get("暂存编号") == staging_code),
                None,
            )
            resumed = True
        else:
            staging_code = f"STAG-{batch_code}-{int(time.time())}"
            landing_path = f"/staging/{batch_code}/"
            values = {
                "暂存编号": staging_code,
                "批次编号": batch_code,
                "落地路径": landing_path,
                "文件数量": "1",
                "记录条数": str(record_count),
                "占用字节": "0",
                "驻留时限": "86400",
                "落地水位": "0",
            }
            entry, missing = self.create_entry(values)
            if entry is None:
                raise ValueError(f"创建暂存分区失败: {missing}")
            output_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        # 分片写入 + 本侧数据确认 + 提交清单；中断时直接向上抛出，
        # 不更新水位、不写断点，调用方（传输侧）不得放行更下游环节。
        write_result = write_artifact(
            output_file,
            records,
            shard_size=self.STAGING_SHARD_SIZE,
            allow_empty_file=True,  # 暂存侧旧行为：即使零条也建立产物文件
            extra_manifest={"staging_code": staging_code, "batch_code": batch_code},
        )

        written = write_result["record_count"]
        total_bytes = write_result["total_bytes"]

        # 更新暂存分区水位（与旧落盘值一致）
        entry["占用字节"] = str(total_bytes)
        entry["文件数量"] = "1"
        entry["落地水位"] = str(written)

        # 写入落地水位断点
        self._write_checkpoint(f"staging_{staging_code}", {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "landed_at": time.time(),
            "retention_seconds": 86400,
            "resumed": resumed,
        })

        return {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "artifact_path": str(output_file),
            "resumed": resumed,
        }

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存分区落地完成，校验落地产物数据完整性并写入落地确认日志。

        落地产物存在但未完整提交（partial，写入中断）时拒绝确认，
        状态不流转、不写确认日志；缺文件/空文件沿用旧行为（读到零条）。
        """
        staging_entry = self.get_entry(staging_id)
        if staging_entry is None:
            return {"error": f"暂存分区 {staging_id} 不存在或已归档", "staging_id": staging_id}
        staging_code = staging_entry["暂存编号"]
        artifact_file = self._data_dir("staging") / f"{staging_code}.jsonl"
        artifact = read_artifact("staging", staging_code)
        if artifact.state == "partial":
            return {"error": "暂存产物尚未完成提交确认，不能确认落地",
                    "staging_code": staging_code, "records_on_disk": len(artifact.records),
                    "complete": False}

        entry, msg = self.run_action(staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        # 验证落地产物文件记录数（缺文件/空文件旧行为：空记录、空校验）
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": staging_code,
              "record_count": len(records), "checksum": checksum,
              "confirmed_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已落地", "record_count": len(records),
                "checksum": checksum}

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，删除落地产物文件并写入清空日志。"""
        entry, msg = self.run_action(staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        # 标记清空断点
        self._write_checkpoint(f"staging_{staging_code}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        self._write_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}]
        )
        return {"staging_code": staging_code, "status": "已清空"}
