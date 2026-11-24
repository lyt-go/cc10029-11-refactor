"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物文件后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地和校验核对，形成窗口抽取→批次传输→落地校验的贯通链路。

JSONL 产物的读取、分片写入与提交确认统一走
:mod:`app.services.jsonl_artifact` 的共用约定；本服务只保留批次交付数据
组装、调用返回结构和传输侧自身的提交语义。
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.services.jsonl_artifact import (
    ArtifactWriteInterrupted,
    read_artifact,
    write_artifact,
)


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001") -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = f"TRAN-{window_code}-{int(time.time())}"
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        # 写入批次创建事件
        self._write_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code, "at": time.time()}]
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code}

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物文件，按分片大小写入传输产物文件并推进水位。

        本方法执行实际的批量数据传输：从抽取产物文件读取数据，切分为分片，
        写入传输产物文件（jsonl），计算字节总量，然后自动触发暂存落地和校验核对。

        恢复约定（同一批次可重复进入本方法）：
        - 上游产物缺文件：沿用旧兜底生成模拟数据；存在但空文件：读到零条；
        - 传输产物已提交：幂等复用，不再重复触发下游；
        - 传输产物 partial（如首片落盘后后端写入中断）：只追加缺失分片，
          完成本侧数据确认与提交后才放行下游。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        # 读取上游抽取窗口的产物文件（共用读取约定，区分缺文件/空文件）
        source_state = "missing"
        records: list[dict[str, Any]] = []
        if window_code != "unknown":
            source = read_artifact("extracted", window_code)
            source_state = source.state
            if source.exists:
                # 存在（含空文件）：以产物为准，保留空文件读到零条的旧行为
                records = [dict(r) for r in source.records]
        if source_state == "missing":
            # 缺文件：沿用旧兜底生成模拟数据
            records = self._simulate_records(
                batch_code, count=int(batch_entry.get("记录条数", 10)), field_prefix="trans"
            )

        # 保留原批次交付数据字段：批次编号、分片序号、传输时间戳
        chunk_index = 0
        for i, rec in enumerate(records):
            if i > 0 and i % chunk_size == 0:
                chunk_index += 1
            rec["batch_code"] = batch_code
            rec["chunk_index"] = chunk_index
            rec["transferred_at"] = time.time()

        output_file = self._data_dir("batches") / f"{batch_code}.jsonl"

        # 分片写入 + 本侧数据确认 + 提交清单；中断时直接向上抛出，
        # 不更新水位、不写断点、不触发任何下游。
        write_result = write_artifact(
            output_file,
            records,
            shard_size=chunk_size,
            allow_empty_file=False,  # 传输侧旧行为：零条数据不建产物文件
            extra_manifest={"batch_code": batch_code, "window_code": window_code},
        )

        total_written = write_result["record_count"]
        total_bytes = write_result["total_bytes"]
        shard_count = write_result["manifest"]["shard_count"] if write_result["manifest"] else 0

        # 更新批次水位和字节总量（与旧落盘值一致）
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        # 写入传输水位断点
        self._write_checkpoint(f"batch_{batch_code}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": shard_count,
            "watermark": str(total_written),
            "last_transfer_at": time.time(),
        })

        result: dict[str, Any] = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": shard_count,
            "artifact_path": str(output_file),
        }
        if write_result["resumed"]:
            result["resumed_from"] = {
                "records_on_disk": total_written - write_result["records_written"],
                "shards_flushed_this_run": write_result["shards_flushed"],
            }
        if write_result["reused"]:
            # 已完整提交的批次再次执行：幂等返回，下游不重复触发
            result["already_committed"] = True
            result["auto_staging"] = {"skipped": "batch_already_committed"}
            result["auto_verify"] = {"skipped": "batch_already_committed"}
            result["auto_reconcile"] = {"skipped": "batch_already_committed"}
            return result

        # 本侧完成数据确认并提交后，才允许下游继续
        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except ArtifactWriteInterrupted as e:
            # 暂存侧写入中断：只在它自己完成确认后才应继续，此处不放行后续环节
            result["auto_staging"] = {"error": str(e), "interrupted": True,
                                      "flushed_records": e.flushed_records}
            result["auto_verify"] = {"skipped": "staging_not_committed"}
            result["auto_reconcile"] = {"skipped": "staging_not_committed"}
            return result
        except Exception as e:
            result["auto_staging"] = {"error": str(e)}
            result["auto_verify"] = {"skipped": "staging_not_committed"}
            result["auto_reconcile"] = {"skipped": "staging_not_committed"}
            return result

        # 暂存落地未完整提交时，下游校验/对账不得继续
        staging_commit = read_artifact("staging", Path(result["auto_staging"]["artifact_path"]).stem)
        if not staging_commit.committed:
            result["auto_verify"] = {"skipped": "staging_not_committed"}
            result["auto_reconcile"] = {"skipped": "staging_not_committed"}
            return result

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            verify_result = cv_svc.create_verification(batch_code, total_written)
            result["auto_verify"] = verify_result
        except Exception as e:
            result["auto_verify"] = {"error": str(e)}

        # 传输完成后自动创建到达对账单
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            reconcile_result = ar_svc.create_reconciliation(batch_code, total_written)
            result["auto_reconcile"] = reconcile_result
        except Exception as e:
            result["auto_reconcile"] = {"error": str(e)}

        return result

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。

        批次产物未完整提交（partial，例如写入中断）时拒绝完成确认，
        状态不流转、不写完成日志；缺文件/空文件沿用旧行为（读到零条）。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在或已归档"}
        batch_code = batch_entry["批次编号"]
        artifact_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        artifact = read_artifact("batches", batch_code)
        if artifact.state == "partial":
            return {"error": "批次产物尚未完成提交确认，不能标记完成", "batch_code": batch_code,
                    "records_on_disk": len(artifact.records), "complete": False}

        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        # 从产物文件验证批次完整性（缺文件/空文件旧行为：空记录、空校验）
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        self._write_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        self._write_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}]
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}
