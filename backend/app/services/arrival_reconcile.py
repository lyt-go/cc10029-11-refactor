"""到达对账领域服务：维护批次应到与实到记录数的对账、迟到与缺失条数统计及结论。

本模块读取传输批次产物文件和暂存落地产物文件进行数量对账，统计迟到与缺失条数，
对账完成后自动驱动时效监控指标更新和传输血缘记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.jsonl_artifact import read_artifact
from app.store import store


class ArrivalReconcileService(BaseModuleService):
    MODULE = "arrival_reconcile"
    ENTITY = "对账单"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["对账编号", "批次编号", "应到条数", "实到条数", "迟到条数", "缺失条数", "对账窗口", "对账结论"]
    REQUIRED_FIELDS = ["对账编号", "批次编号", "应到条数"]
    STATUS_ORDER = ["草稿", "待对账", "对账中", "已平账", "存在差异"]
    ACTION_RULES = {"启动对账": "对账中", "确认平账": "已平账", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = ["标记差异"]

    def create_reconciliation(self, batch_code: str, expected_count: int) -> dict[str, Any]:
        """为传输批次创建到达对账单，登记应到条数，为后续到账核对准备基线。"""
        recon_code = f"ARRI-{batch_code}-{int(time.time())}"
        values = {
            "对账编号": recon_code,
            "批次编号": batch_code,
            "应到条数": str(expected_count),
            "实到条数": "0",
            "迟到条数": "0",
            "缺失条数": "0",
            "对账窗口": "daily",
            "对账结论": "待对账",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建对账单失败: {missing}")
        return {"recon_code": recon_code, "recon_id": entry["id"], "batch_code": batch_code,
                "expected_count": expected_count}

    def execute_reconciliation(self, recon_id: int) -> dict[str, Any]:
        """执行到达对账：核实传输批次应到与实到记录数的一致性，统计迟到与缺失条数。

        本方法从传输批次产物文件获取应到条数，从暂存落地产物文件获取实到条数，
        计算迟到（超时到达）和缺失（未到达）条数，判定对账结论。
        """
        recon_entry = self.get_entry(recon_id)
        if recon_entry is None:
            return {"error": f"对账单 {recon_id} 不存在"}

        batch_code = recon_entry["批次编号"]
        expected = int(recon_entry.get("应到条数", 0))

        # 门控前置：未完整提交的批次不参与对账，状态不流转、不写结论日志
        batch_artifact = read_artifact("batches", batch_code)
        if batch_artifact.state == "partial":
            return {"recon_code": recon_entry["对账编号"], "batch_code": batch_code,
                    "complete": False,
                    "error": "批次产物尚未完成提交确认，到达对账等待完整批次信号",
                    "records_on_disk": len(batch_artifact.records)}

        entry, msg = self.run_action(recon_id, "启动对账")
        if entry is None:
            return {"error": msg, "recon_id": recon_id}

        # 缺文件/空文件沿用旧行为：读到空记录，应到/实到差异按旧公式计算
        batch_records = batch_artifact.records
        source_count = len(batch_records)

        # 从暂存产品目录查找目标落地产物文件
        staging_dir = self._data_dir("staging")
        staging_count = 0
        late_count = 0
        staging_prefix = f"STAG-{batch_code}-"
        staging_matches = sorted(
            p for p in staging_dir.glob("STAG-*.jsonl") if p.name.startswith(staging_prefix)
        )
        if staging_matches:
            s_records = self._read_jsonl(staging_matches[0])
            staging_count = len(s_records)
            # 计算迟到条数：目标端实际记录数少于源端基准
            if staging_count < expected:
                late_count = expected - staging_count

        missing_count = max(0, expected - staging_count)
        is_balanced = expected == staging_count

        entry["实到条数"] = str(staging_count)
        entry["迟到条数"] = str(late_count)
        entry["缺失条数"] = str(missing_count)

        result_action = "确认平账" if is_balanced else "标记差异"
        final_entry, _ = self.run_action(recon_id, result_action)

        # 写入对账结论日志
        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "balanced": is_balanced, "reconciled_at": time.time()}]
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
        }

        # 对账结果驱动时效监控指标更新
        if late_count > 0 or not is_balanced:
            try:
                sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
                sm_result = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
                result["auto_sla_update"] = sm_result
            except Exception as e:
                result["auto_sla_update"] = {"error": str(e)}

        # 对账完成后记录传输血缘
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            lineage_result = tl_svc.record_lineage(batch_code, is_balanced)
            result["auto_lineage"] = lineage_result
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}

        return result