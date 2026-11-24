"""传输/暂存 JSONL 产物读写与提交确认的重构验证。

覆盖任务要求的全部对照场景：
1. 两次恢复入口：读到存在但无记录的文件（empty）、收到缺文件结果（missing）；
2. 写完首片未交付时后端写入中断（partial），中断侧完成自身数据确认前
   下游（暂存/对账/校验）收不到完整批次信号；
3. 传输侧与暂存侧各自的空文件/缺文件旧返回与落盘值单独对照；
4. 再次执行的恢复范围：partial 只追加缺失分片，committed 幂等复用。
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.jsonl_artifact import (
    ArtifactWriteInterrupted,
    BatchNotCommitted,
    arm_write_fault,
    clear_all_write_faults,
    commit_path_for,
    read_artifact,
    require_committed,
)
from app.services.transfer_batch import TransferBatchService
from app.services.staging_area import StagingAreaService
from app.services.checksum_verify import ChecksumVerifyService
from app.services.arrival_reconcile import ArrivalReconcileService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    clear_all_write_faults()
    # 新测试使用的窗口/批次编号前缀，按前缀清理持久化产物，保证多轮运行隔离
    markers = (
        "EXTR-MISSING-ENTRY", "EXTR-EMPTY-ENTRY", "EXTR-INTERRUPT-1",
        "EXTR-RESUME-1", "EXTR-IDEMPOTENT-1", "EXTR-STAGING-GATE-1",
        "EXTR-STAGING-INTERRUPT-1", "EXTR-STAGING-IDEM-1",
        "EXTR-GATE-CHAIN-1", "EXTR-GATE-INTERRUPT-2", "EXTR-MANIFEST-1",
        "EXTR-LEGACY-1",
        "TRAN-MISSING-FOR-STAGING-1", "TRAN-EMPTY-FOR-STAGING-1",
        "EMPTY-BATCH-XYZ", "NOT-EXIST-BATCH-XYZ",
    )

    def _purge():
        for sub in ("batches", "staging", "extracted", "checkpoints", "events"):
            directory = DATA_ROOT / sub
            if not directory.exists():
                continue
            for path in directory.glob("*"):
                if any(marker in path.name for marker in markers):
                    path.unlink(missing_ok=True)

    _purge()
    yield
    _purge()
    clear_all_write_faults()


def _batch_file(batch_code: str) -> Path:
    return DATA_ROOT / "batches" / f"{batch_code}.jsonl"


def _staging_files(batch_code: str) -> list[Path]:
    return sorted((DATA_ROOT / "staging").glob(f"STAG-{batch_code}-*.jsonl"))


def _staging_commit(batch_code: str) -> Path | None:
    files = _staging_files(batch_code)
    if not files:
        return None
    return commit_path_for(files[0])


class TestTransferEmptyAndMissingReads:
    """传输侧：缺文件与空文件两种恢复入口的旧返回/落盘值对照。"""

    def test_missing_upstream_falls_back_to_simulated_records(self):
        """恢复入口①：上游产物缺文件 -> 旧兜底模拟数据，批次正常提交。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-MISSING-ENTRY", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        target = _batch_file(batch_code)

        # 上游 extracted 产物确实不存在
        assert not (DATA_ROOT / "extracted" / "EXTR-MISSING-ENTRY.jsonl").exists()

        result = tb.start_transfer(batch["batch_id"], chunk_size=3)
        assert "error" not in result
        # 旧返回不变：10 条、4 个分片、产物路径
        assert result["records_transferred"] == 10
        assert result["chunks"] == 4
        assert result["artifact_path"] == str(target)

        # 落盘值：产物 + 提交清单（完整批次信号）
        assert target.exists()
        assert commit_path_for(target).exists()
        read = read_artifact("batches", batch_code)
        assert read.state == "committed"
        assert len(read.records) == 10
        assert read.manifest["record_count"] == 10

    def test_empty_upstream_file_reads_zero_records(self):
        """恢复入口②：上游产物存在但零条记录 -> 读到零条，旧空文件行为保留。"""
        tb = TransferBatchService()
        extracted = DATA_ROOT / "extracted"
        extracted.mkdir(parents=True, exist_ok=True)
        empty_window = "EXTR-EMPTY-ENTRY"
        empty_file = extracted / f"{empty_window}.jsonl"
        empty_file.touch()  # 存在但没有任何记录
        try:
            batch = tb.create_batch(empty_window, 10, "TARG-0001")
            batch_code = batch["batch_code"]
            result = tb.start_transfer(batch["batch_id"], chunk_size=3)

            assert "error" not in result
            # 空文件旧返回：零条传输、零字节、零分片
            assert result["records_transferred"] == 0
            assert result["total_bytes"] == 0
            assert result["chunks"] == 0
            # 传输侧旧落盘值：零条数据不建产物文件，也没有提交清单
            target = _batch_file(batch_code)
            assert not target.exists()
            assert not commit_path_for(target).exists()
            assert read_artifact("batches", batch_code).state == "missing"
        finally:
            empty_file.unlink(missing_ok=True)

    def test_missing_and_empty_both_return_empty_record_list(self):
        """缺文件与空文件的读取返回都是空列表，仅 state 不同，旧调用方无感。"""
        missing = read_artifact("batches", "NOT-EXIST-BATCH-XYZ")
        assert missing.state == "missing"
        assert missing.records == []
        assert missing.committed is False

        empty_path = DATA_ROOT / "batches" / "EMPTY-BATCH-XYZ.jsonl"
        empty_path.parent.mkdir(parents=True, exist_ok=True)
        empty_path.touch()
        try:
            empty = read_artifact("batches", "EMPTY-BATCH-XYZ")
            assert empty.state == "empty"
            assert empty.records == []  # 与缺文件相同的旧返回
            assert empty.committed is False
        finally:
            empty_path.unlink(missing_ok=True)


class TestTransferWriteInterruptionAndResume:
    """传输侧：首片落盘后写入中断、恢复范围与下游门控。"""

    def test_interrupt_after_first_shard_blocks_downstream(self):
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-INTERRUPT-1", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        target = _batch_file(batch_code)
        arm_write_fault(target, fail_after_shards=1)

        with pytest.raises(ArtifactWriteInterrupted) as exc:
            tb.start_transfer(batch["batch_id"], chunk_size=3)
        # 首片 3 条已落盘
        assert exc.value.flushed_records == 3
        assert exc.value.flushed_shards == 1

        # 落盘值：partial 产物存在，提交清单不存在
        assert target.exists()
        assert not commit_path_for(target).exists()
        read = read_artifact("batches", batch_code)
        assert read.state == "partial"
        assert len(read.records) == 3

        # 暂存与对账没有收到完整批次信号
        assert _staging_files(batch_code) == []
        with pytest.raises(BatchNotCommitted):
            require_committed("batches", batch_code)

        # 校验与对账在 partial 上拒绝出结论：状态不流转、不写结论日志
        cv = ChecksumVerifyService()
        verify = cv.create_verification(batch_code, 10)
        verify_result = cv.execute_verification(verify["verify_id"])
        assert verify_result["complete"] is False
        assert "source_checksum" not in verify_result
        assert cv.get_entry(verify["verify_id"])["status"] == "草稿"

        ar = ArrivalReconcileService()
        recon = ar.create_reconciliation(batch_code, 10)
        recon_result = ar.execute_reconciliation(recon["recon_id"])
        assert recon_result["complete"] is False
        assert "balanced" not in recon_result
        assert ar.get_entry(recon["recon_id"])["status"] == "草稿"

        # 批次完成确认同样被门控
        completed = tb.mark_completed(batch["batch_id"])
        assert completed["complete"] is False
        assert tb.get_entry(batch["batch_id"])["status"] != "已完成"

    def test_resume_only_appends_missing_shards(self):
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-RESUME-1", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        target = _batch_file(batch_code)
        arm_write_fault(target, fail_after_shards=1)

        with pytest.raises(ArtifactWriteInterrupted):
            tb.start_transfer(batch["batch_id"], chunk_size=3)
        partial_lines = target.read_text(encoding="utf-8").strip().splitlines()
        assert len(partial_lines) == 3

        # 再次执行：恢复范围仅限缺失分片（故障已一次性消费）
        result = tb.start_transfer(batch["batch_id"], chunk_size=3)
        assert "error" not in result
        assert result["records_transferred"] == 10
        assert result["resumed_from"]["records_on_disk"] == 3
        assert result["resumed_from"]["shards_flushed_this_run"] == 3  # 4 片 - 已落 1 片

        # 同一产物文件，无重复数据，提交清单生成
        final_lines = target.read_text(encoding="utf-8").strip().splitlines()
        assert len(final_lines) == 10
        read = read_artifact("batches", batch_code)
        assert read.state == "committed"
        assert read.manifest["shard_count"] == 4

        # 恢复完成后下游才被驱动
        assert "auto_staging" in result and "error" not in result["auto_staging"]
        assert "auto_verify" in result and "error" not in result["auto_verify"]
        assert "auto_reconcile" in result and "error" not in result["auto_reconcile"]
        assert _staging_commit(batch_code) is not None

    def test_re_run_committed_batch_is_idempotent(self):
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-IDEMPOTENT-1", 6, "TARG-0001")
        batch_code = batch["batch_code"]
        target = _batch_file(batch_code)

        first = tb.start_transfer(batch["batch_id"], chunk_size=3)
        assert first["records_transferred"] == 6
        mtime_after_first = target.stat().st_mtime_ns

        second = tb.start_transfer(batch["batch_id"], chunk_size=3)
        assert second["already_committed"] is True
        assert second["records_transferred"] == 6
        # 产物未被重写，下游不重复触发
        assert target.stat().st_mtime_ns == mtime_after_first
        assert second["auto_verify"] == {"skipped": "batch_already_committed"}
        # 暂存侧同样只有一个产物
        assert len(_staging_files(batch_code)) == 1


class TestStagingEmptyAndMissingReads:
    """暂存侧：缺文件与空文件两种输入的旧返回/落盘值单独对照。"""

    def test_missing_batch_file_uses_simulated_records(self):
        """缺文件入口：沿用旧兜底模拟数据，暂存正常生成文件、条数与交付结果。"""
        sa = StagingAreaService()
        batch_code = "TRAN-MISSING-FOR-STAGING-1"
        assert not _batch_file(batch_code).exists()

        result = sa.land_batch(batch_code, 10)
        assert result["records_landed"] == 10
        artifact = Path(result["artifact_path"])
        assert artifact.exists()
        # 暂存侧提交清单存在，条数与交付结果齐全
        assert commit_path_for(artifact).exists()
        read = read_artifact("staging", artifact.stem)
        assert read.state == "committed"
        assert read.manifest["record_count"] == 10

        # 新建或复用既有分区，水位都与交付条数一致
        entry = next((r for r in store.rows("staging_area")
                      if r["暂存编号"] == result["staging_code"]), None)
        if entry is not None:
            assert entry["落地水位"] == "10"
            assert int(entry["占用字节"]) > 0

    def test_empty_batch_file_lands_zero_records(self):
        """空文件入口：读到零条，暂存侧旧行为是建立空产物文件。"""
        sa = StagingAreaService()
        batch_code = "TRAN-EMPTY-FOR-STAGING-1"
        empty_batch = _batch_file(batch_code)
        empty_batch.parent.mkdir(parents=True, exist_ok=True)
        empty_batch.touch()
        # 清掉历史运行可能留下的暂存产物，保证走新建分支
        for old in (DATA_ROOT / "staging").glob(f"STAG-{batch_code}-*"):
            old.unlink()
        try:
            result = sa.land_batch(batch_code, 0)
            artifact = Path(result["artifact_path"])
            # 与传输侧不同：暂存侧空数据也落盘一个空文件
            assert artifact.exists()
            assert artifact.read_text(encoding="utf-8") == ""
            assert result["records_landed"] == 0
            assert result["total_bytes"] == 0
            read = read_artifact("staging", artifact.stem)
            # 空文件没有记录可确认，状态保持 empty（无提交清单）
            assert read.state == "empty"
            assert read.records == []
        finally:
            empty_batch.unlink(missing_ok=True)

    def test_partial_batch_is_rejected_by_staging(self):
        """暂存收到 partial 批次（无完整批次信号）必须拒绝落地。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-STAGING-GATE-1", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        arm_write_fault(_batch_file(batch_code), fail_after_shards=1)
        with pytest.raises(ArtifactWriteInterrupted):
            tb.start_transfer(batch["batch_id"], chunk_size=3)

        sa = StagingAreaService()
        with pytest.raises(BatchNotCommitted):
            sa.land_batch(batch_code, 10)
        # 没有产生任何暂存产物或分区条目
        assert _staging_files(batch_code) == []
        assert all(batch_code not in str(r.get("批次编号", ""))
                   for r in store.rows("staging_area"))


class TestStagingWriteInterruptionAndResume:
    """暂存侧：首片落盘后中断、恢复范围与确认门控。"""

    @staticmethod
    def _arm_fault_on_staging_write(monkeypatch, batch_code: str):
        """在暂存侧 write_artifact 首次写入该批次产物时注入首片后中断。"""
        import app.services.staging_area as sa_mod

        real_write = sa_mod.write_artifact

        def faulting_write(filepath, records, **kwargs):
            if batch_code in filepath.name:
                arm_write_fault(filepath, fail_after_shards=1)
            return real_write(filepath, records, **kwargs)

        monkeypatch.setattr(sa_mod, "write_artifact", faulting_write)

    def test_interrupt_then_resume_reuses_same_partition(self, monkeypatch):
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-STAGING-INTERRUPT-1", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        tb.start_transfer(batch["batch_id"], chunk_size=5)
        # 传输侧已自动落地过一次，清掉暂存产物以重演一次干净的中断
        for f in _staging_files(batch_code):
            f.unlink()
            commit_path_for(f).unlink(missing_ok=True)
        for row in [r for r in store.rows("staging_area") if r.get("批次编号") == batch_code]:
            store.rows("staging_area").remove(row)

        self._arm_fault_on_staging_write(monkeypatch, batch_code)
        sa = StagingAreaService()

        # 首片（3 条）落盘后中断
        with pytest.raises(ArtifactWriteInterrupted) as exc:
            sa.land_batch(batch_code, 10)
        assert exc.value.flushed_records == 3
        staging_files = _staging_files(batch_code)
        assert len(staging_files) == 1
        staging_file = staging_files[0]
        staging_code = staging_file.stem
        read = read_artifact("staging", staging_code)
        assert read.state == "partial"
        assert len(read.records) == 3

        # 确认落地被门控：状态不流转、不写确认日志
        partial_entry = next(r for r in store.rows("staging_area")
                             if r["暂存编号"] == staging_code)
        blocked = sa.confirm_land(partial_entry["id"])
        assert blocked["complete"] is False
        assert sa.get_entry(partial_entry["id"])["status"] == "草稿"

        # 再次执行：复用同一暂存编号/分区，只追加缺失分片
        monkeypatch.undo()
        resumed = sa.land_batch(batch_code, 10)
        assert resumed["staging_code"] == staging_code
        assert resumed["resumed"] is True
        assert resumed["records_landed"] == 10
        assert len(_staging_files(batch_code)) == 1  # 没有新建第二个分区
        final_read = read_artifact("staging", staging_code)
        assert final_read.state == "committed"
        assert len(final_read.records) == 10
        # 同一文件内无重复数据
        assert len(staging_file.read_text(encoding="utf-8").strip().splitlines()) == 10

        # 完成自身确认后确认动作才放行
        confirmed = sa.confirm_land(partial_entry["id"])
        assert confirmed["status"] == "已落地"
        assert confirmed["record_count"] == 10

    def test_committed_staging_landing_is_idempotent(self):
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-STAGING-IDEM-1", 6, "TARG-0001")
        batch_code = batch["batch_code"]
        tb.start_transfer(batch["batch_id"], chunk_size=3)

        sa = StagingAreaService()
        first = sa.land_batch(batch_code, 6)
        second = sa.land_batch(batch_code, 6)
        assert second["already_committed"] is True
        assert second["staging_code"] == first["staging_code"]
        assert len(_staging_files(batch_code)) == 1


class TestCrossSideCommitContract:
    """两侧共用产物读写约定：提交清单是完整批次的唯一信号。"""

    def test_transfer_gates_verify_and_reconcile_until_staging_committed(self):
        """传输侧只有在暂存侧也完成自己的数据确认后才放行校验/对账。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-GATE-CHAIN-1", 10, "TARG-0001")
        batch_code = batch["batch_code"]

        result = tb.start_transfer(batch["batch_id"], chunk_size=4)
        assert "error" not in result
        # 两侧提交清单齐备
        assert commit_path_for(_batch_file(batch_code)).exists()
        assert _staging_commit(batch_code) is not None
        # 下游被正常驱动
        assert "verify_id" in result["auto_verify"]
        assert "recon_id" in result["auto_reconcile"]

    def test_staging_interruption_skips_downstream_in_transfer_result(self, monkeypatch):
        """暂存写入中断时，传输结果中校验/对账显式跳过而非静默执行。"""
        import app.services.staging_area as sa_mod

        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-GATE-INTERRUPT-2", 10, "TARG-0001")
        batch_code = batch["batch_code"]
        real_write = sa_mod.write_artifact

        def faulting_write(filepath, records, **kwargs):
            if filepath.parent.name == "staging" and batch_code in filepath.name:
                arm_write_fault(filepath, fail_after_shards=1)
            return real_write(filepath, records, **kwargs)

        monkeypatch.setattr(sa_mod, "write_artifact", faulting_write)

        result = tb.start_transfer(batch["batch_id"], chunk_size=5)
        # 传输侧自身已提交
        assert read_artifact("batches", batch_code).state == "committed"
        # 暂存侧中断信息被捕获，且校验/对账被门控跳过
        assert result["auto_staging"].get("interrupted") is True
        assert result["auto_staging"]["flushed_records"] == 3
        assert result["auto_verify"] == {"skipped": "staging_not_committed"}
        assert result["auto_reconcile"] == {"skipped": "staging_not_committed"}
        # 暂存产物处于 partial，没有提交清单
        staging_files = _staging_files(batch_code)
        assert len(staging_files) == 1
        assert not commit_path_for(staging_files[0]).exists()

        # 再次执行：传输侧幂等返回不重跑；直接走暂存恢复入口完成落地
        monkeypatch.undo()
        sa = StagingAreaService()
        resumed = sa.land_batch(batch_code, 10)
        assert resumed["records_landed"] == 10
        assert read_artifact("staging", staging_files[0].stem).state == "committed"

    def test_commit_manifest_records_match_artifact(self):
        """提交清单记录的条数、分片数、校验值与落盘产物严格一致。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-MANIFEST-1", 7, "TARG-0001")
        batch_code = batch["batch_code"]
        tb.start_transfer(batch["batch_id"], chunk_size=3)

        read = read_artifact("batches", batch_code)
        assert read.committed is True
        manifest = read.manifest
        assert manifest["record_count"] == 7
        assert manifest["shard_count"] == 3  # 3+3+1
        assert manifest["shard_size"] == 3
        assert manifest["checksum"] == tb._compute_checksum(str(read.records))
        assert manifest["total_bytes"] == sum(
            len(str(r).encode("utf-8")) for r in read.records
        )

    def test_manifest_values_match_legacy_formulas(self):
        """落盘计数、字节总量、校验值与重构前的旧公式逐项等价。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-LEGACY-1", 12, "TARG-0001")
        batch_code = batch["batch_code"]
        result = tb.start_transfer(batch["batch_id"], chunk_size=4)

        # 旧公式：字节总量 = 每条 str(record).encode 长度之和
        records = tb._read_jsonl(_batch_file(batch_code))
        legacy_bytes = sum(len(str(r).encode("utf-8")) for r in records)
        legacy_checksum = tb._compute_checksum(str(records))
        assert result["total_bytes"] == legacy_bytes
        assert result["records_transferred"] == len(records) == 12

        manifest = read_artifact("batches", batch_code).manifest
        assert manifest["total_bytes"] == legacy_bytes
        assert manifest["checksum"] == legacy_checksum

        # 暂存侧同样沿用旧公式
        sa = StagingAreaService()
        staging = sa.land_batch(batch_code, 12)
        staging_records = sa._read_jsonl(Path(staging["artifact_path"]))
        assert staging["total_bytes"] == sum(
            len(str(r).encode("utf-8")) for r in staging_records
        )
        assert staging["records_landed"] == len(staging_records) == 12
