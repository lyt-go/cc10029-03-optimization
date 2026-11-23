"""开窗状态流转与失败恢复专项测试。

覆盖场景：
1. 确认登记读取登记配置生成可抽取窗口清单与调度批次，只物化首窗；
2. 首窗准备期间、首窗生成之后两个失败点的登记中断与恢复；
3. 停用请求分别在两个时点撞上登记/恢复的冲突处理与恢复后生效；
4. 恢复从调度记录回溯失败前的登记配置，重跑不多开一窗；
5. 调度可开始哪一窗、该窗实际增量字段与范围、契约门禁与批次推进。
"""
from __future__ import annotations

import pytest

from app.seed import SEED_ROWS
from app.store import store
from app.services.source_register import (
    SourceRegisterService, WindowPreparationError, SCHEDULE_MODULE,
)
from app.services.extract_window import (
    ExtractWindowService, parse_window_range, split_ranges, window_code,
)
from app.services.field_contract import FieldContractService


def _base_values(code: str, **overrides) -> dict:
    values = {
        "源端编号": code,
        "系统名称": f"系统{code}",
        "数据域": "订单域",
        "记录格式": "JSON",
        "编码方式": "UTF-8",
        "日增量级": "10万",
        "抽取方式": "增量抽取",
        "登记游标": "0",
        "增量字段": "updated_at",
        "开窗范围": "今日",
        "窗口份数": "3",
    }
    values.update(overrides)
    return values


@pytest.fixture(autouse=True)
def reset_store():
    """每个用例前还原种子数据并清空调度记录表，保证隔离。"""
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    store._tables[SCHEDULE_MODULE] = []
    yield


def _windows_of(source_code: str) -> list[dict]:
    return [r for r in store.rows("extract_window") if r.get("源端编号") == source_code]


# ----------------------------------------------------------------------
# 一、范围解析与清单/调度批次生成
# ----------------------------------------------------------------------

class TestManifestAndScheduleBuild:

    def test_range_parsing_supports_keywords_and_explicit_intervals(self):
        start, end = parse_window_range("昨日")
        assert end > start
        start2, end2 = parse_window_range("2026-11-01~2026-11-03")
        assert start2.strftime("%Y-%m-%d %H:%M:%S") == "2026-11-01 00:00:00"
        assert end2.strftime("%Y-%m-%d %H:%M:%S") == "2026-11-03 23:59:59"
        parts = split_ranges(start2, end2, 3)
        assert len(parts) == 3
        # 子范围首尾相接、不重不漏
        assert parts[0][0] == start2.strftime("%Y-%m-%dT%H:%M:%S")
        assert parts[-1][1] == end2.strftime("%Y-%m-%dT%H:%M:%S")
        with pytest.raises(ValueError):
            parse_window_range("不合法范围")

    def test_confirm_builds_manifest_and_materializes_only_first_window(self):
        svc = SourceRegisterService()
        entry = svc.register_source(
            _base_values("SOUR-MANIFEST", 增量字段="etl_time",
                         开窗范围="2026-11-01~2026-11-03", 窗口份数="3"))
        result = svc.confirm_register(entry["id"])
        assert "error" not in result

        windows = _windows_of("SOUR-MANIFEST")
        assert len(windows) == 1  # 只物化首窗，后续批次留给调度
        first = windows[0]
        assert first["窗口编号"] == window_code("SOUR-MANIFEST", 1)
        assert first["status"] == "待抽取"
        assert first["增量字段"] == "etl_time"  # 实际增量字段来自登记配置
        assert "FIEL-SOUR-MANIFEST" in first["许可来源"]
        assert "SCHE-SOUR-MANIFEST-1" in first["许可来源"]

        manifest = result["window_manifest"]
        assert [m["窗口序号"] for m in manifest] == [1, 2, 3]
        assert [m["调度批次"] for m in manifest] == [1, 2, 3]
        assert manifest[0]["状态"] == "待抽取"
        assert manifest[1]["状态"] == "未安排"  # 后续窗口尚未物化
        # 每个窗口都能说明范围与许可来源
        for item in manifest:
            assert item["窗口起点"] < item["窗口终点"]
            assert "许可来源" in item and item["许可来源"]

    def test_confirm_requires_complete_range_and_contract(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-BADRANGE", 开窗范围="坏掉的范围"))
        result = svc.confirm_register(entry["id"])
        assert "error" in result
        assert entry_id_status(entry["id"]) == "登记中断"
        # 范围不齐时不得开窗
        assert _windows_of("SOUR-BADRANGE") == []


def entry_id_status(entry_id: int) -> str:
    row = store.find("source_register", entry_id)
    return row["status"] if row else ""


# ----------------------------------------------------------------------
# 二、首窗准备期间失败 → 登记中断 → 恢复
# ----------------------------------------------------------------------

class TestFailDuringFirstWindowPreparation:

    def test_preparation_failure_leaves_no_window_and_interrupts(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-FAIL1"))
        result = svc.confirm_register(entry["id"], fail_at="首窗准备")
        assert "error" in result
        assert result["interrupted_stage"] == "首窗准备"
        assert entry_id_status(entry["id"]) == "登记中断"
        # 失败发生在首窗物化之前：一个窗口都不能留下
        assert _windows_of("SOUR-FAIL1") == []

        schedule = svc.get_schedule_record("SOUR-FAIL1")
        assert schedule["状态"] == "已中断"
        assert schedule["失败阶段"] == "首窗准备"
        # 失败前的登记配置已随调度记录保存
        assert schedule["登记配置"]["源端编号"] == "SOUR-FAIL1"
        assert schedule["登记配置"]["增量字段"] == "updated_at"

    def test_resume_restores_config_and_creates_first_window_once(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-FAIL1"))
        svc.confirm_register(entry["id"], fail_at="首窗准备")

        # 现场篡改登记配置，恢复必须回到失败之前的快照
        entry["增量字段"] = "tampered_field"
        entry["开窗范围"] = "昨日"

        resume = svc.resume_register(entry["id"])
        assert "error" not in resume
        assert entry_id_status(entry["id"]) == "已登记"
        assert resume["restored_config"]["增量字段"] == "updated_at"
        assert entry["增量字段"] == "updated_at"  # 配置已回溯
        assert entry["开窗范围"] == "今日"

        windows = _windows_of("SOUR-FAIL1")
        assert len(windows) == 1
        assert windows[0]["窗口编号"] == window_code("SOUR-FAIL1", 1)
        assert resume["window_created"] == 1 and resume["window_reused"] == 0

        # 重跑恢复入口：窗口已存在，必须复用而不是多开一窗
        again = svc.resume_register(entry["id"])
        # 已恢复后没有中断可续做，调用被拒绝且窗口数不变
        assert "error" in again
        assert len(_windows_of("SOUR-FAIL1")) == 1
        manifest = svc.live_manifest("SOUR-FAIL1")
        assert len(manifest) == 3
        assert manifest[0]["状态"] == "待抽取"

    def test_deactivate_during_preparation_is_rejected(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-LOCK1"))
        captured = {}

        def hit_deactivate():
            # 持有准备锁期间发起停用：必须被冲突拦截
            row, msg = svc.deactivate(entry["id"])
            captured["row"] = row
            captured["msg"] = msg

        result = svc.confirm_register(entry["id"], on_preparing=hit_deactivate)
        assert "error" not in result
        assert captured["row"] is None
        assert "冲突" in captured["msg"]
        assert entry_id_status(entry["id"]) == "已登记"

    def test_deactivate_before_resume_takes_effect_after_resume(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-DEFER1"))
        svc.confirm_register(entry["id"], fail_at="首窗准备")
        assert entry_id_status(entry["id"]) == "登记中断"

        # 停用请求碰上登记恢复（中断态先申请停用）
        row, msg = svc.deactivate(entry["id"])
        assert row["status"] == "停用待恢复"
        assert "挂起" in msg

        resume = svc.resume_register(entry["id"])
        assert "error" not in resume
        assert resume["deactivated_after_resume"] is True
        assert entry_id_status(entry["id"]) == "已停用"
        # 停用后窗口状态、范围、许可来源仍可说明
        windows = _windows_of("SOUR-DEFER1")
        assert len(windows) == 1
        assert windows[0]["许可来源"]
        manifest = svc.live_manifest("SOUR-DEFER1")
        assert manifest[0]["状态"] == "待抽取"
        # 停用态调度不能开窗
        dispatch = ExtractWindowService().get_schedule_dispatch("SOUR-DEFER1")
        assert dispatch["can_start"] is False
        assert "停用" in dispatch["reason"] or "已停用" in dispatch["reason"]

    def test_deactivate_hitting_resume_preparation_is_rejected(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-DEFER2"))
        svc.confirm_register(entry["id"], fail_at="首窗准备")
        captured = {}

        def hit_deactivate():
            row, msg = svc.deactivate(entry["id"])
            captured["row"], captured["msg"] = row, msg

        svc.resume_register(entry["id"], on_preparing=hit_deactivate)
        assert captured["row"] is None
        assert "冲突" in captured["msg"]
        assert entry_id_status(entry["id"]) == "已登记"


# ----------------------------------------------------------------------
# 三、首窗生成之后失败 → 首窗保留 → 恢复复用
# ----------------------------------------------------------------------

class TestFailAfterFirstWindowCreated:

    def test_failure_keeps_first_window_and_resume_reuses_it(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-FAIL2", 窗口份数="2"))
        result = svc.confirm_register(entry["id"], fail_at="首窗之后")
        assert "error" in result
        assert result["interrupted_stage"] == "首窗生成后"
        assert entry_id_status(entry["id"]) == "登记中断"

        windows = _windows_of("SOUR-FAIL2")
        assert len(windows) == 1  # 首窗已生成，必须保留
        first_code = windows[0]["窗口编号"]

        resume = svc.resume_register(entry["id"])
        assert "error" not in resume
        assert resume["window_reused"] == 1 and resume["window_created"] == 0
        # 复用同一窗口而不是新开一窗
        after = _windows_of("SOUR-FAIL2")
        assert len(after) == 1
        assert after[0]["窗口编号"] == first_code
        assert entry_id_status(entry["id"]) == "已登记"

    def test_deactivate_after_first_window_then_resume(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-FAIL3", 窗口份数="3"))
        svc.confirm_register(entry["id"], fail_at="首窗之后")
        row, _ = svc.deactivate(entry["id"])
        assert row["status"] == "停用待恢复"

        resume = svc.resume_register(entry["id"])
        assert resume["deactivated_after_resume"] is True
        assert entry_id_status(entry["id"]) == "已停用"
        # 恢复后重新启用，清单不重建、窗口不多开
        reactivated, _ = svc.reactivate(entry["id"])
        assert reactivated["status"] == "已登记"
        assert len(_windows_of("SOUR-FAIL3")) == 1
        schedule = svc.get_schedule_record("SOUR-FAIL3")
        assert len(schedule["窗口清单"]) == 3

    def test_repeated_resume_is_idempotent(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-IDEM"))
        svc.confirm_register(entry["id"], fail_at="首窗之后")
        svc.resume_register(entry["id"])
        # 已恢复后再次调用恢复应被拒绝，且窗口数不变
        again = svc.resume_register(entry["id"])
        assert "error" in again
        assert len(_windows_of("SOUR-IDEM")) == 1


# ----------------------------------------------------------------------
# 四、调度消费：能开始哪一窗、增量字段与范围、后续批次
# ----------------------------------------------------------------------

class TestScheduleDispatch:

    def test_dispatch_starts_with_first_window_using_real_field_and_range(self):
        svc = SourceRegisterService()
        entry = svc.register_source(
            _base_values("SOUR-DISP1", 增量字段="biz_time",
                         开窗范围="2026-11-01~2026-11-02", 窗口份数="2"))
        svc.confirm_register(entry["id"])
        ew = ExtractWindowService()

        dispatch = ew.get_schedule_dispatch("SOUR-DISP1")
        assert dispatch["can_start"] is True
        assert dispatch["窗口编号"] == window_code("SOUR-DISP1", 1)
        assert dispatch["增量字段"] == "biz_time"
        assert dispatch["窗口起点"].startswith("2026-11-01")
        assert "FIEL-SOUR-DISP1" in dispatch["许可来源"]
        assert dispatch["窗口清单"] == "SCHE-SOUR-DISP1-1"

        plan = ew.plan_next_batch("SOUR-DISP1")
        assert plan["current_batch"] == 1
        assert len(plan["upcoming_batches"]) == 1
        assert plan["upcoming_batches"][0]["调度批次"] == 2

    def test_dispatch_advances_to_next_batch_after_first_window_closes(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-DISP2", 窗口份数="3"))
        svc.confirm_register(entry["id"])
        ew = ExtractWindowService()

        first = ew.get_schedule_dispatch("SOUR-DISP2")
        ew.execute_extract(first["window_id"], record_count=5)
        ew.close_window(first["window_id"])

        second = ew.get_schedule_dispatch("SOUR-DISP2")
        assert second["can_start"] is True
        assert second["窗口编号"] == window_code("SOUR-DISP2", 2)
        assert second["调度批次"] == 2
        # 物化的窗口仍携带清单与许可来源，范围即清单切分结果
        assert second["许可来源"]
        assert second["窗口起点"] and second["窗口终点"]
        assert len(_windows_of("SOUR-DISP2")) == 2  # 按批次推进，没有预开全部窗

        plan = ew.plan_next_batch("SOUR-DISP2")
        assert plan["current_batch"] == 2
        assert [b["调度批次"] for b in plan["upcoming_batches"]] == [3]

    def test_dispatch_blocked_when_contract_not_active(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-GATE1"))
        svc.confirm_register(entry["id"])
        # 把契约改回未生效
        contract = next(r for r in store.rows("field_contract")
                        if r["契约编号"] == "FIEL-SOUR-GATE1")
        contract["status"] = "待校验"
        ew = ExtractWindowService()
        dispatch = ew.get_schedule_dispatch("SOUR-GATE1")
        assert dispatch["can_start"] is False
        assert "契约" in dispatch["reason"]
        # 抽取执行同样被门禁拦截
        window = _windows_of("SOUR-GATE1")[0]
        result = ew.execute_extract(window["id"])
        assert "error" in result and "契约" in result["error"]

    def test_dispatch_blocked_when_source_deactivated(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-GATE2"))
        svc.confirm_register(entry["id"])
        svc.deactivate(entry["id"])
        dispatch = ExtractWindowService().get_schedule_dispatch("SOUR-GATE2")
        assert dispatch["can_start"] is False
        assert dispatch["source_status"] == "已停用"

    def test_dispatch_reports_completion_after_all_windows_closed(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-DONE", 窗口份数="2"))
        svc.confirm_register(entry["id"])
        ew = ExtractWindowService()
        for _ in range(2):
            d = ew.get_schedule_dispatch("SOUR-DONE")
            assert d["can_start"]
            ew.execute_extract(d["window_id"], record_count=3)
            ew.close_window(d["window_id"])
        final = ew.get_schedule_dispatch("SOUR-DONE")
        assert final["can_start"] is False
        assert "全部" in final["reason"]
        # 全部窗口只开了清单声明的数量，重跑不多开
        assert len(_windows_of("SOUR-DONE")) == 2


# ----------------------------------------------------------------------
# 五、调度记录回溯与许可来源可说明
# ----------------------------------------------------------------------

class TestScheduleRecordTraceability:

    def test_schedule_record_holds_pre_failure_configuration(self):
        svc = SourceRegisterService()
        entry = svc.register_source(
            _base_values("SOUR-TRACE", 增量字段="sync_ts", 窗口份数="4"))
        svc.confirm_register(entry["id"], fail_at="首窗准备")
        record = svc.get_schedule_record("SOUR-TRACE")
        assert record["登记配置"]["增量字段"] == "sync_ts"
        assert len(record["窗口清单"]) == 4
        assert all(item["许可来源"].startswith("字段契约 FIEL-SOUR-TRACE")
                   for item in record["窗口清单"])

    def test_manifest_explains_status_range_license_after_deactivate_and_resume(self):
        svc = SourceRegisterService()
        entry = svc.register_source(_base_values("SOUR-TRACE2", 窗口份数="3"))
        svc.confirm_register(entry["id"], fail_at="首窗之后")
        svc.deactivate(entry["id"])
        svc.resume_register(entry["id"])
        assert entry_id_status(entry["id"]) == "已停用"

        manifest = svc.live_manifest("SOUR-TRACE2")
        assert len(manifest) == 3
        first = manifest[0]
        assert first["状态"] == "待抽取"
        assert first["已物化"] is True
        assert first["窗口起点"] and first["窗口终点"]
        assert "SCHE-SOUR-TRACE2-1" in first["许可来源"]
        # 后续窗口虽未物化，范围和许可来源同样齐备可说明
        for item in manifest[1:]:
            assert item["状态"] == "未安排"
            assert item["窗口起点"] < item["窗口终点"]
            assert item["许可来源"]
