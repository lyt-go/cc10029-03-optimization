"""源端登记领域服务：管理参与数据交换的源端系统注册、确认编排、停用冲突与登记恢复。

确认登记时从源端登记配置读取实际的增量字段、开窗范围与窗口份数，生成有关联的
可抽取窗口清单与调度批次，只物化首窗；首窗准备失败会把源端置为「登记中断」并
保留失败前的登记配置快照，支持操作人员从调度记录发起恢复。

并发约定：确认/恢复的首窗准备期间持有进程内准备锁，停用请求撞上准备或恢复时会
被明确拒绝，避免两项操作在同一源端上互相覆盖。
"""
from __future__ import annotations

import time
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import store

# 调度记录表（不注册为前端业务模块，仅由源端登记与增量窗口服务消费）
SCHEDULE_MODULE = "schedule_record"


class WindowPreparationError(RuntimeError):
    """首窗准备阶段的模拟失败，用于驱动登记中断与恢复链路。"""

    def __init__(self, stage: str, reason: str) -> None:
        super().__init__(reason)
        self.stage = stage
        self.reason = reason


class SourceRegisterService(BaseModuleService):
    MODULE = "source_register"
    ENTITY = "源端系统"
    OPERATION_TERM = "校验"
    LIST_FIELDS = [
        "源端编号", "系统名称", "数据域", "记录格式", "编码方式", "日增量级",
        "抽取方式", "登记游标", "增量字段", "开窗范围", "窗口份数",
    ]
    REQUIRED_FIELDS = ["源端编号", "系统名称", "数据域"]
    STATUS_ORDER = ["草稿", "待校验", "登记中断", "已登记", "停用待恢复", "已停用"]
    ACTION_RULES = {
        "提交校验": "待校验",
        "确认登记": "已登记",
        "恢复登记": "已登记",
        "重新启用": "已登记",
        "标记停用": "已停用",
    }
    NEGATIVE_ACTIONS = ["标记停用"]

    # 允许发起确认登记的状态（历史链路里存在草稿态直接确认的用法，保持兼容）
    _CONFIRMABLE = {"草稿", "待校验", "登记中断"}

    def __init__(self) -> None:
        # 正在进行首窗准备/登记恢复的源端行 id，充当进程内互斥锁
        self._preparing: set[int] = set()

    # ------------------------------------------------------------------
    # 通用动作网关：状态流转只允许在服务层收口
    # ------------------------------------------------------------------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "确认登记":
            result = self.confirm_register(entry_id)
            if "error" in result:
                return None, str(result["error"])
            return result["source_entry"], "源端系统已确认登记"
        if action == "恢复登记":
            result = self.resume_register(entry_id)
            if "error" in result:
                return None, str(result["error"])
            return result["source_entry"], "中断的登记已恢复"
        if action == "标记停用":
            return self.deactivate(entry_id)
        if action == "重新启用":
            return self.reactivate(entry_id)
        return super().run_action(entry_id, action)

    # ------------------------------------------------------------------
    # 登记与调度记录
    # ------------------------------------------------------------------

    def register_source(self, values: dict[str, Any]) -> dict[str, Any]:
        """登记新源端系统并写入登记事件日志，初始化源端配置快照。"""
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"缺少必填字段: {missing}")
        source_key = entry["源端编号"]
        # 初始化登记水位
        self._write_checkpoint(f"source_{source_key}", {
            "source_key": source_key,
            "status": "registered",
            "registered_at": time.time(),
            "extract_count": 0,
            "total_records": 0,
        })
        # 写入登记日志，记录源端配置快照
        self._write_jsonl(
            self._data_dir("events") / "source_register_log.jsonl",
            [{"action": "register", "source_key": source_key, "name": entry["系统名称"],
              "domain": entry["数据域"], "format": entry["记录格式"], "at": time.time()}]
        )
        return entry

    def _schedule_rows(self) -> list[dict[str, Any]]:
        return store.rows(SCHEDULE_MODULE)

    def _latest_schedule(self, source_key: str) -> dict[str, Any] | None:
        """读取某源端最近一次确认生成的调度记录（窗口清单与登记配置快照的载体）。"""
        matched = [r for r in self._schedule_rows() if r.get("源端编号") == source_key]
        return matched[-1] if matched else None

    def _config_snapshot(self, entry: dict[str, Any]) -> dict[str, Any]:
        """固化失败之前的登记配置，恢复时逐项回溯，不依赖现场可能被改动的行数据。"""
        return {field: entry.get(field, "") for field in self.LIST_FIELDS}

    def _log_schedule_event(self, action: str, source_key: str, schedule_code: str,
                            **extra: Any) -> None:
        record = {"action": action, "source_key": source_key,
                  "schedule_code": schedule_code, "at": time.time()}
        record.update(extra)
        self._write_jsonl(self._data_dir("events") / "schedule_log.jsonl", [record])

    def _persist_schedule(self, schedule: dict[str, Any]) -> None:
        """调度记录落断点文件，保证中断后能回到失败之前的登记配置与窗口清单。"""
        self._write_checkpoint(
            f"schedule_{schedule['源端编号']}_{schedule['attempt']}", dict(schedule))

    # ------------------------------------------------------------------
    # 确认登记：清单编排 + 首窗准备 + 失败恢复
    # ------------------------------------------------------------------

    def confirm_register(
        self,
        source_id: int,
        *,
        fail_at: str | None = None,
        on_preparing: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """确认源端登记，生成可抽取窗口清单与调度批次并物化首窗。

        fail_at:
            - ``首窗准备``：清单落盘后、首窗物化前模拟失败，源端进入登记中断且没有窗口；
            - ``首窗之后``：首窗物化后模拟失败，源端进入登记中断但首窗保留。
        on_preparing：持有准备锁期间回调，用于确定性模拟停用请求撞上准备/恢复。
        """
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"error": f"源端系统 {source_id} 不存在或已归档", "source_id": source_id}
        if entry["status"] not in self._CONFIRMABLE:
            return {"error": f"源端当前状态为「{entry['status']}」，不能确认登记",
                    "source_id": source_id}
        if source_id in self._preparing:
            return {"error": "首窗准备进行中，请等待本次登记恢复结束后再操作",
                    "source_id": source_id}

        source_key = entry["源端编号"]
        prior = self._latest_schedule(source_key)
        attempt = int(prior["attempt"]) + 1 if prior else 1
        schedule_code = f"SCHE-{source_key}-{attempt}"

        self._preparing.add(source_id)
        try:
            # 1) 前置门禁：字段契约与窗口范围齐备才能安排抽取
            try:
                contract = self._ensure_field_contract(entry)
                manifest = self._build_manifest(entry, schedule_code, contract["契约编号"])
            except WindowPreparationError as exc:
                return self._interrupt(entry, None, schedule_code, attempt, exc,
                                       self._config_snapshot(entry))

            # 2) 调度记录落盘：先存配置快照与清单，再做首窗准备
            schedule = {
                "id": max((int(r.get("id", 0)) for r in self._schedule_rows()), default=0) + 1,
                "调度编号": schedule_code,
                "源端编号": source_key,
                "attempt": attempt,
                "登记配置": self._config_snapshot(entry),
                "窗口清单": manifest,
                "状态": "首窗准备中",
                "失败阶段": "",
                "失败原因": "",
                "停用挂起": False,
                "created_at": time.time(),
            }
            self._schedule_rows().append(schedule)
            self._persist_schedule(schedule)
            self._log_schedule_event("confirm", source_key, schedule_code, stage="首窗准备中")

            # 准备锁持有期间允许外部插入冲突操作（停用请求碰上登记恢复）
            if on_preparing is not None:
                on_preparing()

            # 3) 首窗准备（模拟失败点一：准备期间失败，不得留下任何窗口）
            if fail_at == "首窗准备":
                raise WindowPreparationError("首窗准备", "首窗准备期间源端连接中断，窗口未生成")

            license_source = self._license_source(contract["契约编号"], schedule_code)
            first_window = self._materialize_first_window(entry, manifest, schedule_code,
                                                          license_source)

            # 4) 模拟失败点二：首窗已生成，清单与首窗必须原样保留
            if fail_at == "首窗之后":
                raise WindowPreparationError("首窗生成后", "首窗生成后登记确认中断，待恢复续做")

            # 5) 配套通道配置（保留既有跨模块联动）
            triggers = self._collect_triggers(entry, contract, first_window)
            self._mark_active(entry, schedule)
            self._log_schedule_event("confirmed", source_key, schedule_code,
                                     window_count=len(manifest))
            return {
                "source_entry": entry,
                "source_key": source_key,
                "confirmed_at": time.time(),
                "schedule_code": schedule_code,
                "window_manifest": self.live_manifest(source_key),
                "auto_triggers": triggers,
            }
        except WindowPreparationError as exc:
            return self._interrupt(entry, schedule, schedule_code, attempt, exc,
                                   self._config_snapshot(entry))
        finally:
            self._preparing.discard(source_id)

    def _interrupt(self, entry: dict[str, Any], schedule: dict[str, Any] | None,
                   schedule_code: str, attempt: int, exc: WindowPreparationError,
                   config_snapshot: dict[str, Any]) -> dict[str, Any]:
        """把源端与调度记录置为中断态，保留失败前配置，等待恢复。"""
        entry["status"] = "登记中断"
        entry["pending"] = True
        entry["abnormal"] = True
        if schedule is None:
            # 前置门禁失败时还没有调度记录，补建一条，确保恢复有据可依
            schedule = {
                "id": max((int(r.get("id", 0)) for r in self._schedule_rows()), default=0) + 1,
                "调度编号": schedule_code,
                "源端编号": entry["源端编号"],
                "attempt": attempt,
                "登记配置": config_snapshot,
                "窗口清单": [],
                "状态": "已中断",
                "失败阶段": exc.stage,
                "失败原因": exc.reason,
                "停用挂起": False,
                "created_at": time.time(),
            }
            self._schedule_rows().append(schedule)
        schedule["状态"] = "已中断"
        schedule["失败阶段"] = exc.stage
        schedule["失败原因"] = exc.reason
        self._persist_schedule(schedule)
        self._log_schedule_event("interrupt", entry["源端编号"], schedule_code,
                                 stage=exc.stage, reason=exc.reason)
        return {
            "error": f"登记确认中断（{exc.stage}）：{exc.reason}",
            "source_id": entry["id"],
            "source_entry": entry,
            "schedule_code": schedule_code,
            "interrupted_stage": exc.stage,
        }

    def _mark_active(self, entry: dict[str, Any], schedule: dict[str, Any]) -> None:
        entry["status"] = "已登记"
        entry["pending"] = False
        entry["abnormal"] = False
        schedule["状态"] = "进行中"
        schedule["失败阶段"] = ""
        schedule["失败原因"] = ""
        self._persist_schedule(schedule)

    # ------------------------------------------------------------------
    # 停用 / 恢复 / 重新启用
    # ------------------------------------------------------------------

    def deactivate(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """停用源端；撞上首窗准备/登记恢复时拒绝，中断态停用则挂起等待恢复后生效。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry_id in self._preparing:
            return None, "源端正处于首窗准备/登记恢复中，停用请求与登记操作冲突，请恢复完成后再停用"
        status = entry["status"]
        if status == "已停用":
            return entry, "源端系统已是停用状态"
        schedule = self._latest_schedule(entry["源端编号"])
        if status == "登记中断":
            # 停用请求碰上登记恢复：先挂起，恢复续做完成后按停用生效
            entry["status"] = "停用待恢复"
            entry["pending"] = True
            entry["abnormal"] = True
            if schedule is not None:
                schedule["停用挂起"] = True
                self._persist_schedule(schedule)
                self._log_schedule_event("defer_deactivate", entry["源端编号"],
                                         schedule["调度编号"])
            return entry, "停用请求已挂起，待中断的登记恢复后生效"
        entry["status"] = "已停用"
        entry["pending"] = False
        entry["abnormal"] = True
        if schedule is not None:
            schedule["状态"] = "已停用"
            self._persist_schedule(schedule)
            self._log_schedule_event("deactivate", entry["源端编号"], schedule["调度编号"])
        return entry, "源端系统已标记停用，窗口清单与许可来源保留可查"

    def resume_register(
        self,
        source_id: int,
        *,
        on_preparing: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        """从调度记录恢复中断的登记：回溯失败前配置，幂等续做，不多开一窗。"""
        entry = store.find(self.MODULE, source_id)
        if entry is None:
            return {"error": f"源端系统 {source_id} 不存在或已归档", "source_id": source_id}
        if entry["status"] not in {"登记中断", "停用待恢复"}:
            return {"error": f"源端当前状态为「{entry['status']}」，没有可恢复的中断登记",
                    "source_id": source_id}
        schedule = self._latest_schedule(entry["源端编号"])
        if schedule is None:
            return {"error": "找不到调度记录，无法回溯登记配置", "source_id": source_id}
        if source_id in self._preparing:
            return {"error": "登记恢复正在进行中，请勿重复发起", "source_id": source_id}

        deactivate_after = entry["status"] == "停用待恢复"
        self._preparing.add(source_id)
        try:
            # 1) 回到失败之前的登记配置
            snapshot = schedule["登记配置"]
            for field, value in snapshot.items():
                entry[field] = value

            try:
                # 2) 重新校验契约与清单（配置回溯后仍须满足门禁）
                contract = self._ensure_field_contract(entry)
                if not schedule["窗口清单"]:
                    manifest = self._build_manifest(entry, schedule["调度编号"],
                                                    contract["契约编号"])
                    schedule["窗口清单"] = manifest
                license_source = self._license_source(contract["契约编号"],
                                                       schedule["调度编号"])
            except WindowPreparationError as exc:
                # 回溯后的配置仍不满足门禁：保持中断现场，等待修正后再次恢复
                entry["status"] = "停用待恢复" if deactivate_after else "登记中断"
                entry["pending"] = True
                entry["abnormal"] = True
                schedule["状态"] = "已中断"
                schedule["失败阶段"] = exc.stage
                schedule["失败原因"] = exc.reason
                self._persist_schedule(schedule)
                return {"error": f"恢复前置校验未通过（{exc.stage}）：{exc.reason}",
                        "source_id": source_id, "schedule_code": schedule["调度编号"]}

            self._log_schedule_event("resume", entry["源端编号"], schedule["调度编号"],
                                     stage=schedule.get("失败阶段", ""))
            if on_preparing is not None:
                on_preparing()

            # 3) 幂等续做：只补建缺失的首窗，已存在的窗口一律复用，重跑不多开一窗
            created, reused = self._reconcile_first_window(entry, schedule, license_source)

            # 4) 停用在恢复后生效：窗口清单、范围与许可来源全部保留
            if deactivate_after:
                entry["status"] = "已停用"
                entry["pending"] = False
                entry["abnormal"] = True
                schedule["状态"] = "已停用"
                schedule["停用挂起"] = False
            else:
                entry["status"] = "已登记"
                entry["pending"] = False
                entry["abnormal"] = False
                schedule["状态"] = "进行中"
            schedule["失败阶段"] = ""
            schedule["失败原因"] = ""
            self._persist_schedule(schedule)
            self._log_schedule_event("resumed", entry["源端编号"], schedule["调度编号"],
                                     created=created, reused=reused,
                                     deactivated=deactivate_after)
            return {
                "source_entry": entry,
                "source_key": entry["源端编号"],
                "schedule_code": schedule["调度编号"],
                "restored_config": snapshot,
                "window_created": created,
                "window_reused": reused,
                "deactivated_after_resume": deactivate_after,
                "window_manifest": self.live_manifest(entry["源端编号"]),
            }
        finally:
            self._preparing.discard(source_id)

    def reactivate(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """重新启用已停用源端，调度记录切回进行中，窗口清单保持不变。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry["status"] != "已停用":
            return None, f"源端当前状态为「{entry['status']}」，不能重新启用"
        entry["status"] = "已登记"
        entry["pending"] = False
        entry["abnormal"] = False
        schedule = self._latest_schedule(entry["源端编号"])
        if schedule is not None:
            schedule["状态"] = "进行中"
            self._persist_schedule(schedule)
            self._log_schedule_event("reactivate", entry["源端编号"], schedule["调度编号"])
        return entry, "源端系统已重新启用，沿用既有窗口清单与许可来源"

    # ------------------------------------------------------------------
    # 清单与首窗物化
    # ------------------------------------------------------------------

    def _ensure_field_contract(self, entry: dict[str, Any]) -> dict[str, Any]:
        """确认字段契约已生效；契约缺失或未生效视为前置条件不齐。"""
        from app.services.field_contract import FieldContractService
        svc = FieldContractService()
        source_key = entry["源端编号"]
        contract_code = f"FIEL-{source_key}"
        existing = next((r for r in store.rows("field_contract")
                         if r.get("契约编号") == contract_code), None)
        if existing is not None:
            if existing.get("status") != "已生效":
                raise WindowPreparationError(
                    "前置校验", f"字段契约 {contract_code} 尚未生效，不能安排抽取")
            return existing
        created = self._auto_create_seed("field_contract", {
            "契约编号": contract_code,
            "源字段": "src_field_001",
            "目标字段": "tgt_field_001",
            "数据类型": "string",
            "转换规则": "直接映射",
            "必填标记": "是",
            "默认值": "",
            "契约版本": "v1.0",
        })
        return created

    def _build_manifest(self, entry: dict[str, Any], schedule_code: str,
                        contract_code: str) -> list[dict[str, Any]]:
        """从登记配置读取增量字段与开窗范围，切出有关联的可抽取窗口清单。"""
        from app.services.extract_window import parse_window_range, split_ranges, window_code
        source_key = entry["源端编号"]
        incremental_field = str(entry.get("增量字段") or "").strip() or "updated_at"
        range_spec = str(entry.get("开窗范围") or "").strip() or "今日"
        try:
            count = int(str(entry.get("窗口份数") or "3").strip() or "3")
        except ValueError:
            count = 3
        count = max(1, count)
        try:
            start_dt, end_dt = parse_window_range(range_spec)
        except ValueError as exc:
            raise WindowPreparationError("前置校验", f"开窗范围「{range_spec}」无法解析：{exc}")
        ranges = split_ranges(start_dt, end_dt, count)
        license_source = self._license_source(contract_code, schedule_code)
        manifest = []
        for seq, (start_text, end_text) in enumerate(ranges, start=1):
            manifest.append({
                "窗口编号": window_code(source_key, seq),
                "窗口序号": seq,
                "调度批次": seq,
                "源端编号": source_key,
                "增量字段": incremental_field,
                "窗口起点": start_text,
                "窗口终点": end_text,
                "窗口类型": "增量抽取",
                "许可来源": license_source,
                "窗口清单": schedule_code,
                "状态": "未安排",
                "已物化": False,
            })
        return manifest

    @staticmethod
    def _license_source(contract_code: str, schedule_code: str) -> str:
        return f"字段契约 {contract_code}（已生效）；调度记录 {schedule_code}"

    def _materialize_first_window(self, entry: dict[str, Any], manifest: list[dict[str, Any]],
                                  schedule_code: str, license_source: str) -> dict[str, Any]:
        """物化清单首窗；确定性编号 + 先查后建，保证重跑不多开一窗。"""
        from app.services.extract_window import ExtractWindowService
        svc = ExtractWindowService()
        first_spec = next(item for item in manifest if item["窗口序号"] == 1)
        return svc.materialize_manifest_window(first_spec, schedule_code, license_source)

    def _reconcile_first_window(self, entry: dict[str, Any], schedule: dict[str, Any],
                                license_source: str) -> tuple[int, int]:
        """恢复时按清单对账：首窗缺失才补建，其余窗口仍交由调度按批次物化。"""
        from app.services.extract_window import ExtractWindowService
        svc = ExtractWindowService()
        created = reused = 0
        for spec in schedule["窗口清单"]:
            if spec["窗口序号"] != 1:
                continue
            existing = next((r for r in store.rows("extract_window")
                             if r.get("窗口编号") == spec["窗口编号"]), None)
            if existing is None:
                svc.materialize_manifest_window(spec, schedule["调度编号"], license_source)
                created += 1
            else:
                # 复用既有窗口，只刷新许可来源与清单关联，状态、范围保持原样
                existing["许可来源"] = license_source
                existing["窗口清单"] = schedule["调度编号"]
                reused += 1
        return created, reused

    def _collect_triggers(self, entry: dict[str, Any], contract: dict[str, Any],
                          first_window: dict[str, Any]) -> dict[str, Any]:
        triggers: dict[str, Any] = {
            "extract_window": {"created": first_window["窗口编号"],
                               "status": first_window["status"]},
            "field_contract": {"created": contract["契约编号"], "status": contract["status"]},
        }
        try:
            ch_entry = self._auto_create_seed("channel_config", {
                "通道编号": f"CHAN-{entry['源端编号']}",
                "传输协议": "HTTPS",
                "并发连接数": "4",
                "重试间隔": "30",
                "超时秒数": "300",
                "加密方式": "AES256",
                "通道类型": "源端通道",
                "配置版本": "v1.0",
            })
            if ch_entry:
                triggers["channel_config"] = {"created": ch_entry["通道编号"],
                                              "status": "已生效"}
        except Exception as e:  # pragma: no cover - 通道联动失败不阻断主链路
            triggers["channel_config"] = {"error": str(e)}
        return triggers

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def live_manifest(self, source_key: str) -> list[dict[str, Any]]:
        """返回带实时状态的窗口清单：状态、范围、许可来源始终可说明。"""
        schedule = self._latest_schedule(source_key)
        if schedule is None:
            return []
        windows = {r.get("窗口编号"): r for r in store.rows("extract_window")}
        result = []
        for spec in schedule["窗口清单"]:
            item = dict(spec)
            row = windows.get(spec["窗口编号"])
            if row is not None:
                item["状态"] = row.get("status", item["状态"])
                item["已物化"] = True
                item["增量字段"] = row.get("增量字段", item["增量字段"])
                item["窗口起点"] = row.get("窗口起点", item["窗口起点"])
                item["窗口终点"] = row.get("窗口终点", item["窗口终点"])
                item["许可来源"] = row.get("许可来源", item["许可来源"])
            result.append(item)
        return result

    def get_schedule_record(self, source_key: str) -> dict[str, Any] | None:
        """供操作人员从调度记录发起恢复，读取失败前的登记配置与中断现场。"""
        return self._latest_schedule(source_key)

    def get_registered_sources(self) -> list[dict[str, Any]]:
        """获取所有已登记的活跃源端系统列表，用于下游窗口抽取调度。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已登记"]
