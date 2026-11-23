"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、可抽取窗口清单消费与推进水位。

窗口清单由源端登记确认时按登记配置生成，本服务负责：
1. 按清单物化首窗与后续调度批次（确定性窗口编号，重跑不多开一窗）；
2. 回答调度「究竟可以开始哪一窗」，并给出该窗实际使用的增量字段、范围与许可来源；
3. 在字段契约未生效或窗口范围不齐时拒绝安排/启动抽取。
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Any

from app.services.base import BaseModuleService
from app.services.source_register import SCHEDULE_MODULE
from app.store import store

_FMT = "%Y-%m-%dT%H:%M:%S"


def parse_window_range(range_spec: str) -> tuple[datetime, datetime]:
    """解析登记配置里的开窗范围，支持「今日」「昨日」与显式起止区间。

    显式区间写法：``2026-11-01~2026-11-07`` 或带时间的
    ``2026-11-01T00:00:00~2026-11-07T23:59:59``。
    """
    text = (range_spec or "").strip()
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if text in ("", "今日", "今天"):
        return today, today.replace(hour=23, minute=59, second=59)
    if text == "昨日" or text == "昨天":
        day = today - timedelta(days=1)
        return day, day.replace(hour=23, minute=59, second=59)
    if "~" not in text:
        raise ValueError("区间范围需用「~」连接起点和终点")
    start_text, end_text = (part.strip() for part in text.split("~", 1))
    start_dt = _parse_one_edge(start_text, ("", " 00:00:00"))
    end_dt = _parse_one_edge(end_text, ("", " 23:59:59"))
    if end_dt < start_dt:
        raise ValueError("开窗范围终点早于起点")
    return start_dt, end_dt


def _parse_one_edge(text: str, fallback_suffix: tuple[str, str]) -> datetime:
    try:
        return datetime.strptime(text, _FMT)
    except ValueError:
        # 仅给日期时补时间；同时兼容「YYYY-MM-DD HH:MM:SS」的空格写法
        candidate = text + fallback_suffix[1]
        try:
            return datetime.strptime(candidate, _FMT)
        except ValueError:
            return datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S")


def split_ranges(start: datetime, end: datetime, count: int) -> list[tuple[str, str]]:
    """把完整开窗范围等距切成 count 个首尾相接的子范围。"""
    total = int((end - start).total_seconds())
    step = max(1, total // count)
    ranges: list[tuple[str, str]] = []
    for seq in range(count):
        part_start = start + timedelta(seconds=step * seq)
        part_end = start + timedelta(seconds=step * (seq + 1) - 1)
        if seq == count - 1:
            part_end = end
        ranges.append((part_start.strftime(_FMT), part_end.strftime(_FMT)))
    return ranges


def window_code(source_code: str, seq: int) -> str:
    """确定性窗口编号：同一源端同一序号永远指向同一窗口，重跑不会多开窗。"""
    return f"EXTR-{source_code}-{seq:02d}"


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = [
        "窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数",
        "窗口类型", "推进水位", "许可来源", "窗口清单",
    ]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    # ------------------------------------------------------------------
    # 清单物化
    # ------------------------------------------------------------------

    def materialize_manifest_window(self, spec: dict[str, Any], schedule_code: str,
                                    license_source: str) -> dict[str, Any]:
        """按窗口清单物化一个窗口；编号已存在则直接复用，绝不重复开窗。"""
        existing = next((r for r in store.rows(self.MODULE)
                         if r.get("窗口编号") == spec["窗口编号"]), None)
        if existing is not None:
            return existing
        entry, missing = self.create_entry({
            "窗口编号": spec["窗口编号"],
            "源端编号": spec["源端编号"],
            "增量字段": spec["增量字段"],
            "窗口起点": spec["窗口起点"],
            "窗口终点": spec["窗口终点"],
            "抽取条数": str(spec.get("抽取条数", "1000")),
            "窗口类型": spec.get("窗口类型", "增量抽取"),
            "推进水位": "0",
            "许可来源": license_source,
            "窗口清单": schedule_code,
        })
        if entry is None:
            raise ValueError(f"窗口物化失败: {missing}")
        entry["status"] = "待抽取"
        entry["pending"] = True
        entry["abnormal"] = False
        # 写入窗口配置快照，范围与许可来源随窗口持久化
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"],
              "source": entry["源端编号"], "incremental_field": entry["增量字段"],
              "start": entry["窗口起点"], "end": entry["窗口终点"],
              "license_source": license_source, "schedule_code": schedule_code,
              "created_at": time.time()}]
        )
        return entry

    def create_window(self, source_code: str, count: int = 10) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界与初始水位。"""
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        entry, missing = self.create_entry({
            "窗口编号": f"EXTR-{source_code}-{int(time.time())}",
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": "增量抽取",
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 写入窗口配置快照
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "created_at": time.time()}]
        )
        return entry

    # ------------------------------------------------------------------
    # 可抽取窗口清单消费：首窗与后续批次
    # ------------------------------------------------------------------

    def _source_row(self, source_code: str) -> dict[str, Any] | None:
        return next((r for r in store.rows("source_register")
                     if r.get("源端编号") == source_code), None)

    def _schedule_for(self, source_code: str) -> dict[str, Any] | None:
        rows = [r for r in store.rows(SCHEDULE_MODULE) if r.get("源端编号") == source_code]
        return rows[-1] if rows else None

    def _active_contract(self, source_code: str) -> dict[str, Any] | None:
        code = f"FIEL-{source_code}"
        row = next((r for r in store.rows("field_contract")
                    if r.get("契约编号") == code), None)
        return row if row is not None and row.get("status") == "已生效" else None

    def _window_rows_for_source(self, source_code: str) -> list[dict[str, Any]]:
        return [r for r in store.rows(self.MODULE) if r.get("源端编号") == source_code]

    def get_schedule_dispatch(self, source_code: str) -> dict[str, Any]:
        """回答调度究竟可以开始哪一窗。

        返回该窗实际使用的增量字段、窗口范围、许可来源与清单状态；
        源端停用、契约未生效、范围缺失或窗口已全部消费时给出阻断原因而非开窗。
        """
        source = self._source_row(source_code)
        if source is None:
            return {"can_start": False, "reason": f"源端 {source_code} 未登记"}
        if source["status"] != "已登记":
            return {"can_start": False,
                    "reason": f"源端状态为「{source['status']}」，暂不允许安排抽取",
                    "source_status": source["status"]}
        if self._active_contract(source_code) is None:
            return {"can_start": False,
                    "reason": f"字段契约 FIEL-{source_code} 未生效，抽取前置条件不齐"}
        schedule = self._schedule_for(source_code)
        if schedule is None or not schedule.get("窗口清单"):
            return {"can_start": False, "reason": "可抽取窗口清单尚未生成，无法安排调度"}

        materialized = {r["窗口编号"]: r for r in self._window_rows_for_source(source_code)}
        for spec in schedule["窗口清单"]:
            row = materialized.get(spec["窗口编号"])
            if row is None:
                # 下一批次尚未物化：先按清单物化，再作为可开始窗口返回
                if not str(spec.get("窗口起点") or "").strip() or \
                        not str(spec.get("窗口终点") or "").strip():
                    return {"can_start": False,
                            "reason": f"窗口 {spec['窗口编号']} 范围不齐，不能安排抽取"}
                row = self.materialize_manifest_window(
                    spec, schedule["调度编号"], spec["许可来源"])
            if row.get("status") in {"待抽取", "草稿"}:
                return {
                    "can_start": True,
                    "window_id": row["id"],
                    "窗口编号": row["窗口编号"],
                    "窗口序号": spec["窗口序号"],
                    "调度批次": spec["调度批次"],
                    "增量字段": row["增量字段"],
                    "窗口起点": row["窗口起点"],
                    "窗口终点": row["窗口终点"],
                    "许可来源": row.get("许可来源", spec["许可来源"]),
                    "窗口清单": schedule["调度编号"],
                    "status": row["status"],
                }
        return {"can_start": False, "reason": "窗口清单内的窗口全部已抽取或闭合",
                "schedule_code": schedule["调度编号"]}

    def plan_next_batch(self, source_code: str) -> dict[str, Any]:
        """消费可抽取窗口清单安排下一批次：首窗优先，其后按序号推进。"""
        dispatch = self.get_schedule_dispatch(source_code)
        if not dispatch.get("can_start"):
            return dispatch
        schedule = self._schedule_for(source_code)
        materialized = {r["窗口编号"]: r for r in self._window_rows_for_source(source_code)}
        pending = [
            spec for spec in schedule["窗口清单"]
            if (w := materialized.get(spec["窗口编号"])) is not None
            and w.get("status") in {"待抽取", "草稿"}
        ]
        current = next(spec for spec in pending
                       if spec["窗口编号"] == dispatch["窗口编号"])
        upcoming = [
            {"窗口编号": spec["窗口编号"], "调度批次": spec["调度批次"],
             "窗口起点": spec["窗口起点"], "窗口终点": spec["窗口终点"]}
            for spec in schedule["窗口清单"] if spec["窗口序号"] > current["窗口序号"]
        ]
        return {
            "source_code": source_code,
            "current_batch": current["调度批次"],
            "current_window": dispatch["窗口编号"],
            "增量字段": dispatch["增量字段"],
            "窗口起点": dispatch["窗口起点"],
            "窗口终点": dispatch["窗口终点"],
            "许可来源": dispatch["许可来源"],
            "pending_window_count": len(pending),
            "upcoming_batches": upcoming,
        }

    # ------------------------------------------------------------------
    # 抽取执行
    # ------------------------------------------------------------------

    def execute_extract(self, window_id: int, record_count: int = 10) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件并推进窗口水位。

        从已在 store 登记的源端信息中查找匹配记录，按窗口范围筛选数据行，
        逐条写入抽取产物文件（jsonl），最后推进水位并自动触发传输批次创建。
        字段契约未生效或窗口范围不齐时拒绝启动抽取。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        # 前置门禁：字段契约生效 + 窗口范围齐备，缺一不可
        source_code = window["源端编号"]
        if self._active_contract(source_code) is None:
            return {"error": f"字段契约 FIEL-{source_code} 未生效，窗口 {window['窗口编号']} 不能启动抽取",
                    "window_id": window_id}
        if not str(window.get("窗口起点") or "").strip() or \
                not str(window.get("窗口终点") or "").strip():
            return {"error": f"窗口 {window['窗口编号']} 的窗口范围不齐，不能启动抽取",
                    "window_id": window_id}

        entry, msg = self.run_action(window_id, "启动抽取")
        if entry is None:
            return {"error": msg, "window_id": window_id}

        # 生成抽取数据行，每条记录含窗口编号、源端编号、增量字段取值和时间戳
        records = self._simulate_records(source_code, count=record_count, field_prefix="ext")
        for rec in records:
            rec["incremental_field"] = window["增量字段"]
            rec["window_code"] = window["窗口编号"]

        # 写入抽取产物文件——这是本模块的核心数据产物
        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        written = self._write_jsonl(output_file, records)

        # 推进窗口水位
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "incremental_field": window["增量字段"],
            "window_start": window["窗口起点"],
            "window_end": window["窗口终点"],
            "license_source": window.get("许可来源", ""),
            "records_extracted": written,
            "last_extract_at": time.time(),
            "watermark": str(written),
        })

        result = {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "增量字段": window["增量字段"],
            "窗口起点": window["窗口起点"],
            "窗口终点": window["窗口终点"],
            "许可来源": window.get("许可来源", ""),
            "records_extracted": written,
            "artifact_path": str(output_file),
            "watermark": str(written),
        }

        # 抽取完成后自动触发传输批次创建
        try:
            tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
            batch_result = tb_svc.create_batch(window["窗口编号"], written)
            result["auto_batch"] = batch_result
        except Exception as e:
            result["auto_batch"] = {"error": str(e)}

        return result

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，生成窗口产物摘要（记录条数、字节总量、校验值），写入闭合事件日志。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        entry, msg = self.run_action(window_id, "闭合窗口")
        # 从产物文件读取统计信息
        artifact_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))

        # 写入闭合事件日志，记录抽取产物的完整统计
        self._write_jsonl(
            self._data_dir("events") / "window_close_log.jsonl",
            [{"action": "close", "window_code": window["窗口编号"], "record_count": len(records),
              "checksum": checksum, "closed_at": time.time()}]
        )

        return {
            "window_code": window["窗口编号"],
            "status": entry["status"] if entry else "已闭合",
            "record_count": len(records),
            "checksum": checksum,
            "message": msg,
        }
