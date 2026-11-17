"""运行期观测——心跳输出与最终统计汇总。

职责对应「监控任务」：按固定间隔把在途区间数、已盘点对象数、HTTP 状态码
分布、超时与错误计数打到日志，便于在亿级对象桶的长跑过程中判断进展与健康度。
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

from .data_map import KeySpaceHints
from .listing import ListContext
from .model import DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS
from .parquet_writer import DIFF_FLAG_LABELS

logger = logging.getLogger("obsfast.monitor")


def report(ctx: ListContext, hints: KeySpaceHints) -> None:
    """单次心跳输出。"""
    snapshot = ctx.shared.snapshot()
    logger.info(
        "%s 心跳：区间 %d 待领 / %d 在途 / %d 完成，已盘点 %d 个对象（%d 页）",
        ctx.label or ctx.bucket,
        hints.pending,
        hints.inflight,
        hints.completed,
        snapshot["listed_objects"],
        snapshot["listed_pages"],
    )
    http_status = snapshot["http_status"]
    if http_status:
        rendered = ", ".join(f"[{code} => {count}]" for code, count in http_status)
        logger.info("  HTTP 状态码分布：%s", rendered)

    timeouts = (
        snapshot["next_stream_timeouts"]
        + snapshot["client_timeouts"]
        + snapshot["client_generic_errors"]
    )
    if timeouts:
        logger.warning(
            "  异常计数：列举续跑 %d，连接超时 %d，其他客户端错误 %d",
            snapshot["next_stream_timeouts"],
            snapshot["client_timeouts"],
            snapshot["client_generic_errors"],
        )


class Heartbeat:
    """后台心跳线程；与盘点任务并行运行，随任务结束而退出。"""

    def __init__(self, contexts: list[ListContext], hints_map: dict[str, KeySpaceHints]) -> None:
        self._contexts = contexts
        self._hints = hints_map
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def __enter__(self) -> "Heartbeat":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="obsfast-monitor", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS):
            for ctx in self._contexts:
                hints = self._hints.get(ctx.label or ctx.bucket)
                if hints is None:
                    continue
                try:
                    report(ctx, hints)
                except Exception:  # pragma: no cover - 心跳不得影响主流程
                    logger.debug("心跳输出异常", exc_info=True)

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def summarize(
    mode: str,
    groups: dict[int, list[tuple[str, ...]]],
    prefix_count: int,
    object_count: int,
) -> str:
    """生成最终统计摘要文本。"""
    lines = [
        f"运行模式：{'单桶盘点' if mode == 'list' else '双桶比对'}",
        f"前缀数量：{prefix_count}",
        f"对象数量：{object_count}",
    ]
    total_diff = 0
    for flag in sorted(groups):
        rows = groups[flag]
        if not rows:
            continue
        lines.append(f"  DiffFlag={flag}（{DIFF_FLAG_LABELS.get(flag, '未知')}）：{len(rows)} 条")
        total_diff += len(rows)
    lines.append(f"导出记录合计：{total_diff} 条")
    return "\n".join(lines)
