"""并发盘点任务——把键空间区间展开为并行的对象列举。

整体结构与单机多线程模型对应：

* **协调器**：按 ``concurrency`` 从 :class:`~obsfast.data_map.KeySpaceHints` 领取
  区间，交给工作线程，并回收已完成区间；
* **工作线程**：对每个区间持续分页拉取，直到越过区间上界或列举完毕；
* **批次队列**：工作线程把「前缀 → 对象列表」的批次推入队列，由聚合任务消费。

失败处理沿用「可续跑 / 终止」两分法：网络类与超时类错误会从该批次记录的
``next_start`` 原地续跑；桶不存在、鉴权失败、重定向等错误直接终止并向上抛出。
由于续跑可能重复投递同一对象，聚合层对同名对象做了覆盖式去重。
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from .data_map import KeySpaceHints
from .model import (
    DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS,
    DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS,
    ERR_NEXT_STREAM_TIMEOUT,
    FLAG_DIFF_MODE,
    FLAG_DIR_LEFT,
    FLAG_DIR_RIGHT,
    FastListError,
    HttpStatusTracker,
    ObjectProps,
)
from .obs_client import ObsEndpoint

logger = logging.getLogger("obsfast.listing")

#: 单个批次（前缀 → 对象列表）
Batch = dict[str, list[tuple[str, ObjectProps]]]


class SharedState:
    """跨任务共享的运行状态。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._quit = threading.Event()
        self.tracker = HttpStatusTracker()
        self.next_stream_timeouts = 0
        self.client_timeouts = 0
        self.client_generic_errors = 0
        self.listed_objects = 0
        self.listed_pages = 0

    # -- 退出信号 -----------------------------------------------------------

    def request_quit(self) -> None:
        self._quit.set()

    @property
    def quit_requested(self) -> bool:
        return self._quit.is_set()

    # -- 计数 ---------------------------------------------------------------

    def bump(self, **deltas: int) -> None:
        with self._lock:
            for name, delta in deltas.items():
                setattr(self, name, getattr(self, name) + delta)

    def record_status(self, code: int) -> None:
        if code:
            self.tracker.inc(code)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "listed_objects": self.listed_objects,
                "listed_pages": self.listed_pages,
                "next_stream_timeouts": self.next_stream_timeouts,
                "client_timeouts": self.client_timeouts,
                "client_generic_errors": self.client_generic_errors,
                "http_status": self.tracker.snapshot(),
            }


@dataclass
class ListContext:
    """一个盘点方向（左/右）的上下文。"""

    endpoint: ObsEndpoint
    bucket: str
    side_flags: int
    shared: SharedState
    label: str = ""
    batch_queue: "queue.Queue[Optional[Batch]]" = field(default_factory=queue.Queue)

    @property
    def is_diff_mode(self) -> bool:
        return bool(self.side_flags & FLAG_DIFF_MODE)

    @property
    def side_name(self) -> str:
        if self.side_flags & FLAG_DIR_LEFT:
            return "SOURCE"
        if self.side_flags & FLAG_DIR_RIGHT:
            return "TARGET"
        return "UNKNOWN"


def left_flags(diff_mode: bool) -> int:
    return FLAG_DIR_LEFT | (FLAG_DIFF_MODE if diff_mode else 0)


def right_flags(diff_mode: bool) -> int:
    return FLAG_DIR_RIGHT | (FLAG_DIFF_MODE if diff_mode else 0)


# ---------------------------------------------------------------------------
# 单个区间的分页拉取
# ---------------------------------------------------------------------------


def flat_list(
    ctx: ListContext,
    prefix: str,
    start_after: str,
    until: Optional[str],
) -> None:
    """拉取 ``[start_after, until)`` 区间内的全部对象并投递为批次。

    与区间上界的比较使用字典序，因此 ``until`` 允许是「不存在的键或前缀」。
    一旦当前键不小于上界即停止，避免把后续区间的对象重复计入。
    """
    marker = start_after
    next_start = start_after
    while True:
        try:
            page = ctx.endpoint.list_objects(ctx.bucket, prefix=prefix, marker=marker)
        except FastListError as exc:
            ctx.shared.record_status(exc.http_status)
            raise
        ctx.shared.bump(listed_pages=1)

        # 由最后一个对象的键推导续跑起点：即使本页中途失败，也能从安全位置重来
        if page.items:
            next_start = page.items[-1][0]

        batch: Batch = {}
        ended = False
        for key, props in page.items:
            if until is not None and key >= until:
                logger.debug("区间在键 %s 处提前结束", key)
                ended = True
                break
            props.set_side(ctx.side_flags)
            holder = batch.setdefault(_prefix_of(key), [])
            holder.append((_name_of(key), props))

        ctx.shared.bump(listed_objects=page.key_count)
        ctx.batch_queue.put(batch)

        if ended or not page.is_truncated:
            return
        marker = page.next_marker or next_start
        if not marker:
            return


def run_to_complete(
    ctx: ListContext,
    prefix: str,
    start_after: str,
    until: Optional[str],
) -> None:
    """反复尝试直到区间完成；仅在遇到不可续跑错误时放弃。"""
    marker = start_after
    while True:
        try:
            flat_list(ctx, prefix, marker, until)
            return
        except FastListError as exc:
            if exc.continue_on_error():
                marker = exc.next_start or marker
                ctx.shared.bump(
                    next_stream_timeouts=1
                    if exc.errno == ERR_NEXT_STREAM_TIMEOUT
                    else 0,
                )
                logger.warning(
                    "盘点 %s 区间 [%s, %s) 出错，从 %s 续跑：%s",
                    ctx.bucket,
                    start_after,
                    until,
                    marker or "/",
                    exc,
                )
                if ctx.shared.quit_requested:
                    return
                continue
            logger.error("盘点 %s 失败：%s", ctx.bucket, exc)
            raise


def _prefix_of(key: str) -> str:
    """按 ``/`` 拆分对象键，返回前缀（无分隔符时记为根前缀 ``"/"``）。"""
    head, sep, _ = key.rpartition("/")
    return head if sep else "/"


def _name_of(key: str) -> str:
    _, sep, tail = key.rpartition("/")
    return tail if sep else key


# ---------------------------------------------------------------------------
# 工作线程与协调器
# ---------------------------------------------------------------------------


def run_flat_list(
    ctx: ListContext,
    start_prefix: str,
    concurrency: int,
    hints: KeySpaceHints,
    progress=(),
) -> None:
    """按给定并发度消费全部键空间区间。

    :param progress: 可选的进度报告回调集合（由 :mod:`obsfast.monitor` 提供），
        以 ``(ctx, hints)`` 为参数，按心跳间隔调用
    """
    lock = threading.Lock()
    error_box: list[BaseException] = []

    def worker() -> None:
        while True:
            with lock:
                if error_box:
                    return
                pair = hints.next()
            if pair is None:
                return
            try:
                start, until = pair.to_task_input()
                run_to_complete(ctx, start_prefix, start, until)
            except BaseException as exc:  # noqa: BLE001 - 汇总后统一向上抛出
                with lock:
                    error_box.append(exc)
                return
            finally:
                with lock:
                    try:
                        hints.finish(pair.index)
                    except KeyError:
                        pass

    workers = [
        threading.Thread(target=worker, name=f"obsfast-lister-{i}", daemon=True)
        for i in range(max(1, concurrency))
    ]
    for thread in workers:
        thread.start()

    while any(thread.is_alive() for thread in workers):
        _report(progress, ctx, hints)
        if ctx.shared.quit_requested:
            break
        time.sleep(0.5)

    for thread in workers:
        thread.join(timeout=DEFAULT_TASK_COMPLETE_QUIT_WAIT_SECS * 2 + 10)
        if thread.is_alive():
            logger.warning("盘点线程 %s 未在等待窗口内结束", thread.name)

    if error_box:
        raise error_box[0]


_last_report: dict[str, float] = {}


def _report(progress, ctx: ListContext, hints: KeySpaceHints) -> None:
    if not progress:
        return
    now = time.time()
    key = ctx.label or ctx.bucket
    if now - _last_report.get(key, 0) < DEFAULT_TASK_HEARTBEAT_INTERVAL_SECS:
        return
    _last_report[key] = now
    for reporter in progress:
        reporter(ctx, hints)


def send_completion_sentinels(queues: list["queue.Queue[Optional[Batch]]"]) -> None:
    """向各方向队列投递结束标记，唤醒聚合任务。"""
    for batch_queue in queues:
        batch_queue.put(None)
