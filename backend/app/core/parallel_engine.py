import asyncio
import heapq
import ipaddress
import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
)

from ..config import ServerConfig, settings
from ..models import ExecutionResult
from .script_executor import script_executor
from .ssh_pool import ssh_pool


T = TypeVar("T")


class TaskStatus(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    CANCELLED = "cancelled"


class BatchStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ServerGroup:
    group_id: str
    name: str
    servers: List[ServerConfig]
    tags: Set[str] = field(default_factory=set)
    location: Optional[str] = None
    network_zone: Optional[str] = None
    priority: int = 0


@dataclass
class TaskResult:
    task_id: str
    server_id: str
    server_name: str
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    duration: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "server_id": self.server_id,
            "server_name": self.server_name,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "duration": self.duration,
            "error_message": self.error_message,
        }


@dataclass
class Batch:
    batch_id: str
    group_id: str
    group_name: str
    tasks: List["EngineTask"]
    status: BatchStatus = BatchStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    success_count: int = 0
    failed_count: int = 0
    error_count: int = 0

    @property
    def total_count(self) -> int:
        return len(self.tasks)

    @property
    def failure_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.failed_count + self.error_count) / self.total_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "group_id": self.group_id,
            "group_name": self.group_name,
            "total_count": self.total_count,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "error_count": self.error_count,
            "failure_rate": self.failure_rate,
            "status": self.status.value,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
        }


@dataclass
class EngineTask:
    task_id: str
    job_id: str
    server: ServerConfig
    task_type: str
    command: Optional[str] = None
    script_content: Optional[str] = None
    script_name: Optional[str] = None
    interpreter: str = "bash"
    args: List[str] = field(default_factory=list)
    timeout: int = 300
    env: Dict[str, str] = field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    result: TaskResult = field(init=False)
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: Optional[float] = None
    batch_id: Optional[str] = None
    priority: int = 0
    sequence: int = 0
    estimated_duration: float = 0.0

    def __post_init__(self) -> None:
        self.result = TaskResult(
            task_id=self.task_id,
            server_id=self.server.id,
            server_name=self.server.name,
        )

    def __lt__(self, other: "EngineTask") -> bool:
        if self.priority != other.priority:
            return self.priority > other.priority
        if self.estimated_duration and other.estimated_duration:
            return self.estimated_duration < other.estimated_duration
        return self.sequence < other.sequence


@dataclass
class CircuitBreaker:
    group_id: str
    failure_threshold: float = 0.5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    total_count: int = 0
    last_failure_time: Optional[float] = None
    half_open_calls: int = 0

    def record_failure(self) -> None:
        self.failure_count += 1
        self.total_count += 1
        self.last_failure_time = time.time()
        self._check_and_open()

    def record_success(self) -> None:
        self.success_count += 1
        self.total_count += 1
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.half_open_calls += 1
            if self.half_open_calls >= self.half_open_max_calls:
                self._close()

    def _check_and_open(self) -> None:
        if self.state != CircuitBreakerState.CLOSED:
            return
        if self.total_count < 10:
            return
        rate = self.failure_count / self.total_count
        if rate >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            self.last_failure_time = time.time()

    def allow_request(self) -> bool:
        if self.state == CircuitBreakerState.CLOSED:
            return True
        if self.state == CircuitBreakerState.OPEN:
            if time.time() - (self.last_failure_time or 0) >= self.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                self.failure_count = 0
                self.success_count = 0
                self.total_count = 0
                return True
            return False
        if self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_calls < self.half_open_max_calls
        return False

    def _close(self) -> None:
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.total_count = 0
        self.half_open_calls = 0


@dataclass
class ExecutionMetrics:
    avg_duration: float = 0.0
    success_rate: float = 0.0
    total_executions: int = 0
    min_duration: float = float("inf")
    max_duration: float = 0.0


class ServerGrouper:
    @staticmethod
    def group_by_tags(servers: List[ServerConfig]) -> List[ServerGroup]:
        tag_groups: Dict[str, List[ServerConfig]] = defaultdict(list)
        for server in servers:
            for tag in server.tags:
                tag_groups[tag].append(server)

        groups = []
        for tag, servers_in_group in tag_groups.items():
            groups.append(ServerGroup(
                group_id=f"tag-{tag}",
                name=f"标签: {tag}",
                servers=servers_in_group,
                tags={tag},
            ))
        return groups

    @staticmethod
    def group_by_network(servers: List[ServerConfig]) -> List[ServerGroup]:
        network_groups: Dict[str, List[ServerConfig]] = defaultdict(list)
        for server in servers:
            try:
                ip = ipaddress.ip_address(server.host)
                if isinstance(ip, ipaddress.IPv4Address):
                    subnet = f"{ip.network_address}/24"
                else:
                    subnet = f"{ip.network_address}/64"
                network_groups[subnet].append(server)
            except ValueError:
                network_groups["unknown"].append(server)

        groups = []
        for subnet, servers_in_group in network_groups.items():
            groups.append(ServerGroup(
                group_id=f"net-{subnet.replace('/', '-')}",
                name=f"网络: {subnet}",
                servers=servers_in_group,
                network_zone=subnet,
            ))
        return groups

    @staticmethod
    def group_by_location(servers: List[ServerConfig]) -> List[ServerGroup]:
        location_groups: Dict[str, List[ServerConfig]] = defaultdict(list)
        for server in servers:
            location = "unknown"
            for tag in server.tags:
                if tag.startswith("loc-") or tag.startswith("region-") or tag.startswith("zone-"):
                    location = tag
                    break
            location_groups[location].append(server)

        groups = []
        for loc, servers_in_group in location_groups.items():
            groups.append(ServerGroup(
                group_id=f"loc-{loc}",
                name=f"位置: {loc}",
                servers=servers_in_group,
                location=loc,
            ))
        return groups

    @staticmethod
    def group_auto(
        servers: List[ServerConfig],
        max_batch_size: int = 50,
    ) -> List[ServerGroup]:
        network_groups = ServerGrouper.group_by_network(servers)
        all_groups: List[ServerGroup] = []

        for net_group in network_groups:
            tag_servers: Dict[str, List[ServerConfig]] = defaultdict(list)
            for server in net_group.servers:
                key = ",".join(sorted(server.tags)) if server.tags else "default"
                tag_servers[key].append(server)

            for tag_key, tag_server_list in tag_servers.items():
                if len(tag_server_list) <= max_batch_size:
                    all_groups.append(ServerGroup(
                        group_id=f"auto-{net_group.group_id}-{tag_key}",
                        name=f"{net_group.name} / {tag_key}",
                        servers=tag_server_list,
                        tags=set(tag_key.split(",")),
                        network_zone=net_group.network_zone,
                    ))
                else:
                    for i in range(0, len(tag_server_list), max_batch_size):
                        chunk = tag_server_list[i:i + max_batch_size]
                        all_groups.append(ServerGroup(
                            group_id=f"auto-{net_group.group_id}-{tag_key}-{i // max_batch_size}",
                            name=f"{net_group.name} / {tag_key} ({i // max_batch_size + 1})",
                            servers=chunk,
                            tags=set(tag_key.split(",")),
                            network_zone=net_group.network_zone,
                        ))

        return all_groups


@dataclass
class JobExecutionPlan:
    job_id: str
    name: str
    batches: List[Batch]
    total_tasks: int
    total_batches: int
    grouping_strategy: str
    created_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "total_tasks": self.total_tasks,
            "total_batches": self.total_batches,
            "grouping_strategy": self.grouping_strategy,
            "created_at": self.created_at.isoformat(),
            "batches": [batch.to_dict() for batch in self.batches],
        }


class ConcurrentController:
    def __init__(
        self,
        initial_concurrency: int = 20,
        min_concurrency: int = 2,
        max_concurrency: int = 100,
        target_error_rate: float = 0.05,
        adjust_interval: float = 10.0,
    ) -> None:
        self.current_concurrency = initial_concurrency
        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.target_error_rate = target_error_rate
        self.adjust_interval = adjust_interval
        self._semaphore = threading.Semaphore(initial_concurrency)
        self._lock = threading.Lock()
        self._last_adjust_time = time.time()
        self._recent_errors: Deque[bool] = deque(maxlen=100)
        self._recent_durations: Deque[float] = deque(maxlen=100)

    def acquire(self) -> None:
        self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()

    def record_result(self, success: bool, duration: float) -> None:
        with self._lock:
            self._recent_errors.append(not success)
            self._recent_durations.append(duration)
            self._maybe_adjust()

    def _maybe_adjust(self) -> None:
        now = time.time()
        if now - self._last_adjust_time < self.adjust_interval:
            return

        if len(self._recent_errors) < 20:
            return

        error_rate = sum(self._recent_errors) / len(self._recent_errors)
        avg_duration = sum(self._recent_durations) / len(self._recent_durations) if self._recent_durations else 0

        new_concurrency = self.current_concurrency

        if error_rate > self.target_error_rate * 2:
            new_concurrency = max(self.min_concurrency, int(self.current_concurrency * 0.5))
        elif error_rate > self.target_error_rate:
            new_concurrency = max(self.min_concurrency, int(self.current_concurrency * 0.8))
        elif error_rate < self.target_error_rate * 0.5 and avg_duration < 5:
            new_concurrency = min(self.max_concurrency, int(self.current_concurrency * 1.2))
        elif error_rate < self.target_error_rate * 0.2 and avg_duration < 2:
            new_concurrency = min(self.max_concurrency, int(self.current_concurrency * 1.5))

        new_concurrency = max(self.min_concurrency, min(self.max_concurrency, new_concurrency))

        if new_concurrency != self.current_concurrency:
            diff = new_concurrency - self.current_concurrency
            if diff > 0:
                for _ in range(diff):
                    self._semaphore.release()
            elif diff < 0:
                for _ in range(-diff):
                    self._semaphore.acquire()
            self.current_concurrency = new_concurrency

        self._last_adjust_time = now

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            error_count = sum(self._recent_errors)
            total = len(self._recent_errors)
            return {
                "current_concurrency": self.current_concurrency,
                "min_concurrency": self.min_concurrency,
                "max_concurrency": self.max_concurrency,
                "recent_error_rate": error_count / total if total else 0.0,
                "recent_avg_duration": sum(self._recent_durations) / len(self._recent_durations) if self._recent_durations else 0.0,
            }


class HistoryDataStore:
    def __init__(self, data_file: Optional[str] = None) -> None:
        self.data_file = data_file or os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "execution_history.json"
        )
        self._data: Dict[str, ExecutionMetrics] = defaultdict(ExecutionMetrics)
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            if os.path.exists(self.data_file):
                with open(self.data_file, "r", encoding="utf-8") as f:
                    raw = json.load(f)
                    for server_id, metrics in raw.items():
                        self._data[server_id] = ExecutionMetrics(**metrics)
        except Exception:
            pass

    def _save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump(
                    {k: v.__dict__ for k, v in self._data.items()},
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
        except Exception:
            pass

    def record_execution(
        self,
        server_id: str,
        duration: float,
        success: bool,
    ) -> None:
        with self._lock:
            metrics = self._data[server_id]
            metrics.total_executions += 1

            if metrics.total_executions == 1:
                metrics.avg_duration = duration
                metrics.min_duration = duration
                metrics.max_duration = duration
            else:
                metrics.avg_duration = (
                    metrics.avg_duration * (metrics.total_executions - 1) + duration
                ) / metrics.total_executions
                metrics.min_duration = min(metrics.min_duration, duration)
                metrics.max_duration = max(metrics.max_duration, duration)

            metrics.success_rate = (
                metrics.success_rate * (metrics.total_executions - 1) + (1.0 if success else 0.0)
            ) / metrics.total_executions

            if metrics.total_executions % 10 == 0:
                self._save()

    def get_estimated_duration(self, server_id: str) -> float:
        with self._lock:
            return self._data[server_id].avg_duration

    def get_server_score(self, server_id: str) -> float:
        with self._lock:
            metrics = self._data[server_id]
            if metrics.total_executions == 0:
                return 1.0
            duration_score = 1.0 / (1.0 + metrics.avg_duration / 10.0)
            success_score = metrics.success_rate
            return duration_score * 0.6 + success_score * 0.4

    def get_all_metrics(self) -> Dict[str, ExecutionMetrics]:
        with self._lock:
            return dict(self._data)


class OutputOrderPreserver:
    def __init__(self) -> None:
        self._buffers: Dict[str, List[Tuple[int, bytes]]] = defaultdict(list)
        self._next_expected: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def add_chunk(self, job_id: str, sequence: int, content: bytes) -> List[bytes]:
        with self._lock:
            heapq.heappush(self._buffers[job_id], (sequence, content))

            output: List[bytes] = []
            expected = self._next_expected[job_id]

            while self._buffers[job_id] and self._buffers[job_id][0][0] == expected:
                _, content = heapq.heappop(self._buffers[job_id])
                output.append(content)
                expected += 1

            self._next_expected[job_id] = expected
            return output

    def cleanup(self, job_id: str) -> None:
        with self._lock:
            self._buffers.pop(job_id, None)
            self._next_expected.pop(job_id, None)


@dataclass
class JobProgress:
    job_id: str
    name: str
    status: str
    total_tasks: int
    completed_tasks: int
    running_tasks: int
    pending_tasks: int
    failed_tasks: int
    success_tasks: int
    batches: List[Dict[str, Any]]
    resource_usage: Dict[str, Any]
    start_time: Optional[datetime]
    estimated_finish_time: Optional[datetime]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "status": self.status,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "running_tasks": self.running_tasks,
            "pending_tasks": self.pending_tasks,
            "failed_tasks": self.failed_tasks,
            "success_tasks": self.success_tasks,
            "progress_pct": (self.completed_tasks / self.total_tasks * 100) if self.total_tasks else 0,
            "batches": self.batches,
            "resource_usage": self.resource_usage,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "estimated_finish_time": self.estimated_finish_time.isoformat() if self.estimated_finish_time else None,
        }


class ParallelExecutionEngine:
    def __init__(
        self,
        max_workers: int = 100,
        max_batch_size: int = 50,
        failure_threshold: float = 0.5,
    ) -> None:
        self.max_workers = max_workers
        self.max_batch_size = max_batch_size
        self.failure_threshold = failure_threshold

        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._controller = ConcurrentController(
            initial_concurrency=settings.max_concurrent_tasks,
            min_concurrency=2,
            max_concurrency=max_workers,
        )
        self._history = HistoryDataStore()
        self._order_preserver = OutputOrderPreserver()

        self._circuit_breakers: Dict[str, CircuitBreaker] = defaultdict(
            lambda: CircuitBreaker(failure_threshold=failure_threshold)
        )

        self._jobs: Dict[str, JobExecutionPlan] = {}
        self._tasks: Dict[str, EngineTask] = {}
        self._batches: Dict[str, Batch] = {}
        self._job_tasks: Dict[str, List[EngineTask]] = defaultdict(list)

        self._task_queue: List[EngineTask] = []
        self._queue_lock = threading.Lock()
        self._queue_cv = threading.Condition(self._queue_lock)

        self._shutdown = threading.Event()
        self._graceful_shutdown = threading.Event()
        self._active_tasks: Set[str] = set()
        self._active_lock = threading.Lock()

        self._output_callbacks: List[Callable[[str, str, str, str, str], None]] = []
        self._status_callbacks: List[Callable[[str, Dict[str, Any]], None]] = []

        self._worker_thread: Optional[threading.Thread] = None
        self._start_worker()

    def _start_worker(self) -> None:
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name="engine-worker",
        )
        self._worker_thread.start()

    def register_output_callback(
        self,
        callback: Callable[[str, str, str, str, str], None],
    ) -> None:
        self._output_callbacks.append(callback)

    def register_status_callback(
        self,
        callback: Callable[[str, Dict[str, Any]], None],
    ) -> None:
        self._status_callbacks.append(callback)

    def create_job_plan(
        self,
        servers: List[ServerConfig],
        name: str = "",
        grouping_strategy: str = "auto",
    ) -> JobExecutionPlan:
        job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"

        if grouping_strategy == "tags":
            groups = ServerGrouper.group_by_tags(servers)
        elif grouping_strategy == "network":
            groups = ServerGrouper.group_by_network(servers)
        elif grouping_strategy == "location":
            groups = ServerGrouper.group_by_location(servers)
        else:
            groups = ServerGrouper.group_auto(servers, self.max_batch_size)

        batches: List[Batch] = []
        total_tasks = 0

        for group in groups:
            batch_id = f"{job_id}-batch-{len(batches)}"
            tasks: List[EngineTask] = []
            for i, server in enumerate(group.servers):
                task_id = f"{batch_id}-task-{i}"
                tasks.append(EngineTask(
                    task_id=task_id,
                    job_id=job_id,
                    server=server,
                    task_type="",
                    sequence=i,
                    estimated_duration=self._history.get_estimated_duration(server.id),
                    batch_id=batch_id,
                ))
                total_tasks += 1

            batch = Batch(
                batch_id=batch_id,
                group_id=group.group_id,
                group_name=group.name,
                tasks=tasks,
            )
            batches.append(batch)

        plan = JobExecutionPlan(
            job_id=job_id,
            name=name or f"任务-{job_id}",
            batches=batches,
            total_tasks=total_tasks,
            total_batches=len(batches),
            grouping_strategy=grouping_strategy,
            created_at=datetime.now(),
        )

        self._jobs[job_id] = plan
        for batch in batches:
            self._batches[batch.batch_id] = batch
            for task in batch.tasks:
                self._tasks[task.task_id] = task
                self._job_tasks[job_id].append(task)

        return plan

    def execute_commands(
        self,
        servers: List[ServerConfig],
        command: str,
        name: str = "",
        timeout: int = 300,
        env: Optional[Dict[str, str]] = None,
        grouping_strategy: str = "auto",
        max_retries: int = 3,
        order_by: str = "server_id",
    ) -> JobExecutionPlan:
        env = env or {}
        plan = self.create_job_plan(servers, name, grouping_strategy)

        for task in self._job_tasks[plan.job_id]:
            task.task_type = "command"
            task.command = command
            task.timeout = timeout
            task.env = env
            task.max_retries = max_retries

        self._sort_tasks(self._job_tasks[plan.job_id], order_by)
        self._enqueue_tasks(self._job_tasks[plan.job_id])
        self._notify_status(plan.job_id)

        return plan

    def execute_scripts(
        self,
        servers: List[ServerConfig],
        script_content: str,
        script_name: str = "script.sh",
        interpreter: str = "bash",
        args: Optional[List[str]] = None,
        name: str = "",
        timeout: int = 300,
        grouping_strategy: str = "auto",
        max_retries: int = 3,
        order_by: str = "server_id",
    ) -> JobExecutionPlan:
        args = args or []
        plan = self.create_job_plan(servers, name, grouping_strategy)

        for task in self._job_tasks[plan.job_id]:
            task.task_type = "script"
            task.script_content = script_content
            task.script_name = script_name
            task.interpreter = interpreter
            task.args = args
            task.timeout = timeout
            task.max_retries = max_retries

        self._sort_tasks(self._job_tasks[plan.job_id], order_by)
        self._enqueue_tasks(self._job_tasks[plan.job_id])
        self._notify_status(plan.job_id)

        return plan

    def _sort_tasks(self, tasks: List[EngineTask], order_by: str) -> None:
        if order_by == "server_id":
            tasks.sort(key=lambda t: t.server.id)
        elif order_by == "sequence":
            tasks.sort(key=lambda t: t.sequence)
        elif order_by == "performance":
            tasks.sort(key=lambda t: self._history.get_server_score(t.server.id), reverse=True)
        elif order_by == "duration":
            tasks.sort(key=lambda t: t.estimated_duration)

        for i, task in enumerate(tasks):
            task.sequence = i

    def _enqueue_tasks(self, tasks: List[EngineTask]) -> None:
        with self._queue_cv:
            for task in tasks:
                task.status = TaskStatus.QUEUED
                heapq.heappush(self._task_queue, task)
            self._queue_cv.notify_all()

    def _worker_loop(self) -> None:
        while not self._shutdown.is_set():
            if self._graceful_shutdown.is_set() and not self._task_queue:
                break

            with self._queue_cv:
                while not self._task_queue:
                    if self._graceful_shutdown.is_set() or self._shutdown.is_set():
                        break
                    self._queue_cv.wait(timeout=1)
                if not self._task_queue:
                    continue

                task = heapq.heappop(self._task_queue)

            if task.next_retry_at and time.time() < task.next_retry_at:
                time.sleep(min(0.1, task.next_retry_at - time.time()))
                with self._queue_cv:
                    heapq.heappush(self._task_queue, task)
                continue

            cb = self._circuit_breakers[task.batch_id or task.job_id]
            if not cb.allow_request():
                task.status = TaskStatus.CANCELLED
                task.result.status = TaskStatus.CANCELLED
                task.result.error_message = "Circuit breaker open"
                self._complete_task(task)
                continue

            try:
                self._controller.acquire()
            except Exception:
                break

            if self._graceful_shutdown.is_set():
                self._controller.release()
                break

            with self._active_lock:
                self._active_tasks.add(task.task_id)

            self._executor.submit(self._execute_task, task)

    def _execute_task(self, task: EngineTask) -> None:
        try:
            task.status = TaskStatus.RUNNING
            task.result.start_time = datetime.now()
            task.result.status = TaskStatus.RUNNING

            self._notify_output(
                task.job_id,
                task.server.id,
                task.server.name,
                "stderr",
                f"[INFO] 开始执行 (重试次数: {task.retry_count})\n",
            )
            self._notify_status(task.job_id)

            def stream_cb(stream: str, content: str) -> None:
                task.result.stdout += content if stream == "stdout" else ""
                task.result.stderr += content if stream == "stderr" else ""
                self._notify_output(task.job_id, task.server.id, task.server.name, stream, content)

            start_time = time.time()
            success = False

            try:
                if task.task_type == "command":
                    exit_code, stdout, stderr = ssh_pool.execute_command(
                        server=task.server,
                        command=task.command or "",
                        timeout=task.timeout,
                        env=task.env,
                        stream_callback=stream_cb,
                    )
                    task.result.exit_code = exit_code
                    task.result.stdout += stdout
                    task.result.stderr += stderr

                    if exit_code == 0:
                        task.status = TaskStatus.SUCCESS
                        task.result.status = TaskStatus.SUCCESS
                        success = True
                    else:
                        task.status = TaskStatus.FAILED
                        task.result.status = TaskStatus.FAILED
                        task.result.error_message = f"Exit code: {exit_code}"

                elif task.task_type == "script":
                    plan = script_executor.plan_execution(
                        server=task.server,
                        script_content=task.script_content or "",
                        script_name=task.script_name or "script.sh",
                        interpreter=task.interpreter,
                        args=task.args,
                        task_id=task.task_id,
                    )
                    stream_cb("stderr", f"[exec-mode={plan.mode}] " + " ".join(plan.notes[-2:]) + "\n")

                    exit_code, stdout, stderr = script_executor.execute(
                        server=task.server,
                        plan=plan,
                        script_content=task.script_content,
                        timeout=task.timeout,
                        stream_callback=stream_cb,
                    )

                    task.result.exit_code = exit_code
                    task.result.stdout += stdout
                    task.result.stderr += stderr

                    if exit_code == 0:
                        task.status = TaskStatus.SUCCESS
                        task.result.status = TaskStatus.SUCCESS
                        success = True
                    else:
                        task.status = TaskStatus.FAILED
                        task.result.status = TaskStatus.FAILED
                        task.result.error_message = f"Exit code: {exit_code}"

            except Exception as e:
                task.status = TaskStatus.ERROR
                task.result.status = TaskStatus.ERROR
                task.result.error_message = f"{type(e).__name__}: {str(e)}"
                stream_cb("stderr", f"\n[ERROR] {type(e).__name__}: {str(e)}\n")

            duration = time.time() - start_time
            task.result.duration = duration
            task.result.end_time = datetime.now()

            if not success and task.retry_count < task.max_retries:
                task.retry_count += 1
                task.result.retry_count = task.retry_count
                task.status = TaskStatus.RETRYING
                task.result.status = TaskStatus.RETRYING

                delay = min(60, 2 ** task.retry_count + 0.1)
                task.next_retry_at = time.time() + delay

                self._notify_output(
                    task.job_id,
                    task.server.id,
                    task.server.name,
                    "stderr",
                    f"[RETRY] 第 {task.retry_count} 次重试，等待 {delay:.1f} 秒...\n",
                )

                with self._queue_cv:
                    heapq.heappush(self._task_queue, task)
                    self._queue_cv.notify_all()

                self._controller.record_result(False, duration)
                self._history.record_execution(task.server.id, duration, False)
                return

            self._history.record_execution(task.server.id, duration, success)
            self._controller.record_result(success, duration)

            cb = self._circuit_breakers[task.batch_id or task.job_id]
            if success:
                cb.record_success()
            else:
                cb.record_failure()

            self._complete_task(task)

        finally:
            with self._active_lock:
                self._active_tasks.discard(task.task_id)
            self._controller.release()

    def _complete_task(self, task: EngineTask) -> None:
        batch = self._batches.get(task.batch_id) if task.batch_id else None
        if batch:
            if task.status == TaskStatus.SUCCESS:
                batch.success_count += 1
            elif task.status == TaskStatus.FAILED:
                batch.failed_count += 1
            elif task.status == TaskStatus.ERROR:
                batch.error_count += 1

            if batch.failure_rate >= self.failure_threshold:
                batch.status = BatchStatus.PAUSED
                self._notify_output(
                    task.job_id,
                    "",
                    "",
                    "stderr",
                    f"\n[ALERT] 批次 {batch.batch_id} 失败率 {batch.failure_rate:.2%} 超过阈值 {self.failure_threshold:.0%}，已暂停！\n",
                )

                with self._queue_cv:
                    self._task_queue = [
                        t for t in self._task_queue if t.batch_id != batch.batch_id
                    ]
                    heapq.heapify(self._task_queue)

        self._notify_status(task.job_id)

    def _notify_output(
        self,
        job_id: str,
        server_id: str,
        server_name: str,
        stream: str,
        content: str,
    ) -> None:
        for cb in self._output_callbacks:
            try:
                cb(job_id, server_id, server_name, stream, content)
            except Exception:
                pass

    def _notify_status(self, job_id: str) -> None:
        progress = self.get_job_progress(job_id)
        for cb in self._status_callbacks:
            try:
                cb(job_id, progress.to_dict())
            except Exception:
                pass

    def get_job_progress(self, job_id: str) -> JobProgress:
        plan = self._jobs.get(job_id)
        if not plan:
            raise ValueError(f"Job not found: {job_id}")

        tasks = self._job_tasks[job_id]
        completed = 0
        running = 0
        pending = 0
        failed = 0
        success = 0

        for task in tasks:
            if task.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.ERROR, TaskStatus.CANCELLED):
                completed += 1
                if task.status == TaskStatus.SUCCESS:
                    success += 1
                else:
                    failed += 1
            elif task.status in (TaskStatus.RUNNING, TaskStatus.RETRYING):
                running += 1
            else:
                pending += 1

        start_time = None
        if tasks:
            running_tasks = [t for t in tasks if t.result.start_time]
            if running_tasks:
                start_time = min(t.result.start_time for t in running_tasks)

        estimated_finish = None
        if start_time and completed > 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            remaining = len(tasks) - completed
            avg_time = elapsed / completed
            estimated_finish = datetime.now() + timedelta(seconds=remaining * avg_time)

        return JobProgress(
            job_id=job_id,
            name=plan.name,
            status="running" if running > 0 or pending > 0 else "completed",
            total_tasks=len(tasks),
            completed_tasks=completed,
            running_tasks=running,
            pending_tasks=pending,
            failed_tasks=failed,
            success_tasks=success,
            batches=[batch.to_dict() for batch in plan.batches],
            resource_usage={
                "concurrency": self._controller.current_concurrency,
                "active_connections": ssh_pool._total_connections if hasattr(ssh_pool, "_total_connections") else 0,
                "controller_stats": self._controller.get_stats(),
            },
            start_time=start_time,
            estimated_finish_time=estimated_finish,
        )

    def get_job_results(self, job_id: str) -> List[TaskResult]:
        tasks = self._job_tasks.get(job_id, [])
        return [t.result for t in tasks]

    def get_ordered_output(
        self,
        job_id: str,
        order_by: str = "server_id",
    ) -> List[Tuple[str, TaskResult]]:
        results = self.get_job_results(job_id)
        tasks = self._job_tasks.get(job_id, [])

        result_map = {r.task_id: r for r in results}
        ordered_tasks = sorted(
            tasks,
            key=lambda t: t.server.id if order_by == "server_id" else t.sequence,
        )

        return [(task.task_id, result_map[task.task_id]) for task in ordered_tasks]

    def resume_batch(self, batch_id: str) -> bool:
        batch = self._batches.get(batch_id)
        if not batch:
            return False

        if batch.status != BatchStatus.PAUSED:
            return False

        cb = self._circuit_breakers.get(batch_id)
        if cb:
            cb._close()

        batch.status = BatchStatus.RUNNING
        paused_tasks = [t for t in batch.tasks if t.status in (TaskStatus.PENDING, TaskStatus.QUEUED)]

        with self._queue_cv:
            for task in paused_tasks:
                heapq.heappush(self._task_queue, task)
            self._queue_cv.notify_all()

        return True

    def shutdown_graceful(self, wait: bool = True) -> None:
        self._graceful_shutdown.set()
        with self._queue_cv:
            self._queue_cv.notify_all()

        if wait:
            while True:
                with self._active_lock:
                    if not self._active_tasks:
                        break
                time.sleep(0.5)

        self._executor.shutdown(wait=wait)
        self._shutdown.set()

    def shutdown_force(self) -> None:
        self._shutdown.set()
        self._graceful_shutdown.set()
        with self._queue_cv:
            self._queue_cv.notify_all()
        self._executor.shutdown(wait=False, cancel_futures=True)

    def get_all_jobs(self) -> List[JobExecutionPlan]:
        return list(self._jobs.values())

    def get_server_heatmap(self) -> Dict[str, Dict[str, Any]]:
        metrics = self._history.get_all_metrics()
        heatmap: Dict[str, Dict[str, Any]] = {}

        for server in settings.servers:
            m = metrics.get(server.id, ExecutionMetrics())
            heatmap[server.id] = {
                "server_id": server.id,
                "server_name": server.name,
                "host": server.host,
                "tags": server.tags,
                "avg_duration": m.avg_duration,
                "success_rate": m.success_rate,
                "total_executions": m.total_executions,
                "performance_score": self._history.get_server_score(server.id),
            }

        return heatmap


engine = ParallelExecutionEngine()
