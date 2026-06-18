import os
import sys
import time
import unittest
import tempfile
import json
from datetime import datetime
from unittest.mock import patch, MagicMock, PropertyMock
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import ServerConfig
from app.core.parallel_engine import (
    ServerGrouper,
    CircuitBreaker,
    CircuitBreakerState,
    ConcurrentController,
    HistoryDataStore,
    OutputOrderPreserver,
    TaskStatus,
    BatchStatus,
    ServerGroup,
    EngineTask,
    TaskResult,
    Batch,
    ExecutionMetrics,
)


TEST_SERVERS = [
    ServerConfig(
        id="server-01",
        name="生产服务器-01",
        host="192.168.1.101",
        port=22,
        username="root",
        password="",
        private_key="",
        tags=["production", "web", "loc-beijing"],
    ),
    ServerConfig(
        id="server-02",
        name="生产服务器-02",
        host="192.168.1.102",
        port=22,
        username="root",
        password="",
        private_key="",
        tags=["production", "database", "loc-beijing"],
    ),
    ServerConfig(
        id="server-03",
        name="生产服务器-03",
        host="192.168.2.101",
        port=22,
        username="root",
        password="",
        private_key="",
        tags=["production", "web", "loc-shanghai"],
    ),
    ServerConfig(
        id="server-04",
        name="测试服务器-01",
        host="10.0.0.101",
        port=22,
        username="deploy",
        password="",
        private_key="",
        tags=["test", "web", "loc-beijing"],
    ),
    ServerConfig(
        id="server-05",
        name="测试服务器-02",
        host="10.0.0.102",
        port=22,
        username="deploy",
        password="",
        private_key="",
        tags=["test", "database", "loc-shanghai"],
    ),
]


class TestServerGrouper(unittest.TestCase):
    def test_group_by_tags(self):
        groups = ServerGrouper.group_by_tags(TEST_SERVERS)
        group_names = {g.name for g in groups}

        self.assertIn("标签: production", group_names)
        self.assertIn("标签: test", group_names)
        self.assertIn("标签: web", group_names)
        self.assertIn("标签: database", group_names)

        production_group = next(g for g in groups if g.name == "标签: production")
        self.assertEqual(len(production_group.servers), 3)

    def test_group_by_network(self):
        groups = ServerGrouper.group_by_network(TEST_SERVERS)
        group_ids = {g.group_id for g in groups}

        self.assertIn("net-192.168.1.0-24", group_ids)
        self.assertIn("net-192.168.2.0-24", group_ids)
        self.assertIn("net-10.0.0.0-24", group_ids)

        group_192 = next(g for g in groups if g.group_id == "net-192.168.1.0-24")
        self.assertEqual(len(group_192.servers), 2)

    def test_group_by_location(self):
        groups = ServerGrouper.group_by_location(TEST_SERVERS)
        group_names = {g.name for g in groups}

        self.assertIn("位置: loc-beijing", group_names)
        self.assertIn("位置: loc-shanghai", group_names)

        beijing_group = next(g for g in groups if g.name == "位置: loc-beijing")
        self.assertEqual(len(beijing_group.servers), 3)

    def test_group_auto(self):
        groups = ServerGrouper.group_auto(TEST_SERVERS, max_batch_size=10)
        self.assertGreater(len(groups), 0)

        for group in groups:
            self.assertLessEqual(len(group.servers), 10)
            self.assertIsNotNone(group.network_zone)

    def test_group_auto_large_batch(self):
        many_servers = []
        for i in range(100):
            many_servers.append(ServerConfig(
                id=f"server-{i:03d}",
                name=f"服务器-{i}",
                host=f"192.168.1.{i + 10}",
                port=22,
                username="root",
                password="",
                private_key="",
                tags=["web"],
            ))

        groups = ServerGrouper.group_auto(many_servers, max_batch_size=30)
        total_servers = sum(len(g.servers) for g in groups)
        self.assertEqual(total_servers, 100)

        for group in groups:
            self.assertLessEqual(len(group.servers), 30)

    def test_group_network_with_invalid_ip(self):
        servers_with_hostname = [
            ServerConfig(
                id="server-hostname",
                name="主机名服务器",
                host="example.com",
                port=22,
                username="root",
                password="",
                private_key="",
                tags=[],
            ),
        ]
        groups = ServerGrouper.group_by_network(servers_with_hostname)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].group_id, "net-unknown")


class TestCircuitBreaker(unittest.TestCase):
    def test_initial_state_is_closed(self):
        cb = CircuitBreaker("test-group")
        self.assertEqual(cb.state, CircuitBreakerState.CLOSED)
        self.assertTrue(cb.allow_request())

    def test_opens_after_failure_threshold(self):
        cb = CircuitBreaker("test-group", failure_threshold=0.5, half_open_max_calls=1)

        for i in range(10):
            if i < 5:
                cb.record_success()
            else:
                cb.record_failure()

        self.assertEqual(cb.state, CircuitBreakerState.OPEN)
        self.assertFalse(cb.allow_request())

    def test_half_open_after_recovery_timeout(self):
        cb = CircuitBreaker("test-group", failure_threshold=0.5, recovery_timeout=0.1)

        for i in range(10):
            cb.record_failure()

        self.assertEqual(cb.state, CircuitBreakerState.OPEN)
        time.sleep(0.15)

        self.assertTrue(cb.allow_request())
        self.assertEqual(cb.state, CircuitBreakerState.HALF_OPEN)

    def test_closes_after_successful_half_open(self):
        cb = CircuitBreaker("test-group", failure_threshold=0.5, recovery_timeout=0.1, half_open_max_calls=2)

        for i in range(10):
            cb.record_failure()

        time.sleep(0.15)
        self.assertTrue(cb.allow_request())

        cb.record_success()
        cb.record_success()

        self.assertEqual(cb.state, CircuitBreakerState.CLOSED)

    def test_reopens_after_failure_in_half_open(self):
        cb = CircuitBreaker("test-group", failure_threshold=0.5, recovery_timeout=0.1)

        for i in range(10):
            cb.record_failure()

        time.sleep(0.15)
        self.assertTrue(cb.allow_request())

        cb.record_failure()
        self.assertEqual(cb.state, CircuitBreakerState.OPEN)
        self.assertFalse(cb.allow_request())

    def test_requires_minimum_samples_before_opening(self):
        cb = CircuitBreaker("test-group", failure_threshold=0.5)

        for i in range(5):
            cb.record_failure()

        self.assertEqual(cb.state, CircuitBreakerState.CLOSED)
        self.assertTrue(cb.allow_request())


class TestConcurrentController(unittest.TestCase):
    def test_initial_concurrency(self):
        controller = ConcurrentController(initial_concurrency=10, min_concurrency=2, max_concurrency=20)
        self.assertEqual(controller.current_concurrency, 10)

    def test_decreases_concurrency_on_high_error_rate(self):
        controller = ConcurrentController(
            initial_concurrency=20,
            min_concurrency=2,
            max_concurrency=30,
            target_error_rate=0.05,
            adjust_interval=0,
        )

        for i in range(30):
            controller.record_result(i < 20, 1.0)

        self.assertLess(controller.current_concurrency, 20)
        self.assertGreaterEqual(controller.current_concurrency, 2)

    def test_increases_concurrency_on_low_error_rate(self):
        controller = ConcurrentController(
            initial_concurrency=10,
            min_concurrency=2,
            max_concurrency=30,
            target_error_rate=0.05,
            adjust_interval=0,
        )

        for i in range(30):
            controller.record_result(True, 0.5)

        self.assertGreater(controller.current_concurrency, 10)
        self.assertLessEqual(controller.current_concurrency, 30)

    def test_respects_min_max_bounds(self):
        controller = ConcurrentController(
            initial_concurrency=10,
            min_concurrency=5,
            max_concurrency=15,
            target_error_rate=0.05,
            adjust_interval=0,
        )

        controller.current_concurrency = 100
        controller.record_result(False, 1.0)
        self.assertLessEqual(controller.current_concurrency, 15)

        controller.current_concurrency = 1
        controller.record_result(True, 0.1)
        self.assertGreaterEqual(controller.current_concurrency, 5)

    def test_acquire_release(self):
        controller = ConcurrentController(initial_concurrency=2)
        controller.acquire()
        controller.acquire()

        acquired = False

        def try_acquire():
            nonlocal acquired
            controller.acquire()
            acquired = True

        import threading
        t = threading.Thread(target=try_acquire, daemon=True)
        t.start()
        t.join(timeout=0.1)
        self.assertFalse(acquired)

        controller.release()
        t.join(timeout=0.1)
        self.assertTrue(acquired)
        controller.release()
        controller.release()

    def test_get_stats(self):
        controller = ConcurrentController(initial_concurrency=10)
        controller.record_result(True, 1.0)
        controller.record_result(False, 2.0)

        stats = controller.get_stats()
        self.assertIn("current_concurrency", stats)
        self.assertIn("recent_error_rate", stats)
        self.assertIn("recent_avg_duration", stats)
        self.assertEqual(stats["current_concurrency"], 10)


class TestHistoryDataStore(unittest.TestCase):
    def setUp(self):
        self.temp_file = tempfile.mktemp(suffix=".json")
        self.store = HistoryDataStore(data_file=self.temp_file)

    def tearDown(self):
        if os.path.exists(self.temp_file):
            os.remove(self.temp_file)

    def test_initial_metrics(self):
        metrics = self.store.get_all_metrics()
        self.assertEqual(len(metrics), 0)

        duration = self.store.get_estimated_duration("server-1")
        self.assertEqual(duration, 0.0)

        score = self.store.get_server_score("server-1")
        self.assertEqual(score, 1.0)

    def test_record_execution(self):
        self.store.record_execution("server-1", 5.0, True)
        self.store.record_execution("server-1", 3.0, True)
        self.store.record_execution("server-1", 7.0, False)

        avg_duration = self.store.get_estimated_duration("server-1")
        self.assertAlmostEqual(avg_duration, 5.0, places=1)

        metrics = self.store.get_all_metrics()
        self.assertEqual(metrics["server-1"].total_executions, 3)
        self.assertAlmostEqual(metrics["server-1"].success_rate, 2 / 3, places=2)
        self.assertEqual(metrics["server-1"].min_duration, 3.0)
        self.assertEqual(metrics["server-1"].max_duration, 7.0)

    def test_server_score_calculation(self):
        self.store.record_execution("fast-server", 1.0, True)
        self.store.record_execution("fast-server", 1.5, True)

        self.store.record_execution("slow-server", 10.0, True)
        self.store.record_execution("slow-server", 15.0, False)

        fast_score = self.store.get_server_score("fast-server")
        slow_score = self.store.get_server_score("slow-server")

        self.assertGreater(fast_score, slow_score)

    def test_persistence(self):
        self.store.record_execution("server-1", 5.0, True)
        self.store._save()

        new_store = HistoryDataStore(data_file=self.temp_file)
        avg_duration = new_store.get_estimated_duration("server-1")
        self.assertEqual(avg_duration, 5.0)

    def test_auto_save_on_interval(self):
        for i in range(15):
            self.store.record_execution("server-1", 1.0, True)

        self.assertTrue(os.path.exists(self.temp_file))


class TestOutputOrderPreserver(unittest.TestCase):
    def test_returns_chunks_in_order(self):
        preserver = OutputOrderPreserver()

        chunks = [
            (0, b"chunk 0"),
            (1, b"chunk 1"),
            (2, b"chunk 2"),
        ]

        output = []
        for seq, content in chunks:
            output.extend(preserver.add_chunk("job-1", seq, content))

        self.assertEqual(output, [b"chunk 0", b"chunk 1", b"chunk 2"])

    def test_buffers_out_of_order_chunks(self):
        preserver = OutputOrderPreserver()

        result1 = preserver.add_chunk("job-1", 1, b"chunk 1")
        self.assertEqual(result1, [])

        result2 = preserver.add_chunk("job-1", 0, b"chunk 0")
        self.assertEqual(result2, [b"chunk 0", b"chunk 1"])

        result3 = preserver.add_chunk("job-1", 2, b"chunk 2")
        self.assertEqual(result3, [b"chunk 2"])

    def test_handles_multiple_jobs(self):
        preserver = OutputOrderPreserver()

        preserver.add_chunk("job-1", 1, b"job1 chunk1")
        result = preserver.add_chunk("job-2", 0, b"job2 chunk0")
        self.assertEqual(result, [b"job2 chunk0"])

        result2 = preserver.add_chunk("job-1", 0, b"job1 chunk0")
        self.assertEqual(result2, [b"job1 chunk0", b"job1 chunk1"])

    def test_cleanup(self):
        preserver = OutputOrderPreserver()
        preserver.add_chunk("job-1", 0, b"chunk 0")
        preserver.cleanup("job-1")

        self.assertNotIn("job-1", preserver._buffers)
        self.assertNotIn("job-1", preserver._next_expected)


class TestEngineTask(unittest.TestCase):
    def test_task_creation(self):
        server = TEST_SERVERS[0]
        task = EngineTask(
            task_id="task-001",
            job_id="job-001",
            server=server,
            task_type="command",
            command="echo hello",
            sequence=0,
        )

        self.assertEqual(task.task_id, "task-001")
        self.assertEqual(task.job_id, "job-001")
        self.assertEqual(task.server.id, server.id)
        self.assertEqual(task.status, TaskStatus.PENDING)
        self.assertIsNotNone(task.result)
        self.assertEqual(task.result.server_id, server.id)

    def test_task_priority_ordering(self):
        server = TEST_SERVERS[0]

        task_high = EngineTask(
            task_id="task-high",
            job_id="job-1",
            server=server,
            task_type="command",
            priority=10,
            sequence=0,
        )

        task_low = EngineTask(
            task_id="task-low",
            job_id="job-1",
            server=server,
            task_type="command",
            priority=1,
            sequence=0,
        )

        self.assertLess(task_high, task_low)

    def test_task_duration_ordering(self):
        server = TEST_SERVERS[0]

        task_fast = EngineTask(
            task_id="task-fast",
            job_id="job-1",
            server=server,
            task_type="command",
            estimated_duration=1.0,
            sequence=0,
        )

        task_slow = EngineTask(
            task_id="task-slow",
            job_id="job-1",
            server=server,
            task_type="command",
            estimated_duration=5.0,
            sequence=0,
        )

        self.assertLess(task_fast, task_slow)

    def test_task_sequence_ordering(self):
        server = TEST_SERVERS[0]

        task_first = EngineTask(
            task_id="task-1",
            job_id="job-1",
            server=server,
            task_type="command",
            sequence=0,
        )

        task_second = EngineTask(
            task_id="task-2",
            job_id="job-1",
            server=server,
            task_type="command",
            sequence=1,
        )

        self.assertLess(task_first, task_second)


class TestBatch(unittest.TestCase):
    def test_batch_creation(self):
        server = TEST_SERVERS[0]
        task = EngineTask(
            task_id="task-001",
            job_id="job-001",
            server=server,
            task_type="command",
            sequence=0,
        )

        batch = Batch(
            batch_id="batch-001",
            group_id="group-001",
            group_name="测试组",
            tasks=[task],
        )

        self.assertEqual(batch.batch_id, "batch-001")
        self.assertEqual(batch.status, BatchStatus.PENDING)
        self.assertEqual(batch.total_count, 1)
        self.assertEqual(batch.failure_rate, 0.0)

    def test_batch_failure_rate(self):
        server = TEST_SERVERS[0]
        tasks = []
        for i in range(10):
            tasks.append(EngineTask(
                task_id=f"task-{i}",
                job_id="job-1",
                server=server,
                task_type="command",
                sequence=i,
            ))

        batch = Batch(
            batch_id="batch-001",
            group_id="group-001",
            group_name="测试组",
            tasks=tasks,
        )

        batch.success_count = 7
        batch.failed_count = 2
        batch.error_count = 1

        self.assertEqual(batch.failure_rate, 0.3)

    def test_batch_to_dict(self):
        server = TEST_SERVERS[0]
        task = EngineTask(
            task_id="task-001",
            job_id="job-001",
            server=server,
            task_type="command",
            sequence=0,
        )

        batch = Batch(
            batch_id="batch-001",
            group_id="group-001",
            group_name="测试组",
            tasks=[task],
            status=BatchStatus.RUNNING,
        )

        batch_dict = batch.to_dict()
        self.assertEqual(batch_dict["batch_id"], "batch-001")
        self.assertEqual(batch_dict["status"], "running")
        self.assertEqual(batch_dict["total_count"], 1)


class TestTaskResult(unittest.TestCase):
    def test_result_creation(self):
        result = TaskResult(
            task_id="task-001",
            server_id="server-01",
            server_name="测试服务器",
        )

        self.assertEqual(result.task_id, "task-001")
        self.assertEqual(result.status, TaskStatus.PENDING)

    def test_result_to_dict(self):
        now = datetime.now()
        result = TaskResult(
            task_id="task-001",
            server_id="server-01",
            server_name="测试服务器",
            exit_code=0,
            stdout="hello\n",
            stderr="",
            start_time=now,
            end_time=now,
            status=TaskStatus.SUCCESS,
            retry_count=0,
            duration=1.5,
        )

        result_dict = result.to_dict()
        self.assertEqual(result_dict["task_id"], "task-001")
        self.assertEqual(result_dict["exit_code"], 0)
        self.assertEqual(result_dict["status"], "success")
        self.assertIsNotNone(result_dict["start_time"])


class TestExecutionMetrics(unittest.TestCase):
    def test_default_values(self):
        metrics = ExecutionMetrics()
        self.assertEqual(metrics.avg_duration, 0.0)
        self.assertEqual(metrics.success_rate, 0.0)
        self.assertEqual(metrics.total_executions, 0)
        self.assertEqual(metrics.min_duration, float("inf"))
        self.assertEqual(metrics.max_duration, 0.0)


class TestServerGroup(unittest.TestCase):
    def test_server_group_creation(self):
        group = ServerGroup(
            group_id="group-001",
            name="测试组",
            servers=TEST_SERVERS[:2],
            tags={"production", "web"},
            location="loc-beijing",
            network_zone="192.168.1.0/24",
            priority=5,
        )

        self.assertEqual(group.group_id, "group-001")
        self.assertEqual(len(group.servers), 2)
        self.assertEqual(group.location, "loc-beijing")
        self.assertEqual(group.network_zone, "192.168.1.0/24")


class TestEnums(unittest.TestCase):
    def test_task_status_values(self):
        self.assertEqual(TaskStatus.PENDING, "pending")
        self.assertEqual(TaskStatus.RUNNING, "running")
        self.assertEqual(TaskStatus.SUCCESS, "success")
        self.assertEqual(TaskStatus.FAILED, "failed")
        self.assertEqual(TaskStatus.ERROR, "error")
        self.assertEqual(TaskStatus.CANCELLED, "cancelled")
        self.assertEqual(TaskStatus.RETRYING, "retrying")
        self.assertEqual(TaskStatus.QUEUED, "queued")

    def test_batch_status_values(self):
        self.assertEqual(BatchStatus.PENDING, "pending")
        self.assertEqual(BatchStatus.RUNNING, "running")
        self.assertEqual(BatchStatus.PAUSED, "paused")
        self.assertEqual(BatchStatus.COMPLETED, "completed")
        self.assertEqual(BatchStatus.FAILED, "failed")
        self.assertEqual(BatchStatus.CANCELLED, "cancelled")

    def test_circuit_breaker_state_values(self):
        self.assertEqual(CircuitBreakerState.CLOSED, "closed")
        self.assertEqual(CircuitBreakerState.OPEN, "open")
        self.assertEqual(CircuitBreakerState.HALF_OPEN, "half_open")


if __name__ == "__main__":
    unittest.main(verbosity=2)
