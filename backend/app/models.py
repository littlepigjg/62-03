from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class CommandExecuteRequest(BaseModel):
    server_ids: List[str]
    command: str
    timeout: int = 300
    env: Dict[str, str] = Field(default_factory=dict)


class ScriptExecuteRequest(BaseModel):
    server_ids: List[str]
    script_content: str
    script_name: Optional[str] = "script.sh"
    interpreter: str = "bash"
    args: List[str] = Field(default_factory=list)
    timeout: int = 300


class EngineExecuteRequest(BaseModel):
    server_ids: List[str]
    command: Optional[str] = None
    script_content: Optional[str] = None
    script_name: Optional[str] = "script.sh"
    interpreter: str = "bash"
    args: List[str] = Field(default_factory=list)
    timeout: int = 300
    env: Dict[str, str] = Field(default_factory=dict)
    name: str = ""
    grouping_strategy: str = "auto"
    max_retries: int = 3
    max_batch_size: int = 50
    order_by: str = "server_id"


class BatchResumeRequest(BaseModel):
    batch_id: str


class JobProgressResponse(BaseModel):
    job_id: str
    name: str
    status: str
    total_tasks: int
    completed_tasks: int
    running_tasks: int
    pending_tasks: int
    failed_tasks: int
    success_tasks: int
    progress_pct: float
    batches: List[Dict[str, Any]]
    resource_usage: Dict[str, Any]
    start_time: Optional[str]
    estimated_finish_time: Optional[str]


class TaskResultResponse(BaseModel):
    task_id: str
    server_id: str
    server_name: str
    exit_code: Optional[int]
    stdout: str
    stderr: str
    start_time: Optional[str]
    end_time: Optional[str]
    status: str
    retry_count: int
    duration: float
    error_message: Optional[str]


class ServerHeatmapResponse(BaseModel):
    server_id: str
    server_name: str
    host: str
    tags: List[str]
    avg_duration: float
    success_rate: float
    total_executions: int
    performance_score: float


class JobPlanResponse(BaseModel):
    job_id: str
    name: str
    total_tasks: int
    total_batches: int
    grouping_strategy: str
    created_at: str
    batches: List[Dict[str, Any]]


class TemplateCreateRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    script_content: str
    interpreter: str = "bash"
    tags: List[str] = Field(default_factory=list)


class TemplateUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    script_content: Optional[str] = None
    interpreter: Optional[str] = None
    tags: Optional[List[str]] = None


class ExecutionOutput(BaseModel):
    server_id: str
    server_name: str
    task_id: str
    stream: str
    content: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class ExecutionResult(BaseModel):
    task_id: str
    server_id: str
    server_name: str
    command: str
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    start_time: str
    end_time: Optional[str] = None
    status: str = "running"


class LogEntry(BaseModel):
    task_id: str
    server_id: str
    server_name: str
    command: str
    script_name: Optional[str] = None
    exit_code: Optional[int] = None
    start_time: str
    end_time: Optional[str] = None
    status: str
    output: str = ""
