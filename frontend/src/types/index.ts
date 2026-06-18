export interface ServerConfig {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  password: string;
  private_key: string;
  tags: string[];
}

export interface ExecutionResult {
  task_id: string;
  server_id: string;
  server_name: string;
  command: string;
  exit_code: number | null;
  stdout: string;
  stderr: string;
  start_time: string;
  end_time: string | null;
  status: 'pending' | 'running' | 'success' | 'failed' | 'error';
}

export interface StreamMessage {
  type: 'output' | 'status';
  task_id: string;
  server_id: string;
  server_name: string;
  stream: 'stdout' | 'stderr' | '';
  content: string;
  exit_code: number | null;
  status: string;
  timestamp: string;
}

export interface ScriptTemplate {
  id: string;
  name: string;
  description: string;
  script_content: string;
  interpreter: string;
  tags: string[];
  created_at: string;
  updated_at: string;
}

export interface LogEntry {
  task_id: string;
  server_name: string;
  server_id: string;
  command: string;
  script_name: string | null;
  start_time: string;
  end_time: string;
  status: string;
  exit_code: number | null;
  output: string;
  log_file: string;
}

export interface CommandExecuteRequest {
  server_ids: string[];
  command: string;
  timeout?: number;
  env?: Record<string, string>;
}

export interface ScriptExecuteRequest {
  server_ids: string[];
  script_content: string;
  script_name?: string;
  interpreter?: string;
  args?: string[];
  timeout?: number;
}

export interface EngineExecuteRequest {
  server_ids: string[];
  command?: string;
  script_content?: string;
  script_name?: string;
  interpreter?: string;
  args?: string[];
  timeout?: number;
  env?: Record<string, string>;
  name?: string;
  grouping_strategy?: 'auto' | 'tags' | 'network' | 'location';
  max_retries?: number;
  max_batch_size?: number;
  order_by?: 'server_id' | 'sequence' | 'performance' | 'duration';
}

export interface BatchInfo {
  batch_id: string;
  group_id: string;
  group_name: string;
  total_count: number;
  success_count: number;
  failed_count: number;
  error_count: number;
  failure_rate: number;
  status: 'pending' | 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';
  start_time: string | null;
  end_time: string | null;
}

export interface JobPlanResponse {
  job_id: string;
  name: string;
  total_tasks: number;
  total_batches: number;
  grouping_strategy: string;
  created_at: string;
  batches: BatchInfo[];
}

export interface JobProgressResponse {
  job_id: string;
  name: string;
  status: 'running' | 'completed';
  total_tasks: number;
  completed_tasks: number;
  running_tasks: number;
  pending_tasks: number;
  failed_tasks: number;
  success_tasks: number;
  progress_pct: number;
  batches: BatchInfo[];
  resource_usage: {
    concurrency: number;
    active_connections: number;
    controller_stats: {
      current_concurrency: number;
      min_concurrency: number;
      max_concurrency: number;
      recent_error_rate: number;
      recent_avg_duration: number;
    };
  };
  start_time: string | null;
  estimated_finish_time: string | null;
}

export interface TaskResultResponse {
  task_id: string;
  server_id: string;
  server_name: string;
  exit_code: number | null;
  stdout: string;
  stderr: string;
  start_time: string | null;
  end_time: string | null;
  status: 'pending' | 'queued' | 'running' | 'retrying' | 'success' | 'failed' | 'error' | 'cancelled';
  retry_count: number;
  duration: number;
  error_message: string | null;
}

export interface ServerHeatmapResponse {
  server_id: string;
  server_name: string;
  host: string;
  tags: string[];
  avg_duration: number;
  success_rate: number;
  total_executions: number;
  performance_score: number;
}

export interface EngineStatusResponse {
  max_workers: number;
  max_batch_size: number;
  failure_threshold: number;
  current_concurrency: number;
  active_tasks: number;
  queued_tasks: number;
  is_shutdown: boolean;
  is_graceful_shutdown: boolean;
  controller_stats: {
    current_concurrency: number;
    min_concurrency: number;
    max_concurrency: number;
    recent_error_rate: number;
    recent_avg_duration: number;
  };
}
