import axios from 'axios';
import type {
  ServerConfig,
  ExecutionResult,
  ScriptTemplate,
  LogEntry,
  CommandExecuteRequest,
  ScriptExecuteRequest,
  EngineExecuteRequest,
  JobPlanResponse,
  JobProgressResponse,
  TaskResultResponse,
  ServerHeatmapResponse,
  EngineStatusResponse,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
});

export const serversApi = {
  list: (tag?: string): Promise<ServerConfig[]> =>
    api.get('/servers', { params: { tag } }).then(r => r.data),
  get: (id: string): Promise<ServerConfig> =>
    api.get(`/servers/${id}`).then(r => r.data),
  tags: (): Promise<string[]> =>
    api.get('/servers/tags').then(r => r.data),
  create: (data: Partial<ServerConfig>): Promise<ServerConfig> =>
    api.post('/servers', data).then(r => r.data),
  update: (id: string, data: Partial<ServerConfig>): Promise<ServerConfig> =>
    api.put(`/servers/${id}`, data).then(r => r.data),
  delete: (id: string): Promise<void> =>
    api.delete(`/servers/${id}`).then(r => r.data),
  test: (id: string): Promise<{ success: boolean; message: string }> =>
    api.post(`/servers/${id}/test`).then(r => r.data),
};

export const executeApi = {
  command: (data: CommandExecuteRequest): Promise<ExecutionResult[]> =>
    api.post('/execute/command', data).then(r => r.data),
  script: (data: ScriptExecuteRequest): Promise<ExecutionResult[]> =>
    api.post('/execute/script', data).then(r => r.data),
  listTasks: (serverId?: string, limit = 100): Promise<ExecutionResult[]> =>
    api.get('/execute/tasks', { params: { server_id: serverId, limit } }).then(r => r.data),
  getTask: (taskId: string): Promise<ExecutionResult> =>
    api.get(`/execute/tasks/${taskId}`).then(r => r.data),
};

export const templatesApi = {
  list: (tag?: string, keyword?: string): Promise<ScriptTemplate[]> =>
    api.get('/templates', { params: { tag, keyword } }).then(r => r.data),
  tags: (): Promise<string[]> =>
    api.get('/templates/tags').then(r => r.data),
  get: (id: string): Promise<ScriptTemplate> =>
    api.get(`/templates/${id}`).then(r => r.data),
  create: (data: Partial<ScriptTemplate> & { name: string; script_content: string }): Promise<ScriptTemplate> =>
    api.post('/templates', data).then(r => r.data),
  update: (id: string, data: Partial<ScriptTemplate>): Promise<ScriptTemplate> =>
    api.put(`/templates/${id}`, data).then(r => r.data),
  delete: (id: string): Promise<void> =>
    api.delete(`/templates/${id}`).then(r => r.data),
};

export const logsApi = {
  list: (params: { date?: string; server_id?: string; limit?: number } = {}): Promise<LogEntry[]> =>
    api.get('/logs', { params }).then(r => r.data),
  dates: (): Promise<string[]> =>
    api.get('/logs/dates').then(r => r.data),
  getByTask: (taskId: string): Promise<LogEntry> =>
    api.get(`/logs/${taskId}`).then(r => r.data),
};

export const engineApi = {
  executeCommand: (data: EngineExecuteRequest): Promise<JobPlanResponse> =>
    api.post('/engine/command', data).then(r => r.data),
  executeScript: (data: EngineExecuteRequest): Promise<JobPlanResponse> =>
    api.post('/engine/script', data).then(r => r.data),
  listJobs: (): Promise<JobPlanResponse[]> =>
    api.get('/engine/jobs').then(r => r.data),
  getJobProgress: (jobId: string): Promise<JobProgressResponse> =>
    api.get(`/engine/jobs/${jobId}/progress`).then(r => r.data),
  getJobResults: (jobId: string, orderBy = 'server_id'): Promise<TaskResultResponse[]> =>
    api.get(`/engine/jobs/${jobId}/results`, { params: { order_by: orderBy } }).then(r => r.data),
  resumeBatch: (batchId: string): Promise<{ status: string; message: string }> =>
    api.post('/engine/batches/resume', { batch_id: batchId }).then(r => r.data),
  getHeatmap: (): Promise<ServerHeatmapResponse[]> =>
    api.get('/engine/heatmap').then(r => r.data),
  getStatus: (): Promise<EngineStatusResponse> =>
    api.get('/engine/status').then(r => r.data),
  shutdownGraceful: (): Promise<{ status: string; message: string }> =>
    api.post('/engine/shutdown/graceful').then(r => r.data),
  shutdownForce: (): Promise<{ status: string; message: string }> =>
    api.post('/engine/shutdown/force').then(r => r.data),
};

export default api;
