import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Row,
  Col,
  Progress,
  Statistic,
  Table,
  Tag,
  Button,
  Select,
  Space,
  Modal,
  Alert,
  Tooltip,
  Empty,
} from 'antd';
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  ReloadOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  DashboardOutlined,
  FireOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { engineApi } from '../../services/api';
import type {
  JobProgressResponse,
  BatchInfo,
  JobPlanResponse,
  TaskResultResponse,
  ServerHeatmapResponse,
} from '../../types';

const { Option } = Select;

const StatusTag: React.FC<{ status: string }> = ({ status }) => {
  const colorMap: Record<string, string> = {
    pending: 'default',
    running: 'processing',
    success: 'success',
    failed: 'error',
    error: 'error',
    paused: 'warning',
    retrying: 'orange',
    cancelled: 'default',
    completed: 'success',
  };

  const textMap: Record<string, string> = {
    pending: '等待中',
    running: '运行中',
    success: '成功',
    failed: '失败',
    error: '错误',
    paused: '已暂停',
    retrying: '重试中',
    cancelled: '已取消',
    completed: '已完成',
  };

  return <Tag color={colorMap[status] || 'default'}>{textMap[status] || status}</Tag>;
};

const BatchCard: React.FC<{
  batch: BatchInfo;
  onResume?: (batchId: string) => void;
}> = ({ batch, onResume }) => {
  const successRate = batch.total_count > 0
    ? ((batch.success_count / batch.total_count) * 100).toFixed(1)
    : '0';

  return (
    <Card
      size="small"
      style={{ marginBottom: 12 }}
      title={
        <Space>
          <span>{batch.group_name}</span>
          <StatusTag status={batch.status} />
          {batch.failure_rate >= 0.5 && (
            <Tag color="red" icon={<FireOutlined />}>
              失败率 {(batch.failure_rate * 100).toFixed(1)}%
            </Tag>
          )}
        </Space>
      }
      extra={
        batch.status === 'paused' && onResume && (
          <Button
            type="primary"
            size="small"
            icon={<PlayCircleOutlined />}
            onClick={() => onResume(batch.batch_id)}
          >
            恢复
          </Button>
        )
      }
    >
      <Row gutter={16}>
        <Col span={8}>
          <Statistic
            title="总数"
            value={batch.total_count}
            valueStyle={{ fontSize: 16 }}
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="成功"
            value={batch.success_count}
            valueStyle={{ color: '#52c41a', fontSize: 16 }}
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="失败"
            value={batch.failed_count + batch.error_count}
            valueStyle={{ color: '#ff4d4f', fontSize: 16 }}
          />
        </Col>
      </Row>
      <Progress
        percent={parseFloat(successRate)}
        status={
          batch.status === 'paused' ? 'exception' :
          batch.status === 'completed' ? 'success' : 'active'
        }
        style={{ marginTop: 8 }}
      />
    </Card>
  );
};

const HeatmapGrid: React.FC<{ data: ServerHeatmapResponse[] }> = ({ data }) => {
  const getColor = (score: number) => {
    if (score >= 0.8) return '#52c41a';
    if (score >= 0.6) return '#73d13d';
    if (score >= 0.4) return '#faad14';
    if (score >= 0.2) return '#fa8c16';
    return '#ff4d4f';
  };

  if (data.length === 0) {
    return <Empty description="暂无执行历史数据" />;
  }

  return (
    <div
      style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(120px, 1fr))',
        gap: 8,
        padding: 16,
      }}
    >
      {data.map((server) => (
        <Tooltip
          key={server.server_id}
          title={
            <div>
              <div><strong>{server.server_name}</strong></div>
              <div>主机: {server.host}</div>
              <div>标签: {server.tags.join(', ')}</div>
              <div>平均耗时: {server.avg_duration.toFixed(2)}s</div>
              <div>成功率: {(server.success_rate * 100).toFixed(1)}%</div>
              <div>执行次数: {server.total_executions}</div>
              <div>性能评分: {(server.performance_score * 100).toFixed(0)}</div>
            </div>
          }
        >
          <div
            style={{
              backgroundColor: getColor(server.performance_score),
              color: '#fff',
              padding: '12px 8px',
              borderRadius: 4,
              textAlign: 'center',
              cursor: 'pointer',
              transition: 'transform 0.2s',
              minHeight: 60,
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.transform = 'scale(1.05)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.transform = 'scale(1)';
            }}
          >
            <div style={{ fontSize: 12, fontWeight: 500, marginBottom: 4 }}>
              {server.server_name}
            </div>
            <div style={{ fontSize: 11, opacity: 0.9 }}>
              {(server.performance_score * 100).toFixed(0)}分
            </div>
          </div>
        </Tooltip>
      ))}
    </div>
  );
};

const ExecutionDashboard: React.FC = () => {
  const [jobs, setJobs] = useState<JobPlanResponse[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [jobProgress, setJobProgress] = useState<JobProgressResponse | null>(null);
  const [jobResults, setJobResults] = useState<TaskResultResponse[]>([]);
  const [heatmapData, setHeatmapData] = useState<ServerHeatmapResponse[]>([]);
  const [activeTab, setActiveTab] = useState<'progress' | 'results' | 'heatmap'>('progress');
  const [orderBy, setOrderBy] = useState<'server_id' | 'sequence' | 'duration' | 'status'>('server_id');
  const [resultModalVisible, setResultModalVisible] = useState(false);
  const [selectedResult, setSelectedResult] = useState<TaskResultResponse | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchJobs = useCallback(async () => {
    try {
      const data = await engineApi.listJobs();
      setJobs(data);
      if (data.length > 0 && !selectedJobId) {
        setSelectedJobId(data[0].job_id);
      }
    } catch (error) {
      console.error('Failed to fetch jobs:', error);
    }
  }, [selectedJobId]);

  const fetchJobProgress = useCallback(async (jobId: string) => {
    try {
      const data = await engineApi.getJobProgress(jobId);
      setJobProgress(data);
    } catch (error) {
      console.error('Failed to fetch job progress:', error);
    }
  }, []);

  const fetchJobResults = useCallback(async (jobId: string, order: string) => {
    try {
      const data = await engineApi.getJobResults(jobId, order);
      setJobResults(data);
    } catch (error) {
      console.error('Failed to fetch job results:', error);
    }
  }, []);

  const fetchHeatmap = useCallback(async () => {
    try {
      const data = await engineApi.getHeatmap();
      setHeatmapData(data);
    } catch (error) {
      console.error('Failed to fetch heatmap:', error);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
    fetchHeatmap();

    const interval = setInterval(() => {
      if (selectedJobId && activeTab === 'progress') {
        fetchJobProgress(selectedJobId);
      }
      if (selectedJobId && activeTab === 'results') {
        fetchJobResults(selectedJobId, orderBy);
      }
      fetchHeatmap();
    }, 3000);

    return () => clearInterval(interval);
  }, [selectedJobId, activeTab, orderBy, fetchJobs, fetchJobProgress, fetchJobResults, fetchHeatmap]);

  useEffect(() => {
    if (selectedJobId) {
      fetchJobProgress(selectedJobId);
      fetchJobResults(selectedJobId, orderBy);
    }
  }, [selectedJobId, orderBy, fetchJobProgress, fetchJobResults]);

  const handleResumeBatch = async (batchId: string) => {
    try {
      await engineApi.resumeBatch(batchId);
      if (selectedJobId) {
        fetchJobProgress(selectedJobId);
      }
    } catch (error) {
      console.error('Failed to resume batch:', error);
    }
  };

  const handleShutdownGraceful = async () => {
    Modal.confirm({
      title: '确认优雅关闭',
      content: '正在执行的任务将继续完成，但不会接受新任务。确定要优雅关闭引擎吗？',
      okText: '确认关闭',
      okType: 'danger',
      onOk: async () => {
        try {
          await engineApi.shutdownGraceful();
        } catch (error) {
          console.error('Failed to shutdown:', error);
        }
      },
    });
  };

  const resultColumns: ColumnsType<TaskResultResponse> = [
    {
      title: '服务器',
      dataIndex: 'server_name',
      key: 'server_name',
      width: 150,
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status) => <StatusTag status={status} />,
    },
    {
      title: '退出码',
      dataIndex: 'exit_code',
      key: 'exit_code',
      width: 80,
      render: (code) => (
        <Tag color={code === 0 ? 'success' : 'error'}>
          {code !== null ? code : '-'}
        </Tag>
      ),
    },
    {
      title: '重试次数',
      dataIndex: 'retry_count',
      key: 'retry_count',
      width: 100,
    },
    {
      title: '耗时',
      dataIndex: 'duration',
      key: 'duration',
      width: 100,
      render: (duration) => `${duration.toFixed(2)}s`,
    },
    {
      title: '开始时间',
      dataIndex: 'start_time',
      key: 'start_time',
      width: 180,
      render: (time) => time ? new Date(time).toLocaleString() : '-',
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_, record) => (
        <Button
          type="link"
          onClick={() => {
            setSelectedResult(record);
            setResultModalVisible(true);
          }}
        >
          查看详情
        </Button>
      ),
    },
  ];

  return (
    <div className="execution-dashboard">
      <Card
        title={
          <Space>
            <DashboardOutlined />
            <span>智能并行执行引擎看板</span>
          </Space>
        }
        extra={
          <Space>
            <Select
              style={{ width: 200 }}
              placeholder="选择任务"
              value={selectedJobId}
              onChange={setSelectedJobId}
            >
              {jobs.map((job) => (
                <Option key={job.job_id} value={job.job_id}>
                  {job.name} ({job.total_tasks}台服务器)
                </Option>
              ))}
            </Select>
            <Button
              icon={<ReloadOutlined />}
              onClick={fetchJobs}
            >
              刷新
            </Button>
            <Button
              type="primary"
              danger
              icon={<PauseCircleOutlined />}
              onClick={handleShutdownGraceful}
            >
              优雅关闭
            </Button>
          </Space>
        }
      >
        {jobProgress && (
          <>
            <Alert
              message={
                <Space>
                  {jobProgress.status === 'running' ? (
                    <><ThunderboltOutlined /> 任务运行中</>
                  ) : (
                    <><CheckCircleOutlined /> 任务已完成</>
                  )}
                  <span>预计完成时间: {jobProgress.estimated_finish_time ? new Date(jobProgress.estimated_finish_time).toLocaleString() : '计算中...'}</span>
                </Space>
              }
              type={jobProgress.status === 'running' ? 'info' : 'success'}
              showIcon
              style={{ marginBottom: 16 }}
            />

            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={6}>
                <Card>
                  <Statistic
                    title="总任务数"
                    value={jobProgress.total_tasks}
                    prefix={<ClockCircleOutlined />}
                  />
                </Card>
              </Col>
              <Col span={6}>
                <Card>
                  <Statistic
                    title="已完成"
                    value={jobProgress.completed_tasks}
                    valueStyle={{ color: '#1890ff' }}
                    prefix={<CheckCircleOutlined />}
                  />
                </Card>
              </Col>
              <Col span={6}>
                <Card>
                  <Statistic
                    title="成功"
                    value={jobProgress.success_tasks}
                    valueStyle={{ color: '#52c41a' }}
                    prefix={<CheckCircleOutlined />}
                  />
                </Card>
              </Col>
              <Col span={6}>
                <Card>
                  <Statistic
                    title="失败"
                    value={jobProgress.failed_tasks}
                    valueStyle={{ color: '#ff4d4f' }}
                    prefix={<CloseCircleOutlined />}
                  />
                </Card>
              </Col>
            </Row>

            <Card
              style={{ marginBottom: 16 }}
              title={
                <Space>
                  <Progress
                    type="circle"
                    percent={jobProgress.progress_pct}
                    size={80}
                    status={jobProgress.status === 'completed' ? 'success' : 'active'}
                  />
                  <div>
                    <div style={{ fontSize: 16, fontWeight: 500 }}>
                      整体进度: {jobProgress.progress_pct.toFixed(1)}%
                    </div>
                    <div style={{ color: '#666', fontSize: 12 }}>
                      并发数: {jobProgress.resource_usage.concurrency} |
                      活动连接: {jobProgress.resource_usage.active_connections} |
                      错误率: {(jobProgress.resource_usage.controller_stats.recent_error_rate * 100).toFixed(1)}%
                    </div>
                  </div>
                </Space>
              }
            />

            <Card
              tabList={[
                { key: 'progress', tab: '批次进度' },
                { key: 'results', tab: '执行结果' },
                { key: 'heatmap', tab: '资源热力图' },
              ]}
              activeTabKey={activeTab}
              onTabChange={(key) => setActiveTab(key as typeof activeTab)}
              extra={
                activeTab === 'results' && (
                  <Select
                    value={orderBy}
                    onChange={setOrderBy}
                    size="small"
                    style={{ width: 140 }}
                  >
                    <Option value="server_id">按服务器ID</Option>
                    <Option value="sequence">按执行顺序</Option>
                    <Option value="duration">按耗时</Option>
                  </Select>
                )
              }
            >
              {activeTab === 'progress' && (
                <div>
                  {jobProgress.batches.map((batch) => (
                    <BatchCard
                      key={batch.batch_id}
                      batch={batch}
                      onResume={handleResumeBatch}
                    />
                  ))}
                </div>
              )}

              {activeTab === 'results' && (
                <Table
                  columns={resultColumns}
                  dataSource={jobResults}
                  rowKey="task_id"
                  pagination={{ pageSize: 10 }}
                  size="small"
                  scroll={{ y: 400 }}
                />
              )}

              {activeTab === 'heatmap' && (
                <HeatmapGrid data={heatmapData} />
              )}
            </Card>
          </>
        )}

        {!jobProgress && jobs.length === 0 && (
          <Empty description="暂无执行任务，请先创建并执行任务" />
        )}
      </Card>

      <Modal
        title={`执行详情 - ${selectedResult?.server_name}`}
        open={resultModalVisible}
        onCancel={() => setResultModalVisible(false)}
        footer={[
          <Button key="close" onClick={() => setResultModalVisible(false)}>
            关闭
          </Button>,
        ]}
        width={800}
      >
        {selectedResult && (
          <div>
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={8}>
                <Statistic
                  title="状态"
                  valueRender={() => <StatusTag status={selectedResult.status} />}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="退出码"
                  value={selectedResult.exit_code ?? '-'}
                />
              </Col>
              <Col span={8}>
                <Statistic
                  title="耗时"
                  value={`${selectedResult.duration.toFixed(2)}s`}
                />
              </Col>
            </Row>

            {selectedResult.error_message && (
              <Alert
                message="错误信息"
                description={selectedResult.error_message}
                type="error"
                showIcon
                style={{ marginBottom: 16 }}
              />
            )}

            <Card title="标准输出 (stdout)" size="small" style={{ marginBottom: 8 }}>
              <pre
                style={{
                  maxHeight: 200,
                  overflow: 'auto',
                  background: '#f5f5f5',
                  padding: 12,
                  borderRadius: 4,
                  margin: 0,
                  fontSize: 12,
                }}
              >
                {selectedResult.stdout || '(无输出)'}
              </pre>
            </Card>

            <Card title="错误输出 (stderr)" size="small">
              <pre
                style={{
                  maxHeight: 200,
                  overflow: 'auto',
                  background: '#fff2f0',
                  padding: 12,
                  borderRadius: 4,
                  margin: 0,
                  fontSize: 12,
                  color: '#cf1322',
                }}
              >
                {selectedResult.stderr || '(无输出)'}
              </pre>
            </Card>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default ExecutionDashboard;
