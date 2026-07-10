import React, { useEffect, useMemo, useState } from 'react';
import { Table, Button, Modal, Form, Input, Select, Checkbox, Tag, message, Space, Popconfirm, Card, Row, Col } from 'antd';
import { PlayCircleOutlined, StopOutlined, ReloadOutlined, PlusOutlined, DeleteOutlined, EyeOutlined } from '@ant-design/icons';
import api from '../api';

const { Option } = Select;

const cellStyle = {
  maxWidth: 180,
  whiteSpace: 'nowrap',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  display: 'inline-block' 
};

const formatBytes = (bytes) => {
  if (!bytes || bytes <= 0) return '-';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let index = 0;
  while (value >= 1024 && index < units.length - 1) {
    value /= 1024;
    index += 1;
  }
  return `${value.toFixed(index === 0 ? 0 : 2)} ${units[index]}`;
};

const formatNumber = (value) => new Intl.NumberFormat('en-US').format(Number(value || 0));

const chartColors = ['#1677ff', '#52c41a', '#faad14', '#eb2f96', '#13c2c2', '#722ed1', '#fa541c', '#2f54eb', '#a0d911', '#f5222d'];

const buildConicGradient = (items, valueKey) => {
  const total = items.reduce((sum, item) => sum + (item[valueKey] || 0), 0);
  if (total <= 0) return '#f0f0f0';
  let start = 0;
  const parts = items.map((item, index) => {
    const value = item[valueKey] || 0;
    const percent = (value / total) * 100;
    const end = start + percent;
    const color = chartColors[index % chartColors.length];
    const segment = `${color} ${start.toFixed(2)}% ${end.toFixed(2)}%`;
    start = end;
    return segment;
  });
  return `conic-gradient(${parts.join(',')})`;
};

const PiePanel = ({ title, data }) => {
  const [hoveredIndex, setHoveredIndex] = useState(-1);
  const totalSize = data.reduce((sum, item) => sum + (item.size || 0), 0);
  const totalCount = data.reduce((sum, item) => sum + (item.count || 0), 0);
  const radius = 70;
  const cx = 90;
  const cy = 90;
  let cumulative = 0;
  const arcs = data.map((item, index) => {
    const value = item.size || 0;
    const ratio = totalSize > 0 ? value / totalSize : 0;
    const start = cumulative * Math.PI * 2;
    cumulative += ratio;
    const end = cumulative * Math.PI * 2;
    const x1 = cx + radius * Math.cos(start - Math.PI / 2);
    const y1 = cy + radius * Math.sin(start - Math.PI / 2);
    const x2 = cx + radius * Math.cos(end - Math.PI / 2);
    const y2 = cy + radius * Math.sin(end - Math.PI / 2);
    const largeArc = end-start > Math.PI ? 1 : 0;
    const path = `M ${cx} ${cy} L ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2} Z`;
    return { item, index, path, ratio };
  });
  return (
    <Card size="small" title={title} bodyStyle={{ padding: 12 }}>
      {totalSize <= 0 && totalCount <= 0 ? (
        <div style={{ textAlign: 'center', color: '#999', padding: '24px 0' }}>暂无数据</div>
      ) : (
        <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
          <svg viewBox="0 0 180 180" width={180} height={180} style={{ flexShrink: 0 }}>
            {arcs.map((arc) => (
              <path
                key={`${arc.item.label}-${arc.index}`}
                d={arc.path}
                fill={chartColors[arc.index % chartColors.length]}
                stroke="#fff"
                strokeWidth="1"
                opacity={hoveredIndex === -1 || hoveredIndex === arc.index ? 1 : 0.45}
                onMouseEnter={() => setHoveredIndex(arc.index)}
                onMouseLeave={() => setHoveredIndex(-1)}
              >
                <title>{`${arc.item.label}
文件数量: ${formatNumber(arc.item.count)}
大小: ${formatBytes(arc.item.size)}
占比: ${(arc.ratio * 100).toFixed(1)}%`}</title>
              </path>
            ))}
          </svg>
          <div style={{ minWidth: 220, flex: 1, maxHeight: 180, overflow: 'auto' }}>
            {data.map((item, index) => {
              const percent = totalSize > 0 ? (((item.size || 0) / totalSize) * 100).toFixed(1) : '0.0';
              return (
                <div
                  key={`${item.label}-${index}`}
                  style={{ display: 'flex', alignItems: 'center', marginBottom: 8, opacity: hoveredIndex === -1 || hoveredIndex === index ? 1 : 0.5 }}
                  onMouseEnter={() => setHoveredIndex(index)}
                  onMouseLeave={() => setHoveredIndex(-1)}
                >
                  <span style={{ width: 10, height: 10, background: chartColors[index % chartColors.length], borderRadius: 2, marginRight: 8 }} />
                  <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={item.label}>{item.label}</span>
                  <span style={{ marginLeft: 8, color: '#666', whiteSpace: 'nowrap' }}>
                    {formatNumber(item.count)} | {formatBytes(item.size)} ({percent}%)
                  </span>
                </div>
              );
            })}
            <div style={{ marginTop: 6, color: '#8c8c8c', fontSize: 12 }}>
              合计：{formatNumber(totalCount)} | {formatBytes(totalSize)}
            </div>
          </div>
        </div>
      )}
    </Card>
  );
};

const DailyBarPanel = ({ title, data }) => {
  const maxCount = Math.max(1, ...data.map(item => item.count));
  const maxSize = Math.max(1, ...data.map(item => item.size));
  const width = Math.max(680, data.length * 58 + 180);
  const height = 300;
  const margin = { top: 20, right: 72, bottom: 48, left: 72 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;
  const groupWidth = chartWidth / Math.max(1, data.length);
  const barWidth = Math.min(18, Math.max(8, groupWidth * 0.25));

  const yByCount = (v) => margin.top + chartHeight - (v / maxCount) * chartHeight;
  const yBySize = (v) => margin.top + chartHeight - (v / maxSize) * chartHeight;
  const countTicks = [0, 0.25, 0.5, 0.75, 1].map(r => Math.round(maxCount * r));
  const sizeTicks = [0, 0.25, 0.5, 0.75, 1].map(r => Math.round(maxSize * r));

  return (
    <Card size="small" title={title} bodyStyle={{ padding: 12 }}>
      {data.length === 0 ? (
        <div style={{ textAlign: 'center', color: '#999', padding: '24px 0' }}>暂无数据</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <svg viewBox={`0 0 ${width} ${height}`} style={{ width: '100%', minWidth: width, height: 'auto', display: 'block' }}>
            <line x1={margin.left} y1={margin.top + chartHeight} x2={margin.left + chartWidth} y2={margin.top + chartHeight} stroke="#d9d9d9" />
            <line x1={margin.left} y1={margin.top} x2={margin.left} y2={margin.top + chartHeight} stroke="#d9d9d9" />
            <line x1={margin.left + chartWidth} y1={margin.top} x2={margin.left + chartWidth} y2={margin.top + chartHeight} stroke="#d9d9d9" />

            {countTicks.map((tick, idx) => {
              const y = yByCount(tick);
              return (
                <g key={`ct-${idx}`}>
                  <line x1={margin.left} y1={y} x2={margin.left + chartWidth} y2={y} stroke="#f0f0f0" />
                  <text x={margin.left - 8} y={y + 4} textAnchor="end" fontSize="11" fill="#8c8c8c">{formatNumber(tick)}</text>
                </g>
              );
            })}

            {sizeTicks.map((tick, idx) => {
              const y = yBySize(tick);
              return (
                <text key={`st-${idx}`} x={margin.left + chartWidth + 8} y={y + 4} fontSize="11" fill="#8c8c8c">
                  {formatBytes(tick)}
                </text>
              );
            })}

            <text x={margin.left} y={14} fontSize="11" fill="#1677ff">文件数量</text>
            <text x={margin.left + chartWidth - 4} y={14} fontSize="11" fill="#52c41a" textAnchor="end">大小</text>

            {data.map((item, index) => {
              const centerX = margin.left + groupWidth * index + groupWidth / 2;
              const countTop = yByCount(item.count);
              const sizeTop = yBySize(item.size);
              return (
                <g key={item.date}>
                  <rect
                    x={centerX - barWidth - 2}
                    y={countTop}
                    width={barWidth}
                    height={margin.top + chartHeight - countTop}
                    fill="#1677ff"
                    rx="3"
                  >
                    <title>{`${item.date} 文件数量 ${formatNumber(item.count)}`}</title>
                  </rect>
                  <rect
                    x={centerX + 2}
                    y={sizeTop}
                    width={barWidth}
                    height={margin.top + chartHeight - sizeTop}
                    fill="#52c41a"
                    rx="3"
                  >
                    <title>{`${item.date} 大小 ${formatBytes(item.size)}`}</title>
                  </rect>
                  <text x={centerX} y={margin.top + chartHeight + 18} textAnchor="middle" fontSize="11" fill="#8c8c8c">
                    {item.date.slice(5)}
                  </text>
                </g>
              );
            })}
          </svg>
        </div>
      )}
      <div style={{ marginTop: 8, display: 'flex', gap: 16, color: '#666' }}>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#1677ff', marginRight: 6 }} />文件数量</span>
        <span><span style={{ display: 'inline-block', width: 10, height: 10, background: '#52c41a', marginRight: 6 }} />大小</span>
      </div>
    </Card>
  );
};

const JobList = () => {
  const [jobs, setJobs] = useState([]);
  const [statsData, setStatsData] = useState({ dest: [], daily: [] });
  const [metadataList, setMetadataList] = useState([]);
  const [loading, setLoading] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [detailVisible, setDetailVisible] = useState(false);
  const [selectedJob, setSelectedJob] = useState(null);
  const [pagination, setPagination] = useState({
    current: 1,
    pageSize: 10,
    total: 0
  });
  const [form] = Form.useForm();

  const getMetadataName = (record) => {
    if (record?.metadata?.client_name) {
      return record.metadata.client_name;
    }
    const matched = metadataList.find(m => m.id === record?.metadata_id);
    return matched?.client_name || '-';
  };

  const getDestSecondary = (dstDir) => {
    if (!dstDir) return '-';
    const parts = dstDir.split('/').filter(Boolean);
    if (parts.length >= 2) return `${parts[0]}/${parts[1]}`;
    return parts[0] || '-';
  };

  const fetchJobs = async (page = 1, pageSize = 10) => {
    setLoading(true);
    try {
      const res = await api.get(`/jobs/?page=${page}&limit=${pageSize}`);
      setJobs(res.data);
      
      if (res.headers && res.headers['x-total-count']) {
        setPagination(prev => ({
          ...prev,
          current: page,
          pageSize: pageSize,
          total: parseInt(res.headers['x-total-count'])
        }));
      } else {
        setPagination(prev => ({
          ...prev,
          current: page,
          pageSize: pageSize
        }));
      }
    } catch (error) {
      message.error('Failed to load jobs');
    } finally {
      setLoading(false);
    }
  };

  const fetchStatsData = async () => {
    try {
      const res = await api.get('/jobs/stats');
      setStatsData({
        dest: Array.isArray(res.data?.dest) ? res.data.dest : [],
        daily: Array.isArray(res.data?.daily) ? res.data.daily : [],
      });
    } catch (error) {
      setStatsData({ dest: [], daily: [] });
    }
  };

  const fetchMetadata = async () => {
    try {
      const res = await api.get('/metadata/');
      setMetadataList(res.data);
    } catch (error) {
      console.error(error);
    }
  };

  useEffect(() => {
    fetchJobs(pagination.current, pagination.pageSize);
    fetchStatsData();
    fetchMetadata();
  }, [pagination.current, pagination.pageSize]);

  const handleCreate = async () => {
    try {
      const values = await form.validateFields();
      const payload = {
        ...values,
        delete_source: !!values.delete_source,
        is_incremental: !!values.is_incremental
      };
      await api.post('/jobs/', payload);
      message.success('Job created');
      setIsModalOpen(false);
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsData();
    } catch (error) {
      console.error(error);
    }
  };

  const handleAction = async (jobId, action) => {
    try {
      await api.post(`/jobs/${jobId}/${action}`);
      message.success(`Job ${action}ed`);
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsData();
    } catch (error) {
      message.error(`Failed to ${action} job`);
    }
  };

  const handleDelete = async (jobId) => {
    try {
      await api.delete(`/jobs/${jobId}`);
      message.success('Job deleted');
      fetchJobs(pagination.current, pagination.pageSize);
      fetchStatsData();
    } catch (error) {
      message.error('Failed to delete job');
    }
  };

  const handleDuplicateJob = () => {
    if (!selectedJob) return;
    const payload = {
      metadata_id: selectedJob.metadata_id,
      src_dir: selectedJob.src_dir,
      dst_dir: selectedJob.dst_dir,
      include: selectedJob.include || '',
      exclude: selectedJob.exclude || '',
      delete_source: !!selectedJob.delete_source,
      is_incremental: !!selectedJob.is_incremental
    };
    if (selectedJob.is_incremental) {
      payload.periodic_interval = selectedJob.periodic_interval > 0 ? selectedJob.periodic_interval : 600;
    }
    form.resetFields();
    form.setFieldsValue(payload);
    setIsModalOpen(true);
    setDetailVisible(false);
  };

  const statusColors = {
    PENDING: 'default',
    RUNNING: 'processing',
    PHASEDCOMPLETED: 'cyan',
    PAUSED: 'warning',
    STOPPED: 'error',
    COMPLETED: 'success',
    FAILED: 'error'
  };

  const metadataNameMap = useMemo(() => {
    const map = new Map();
    metadataList.forEach(item => {
      map.set(item.id, item.client_name || `meta-${item.id}`);
    });
    return map;
  }, [metadataList]);

  const chartData = useMemo(() => {
    if ((statsData.dest?.length || 0) > 0 || (statsData.daily?.length || 0) > 0) {
      return statsData;
    }

    const source = jobs;
    const byMeta = new Map();
    const byDest = new Map();
    const byDay = new Map();

    source.forEach(job => {
      const metaId = job.metadata_id;
      const metaName = job?.metadata?.client_name || metadataNameMap.get(metaId) || `meta-${metaId ?? '-'}`;
      const size = Number(job.success_size_bytes || 0);
      const count = Number(job.success_count ?? job.total_count ?? 0);

      const metaKey = `${metaId || '-'}|${metaName}`;
      if (!byMeta.has(metaKey)) byMeta.set(metaKey, { label: metaName, count: 0, size: 0 });
      const metaAgg = byMeta.get(metaKey);
      metaAgg.count += count;
      metaAgg.size += size;

      const dest2 = getDestSecondary(job.dst_dir);
      const destLabel = `${metaName} | ${dest2}`;
      if (!byDest.has(destLabel)) byDest.set(destLabel, { label: destLabel, count: 0, size: 0 });
      const destAgg = byDest.get(destLabel);
      destAgg.count += count;
      destAgg.size += size;

      const day = (job.created_at || '').slice(0, 10) || '-';
      if (!byDay.has(day)) byDay.set(day, { date: day, count: 0, size: 0 });
      const dayAgg = byDay.get(day);
      dayAgg.count += count;
      dayAgg.size += size;

    });

    const topBySize = (arr) => arr.sort((a, b) => b.size - a.size).slice(0, 10);
    const daily = Array.from(byDay.values())
      .filter(item => item.date !== '-')
      .sort((a, b) => a.date.localeCompare(b.date))
      .slice(-14);

    return {
      dest: topBySize(Array.from(byDest.values())),
      daily
    };
  }, [jobs, statsData, metadataNameMap]);

  const columns = [
    { title: 'ID', dataIndex: 'job_id', key: 'job_id', width: 60 },
    { title: 'Metadata ID', dataIndex: 'metadata_id', key: 'metadata_id', width: 100, responsive: ['md'] },
    {
      title: 'Metadata Name',
      key: 'metadata_name',
      width: 180,
      render: (_, record) => (
        <div style={cellStyle} title={getMetadataName(record)}>
          {getMetadataName(record)}
        </div>
      ),
      responsive: ['md']
    },
    { 
      title: 'Source', 
      dataIndex: 'src_dir', 
      key: 'src_dir',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      ) 
    },
    { 
      title: 'Dest', 
      dataIndex: 'dst_dir', 
      key: 'dst_dir',
      render: (text) => (
        <div style={cellStyle} title={text}>
          {text}
        </div>
      )
    },
    { 
      title: 'Status', 
      dataIndex: 'status', 
      key: 'status',
      render: (status) => <Tag color={statusColors[status] || 'default'}>{status}</Tag>
    },
    {
      title: 'Success Size',
      dataIndex: 'success_size_bytes',
      key: 'success_size_bytes',
      width: 120,
      render: (value) => formatBytes(value),
      responsive: ['md']
    },
    { 
      title: 'Action',
      key: 'action',
      render: (_, record) => (
        <Space>
          <Button 
            icon={<EyeOutlined />} 
            size="small" 
            onClick={() => { setSelectedJob(record); setDetailVisible(true); }}
          >
            Details
          </Button>
          {(record.status === 'PENDING' || record.status === 'PAUSED' || record.status === 'STOPPED' || record.status === 'FAILED') && (
            <Button icon={<PlayCircleOutlined />} size="small" onClick={() => handleAction(record.job_id, 'start')}>Start</Button>
          )}
          {(record.status === 'RUNNING' || record.status === 'PHASEDCOMPLETED') && (
            <Button icon={<StopOutlined />} size="small" danger onClick={() => handleAction(record.job_id, 'stop')}>Stop</Button>
          )}
          {record.failed_count > 0 && (
            <Button icon={<ReloadOutlined />} size="small" onClick={() => handleAction(record.job_id, 'retry')}>Retry Failed</Button>
          )}
          <Popconfirm 
            title="Are you sure delete this job?"  
            onConfirm={() => handleDelete(record.job_id)} 
            okText="Yes" 
            cancelText="No"
          >
             <Button icon={<DeleteOutlined />} size="small" danger>Delete</Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
        <Col xs={24} lg={8}>
          <PiePanel title="按 Destination 二级目录展示 Transfer（文件数量 | 大小）" data={chartData.dest} />
        </Col>
        <Col xs={24} lg={16}>
          <DailyBarPanel title="每天 Transfer 文件数量与大小（近14天）" data={chartData.daily} />
        </Col>
      </Row>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { form.resetFields(); setIsModalOpen(true); }}>
          New Transfer Job
        </Button>
        <Button icon={<ReloadOutlined />} onClick={() => { fetchJobs(pagination.current, pagination.pageSize); fetchStatsData(); }}>Refresh</Button>
      </div>
      
      <Table 
        columns={columns} 
        dataSource={jobs} 
        rowKey="job_id" 
        loading={loading}
        scroll={{ x: 'max-content' }}
        pagination={{
          current: pagination.current,
          pageSize: pagination.pageSize,
          total: pagination.total,
          onChange: (page, pageSize) => setPagination(prev => ({ ...prev, current: page, pageSize })),
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} jobs`
        }}
      />

      <Modal 
        title="Create Transfer Job" 
        open={isModalOpen} 
        onOk={handleCreate} 
        onCancel={() => setIsModalOpen(false)}
      >
        <Form form={form} layout="vertical" initialValues={{ delete_source: false, is_incremental: false }}>
          <Form.Item name="metadata_id" label="Client/Metadata" rules={[{ required: true }]}>
            <Select>
              {metadataList.map(m => (
                <Option key={m.id} value={m.id}>{m.client_name} ({m.endpoint})</Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="src_dir" label="Source Directory" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="dst_dir" label="Destination Directory" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="include" label="Include Pattern (Glob)" tooltip="Example: *.jpg">
            <Input placeholder="*.jpg" />
          </Form.Item>
          <Form.Item name="exclude" label="Exclude Pattern (Glob)" tooltip="Example: temp/*">
            <Input placeholder="temp/*" />
          </Form.Item>
          <Form.Item name="delete_source" valuePropName="checked">
            <Checkbox>Delete source files after transfer</Checkbox>
          </Form.Item>
          <Form.Item name="is_incremental" valuePropName="checked">
            <Checkbox>Incremental Transfer (Continuous Sync)</Checkbox>
          </Form.Item>
          <Form.Item
            noStyle
            shouldUpdate={(prev, current) => prev.is_incremental !== current.is_incremental}
          >
            {({ getFieldValue }) =>
              getFieldValue('is_incremental') ? (
                <Form.Item
                  name="periodic_interval"
                  label="Periodic Interval (Seconds)"
                  rules={[{ required: true, message: 'Please set interval' }]}
                  initialValue={600}
                >
                  <Input type="number" />
                </Form.Item>
              ) : null
            }
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            {selectedJob && (
              <Button type="primary" size="small" onClick={handleDuplicateJob}>新建复制任务</Button>
            )}
            <span>Job Details</span>
          </div>
        }
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={[
          <Button key="close" onClick={() => setDetailVisible(false)}>Close</Button>,
          selectedJob && selectedJob.failed_count > 0 && (
            <Button key="retry" icon={<ReloadOutlined />} onClick={() => handleAction(selectedJob.job_id, 'retry')}>Retry Failed</Button>
          )
        ]}
        width={700}
      >
        {selectedJob && (
          <div style={{ border: '1px solid #f0f0f0' }}>
            {[
              { label: 'Job ID', value: selectedJob.job_id },
              { label: 'Client/Metadata ID', value: selectedJob.metadata_id },
              { label: 'Client/Metadata Name', value: getMetadataName(selectedJob) },
              { label: 'Source', value: selectedJob.src_dir, fullWidth: true },
              { label: 'Destination', value: selectedJob.dst_dir, fullWidth: true },
              { label: 'Include', value: selectedJob.include || '-' },
              { label: 'Exclude', value: selectedJob.exclude || '-' },
              { label: 'Delete Source', value: selectedJob.delete_source ? 'Yes' : 'No' },
              { label: 'Incremental', value: selectedJob.is_incremental ? 'Yes' : 'No' },
              ...(selectedJob.is_incremental ? [{ label: 'Periodic Interval', value: `${selectedJob.periodic_interval} s` }] : []),
              { label: 'Status', value: <Tag color={statusColors[selectedJob.status] || 'default'}>{selectedJob.status}</Tag> },
              { label: 'Success Size', value: formatBytes(selectedJob.success_size_bytes) },
              { label: 'Total Count', value: selectedJob.total_count },
              { label: 'Pending Count', value: selectedJob.pending_count },
              { label: 'Running Count', value: selectedJob.running_count },
              { label: 'Success Count', value: selectedJob.success_count },
              { label: 'Failed Count', value: selectedJob.failed_count },
              { label: 'Start Time', value: selectedJob.start_time || '-' },
              { label: 'End Time', value: selectedJob.end_time || '-' },
              { label: 'Duration', value: `${selectedJob.duration_seconds} seconds` },
              { label: 'Execution Count', value: selectedJob.execution_count },
              { label: 'Result Message', value: selectedJob.result_message || 'N/A', fullWidth: true },
              { label: 'Created At', value: selectedJob.created_at },
              { label: 'Updated At', value: selectedJob.updated_at },
            ].map((item, index) => (
              <div 
                key={index}
                style={{ 
                  display: 'flex', 
                  borderBottom: (index < 21) ? '1px solid #f0f0f0' : 'none',
                  flexWrap: 'wrap'
                }}
              >
                <div style={{
                  width: '100%',
                  padding: '12px 16px',
                  fontWeight: 'bold',
                  background: '#fafafa',
                  borderRight: '1px solid #f0f0f0',
                  flex: '0 0 150px'
                }}>
                  {item.label}
                </div>
                <div style={{
                  width: '100%',
                  padding: '12px 16px',
                  flex: 1,
                  overflowWrap: 'break-word',
                  wordWrap: 'break-word',
                }}>
                  {item.value}
                </div>
              </div>
            ))}
          </div>
        )}
      </Modal>
    </div>
  );
};

export default JobList;
