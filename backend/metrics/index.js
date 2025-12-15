const client = require('prom-client');

const isEnabled = process.env.ENABLE_PROMETHEUS !== 'false';
const register = new client.Registry();
const metrics = {};

if (isEnabled) {
  const defaultLabels = {};
  if (process.env.PROMETHEUS_SERVICE_NAME) {
    defaultLabels.service_name = process.env.PROMETHEUS_SERVICE_NAME;
  }

  if (Object.keys(defaultLabels).length > 0) {
    register.setDefaultLabels(defaultLabels);
  }

  client.collectDefaultMetrics({
    register,
    prefix: process.env.PROMETHEUS_METRIC_PREFIX || '',
  });

  metrics.httpRequestsTotal = new client.Counter({
    name: 'hottrack_http_requests_total',
    help: 'Total de requisições HTTP processadas pelo backend.',
    labelNames: ['method', 'route', 'status_code'],
  });

  metrics.httpRequestDuration = new client.Histogram({
    name: 'hottrack_http_request_duration_seconds',
    help: 'Duração das requisições HTTP em segundos.',
    labelNames: ['method', 'route', 'status_code'],
    buckets: client.exponentialBuckets(0.05, 2, 8),
  });

  metrics.httpRequestBytes = new client.Histogram({
    name: 'hottrack_http_request_bytes',
    help: 'Tamanho (em bytes) dos payloads recebidos pelo backend.',
    labelNames: ['method', 'route'],
    buckets: [512, 1024, 4096, 16384, 65536, 262144, 1048576, 5242880, 10485760, 52428800],
  });

  metrics.httpResponseBytes = new client.Histogram({
    name: 'hottrack_http_response_bytes',
    help: 'Tamanho (em bytes) das respostas servidas pelo backend.',
    labelNames: ['method', 'route', 'status_code'],
    buckets: [512, 1024, 4096, 16384, 65536, 262144, 1048576, 5242880, 10485760, 52428800],
  });

  metrics.flowActionDuration = new client.Histogram({
    name: 'hottrack_flow_action_duration_seconds',
    help: 'Duração da execução de ações no motor de fluxos.',
    labelNames: ['flow_id', 'node_id', 'action_type'],
    buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10],
  });

  metrics.flowErrorsTotal = new client.Counter({
    name: 'hottrack_flow_errors_total',
    help: 'Total de erros registrados durante a execução de fluxos.',
    labelNames: ['flow_id', 'node_id', 'action_type', 'error_type'],
  });

  metrics.pixGenerationDuration = new client.Histogram({
    name: 'hottrack_pix_generation_duration_seconds',
    help: 'Duração das operações de geração de PIX.',
    labelNames: ['provider'],
    buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16],
  });

  metrics.pixErrorsTotal = new client.Counter({
    name: 'hottrack_pix_errors_total',
    help: 'Total de erros ao gerar ou consultar PIX.',
    labelNames: ['provider', 'stage'],
  });

  metrics.dbQueryDuration = new client.Histogram({
    name: 'hottrack_db_query_duration_seconds',
    help: 'Duração de consultas SQL instrumentadas.',
    labelNames: ['operation'],
    buckets: [0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
  });

  metrics.workerJobsTotal = new client.Counter({
    name: 'hottrack_worker_jobs_total',
    help: 'Total de jobs processados pelos workers.',
    labelNames: ['worker', 'job_type', 'status'],
  });

  metrics.workerJobDuration = new client.Histogram({
    name: 'hottrack_worker_job_duration_seconds',
    help: 'Duração do processamento de jobs nos workers.',
    labelNames: ['worker', 'job_type', 'status'],
    buckets: [0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32],
  });

  metrics.telegramMediaRequests = new client.Counter({
    name: 'hottrack_telegram_media_requests_total',
    help: 'Total de operações de mídia envolvendo o Telegram.',
    labelNames: ['source', 'type', 'status'],
  });

  metrics.telegramMediaBytes = new client.Histogram({
    name: 'hottrack_telegram_media_bytes',
    help: 'Volume de bytes processados em operações de mídia com o Telegram.',
    labelNames: ['source', 'type', 'direction'],
    buckets: [65536, 131072, 262144, 524288, 1048576, 3145728, 5242880, 10485760, 20971520, 52428800],
  });

  metrics.queueJobStalledTotal = new client.Counter({
    name: 'hottrack_queue_job_stalled_total',
    help: 'Total de jobs marcados como stalled nas filas BullMQ.',
    labelNames: ['queue_name'],
  });

  metrics.queueJobStalledDuration = new client.Histogram({
    name: 'hottrack_queue_job_stalled_duration_seconds',
    help: 'Tempo de processamento quando job foi marcado como stalled.',
    labelNames: ['queue_name'],
    buckets: [60, 300, 600, 900, 1200, 1800, 2400, 3600, 7200],
  });

  metrics.queueLockRenewalTotal = new client.Counter({
    name: 'hottrack_queue_lock_renewal_total',
    help: 'Total de renovações de lock realizadas.',
    labelNames: ['queue_name', 'status'],
  });

  metrics.queueLockRenewalDuration = new client.Histogram({
    name: 'hottrack_queue_lock_renewal_duration_seconds',
    help: 'Tempo entre renovações de lock.',
    labelNames: ['queue_name'],
    buckets: [30, 60, 90, 120, 180, 240, 300],
  });

  metrics.queueJobUtilizationRatio = new client.Histogram({
    name: 'hottrack_queue_job_utilization_ratio',
    help: 'Razão entre tempo de processamento e lockDuration calculado (0-1).',
    labelNames: ['queue_name'],
    buckets: [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
  });

  metrics.queueAdaptiveLockDuration = new client.Histogram({
    name: 'hottrack_queue_adaptive_lock_duration_seconds',
    help: 'LockDuration calculado adaptativamente baseado em métricas históricas.',
    labelNames: ['queue_name', 'source'],
    buckets: [60, 300, 600, 900, 1800, 3600, 7200, 14400, 28800],
  });

  // Métricas de escalabilidade
  metrics.contactsQuerySize = new client.Histogram({
    name: 'hottrack_contacts_query_size',
    help: 'Número de contatos retornados por query de busca de contatos.',
    labelNames: ['operation'],
    buckets: [10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
  });

  metrics.contactsQueryDuration = new client.Histogram({
    name: 'hottrack_contacts_query_duration_seconds',
    help: 'Tempo de execução de queries de busca de contatos.',
    labelNames: ['operation', 'has_tags'],
    buckets: [0.1, 0.5, 1, 2, 5, 10, 20, 30, 60],
  });

  metrics.contactsProcessedTotal = new client.Counter({
    name: 'hottrack_contacts_processed_total',
    help: 'Total de contatos processados em operações de validação, disparo, etc.',
    labelNames: ['operation', 'status'],
  });

  metrics.chatsQuerySize = new client.Histogram({
    name: 'hottrack_chats_query_size',
    help: 'Número de mensagens processadas em queries de chats.',
    labelNames: ['operation'],
    buckets: [100, 500, 1000, 5000, 10000, 20000, 50000, 100000],
  });

  metrics.chatsQueryDuration = new client.Histogram({
    name: 'hottrack_chats_query_duration_seconds',
    help: 'Tempo de execução de queries de chats.',
    labelNames: ['operation'],
    buckets: [0.1, 0.5, 1, 2, 5, 10, 20, 30],
  });

  metrics.dbConnectionPoolUsage = new client.Gauge({
    name: 'hottrack_db_connection_pool_usage',
    help: 'Uso atual do pool de conexões do banco de dados.',
    labelNames: ['state'],
  });

  // Métricas de streaming para validação e disparos
  metrics.streamingPagesProcessed = new client.Counter({
    name: 'hottrack_streaming_pages_processed_total',
    help: 'Total de páginas processadas em operações de streaming.',
    labelNames: ['operation'],
  });

  metrics.streamingPageSize = new client.Histogram({
    name: 'hottrack_streaming_page_size',
    help: 'Tamanho de cada página processada em streaming.',
    labelNames: ['operation'],
    buckets: [10, 50, 100, 500, 1000, 5000, 10000],
  });

  metrics.streamingPageDuration = new client.Histogram({
    name: 'hottrack_streaming_page_duration_seconds',
    help: 'Tempo de processamento de cada página em streaming.',
    labelNames: ['operation'],
    buckets: [0.1, 0.5, 1, 2, 5, 10, 20, 30],
  });

  metrics.streamingTotalProcessed = new client.Counter({
    name: 'hottrack_streaming_total_processed',
    help: 'Total de contatos processados via streaming.',
    labelNames: ['operation'],
  });

  Object.values(metrics).forEach((metric) => register.registerMetric(metric));
}

module.exports = {
  client,
  register,
  isEnabled,
  metrics,
};

