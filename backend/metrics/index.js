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

  Object.values(metrics).forEach((metric) => register.registerMetric(metric));
}

module.exports = {
  client,
  register,
  isEnabled,
  metrics,
};

