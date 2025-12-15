// Sistema de métricas históricas para cálculo adaptativo de lockDuration
const { redisConnection } = require('./queue');
const logger = require('../logger');

const METRICS_TTL = 30 * 24 * 60 * 60 * 1000; // 30 dias em milissegundos
const MIN_SAMPLES_FOR_PERCENTILES = 10; // Mínimo de amostras para calcular percentis confiáveis

/**
 * Gera chave Redis para métricas
 * @param {string} queueName - Nome da fila
 * @param {string} metricType - Tipo de métrica (nodes, contacts, delay, default)
 * @param {string|number} metricKey - Chave específica da métrica
 * @returns {string} - Chave Redis
 */
function getMetricsKey(queueName, metricType, metricKey) {
    return `queue:metrics:${queueName}:${metricType}:${metricKey}`;
}

/**
 * Calcula percentis de forma incremental usando algoritmo aproximado
 * Baseado em t-digest ou aproximação simples para percentis
 * @param {Array<number>} values - Array de valores ordenados
 * @param {Array<number>} percentiles - Array de percentis desejados [50, 75, 90, 95, 99]
 * @returns {Object} - Objeto com percentis calculados
 */
function calculatePercentiles(values, percentiles = [50, 75, 90, 95, 99]) {
    if (!values || values.length === 0) {
        return {};
    }
    
    const sorted = [...values].sort((a, b) => a - b);
    const result = {};
    
    for (const p of percentiles) {
        const index = Math.ceil((p / 100) * sorted.length) - 1;
        result[`p${p}`] = sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }
    
    return result;
}

/**
 * Atualiza métricas históricas no Redis
 * @param {string} queueName - Nome da fila
 * @param {number} processingTimeMs - Tempo de processamento em milissegundos
 * @param {Object} jobCharacteristics - Características do job (nodes, contacts, delay, etc.)
 * @returns {Promise<void>}
 */
async function updateMetrics(queueName, processingTimeMs, jobCharacteristics = {}) {
    try {
        const { nodes, contacts, delaySeconds } = jobCharacteristics;
        
        // Atualizar métricas gerais (default)
        await updateMetricSet(queueName, 'default', 'all', processingTimeMs);
        
        // Atualizar métricas por número de nós
        if (nodes !== undefined && nodes !== null) {
            await updateMetricSet(queueName, 'nodes', nodes, processingTimeMs);
        }
        
        // Atualizar métricas por número de contatos
        if (contacts !== undefined && contacts !== null) {
            await updateMetricSet(queueName, 'contacts', contacts, processingTimeMs);
        }
        
        // Atualizar métricas por delay
        if (delaySeconds !== undefined && delaySeconds !== null) {
            await updateMetricSet(queueName, 'delay', delaySeconds, processingTimeMs);
        }
    } catch (error) {
        logger.warn(`[QueueMetrics] Erro ao atualizar métricas:`, error.message);
    }
}

/**
 * Atualiza um conjunto específico de métricas
 * @param {string} queueName - Nome da fila
 * @param {string} metricType - Tipo de métrica
 * @param {string|number} metricKey - Chave da métrica
 * @param {number} processingTimeMs - Tempo de processamento em milissegundos
 * @returns {Promise<void>}
 */
async function updateMetricSet(queueName, metricType, metricKey, processingTimeMs) {
    const key = getMetricsKey(queueName, metricType, metricKey);
    
    try {
        // Buscar métricas existentes
        const existing = await redisConnection.get(key);
        let metrics = existing ? JSON.parse(existing) : {
            values: [],
            count: 0,
            sum: 0,
            lastUpdated: Date.now()
        };
        
        // Adicionar novo valor
        metrics.values.push(processingTimeMs);
        metrics.count += 1;
        metrics.sum += processingTimeMs;
        metrics.lastUpdated = Date.now();
        
        // Manter apenas últimos 1000 valores para performance (rolling window)
        if (metrics.values.length > 1000) {
            metrics.values = metrics.values.slice(-1000);
        }
        
        // Calcular percentis se tiver amostras suficientes
        if (metrics.values.length >= MIN_SAMPLES_FOR_PERCENTILES) {
            const percentiles = calculatePercentiles(metrics.values);
            metrics.p50 = percentiles.p50;
            metrics.p75 = percentiles.p75;
            metrics.p90 = percentiles.p90;
            metrics.p95 = percentiles.p95;
            metrics.p99 = percentiles.p99;
        }
        
        // Calcular média
        metrics.avg = metrics.sum / metrics.count;
        
        // Salvar no Redis com TTL
        await redisConnection.setex(key, Math.floor(METRICS_TTL / 1000), JSON.stringify(metrics));
    } catch (error) {
        logger.warn(`[QueueMetrics] Erro ao atualizar métrica ${key}:`, error.message);
    }
}

/**
 * Consulta métricas históricas
 * @param {string} queueName - Nome da fila
 * @param {Object} jobCharacteristics - Características do job para buscar métricas específicas
 * @returns {Promise<Object|null>} - Métricas encontradas ou null
 */
async function getMetrics(queueName, jobCharacteristics = {}) {
    try {
        const { nodes, contacts, delaySeconds } = jobCharacteristics;
        
        // Prioridade: métricas específicas > métricas gerais
        let metrics = null;
        
        // Tentar buscar métricas por número de nós (mais específico)
        if (nodes !== undefined && nodes !== null) {
            const nodesKey = getMetricsKey(queueName, 'nodes', nodes);
            const nodesData = await redisConnection.get(nodesKey);
            if (nodesData) {
                metrics = JSON.parse(nodesData);
                metrics.source = 'nodes';
                metrics.sourceKey = nodes;
            }
        }
        
        // Se não encontrou por nós, tentar por contatos
        if (!metrics && contacts !== undefined && contacts !== null) {
            const contactsKey = getMetricsKey(queueName, 'contacts', contacts);
            const contactsData = await redisConnection.get(contactsKey);
            if (contactsData) {
                metrics = JSON.parse(contactsData);
                metrics.source = 'contacts';
                metrics.sourceKey = contacts;
            }
        }
        
        // Se não encontrou por contatos, tentar por delay
        if (!metrics && delaySeconds !== undefined && delaySeconds !== null) {
            const delayKey = getMetricsKey(queueName, 'delay', delaySeconds);
            const delayData = await redisConnection.get(delayKey);
            if (delayData) {
                metrics = JSON.parse(delayData);
                metrics.source = 'delay';
                metrics.sourceKey = delaySeconds;
            }
        }
        
        // Fallback para métricas gerais
        if (!metrics) {
            const defaultKey = getMetricsKey(queueName, 'default', 'all');
            const defaultData = await redisConnection.get(defaultKey);
            if (defaultData) {
                metrics = JSON.parse(defaultData);
                metrics.source = 'default';
                metrics.sourceKey = 'all';
            }
        }
        
        return metrics;
    } catch (error) {
        logger.warn(`[QueueMetrics] Erro ao consultar métricas:`, error.message);
        return null;
    }
}

/**
 * Extrai características do job para métricas
 * @param {string} queueName - Nome da fila
 * @param {Object} jobData - Dados do job
 * @returns {Object} - Características extraídas
 */
function extractJobCharacteristics(queueName, jobData) {
    const characteristics = {};
    
    // Extrair número de nós do flow
    if (jobData.flow_nodes) {
        try {
            const flowNodes = typeof jobData.flow_nodes === 'string' 
                ? JSON.parse(jobData.flow_nodes) 
                : jobData.flow_nodes;
            const nodes = Array.isArray(flowNodes) ? flowNodes : (flowNodes.nodes || []);
            characteristics.nodes = nodes.length;
        } catch (e) {
            // Ignorar erro de parsing
        }
    }
    
    // Extrair número de contatos
    if (jobData.contacts && Array.isArray(jobData.contacts)) {
        characteristics.contacts = jobData.contacts.length;
    } else if (jobData.total_contacts) {
        characteristics.contacts = jobData.total_contacts;
    }
    
    // Extrair delay em segundos
    if (jobData.delay_seconds) {
        characteristics.delaySeconds = jobData.delay_seconds;
    }
    
    return characteristics;
}

/**
 * Calcula lockDuration adaptativo baseado em métricas históricas
 * @param {string} queueName - Nome da fila
 * @param {Object} jobData - Dados do job
 * @returns {Promise<number|null>} - LockDuration em milissegundos ou null se não houver métricas
 */
async function getAdaptiveLockDuration(queueName, jobData) {
    const characteristics = extractJobCharacteristics(queueName, jobData);
    const metrics = await getMetrics(queueName, characteristics);
    
    if (!metrics || metrics.count < MIN_SAMPLES_FOR_PERCENTILES) {
        return null; // Não há métricas suficientes
    }
    
    // Usar percentil p95 ou p99 como base (mais conservador)
    const baseTime = metrics.p99 || metrics.p95 || metrics.p90 || metrics.avg;
    
    if (!baseTime) {
        return null;
    }
    
    // Calcular margem de segurança baseada na variância
    // Se p99 está muito acima da média, usar margem maior
    const variance = metrics.p99 && metrics.avg 
        ? (metrics.p99 / metrics.avg) 
        : 1.5; // Margem padrão de 1.5x se não houver p99
    
    // Margem de segurança adaptativa: mínimo 1.5x, máximo 3x baseado na variância
    const safetyMargin = Math.min(3, Math.max(1.5, variance * 1.2));
    
    const lockDuration = baseTime * safetyMargin;
    
    logger.debug(`[QueueMetrics] LockDuration adaptativo calculado: ${Math.round(lockDuration / 60000)}min (base: ${Math.round(baseTime / 60000)}min, margem: ${safetyMargin.toFixed(2)}x, amostras: ${metrics.count})`);
    
    return Math.ceil(lockDuration);
}

/**
 * Calcula stalledInterval adaptativo baseado em lockDuration e métricas históricas
 * @param {number} lockDuration - LockDuration em milissegundos
 * @param {string} queueName - Nome da fila
 * @param {Object} jobData - Dados do job (opcional, para buscar métricas específicas)
 * @returns {Promise<number>} - StalledInterval em milissegundos
 */
async function getAdaptiveStalledInterval(lockDuration, queueName, jobData = {}) {
    // Base: 1/3 do lockDuration (padrão conservador)
    let stalledInterval = Math.floor(lockDuration / 3);
    
    // Tentar ajustar baseado em métricas históricas de tempo entre renovações
    if (jobData) {
        const characteristics = extractJobCharacteristics(queueName, jobData);
        const metrics = await getMetrics(queueName, characteristics);
        
        if (metrics && metrics.avg) {
            // Se tempo médio de processamento é conhecido, ajustar stalledInterval
            // Garantir que stalledInterval seja sempre menor que lockDuration com margem
            // Usar mínimo de 20% do lockDuration, máximo de 50%
            const minStalled = lockDuration * 0.2;
            const maxStalled = lockDuration * 0.5;
            
            // Baseado no tempo médio, ajustar stalledInterval
            // Se tempo médio é baixo comparado ao lockDuration, usar stalledInterval menor
            const avgRatio = metrics.avg / lockDuration;
            if (avgRatio < 0.3) {
                // Processamento rápido, usar stalledInterval menor (mais agressivo)
                stalledInterval = Math.max(minStalled, lockDuration * 0.25);
            } else if (avgRatio > 0.7) {
                // Processamento lento, usar stalledInterval maior (menos agressivo)
                stalledInterval = Math.min(maxStalled, lockDuration * 0.45);
            }
        }
    }
    
    // Garantir limites mínimos e máximos baseados no lockDuration
    const minStalled = Math.max(5 * 60 * 1000, lockDuration * 0.15); // Mínimo 5min ou 15% do lockDuration
    const maxStalled = lockDuration * 0.6; // Máximo 60% do lockDuration
    
    stalledInterval = Math.max(minStalled, Math.min(stalledInterval, maxStalled));
    
    return Math.ceil(stalledInterval);
}

module.exports = {
    updateMetrics,
    getMetrics,
    getAdaptiveLockDuration,
    getAdaptiveStalledInterval,
    extractJobCharacteristics,
    MIN_SAMPLES_FOR_PERCENTILES
};
