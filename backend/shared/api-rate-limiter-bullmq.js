/**
 * API Rate Limiter com BullMQ
 * Sistema de rate limiting usando BullMQ para filas persistentes
 * Suporta:
 * - Rate limiting por provedor e seller
 * - Cache por resource usando Redis
 * - Retry automático em caso de 429
 * - Circuit breakers
 * - Prioridades (high para PIX generate)
 */

const { Queue } = require('bullmq');
const axios = require('axios');
const { redisConnection } = require('./queue');
const redisCache = require('./redis-cache');
const logger = require('../logger');

class ApiRateLimiterBullMQ {
    constructor() {
        // Filas por provedor e seller: `${provider}-${sellerId}`
        this.queues = new Map();
        
        // Circuit breakers: `${provider}_${sellerId}` -> { failures: 0, successes: 0, openedAt: null, state: 'closed' }
        this.circuitBreakers = new Map();
        
        // Métricas por provedor: `${provider}_${sellerId}` -> { requests: 0, errors: 0, lastReset: timestamp }
        this.providerMetrics = new Map();
        
        // Configurações por provedor
        this.providerConfigs = new Map([
            ['pushinpay', {
                limiter: { max: 2, duration: 1000 }, // 2 req/segundo
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 1,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['syncpay', {
                limiter: { max: 1, duration: 2000 }, // 1 req/2s
                concurrency: 1,
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['brpix', {
                limiter: { max: 1, duration: 2000 }, // 1 req/2s
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['cnpay', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['oasyfy', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['pixup', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['wiinpay', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['paradise', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['ip-api', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 2,
                timeout: 5000,
                cacheTTL: 24 * 3600_000, // 24 horas
                maxRetries: 1,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['utmify', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 1,
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 1,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['default', {
                limiter: { max: 1, duration: 1000 },
                concurrency: 1,
                timeout: 10000,
                cacheTTL: 30_000,
                maxRetries: 3,
                circuitBreakerThreshold: 10, // Aumentado de 5 para 10
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }]
        ]);
    }

    /**
     * Obtém configuração do provedor
     */
    _getConfig(provider) {
        return this.providerConfigs.get(provider) || this.providerConfigs.get('default');
    }

    /**
     * Obtém ou cria fila para provedor e seller
     */
    _getQueue(provider, sellerId) {
        const key = `${provider}-${sellerId}`;
        
        if (!this.queues.has(key)) {
            const config = this._getConfig(provider);
            const queue = new Queue(`api-${key}`, {
                connection: redisConnection,
                defaultJobOptions: {
                    attempts: config.maxRetries,
                    backoff: {
                        type: 'exponential',
                        delay: 2000
                    },
                    removeOnComplete: { age: 3600 }, // 1 hora
                    removeOnFail: { age: 86400 } // 24 horas
                }
            });
            
            this.queues.set(key, queue);
            
            // Criar worker para esta fila se ainda não existir (síncrono, não bloqueia)
            // O worker será garantido antes de adicionar jobs no método request()
            try {
                const { getOrCreateWorker } = require('../worker/api-rate-limiter-worker');
                const worker = getOrCreateWorker(`api-${key}`);
                if (worker) {
                    logger.debug(`[API Rate Limiter] Worker criado para fila api-${key}`);
                }
            } catch (error) {
                // Não lançar erro aqui - será tratado no request() quando tentar usar
                logger.debug(`[API Rate Limiter] Worker será criado sob demanda para api-${key}`);
            }
        }
        
        return this.queues.get(key);
    }

    /**
     * Verifica Circuit Breaker
     */
    _isCircuitOpen(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const breaker = this.circuitBreakers.get(key);
        
        if (!breaker || breaker.state === 'closed') {
            return false;
        }
        
        if (breaker.state === 'open') {
            const now = Date.now();
            if (breaker.openedAt && (now - breaker.openedAt) >= this._getConfig(provider).circuitBreakerTimeout) {
                // Mover para half-open
                breaker.state = 'half-open';
                breaker.successCount = 0;
                return false;
            }
            return true;
        }
        
        return false;
    }

    /**
     * Registra sucesso no Circuit Breaker
     */
    _recordSuccess(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const breaker = this.circuitBreakers.get(key);
        
        if (breaker) {
            if (breaker.state === 'half-open') {
                breaker.successCount++;
                if (breaker.successCount >= this._getConfig(provider).circuitBreakerSuccessThreshold) {
                    breaker.state = 'closed';
                    breaker.failures = 0;
                    breaker.successCount = 0;
                }
            } else {
                breaker.failures = 0;
            }
        }
    }

    /**
     * Registra falha no Circuit Breaker
     */
    _recordFailure(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const config = this._getConfig(provider);
        
        if (!this.circuitBreakers.has(key)) {
            this.circuitBreakers.set(key, {
                failures: 0,
                successes: 0,
                openedAt: null,
                state: 'closed',
                successCount: 0
            });
        }
        
        const breaker = this.circuitBreakers.get(key);
        breaker.failures++;
        
        if (breaker.state === 'half-open') {
            breaker.state = 'open';
            breaker.openedAt = Date.now();
            breaker.successCount = 0;
        } else if (breaker.failures >= config.circuitBreakerThreshold) {
            breaker.state = 'open';
            breaker.openedAt = Date.now();
        }
    }

    /**
     * Obtém métricas do provedor
     */
    _getMetrics(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        
        if (!this.providerMetrics.has(key)) {
            this.providerMetrics.set(key, {
                requests: 0,
                errors: 0,
                lastReset: Date.now()
            });
        }
        
        return this.providerMetrics.get(key);
    }

    /**
     * Faz requisição HTTP com rate limiting via BullMQ
     */
    async request({
        provider,
        sellerId,
        resourceId = null,
        method = 'get',
        url,
        headers = {},
        data = null,
        params = null,
        skipCache = false,
        responseTransformer = null,
        priority = 'normal'
    }) {
        const config = this._getConfig(provider);

        // Verificar Circuit Breaker
        if (this._isCircuitOpen(provider, sellerId)) {
            const error = new Error(`Circuit breaker aberto para ${provider}. Muitos erros recentes.`);
            error.circuitBreakerOpen = true;
            throw error;
        }

        // Atualizar métricas
        const metrics = this._getMetrics(provider, sellerId);
        metrics.requests++;

        // Verificar cache (apenas para GET e se resourceId fornecido)
        if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
            const cacheKey = `api-cache:${provider}:${resourceId}`;
            const cached = await redisCache.get(cacheKey);
            if (cached !== null) {
                return cached;
            }
        }

        // Obter fila e garantir que worker existe
        const queue = this._getQueue(provider, sellerId);
        const queueName = `api-${provider}-${sellerId}`;
        
        // Garantir que worker foi criado ANTES de adicionar job (crítico!)
        // Tentar até 3 vezes com delay crescente
        let worker = null;
        const maxRetries = 3;
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                const { getOrCreateWorker } = require('../worker/api-rate-limiter-worker');
                worker = getOrCreateWorker(queueName);
                
                if (worker) {
                    break; // Worker criado com sucesso
                }
                
                // Se não conseguiu criar, aguardar antes de tentar novamente
                if (attempt < maxRetries - 1) {
                    await new Promise(resolve => setTimeout(resolve, 100 * (attempt + 1)));
                }
            } catch (error) {
                logger.warn(`[API Rate Limiter] Tentativa ${attempt + 1} de criar worker para ${queueName} falhou:`, error.message);
                if (attempt < maxRetries - 1) {
                    await new Promise(resolve => setTimeout(resolve, 100 * (attempt + 1)));
                } else {
                    logger.error(`[API Rate Limiter] Erro crítico ao garantir worker para ${queueName} após ${maxRetries} tentativas:`, error.message);
                    throw new Error(`Worker não disponível para ${queueName}: ${error.message}`);
                }
            }
        }
        
        if (!worker) {
            throw new Error(`Não foi possível criar worker para ${queueName} após ${maxRetries} tentativas`);
        }

        // Adicionar job na fila
        const job = await queue.add(
            'api-request',
            {
                method,
                url,
                headers,
                data,
                params,
                timeout: config.timeout
            },
            {
                priority: priority === 'high' ? 1 : 5, // Alta prioridade = número menor
                jobId: `${provider}-${sellerId}-${Date.now()}-${Math.random()}`
            }
        );

        // Aguardar resultado com timeout e verificação de método
        try {
            // Verificar se job.finished existe (pode não existir se worker não foi criado)
            if (!job || typeof job.finished !== 'function') {
                throw new Error(`Job não possui método finished(). Worker pode não ter sido criado para api-${provider}-${sellerId}`);
            }

            // Aguardar resultado com timeout (máximo 30 segundos ou timeout do config)
            const timeoutMs = Math.max(config.timeout || 10000, 30000);
            const result = await Promise.race([
                job.finished(),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error(`Timeout aguardando job ${job.id} (${timeoutMs}ms)`)), timeoutMs)
                )
            ]);

            // Registrar sucesso
            this._recordSuccess(provider, sellerId);

            // Cachear resultado (apenas GET com resourceId)
            if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
                const cacheKey = `api-cache:${provider}:${resourceId}`;
                const dataToCache = responseTransformer 
                    ? responseTransformer(result) 
                    : result;
                await redisCache.set(cacheKey, dataToCache, config.cacheTTL);
            }

            return result;
        } catch (error) {
            // Não registrar falha no circuit breaker para erros específicos
            const shouldRecordFailure = !error.circuitBreakerOpen && 
                                       error.response?.status !== 405 && // Método incorreto - erro de configuração
                                       error.response?.status !== 429;   // Rate limit - esperado, não é falha do provedor
            
            if (shouldRecordFailure) {
                this._recordFailure(provider, sellerId);
            }
            
            const metrics = this._getMetrics(provider, sellerId);
            metrics.errors++;
            
            throw error;
        }
    }

    /**
     * Cria transação (wrapper para request com prioridade alta)
     */
    async createTransaction({
        provider,
        sellerId,
        method = 'post', // Default POST para transações
        url,
        headers,
        data,
        priority = 'high' // PIX generate tem prioridade alta
    }) {
        return this.request({
            provider,
            sellerId,
            method: method.toLowerCase(), // Garantir lowercase
            url,
            headers,
            data,
            skipCache: true, // POST nunca usa cache
            priority
        });
    }

    /**
     * Fecha todas as filas
     */
    async close() {
        for (const queue of this.queues.values()) {
            await queue.close();
        }
        this.queues.clear();
    }
}

module.exports = new ApiRateLimiterBullMQ();

