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

const { Queue, QueueEvents } = require('bullmq');
const axios = require('axios');
const { redisConnection } = require('./queue');
const redisCache = require('./redis-cache');
const logger = require('../logger');

class ApiRateLimiterBullMQ {
    constructor() {
        // Filas por provedor e seller: `${provider}-${sellerId}`
        this.queues = new Map();
        
        // QueueEvents por fila para aguardar conclusão de jobs
        this.queueEvents = new Map();
        
        // Circuit breakers: `${provider}_${sellerId}` -> { failures: 0, successes: 0, openedAt: null, state: 'closed' }
        this.circuitBreakers = new Map();
        
        // Métricas por provedor: `${provider}_${sellerId}` -> { requests: 0, errors: 0, lastReset: timestamp }
        this.providerMetrics = new Map();
        
        // Configurações do sistema híbrido
        this.QUEUE_THRESHOLD = 3; // Reduzido de 10 para 3 - se há mais de 3 jobs na fila, usar requisição direta
        this.MAX_DIRECT_TIMEOUT = 15000; // Reduzido de 20s para 15s - timeout máximo para requisições diretas
        this.DYNAMIC_TIMEOUT_MULTIPLIER = 1000; // Reduzido de 2000 para 1000 - multiplicador para timeout dinâmico (1s por job)
        this.MIN_QUEUE_TIMEOUT = 10000; // Reduzido de 15s para 10s - timeout mínimo quando usa fila
        this.FALLBACK_ON_TIMEOUT = true; // Tentar requisição direta se timeout na fila
        
        // Cache de contagem de jobs por fila (evita múltiplas queries Redis)
        this.queueSizeCache = new Map(); // queueName -> { count: number, timestamp: number }
        this.QUEUE_SIZE_CACHE_TTL = 2000; // Cache por 2 segundos
        
        // Configurações por provedor
        this.providerConfigs = new Map([
            ['pushinpay', {
                limiter: { max: 2, duration: 1000 }, // 2 req/segundo
                concurrency: 3, // Aumentado de 1 para 3 para processar mais jobs simultaneamente
                timeout: 15000, // Reduzido de 20s para 15s para evitar 499
                cacheTTL: 60_000,
                maxRetries: 1,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['syncpay', {
                limiter: { max: 1, duration: 2000 }, // 1 req/2s
                concurrency: 2, // Aumentado de 1 para 2 (mais conservador, 1 req/2s)
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['brpix', {
                limiter: { max: 1, duration: 2000 }, // 1 req/2s
                concurrency: 3, // Aumentado de 1 para 3 para processar mais jobs simultaneamente
                timeout: 15000, // Reduzido de 20s para 15s para evitar 499
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['cnpay', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 3, // Aumentado de 1 para 3 para processar mais jobs simultaneamente
                timeout: 15000, // Reduzido de 20s para 15s para evitar 499
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['oasyfy', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 3, // Aumentado de 1 para 3 para processar mais jobs simultaneamente
                timeout: 15000, // Reduzido de 20s para 15s para evitar 499
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['pixup', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 3, // Aumentado de 1 para 3 para processar mais jobs simultaneamente
                timeout: 15000, // Reduzido de 20s para 15s para evitar 499
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['wiinpay', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 2, // Aumentado de 1 para 2 (mais conservador)
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['paradise', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 2, // Aumentado de 1 para 2 (mais conservador)
                timeout: 10000,
                cacheTTL: 60_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['ip-api', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 2,
                timeout: 5000,
                cacheTTL: 24 * 3600_000, // 24 horas
                maxRetries: 1,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['utmify', {
                limiter: { max: 1, duration: 2000 },
                concurrency: 2, // Aumentado de 1 para 2 (mais conservador)
                timeout: 20000,
                cacheTTL: 60_000,
                maxRetries: 1,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
                circuitBreakerSuccessThreshold: 2
            }],
            ['default', {
                limiter: { max: 1, duration: 1000 },
                concurrency: 1,
                timeout: 10000,
                cacheTTL: 30_000,
                maxRetries: 3,
                circuitBreakerThreshold: 20, // Aumentado de 10 para 20 (mais tolerante a erros)
                circuitBreakerTimeout: 60000, // Reduzido de 5min para 1min (recupera mais rápido)
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
     * Obtém ou cria QueueEvents para uma fila
     */
    _getQueueEvents(queueName) {
        if (!this.queueEvents.has(queueName)) {
            const queueEvents = new QueueEvents(queueName, {
                connection: redisConnection
            });
            // Aumentar limite de listeners para evitar warnings quando há muitas requisições simultâneas
            // Cada requisição adiciona 2 listeners (completed + failed)
            // 200 permite ~100 requisições simultâneas por fila, que é suficiente para a maioria dos casos
            queueEvents.setMaxListeners(200);
            this.queueEvents.set(queueName, queueEvents);
        }
        return this.queueEvents.get(queueName);
    }

    /**
     * Aguarda conclusão de um job usando QueueEvents
     */
    async _waitForJobCompletion(queueName, jobId, timeoutMs) {
        const queueEvents = this._getQueueEvents(queueName);
        
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                cleanup();
                const timeoutError = new Error(`Timeout aguardando job ${jobId} (${timeoutMs}ms)`);
                timeoutError.timeout = true; // Marcar como timeout para não registrar como falha no circuit breaker
                timeoutError.jobId = jobId;
                timeoutError.queueName = queueName;
                reject(timeoutError);
            }, timeoutMs);

            const cleanup = () => {
                clearTimeout(timeout);
                queueEvents.off('completed', onCompleted);
                queueEvents.off('failed', onFailed);
            };

            const onCompleted = ({ jobId: eventJobId, returnvalue }) => {
                if (eventJobId === jobId) {
                    cleanup();
                    resolve(returnvalue);
                }
            };

            const onFailed = ({ jobId: eventJobId, failedReason }) => {
                if (eventJobId === jobId) {
                    cleanup();
                    const error = new Error(failedReason || 'Job failed');
                    error.jobId = jobId;
                    reject(error);
                }
            };

            queueEvents.on('completed', onCompleted);
            queueEvents.on('failed', onFailed);
        });
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
     * Obtém tamanho da fila (com cache para evitar múltiplas queries)
     */
    async _getQueueSize(queue) {
        const queueName = queue.name;
        const now = Date.now();
        
        // Verificar cache
        const cached = this.queueSizeCache.get(queueName);
        if (cached && (now - cached.timestamp) < this.QUEUE_SIZE_CACHE_TTL) {
            return cached.count;
        }
        
        // Buscar tamanho real da fila
        try {
            const [waiting, delayed, active] = await Promise.all([
                queue.getWaitingCount(),
                queue.getDelayedCount(),
                queue.getActiveCount()
            ]);
            
            const total = waiting + delayed + active;
            
            // Atualizar cache
            this.queueSizeCache.set(queueName, {
                count: total,
                timestamp: now
            });
            
            return total;
        } catch (error) {
            logger.warn(`[API Rate Limiter] Erro ao obter tamanho da fila ${queueName}:`, error.message);
            // Em caso de erro, assumir fila vazia (fail open)
            return 0;
        }
    }

    /**
     * Aplica rate limiting direto via Redis (sliding window log)
     * Usado quando fila está cheia ou para fallback
     */
    async _applyDirectRateLimit(provider, sellerId, config) {
        const key = `api-rate-limit:${provider}:${sellerId}`;
        const now = Date.now();
        const windowMs = config.limiter.duration * 1000;
        const maxRequests = config.limiter.max;
        
        try {
            // Remover requisições antigas da janela
            await redisConnection.zremrangebyscore(key, 0, now - windowMs);
            
            // Contar requisições na janela
            const count = await redisConnection.zcard(key);
            
            if (count >= maxRequests) {
                // Calcular tempo de espera necessário
                const oldestRequests = await redisConnection.zrange(key, 0, 0, 'WITHSCORES');
                if (oldestRequests && oldestRequests.length > 0) {
                    const oldestTimestamp = parseInt(oldestRequests[1]);
                    const waitTime = (oldestTimestamp + windowMs) - now;
                    
                    if (waitTime > 0) {
                        logger.debug(`[API Rate Limiter] Aguardando ${waitTime}ms para respeitar rate limit de ${provider} (${count}/${maxRequests} na janela)`);
                        await new Promise(resolve => setTimeout(resolve, waitTime));
                        // Remover requisições antigas novamente após espera
                        await redisConnection.zremrangebyscore(key, 0, Date.now() - windowMs);
                    }
                }
            }
            
            // Adicionar requisição atual
            await redisConnection.zadd(key, now, `${now}-${Math.random()}`);
            await redisConnection.expire(key, Math.ceil(windowMs / 1000) + 10); // +10s de margem
        } catch (error) {
            logger.warn(`[API Rate Limiter] Erro ao aplicar rate limiting direto para ${provider} (fail open):`, error.message);
            // Fail open: continuar mesmo se rate limiting falhar
        }
    }

    /**
     * Faz requisição HTTP direta (bypass da fila BullMQ)
     * Usado quando fila está cheia ou como fallback
     */
    async _makeDirectRequest({
        provider,
        sellerId,
        method,
        url,
        headers,
        data,
        params,
        config,
        skipCache,
        resourceId,
        responseTransformer
    }) {
        logger.debug(`[API Rate Limiter] Fazendo requisição direta para ${provider} (bypass da fila)`);
        
        // Aplicar rate limiting manualmente
        await this._applyDirectRateLimit(provider, sellerId, config);
        
        try {
            const axios = require('axios');
            const response = await axios({
                method,
                url,
                headers,
                data,
                params,
                timeout: Math.min(config.timeout || 15000, this.MAX_DIRECT_TIMEOUT)
            });
            
            // Registrar sucesso
            this._recordSuccess(provider, sellerId);
            
            // Cachear resultado se aplicável
            if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
                const cacheKey = `api-cache:${provider}:${resourceId}`;
                const dataToCache = responseTransformer ? responseTransformer(response.data) : response.data;
                await redisCache.set(cacheKey, dataToCache, config.cacheTTL);
            }
            
            return response.data;
        } catch (error) {
            const shouldRecordFailure = !error.circuitBreakerOpen && 
                                       error.response?.status !== 405 && 
                                       error.response?.status !== 429;
            
            if (shouldRecordFailure) {
                this._recordFailure(provider, sellerId);
            }
            
            const metrics = this._getMetrics(provider, sellerId);
            metrics.errors++;
            
            throw error;
        }
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

        // Obter fila e verificar tamanho
        const queue = this._getQueue(provider, sellerId);
        const queueName = `api-${provider}-${sellerId}`;
        
        // Verificar tamanho da fila antes de decidir estratégia
        const queueSize = await this._getQueueSize(queue);
        // Para PIX generation (priority === 'high'), SEMPRE usar requisição direta se há jobs na fila
        // Para outras requisições, usar direta apenas se fila está cheia (threshold)
        const shouldUseDirectRequest = (priority === 'high' && queueSize > 0) || queueSize > this.QUEUE_THRESHOLD;
        
        // Se fila está muito cheia ou é alta prioridade com jobs na fila, usar requisição direta
        if (shouldUseDirectRequest) {
            logger.debug(`[API Rate Limiter] Fila ${queueName} tem ${queueSize} jobs. Usando requisição direta (threshold: ${this.QUEUE_THRESHOLD})`);
            
            try {
                return await this._makeDirectRequest({
                    provider,
                    sellerId,
                    method,
                    url,
                    headers,
                    data,
                    params,
                    config,
                    skipCache,
                    resourceId,
                    responseTransformer
                });
            } catch (error) {
                // Se requisição direta falhar, não tentar fila (já está sobrecarregada)
                throw error;
            }
        }
        
        // Modo normal: usar fila BullMQ
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
                    // Se não conseguir criar worker, tentar requisição direta como fallback
                    logger.warn(`[API Rate Limiter] Tentando requisição direta como fallback para ${queueName}`);
                    return await this._makeDirectRequest({
                        provider,
                        sellerId,
                        method,
                        url,
                        headers,
                        data,
                        params,
                        config,
                        skipCache,
                        resourceId,
                        responseTransformer
                    });
                }
            }
        }
        
        if (!worker) {
            // Se não conseguir criar worker, tentar requisição direta como fallback
            logger.warn(`[API Rate Limiter] Worker não disponível para ${queueName}. Tentando requisição direta como fallback`);
            return await this._makeDirectRequest({
                provider,
                sellerId,
                method,
                url,
                headers,
                data,
                params,
                config,
                skipCache,
                resourceId,
                responseTransformer
            });
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

        // Aguardar resultado usando QueueEvents
        // Timeout dinâmico baseado no tamanho da fila
        // Mínimo 10s + 1s por job na fila, máximo 15s para evitar 499
        const dynamicTimeout = Math.min(
            Math.max(this.MIN_QUEUE_TIMEOUT, this.MIN_QUEUE_TIMEOUT + (queueSize * this.DYNAMIC_TIMEOUT_MULTIPLIER)),
            15000 // Máximo de 15s para evitar 499
        );
        const timeoutMs = Math.min(dynamicTimeout, config.timeout || 15000);
        
        try {
            logger.debug(`[API Rate Limiter] Aguardando job ${job.id} na fila ${queueName} (timeout: ${timeoutMs}ms, fila: ${queueSize} jobs)`);
            
            const result = await this._waitForJobCompletion(queueName, job.id, timeoutMs);

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
            // Se timeout na fila e fallback está habilitado, tentar requisição direta
            if (error.timeout && this.FALLBACK_ON_TIMEOUT) {
                logger.warn(`[API Rate Limiter] Timeout na fila ${queueName} após ${timeoutMs}ms. Tentando requisição direta como fallback`);
                
                try {
                    return await this._makeDirectRequest({
                        provider,
                        sellerId,
                        method,
                        url,
                        headers,
                        data,
                        params,
                        config,
                        skipCache,
                        resourceId,
                        responseTransformer
                    });
                } catch (fallbackError) {
                    // Se fallback também falhar, lançar erro original (timeout na fila)
                    logger.error(`[API Rate Limiter] Fallback direto também falhou para ${queueName}:`, fallbackError.message);
                    throw error; // Lançar erro original (timeout na fila)
                }
            }
            
            // Não registrar falha no circuit breaker para erros específicos
            const shouldRecordFailure = !error.circuitBreakerOpen && 
                                       !error.timeout && // Timeout - pode ser temporário (fila cheia), não indica problema real com o provedor
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
     * Fecha todas as filas e QueueEvents
     */
    async close() {
        for (const queue of this.queues.values()) {
            await queue.close();
        }
        for (const queueEvents of this.queueEvents.values()) {
            await queueEvents.close();
        }
        this.queues.clear();
        this.queueEvents.clear();
    }
}

module.exports = new ApiRateLimiterBullMQ();

