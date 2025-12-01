/**
 * API Rate Limiter Global
 * Sistema genérico de rate limiting para qualquer API externa
 * Suporta:
 * - Rate limiting global por seller/token
 * - Cache por resource (ex: transaction_id)
 * - Retry automático em caso de 429
 * - Configuração por provedor
 */

const axios = require('axios');

class ApiRateLimiter {
    constructor() {
        // Rate limiters globais: `${provider}_${sellerId}` -> { lastRequest, queue }
        this.globalLimiters = new Map();
        
        // Cache por resource: `${provider}_${resourceId}` -> { data, timestamp }
        this.resourceCache = new Map();
        
        // Limite máximo de entradas no resourceCache (evita crescimento indefinido)
        this.MAX_RESOURCE_CACHE_SIZE = 5000;
        
        // Circuit breakers: `${provider}_${sellerId}` -> { failures: 0, successes: 0, openedAt: null, state: 'closed' }
        this.circuitBreakers = new Map();
        
        // Métricas por provedor: `${provider}_${sellerId}` -> { requests: 0, errors: 0, lastReset: timestamp }
        this.providerMetrics = new Map();
        
        // Configurações por provedor
        this.providerConfigs = new Map([
            ['pushinpay', {
                globalRateLimit: 500, // 2 requisições por segundo (reduzido de 1000)
                cacheTTL: 60_000, // 1 minuto de cache
                maxRetries: 1, // Reduzido de 3 para 1 (sem retry para evitar delays)
                retryDelay: 500, // Reduzido de 2000 para 500ms
                timeout: 20000, // Aumentado para 20 segundos para evitar timeouts em disparos simultâneos
                circuitBreakerThreshold: 5, // Abrir após 5 falhas consecutivas
                circuitBreakerTimeout: 300_000, // 5 minutos em estado aberto
                circuitBreakerSuccessThreshold: 2 // Fechar após 2 sucessos consecutivos
            }],
            ['syncpay', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['brpix', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 20000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['cnpay', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 20000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['oasyfy', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 20000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['pixup', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 20000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['wiinpay', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos (mais conservador para evitar 429)
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['paradise', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos (mais conservador para evitar 429)
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['ip-api', {
                globalRateLimit: 2000, // 30 req/min (mais conservador, margem de segurança)
                cacheTTL: 24 * 3600_000, // 24 horas (IPs não mudam de localização)
                maxRetries: 1, // Reduzir retries para evitar mais 429s
                retryDelay: 5000, // Aumentar delay entre retries
                timeout: 5000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['utmify', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos (mais conservador para evitar 429)
                cacheTTL: 60_000, // 1 minuto de cache
                maxRetries: 1, // Sem retry para evitar delays
                retryDelay: 500,
                timeout: 20000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }],
            ['default', {
                globalRateLimit: 1000, // 1 req/segundo padrão
                cacheTTL: 30_000, // 30 segundos padrão
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000,
                circuitBreakerThreshold: 5,
                circuitBreakerTimeout: 300_000,
                circuitBreakerSuccessThreshold: 2
            }]
        ]);
        
        // Cleanup periódico
        this.lastCleanup = Date.now();
        this.CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutos
        this.CACHE_MAX_AGE = 24 * 60 * 60 * 1000; // 24 horas (consistente com TTL de ip-api)
    }

    /**
     * Obtém configuração do provedor ou usa default
     */
    _getConfig(provider) {
        return this.providerConfigs.get(provider) || this.providerConfigs.get('default');
    }

    /**
     * Circuit Breaker: Verifica se circuito está aberto
     */
    _isCircuitOpen(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const breaker = this.circuitBreakers.get(key);
        if (!breaker) return false;

        const config = this._getConfig(provider);
        const now = Date.now();

        // Se está aberto, verificar se já passou o timeout
        if (breaker.state === 'open') {
            if (breaker.openedAt && (now - breaker.openedAt) >= config.circuitBreakerTimeout) {
                // Transição para half-open
                breaker.state = 'half-open';
                breaker.successes = 0;
            } else {
                return true; // Ainda está aberto
            }
        }

        return false;
    }

    /**
     * Circuit Breaker: Registra falha
     */
    _recordFailure(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const config = this._getConfig(provider);
        
        let breaker = this.circuitBreakers.get(key);
        if (!breaker) {
            breaker = { failures: 0, successes: 0, openedAt: null, state: 'closed' };
            this.circuitBreakers.set(key, breaker);
        }

        if (breaker.state === 'half-open') {
            // Se estava testando e falhou, abrir novamente
            breaker.state = 'open';
            breaker.openedAt = Date.now();
            breaker.failures = 0;
            breaker.successes = 0;
        } else {
            breaker.failures++;
            breaker.successes = 0; // Reset sucessos em caso de falha

            // Se atingiu o threshold, abrir circuito
            if (breaker.failures >= config.circuitBreakerThreshold) {
                breaker.state = 'open';
                breaker.openedAt = Date.now();
            }
        }
    }

    /**
     * Circuit Breaker: Registra sucesso
     */
    _recordSuccess(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const config = this._getConfig(provider);
        
        let breaker = this.circuitBreakers.get(key);
        if (!breaker) {
            breaker = { failures: 0, successes: 0, openedAt: null, state: 'closed' };
            this.circuitBreakers.set(key, breaker);
        }

        breaker.successes++;
        breaker.failures = 0; // Reset falhas em caso de sucesso

        // Se estava em half-open e atingiu threshold de sucesso, fechar
        if (breaker.state === 'half-open' && breaker.successes >= config.circuitBreakerSuccessThreshold) {
            breaker.state = 'closed';
            breaker.openedAt = null;
            breaker.successes = 0;
        } else if (breaker.state === 'open') {
            // Se estava aberto mas teve sucesso (não deveria acontecer), resetar
            breaker.state = 'closed';
            breaker.openedAt = null;
            breaker.successes = 0;
        }
    }

    /**
     * Métricas: Obtém ou cria métricas para provedor/seller
     */
    _getMetrics(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const now = Date.now();
        const METRICS_WINDOW = 60_000; // 60 segundos

        let metrics = this.providerMetrics.get(key);
        if (!metrics) {
            metrics = { requests: 0, errors: 0, lastReset: now };
            this.providerMetrics.set(key, metrics);
        }

        // Resetar métricas se passou a janela
        if (now - metrics.lastReset > METRICS_WINDOW) {
            metrics.requests = 0;
            metrics.errors = 0;
            metrics.lastReset = now;
        }

        return metrics;
    }

    /**
     * Métricas: Calcula taxa de erro
     */
    _calculateErrorRate(provider, sellerId) {
        const metrics = this._getMetrics(provider, sellerId);
        if (metrics.requests === 0) return 0;
        return metrics.errors / metrics.requests;
    }

    /**
     * Rate Limiting Adaptativo: Ajusta rate limit baseado em taxa de erro
     */
    _getAdaptiveRateLimit(provider, sellerId, baseRateLimit) {
        const errorRate = this._calculateErrorRate(provider, sellerId);
        const metrics = this._getMetrics(provider, sellerId);
        let adjustedRateLimit = baseRateLimit;

        if (errorRate > 0.2) {
            // Mais de 20% de erros: aumentar em 100% (máx 10000ms)
            adjustedRateLimit = Math.min(baseRateLimit * 2, 10000);
        } else if (errorRate > 0.1) {
            // Mais de 10% de erros: aumentar em 50% (máx 5000ms)
            adjustedRateLimit = Math.min(baseRateLimit * 1.5, 5000);
        } else if (errorRate < 0.01 && metrics.requests > 10) {
            // Menos de 1% de erros e pelo menos 10 requisições: reduzir em 10% (mín 1000ms)
            adjustedRateLimit = Math.max(baseRateLimit * 0.9, 1000);
        }

        return adjustedRateLimit;
    }

    /**
     * Limpa caches e limiters antigos
     */
    _cleanup() {
        const now = Date.now();
        
        // Limpar cache antigo
        for (const [key, cached] of this.resourceCache.entries()) {
            if (now - cached.timestamp > this.CACHE_MAX_AGE) {
                this.resourceCache.delete(key);
            }
        }
        
        // Se cache ainda estiver acima do limite, remover 20% das entradas mais antigas
        if (this.resourceCache.size >= this.MAX_RESOURCE_CACHE_SIZE) {
            const entries = Array.from(this.resourceCache.entries())
                .sort((a, b) => a[1].timestamp - b[1].timestamp);
            const toRemove = Math.floor(this.MAX_RESOURCE_CACHE_SIZE * 0.2);
            for (let i = 0; i < toRemove && i < entries.length; i++) {
                this.resourceCache.delete(entries[i][0]);
            }
        }
        
        // Limpar limiters não utilizados há muito tempo
        for (const [key, limiter] of this.globalLimiters.entries()) {
            if (now - limiter.lastRequest > 10 * 60 * 1000 && limiter.queue.length === 0 && limiter.activeRequests === 0) {
                this.globalLimiters.delete(key);
            }
        }
        
        this.lastCleanup = now;
    }

    /**
     * Aguarda se necessário para respeitar rate limit global
     */
    async _waitForRateLimit(provider, sellerId) {
        const key = `${provider}_${sellerId}`;
        const config = this._getConfig(provider);
        
        if (!this.globalLimiters.has(key)) {
            this.globalLimiters.set(key, {
                lastRequest: 0,
                queue: [],
                processing: false,
                activeRequests: 0,
                maxConcurrency: provider === 'ip-api' ? 2 : 1
            });
        }
        
        const limiter = this.globalLimiters.get(key);
        
        // Para ip-api, pushinpay, utmify e todos os provedores de PIX, usar fila real com concorrência limitada
        if (provider === 'ip-api' || provider === 'pushinpay' || provider === 'utmify' || provider === 'wiinpay' || provider === 'brpix' || provider === 'syncpay' || provider === 'cnpay' || provider === 'oasyfy' || provider === 'pixup' || provider === 'paradise') {
            return new Promise((resolve) => {
                limiter.queue.push(resolve);
                this._processQueue(provider, sellerId, limiter, config);
            });
        }
        
        // Para outros provedores, comportamento atual
        const now = Date.now();
        const timeSinceLastRequest = now - limiter.lastRequest;
        
        // Se fez requisição há menos tempo que o rate limit, aguardar
        if (timeSinceLastRequest < config.globalRateLimit) {
            const waitTime = config.globalRateLimit - timeSinceLastRequest;
            await new Promise(resolve => setTimeout(resolve, waitTime));
        }
        
        limiter.lastRequest = Date.now();
    }

    /**
     * Processa fila de requisições com concorrência limitada (para ip-api, pushinpay e utmify)
     */
    async _processQueue(provider, sellerId, limiter, config) {
        // Processar enquanto houver espaço na concorrência e itens na fila
        while (limiter.queue.length > 0 && limiter.activeRequests < limiter.maxConcurrency) {
            const resolve = limiter.queue.shift();
            limiter.activeRequests++;
            
            // Processar esta requisição em background
            (async () => {
                try {
                    const now = Date.now();
                    const timeSinceLastRequest = now - limiter.lastRequest;
                    
                    // Aguardar tempo necessário antes de processar
                    if (timeSinceLastRequest < config.globalRateLimit) {
                        const waitTime = config.globalRateLimit - timeSinceLastRequest;
                        await new Promise(res => setTimeout(res, waitTime));
                    }
                    
                    limiter.lastRequest = Date.now();
                    resolve(); // Libera a requisição para continuar
                } catch (error) {
                    // Em caso de erro, ainda liberar a requisição
                    resolve();
                    console.error(`[API Rate Limiter] Erro ao processar fila para ${provider}:`, error);
                } finally {
                    limiter.activeRequests--;
                    // Tentar processar próximo item da fila
                    if (limiter.queue.length > 0) {
                        this._processQueue(provider, sellerId, limiter, config);
                    }
                }
            })();
        }
    }

    /**
     * Verifica cache e retorna se disponível
     */
    _getCached(provider, resourceId) {
        const key = `${provider}_${resourceId}`;
        const cached = this.resourceCache.get(key);
        
        if (!cached) return null;
        
        const config = this._getConfig(provider);
        const now = Date.now();
        
        // Se cache expirou, remover e retornar null
        if (now - cached.timestamp > config.cacheTTL) {
            this.resourceCache.delete(key);
            return null;
        }
        
        return cached.data;
    }

    /**
     * Salva no cache
     */
    _setCache(provider, resourceId, data) {
        // Se cache está cheio, fazer limpeza
        if (this.resourceCache.size >= this.MAX_RESOURCE_CACHE_SIZE) {
            this._cleanup();
            
            // Se ainda estiver cheio após cleanup, remover 20% das entradas mais antigas
            if (this.resourceCache.size >= this.MAX_RESOURCE_CACHE_SIZE) {
                const entries = Array.from(this.resourceCache.entries())
                    .sort((a, b) => a[1].timestamp - b[1].timestamp);
                const toRemove = Math.floor(this.MAX_RESOURCE_CACHE_SIZE * 0.2);
                for (let i = 0; i < toRemove && i < entries.length; i++) {
                    this.resourceCache.delete(entries[i][0]);
                }
            }
        }
        
        const key = `${provider}_${resourceId}`;
        this.resourceCache.set(key, {
            data,
            timestamp: Date.now()
        });
    }

    /**
     * Faz requisição HTTP com rate limiting, cache e retry automático
     * 
     * @param {Object} options
     * @param {string} options.provider - Nome do provedor (ex: 'pushinpay', 'syncpay')
     * @param {string|number} options.sellerId - ID do seller (para rate limiting global)
     * @param {string} options.resourceId - ID do resource (ex: transaction_id) para cache
     * @param {string} options.method - Método HTTP ('get', 'post', 'put', 'delete')
     * @param {string} options.url - URL da requisição
     * @param {Object} options.headers - Headers HTTP
     * @param {Object} options.data - Body da requisição (para POST/PUT)
     * @param {Object} options.params - Query params (para GET)
     * @param {boolean} options.skipCache - Se true, ignora cache
     * @param {Function} options.responseTransformer - Função para transformar resposta antes de cachear
     * 
     * @returns {Promise<Object>} Resposta da API
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
        responseTransformer = null
    }) {
        // Cleanup periódico
        if (Date.now() - this.lastCleanup > this.CLEANUP_INTERVAL) {
            this._cleanup();
        }

        const config = this._getConfig(provider);

        // Verificar Circuit Breaker antes de fazer requisição
        if (this._isCircuitOpen(provider, sellerId)) {
            const error = new Error(`Circuit breaker aberto para ${provider}. Muitos erros recentes.`);
            error.circuitBreakerOpen = true;
            throw error;
        }

        // Atualizar métricas: incrementar requests
        const metrics = this._getMetrics(provider, sellerId);
        metrics.requests++;

        // Verificar cache (apenas para GET e se resourceId fornecido)
        if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
            const cached = this._getCached(provider, resourceId);
            if (cached !== null) {
                return cached;
            }
        }

        // Aplicar rate limit adaptativo
        const adaptiveRateLimit = this._getAdaptiveRateLimit(provider, sellerId, config.globalRateLimit);
        const originalRateLimit = config.globalRateLimit;
        config.globalRateLimit = adaptiveRateLimit;

        // Aguardar rate limit
        await this._waitForRateLimit(provider, sellerId);

        // Restaurar rate limit original
        config.globalRateLimit = originalRateLimit;

        // Fazer requisição com retry
        let lastError = null;
        for (let attempt = 0; attempt < config.maxRetries; attempt++) {
            try {
                const requestConfig = {
                    method,
                    url,
                    headers,
                    timeout: config.timeout,
                    ...(data && { data }),
                    ...(params && { params })
                };

                const response = await axios(requestConfig);

                // Registrar sucesso no Circuit Breaker e métricas
                this._recordSuccess(provider, sellerId);

                // Se sucesso, cachear (apenas GET com resourceId)
                if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
                    const dataToCache = responseTransformer 
                        ? responseTransformer(response.data) 
                        : response.data;
                    this._setCache(provider, resourceId, dataToCache);
                }

                return response.data;

            } catch (error) {
                lastError = error;

                // Se for 429, tratamento especial
                if (error.response?.status === 429) {
                    // Para ip-api, não fazer retry em caso de 429 - cache já deve ter o valor
                    if (provider === 'ip-api') {
                        // Log apenas muito ocasionalmente (0.1% das vezes) para não saturar logs
                        if (attempt === 0 && Math.random() < 0.001) {
                            console.warn(
                                `[API Rate Limiter] Rate limit 429 para ${provider} (seller ${sellerId}). ` +
                                `Não tentando novamente (cache deve ter o valor).`
                            );
                        }
                        // Não fazer retry para ip-api em caso de 429
                        throw error;
                    }
                    
                    // Para utmify, quando recebe 429, aguardar mais tempo antes de falhar
                    if (provider === 'utmify') {
                        // Registrar falha no Circuit Breaker e métricas
                        this._recordFailure(provider, sellerId);
                        const metrics = this._getMetrics(provider, sellerId);
                        metrics.errors++;

                        const retryAfter = Math.max(5, parseInt( // Mínimo 5 segundos
                            error.response.headers['retry-after'] || 
                            error.response.headers['Retry-After'] || 
                            '5'
                        ));
                        
                        if (attempt === 0 && Math.random() < 0.1) { // Log 10% das vezes
                            console.warn(
                                `[API Rate Limiter] Rate limit 429 para utmify (seller ${sellerId}). ` +
                                `Aguardando ${retryAfter}s antes de falhar...`
                            );
                        }
                        
                        // Aguardar antes de lançar erro (não fazer retry, apenas aguardar e falhar)
                        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                        throw error;
                    }
                    
                    // Para wiinpay, quando recebe 429, extrair tempo de retry da mensagem
                    if (provider === 'wiinpay') {
                        // Registrar falha no Circuit Breaker e métricas
                        this._recordFailure(provider, sellerId);
                        const metrics = this._getMetrics(provider, sellerId);
                        metrics.errors++;

                        let retryAfter = 30; // Padrão: 30 segundos
                        
                        // Tentar extrair do header retry-after primeiro
                        const headerRetryAfter = error.response?.headers['retry-after'] || error.response?.headers['Retry-After'];
                        if (headerRetryAfter) {
                            retryAfter = Math.max(5, parseInt(headerRetryAfter));
                        } else {
                            // Tentar extrair da mensagem de erro
                            const errorMessage = error.response?.data?.message || error.message || '';
                            const match = errorMessage.match(/retry in (\d+) seconds?/i);
                            if (match) {
                                retryAfter = Math.max(5, parseInt(match[1]));
                            }
                        }
                        
                        if (attempt === 0 && Math.random() < 0.1) { // Log 10% das vezes
                            console.warn(
                                `[API Rate Limiter] Rate limit 429 para wiinpay (seller ${sellerId}). ` +
                                `Aguardando ${retryAfter}s antes de retry...`
                            );
                        }
                        
                        // Aguardar antes de retry
                        if (attempt < config.maxRetries - 1) {
                            await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                            continue;
                        }
                        
                        // Se última tentativa, aguardar e lançar erro
                        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
                        throw error;
                    }
                    
                    // Registrar falha no Circuit Breaker e métricas
                    this._recordFailure(provider, sellerId);
                    const metrics = this._getMetrics(provider, sellerId);
                    metrics.errors++;

                    // Para outros provedores, usar backoff exponencial com jitter
                    const MAX_BACKOFF_DELAY = 30_000; // 30 segundos máximo
                    const baseDelay = config.retryDelay * Math.pow(2, attempt);
                    const backoffDelay = Math.min(baseDelay, MAX_BACKOFF_DELAY);
                    const jitter = backoffDelay * 0.2 * (Math.random() * 2 - 1); // ±20%
                    const finalDelay = Math.max(1000, backoffDelay + jitter); // Mínimo 1 segundo

                    // Tentar usar retry-after do header se disponível
                    const headerRetryAfter = error.response?.headers['retry-after'] || error.response?.headers['Retry-After'];
                    const retryAfter = headerRetryAfter 
                        ? Math.max(1000, parseInt(headerRetryAfter) * 1000)
                        : finalDelay;
                    
                    // Log apenas na última tentativa ou muito ocasionalmente
                    if (attempt === config.maxRetries - 1 || (attempt === 0 && Math.random() < 0.01)) {
                        console.warn(
                            `[API Rate Limiter] Rate limit 429 para ${provider} (seller ${sellerId}). ` +
                            `Tentativa ${attempt + 1}/${config.maxRetries}. Aguardando ${Math.round(retryAfter / 1000)}s...`
                        );
                    }

                    if (attempt < config.maxRetries - 1) {
                        await new Promise(resolve => 
                            setTimeout(resolve, retryAfter)
                        );
                        continue;
                    }
                }

                // Para outros erros (5xx), usar backoff exponencial
                if (error.response?.status >= 500) {
                    // Registrar falha no Circuit Breaker e métricas
                    this._recordFailure(provider, sellerId);
                    const metrics = this._getMetrics(provider, sellerId);
                    metrics.errors++;

                    const MAX_BACKOFF_DELAY = 30_000; // 30 segundos máximo
                    const baseDelay = config.retryDelay * Math.pow(2, attempt);
                    const backoffDelay = Math.min(baseDelay, MAX_BACKOFF_DELAY);
                    const jitter = backoffDelay * 0.2 * (Math.random() * 2 - 1); // ±20%
                    const finalDelay = Math.max(1000, backoffDelay + jitter); // Mínimo 1 segundo

                    if (attempt < config.maxRetries - 1) {
                        await new Promise(resolve => 
                            setTimeout(resolve, finalDelay)
                        );
                        continue;
                    }
                }

                // Para outros erros (4xx exceto 429), não fazer retry (erro do cliente)
                if (error.response?.status >= 400 && error.response?.status < 500 && error.response?.status !== 429) {
                    // Registrar falha no Circuit Breaker e métricas
                    this._recordFailure(provider, sellerId);
                    const metrics = this._getMetrics(provider, sellerId);
                    metrics.errors++;
                    throw error; // Não fazer retry para erros do cliente
                }

                // Para outros erros (timeout, network, etc), usar backoff exponencial
                if (attempt < config.maxRetries - 1) {
                    const MAX_BACKOFF_DELAY = 30_000; // 30 segundos máximo
                    const baseDelay = config.retryDelay * Math.pow(2, attempt);
                    const backoffDelay = Math.min(baseDelay, MAX_BACKOFF_DELAY);
                    const jitter = backoffDelay * 0.2 * (Math.random() * 2 - 1); // ±20%
                    const finalDelay = Math.max(1000, backoffDelay + jitter); // Mínimo 1 segundo
                    
                    await new Promise(resolve => setTimeout(resolve, finalDelay));
                    continue;
                }
            }
        }

        // Se chegou aqui, todas as tentativas falharam
        // Registrar falha final no Circuit Breaker e métricas
        if (lastError && !lastError.circuitBreakerOpen) {
            this._recordFailure(provider, sellerId);
            const metrics = this._getMetrics(provider, sellerId);
            metrics.errors++;
        }
        throw lastError || new Error(`Falha ao fazer requisição para ${provider}`);
    }

    /**
     * Helper para consultar status de transação (GET)
     */
    async getTransactionStatus({
        provider,
        sellerId,
        transactionId,
        url,
        headers,
        responseTransformer = null
    }) {
        return this.request({
            provider,
            sellerId,
            resourceId: transactionId,
            method: 'get',
            url,
            headers,
            responseTransformer
        });
    }

    /**
     * Helper para criar transação (POST)
     */
    async createTransaction({
        provider,
        sellerId,
        method = 'post',
        url,
        headers,
        data
    }) {
        return this.request({
            provider,
            sellerId,
            method,
            url,
            headers,
            data,
            skipCache: true // POST nunca usa cache
        });
    }

    /**
     * Limpa cache de um resource específico
     */
    clearCache(provider, resourceId) {
        const key = `${provider}_${resourceId}`;
        this.resourceCache.delete(key);
    }

    /**
     * Limpa todo o cache de um provedor
     */
    clearProviderCache(provider) {
        for (const key of this.resourceCache.keys()) {
            if (key.startsWith(`${provider}_`)) {
                this.resourceCache.delete(key);
            }
        }
    }

    /**
     * Adiciona ou atualiza configuração de um provedor
     */
    setProviderConfig(provider, config) {
        this.providerConfigs.set(provider, {
            ...this.providerConfigs.get('default'),
            ...config
        });
    }
}

// Exportar instância singleton
module.exports = new ApiRateLimiter();

