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
        
        // Configurações por provedor
        this.providerConfigs = new Map([
            ['pushinpay', {
                globalRateLimit: 500, // 2 requisições por segundo (reduzido de 1000)
                cacheTTL: 60_000, // 1 minuto de cache
                maxRetries: 1, // Reduzido de 3 para 1 (sem retry para evitar delays)
                retryDelay: 500, // Reduzido de 2000 para 500ms
                timeout: 20000 // Aumentado para 20 segundos para evitar timeouts em disparos simultâneos
            }],
            ['syncpay', {
                globalRateLimit: 2000, // 1 requisição a cada 2 segundos
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000
            }],
            ['wiinpay', {
                globalRateLimit: 1000,
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000
            }],
            ['paradise', {
                globalRateLimit: 1000,
                cacheTTL: 60_000,
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000
            }],
            ['ip-api', {
                globalRateLimit: 2000, // 30 req/min (mais conservador, margem de segurança)
                cacheTTL: 24 * 3600_000, // 24 horas (IPs não mudam de localização)
                maxRetries: 1, // Reduzir retries para evitar mais 429s
                retryDelay: 5000, // Aumentar delay entre retries
                timeout: 5000
            }],
            ['default', {
                globalRateLimit: 1000, // 1 req/segundo padrão
                cacheTTL: 30_000, // 30 segundos padrão
                maxRetries: 3,
                retryDelay: 2000,
                timeout: 10000
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
        
        // Para ip-api, usar fila real com concorrência limitada
        if (provider === 'ip-api') {
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
     * Processa fila de requisições com concorrência limitada (para ip-api)
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

        // Verificar cache (apenas para GET e se resourceId fornecido)
        if (!skipCache && method.toLowerCase() === 'get' && resourceId) {
            const cached = this._getCached(provider, resourceId);
            if (cached !== null) {
                return cached;
            }
        }

        // Aguardar rate limit
        await this._waitForRateLimit(provider, sellerId);

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

                // Se for 429, tratamento especial para ip-api (não retry)
                if (error.response?.status === 429) {
                    // Para ip-api, não fazer retry em caso de 429 - cache já deve ter o valor
                    if (provider === 'ip-api') {
                        // Log apenas ocasionalmente para não saturar logs
                        if (attempt === 0 || Math.random() < 0.05) {
                            console.warn(
                                `[API Rate Limiter] Rate limit 429 para ${provider} (seller ${sellerId}). ` +
                                `Não tentando novamente (cache deve ter o valor).`
                            );
                        }
                        // Não fazer retry para ip-api em caso de 429
                        throw error;
                    }
                    
                    // Para outros provedores, comportamento normal
                    const retryAfter = parseInt(
                        error.response.headers['retry-after'] || 
                        error.response.headers['Retry-After'] || 
                        String(config.retryDelay / 1000)
                    );
                    
                    // Log apenas ocasionalmente para não saturar logs
                    if (attempt === 0 || attempt === config.maxRetries - 1 || Math.random() < 0.1) {
                        console.warn(
                            `[API Rate Limiter] Rate limit 429 para ${provider} (seller ${sellerId}). ` +
                            `Tentativa ${attempt + 1}/${config.maxRetries}. Aguardando ${retryAfter}s...`
                        );
                    }

                    if (attempt < config.maxRetries - 1) {
                        await new Promise(resolve => 
                            setTimeout(resolve, retryAfter * 1000)
                        );
                        continue;
                    }
                }

                // Para outros erros, aguardar delay padrão antes de retry
                if (attempt < config.maxRetries - 1) {
                    const delay = config.retryDelay * (attempt + 1); // Backoff exponencial
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
        }

        // Se chegou aqui, todas as tentativas falharam
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

