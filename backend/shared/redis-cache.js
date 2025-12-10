/**
 * Redis Cache Helper
 * Sistema de cache usando Redis com fallback para Map em memória
 * Suporta TTL automático e operações atômicas
 */

const { redisConnection, redisCircuitBreaker } = require('./queue');

class RedisCache {
    constructor() {
        // Fallback Map para quando Redis está indisponível
        this.fallbackCache = new Map();
        this.useFallback = false;
    }

    /**
     * Obtém valor do cache
     * @param {string} key - Chave do cache
     * @returns {Promise<any|null>} - Valor do cache ou null se não existir
     */
    async get(key) {
        try {
            return await redisCircuitBreaker.execute(async () => {
                const value = await redisConnection.get(key);
                if (value === null) {
                    // Verificar fallback
                    if (this.fallbackCache.has(key)) {
                        const cached = this.fallbackCache.get(key);
                        if (cached.expiresAt > Date.now()) {
                            return cached.value;
                        }
                        this.fallbackCache.delete(key);
                    }
                    return null;
                }
                return JSON.parse(value);
            });
        } catch (error) {
            // Fallback para Map em memória
            if (!this.useFallback) {
                const logger = require('../logger');
                logger.warn(`[RedisCache] Redis indisponível, usando fallback em memória:`, error.message);
                this.useFallback = true;
            }
            
            if (this.fallbackCache.has(key)) {
                const cached = this.fallbackCache.get(key);
                if (cached.expiresAt > Date.now()) {
                    return cached.value;
                }
                this.fallbackCache.delete(key);
            }
            return null;
        }
    }

    /**
     * Define valor no cache com TTL
     * @param {string} key - Chave do cache
     * @param {any} value - Valor a ser armazenado
     * @param {number} ttlMs - TTL em milissegundos
     * @returns {Promise<void>}
     */
    async set(key, value, ttlMs = 0) {
        try {
            await redisCircuitBreaker.execute(async () => {
                const serialized = JSON.stringify(value);
                if (ttlMs > 0) {
                    await redisConnection.setex(key, Math.ceil(ttlMs / 1000), serialized);
                } else {
                    await redisConnection.set(key, serialized);
                }
            });
            
            // Também armazenar no fallback
            this.fallbackCache.set(key, {
                value,
                expiresAt: ttlMs > 0 ? Date.now() + ttlMs : Number.MAX_SAFE_INTEGER
            });
            
            // Limpar fallback se Redis voltou
            if (this.useFallback) {
                this.useFallback = false;
            }
        } catch (error) {
            // Fallback para Map em memória
            if (!this.useFallback) {
                const logger = require('../logger');
                logger.warn(`[RedisCache] Redis indisponível, usando fallback em memória:`, error.message);
                this.useFallback = true;
            }
            
            this.fallbackCache.set(key, {
                value,
                expiresAt: ttlMs > 0 ? Date.now() + ttlMs : Number.MAX_SAFE_INTEGER
            });
        }
    }

    /**
     * Remove valor do cache
     * @param {string} key - Chave do cache
     * @returns {Promise<void>}
     */
    async delete(key) {
        try {
            await redisCircuitBreaker.execute(async () => {
                await redisConnection.del(key);
            });
        } catch (error) {
            // Ignorar erro, continuar com fallback
        }
        
        this.fallbackCache.delete(key);
    }

    /**
     * Verifica se chave existe no cache
     * @param {string} key - Chave do cache
     * @returns {Promise<boolean>}
     */
    async has(key) {
        try {
            return await redisCircuitBreaker.execute(async () => {
                const exists = await redisConnection.exists(key);
                if (exists) return true;
                
                // Verificar fallback
                if (this.fallbackCache.has(key)) {
                    const cached = this.fallbackCache.get(key);
                    if (cached.expiresAt > Date.now()) {
                        return true;
                    }
                    this.fallbackCache.delete(key);
                }
                return false;
            });
        } catch (error) {
            // Fallback
            if (this.fallbackCache.has(key)) {
                const cached = this.fallbackCache.get(key);
                if (cached.expiresAt > Date.now()) {
                    return true;
                }
                this.fallbackCache.delete(key);
            }
            return false;
        }
    }

    /**
     * Limpa cache antigo do fallback (chamado periodicamente)
     */
    _cleanupFallback() {
        const now = Date.now();
        for (const [key, cached] of this.fallbackCache.entries()) {
            if (cached.expiresAt <= now) {
                this.fallbackCache.delete(key);
            }
        }
    }
}

// Instância singleton
const redisCache = new RedisCache();

// Cleanup periódico do fallback (a cada 5 minutos)
setInterval(() => {
    redisCache._cleanupFallback();
}, 5 * 60 * 1000);

module.exports = redisCache;

