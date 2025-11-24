/**
 * Database Cache Module
 * Cache genérico em memória com TTL configurável
 * Suporta invalidação manual e cleanup automático
 */

class DbCache {
    constructor() {
        // Cache: key -> { value, expiresAt }
        this.cache = new Map();
        
        // Cleanup periódico
        this.lastCleanup = Date.now();
        this.CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutos
    }

    /**
     * Obtém valor do cache se ainda não expirou
     * @param {string} key - Chave do cache
     * @returns {any|null} - Valor cacheado ou null se expirado/não existe
     */
    get(key) {
        const entry = this.cache.get(key);
        
        if (!entry) {
            return null;
        }
        
        // Verificar se expirou
        if (Date.now() > entry.expiresAt) {
            this.cache.delete(key);
            return null;
        }
        
        return entry.value;
    }

    /**
     * Salva valor no cache com TTL
     * @param {string} key - Chave do cache
     * @param {any} value - Valor a ser cacheado
     * @param {number} ttlMs - Time to live em milissegundos
     */
    set(key, value, ttlMs) {
        const expiresAt = Date.now() + ttlMs;
        this.cache.set(key, { value, expiresAt });
        
        // Cleanup periódico
        if (Date.now() - this.lastCleanup > this.CLEANUP_INTERVAL) {
            this._cleanup();
        }
    }

    /**
     * Remove entrada do cache
     * @param {string} key - Chave a ser removida
     */
    delete(key) {
        this.cache.delete(key);
    }

    /**
     * Limpa todo o cache
     */
    clear() {
        this.cache.clear();
    }

    /**
     * Remove entradas expiradas do cache
     */
    _cleanup() {
        const now = Date.now();
        for (const [key, entry] of this.cache.entries()) {
            if (now > entry.expiresAt) {
                this.cache.delete(key);
            }
        }
        this.lastCleanup = now;
    }

    /**
     * Obtém estatísticas do cache (útil para debug)
     * @returns {Object} - Estatísticas do cache
     */
    getStats() {
        const now = Date.now();
        let expired = 0;
        let active = 0;
        
        for (const entry of this.cache.values()) {
            if (now > entry.expiresAt) {
                expired++;
            } else {
                active++;
            }
        }
        
        return {
            total: this.cache.size,
            active,
            expired
        };
    }
}

// Exportar instância singleton
module.exports = new DbCache();

