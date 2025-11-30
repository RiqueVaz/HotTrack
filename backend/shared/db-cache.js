/**
 * Database Cache Module
 * Cache genérico em memória com TTL configurável
 * Suporta invalidação manual e cleanup automático
 */

class DbCache {
    constructor() {
        // Cache: key -> { value, expiresAt }
        this.cache = new Map();
        
        // Limite máximo de entradas no cache (evita crescimento indefinido)
        this.MAX_CACHE_SIZE = 10000;
        
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
        // Se cache está cheio, fazer limpeza agressiva
        if (this.cache.size >= this.MAX_CACHE_SIZE) {
            this._cleanup();
            
            // Se ainda estiver cheio após cleanup, remover 20% das entradas mais antigas
            if (this.cache.size >= this.MAX_CACHE_SIZE) {
                const entries = Array.from(this.cache.entries())
                    .sort((a, b) => a[1].expiresAt - b[1].expiresAt);
                const toRemove = Math.floor(this.MAX_CACHE_SIZE * 0.2);
                for (let i = 0; i < toRemove && i < entries.length; i++) {
                    this.cache.delete(entries[i][0]);
                }
            }
        }
        
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

    /**
     * Verifica se um bot está bloqueado por um chat
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     * @returns {boolean} - true se bloqueado
     */
    isBotBlocked(botId, chatId) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        return this.get(cacheKey) === true;
    }

    /**
     * Marca um bot como bloqueado por um chat
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     * @param {number} ttlHours - TTL em horas (padrão 24h)
     */
    markBotBlocked(botId, chatId, ttlHours = 24) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        this.set(cacheKey, true, ttlHours * 60 * 60 * 1000);
    }

    /**
     * Remove marcação de bloqueio (quando usuário desbloqueia)
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     */
    unmarkBotBlocked(botId, chatId) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        this.delete(cacheKey);
    }

    /**
     * Verifica se um bot está bloqueado usando botToken (fallback quando botId não disponível)
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     * @returns {boolean} - true se bloqueado
     */
    isBotTokenBlocked(botToken, chatId) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        return this.get(cacheKey) === true;
    }

    /**
     * Marca um bot como bloqueado usando botToken (fallback quando botId não disponível)
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     * @param {number} ttlHours - TTL em horas (padrão 24h)
     */
    markBotTokenBlocked(botToken, chatId, ttlHours = 24) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        this.set(cacheKey, true, ttlHours * 60 * 60 * 1000);
    }

    /**
     * Remove marcação de bloqueio usando botToken
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     */
    unmarkBotTokenBlocked(botToken, chatId) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        this.delete(cacheKey);
    }
}

// Exportar instância singleton
module.exports = new DbCache();

