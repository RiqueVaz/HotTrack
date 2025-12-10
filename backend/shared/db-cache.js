/**
 * Database Cache Module
 * Cache usando Redis com TTL configurável
 * Mantém a mesma interface do cache em memória para compatibilidade
 */

const redisCache = require('./redis-cache');

class DbCache {
    constructor() {
        // Usa redis-cache.js internamente
        // Não precisa mais de Map, cleanup manual, etc. - Redis gerencia TTL automaticamente
    }

    /**
     * Obtém valor do cache
     * @param {string} key - Chave do cache
     * @returns {Promise<any|null>} - Valor cacheado ou null se expirado/não existe
     */
    async get(key) {
        return await redisCache.get(key);
    }

    /**
     * Salva valor no cache com TTL
     * @param {string} key - Chave do cache
     * @param {any} value - Valor a ser cacheado
     * @param {number} ttlMs - Time to live em milissegundos
     */
    async set(key, value, ttlMs) {
        // Converter TTL de ms para segundos (Redis usa segundos)
        const ttlSeconds = Math.ceil(ttlMs / 1000);
        await redisCache.set(key, value, ttlSeconds);
    }

    /**
     * Remove entrada do cache
     * @param {string} key - Chave a ser removida
     */
    async delete(key) {
        await redisCache.delete(key);
    }

    /**
     * Limpa todo o cache (não implementado para evitar apagar tudo acidentalmente)
     */
    async clear() {
        // Não implementado - Redis não tem método para limpar tudo facilmente
        // e pode ser perigoso apagar todo o cache
        const logger = require('../logger');
        logger.warn(`[DbCache] clear() não implementado para evitar apagar todo o cache Redis.`);
    }

    /**
     * Obtém estatísticas do cache (não disponível para Redis sem operações custosas)
     * @returns {Promise<Object>} - Estatísticas do cache
     */
    async getStats() {
        // Redis não fornece estatísticas fáceis sem scan de todas as chaves
        // Retornar objeto vazio para manter compatibilidade
        return {
            total: 0,
            active: 0,
            expired: 0,
            note: 'Estatísticas não disponíveis para cache Redis'
        };
    }

    /**
     * Verifica se um bot está bloqueado por um chat
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     * @returns {Promise<boolean>} - true se bloqueado
     */
    async isBotBlocked(botId, chatId) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        const value = await this.get(cacheKey);
        return value === true;
    }

    /**
     * Marca um bot como bloqueado por um chat
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     * @param {number} ttlHours - TTL em horas (padrão 24h)
     */
    async markBotBlocked(botId, chatId, ttlHours = 24) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        await this.set(cacheKey, true, ttlHours * 60 * 60 * 1000);
    }

    /**
     * Remove marcação de bloqueio (quando usuário desbloqueia)
     * @param {number} botId - ID do bot
     * @param {string|number} chatId - ID do chat
     */
    async unmarkBotBlocked(botId, chatId) {
        const cacheKey = `blocked:${botId}:${chatId}`;
        await this.delete(cacheKey);
    }

    /**
     * Verifica se um bot está bloqueado usando botToken (fallback quando botId não disponível)
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     * @returns {Promise<boolean>} - true se bloqueado
     */
    async isBotTokenBlocked(botToken, chatId) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        const value = await this.get(cacheKey);
        return value === true;
    }

    /**
     * Marca um bot como bloqueado usando botToken (fallback quando botId não disponível)
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     * @param {number} ttlHours - TTL em horas (padrão 24h)
     */
    async markBotTokenBlocked(botToken, chatId, ttlHours = 24) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        await this.set(cacheKey, true, ttlHours * 60 * 60 * 1000);
    }

    /**
     * Remove marcação de bloqueio usando botToken
     * @param {string} botToken - Token do bot
     * @param {string|number} chatId - ID do chat
     */
    async unmarkBotTokenBlocked(botToken, chatId) {
        const cacheKey = `blocked_token:${botToken}:${chatId}`;
        await this.delete(cacheKey);
    }
}

// Exportar instância singleton
module.exports = new DbCache();

