/**
 * Telegram Rate Limiter com Redis
 * Implementa rate limiting para Telegram API usando Redis para estado compartilhado:
 * - 30 mensagens por segundo globalmente (por bot_token)
 * - 1 mensagem por segundo para o mesmo chat (bot_token + chat_id)
 * 
 * Mantém interface compatível com telegram-rate-limiter.js
 * Usa Redis para estado compartilhado entre múltiplos workers
 */

const { redisConnection, redisCircuitBreaker } = require('./queue');
const logger = require('../logger');

class TelegramRateLimiterBullMQ {
    constructor() {
        // Limites
        this.GLOBAL_CAPACITY = 30; // 30 mensagens por segundo
        this.GLOBAL_RATE = 30; // 30 tokens por segundo
        this.CHAT_CAPACITY = 1; // 1 mensagem por segundo
        this.CHAT_RATE = 1; // 1 token por segundo
    }

    /**
     * Repõe tokens de um bucket baseado no tempo decorrido (usando Redis)
     */
    async _refillTokens(key, capacity, ratePerSecond) {
        try {
            return await redisCircuitBreaker.execute(async () => {
                const now = Date.now();
                
                // Obter estado atual do bucket
                const bucket = await redisConnection.hgetall(key);
                
                const lastRefill = bucket.lastRefill ? parseInt(bucket.lastRefill) : now;
                const tokens = bucket.tokens ? parseFloat(bucket.tokens) : capacity;
                
                const elapsed = (now - lastRefill) / 1000; // segundos
                const tokensToAdd = elapsed * ratePerSecond;
                const newTokens = Math.min(capacity, tokens + tokensToAdd);
                
                // Atualizar bucket no Redis
                await redisConnection.hset(key, {
                    tokens: newTokens.toString(),
                    lastRefill: now.toString()
                });
                
                // Expirar após 10 minutos de inatividade
                await redisConnection.expire(key, 600);
                
                return newTokens;
            });
        } catch (error) {
            // Fallback: assumir que tem tokens disponíveis (fail open)
            logger.warn(`[Telegram Rate Limiter] Erro ao refill tokens (fail open):`, error.message);
            return capacity;
        }
    }

    /**
     * Aguarda se necessário para respeitar rate limits
     * Mantém interface compatível com telegram-rate-limiter.js
     * 
     * @param {string} botToken - Token do bot do Telegram
     * @param {number|string} chatId - ID do chat (opcional, para limite por chat)
     * @returns {Promise<void>}
     */
    async waitIfNeeded(botToken, chatId = null) {
        if (!botToken) {
            return; // Sem bot token, não precisa rate limiting
        }

        try {
            // Verificar e aguardar para limite global (por bot_token)
            const globalKey = `telegram-rate:global:${botToken}`;
            const globalTokens = await this._refillTokens(globalKey, this.GLOBAL_CAPACITY, this.GLOBAL_RATE);
            
            // Se não há tokens globais disponíveis, aguardar até ter pelo menos 1 token
            if (globalTokens < 1) {
                const tokensNeeded = 1 - globalTokens;
                const waitTime = (tokensNeeded / this.GLOBAL_RATE) * 1000;
                if (waitTime > 0) {
                    await new Promise(resolve => setTimeout(resolve, Math.ceil(waitTime)));
                    await this._refillTokens(globalKey, this.GLOBAL_CAPACITY, this.GLOBAL_RATE);
                }
            }
            
            // Consumir token global
            await redisCircuitBreaker.execute(async () => {
                const currentTokens = await redisConnection.hget(globalKey, 'tokens');
                const newTokens = Math.max(0, parseFloat(currentTokens || this.GLOBAL_CAPACITY) - 1);
                await redisConnection.hset(globalKey, 'tokens', newTokens.toString());
            });
            
            // Verificar e aguardar para limite por chat (se chatId fornecido)
            if (chatId !== null && chatId !== undefined) {
                const chatKey = `telegram-rate:chat:${botToken}:${chatId}`;
                const chatTokens = await this._refillTokens(chatKey, this.CHAT_CAPACITY, this.CHAT_RATE);
                
                // Se não há tokens disponíveis para este chat, aguardar até ter pelo menos 1 token
                if (chatTokens < 1) {
                    const tokensNeeded = 1 - chatTokens;
                    const waitTime = (tokensNeeded / this.CHAT_RATE) * 1000;
                    if (waitTime > 0) {
                        await new Promise(resolve => setTimeout(resolve, Math.ceil(waitTime)));
                        await this._refillTokens(chatKey, this.CHAT_CAPACITY, this.CHAT_RATE);
                    }
                }
                
                // Consumir token do chat
                await redisCircuitBreaker.execute(async () => {
                    const currentTokens = await redisConnection.hget(chatKey, 'tokens');
                    const newTokens = Math.max(0, parseFloat(currentTokens || this.CHAT_CAPACITY) - 1);
                    await redisConnection.hset(chatKey, 'tokens', newTokens.toString());
                });
            }
        } catch (error) {
            // Em caso de erro, não bloquear (fail open)
            logger.warn(`[Telegram Rate Limiter] Erro ao verificar rate limit (fail open):`, error.message);
        }
    }

    /**
     * Fecha (não há filas para fechar, apenas Redis)
     */
    async close() {
        // Nada para fechar, Redis é gerenciado externamente
    }
}

module.exports = new TelegramRateLimiterBullMQ();

