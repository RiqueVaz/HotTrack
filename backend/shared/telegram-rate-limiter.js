/**
 * Telegram Rate Limiter
 * Implementa Token Bucket algorithm para respeitar limites do Telegram API:
 * - 30 mensagens por segundo globalmente (por bot_token)
 * - 1 mensagem por segundo para o mesmo chat (bot_token + chat_id)
 */

class TelegramRateLimiter {
    constructor() {
        // Buckets globais: bot_token -> { tokens, lastRefill }
        this.globalBuckets = new Map();
        
        // Buckets por chat: `${bot_token}_${chat_id}` -> { tokens, lastRefill }
        this.chatBuckets = new Map();
        
        // Limites
        this.GLOBAL_CAPACITY = 30; // 30 mensagens por segundo
        this.GLOBAL_RATE = 30; // 30 tokens por segundo
        this.CHAT_CAPACITY = 1; // 1 mensagem por segundo
        this.CHAT_RATE = 1; // 1 token por segundo
        
        // Cleanup periódico de buckets não utilizados (a cada 5 minutos)
        this.lastCleanup = Date.now();
        this.CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutos
    }

    /**
     * Repõe tokens de um bucket baseado no tempo decorrido
     */
    _refillTokens(bucket, capacity, ratePerSecond) {
        const now = Date.now();
        const elapsed = (now - bucket.lastRefill) / 1000; // segundos
        const tokensToAdd = elapsed * ratePerSecond;
        
        bucket.tokens = Math.min(capacity, bucket.tokens + tokensToAdd);
        bucket.lastRefill = now;
        
        return bucket.tokens;
    }

    /**
     * Limpa buckets não utilizados há mais de 10 minutos
     */
    _cleanupBuckets() {
        const now = Date.now();
        const maxAge = 10 * 60 * 1000; // 10 minutos
        
        // Limpar buckets globais
        for (const [key, bucket] of this.globalBuckets.entries()) {
            if (now - bucket.lastRefill > maxAge) {
                this.globalBuckets.delete(key);
            }
        }
        
        // Limpar buckets por chat
        for (const [key, bucket] of this.chatBuckets.entries()) {
            if (now - bucket.lastRefill > maxAge) {
                this.chatBuckets.delete(key);
            }
        }
        
        this.lastCleanup = now;
    }

    /**
     * Aguarda se necessário para respeitar rate limits
     * @param {string} botToken - Token do bot do Telegram
     * @param {number|string} chatId - ID do chat (opcional, para limite por chat)
     * @returns {Promise<void>}
     */
    async waitIfNeeded(botToken, chatId = null) {
        const now = Date.now();
        
        // Cleanup periódico
        if (now - this.lastCleanup > this.CLEANUP_INTERVAL) {
            this._cleanupBuckets();
        }
        
        // Verificar e aguardar para limite global (por bot_token)
        const globalKey = botToken;
        if (!this.globalBuckets.has(globalKey)) {
            this.globalBuckets.set(globalKey, {
                tokens: this.GLOBAL_CAPACITY,
                lastRefill: now
            });
        }
        
        const globalBucket = this.globalBuckets.get(globalKey);
        this._refillTokens(globalBucket, this.GLOBAL_CAPACITY, this.GLOBAL_RATE);
        
        // Se não há tokens globais disponíveis, aguardar até ter pelo menos 1 token
        if (globalBucket.tokens < 1) {
            const tokensNeeded = 1 - globalBucket.tokens;
            const waitTime = (tokensNeeded / this.GLOBAL_RATE) * 1000;
            if (waitTime > 0) {
                await new Promise(resolve => setTimeout(resolve, Math.ceil(waitTime)));
                this._refillTokens(globalBucket, this.GLOBAL_CAPACITY, this.GLOBAL_RATE);
            }
        }
        
        // Consumir token global
        globalBucket.tokens -= 1;
        
        // Verificar e aguardar para limite por chat (se chatId fornecido)
        if (chatId !== null && chatId !== undefined) {
            const chatKey = `${botToken}_${chatId}`;
            if (!this.chatBuckets.has(chatKey)) {
                this.chatBuckets.set(chatKey, {
                    tokens: this.CHAT_CAPACITY,
                    lastRefill: now
                });
            }
            
            const chatBucket = this.chatBuckets.get(chatKey);
            this._refillTokens(chatBucket, this.CHAT_CAPACITY, this.CHAT_RATE);
            
            // Se não há tokens disponíveis para este chat, aguardar até ter pelo menos 1 token
            if (chatBucket.tokens < 1) {
                const tokensNeeded = 1 - chatBucket.tokens;
                const waitTime = (tokensNeeded / this.CHAT_RATE) * 1000;
                if (waitTime > 0) {
                    await new Promise(resolve => setTimeout(resolve, Math.ceil(waitTime)));
                    this._refillTokens(chatBucket, this.CHAT_CAPACITY, this.CHAT_RATE);
                }
            }
            
            // Consumir token do chat
            chatBucket.tokens -= 1;
        }
    }
}

// Exportar instância singleton
module.exports = new TelegramRateLimiter();

