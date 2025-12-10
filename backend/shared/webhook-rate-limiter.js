/**
 * Webhook Rate Limiter usando Redis
 * Rate limiting para webhooks do Telegram usando Redis diretamente
 */

const { redisConnection } = require('./queue');
const { redisCircuitBreaker } = require('./queue');
const logger = require('../logger');

class WebhookRateLimiter {
    constructor() {
        this.WINDOW_MS = 60 * 1000; // 1 minuto
        this.MAX_REQUESTS = 100; // 100 requisições por minuto por bot
    }

    /**
     * Verifica e incrementa contador de requisições para um bot
     * @param {string|number} botId - ID do bot
     * @returns {Promise<boolean>} - true se dentro do limite, false se excedido
     */
    async checkAndIncrement(botId) {
        if (!botId) return true; // Sem botId, permite processamento
        
        const key = `webhook-rate:${botId}`;
        const now = Date.now();
        const windowStart = now - this.WINDOW_MS;
        
        try {
            return await redisCircuitBreaker.execute(async () => {
                // Usar pipeline do Redis para operações atômicas
                const pipeline = redisConnection.pipeline();
                
                // Remover entradas antigas (fora da janela)
                pipeline.zremrangebyscore(key, 0, windowStart);
                
                // Adicionar timestamp atual
                pipeline.zadd(key, now, `${now}-${Math.random()}`);
                
                // Contar requisições na janela atual (depois de adicionar)
                pipeline.zcard(key);
                
                // Definir expiração da chave (1 minuto + margem)
                pipeline.expire(key, Math.ceil(this.WINDOW_MS / 1000) + 60);
                
                const results = await pipeline.exec();
                
                // results[2][1] é o resultado do zcard (contagem depois de adicionar)
                const currentCount = results[2][1];
                
                // Verificar se está dentro do limite
                return currentCount <= this.MAX_REQUESTS;
            });
        } catch (error) {
            logger.warn(`[Webhook Rate Limiter] Erro ao verificar rate limit (fail open):`, error.message);
            return true; // Em caso de erro, permitir processamento (fail open)
        }
    }
}

class WorkerDisparoRateLimiter {
    constructor() {
        this.WINDOW_MS = 10 * 1000; // 10 segundos
        this.MAX_CONCURRENT = 5; // Máximo 5 workers simultâneos
    }

    /**
     * Incrementa contador de workers ativos e verifica se está dentro do limite
     * @param {string|number} sellerId - ID do seller
     * @returns {Promise<{allowed: boolean, active: number}>} - Se permitido e número de workers ativos
     */
    async incrementAndCheck(sellerId) {
        if (!sellerId) return { allowed: true, active: 0 };
        
        const key = `worker-disparo-rate:${sellerId}`;
        const now = Date.now();
        const windowStart = now - this.WINDOW_MS;
        
        try {
            return await redisCircuitBreaker.execute(async () => {
                // Usar pipeline do Redis para operações atômicas
                const pipeline = redisConnection.pipeline();
                
                // Remover entradas antigas (fora da janela)
                pipeline.zremrangebyscore(key, 0, windowStart);
                
                // Adicionar timestamp atual (representa um worker ativo)
                pipeline.zadd(key, now, `${now}-${Math.random()}`);
                
                // Contar workers ativos na janela atual (depois de adicionar)
                pipeline.zcard(key);
                
                // Definir expiração da chave
                pipeline.expire(key, Math.ceil(this.WINDOW_MS / 1000) + 10);
                
                const results = await pipeline.exec();
                
                // results[2][1] é o resultado do zcard (contagem depois de adicionar)
                const activeCount = results[2][1];
                
                // Verificar se está dentro do limite
                const allowed = activeCount <= this.MAX_CONCURRENT;
                
                return { allowed, active: activeCount };
            });
        } catch (error) {
            logger.warn(`[Worker Disparo Rate Limiter] Erro ao verificar rate limit (fail open):`, error.message);
            return { allowed: true, active: 0 }; // Em caso de erro, permitir processamento (fail open)
        }
    }

    /**
     * Decrementa contador de workers ativos
     * @param {string|number} sellerId - ID do seller
     */
    async decrement(sellerId) {
        if (!sellerId) return;
        
        const key = `worker-disparo-rate:${sellerId}`;
        const now = Date.now();
        const windowStart = now - this.WINDOW_MS;
        
        try {
            await redisCircuitBreaker.execute(async () => {
                // Remover uma entrada antiga (a mais antiga da janela atual)
                // Isso simula a remoção de um worker ativo
                const members = await redisConnection.zrangebyscore(key, windowStart, now, 'LIMIT', 0, 1);
                if (members.length > 0) {
                    await redisConnection.zrem(key, members[0]);
                }
            });
        } catch (error) {
            logger.warn(`[Worker Disparo Rate Limiter] Erro ao decrementar (não crítico):`, error.message);
        }
    }
}

module.exports = {
    webhook: new WebhookRateLimiter(),
    workerDisparo: new WorkerDisparoRateLimiter()
};

