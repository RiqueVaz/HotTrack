const Redis = require('ioredis');

// Railway automaticamente fornece REDIS_URL
// Formato: redis://default:password@host:port ou rediss:// para TLS
let redisConnection = null;

function getRedisConnection() {
    if (!redisConnection) {
        const redisUrl = process.env.REDIS_URL;
        
        if (!redisUrl) {
            throw new Error('REDIS_URL não configurado. Certifique-se de que o Redis está adicionado no Railway.');
        }

        redisConnection = new Redis(redisUrl, {
            maxRetriesPerRequest: 3,
            enableReadyCheck: true,
            retryStrategy(times) {
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            // Railway Redis geralmente não precisa de configurações especiais
            // mas pode precisar de TLS dependendo do plano
            ...(redisUrl.includes('rediss://') && {
                tls: {
                    rejectUnauthorized: false
                }
            })
        });

        redisConnection.on('connect', () => {
            console.log('[REDIS] Conectado ao Redis Railway');
        });

        redisConnection.on('error', (error) => {
            console.error('[REDIS] Erro na conexão:', error);
        });

        redisConnection.on('ready', () => {
            console.log('[REDIS] Redis pronto para uso');
        });

        redisConnection.on('close', () => {
            console.log('[REDIS] Conexão fechada');
        });

        redisConnection.on('reconnecting', () => {
            console.log('[REDIS] Reconectando ao Redis...');
        });
    }

    return redisConnection;
}

module.exports = { getRedisConnection };

