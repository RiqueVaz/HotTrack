const postgres = require('postgres');

if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL não configurado.');
}

const parsePositiveInt = (value) => {
    if (!value) return undefined;
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
};

const resolveSslOption = () => {
    const rawMode = (process.env.PGSSLMODE || process.env.PG_SSL_MODE || process.env.DATABASE_SSL || 'require').toLowerCase();

    if (['disable', 'off', 'false', '0'].includes(rawMode)) {
        return false;
    }

    // Para Railway (e a maioria dos provedores gerenciados) precisamos aceitar certificados autoassinados
    return { rejectUnauthorized: false };
};

const sqlTx = postgres(process.env.DATABASE_URL, {
    ssl: resolveSslOption(),
    // Pool de conexões configurável via variável de ambiente
    // Padrão: 10 conexões (suficiente para carga atual)
    // Para alta concorrência, aumentar via PG_POOL_MAX ou PG_MAX_CONNECTIONS
    // Cada conexão consome ~65-70MB de memória quando idle
    max: parsePositiveInt(process.env.PG_POOL_MAX || process.env.PG_MAX_CONNECTIONS) || 10,
    
    // Timeout maior para dar tempo ao pgbouncer processar quando há fila
    // Em picos de tráfego, o pgbouncer pode demorar mais para alocar conexões
    connect_timeout: parsePositiveInt(process.env.PG_CONNECT_TIMEOUT) || 60,
    
    // Idle timeout reduzido para 120s (2 min) para fechar conexões idle mais rapidamente
    // Reduz desperdício de memória (cada conexão idle consome ~65-70MB)
    idle_timeout: parsePositiveInt(process.env.PG_IDLE_TIMEOUT) || 120,
    
    // PgBouncer-friendly: desabilita prepared statements (incompatível com transaction mode)
    prepare: false,
    
    // Max lifetime menor para evitar conexões "zumbis" que podem causar problemas no pgbouncer
    max_lifetime: parsePositiveInt(process.env.PG_MAX_LIFETIME) || (60 * 10), // 10 minutos
    
    connection: {
        application_name: 'hottrack_app'
    }
});

// Função unificada de retry para queries SQL
async function sqlWithRetry(query, ...args) {
    let retries = 5;
    let delay = 1000;
    let params = [];
    const isTemplate = Array.isArray(query);
    const isDirectQuery = !isTemplate && typeof query === 'string';
    const isImmediate = !isTemplate && !isDirectQuery;
    const templateValues = isTemplate ? [...args] : [];

    if (isDirectQuery) {
        if (args.length > 0) {
            params = args[0] ?? [];
        }
        if (args.length > 1) {
            const maybeOptions = args[1];
            if (typeof maybeOptions === 'number') {
                retries = maybeOptions;
                if (args.length > 2 && typeof args[2] === 'number') {
                    delay = args[2];
                }
            } else if (maybeOptions && typeof maybeOptions === 'object') {
                if (typeof maybeOptions.retries === 'number') {
                    retries = maybeOptions.retries;
                }
                if (typeof maybeOptions.delay === 'number') {
                    delay = maybeOptions.delay;
                }
            }
        }
    }

    const execute = async () => {
        if (isTemplate) {
            return await sqlTx(query, ...templateValues);
        }
        if (isImmediate) {
            return await query;
        }
        return await sqlTx.unsafe(query, params);
    };

    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            return await execute();
        } catch (error) {
            // Detectar CONNECT_TIMEOUT especificamente
            const isConnectTimeout = 
                error.message?.includes('CONNECT_TIMEOUT') ||
                error.message?.includes('connect timeout') ||
                error.message?.includes('write CONNECT_TIMEOUT') ||
                error.code === 'ETIMEDOUT' ||
                error.code === 'ECONNREFUSED';
            
            const isRetryable =
                isConnectTimeout ||
                (typeof error.message === 'string' && (
                    error.message.includes('fetch failed') ||
                    error.message.includes('Connection terminated unexpectedly') ||
                    error.message.includes('Client has encountered a connection error') ||
                    error.message.includes('write ECONNRESET') ||
                    error.message.includes('server closed the connection') ||
                    error.message.includes('connection pool') ||
                    error.message.includes('too many connections') ||
                    error.message.includes('socket hang up')
                )) ||
                ['ECONNRESET', 'ETIMEDOUT', 'ECONNREFUSED', 'ESOCKETTIMEDOUT'].includes(error.code);

            if (isRetryable && attempt < retries - 1) {
                // Backoff exponencial com jitter para evitar thundering herd
                const baseDelay = delay * Math.pow(2, attempt);
                const jitter = Math.random() * 1000;
                const backoffDelay = Math.min(baseDelay + jitter, 15000); // Máximo de 15 segundos
                
                // Só logar em tentativas críticas para reduzir spam de logs
                if (isConnectTimeout || attempt >= retries - 2) {
                    console.warn(`[DB] Tentativa ${attempt + 1}/${retries} falhou (${error.message?.substring(0, 100)}). Tentando novamente em ${Math.round(backoffDelay)}ms...`);
                }
                
                await new Promise(res => setTimeout(res, backoffDelay));
            } else {
                // Log detalhado do erro final
                if (isConnectTimeout) {
                    console.error(`[DB] CONNECT_TIMEOUT após ${retries} tentativas. Pool pode estar esgotado ou servidor sobrecarregado.`);
                    console.error(`[DB] Erro completo:`, error.message);
                    console.error(`[DB] Config: max=${sqlTx.options.max}, connect_timeout=${sqlTx.options.connect_timeout}s`);
                }
                throw error;
            }
        }
    }
}

module.exports = {
    sqlTx,
    sqlWithRetry
};

