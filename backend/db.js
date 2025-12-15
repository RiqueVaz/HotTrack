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
    // Padrão reduzido para 5 conexões para evitar esgotamento de memória compartilhada
    // Para alta concorrência, aumentar via PG_POOL_MAX ou PG_MAX_CONNECTIONS
    // Cada conexão consome ~65-70MB de memória quando idle
    max: parsePositiveInt(process.env.PG_POOL_MAX || process.env.PG_MAX_CONNECTIONS) || 5,
    
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
            // #region agent log
            const queryStartTime = Date.now();
            const poolStateBefore = { max: sqlTx.options.max, idle: sqlTx.options.idle_timeout };
            fetch('http://127.0.0.1:7242/ingest/79bbecc6-d356-47c9-93f2-f295c2828f98',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'db.js:94',message:'Query execution start',data:{attempt,isTemplate,isDirectQuery,queryType:isTemplate?'template':isDirectQuery?'direct':'immediate',poolMax:poolStateBefore.max},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'B'})}).catch(()=>{});
            // #endregion
            const result = await execute();
            // #region agent log
            const queryDuration = Date.now() - queryStartTime;
            fetch('http://127.0.0.1:7242/ingest/79bbecc6-d356-47c9-93f2-f295c2828f98',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'db.js:96',message:'Query execution success',data:{attempt,queryDuration,resultSize:Array.isArray(result)?result.length:'non-array'},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'B'})}).catch(()=>{});
            // #endregion
            return result;
        } catch (error) {
            // Detectar erro de memória compartilhada do PostgreSQL especificamente
            const isSharedMemoryError = 
                typeof error.message === 'string' && (
                    error.message.includes('could not resize shared memory segment') ||
                    error.message.includes('No space left on device') ||
                    error.message.includes('shared memory segment')
                );
            
            // #region agent log
            const poolStateOnError = { max: sqlTx.options.max, idle: sqlTx.options.idle_timeout };
            fetch('http://127.0.0.1:7242/ingest/79bbecc6-d356-47c9-93f2-f295c2828f98',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({location:'db.js:104',message:'Query execution error',data:{attempt,isSharedMemoryError,errorCode:error.code,errorMessage:error.message?.substring(0,200),queryType:isTemplate?'template':isDirectQuery?'direct':'immediate',poolMax:poolStateOnError.max,isRetryable},timestamp:Date.now(),sessionId:'debug-session',runId:'run1',hypothesisId:'A,B,C,D,E'})}).catch(()=>{});
            // #endregion
            
            // Detectar CONNECT_TIMEOUT especificamente
            const isConnectTimeout = 
                error.message?.includes('CONNECT_TIMEOUT') ||
                error.message?.includes('connect timeout') ||
                error.message?.includes('write CONNECT_TIMEOUT') ||
                error.code === 'ETIMEDOUT' ||
                error.code === 'ECONNREFUSED';
            
            const isRetryable =
                isSharedMemoryError ||
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
                // Para erros de memória compartilhada, usar delay maior e mais retries
                let backoffDelay;
                if (isSharedMemoryError) {
                    // Delay maior para erros de memória compartilhada (5-30 segundos)
                    const baseDelay = 5000; // 5 segundos base
                    const exponentialDelay = baseDelay * Math.pow(2, attempt);
                    const jitter = Math.random() * 5000; // Jitter de até 5 segundos
                    backoffDelay = Math.min(exponentialDelay + jitter, 30000); // Máximo de 30 segundos
                    
                    console.warn(`[DB] Erro de memória compartilhada detectado (tentativa ${attempt + 1}/${retries}). Aguardando ${Math.round(backoffDelay / 1000)}s antes de tentar novamente...`);
                    console.warn(`[DB] Mensagem: ${error.message?.substring(0, 200)}`);
                } else {
                    // Backoff exponencial padrão com jitter para outros erros
                    const baseDelay = delay * Math.pow(2, attempt);
                    const jitter = Math.random() * 1000;
                    backoffDelay = Math.min(baseDelay + jitter, 15000); // Máximo de 15 segundos
                    
                    // Só logar em tentativas críticas para reduzir spam de logs
                    if (isConnectTimeout || attempt >= retries - 2) {
                        console.warn(`[DB] Tentativa ${attempt + 1}/${retries} falhou (${error.message?.substring(0, 100)}). Tentando novamente em ${Math.round(backoffDelay)}ms...`);
                    }
                }
                
                await new Promise(res => setTimeout(res, backoffDelay));
            } else {
                // Log detalhado do erro final
                if (isSharedMemoryError) {
                    console.error(`[DB] Erro de memória compartilhada após ${retries} tentativas. O PostgreSQL pode estar sobrecarregado ou com limites de memória muito baixos.`);
                    console.error(`[DB] Erro completo:`, error.message);
                    console.error(`[DB] Config: max=${sqlTx.options.max}, connect_timeout=${sqlTx.options.connect_timeout}s`);
                    console.error(`[DB] Sugestão: Reduza PG_POOL_MAX ou aumente os limites de memória compartilhada do PostgreSQL.`);
                } else if (isConnectTimeout) {
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

