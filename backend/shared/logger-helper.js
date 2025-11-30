/**
 * Logger Helper
 * Funções auxiliares para controlar verbosidade de logs em produção
 */

/**
 * Verifica se deve logar mensagens de debug
 * @returns {boolean} true apenas em desenvolvimento ou se ENABLE_VERBOSE_LOGS estiver ativo
 */
function shouldLogDebug() {
    return process.env.NODE_ENV !== 'production' || process.env.ENABLE_VERBOSE_LOGS === 'true';
}

/**
 * Verifica se deve logar mensagens informativas
 * @returns {boolean} true em desenvolvimento ou produção (mas com menos frequência)
 */
function shouldLogInfo() {
    return process.env.NODE_ENV !== 'production' || process.env.ENABLE_VERBOSE_LOGS === 'true';
}

/**
 * Loga apenas ocasionalmente baseado em probabilidade
 * @param {number} probability - Probabilidade de logar (0.01 = 1%, 0.1 = 10%)
 * @returns {boolean} true se deve logar
 */
function shouldLogOccasionally(probability = 0.01) {
    return Math.random() < probability;
}

/**
 * Loga apenas a cada N ocorrências usando contador
 * @param {string} key - Chave única para o contador
 * @param {number} interval - Intervalo (ex: 100 = loga a cada 100 ocorrências)
 * @returns {boolean} true se deve logar
 */
const logCounters = new Map();
function shouldLogEveryN(key, interval = 100) {
    const current = (logCounters.get(key) || 0) + 1;
    logCounters.set(key, current);
    
    if (current >= interval) {
        logCounters.set(key, 0);
        return true;
    }
    return false;
}

/**
 * Limpa contadores antigos periodicamente
 */
setInterval(() => {
    // Limpar contadores não utilizados há mais de 1 hora
    // (implementação simples - em produção pode ser mais sofisticado)
    if (logCounters.size > 1000) {
        logCounters.clear();
    }
}, 60 * 60 * 1000); // A cada 1 hora

module.exports = {
    shouldLogDebug,
    shouldLogInfo,
    shouldLogOccasionally,
    shouldLogEveryN
};

