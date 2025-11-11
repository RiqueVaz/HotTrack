const levels = ['error', 'warn', 'info', 'debug'];

function getConfiguredLevel() {
    const envLevel = process.env.LOG_LEVEL?.toLowerCase();
    if (envLevel && levels.includes(envLevel)) {
        return envLevel;
    }
    return process.env.ENABLE_VERBOSE_LOGS === 'true' ? 'debug' : 'info';
}

const configuredLevel = getConfiguredLevel();
const configuredIndex = levels.indexOf(configuredLevel);

function shouldLog(level) {
    const levelIndex = levels.indexOf(level);
    return levelIndex <= configuredIndex;
}

function baseLogger(level) {
    return (...args) => {
        if (!shouldLog(level)) return;
        const timestamp = new Date().toISOString();
        const prefix = `[${timestamp}] [${level.toUpperCase()}]`;
        // eslint-disable-next-line no-console
        console[level === 'debug' ? 'log' : level](prefix, ...args);
    };
}

module.exports = {
    error: baseLogger('error'),
    warn: baseLogger('warn'),
    info: baseLogger('info'),
    debug: baseLogger('debug'),
    getLevel: () => configuredLevel,
};

