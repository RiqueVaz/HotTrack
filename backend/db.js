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

const sql = postgres(process.env.DATABASE_URL, {
    ssl: resolveSslOption(),
    max: parsePositiveInt(process.env.PG_POOL_MAX || process.env.PG_MAX_CONNECTIONS)
});

module.exports = {
    sql
};

