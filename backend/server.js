// VERSÃO FINAL E COMPLETA - PRONTA PARA PRODUÇÃO
// VERSÃO FINAL E COMPLETA - PRONTA PARA PRODUÇÃO

// Carrega as variáveis de ambiente APENAS se não estivermos em produção (Render/Vercel)
if (process.env.NODE_ENV !== 'production') {
    require('dotenv').config({ path: '../.env' });
}
const express = require('express');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const axios = require('axios');
const FormData = require('form-data');
const http = require('http');
const https = require('https');
const path = require('path');
const crypto = require('crypto');
const webpush = require('web-push');
const { OAuth2Client } = require('google-auth-library');
const { Client } = require("@upstash/qstash");
const { Receiver } = require("@upstash/qstash");
const { MailerSend, EmailParams, Sender, Recipient } = require("mailersend");
const { createPixService } = require('./shared/pix');
const logger = require('./logger');
const { sqlTx, sqlWithRetry } = require('./db');

function parseJsonField(value, context) {
    if (value === null || value === undefined) {
        return value;
    }
    if (typeof value === 'object') {
        return value;
    }
    if (typeof value === 'string') {
        try {
            return JSON.parse(value);
        } catch (error) {
            console.error(`[JSON] Falha ao converter ${context}:`, error);
            throw new Error(`JSON_PARSE_ERROR_${context}`);
        }
    }
    return value;
}


// Configuração do Google OAuth
const googleClient = new OAuth2Client(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI || 'http://localhost:3000/google-callback.html'
);

// Configuração do MailerSend
const mailerSend = new MailerSend({
    apiKey: process.env.MAILERSEND_API_KEY,
});

const qstashClient = new Client({
  token: process.env.QSTASH_TOKEN,
});

const DEFAULT_INVITE_MESSAGE = 'Seu link exclusivo está pronto! Clique no botão abaixo para acessar.';
const DEFAULT_INVITE_BUTTON_TEXT = 'Acessar convite';

const processDisparoWorker = require('./worker/process-disparo');
const processTimeoutWorker = require('./worker/process-timeout');

const receiver = new Receiver({
    currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY,
    nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY,
  });

// Função de validação de URLs em texto (anti-links externos)
const validateTextForUrls = (text) => {
    if (!text || typeof text !== 'string') return { valid: true, urls: [] };
    
    // Remove variáveis do sistema antes de validar
    const textWithoutVariables = text.replace(/\{\{[^}]+\}\}/g, '');
    
    // Padrões de URL a detectar
    const urlPattern = /(https?:\/\/[^\s]+)|(www\.[^\s]+)|([a-zA-Z0-9-]+\.(?:com|net|org|br|app|io|co|dev|tech|link|site|online|store|shop|xyz|info|biz|me|tv|cc|us|uk|de|fr|es|it|pt|ru|cn|jp|kr|in|au|ca|mx|ar|cl|pe|ve|co\.uk|com\.br|gov|edu|mil)[^\s]*)/gi;
    
    const foundUrls = [];
    let match;
    while ((match = urlPattern.exec(textWithoutVariables)) !== null) {
        foundUrls.push(match[0]);
    }
    
    return {
        valid: foundUrls.length === 0,
        urls: foundUrls
    };
};

// Função de validação das ações do fluxo
const validateFlowActions = (nodes) => {
    if (!nodes || !Array.isArray(nodes)) return { valid: true };
    
    for (const node of nodes) {
        const actions = node.data?.actions || [];
        
        for (const action of actions) {
            // Validar texto de mensagem
            if (action.type === 'message' && action.data?.text) {
                const validation = validateTextForUrls(action.data.text);
                if (!validation.valid) {
                    return { 
                        valid: false, 
                        message: `Links não são permitidos no texto das mensagens. Links detectados: ${validation.urls.join(', ')}` 
                    };
                }
            }
            
            // Validar legendas
            if (['image', 'video', 'document'].includes(action.type) && action.data?.caption) {
                const validation = validateTextForUrls(action.data.caption);
                if (!validation.valid) {
                    return { 
                        valid: false, 
                        message: `Links não são permitidos nas legendas. Links detectados: ${validation.urls.join(', ')}` 
                    };
                }
            }
        }
    }
    
    return { valid: true };
};

const app = express();

// Configurar timeouts para evitar requisições presas
app.use((req, res, next) => {
    // Timeout de 60 segundos para requisições
    // Se a requisição demorar mais que isso, retorna timeout
    req.setTimeout(60000, () => {
        if (!res.headersSent) {
            res.status(504).json({ error: 'Request timeout' });
        }
    });
    
    // Timeout de resposta também
    res.setTimeout(60000, () => {
        if (!res.headersSent) {
            res.status(504).json({ error: 'Response timeout' });
        }
    });
    
    next();
});

const METRICS_IGNORED_PATHS = new Set(['/metrics']);

const resolveRouteLabel = (req, statusCode) => {
    if (req.route?.path) {
        const base = req.baseUrl && req.baseUrl !== '/' ? req.baseUrl : '';
        return `${base}${req.route.path}` || req.route.path;
    }

    if (!req.route && statusCode === 404) {
        return 'unmatched';
    }

    if (req.baseUrl) {
        return req.baseUrl;
    }

    if (req.path) {
        return req.path;
    }

    if (req.originalUrl) {
        const withoutQuery = req.originalUrl.split('?')[0];
        return withoutQuery || 'unknown';
    }

    return 'unknown';
};

const parseContentLength = (value) => {
    if (Array.isArray(value)) {
        return parseContentLength(value[0]);
    }
    if (typeof value === 'number') {
        return value;
    }
    if (typeof value === 'string') {
        const parsed = Number.parseInt(value, 10);
        return Number.isNaN(parsed) ? 0 : parsed;
    }
    return 0;
};





// Configuração do servidor
const PORT = process.env.PORT || 3001;


app.post(
  '/api/worker/process-timeout',
  express.raw({ type: 'application/json' }), // 1. Ainda é OBRIGATÓRIO para pegar o corpo original
  async (req, res) => {
    try {
      // 2. Extraia as informações necessárias manualmente
      const signature = req.headers["upstash-signature"];
      const bodyString = req.body.toString(); // O Receiver espera uma string, não um Buffer

      // 3. Verifique a assinatura
      const isValid = await receiver.verify({
        signature,
        body: bodyString,
      });

      // 4. Se a assinatura for inválida, rejeite a requisição
      if (!isValid) {
        console.error("QStash Signature Verification Failed (Manual Receiver)");
        return res.status(401).send("Invalid signature");
      }

      // 5. Se for válida, prossiga: transforme a string de volta em JSON para o worker
      console.log("[WORKER] Assinatura válida. Processando a tarefa.");
      req.body = JSON.parse(bodyString);
      await processTimeoutWorker(req, res);
      
      // Verificar se o worker enviou resposta, se não, enviar uma padrão
      if (!res.headersSent) {
        res.status(200).json({ message: 'Worker processed successfully.' });
      }

    } catch (error) {
      console.error("Erro crítico no handler do worker:", error);
      // Verificar se resposta já foi enviada antes de tentar enviar
      if (!res.headersSent) {
        res.status(500).send("Internal Server Error");
      }
    }
  }
);

app.post(
     '/api/worker/process-disparo',
     express.raw({ type: 'application/json' }), // Obrigatório para verificação do QStash
     workerDisparoRateLimitMiddleware, // Rate limiting para evitar sobrecarga simultânea
     async (req, res) => {
      try {
       // 1. Verificar a assinatura do QStash
       const signature = req.headers["upstash-signature"];
       const bodyString = req.body.toString();
    
       const isValid = await receiver.verify({
        signature,
        body: bodyString,
       });
    
       if (!isValid) {
        console.error("[WORKER-DISPARO] Verificação de assinatura do QStash falhou.");
        return res.status(401).send("Invalid signature");
       }
    
       // 2. Se for válida, processar a tarefa
       console.log("[WORKER-DISPARO] Assinatura válida. Processando disparo.");
       req.body = JSON.parse(bodyString); // Converte de volta para JSON para o worker
       await processDisparoWorker(req, res); // Chama o handler do worker
       
       // Verificar se o worker enviou resposta, se não, enviar uma padrão
       if (!res.headersSent) {
         res.status(200).json({ message: 'Worker de disparo processado com sucesso.' });
       }
    
      } catch (error) {
       console.error("Erro crítico no handler do worker de disparo:", error);
       // Verificar se resposta já foi enviada antes de tentar enviar
       if (!res.headersSent) {
         res.status(500).send("Internal Server Error");
       }
      }
     }
    );
// ==========================================================
// FIM DA ROTA DO QSTASH
// ==========================================================


app.use(express.json({ limit: '70mb' }));
app.use(express.urlencoded({ extended: true, limit: '70mb' }));

// Middleware para tratar requisições abortadas ANTES de processar
app.use((req, res, next) => {
    // Detectar se requisição foi abortada
    req.on('aborted', () => {
        // Cliente fechou conexão - não é um erro real
        // Não fazer nada, apenas prevenir que o erro seja propagado
    });
    
    // Verificar se já foi abortada antes de processar
    if (req.aborted) {
        return res.status(499).end(); // 499 = Client Closed Request
    }
    
    next();
});

// Middleware JSON específico para uploads grandes via base64 (~70MB)
const json70mb = express.json({ limit: '70mb' });

// Validação de tamanho conforme limites do Telegram
function validateTelegramSize(fileBuffer, fileType) {
    const size = fileBuffer.length; // bytes
    const MB = 1024 * 1024;
    try {
        if (fileType && fileType.startsWith && fileType.startsWith('video/')) {
            if (size > 50 * MB) throw new Error('Arquivo de vídeo excede 50 MB (limite do Telegram).');
        } else if (fileType && fileType.startsWith && fileType.startsWith('image/')) {
            if (size > 10 * MB) throw new Error('Imagem excede 10 MB (limite do Telegram).');
        } else if (fileType && fileType.startsWith && fileType.startsWith('audio/')) {
            if (size > 50 * MB) throw new Error('Áudio excede 50 MB (limite do Telegram).');
        }
    } catch (e) {
        throw e;
    }
}

// ==========================================================
//          CONFIGURAÇÃO CORS SEGURA E SELETIVA
// ==========================================================

// Origens permitidas para rotas administrativas (restritivas)
const adminAllowedOrigins = [
  process.env.FRONTEND_URL,
  process.env.ADMIN_FRONTEND_URL,
  'http://localhost:3000',
  'http://localhost:3001',  
  'http://localhost:3000/admin',
  'http://localhost:3001/admin'
].filter(Boolean);

// Cache para domínios permitidos por pressel (performance)
const allowedDomainsCache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutos

// Rate limiting simples para registerClick
const rateLimitMap = new Map();
const RATE_LIMIT_WINDOW = 15 * 60 * 1000; // 15 minutos
const RATE_LIMIT_MAX_REQUESTS = 100; // 100 requests por 15 minutos por IP

// Limpar entradas expiradas do rateLimitMap periodicamente (evita memory leak)
setInterval(() => {
    const now = Date.now();
    for (const [ip, data] of rateLimitMap.entries()) {
        if (now > data.resetTime) {
            rateLimitMap.delete(ip);
        }
    }
}, 5 * 60 * 1000); // Limpar a cada 5 minutos

// Middleware de rate limiting
function rateLimitMiddleware(req, res, next) {
    const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    const now = Date.now();
    
    if (!rateLimitMap.has(ip)) {
        rateLimitMap.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
        return next();
    }
    
    const rateLimitData = rateLimitMap.get(ip);
    
    // Reset se a janela expirou
    if (now > rateLimitData.resetTime) {
        rateLimitMap.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW });
        return next();
    }
    
    // Verificar se excedeu o limite
    if (rateLimitData.count >= RATE_LIMIT_MAX_REQUESTS) {
        return res.status(429).json({ 
            message: 'Muitas tentativas. Tente novamente em alguns minutos.',
            retryAfter: Math.ceil((rateLimitData.resetTime - now) / 1000)
        });
    }
    
    // Incrementar contador
    rateLimitData.count++;
    next();
}

// Rate limiting específico para webhook do Telegram (muitas requisições simultâneas)
const webhookRateLimit = new Map();
const WEBHOOK_RATE_LIMIT_WINDOW = 60 * 1000; // 1 minuto
const WEBHOOK_RATE_LIMIT_MAX = 100; // 100 requisições por minuto por bot

// Limpar entradas expiradas do webhookRateLimit periodicamente (evita memory leak)
setInterval(() => {
    const now = Date.now();
    for (const [key, data] of webhookRateLimit.entries()) {
        if (now > data.resetTime) {
            webhookRateLimit.delete(key);
        }
    }
}, 2 * 60 * 1000); // Limpar a cada 2 minutos

function webhookRateLimitMiddleware(req, res, next) {
    const botId = req.params.botId;
    if (!botId) return next();
    
    const key = `webhook_${botId}`;
    const now = Date.now();
    
    if (!webhookRateLimit.has(key)) {
        webhookRateLimit.set(key, { count: 1, resetTime: now + WEBHOOK_RATE_LIMIT_WINDOW });
        return next();
    }
    
    const data = webhookRateLimit.get(key);
    
    if (now > data.resetTime) {
        webhookRateLimit.set(key, { count: 1, resetTime: now + WEBHOOK_RATE_LIMIT_WINDOW });
        return next();
    }
    
    if (data.count >= WEBHOOK_RATE_LIMIT_MAX) {
        // Retornar 200 para não causar retry do Telegram, mas não processar
        return res.status(200).json({ ok: true, message: 'Rate limit exceeded' });
    }
    
    data.count++;
    next();
}

// Rate limiting para worker de disparo (evita sobrecarga simultânea)
const workerDisparoRateLimit = new Map();
const WORKER_DISPARO_RATE_LIMIT_WINDOW = 10 * 1000; // 10 segundos
const WORKER_DISPARO_MAX_CONCURRENT = 5; // Máximo 5 workers simultâneos

function workerDisparoRateLimitMiddleware(req, res, next) {
    const now = Date.now();
    const key = 'worker_disparo';
    
    // Limpar entradas antigas
    if (workerDisparoRateLimit.has(key)) {
        const data = workerDisparoRateLimit.get(key);
        if (now > data.resetTime) {
            workerDisparoRateLimit.delete(key);
        }
    }
    
    // Contar requisições ativas
    if (!workerDisparoRateLimit.has(key)) {
        workerDisparoRateLimit.set(key, { 
            active: 1, 
            resetTime: now + WORKER_DISPARO_RATE_LIMIT_WINDOW 
        });
        
        // Decrementar quando terminar
        res.on('finish', () => {
            if (workerDisparoRateLimit.has(key)) {
                const d = workerDisparoRateLimit.get(key);
                d.active = Math.max(0, d.active - 1);
            }
        });
        
        return next();
    }
    
    const data = workerDisparoRateLimit.get(key);
    
    if (data.active >= WORKER_DISPARO_MAX_CONCURRENT) {
        // Retornar 429 para QStash retry mais tarde
        return res.status(429).json({ 
            error: 'Too many concurrent workers', 
            retryAfter: Math.ceil((data.resetTime - now) / 1000) 
        });
    }
    
    data.active++;
    
    // Decrementar quando terminar
    res.on('finish', () => {
        if (workerDisparoRateLimit.has(key)) {
            const d = workerDisparoRateLimit.get(key);
            d.active = Math.max(0, d.active - 1);
        }
    });
    
    next();
}

// Função para verificar se um domínio é permitido para uma pressel
async function isDomainAllowedForPressel(presselId, origin) {
  try {
    // Verificar cache primeiro
    const cacheKey = `${presselId}-${origin}`;
    const cached = allowedDomainsCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
      return cached.allowed;
    }

    // Buscar no banco de dados
    const result = await sqlTx`
      SELECT COUNT(*) as count 
      FROM pressel_allowed_domains 
      WHERE pressel_id = ${presselId} 
      AND (domain = ${origin} OR domain = ${origin.replace(/^https?:\/\//, '')})
    `;

    const isAllowed = result[0]?.count > 0;
    
    // Atualizar cache
    allowedDomainsCache.set(cacheKey, {
      allowed: isAllowed,
      timestamp: Date.now()
    });

    return isAllowed;
  } catch (error) {
    console.error('Erro ao verificar domínio permitido:', error);
    return false;
  }
}

const sanitizeTagTitle = (title = '') => title.trim();
const normalizeHexColor = (color = '') => color.trim().toUpperCase();
const isValidTagColor = (color) => TAG_COLOR_REGEX.test(color);

// Middleware CORS seletivo baseado na rota
const corsOptions = async (req, callback) => {
  const origin = req.header('Origin');
  const isPresselRoute = req.path === '/api/registerClick';
  const isVerifyEmailRoute = req.path === '/api/sellers/verify-email';
  
  if (isVerifyEmailRoute) {
    // Para verificação de email, permitir qualquer origem
    callback(null, { 
      origin: true,
      methods: ['GET', 'POST', 'OPTIONS'],
      allowedHeaders: ['Content-Type'],
      credentials: false
    });
  } else if (isPresselRoute) {
    // Para rotas de pressel, verificar domínio no banco
    const presselId = req.body?.presselId;
    
    if (presselId && origin) {
      const isAllowed = await isDomainAllowedForPressel(presselId, origin);
      
      callback(null, { 
        origin: isAllowed ? origin : false,
        methods: ['GET', 'POST', 'OPTIONS'],
        allowedHeaders: ['Content-Type'],
        credentials: false
      });
    } else {
      // Se não tem presselId ou origin, permitir (fallback para compatibilidade)
      callback(null, { 
        origin: origin,
        methods: ['GET', 'POST', 'OPTIONS'],
        allowedHeaders: ['Content-Type'],
        credentials: false
      });
    }
  } else {
    // Para outras rotas (admin), usar lista restritiva
    const isAllowed = adminAllowedOrigins.some(allowedOrigin => {
      if (typeof allowedOrigin === 'string') {
        return allowedOrigin === origin;
      } else if (allowedOrigin instanceof RegExp) {
        return allowedOrigin.test(origin);
      }
      return false;
    });
    
    callback(null, { 
      origin: isAllowed ? origin : false,
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization', 'x-admin-api-key'],
      credentials: true
    });
  }
};

app.use(cors(corsOptions));


// Agentes HTTP/HTTPS para reutilização de conexão (melhora a performance)
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });

// --- OTIMIZAÇÃO CRÍTICA: A conexão com o banco é inicializada UMA VEZ e reutilizada ---

const JWT_SECRET = process.env.JWT_SECRET;
if (!JWT_SECRET) {
    throw new Error('JWT_SECRET não configurado.');
}

if (!process.env.JWT_REFRESH_SECRET) {
    console.warn('[Auth] JWT_REFRESH_SECRET não definido. Usando JWT_SECRET como fallback.');
}

const JWT_REFRESH_SECRET = process.env.JWT_REFRESH_SECRET || JWT_SECRET;

const ACCESS_TOKEN_TTL_SECONDS = resolvePositiveInt(
    process.env.JWT_ACCESS_TOKEN_TTL_SECONDS || process.env.JWT_ACCESS_TTL_SECONDS,
    900
);
const REFRESH_TOKEN_TTL_SECONDS = resolvePositiveInt(
    process.env.JWT_REFRESH_TOKEN_TTL_SECONDS || process.env.JWT_REFRESH_TTL_SECONDS,
    60 * 60 * 24 * 30 // 30 dias
);

// ==========================================================
//          VARIÁVEIS DE AMBIENTE E CONFIGURAÇÕES
// ==========================================================
const HOTTRACK_API_URL = process.env.HOTTRACK_API_URL || 'https://hottrack.vercel.app/api';
const FRONTEND_URL = process.env.FRONTEND_URL || 'https://hottrackerbot.netlify.app';
const DOCUMENTATION_URL = process.env.DOCUMENTATION_URL || 'https://documentacaohot.netlify.app';

// ==========================================================
//          LÓGICA DE RETRY PARA O BANCO DE DADOS
// ==========================================================
function resolvePositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
        return parsed;
    }

    if (value !== undefined && value !== null) {
        console.warn(`[Auth] Valor inválido "${value}" para configuração numérica. Usando fallback ${fallback}.`);
    }
    return fallback;
}



// --- CONFIGURAÇÃO DAS NOTIFICAÇÕES ---
if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    webpush.setVapidDetails(
        process.env.VAPID_SUBJECT,
        process.env.VAPID_PUBLIC_KEY,
        process.env.VAPID_PRIVATE_KEY
    );
}
let adminSubscription = null;

// --- CONFIGURAÇÃO ---
const PUSHINPAY_SPLIT_ACCOUNT_ID = process.env.PUSHINPAY_SPLIT_ACCOUNT_ID;
const CNPAY_SPLIT_PRODUCER_ID = process.env.CNPAY_SPLIT_PRODUCER_ID;
const OASYFY_SPLIT_PRODUCER_ID = process.env.OASYFY_SPLIT_PRODUCER_ID;
const BRPIX_SPLIT_RECIPIENT_ID = process.env.BRPIX_SPLIT_RECIPIENT_ID;
const WIINPAY_SPLIT_USER_ID = process.env.WIINPAY_SPLIT_USER_ID;
const PIXUP_SPLIT_USERNAME = process.env.PIXUP_SPLIT_USERNAME;
const PARADISE_SPLIT_RECIPIENT_ID = process.env.PARADISE_SPLIT_RECIPIENT_ID;
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;
const SYNCPAY_API_BASE_URL = 'https://api.syncpayments.com.br';
const syncPayTokenCache = new Map();
const TAG_TITLE_MAX_LENGTH = 12;
const TAG_COLOR_REGEX = /^#[0-9A-F]{6}$/i;
// Cache simples para evitar ultrapassar rate limit em consultas PushinPay (min. 1/min).
const pushinpayLastCheckAt = new Map(); // key: provider_transaction_id, value: timestamp (ms)
const wiinpayLastCheckAt = new Map();
const paradiseLastCheckAt = new Map();

const {
    getSyncPayAuthToken,
    generatePixForProvider,
    generatePixWithFallback
} = createPixService({
    sql: sqlTx,
    sqlWithRetry,
    axios,
    uuidv4,
    syncPayTokenCache,
    adminApiKey: ADMIN_API_KEY,
    synPayBaseUrl: SYNCPAY_API_BASE_URL,
    pushinpaySplitAccountId: PUSHINPAY_SPLIT_ACCOUNT_ID,
    cnpaySplitProducerId: CNPAY_SPLIT_PRODUCER_ID,
    oasyfySplitProducerId: OASYFY_SPLIT_PRODUCER_ID,
    brpixSplitRecipientId: BRPIX_SPLIT_RECIPIENT_ID,
    wiinpaySplitUserId: WIINPAY_SPLIT_USER_ID,
    pixupSplitUsername: PIXUP_SPLIT_USERNAME,
    paradiseSplitRecipientId: PARADISE_SPLIT_RECIPIENT_ID,
    hottrackApiUrl: process.env.HOTTRACK_API_URL,
});

// ==========================================================
//          FUNÇÕES DO HOTBOT INTEGRADAS
// ==========================================================

// ==========================================================
//          FUNÇÕES DE INTEGRAÇÃO NETLIFY
// ==========================================================


async function createNetlifySite(accessToken, siteName) {
    try {
        const response = await axios.post('https://api.netlify.com/api/v1/sites', {
            name: siteName,
            custom_domain: null
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });
        
        
        const siteUrl = `https://${response.data.subdomain || response.data.name}.netlify.app`;
        console.log('[Netlify] URL gerada:', siteUrl);
        
        return {
            success: true,
            site: response.data,
            url: siteUrl
        };
    } catch (error) {
        console.error('[Netlify] Erro ao criar site:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}
async function deployToNetlify(accessToken, siteId, htmlContent, fileName = 'index.html') {
    try {
        // 1. Calcular hash SHA1 do conteúdo
        const contentHash = crypto.createHash('sha1').update(htmlContent).digest('hex');
        
        // 2. Criar deploy
        const deployResponse = await axios.post(`https://api.netlify.com/api/v1/sites/${siteId}/deploys`, {
            files: {
                [fileName]: contentHash
            }
        }, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        });
        
        const deployId = deployResponse.data.id;
        
        // 3. Upload do arquivo
        await axios.put(`https://api.netlify.com/api/v1/deploys/${deployId}/files/${fileName}`, htmlContent, {
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'text/html'
            }
        });
        
        return {
            success: true,
            deployId: deployId,
            url: `https://${deployResponse.data.subdomain || deployResponse.data.name}.netlify.app`
        };
    } catch (error) {
        console.error('[Netlify] Erro ao fazer deploy:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}

app.post('/api/netlify/validate-token', authenticateJwt, async (req, res) => {
    const { access_token } = req.body;
    const userId = req.user?.id; // Pega o ID do usuário autenticado

    if (!access_token) {
        return res.status(400).json({ message: 'Token de acesso é obrigatório.' });
    }
    if (!userId) {
         console.error("[Netlify Validate Backend] Erro: userId não encontrado em req.user após authenticateJwt.");
         return res.status(401).json({ message: 'Usuário não autenticado ou ID não encontrado.' });
    }

    try {
        // --- INÍCIO DA LÓGICA DE VALIDAÇÃO (Substitui a chamada inexistente) ---
        let validationResult = { success: false, error: null, user: null };
        try {
            // Faz a chamada para a API do Netlify para obter informações do usuário atual
            // Uma chamada bem-sucedida indica que o token é válido.
            const netlifyApiResponse = await axios.get('https://api.netlify.com/api/v1/user', {
                headers: {
                    'Authorization': `Bearer ${access_token}`
                },
                httpsAgent: httpsAgent // Reutiliza o agente HTTPS
            });

            // Se a chamada foi bem-sucedida (status 2xx)
            validationResult = {
                success: true,
                error: null,
                user: netlifyApiResponse.data // Guarda os dados do usuário do Netlify
            };
            console.log(`[Netlify Validate] Token validado com sucesso para usuário: ${netlifyApiResponse.data.email}`);

        } catch (netlifyError) {
            // Se a chamada falhar (ex: 401 Unauthorized), o token é inválido
            console.error('[Netlify Validate] Erro ao chamar API Netlify:', netlifyError.response?.data || netlifyError.message);
            validationResult = {
                success: false,
                error: netlifyError.response?.data?.message || netlifyError.message || 'Falha na comunicação com a API Netlify.',
                user: null
            };
        }
        // --- FIM DA LÓGICA DE VALIDAÇÃO ---

        if (validationResult.success) {
            // Salva o token no banco de dados se for válido
            await sqlTx`UPDATE sellers SET netlify_access_token = ${access_token} WHERE id = ${userId}`;

            res.json({
                success: true,
                message: 'Token válido! Configuração salva.',
                user: validationResult.user // Retorna os dados do usuário Netlify para o frontend
            });
        } else {
            res.status(400).json({
                success: false,
                message: 'Token inválido: ' + validationResult.error
            });
        }
    } catch (error) { // Este catch pega erros do SQL ou outros erros inesperados
        console.error("Erro GERAL ao validar/salvar token Netlify (backend):", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

async function getNetlifySites(accessToken) {
    try {
        const response = await axios.get('https://api.netlify.com/api/v1/sites', {
            headers: {
                'Authorization': `Bearer ${accessToken}`
            }
        });
        
        return {
            success: true,
            sites: response.data
        };
    } catch (error) {
        console.error('[Netlify] Erro ao listar sites:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}
async function deleteNetlifySite(accessToken, siteId) {
    try {
        await axios.delete(`https://api.netlify.com/api/v1/sites/${siteId}`, {
            headers: {
                'Authorization': `Bearer ${accessToken}`
            }
        });

        return {
            success: true,
            message: 'Site excluído com sucesso'
        };
    } catch (error) {
        // Se o site não existe (404), considerar como sucesso
        if (error.response?.status === 404) {
            console.log(`[Netlify] Site ${siteId} não encontrado (já excluído ou não existe)`);
            return {
                success: true,
                message: 'Site não encontrado (já excluído)'
            };
        }
        
        console.error('[Netlify] Erro ao excluir site:', error.response?.data || error.message);
        return {
            success: false,
            error: error.response?.data?.message || error.message
        };
    }
}

/**
 * Gera o código HTML completo para uma página de pressel, espelhando
 * a lógica da função generatePresselCode do frontend.
 * @param {object} pressel - O objeto da pressel do banco de dados.
 * @param {Array<number>} pixelIds - Array de IDs numéricos das configurações de pixel associadas.
 * @returns {Promise<string>} O código HTML completo como string.
 */
async function generatePresselHTML(pressel, pixelIds) {
    try {
        // 1. Buscar dados dos pixels associados
        if (!Array.isArray(pixelIds) || pixelIds.length === 0) {
            throw new Error('Pelo menos um ID de pixel é necessário.');
        }
        const pixels = await sqlTx`
            SELECT pc.pixel_id, pc.account_name
            FROM pixel_configurations pc
            WHERE pc.id = ANY(${pixelIds}) AND pc.seller_id = ${pressel.seller_id}
        `;
        if (pixels.length === 0) {
            console.warn(`[generatePresselHTML] Nenhum pixel encontrado para os IDs fornecidos: ${pixelIds} para seller ${pressel.seller_id}`);
            // Considerar lançar um erro ou retornar um HTML de erro, dependendo da regra de negócio
            // throw new Error('Configurações de pixel não encontradas.');
        }

        // 2. Buscar nome do bot
        const [bot] = await sqlTx`
            SELECT bot_name FROM telegram_bots
            WHERE id = ${pressel.bot_id} AND seller_id = ${pressel.seller_id}
        `;
        if (!bot?.bot_name) {
            throw new Error(`Bot com ID ${pressel.bot_id} não encontrado ou sem nome para a pressel ${pressel.id}.`);
        }
        const botUsername = bot.bot_name.replace('@', '');

        // 3. Definir a URL base da API (do ambiente do backend)
        // Exemplo de ajuste para desenvolvimento local (assumindo HTTP)
        const isProduction = process.env.NODE_ENV === 'production';
        const apiBaseUrl = isProduction
            ? (process.env.HOTTRACK_API_URL)
            : 'http://localhost:3001'; // Ajuste aqui se usar HTTPS localmente

        // 4. Lógica de detecção de dispositivo
        const trafficType = pressel.traffic_type || 'both';
        let deviceDetectionCode = '';
        if (trafficType === 'mobile') {
            deviceDetectionCode = `
            // Verificar se é mobile
            const isMobile = /android|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(navigator.userAgent);
            if (!isMobile) {
                console.log('Dispositivo não móvel detectado. Redirecionando para a página branca.');
                window.location.href = CONFIG.WHITE_PAGE_URL;
                return; // Interrompe a execução
            }`;
        } else if (trafficType === 'desktop') {
            deviceDetectionCode = `
            // Verificar se é desktop
            const isMobile = /android|iphone|ipad|ipod|blackberry|iemobile|opera mini/i.test(navigator.userAgent);
            if (isMobile) {
                console.log('Dispositivo móvel detectado. Redirecionando para a página branca.');
                window.location.href = CONFIG.WHITE_PAGE_URL;
                return; // Interrompe a execução
            }`;
        }

        // 5. Preparar scripts do Pixel Meta
        const pixelInitScripts = pixels.map(p => `fbq('init', '${p.pixel_id}');`).join('\n        ');
        const noscriptPixelTag = pixels.length > 0
            ? `<noscript><img height="1" width="1" style="display:none"
               src="https://www.facebook.com/tr?id=${pixels[0].pixel_id}&ev=PageView&noscript=1"
             /></noscript>`
            : '';

        // 6. Montar o HTML final usando template literals
        const htmlContent = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${pressel.name || 'Carregando...'}</title>
    <style>
        body, html {
            margin: 0;
            padding: 0;
            width: 100%;
            height: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            background-color: #1a1a1a; /* Fundo escuro */
            color: #ffffff;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        }
        .loader-container {
            text-align: center;
        }
        .spinner {
            border: 4px solid rgba(255, 255, 255, 0.2);
            border-left-color: #ffffff;
            border-radius: 50%;
            width: 50px;
            height: 50px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px auto;
        }
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        p {
            font-size: 1.2em;
            opacity: 0;
            animation: fadeIn 1s forwards;
            animation-delay: 0.5s;
        }
        @keyframes fadeIn {
            to { opacity: 1; }
        }
    </style>
    <script>
        !function(f,b,e,v,n,t,s)
        {if(f.fbq)return;n=f.fbq=function(){n.callMethod?
        n.callMethod.apply(n,arguments):n.queue.push(arguments)};
        if(!f._fbq)f._fbq=n;n.push=n;n.loaded=!0;n.version='2.0';
        n.queue=[];t=b.createElement(e);t.async=!0;
        t.src=v;s=b.getElementsByTagName(e)[0];
        s.parentNode.insertBefore(t,s)}(window, document,'script',
        'https://connect.facebook.net/en_US/fbevents.js');
        ${pixelInitScripts}
        fbq('track', 'PageView');
    </script>
    ${noscriptPixelTag}
    <script>
        (async function() {
            const CONFIG = {
                PRESSEL_ID: ${pressel.id},
                WHITE_PAGE_URL: "${pressel.white_page_url}",
                BOT_USERNAME: "${botUsername}",
                API_BASE_URL: "${apiBaseUrl}" // <<< USA A URL DO BACKEND
            };
            function getQueryParam(param) {
                const urlParams = new URLSearchParams(window.location.search);
                return urlParams.get(param);
            }
            function getFacebookCookies() {
                const cookies = document.cookie.split('; ');
                let fbp = null, fbc = null;
                for (const cookie of cookies) {
                    const parts = cookie.split('=');
                    if (parts[0] === '_fbp') fbp = parts[1];
                    if (parts[0] === '_fbc') fbc = parts[1];
                }
                return { fbp, fbc };
            }
            try {
                // Bloqueia bots conhecidos do Facebook
                const userAgent = navigator.userAgent;
                if (/facebookexternalhit|Facebot/i.test(userAgent)) {
                    console.log('Bot do Facebook detectado. Redirecionando...');
                    window.location.href = CONFIG.WHITE_PAGE_URL;
                    return; // Interrompe a execução
                }

                ${deviceDetectionCode} // <<< INJETA O CÓDIGO DE DETECÇÃO

                const { fbp, fbc } = getFacebookCookies();
                // Construir payload para /api/registerClick
                const clickPayload = {
                    presselId: CONFIG.PRESSEL_ID,
                    referer: document.referrer || null,
                    fbclid: getQueryParam('fbclid'),
                    fbp: fbp,
                    fbc: fbc,
                    user_agent: navigator.userAgent,
                    utm_source: getQueryParam('utm_source'),
                    utm_campaign: getQueryParam('utm_campaign'),
                    utm_medium: getQueryParam('utm_medium'),
                    utm_content: getQueryParam('utm_content'),
                    utm_term: getQueryParam('utm_term')
                };

                const response = await fetch(CONFIG.API_BASE_URL + '/api/registerClick', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(clickPayload)
                });
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.message || 'Falha ao registrar clique.');
                }
                const { click_id } = await response.json();

                // Dispara ViewContent no Pixel da Meta com o eventID
                if (typeof fbq !== 'undefined') {
                    fbq('track', 'ViewContent', {}, { eventID: click_id });
                    console.log('Evento ViewContent disparado com eventID:', click_id);
                } else {
                    console.warn('fbq não definido. Não foi possível disparar ViewContent.');
                }

                const botUrl = \`https://t.me/\${CONFIG.BOT_USERNAME}?start=\${click_id}\`;
                window.location.href = botUrl;
            } catch (error) {
                console.error("Erro na lógica da pressel:", error);
                window.location.href = CONFIG.WHITE_PAGE_URL; // Redireciona para página branca em caso de erro
            }
        })();
    </script>
</head>
<body>
    <div class="loader-container">
        <div class="spinner"></div>
        <p>Aguarde, estamos redirecionando...</p>
    </div>
</body>
</html>`;

        return htmlContent;

    } catch (error) {
        console.error(`[generatePresselHTML] Erro ao gerar HTML para pressel ${pressel?.id}:`, error);
        // Retornar um HTML de erro ou relançar a exceção
        throw new Error(`Falha ao gerar o código HTML da pressel: ${error.message}`);
    }
}

// ==========================================================
//          FUNÇÕES DO HOTBOT INTEGRADAS
// ==========================================================
async function sendTelegramRequest(botToken, method, data, options = {}, retries = 3, delay = 1500) {
    const { headers = {}, responseType = 'json', timeout = 30000 } = options;
    const apiUrl = `https://api.telegram.org/bot${botToken}/${method}`;

    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.post(apiUrl, data, {
                headers,
                responseType,
                httpAgent,
                httpsAgent,
                timeout
            });
            return response.data;
        } catch (error) {
            // FormData do Node.js não tem .get(), então tenta extrair do erro ou deixa undefined
            const chatId = data?.chat_id || 'unknown';

            if (error.response && error.response.status === 403) {
                console.warn(`[TELEGRAM API WARN] O bot foi bloqueado pelo usuário. ChatID: ${chatId}`);
                return { ok: false, error_code: 403, description: 'Forbidden: bot was blocked by the user' };
            }

            // Tratamento específico para TOPIC_CLOSED
            if (error.response && error.response.status === 400 && 
                error.response.data?.description?.includes('TOPIC_CLOSED')) {
                console.warn(`[TELEGRAM API WARN] Chat de grupo fechado. ChatID: ${chatId}`);
                return { ok: false, error_code: 400, description: 'Bad Request: TOPIC_CLOSED' };
            }

            const isRetryable = error.code === 'ECONNABORTED' || error.code === 'ECONNRESET' || error.message.includes('socket hang up');

            if (isRetryable && i < retries - 1) {
                await new Promise(res => setTimeout(res, delay * (i + 1)));
                continue;
            }
            
            const errorData = error.response?.data;
            const errorMessage = (errorData instanceof ArrayBuffer)
                ? JSON.parse(Buffer.from(errorData).toString('utf8'))
                : errorData;

            const description = (errorMessage && errorMessage.description) || error.message;
            if (description && description.includes('bot was blocked by the user')) {
                logger.debug(`[Telegram API] Chat ${chatId} bloqueou o bot (method ${method}). Ignorando.`);
                return { ok: false, error_code: 403, description };
            }
            console.error(`[TELEGRAM API ERROR] Method: ${method}, ChatID: ${chatId}:`, errorMessage || error.message);
            throw error;
        }
    }
}

async function saveMessageToDb(sellerId, botId, message, senderType) {
    const { message_id, chat, from, text, photo, video, voice, reply_markup } = message;
    let mediaType = null;
    let mediaFileId = null;
    let messageText = text;
    let newClickId = null;

    if (text && text.startsWith('/start ')) {
        newClickId = text.substring(7);
    }

    let finalClickId = newClickId;
    if (!finalClickId) {
        const result = await sqlWithRetry(
            'SELECT click_id FROM telegram_chats WHERE chat_id = $1 AND bot_id = $2 AND click_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
            [chat.id, botId]
        );
        if (result.length > 0) {
            finalClickId = result[0].click_id;
        }
    }

    if (photo) {
        mediaType = 'photo';
        mediaFileId = photo[photo.length - 1].file_id;
        messageText = message.caption || '[Foto]';
    } else if (video) {
        mediaType = 'video';
        mediaFileId = video.file_id;
        messageText = message.caption || '[Vídeo]';
    } else if (voice) {
        mediaType = 'voice';
        mediaFileId = voice.file_id;
        messageText = '[Mensagem de Voz]';
    }
    const botInfo = senderType === 'bot' ? { first_name: 'Bot', last_name: '(Automação)' } : {};
    const fromUser = from || chat || {};

    // Extrai reply_markup se existir
    const replyMarkupJson = reply_markup ? JSON.stringify(reply_markup) : null;

    const safeValues = [
        sellerId,
        botId,
        chat?.id ?? null,
        message_id ?? null,
        fromUser?.id ?? null,
        fromUser?.first_name ?? botInfo.first_name ?? null,
        fromUser?.last_name ?? botInfo.last_name ?? null,
        fromUser?.username ?? null,
        messageText ?? null,
        senderType ?? null,
        mediaType ?? null,
        mediaFileId ?? null,
        finalClickId ?? null,
        replyMarkupJson
    ];

    await sqlWithRetry(`
        INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, media_type, media_file_id, click_id, reply_markup)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (chat_id, message_id) DO UPDATE SET reply_markup = EXCLUDED.reply_markup;
    `, safeValues);

    if (newClickId) {
        await sqlWithRetry(
            'UPDATE telegram_chats SET click_id = $1 WHERE chat_id = $2 AND bot_id = $3',
            [newClickId, chat.id, botId]
        );
    }
}

async function replaceVariables(text, variables) {
    if (!text) return '';
    let processedText = text;
    for (const key in variables) {
        const regex = new RegExp(`{{${key}}}`, 'g');
        processedText = processedText.replace(regex, variables[key]);
    }
    return processedText;
}
async function sendMediaAsProxy(destinationBotToken, chatId, fileId, fileType, caption) {
    const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
    if (!storageBotToken) throw new Error('Token do bot de armazenamento não configurado.');

    const fileInfo = await sendTelegramRequest(storageBotToken, 'getFile', { file_id: fileId });
    if (!fileInfo.ok) throw new Error('Não foi possível obter informações do arquivo da biblioteca.');

    const fileUrl = `https://api.telegram.org/file/bot${storageBotToken}/${fileInfo.result.file_path}`;

    const { data: fileBuffer, headers: fileHeaders } = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    
    const formData = new FormData();
    formData.append('chat_id', chatId);
    if (caption) {
        formData.append('caption', caption);
    }

    const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
    const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
    const fileNameMap = { image: 'image.jpg', video: 'video.mp4', audio: 'audio.ogg' };

    const method = methodMap[fileType];
    const field = fieldMap[fileType];
    const fileName = fileNameMap[fileType];
    const timeout = fileType === 'video' ? 60000 : 30000;

    if (!method) throw new Error('Tipo de arquivo não suportado.');

    formData.append(field, fileBuffer, { filename: fileName, contentType: fileHeaders['content-type'] });

    return await sendTelegramRequest(destinationBotToken, method, formData, { headers: formData.getHeaders(), timeout });
}

// Função para processar promises do QStash em batches para evitar sobrecarga
async function publishQStashInBatches(promises, batchSize = 10, delayMs = 500) {
    const batches = [];
    for (let i = 0; i < promises.length; i += batchSize) {
        batches.push(promises.slice(i, i + batchSize));
    }
    
    console.log(`[DISPARO] Processando ${promises.length} jobs em ${batches.length} batches de ${batchSize}`);
    
    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        try {
            await Promise.all(batch);
            console.log(`[DISPARO] Batch ${i + 1}/${batches.length} concluído`);
        } catch (error) {
            console.error(`[DISPARO] Erro no batch ${i + 1}:`, error.message);
            // Continuar com próximos batches mesmo se um falhar
        }
        
        // Adiciona delay entre batches, exceto no último
        if (delayMs > 0 && i < batches.length - 1) {
            await new Promise(resolve => setTimeout(resolve, delayMs));
        }
    }
}

// Função para processar múltiplos steps em batch (otimização)
async function processStepsForQStashBatch(steps, sellerId) {
    const processedStepsCache = new Map();
    
    // Identifica steps únicos que precisam de processamento (apenas mídia)
    const mediaSteps = steps.filter(step => ['image', 'video', 'audio'].includes(step.type));
    
    if (mediaSteps.length === 0) {
        // Se não há steps de mídia, retorna cache vazio
        return processedStepsCache;
    }
    
    // Coleta todos os file_ids únicos que precisam ser buscados
    const fileIdsToLookup = new Set();
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    
    for (const step of mediaSteps) {
        const fileUrl = step[urlMap[step.type]];
        if (fileUrl && (fileUrl.startsWith('BAAC') || fileUrl.startsWith('AgAC') || fileUrl.startsWith('AwAC'))) {
            fileIdsToLookup.add(fileUrl);
        }
    }
    
    if (fileIdsToLookup.size === 0) {
        return processedStepsCache;
    }
    
    try {
        // Busca todos os file_ids de uma vez usando IN
        const fileIdsArray = Array.from(fileIdsToLookup);
        const mediaResults = await sqlWithRetry(
            sqlTx`SELECT id, file_id FROM media_library WHERE file_id = ANY(${fileIdsArray}) AND seller_id = ${sellerId}`
        );
        
        // Cria um Map de file_id -> media_id para lookup rápido
        const fileIdToMediaId = new Map();
        for (const media of mediaResults) {
            fileIdToMediaId.set(media.file_id, media.id);
        }
        
        // Processa cada step e armazena no cache
        for (const step of steps) {
            const stepKey = JSON.stringify(step);
            
            if (!['image', 'video', 'audio'].includes(step.type)) {
                // Step não é mídia, não precisa processar
                processedStepsCache.set(stepKey, step);
                continue;
            }
            
            const fileUrl = step[urlMap[step.type]];
            if (!fileUrl) {
                processedStepsCache.set(stepKey, step);
                continue;
            }
            
            const isLibraryFile = fileUrl.startsWith('BAAC') || fileUrl.startsWith('AgAC') || fileUrl.startsWith('AwAC');
            if (!isLibraryFile) {
                processedStepsCache.set(stepKey, step);
                continue;
            }
            
            const mediaId = fileIdToMediaId.get(fileUrl);
            if (mediaId) {
                // Cria uma cópia do step substituindo file_id por mediaLibraryId
                const processedStep = { ...step };
                processedStep[urlMap[step.type]] = null;
                processedStep.mediaLibraryId = mediaId;
                processedStepsCache.set(stepKey, processedStep);
            } else {
                // Não encontrado na biblioteca, mantém original
                processedStepsCache.set(stepKey, step);
            }
        }
    } catch (error) {
        console.error(`[processStepsForQStashBatch] Erro ao processar steps em batch:`, error);
        // Em caso de erro, retorna cache vazio e o código vai usar processStepForQStash individual
    }
    
    return processedStepsCache;
}

// Função para processar steps e substituir file_id da biblioteca por mediaLibraryId antes de enviar ao QStash
async function processStepForQStash(step, sellerId) {
    // Se o step não é de mídia, retorna sem alterações
    if (!['image', 'video', 'audio'].includes(step.type)) {
        return step;
    }
    
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    const fileUrl = step[urlMap[step.type]];
    
    if (!fileUrl) {
        return step;
    }
    
    // Verifica se é um file_id da biblioteca (começa com BAAC, AgAC, AwAC)
    const isLibraryFile = fileUrl.startsWith('BAAC') || fileUrl.startsWith('AgAC') || fileUrl.startsWith('AwAC');
    
    if (!isLibraryFile) {
        // Se não é da biblioteca, pode ser URL ou outro file_id, mantém como está
        return step;
    }
    
    try {
        // Busca o ID da biblioteca de mídia pelo file_id
        const [media] = await sqlWithRetry(
            'SELECT id FROM media_library WHERE file_id = $1 AND seller_id = $2 LIMIT 1',
            [fileUrl, sellerId]
        );
        
        if (!media) {
            console.warn(`[processStepForQStash] Arquivo da biblioteca não encontrado: ${fileUrl}`);
            return step; // Retorna o step original se não encontrar
        }
        
        // Cria uma cópia do step substituindo file_id por mediaLibraryId
        const processedStep = { ...step };
        processedStep[urlMap[step.type]] = null; // Remove o file_id
        processedStep.mediaLibraryId = media.id; // Adiciona o ID da biblioteca
        
        console.log(`[processStepForQStash] File_id ${fileUrl} substituído por mediaLibraryId: ${media.id}`);
        
        return processedStep;
    } catch (error) {
        console.error(`[processStepForQStash] Erro ao processar arquivo da biblioteca:`, error);
        // Em caso de erro, retorna o step original
        return step;
    }
}

async function handleMediaNode(node, botToken, chatId, caption) {
    const type = node.type;
    const nodeData = node.data || {};
    const urlMap = { image: 'imageUrl', video: 'videoUrl', audio: 'audioUrl' };
    const fileIdentifier = nodeData[urlMap[type]];

    if (!fileIdentifier) {
        console.warn(`[Flow Media] Nenhum file_id ou URL fornecido para o nó de ${type} ${node.id}`);
        return null;
    }

    const isLibraryFile = fileIdentifier.startsWith('BAAC') || fileIdentifier.startsWith('AgAC') || fileIdentifier.startsWith('AwAC');
    let response;
    const timeout = type === 'video' ? 60000 : 30000;

    if (isLibraryFile) {
        if (type === 'audio') {
            const duration = parseInt(nodeData.durationInSeconds, 10) || 0;
            if (duration > 0) {
                await sendTelegramRequest(botToken, 'sendChatAction', { chat_id: chatId, action: 'record_voice' });
                await new Promise(resolve => setTimeout(resolve, duration * 1000));
            }
        }
        response = await sendMediaAsProxy(botToken, chatId, fileIdentifier, type, caption);
    } else {
        const methodMap = { image: 'sendPhoto', video: 'sendVideo', audio: 'sendVoice' };
        const fieldMap = { image: 'photo', video: 'video', audio: 'voice' };
        
        const method = methodMap[type];
        const field = fieldMap[type];
        
        const payload = { chat_id: chatId, [field]: fileIdentifier, caption };
        response = await sendTelegramRequest(botToken, method, payload, { timeout });
    }
    
    return response;
}

// --- FUNÇÕES DE SUPORTE A TOKENS ---
function hashToken(token) {
    return crypto.createHash('sha256').update(token).digest('hex');
}

function getRequestFingerprint(req) {
    const forwardedFor = req.headers['x-forwarded-for'];
    let ipAddress = null;

    if (Array.isArray(forwardedFor) && forwardedFor.length > 0) {
        ipAddress = forwardedFor[0];
    } else if (typeof forwardedFor === 'string') {
        ipAddress = forwardedFor.split(',')[0].trim();
    } else if (req.ip) {
        ipAddress = req.ip;
    }

    const userAgentHeader = req.headers['user-agent'];
    const userAgent = userAgentHeader ? String(userAgentHeader).slice(0, 512) : null;

    return { ipAddress, userAgent };
}

async function persistRefreshToken({ sellerId, refreshToken, tokenId, userAgent, ipAddress }) {
    const tokenHash = hashToken(refreshToken);
    const expiresAt = new Date(Date.now() + REFRESH_TOKEN_TTL_SECONDS * 1000);

    await sqlTx`
        INSERT INTO seller_refresh_tokens (seller_id, token_id, token_hash, user_agent, ip_address, expires_at)
        VALUES (${sellerId}, ${tokenId}::uuid, ${tokenHash}, ${userAgent}, ${ipAddress}, ${expiresAt})
    `;

    return { tokenHash, expiresAt };
}

async function findRefreshTokenByHash(tokenHash) {
    if (!tokenHash) {
        return null;
    }

    const rows = await sqlTx`
        SELECT id, seller_id, token_id, token_hash, user_agent, ip_address, created_at, expires_at, last_used_at
        FROM seller_refresh_tokens
        WHERE token_hash = ${tokenHash}
        LIMIT 1
    `;

    return rows[0] || null;
}

async function revokeRefreshTokenByHash(tokenHash) {
    if (!tokenHash) {
        return;
    }

    await sqlTx`
        DELETE FROM seller_refresh_tokens
        WHERE token_hash = ${tokenHash}
    `;
}

async function issueAuthTokensForSeller(seller, req) {
    const tokenPayload = { id: seller.id, email: seller.email };
    const accessToken = jwt.sign(tokenPayload, JWT_SECRET, { expiresIn: ACCESS_TOKEN_TTL_SECONDS });

    const tokenId = uuidv4();
    const refreshPayload = { sub: seller.id, tokenId };
    const refreshToken = jwt.sign(refreshPayload, JWT_REFRESH_SECRET, { expiresIn: REFRESH_TOKEN_TTL_SECONDS });

    const { ipAddress, userAgent } = getRequestFingerprint(req);
    await persistRefreshToken({
        sellerId: seller.id,
        refreshToken,
        tokenId,
        userAgent,
        ipAddress,
    });

    return {
        accessToken,
        refreshToken,
        accessTokenExpiresIn: ACCESS_TOKEN_TTL_SECONDS,
        refreshTokenExpiresIn: REFRESH_TOKEN_TTL_SECONDS,
    };
}

// --- MIDDLEWARE DE AUTENTICAÇÃO ---
async function authenticateJwt(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.status(401).json({ message: 'Token não fornecido.' });
    
    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) {
            if (err.name === 'TokenExpiredError') {
                return res.status(401).json({ message: 'Token expirado.', code: 'ACCESS_TOKEN_EXPIRED' });
            }
            return res.status(403).json({ message: 'Token inválido.', code: 'ACCESS_TOKEN_INVALID' });
        }
        req.user = user;
        next();
    });
}

// --- MIDDLEWARE DE LOG DE REQUISIÇÕES ---
async function logApiRequest(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) return next();
    try {
        const sellerResult = await sqlTx`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length > 0) {
            sqlTx`INSERT INTO api_requests (seller_id, endpoint) VALUES (${sellerResult[0].id}, ${req.path})`.catch(err => console.error("Falha ao logar requisição:", err));
        }
    } catch (error) {
        console.error("Erro no middleware de log:", error);
    }
    next();
}

// --- FUNÇÕES DE LÓGICA DE NEGÓCIO ---
// Funções de geração de PIX movidas para backend/shared/pix.js

function extractWiinpayCustomer(rawData) {
    if (!rawData) return {};
    const container = rawData.customer || rawData.payer || rawData.cliente || rawData?.data?.customer || rawData?.payment?.customer || {};
    const name = container?.name || container?.full_name || container?.nome;
    const document =
        container?.document ||
        container?.document_number ||
        container?.documento ||
        container?.cpf ||
        container?.cnpj ||
        container?.tax_id;
    return {
        name: name || null,
        document: document || null
    };
}

function getSellerWiinpayApiKey(seller) {
    if (!seller) return null;
    return seller.wiinpay_api_key || seller.wiinpay_token || seller.wiinpay_key || null;
}

function parseWiinpayPayment(rawData) {
    if (!rawData) {
        return { id: null, status: null, customer: {} };
    }
    const payment =
        rawData.payment ||
        rawData.data ||
        rawData.payload ||
        rawData.transaction ||
        rawData;

    const id =
        payment?.id ||
        payment?.payment_id ||
        payment?.paymentId ||
        payment?.transaction_id ||
        payment?.transactionId ||
        rawData.payment_id ||
        rawData.paymentId ||
        rawData.id;

    const status = String(
        payment?.status ||
        rawData.status ||
        rawData.payment_status ||
        payment?.payment_status ||
        ''
    ).toLowerCase();

    return {
        id: id || null,
        status,
        customer: extractWiinpayCustomer(payment || rawData || {})
    };
}

async function getWiinpayPaymentStatus(paymentId, apiKey) {
    if (!apiKey) {
        throw new Error('Credenciais da WiinPay não configuradas.');
    }
    const response = await axios.get(`https://api-v2.wiinpay.com.br/payment/list/${paymentId}`, {
        headers: {
            Accept: 'application/json',
            Authorization: `Bearer ${apiKey}`
        }
    });

    const data = Array.isArray(response.data) ? response.data[0] : response.data;
    return parseWiinpayPayment(data);
}

async function getParadisePaymentStatus(transactionId, secretKey) {
    if (!secretKey) {
        throw new Error('Credenciais da Paradise não configuradas.');
    }
    
    try {
        const response = await axios.get(`https://multi.paradisepags.com/api/v1/query.php?action=get_transaction&id=${transactionId}`, {
            headers: {
                'X-API-Key': secretKey,
                'Content-Type': 'application/json',
            },
        });

        const data = response.data;
        const status = String(data?.status || '').toLowerCase();
        const customerData = data?.customer_data?.customer || {};

        return {
            status,
            customer: {
                name: customerData?.name || '',
                document: customerData?.document || '',
                email: customerData?.email || '',
                phone: customerData?.phone || '',
            },
        };
    } catch (error) {
        const errorMessage = error.response?.data?.message || error.response?.data?.error || error.message;
        console.error(`[Paradise Status] Erro ao consultar transação ${transactionId}:`, errorMessage);
        throw new Error(`Erro ao consultar status na Paradise: ${errorMessage || 'Erro desconhecido'}`);
    }
}
async function handleSuccessfulPayment(transaction_id, customerData) {
    try {
        // Primeiro, buscar a transação para verificar status e meta_event_id
        const [transaction] = await sqlTx`SELECT * FROM pix_transactions WHERE id = ${transaction_id}`;
        if (!transaction) {
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} não encontrada.`);
            return;
        }

        // Se já tem meta_event_id, eventos já foram enviados - evitar duplicação
        if (transaction.status === 'paid' && transaction.meta_event_id) {
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já processada e eventos já enviados (meta_event_id: ${transaction.meta_event_id}). Ignorando.`);
            return;
        }

        const wasAlreadyPaid = transaction.status === 'paid';
        
        // Se não estava paga, atualizar status de forma atômica
        if (!wasAlreadyPaid) {
            const [updated] = await sqlTx`
                UPDATE pix_transactions 
                SET status = 'paid', paid_at = NOW() 
                WHERE id = ${transaction_id} AND status != 'paid' 
                RETURNING *
            `;
            if (!updated) {
                console.log(`[handleSuccessfulPayment] Não foi possível atualizar status da transação ${transaction_id}.`);
                return;
            }
            transaction.status = 'paid';
            transaction.paid_at = updated.paid_at || new Date();
        } else {
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já estava paga mas eventos não foram enviados. Enviando agora.`);
            // Garantir que paid_at está definido
            if (!transaction.paid_at) {
                await sqlTx`UPDATE pix_transactions SET paid_at = NOW() WHERE id = ${transaction_id}`;
                transaction.paid_at = new Date();
            }
        }

        // Verificação final antes de enviar eventos (prevenir race condition)
        // Se outra thread já enviou eventos enquanto processávamos, abortar
        const [finalCheck] = await sqlTx`SELECT meta_event_id FROM pix_transactions WHERE id = ${transaction_id}`;
        if (finalCheck.meta_event_id) {
            console.log(`[handleSuccessfulPayment] Transação ${transaction_id} já tem meta_event_id (race condition detectada). Abortando envio duplicado.`);
            return;
        }

        console.log(`[handleSuccessfulPayment] Processando pagamento para transação ${transaction_id}.`);

        if (adminSubscription && webpush) {
            const payload = JSON.stringify({
                title: 'Nova Venda Paga!',
                body: `Venda de R$ ${parseFloat(transaction.pix_value).toFixed(2)} foi confirmada.`,
            });
            webpush.sendNotification(adminSubscription, payload).catch(error => {
                if (error.statusCode === 410) {
                    console.log("Inscrição de notificação expirada. Removendo.");
                    adminSubscription = null;
                } else {
                    console.warn("Falha ao enviar notificação push (não-crítico):", error.message);
                }
            });
        }
        
        const [click] = await sqlTx`SELECT * FROM clicks WHERE id = ${transaction.click_id_internal}`;
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${click.seller_id}`;

        if (click && seller) {
            const finalCustomerData = customerData || { name: "Cliente Pagante", document: null };
            const productData = { id: "prod_final", name: "Produto Vendido" };

            await sendEventToUtmify('paid', click, transaction, seller, finalCustomerData, productData);
            await sendMetaEvent('Purchase', click, transaction, finalCustomerData);
        } else {
            console.error(`[handleSuccessfulPayment] ERRO: Não foi possível encontrar dados do clique ou vendedor para a transação ${transaction_id}`);
        }
    } catch(error) {
        console.error(`[handleSuccessfulPayment] ERRO CRÍTICO ao processar pagamento da transação ${transaction_id}:`, error);
    }
}

// --- MIDDLEWARE DE AUTENTICAÇÃO POR API KEY ---
async function authenticateApiKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    if (!apiKey) {
        return res.status(401).json({ message: 'Chave de API não fornecida.' });
    }
    try {
        const sellerResult = await sqlTx`SELECT id FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            return res.status(401).json({ message: 'Chave de API inválida.' });
        }
        req.sellerId = sellerResult[0].id;
        next();
    } catch (error) {
        console.error("Erro na autenticação por API Key:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
}

// --- ROTAS DO PAINEL ADMINISTRATIVO ---
function authenticateAdmin(req, res, next) {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ message: 'Acesso negado. Chave de administrador inválida.' });
    }
    next();
}

// Endpoint para validar chave de admin
// Endpoint para fornecer configurações ao frontend
app.get('/api/config', (req, res) => {
    // Usa variável de ambiente se definida, senão detecta automaticamente
    const apiBaseUrl = process.env.API_BASE_URL || (() => {
        const isProduction = process.env.NODE_ENV === 'production';
        if (isProduction) {
            const protocol = req.headers['x-forwarded-proto'] || 'https';
            const host = req.headers['host'];
            return `${protocol}://${host}`;
        }
        return `http://localhost:${PORT}`;
    })();

    res.json({
        apiBaseUrl: apiBaseUrl,
        environment: process.env.NODE_ENV || 'development'
    });
});

app.get('/api/pix-status/:transaction_id', async (req, res) => {
    const { transaction_id } = req.params;

    if (!transaction_id) {
        return res.status(400).json({ error: 'ID da transação não fornecido.' });
    }

    try {
        const result = await sqlTx`
            SELECT status, pix_value FROM pix_transactions WHERE provider_transaction_id = ${transaction_id}
        `;

        if (result.length === 0) {
            return res.status(404).json({ error: 'Transação não encontrada.' });
        }

        const transaction = result[0];
        res.status(200).json({ 
            status: transaction.status, 
            pix_value: transaction.pix_value 
        });

    } catch (error) {
        console.error('Erro ao consultar status do PIX:', error);
        res.status(500).json({ error: 'Erro interno ao verificar o status do PIX.' });
    }
});
app.post('/api/admin/validate-key', (req, res) => {
    const adminKey = req.headers['x-admin-api-key'];
    if (!adminKey || adminKey !== ADMIN_API_KEY) {
        return res.status(403).json({ 
            message: 'Chave de administrador inválida.',
            valid: false 
        });
    }
    res.status(200).json({ 
        message: 'Chave válida.',
        valid: true,
        timestamp: new Date().toISOString()
    });
});

async function sendHistoricalMetaEvent(eventName, clickData, transactionData, targetPixel) {
    let payload_sent = null;
    try {
        const userData = {};
        if (clickData.ip_address) userData.client_ip_address = clickData.ip_address;
        if (clickData.user_agent) userData.client_user_agent = clickData.user_agent;
        if (clickData.fbp) userData.fbp = clickData.fbp;
        if (clickData.fbc) userData.fbc = clickData.fbc;
        if (clickData.firstName) userData.fn = crypto.createHash('sha256').update(clickData.firstName.toLowerCase()).digest('hex');
        if (clickData.lastName) userData.ln = crypto.createHash('sha256').update(clickData.lastName.toLowerCase()).digest('hex');
        
        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        const { pixelId, accessToken } = targetPixel;
        const event_time = Math.floor(new Date(transactionData.paid_at).getTime() / 1000);
        const event_id = `${eventName}.${transactionData.id}.${pixelId}`;

        const payload = {
            data: [{
                event_name: eventName,
                event_time: event_time,
                event_id,
                action_source: 'other',
                user_data: userData,
                custom_data: {
                    currency: 'BRL',
                    value: parseFloat(transactionData.pix_value)
                },
            }]
        };
        payload_sent = payload;

        if (Object.keys(userData).length === 0) {
            throw new Error('Dados de usuário insuficientes para envio (IP/UserAgent faltando).');
        }

        await axios.post(`https://graph.facebook.com/v19.0/${pixelId}/events`, payload, { params: { access_token: accessToken } });
        
        return { success: true, payload: payload_sent };

    } catch (error) {
        const metaError = error.response?.data?.error || { message: error.message };
        console.error(`Erro ao reenviar evento (Transação ID: ${transactionData.id}):`, metaError.message);
        return { success: false, error: metaError, payload: payload_sent };
    }
}

app.post('/api/admin/resend-events', authenticateAdmin, async (req, res) => {
    const { 
        target_pixel_id, target_meta_api_token, seller_id, 
        start_date, end_date, page = 1, limit = 50
    } = req.body;

    if (!target_pixel_id || !target_meta_api_token || !start_date || !end_date) {
        return res.status(400).json({ message: 'Todos os campos obrigatórios devem ser preenchidos.' });
    }

    try {
        const query = seller_id
            ? sqlTx`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND c.seller_id = ${seller_id} AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`
            : sqlTx`SELECT pt.*, c.click_id, c.ip_address, c.user_agent, c.fbp, c.fbc, c.city, c.state FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE pt.status = 'paid' AND pt.paid_at BETWEEN ${start_date} AND ${end_date} ORDER BY pt.paid_at ASC`;
        
        const allPaidTransactions = await query;
        
        if (allPaidTransactions.length === 0) {
            return res.status(200).json({ 
                total_events: 0, 
                total_pages: 0, 
                message: 'Nenhuma transação paga encontrada para os filtros fornecidos.' 
            });
        }

        const clickIds = allPaidTransactions.map(t => t.click_id).filter(Boolean);
        let userDataMap = new Map();
        if (clickIds.length > 0) {
            const telegramUsers = await sqlTx`
                SELECT click_id, first_name, last_name 
                FROM telegram_chats 
                WHERE click_id = ANY(${clickIds})
            `;
            telegramUsers.forEach(user => {
                const cleanClickId = user.click_id.startsWith('/start ') ? user.click_id : `/start ${user.click_id}`;
                userDataMap.set(cleanClickId, { firstName: user.first_name, lastName: user.last_name });
            });
        }

        const totalEvents = allPaidTransactions.length;
        const totalPages = Math.ceil(totalEvents / limit);
        const offset = (page - 1) * limit;
        const batch = allPaidTransactions.slice(offset, offset + limit);
        
        const detailedResults = [];
        const targetPixel = { pixelId: target_pixel_id, accessToken: target_meta_api_token };

        console.log(`[ADMIN] Processando página ${page}/${totalPages}. Lote com ${batch.length} eventos.`);

        for (const transaction of batch) {
            const extraUserData = userDataMap.get(transaction.click_id);
            const enrichedTransactionData = { ...transaction, ...extraUserData };
            const result = await sendHistoricalMetaEvent('Purchase', enrichedTransactionData, transaction, targetPixel);
            
            detailedResults.push({
                transaction_id: transaction.id,
                status: result.success ? 'success' : 'failure',
                payload_sent: result.payload,
                meta_response: result.error || 'Enviado com sucesso.'
            });
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        res.status(200).json({
            total_events: totalEvents,
            total_pages: totalPages,
            current_page: page,
            limit: limit,
            results: detailedResults
        });

    } catch (error) {
        console.error("Erro geral na rota de reenviar eventos:", error);
        res.status(500).json({ message: 'Erro interno do servidor ao processar o reenvio.' });
    }
});

app.get('/api/admin/vapidPublicKey', authenticateAdmin, (req, res) => {
    if (!process.env.VAPID_PUBLIC_KEY) {
        return res.status(500).send('VAPID Public Key não configurada no servidor.');
    }
    res.type('text/plain').send(process.env.VAPID_PUBLIC_KEY);
});

app.post('/api/admin/save-subscription', authenticateAdmin, (req, res) => {
    adminSubscription = req.body;
    console.log("Inscrição de admin para notificações recebida e guardada.");
    res.status(201).json({});
});

app.get('/api/admin/dashboard', authenticateAdmin, async (req, res) => {
    try {
        const totalSellers = await sqlTx`SELECT COUNT(*) FROM sellers;`;
        const paidTransactions = await sqlTx`SELECT COUNT(*) as count, SUM(pix_value) as total_revenue FROM pix_transactions WHERE status = 'paid';`;
        const total_sellers = parseInt(totalSellers[0].count);
        const total_paid_transactions = parseInt(paidTransactions[0].count);
        const total_revenue = parseFloat(paidTransactions[0].total_revenue || 0);
        const saas_profit = total_revenue * 0.0500;
        res.json({
            total_sellers, total_paid_transactions,
            total_revenue: total_revenue.toFixed(2),
            saas_profit: saas_profit.toFixed(2)
        });
    } catch (error) {
        console.error("Erro no dashboard admin:", error);
        res.status(500).json({ message: 'Erro ao buscar dados do dashboard.' });
    }
});
app.get('/api/admin/ranking', authenticateAdmin, async (req, res) => {
    try {
        const ranking = await sqlTx`
            SELECT s.id, s.name, s.email, COUNT(pt.id) AS total_sales, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name, s.email ORDER BY total_revenue DESC LIMIT 20;`;
        res.json(ranking);
    } catch (error) {
        console.error("Erro no ranking de sellers:", error);
        res.status(500).json({ message: 'Erro ao buscar ranking.' });
    }
});
// No seu backend.js
app.get('/api/admin/sellers', authenticateAdmin, async (req, res) => {
      try {
        const sellers = await sqlTx`SELECT id, name, email, created_at, is_active, commission_rate FROM sellers ORDER BY created_at DESC;`;
        res.json(sellers);
      } catch (error) {
        res.status(500).json({ message: 'Erro ao listar vendedores.' });
      }
    });
app.post('/api/admin/sellers/:id/toggle-active', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { isActive } = req.body;
    try {
        await sqlTx`UPDATE sellers SET is_active = ${isActive} WHERE id = ${id};`;
        res.status(200).json({ message: `Usuário ${isActive ? 'ativado' : 'bloqueado'} com sucesso.` });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar status do usuário.' });
    }
});
app.put('/api/admin/sellers/:id/password', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { newPassword } = req.body;
    if (!newPassword || newPassword.length < 8) return res.status(400).json({ message: 'A nova senha deve ter pelo menos 8 caracteres.' });
    try {
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        await sqlTx`UPDATE sellers SET password_hash = ${hashedPassword} WHERE id = ${id};`;
        res.status(200).json({ message: 'Senha alterada com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar senha.' });
    }
});
app.put('/api/admin/sellers/:id/credentials', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { pushinpay_token, cnpay_public_key, cnpay_secret_key, wiinpay_api_key, pixup_client_id, pixup_client_secret, paradise_secret_key, paradise_product_hash } = req.body;
    try {
        await sqlTx`
            UPDATE sellers
            SET pushinpay_token = ${pushinpay_token}, cnpay_public_key = ${cnpay_public_key}, cnpay_secret_key = ${cnpay_secret_key}, wiinpay_api_key = ${wiinpay_api_key}, pixup_client_id = ${pixup_client_id || null}, pixup_client_secret = ${pixup_client_secret || null}, paradise_secret_key = ${paradise_secret_key || null}, paradise_product_hash = ${paradise_product_hash || null}
            WHERE id = ${id};`;
        res.status(200).json({ message: 'Credenciais alteradas com sucesso.' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao alterar credenciais.' });
    }
});
app.get('/api/admin/transactions', authenticateAdmin, async (req, res) => {
    try {
        const page = parseInt(req.query.page || 1);
        const limit = parseInt(req.query.limit || 20);
        const offset = (page - 1) * limit;
        const transactions = await sqlTx`
            SELECT pt.id, pt.status, pt.pix_value, pt.provider, pt.created_at, s.name as seller_name, s.email as seller_email
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            JOIN sellers s ON c.seller_id = s.id ORDER BY pt.created_at DESC
            LIMIT ${limit} OFFSET ${offset};`;
         const totalTransactionsResult = await sqlTx`SELECT COUNT(*) FROM pix_transactions;`;
         const total = parseInt(totalTransactionsResult[0].count);
        res.json({ transactions, total, page, pages: Math.ceil(total / limit), limit });
    } catch (error) {
        console.error("Erro ao buscar transações admin:", error);
        res.status(500).json({ message: 'Erro ao buscar transações.' });
    }
});
app.get('/api/admin/usage-analysis', authenticateAdmin, async (req, res) => {
    try {
        const usageData = await sqlTx`
            SELECT
                s.id, s.name, s.email,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '1 hour') AS requests_last_hour,
                COUNT(ar.id) FILTER (WHERE ar.created_at > NOW() - INTERVAL '24 hours') AS requests_last_24_hours
            FROM sellers s
            LEFT JOIN api_requests ar ON s.id = ar.seller_id
            GROUP BY s.id, s.name, s.email
            ORDER BY requests_last_24_hours DESC, requests_last_hour DESC;
        `;
        res.json(usageData);
    } catch (error) {
        console.error("Erro na análise de uso:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de uso.' });
    }
});

app.put('/api/admin/sellers/:id/commission', authenticateAdmin, async (req, res) => {
    const { id } = req.params;
    const { commission_rate } = req.body;

    if (typeof commission_rate !== 'number' || commission_rate < 0 || commission_rate > 1) {
        return res.status(400).json({ message: 'A taxa de comissão deve ser um número entre 0 e 1 (ex: 0.0299 para 2.99%).' });
    }

    try {
        await sqlTx`UPDATE sellers SET commission_rate = ${commission_rate} WHERE id = ${id};`;
        res.status(200).json({ message: 'Comissão do usuário atualizada com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar comissão:", error);
        res.status(500).json({ message: 'Erro ao atualizar a comissão.' });
    }
});

// ==========================================================
//          ROTAS ÚNICAS DO HOTBOT INTEGRADAS
// ==========================================================
app.get('/api/health', async (req, res) => {
    try {
        await sqlWithRetry('SELECT 1 as status;');
        res.status(200).json({ status: 'ok' });
    } catch (error) {
        res.status(500).json({ status: 'error', message: 'Erro de conexão ao BD.' });
    }
});

// --- ROTAS DE FLOWS ---
app.get('/api/flows', authenticateJwt, async (req, res) => {
    try {
        const flows = await sqlWithRetry('SELECT * FROM flows WHERE seller_id = $1 ORDER BY created_at DESC', [req.user.id]);
        res.status(200).json(flows.map(f => ({ ...f, nodes: f.nodes || { nodes: [], edges: [] } })));
    } catch (error) { res.status(500).json({ message: 'Erro ao buscar os fluxos.' }); }
});

app.post('/api/flows', authenticateJwt, async (req, res) => {
    const { name, botId } = req.body;
    if (!name || !botId) return res.status(400).json({ message: 'Nome e ID do bot são obrigatórios.' });
    try {
        const initialFlow = { nodes: [{ id: 'start', type: 'trigger', position: { x: 250, y: 50 }, data: {} }], edges: [] };
        // Cria o novo fluxo como inativo
        const [newFlow] = await sqlWithRetry(`
            INSERT INTO flows (seller_id, bot_id, name, nodes, is_active) VALUES ($1, $2, $3, $4, FALSE) RETURNING *;`, [req.user.id, botId, name, JSON.stringify(initialFlow)]);
        res.status(201).json(newFlow);
    } catch (error) { res.status(500).json({ message: 'Erro ao criar o fluxo.' }); }
});

app.put('/api/flows/:id', authenticateJwt, async (req, res) => {
    const { name, nodes } = req.body;
    if (!name || !nodes) return res.status(400).json({ message: 'Nome e estrutura de nós são obrigatórios.' });
    
    try {
        // Parse e validar os nodes antes de salvar
        const parsedNodes = JSON.parse(nodes);
        const nodesArray = parsedNodes.nodes || [];
        
        // Validar se há links em campos de texto
        const validation = validateFlowActions(nodesArray);
        if (!validation.valid) {
            logger.info(`[Flow Validation] Flow save rejected for seller_id: ${req.user.id} - ${validation.message}`);
            return res.status(400).json({ message: validation.message });
        }
        
        // Busca o fluxo para pegar o bot_id
        const [flow] = await sqlWithRetry('SELECT bot_id FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        if (!flow) return res.status(404).json({ message: 'Fluxo não encontrado.' });
        
        const [updated] = await sqlWithRetry('UPDATE flows SET name = $1, nodes = $2, updated_at = NOW() WHERE id = $3 AND seller_id = $4 RETURNING *;', [name, nodes, req.params.id, req.user.id]);
        if (updated) res.status(200).json(updated);
        else res.status(404).json({ message: 'Fluxo não encontrado.' });
    } catch (error) { 
        console.error('[Flow Save] Error:', error);
        res.status(500).json({ message: 'Erro ao salvar o fluxo.' }); 
    }
});

app.get('/api/flows/:id/node-stats', authenticateJwt, async (req, res) => {
    try {
        const [flow] = await sqlTx`
            SELECT node_execution_counts 
            FROM flows 
            WHERE id = ${req.params.id} AND seller_id = ${req.user.id}
        `;
        
        if (!flow) {
            return res.status(404).json({ message: 'Fluxo não encontrado.' });
        }
        
        // Retorna o objeto de contagens ou objeto vazio se for NULL
        const stats = flow.node_execution_counts || {};
        res.status(200).json(stats);
    } catch (error) {
        console.error('[Node Stats] Error:', error);
        res.status(500).json({ message: 'Erro ao buscar estatísticas dos nodes.' });
    }
});

// ==========================================================
// ENDPOINTS PARA DISPARO_FLOWS
// ==========================================================

// Função auxiliar para validar fluxo de disparo (pode ter trigger, mas deve ter pelo menos uma ação)
function validateDisparoFlow(nodesArray) {
    if (!Array.isArray(nodesArray)) {
        return { valid: false, message: 'Nodes deve ser um array.' };
    }
    
    if (nodesArray.length === 0) {
        return { valid: false, message: 'Fluxo deve ter pelo menos um nó.' };
    }
    
    // Verificar se há pelo menos um nó de ação (trigger é permitido mas não conta como ação)
    const hasAction = nodesArray.some(node => node.type === 'action');
    if (!hasAction) {
        return { valid: false, message: 'Fluxo deve ter pelo menos um nó de ação.' };
    }
    
    return { valid: true };
}

app.get('/api/disparo-flows', authenticateJwt, async (req, res) => {
    try {
        const flows = await sqlWithRetry('SELECT * FROM disparo_flows WHERE seller_id = $1 ORDER BY created_at DESC', [req.user.id]);
        res.status(200).json(flows.map(f => ({ ...f, nodes: f.nodes || { nodes: [], edges: [] } })));
    } catch (error) {
        console.error('[Disparo Flows] Error:', error);
        res.status(500).json({ message: 'Erro ao buscar os fluxos de disparo.' });
    }
});

app.get('/api/disparo-flows/:id', authenticateJwt, async (req, res) => {
    try {
        const [flow] = await sqlWithRetry('SELECT * FROM disparo_flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        if (!flow) {
            return res.status(404).json({ message: 'Fluxo de disparo não encontrado.' });
        }
        res.status(200).json({ ...flow, nodes: flow.nodes || { nodes: [], edges: [] } });
    } catch (error) {
        console.error('[Disparo Flow] Error:', error);
        res.status(500).json({ message: 'Erro ao buscar o fluxo de disparo.' });
    }
});

app.post('/api/disparo-flows', authenticateJwt, async (req, res) => {
    const { name, botId } = req.body;
    if (!name || !botId) return res.status(400).json({ message: 'Nome e ID do bot são obrigatórios.' });
    try {
        // Criar fluxo inicial com trigger (disparo manual)
        const initialFlow = { nodes: [{ id: 'start', type: 'trigger', position: { x: 250, y: 50 }, data: {}, deletable: false }], edges: [] };
        const [newFlow] = await sqlWithRetry(`
            INSERT INTO disparo_flows (seller_id, bot_id, name, nodes) VALUES ($1, $2, $3, $4) RETURNING *;`, 
            [req.user.id, botId, name, JSON.stringify(initialFlow)]);
        res.status(201).json({ ...newFlow, nodes: newFlow.nodes || { nodes: [], edges: [] } });
    } catch (error) {
        console.error('[Disparo Flow Create] Error:', error);
        res.status(500).json({ message: 'Erro ao criar o fluxo de disparo.' });
    }
});

app.put('/api/disparo-flows/:id', authenticateJwt, async (req, res) => {
    const { name, nodes } = req.body;
    if (!name || !nodes) return res.status(400).json({ message: 'Nome e estrutura de nós são obrigatórios.' });
    
    try {
        // Parse e validar os nodes antes de salvar
        const parsedNodes = JSON.parse(nodes);
        const nodesArray = parsedNodes.nodes || [];
        
        // Validar que não há trigger e há pelo menos uma ação
        const disparoValidation = validateDisparoFlow(nodesArray);
        if (!disparoValidation.valid) {
            logger.info(`[Disparo Flow Validation] Flow save rejected for seller_id: ${req.user.id} - ${disparoValidation.message}`);
            return res.status(400).json({ message: disparoValidation.message });
        }
        
        // Validar se há links em campos de texto (mesma validação dos fluxos normais)
        const validation = validateFlowActions(nodesArray);
        if (!validation.valid) {
            logger.info(`[Disparo Flow Validation] Flow save rejected for seller_id: ${req.user.id} - ${validation.message}`);
            return res.status(400).json({ message: validation.message });
        }
        
        // Busca o fluxo para verificar se existe e pertence ao seller
        const [flow] = await sqlWithRetry('SELECT bot_id FROM disparo_flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        if (!flow) return res.status(404).json({ message: 'Fluxo de disparo não encontrado.' });
        
        const [updated] = await sqlWithRetry('UPDATE disparo_flows SET name = $1, nodes = $2, updated_at = NOW() WHERE id = $3 AND seller_id = $4 RETURNING *;', [name, nodes, req.params.id, req.user.id]);
        if (updated) res.status(200).json({ ...updated, nodes: updated.nodes || { nodes: [], edges: [] } });
        else res.status(404).json({ message: 'Fluxo de disparo não encontrado.' });
    } catch (error) {
        console.error('[Disparo Flow Save] Error:', error);
        res.status(500).json({ message: 'Erro ao salvar o fluxo de disparo.' });
    }
});

app.delete('/api/disparo-flows/:id', authenticateJwt, async (req, res) => {
    try {
        const [deleted] = await sqlWithRetry('DELETE FROM disparo_flows WHERE id = $1 AND seller_id = $2 RETURNING *;', [req.params.id, req.user.id]);
        if (deleted) res.status(200).json({ message: 'Fluxo de disparo deletado com sucesso.' });
        else res.status(404).json({ message: 'Fluxo de disparo não encontrado.' });
    } catch (error) {
        console.error('[Disparo Flow Delete] Error:', error);
        res.status(500).json({ message: 'Erro ao deletar o fluxo de disparo.' });
    }
});

app.patch('/api/flows/:id/activate', authenticateJwt, async (req, res) => {
    try {
        const { isActive } = req.body;
        if (typeof isActive !== 'boolean') return res.status(400).json({ message: 'isActive deve ser um boolean.' });
        
        // Busca o fluxo para pegar o bot_id
        const [flow] = await sqlWithRetry('SELECT bot_id FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        if (!flow) return res.status(404).json({ message: 'Fluxo não encontrado.' });
        
        if (isActive) {
            // Se está ativando, desativa todos os outros fluxos do mesmo bot
            await sqlWithRetry('UPDATE flows SET is_active = FALSE WHERE bot_id = $1 AND seller_id = $2 AND id != $3', [flow.bot_id, req.user.id, req.params.id]);
        }
        
        // Atualiza o status do fluxo
        const [updated] = await sqlWithRetry('UPDATE flows SET is_active = $1 WHERE id = $2 AND seller_id = $3 RETURNING *;', [isActive, req.params.id, req.user.id]);
        if (updated) res.status(200).json(updated);
        else res.status(404).json({ message: 'Fluxo não encontrado.' });
    } catch (error) { res.status(500).json({ message: 'Erro ao atualizar status do fluxo.' }); }
});
app.delete('/api/flows/:id', authenticateJwt, async (req, res) => {
    try {

        
        // Primeiro verificar se o flow existe e pertence ao usuário
        const [existingFlow] = await sqlWithRetry('SELECT id FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);

        
        if (!existingFlow) {
            console.log('Flow não encontrado ou não pertence ao usuário');
            return res.status(404).json({ message: 'Fluxo não encontrado.' });
        }
        
        // Se existe, deletar
        const result = await sqlWithRetry('DELETE FROM flows WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);

        res.status(204).send();
        
    } catch (error) { 
        console.error('Erro ao deletar flow:', error);
        res.status(500).json({ message: 'Erro ao deletar o fluxo.' }); 
    }
});

// --- ROTAS DE CHATS ---
app.get('/api/chats/:botId', authenticateJwt, async (req, res) => {
    try {
        const users = await sqlWithRetry(`
            WITH base_chats AS (
                SELECT 
                    t.chat_id,
                    (array_agg(t.first_name ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as first_name,
                    (array_agg(t.last_name ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as last_name,
                    (array_agg(t.username ORDER BY t.created_at DESC) FILTER (WHERE t.sender_type = 'user'))[1] as username,
                    (array_agg(t.click_id ORDER BY t.created_at DESC) FILTER (WHERE t.click_id IS NOT NULL))[1] as click_id,
                    MAX(t.created_at) as last_message_at
                FROM telegram_chats t
                WHERE t.bot_id = $1 AND t.seller_id = $2
                GROUP BY t.chat_id
            ),
            latest_messages AS (
                SELECT DISTINCT ON (chat_id)
                    chat_id,
                    message_text,
                    sender_type,
                    created_at
                FROM telegram_chats
                WHERE bot_id = $1 AND seller_id = $2
                ORDER BY chat_id, created_at DESC
            ),
            paid_leads AS (
                SELECT DISTINCT tc.chat_id
                FROM telegram_chats tc
                JOIN clicks c ON c.click_id = tc.click_id
                JOIN pix_transactions pt ON pt.click_id_internal = c.id
                WHERE tc.bot_id = $1
                  AND tc.seller_id = $2
                  AND pt.status = 'paid'
            ),
            custom_tags AS (
                SELECT
                    lcta.chat_id,
                    json_agg(
                        json_build_object(
                            'id', lct.id,
                            'title', lct.title,
                            'color', lct.color,
                            'bot_id', lct.bot_id
                        )
                        ORDER BY LOWER(lct.title)
                    ) AS tags
                FROM lead_custom_tag_assignments lcta
                JOIN lead_custom_tags lct ON lct.id = lcta.tag_id
                WHERE lcta.bot_id = $1
                  AND lcta.seller_id = $2
                GROUP BY lcta.chat_id
            )
            SELECT
                bc.chat_id,
                bc.first_name,
                bc.last_name,
                bc.username,
                bc.click_id,
                bc.last_message_at,
                lm.message_text,
                lm.sender_type AS last_sender_type,
                COALESCE(ct.tags, '[]'::json) AS custom_tags,
                CASE WHEN pl.chat_id IS NOT NULL THEN ARRAY['Pagante'] ELSE ARRAY[]::TEXT[] END AS automatic_tags
            FROM base_chats bc
            LEFT JOIN latest_messages lm ON lm.chat_id = bc.chat_id
            LEFT JOIN paid_leads pl ON pl.chat_id = bc.chat_id
            LEFT JOIN custom_tags ct ON ct.chat_id = bc.chat_id
            ORDER BY bc.last_message_at DESC NULLS LAST
            LIMIT 1000;
        `, [req.params.botId, req.user.id]);
        res.status(200).json(users);
    } catch (error) { 
        console.error('Erro ao buscar usuários do chat:', error);
        res.status(500).json({ message: 'Erro ao buscar usuários do chat.' }); 
    }
});

app.get('/api/chats/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        const messages = await sqlWithRetry(`
            SELECT * FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND seller_id = $3 ORDER BY created_at ASC;`, [req.params.botId, req.params.chatId, req.user.id]);
        res.status(200).json(messages);
    } catch (error) { res.status(500).json({ message: 'Erro ao buscar mensagens.' }); }
});

app.post('/api/chats/:botId/send-message', authenticateJwt, async (req, res) => {
    const { chatId, text } = req.body;
    if (!chatId || !text) return res.status(400).json({ message: 'Chat ID e texto são obrigatórios.' });
    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [req.params.botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });
        const response = await sendTelegramRequest(bot.bot_token, 'sendMessage', { chat_id: chatId, text });
        if (response.ok) {
            await saveMessageToDb(req.user.id, req.params.botId, response.result, 'operator');
        }
        res.status(200).json({ message: 'Mensagem enviada!' });
    } catch (error) { res.status(500).json({ message: 'Não foi possível enviar a mensagem.' }); }
});

app.post('/api/chats/:botId/send-library-media', authenticateJwt, async (req, res) => {
    const { chatId, fileId, fileType } = req.body;
    const { botId } = req.params;

    if (!chatId || !fileId || !fileType) {
        return res.status(400).json({ message: 'Dados incompletos.' });
    }

    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });

        const response = await sendMediaAsProxy(bot.bot_token, chatId, fileId, fileType, null);

        if (response.ok) {
            await saveMessageToDb(req.user.id, botId, response.result, 'operator');
            res.status(200).json({ message: 'Mídia enviada!' });
        } else {
            throw new Error('Falha ao enviar mídia para o usuário final.');
        }
    } catch (error) {
        res.status(500).json({ message: 'Não foi possível enviar a mídia: ' + error.message });
    }
});

// --- ROTAS DE TAGS PERSONALIZADAS ---
app.get('/api/tags', authenticateJwt, async (req, res) => {
    const { botId } = req.query;

    try {
        let query = `
            SELECT id, title, color, bot_id, created_at
            FROM lead_custom_tags
            WHERE seller_id = $1
        `;
        const params = [req.user.id];

        if (botId !== undefined) {
            const parsedBotId = parseInt(botId, 10);
            if (Number.isNaN(parsedBotId)) {
                return res.status(400).json({ message: 'botId inválido.' });
            }
            query += ' AND bot_id = $2';
            params.push(parsedBotId);
        }

        query += ' ORDER BY LOWER(title)';

        const tags = await sqlWithRetry(query, params);
        res.status(200).json(tags);
    } catch (error) {
        console.error('Erro ao listar tags personalizadas:', error);
        res.status(500).json({ message: 'Erro ao listar tags.' });
    }
});

app.post('/api/tags', authenticateJwt, async (req, res) => {
    const { title, color, botId } = req.body || {};
    const trimmedTitle = sanitizeTagTitle(title || '');
    const normalizedColor = normalizeHexColor(color || '');

    if (!trimmedTitle) {
        return res.status(400).json({ message: 'Título da tag é obrigatório.' });
    }

    if (trimmedTitle.length > TAG_TITLE_MAX_LENGTH) {
        return res.status(400).json({ message: `Título deve ter no máximo ${TAG_TITLE_MAX_LENGTH} caracteres.` });
    }

    if (!normalizedColor || !isValidTagColor(normalizedColor)) {
        return res.status(400).json({ message: 'Cor inválida. Utilize o formato HEX (#RRGGBB).' });
    }

    const parsedBotId = parseInt(botId, 10);
    if (Number.isNaN(parsedBotId)) {
        return res.status(400).json({ message: 'botId inválido.' });
    }

    try {
        const [bot] = await sqlWithRetry(
            'SELECT id FROM telegram_bots WHERE id = $1 AND seller_id = $2',
            [parsedBotId, req.user.id]
        );

        if (!bot) {
            return res.status(404).json({ message: 'Bot não encontrado.' });
        }

        const [tag] = await sqlWithRetry(
            `INSERT INTO lead_custom_tags (seller_id, bot_id, title, color)
             VALUES ($1, $2, $3, $4)
             RETURNING id, title, color, bot_id, created_at`,
            [req.user.id, parsedBotId, trimmedTitle, normalizedColor]
        );

        res.status(201).json(tag);
    } catch (error) {
        if (error.code === '23505') {
            return res.status(409).json({ message: 'Já existe uma tag com esse título para este bot.' });
        }
        console.error('Erro ao criar tag personalizada:', error);
        res.status(500).json({ message: 'Erro ao criar tag.' });
    }
});

app.delete('/api/tags/:id', authenticateJwt, async (req, res) => {
    const tagId = parseInt(req.params.id, 10);
    if (Number.isNaN(tagId)) {
        return res.status(400).json({ message: 'ID de tag inválido.' });
    }

    try {
        const [tag] = await sqlWithRetry(
            'SELECT id FROM lead_custom_tags WHERE id = $1 AND seller_id = $2',
            [tagId, req.user.id]
        );

        if (!tag) {
            return res.status(404).json({ message: 'Tag não encontrada.' });
        }

        await sqlWithRetry('DELETE FROM lead_custom_tags WHERE id = $1', [tagId]);
        res.status(204).send();
    } catch (error) {
        console.error('Erro ao excluir tag personalizada:', error);
        res.status(500).json({ message: 'Erro ao excluir tag.' });
    }
});

app.post('/api/leads/:botId/:chatId/tags', authenticateJwt, async (req, res) => {
    const { tagId } = req.body || {};
    const botId = parseInt(req.params.botId, 10);
    const chatId = req.params.chatId;
    const parsedTagId = parseInt(tagId, 10);

    if (Number.isNaN(botId) || !chatId) {
        return res.status(400).json({ message: 'Parâmetros do lead inválidos.' });
    }

    if (Number.isNaN(parsedTagId)) {
        return res.status(400).json({ message: 'tagId inválido.' });
    }

    try {
        const [tag] = await sqlWithRetry(
            'SELECT id, bot_id FROM lead_custom_tags WHERE id = $1 AND seller_id = $2',
            [parsedTagId, req.user.id]
        );

        if (!tag) {
            return res.status(404).json({ message: 'Tag não encontrada.' });
        }

        if (tag.bot_id !== botId) {
            return res.status(400).json({ message: 'Tag não pertence a este bot.' });
        }

        const [chatExists] = await sqlWithRetry(
            'SELECT 1 FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND seller_id = $3 LIMIT 1',
            [botId, chatId, req.user.id]
        );

        if (!chatExists) {
            return res.status(404).json({ message: 'Lead não encontrado.' });
        }

        await sqlWithRetry(
            `INSERT INTO lead_custom_tag_assignments (tag_id, seller_id, bot_id, chat_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING`,
            [parsedTagId, req.user.id, botId, chatId]
        );

        res.status(201).json({ message: 'Tag adicionada ao lead.' });
    } catch (error) {
        console.error('Erro ao adicionar tag ao lead:', error);
        res.status(500).json({ message: 'Erro ao adicionar tag ao lead.' });
    }
});

app.delete('/api/leads/:botId/:chatId/tags/:tagId', authenticateJwt, async (req, res) => {
    const botId = parseInt(req.params.botId, 10);
    const chatId = req.params.chatId;
    const tagId = parseInt(req.params.tagId, 10);

    if (Number.isNaN(botId) || !chatId || Number.isNaN(tagId)) {
        return res.status(400).json({ message: 'Parâmetros inválidos.' });
    }

    try {
        const deleted = await sqlWithRetry(
            `DELETE FROM lead_custom_tag_assignments
             WHERE tag_id = $1 AND seller_id = $2 AND bot_id = $3 AND chat_id = $4
             RETURNING tag_id`,
            [tagId, req.user.id, botId, chatId]
        );

        if (deleted.length === 0) {
            return res.status(404).json({ message: 'Tag não vinculada a este lead.' });
        }

        res.status(204).send();
    } catch (error) {
        console.error('Erro ao remover tag do lead:', error);
        res.status(500).json({ message: 'Erro ao remover tag do lead.' });
    }
});
// --- ROTAS GERAIS DE USUÁRIO ---
app.post('/api/sellers/register', async (req, res) => {
    const { name, email, password, phone } = req.body;

    if (!name || !email || !password || password.length < 8 || !phone) {
        return res.status(400).json({ message: 'Dados inválidos. Nome, email, senha (mínimo 8 caracteres) e telefone são obrigatórios.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const existingSeller = await sqlTx`SELECT id FROM sellers WHERE LOWER(email) = ${normalizedEmail}`;
        if (existingSeller.length > 0) {
            return res.status(409).json({ message: 'Este email já está em uso.' });
        }

        // Adicionar campos de verificação se não existirem
        try {
            await sqlTx`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE`;
            await sqlTx`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS verification_code TEXT`;
            await sqlTx`ALTER TABLE sellers ADD COLUMN IF NOT EXISTS verification_expires TIMESTAMP`;
        } catch (error) {
            console.log('Campos de verificação já existem ou erro:', error.message);
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        const apiKey = uuidv4();
        const verificationCode = crypto.randomBytes(32).toString('hex');
        const verificationExpires = new Date(Date.now() + 24 * 60 * 60 * 1000); // 24 horas
        
        // Criar usuário como não verificado
        await sqlTx`INSERT INTO sellers (name, email, password_hash, api_key, is_active, email_verified, verification_code, verification_expires, phone) VALUES (${name}, ${normalizedEmail}, ${hashedPassword}, ${apiKey}, FALSE, FALSE, ${verificationCode}, ${verificationExpires}, ${phone})`;
        
        // Enviar email de verificação
        try {
            if (!process.env.MAILERSEND_FROM_EMAIL) {
                throw new Error('MAILERSEND_FROM_EMAIL não configurado');
            }
            
            const verificationUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/verify-email?code=${verificationCode}&email=${encodeURIComponent(normalizedEmail)}`;
            
            const sentFrom = new Sender(process.env.MAILERSEND_FROM_EMAIL, 'HotTrack');
            const recipients = [new Recipient(normalizedEmail, name)];
            
            const emailParams = new EmailParams()
                .setFrom(sentFrom)
                .setTo(recipients)
                .setReplyTo(sentFrom)
                .setSubject('Verifique seu email - HotTrack')
                .setHtml(`
                    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                        <h2 style="color: #0ea5e9;">Bem-vindo ao HotTrack!</h2>
                        <p>Olá ${name},</p>
                        <p>Obrigado por se cadastrar no HotTrack. Para ativar sua conta, clique no botão abaixo:</p>
                        <div style="text-align: center; margin: 30px 0;">
                            <a href="${verificationUrl}" style="background-color: #0ea5e9; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Verificar Email</a>
                        </div>
                        <p>Ou copie e cole este link no seu navegador:</p>
                        <p style="word-break: break-all; color: #666;">${verificationUrl}</p>
                        <p>Este link expira em 24 horas.</p>
                        <p>Se você não criou uma conta no HotTrack, ignore este email.</p>
                        <hr style="margin: 30px 0; border: none; border-top: 1px solid #eee;">
                        <p style="color: #666; font-size: 12px;">Este é um email automático, não responda.</p>
                    </div>
                `)
                .setText(`
                    Bem-vindo ao HotTrack!
                    
                    Olá ${name},
                    
                    Obrigado por se cadastrar no HotTrack. Para ativar sua conta, acesse o link abaixo:
                    ${verificationUrl}
                    
                    Este link expira em 24 horas.
                    
                    Se você não criou uma conta no HotTrack, ignore este email.
                `);

            await mailerSend.email.send(emailParams);
            
            res.status(201).json({ 
                message: 'Cadastro realizado! Verifique seu email para ativar a conta.',
                requiresVerification: true
            });
        } catch (emailError) {
            console.error('Erro ao enviar email de verificação:', emailError);
            // Mesmo com erro no email, o usuário foi criado
            res.status(201).json({ 
                message: 'Cadastro realizado! Verifique seu email para ativar a conta.',
                requiresVerification: true,
                warning: 'Erro ao enviar email. Entre em contato com o suporte.'
            });
        }
        
    } catch (error) {
        console.error("Erro no registro:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
// Endpoint para verificar email
app.post('/api/sellers/verify-email', async (req, res) => {
    const { code, email } = req.body;
    
    if (!code || !email) {
        console.log('Erro: Código ou email não fornecidos');
        return res.status(400).json({ message: 'Código e email são obrigatórios.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        
        // Buscar usuário com código válido
        const sellerResult = await sqlTx`
            SELECT id, verification_code, verification_expires, email_verified 
            FROM sellers 
            WHERE email = ${normalizedEmail} AND verification_code = ${code}
        `;
        
        if (sellerResult.length === 0) {
            return res.status(400).json({ message: 'Código de verificação inválido.' });
        }
        
        const seller = sellerResult[0];
        
        // Verificar se já foi verificado
        if (seller.email_verified) {
            return res.status(400).json({ message: 'Email já foi verificado.' });
        }
        
        // Verificar se o código não expirou
        if (new Date() > new Date(seller.verification_expires)) {
            return res.status(400).json({ message: 'Código de verificação expirado.' });
        }
        
        // Ativar conta
        await sqlTx`
            UPDATE sellers 
            SET email_verified = TRUE, is_active = TRUE, verification_code = NULL, verification_expires = NULL 
            WHERE id = ${seller.id}
        `;
        
        res.json({ message: 'Email verificado com sucesso! Sua conta foi ativada.' });
        
    } catch (error) {
        console.error('Erro na verificação de email:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
// Endpoint para reenviar email de verificação
app.post('/api/sellers/resend-verification', async (req, res) => {
    const { email } = req.body;
    
    if (!email) {
        return res.status(400).json({ message: 'Email é obrigatório.' });
    }
    
    try {
        const normalizedEmail = email.trim().toLowerCase();
        
        // Buscar usuário não verificado
        const sellerResult = await sqlTx`
            SELECT id, name, email_verified 
            FROM sellers 
            WHERE email = ${normalizedEmail}
        `;
        
        if (sellerResult.length === 0) {
            return res.status(404).json({ message: 'Usuário não encontrado.' });
        }
        
        const seller = sellerResult[0];
        
        if (seller.email_verified) {
            return res.status(400).json({ message: 'Email já foi verificado.' });
        }
        
        // Gerar novo código
        const verificationCode = crypto.randomBytes(32).toString('hex');
        const verificationExpires = new Date(Date.now() + 24 * 60 * 60 * 1000);
        
        // Atualizar código no banco
        await sqlTx`
            UPDATE sellers 
            SET verification_code = ${verificationCode}, verification_expires = ${verificationExpires}
            WHERE id = ${seller.id}
        `;
        
        // Enviar novo email
        if (!process.env.MAILERSEND_FROM_EMAIL) {
            return res.status(500).json({ message: 'MAILERSEND_FROM_EMAIL não configurado' });
        }
        
        const verificationUrl = `${process.env.FRONTEND_URL || 'http://localhost:3000'}/verify-email?code=${verificationCode}&email=${encodeURIComponent(normalizedEmail)}`;
        
        const sentFrom = new Sender(process.env.MAILERSEND_FROM_EMAIL, 'HotTrack');
        const recipients = [new Recipient(normalizedEmail, seller.name)];
        
        const emailParams = new EmailParams()
            .setFrom(sentFrom)
            .setTo(recipients)
            .setReplyTo(sentFrom)
            .setSubject('Verifique seu email - HotTrack')
            .setHtml(`
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #0ea5e9;">Verificação de Email - HotTrack</h2>
                    <p>Olá ${seller.name},</p>
                    <p>Você solicitou um novo código de verificação. Clique no botão abaixo para ativar sua conta:</p>
                    <div style="text-align: center; margin: 30px 0;">
                        <a href="${verificationUrl}" style="background-color: #0ea5e9; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; display: inline-block;">Verificar Email</a>
                    </div>
                    <p>Ou copie e cole este link no seu navegador:</p>
                    <p style="word-break: break-all; color: #666;">${verificationUrl}</p>
                    <p>Este link expira em 24 horas.</p>
                </div>
            `)
            .setText(`
                Verificação de Email - HotTrack
                
                Olá ${seller.name},
                
                Você solicitou um novo código de verificação. Para ativar sua conta, acesse o link abaixo:
                ${verificationUrl}
                
                Este link expira em 24 horas.
            `);

        await mailerSend.email.send(emailParams);
        
        res.json({ message: 'Email de verificação reenviado com sucesso!' });
        
    } catch (error) {
        console.error('Erro ao reenviar verificação:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.post('/api/sellers/login', async (req, res) => {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ message: 'Email e senha são obrigatórios.' });
    try {
        const normalizedEmail = email.trim().toLowerCase();
        const sellerResult = await sqlTx`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        if (sellerResult.length === 0) {
             console.warn(`[LOGIN FAILURE] Usuário não encontrado no banco de dados para o email: "${normalizedEmail}"`);
            return res.status(404).json({ message: 'Usuário não encontrado.' });
        }
        
        const seller = sellerResult[0];
        
        // Verificar se email foi verificado
        if (!seller.email_verified) {
            return res.status(403).json({ 
                message: 'Email não verificado. Verifique sua caixa de entrada.',
                requiresVerification: true,
                email: seller.email
            });
        }
        
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }
        
        const isPasswordCorrect = await bcrypt.compare(password, seller.password_hash);
        if (!isPasswordCorrect) return res.status(401).json({ message: 'Senha incorreta.' });
        
        const authTokens = await issueAuthTokensForSeller(seller, req);
        
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({
            message: 'Login bem-sucedido!',
            token: authTokens.accessToken,
            accessToken: authTokens.accessToken,
            refreshToken: authTokens.refreshToken,
            accessTokenExpiresIn: authTokens.accessTokenExpiresIn,
            refreshTokenExpiresIn: authTokens.refreshTokenExpiresIn,
            seller: sellerData
        });

    } catch (error) {
        console.error("ERRO DETALHADO NO LOGIN:", error); 
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para obter URL de autorização do Google
app.get('/api/auth/google/url', (req, res) => {
    try {
        const authUrl = googleClient.generateAuthUrl({
            access_type: 'offline',
            scope: ['profile', 'email']
        });
        
        res.json({ authUrl });
    } catch (error) {
        console.error('Erro ao gerar URL do Google:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

// Endpoint para callback do Google OAuth
app.post('/api/auth/google/callback', async (req, res) => {
    try {
        const { code } = req.body;
        
        if (!code) {
            return res.status(400).json({ message: 'Código de autorização é obrigatório.' });
        }

        // Trocar código por token
        const { tokens } = await googleClient.getToken(code);
        googleClient.setCredentials(tokens);

        // Obter informações do usuário
        const ticket = await googleClient.verifyIdToken({
            idToken: tokens.id_token,
            audience: process.env.GOOGLE_CLIENT_ID
        });

        const payload = ticket.getPayload();
        const { sub: googleId, email, name, picture } = payload;

        if (!email) {
            return res.status(400).json({ message: 'Email não fornecido pelo Google.' });
        }

        const normalizedEmail = email.toLowerCase();

        // Verificar se usuário já existe
        let sellerResult = await sqlTx`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        
        if (sellerResult.length === 0) {
            // Adicionar campos OAuth se não existirem
            
            // Criar novo usuário
            const apiKey = uuidv4();
            
            await sqlTx`INSERT INTO sellers (
                name, email, api_key, is_active, 
                google_id, google_email, google_name, google_picture
            ) VALUES (
                ${name}, ${normalizedEmail}, ${apiKey}, TRUE,
                ${googleId}, ${email}, ${name}, ${picture}
            )`;
            
            sellerResult = await sqlTx`SELECT * FROM sellers WHERE email = ${normalizedEmail}`;
        } else {
            // Atualizar dados do Google se necessário
            await sqlTx`UPDATE sellers SET 
                google_id = ${googleId},
                google_email = ${email},
                google_name = ${name},
                google_picture = ${picture}
                WHERE email = ${normalizedEmail}`;
        }

        const seller = sellerResult[0];
        
        if (!seller.is_active) {
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }

        const authTokens = await issueAuthTokensForSeller(seller, req);
        
        const { password_hash, ...sellerData } = seller;
        res.status(200).json({
            message: 'Login com Google bem-sucedido!',
            token: authTokens.accessToken,
            accessToken: authTokens.accessToken,
            refreshToken: authTokens.refreshToken,
            accessTokenExpiresIn: authTokens.accessTokenExpiresIn,
            refreshTokenExpiresIn: authTokens.refreshTokenExpiresIn,
            seller: sellerData
        });

    } catch (error) {
        console.error('Erro no callback do Google:', error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.post('/api/auth/refresh', async (req, res) => {
    const refreshToken = req.body?.refreshToken;

    if (!refreshToken) {
        return res.status(400).json({ message: 'Refresh token é obrigatório.', code: 'REFRESH_TOKEN_REQUIRED' });
    }

    const tokenHash = hashToken(refreshToken);
    let decoded;

    try {
        decoded = jwt.verify(refreshToken, JWT_REFRESH_SECRET);
    } catch (error) {
        console.warn('[Auth] Falha ao verificar refresh token:', error.message);
        await revokeRefreshTokenByHash(tokenHash);

        if (error.name === 'TokenExpiredError') {
            return res.status(401).json({ message: 'Refresh token expirado.', code: 'REFRESH_TOKEN_EXPIRED' });
        }

        return res.status(401).json({ message: 'Refresh token inválido.', code: 'REFRESH_TOKEN_INVALID' });
    }

    try {
        const storedToken = await findRefreshTokenByHash(tokenHash);
        if (!storedToken) {
            return res.status(401).json({ message: 'Refresh token não reconhecido.', code: 'REFRESH_TOKEN_NOT_FOUND' });
        }

        if (decoded.tokenId && decoded.tokenId !== storedToken.token_id) {
            await revokeRefreshTokenByHash(tokenHash);
            return res.status(401).json({ message: 'Refresh token inválido.', code: 'REFRESH_TOKEN_MISMATCH' });
        }

        if (decoded.sub && Number.parseInt(decoded.sub, 10) !== storedToken.seller_id) {
            await revokeRefreshTokenByHash(tokenHash);
            return res.status(401).json({ message: 'Refresh token inválido.', code: 'REFRESH_TOKEN_SUBJECT_MISMATCH' });
        }

        if (new Date(storedToken.expires_at).getTime() <= Date.now()) {
            await revokeRefreshTokenByHash(tokenHash);
            return res.status(401).json({ message: 'Refresh token expirado.', code: 'REFRESH_TOKEN_EXPIRED' });
        }

        await sqlTx`
            UPDATE seller_refresh_tokens
            SET last_used_at = now()
            WHERE token_hash = ${tokenHash}
        `;

        const sellerResult = await sqlTx`
            SELECT * FROM sellers WHERE id = ${storedToken.seller_id} LIMIT 1
        `;

        if (sellerResult.length === 0) {
            await revokeRefreshTokenByHash(tokenHash);
            return res.status(403).json({ message: 'Usuário não encontrado.' });
        }

        const seller = sellerResult[0];

        if (!seller.is_active) {
            await revokeRefreshTokenByHash(tokenHash);
            return res.status(403).json({ message: 'Este usuário está bloqueado.' });
        }

        await revokeRefreshTokenByHash(tokenHash);

        const authTokens = await issueAuthTokensForSeller(seller, req);

        return res.status(200).json({
            message: 'Tokens renovados com sucesso.',
            token: authTokens.accessToken,
            accessToken: authTokens.accessToken,
            refreshToken: authTokens.refreshToken,
            accessTokenExpiresIn: authTokens.accessTokenExpiresIn,
            refreshTokenExpiresIn: authTokens.refreshTokenExpiresIn,
        });
    } catch (error) {
        console.error('[Auth] Erro ao renovar tokens:', error);
        return res.status(500).json({ message: 'Erro ao renovar tokens.' });
    }
});

app.post('/api/auth/logout', async (req, res) => {
    try {
        const refreshToken = req.body?.refreshToken;
        if (refreshToken) {
            const tokenHash = hashToken(refreshToken);
            await revokeRefreshTokenByHash(tokenHash);
        }
        res.status(200).json({ message: 'Logout realizado com sucesso.' });
    } catch (error) {
        console.error('[Auth] Erro ao realizar logout:', error);
        res.status(500).json({ message: 'Erro ao realizar logout.' });
    }
});

app.get('/api/dashboard/data', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const settingsPromise = sqlTx`SELECT api_key, pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key, wiinpay_api_key, pixup_client_id, pixup_client_secret, syncpay_client_id, syncpay_client_secret, brpix_secret_key, brpix_company_id, paradise_secret_key, paradise_product_hash, pix_provider_primary, pix_provider_secondary, pix_provider_tertiary, commission_rate, netlify_access_token, netlify_site_id FROM sellers WHERE id = ${sellerId}`;
        const pixelsPromise = sqlTx`SELECT * FROM pixel_configurations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const presselsPromise = sqlTx`
            SELECT p.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids, b.bot_name
            FROM pressels p
            LEFT JOIN ( SELECT pressel_id, array_agg(pixel_config_id) as pixel_ids FROM pressel_pixels GROUP BY pressel_id ) px ON p.id = px.pressel_id
            JOIN telegram_bots b ON p.bot_id = b.id
            WHERE p.seller_id = ${sellerId} ORDER BY p.created_at DESC`;
        const botsPromise = sqlTx`SELECT * FROM telegram_bots WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const checkoutsPromise = sqlTx`
            SELECT c.*, COALESCE(px.pixel_ids, ARRAY[]::integer[]) as pixel_ids
            FROM checkouts c
            LEFT JOIN ( SELECT checkout_id, array_agg(pixel_config_id) as pixel_ids FROM checkout_pixels GROUP BY checkout_id ) px ON c.id = px.checkout_id
            WHERE c.seller_id = ${sellerId} ORDER BY c.created_at DESC`;
        const utmifyIntegrationsPromise = sqlTx`SELECT id, account_name FROM utmify_integrations WHERE seller_id = ${sellerId} ORDER BY created_at DESC`;
        const thankYouPagesPromise = sqlTx`
            SELECT id, config->>'page_name' as name
            FROM thank_you_pages
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC`;
        const hostedCheckoutsPromise = sqlTx`
            SELECT id, config->'content'->>'main_title' as name
            FROM hosted_checkouts
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC`;

        const [settingsResult, pixels, pressels, bots, checkouts, utmifyIntegrations, thankYouPages, hostedCheckouts] = await Promise.all([
            settingsPromise, pixelsPromise, presselsPromise, botsPromise, checkoutsPromise, utmifyIntegrationsPromise, thankYouPagesPromise, hostedCheckoutsPromise
        ]);
        
        const settings = settingsResult[0] || {};
        res.json({ settings, pixels, pressels, bots, checkouts, utmifyIntegrations, thankYouPages, hostedCheckouts });
    } catch (error) {
        console.error("Erro ao buscar dados do dashboard:", error);
        res.status(500).json({ message: 'Erro ao buscar dados.' });
    }
});
app.get('/api/dashboard/achievements-and-ranking', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        
        const userAchievements = await sqlTx`
            SELECT a.title, a.description, ua.is_completed, a.sales_goal
            FROM achievements a
            JOIN user_achievements ua ON a.id = ua.achievement_id
            WHERE ua.seller_id = ${sellerId}
            ORDER BY a.sales_goal ASC;
        `;

        const topSellersRanking = await sqlTx`
            SELECT s.name, COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            GROUP BY s.id, s.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        `;
        
        const [userRevenue] = await sqlTx`
            SELECT COALESCE(SUM(pt.pix_value), 0) AS total_revenue
            FROM sellers s
            LEFT JOIN clicks c ON s.id = c.seller_id
            LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
            WHERE s.id = ${sellerId}
            GROUP BY s.id;
        `;

        const userRankResult = await sqlTx`
            SELECT COUNT(T1.id) + 1 AS rank
            FROM (
                SELECT s.id
                FROM sellers s
                LEFT JOIN clicks c ON s.id = c.seller_id
                LEFT JOIN pix_transactions pt ON c.id = pt.click_id_internal AND pt.status = 'paid'
                GROUP BY s.id
                HAVING COALESCE(SUM(pt.pix_value), 0) > ${userRevenue.total_revenue}
            ) AS T1;
        `;
        
        const userRank = userRankResult[0].rank;

        res.json({
            userAchievements,
            topSellersRanking,
            currentUserRank: userRank
        });
    } catch (error) {
        console.error("Erro ao buscar conquistas e ranking:", error);
        res.status(500).json({ message: 'Erro ao buscar dados de ranking.' });
    }
});
app.post('/api/pixels', authenticateJwt, async (req, res) => {
    const { account_name, pixel_id, meta_api_token } = req.body;
    if (!account_name || !pixel_id || !meta_api_token) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    try {
        const newPixel = await sqlTx`INSERT INTO pixel_configurations (seller_id, account_name, pixel_id, meta_api_token) VALUES (${req.user.id}, ${account_name}, ${pixel_id}, ${meta_api_token}) RETURNING *;`;
        res.status(201).json(newPixel[0]);
    } catch (error) {
        if (error.code === '23505') { return res.status(409).json({ message: 'Este ID de Pixel já foi cadastrado.' }); }
        console.error("Erro ao salvar pixel:", error);
        res.status(500).json({ message: 'Erro ao salvar o pixel.' });
    }
});
app.delete('/api/pixels/:id', authenticateJwt, async (req, res) => {
    try {
        await sqlTx`DELETE FROM pixel_configurations WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pixel:", error);
        res.status(500).json({ message: 'Erro ao excluir o pixel.' });
    }
});

app.post('/api/bots', authenticateJwt, async (req, res) => {
    const { bot_name, telegram_supergroup_id } = req.body;
    if (!bot_name) {
        return res.status(400).json({ message: 'O nome do bot é obrigatório.' });
    }
    try {
        const placeholderToken = uuidv4();

        const [newBot] = await sqlTx`
            INSERT INTO telegram_bots (seller_id, bot_name, bot_token, telegram_supergroup_id) 
            VALUES (${req.user.id}, ${bot_name}, ${placeholderToken}, ${telegram_supergroup_id || null}) 
            RETURNING *;
        `;
        res.status(201).json(newBot);
    } catch (error) {
        if (error.code === '23505' && error.constraint_name === 'telegram_bots_bot_name_key') {
            return res.status(409).json({ message: 'Um bot com este nome de usuário já existe.' });
        }
        console.error("Erro ao salvar bot:", error);
        res.status(500).json({ message: 'Erro ao salvar o bot.' });
    }
});

app.delete('/api/bots/:id', authenticateJwt, async (req, res) => {
    try {
        await sqlTx`DELETE FROM telegram_bots WHERE id = ${req.params.id} AND seller_id = ${req.user.id}`;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir bot:", error);
        res.status(500).json({ message: 'Erro ao excluir o bot.' });
    }
});

app.put('/api/bots/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    let { bot_token, telegram_supergroup_id } = req.body;
    if (!bot_token) {
        return res.status(400).json({ message: 'O token do bot é obrigatório.' });
    }
    bot_token = bot_token.trim();
    try {
        await sqlTx`
            UPDATE telegram_bots 
            SET bot_token = ${bot_token},
                telegram_supergroup_id = ${telegram_supergroup_id || null}
            WHERE id = ${id} AND seller_id = ${req.user.id}`;
        res.status(200).json({ message: 'Bot atualizado com sucesso.' });
    } catch (error) {
        console.error("Erro ao atualizar token do bot:", error);
        res.status(500).json({ message: 'Erro ao atualizar o token do bot.' });
    }
});

app.post('/api/bots/:id/set-webhook', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    try {
        const [bot] = await sqlTx`
            SELECT bot_token FROM telegram_bots 
            WHERE id = ${id} AND seller_id = ${sellerId}`;

        if (!bot || !bot.bot_token || bot.bot_token.trim() === '') {
            return res.status(400).json({ message: 'O token do bot não está configurado. Salve um token válido primeiro.' });
        }
        const token = bot.bot_token.trim();
        const webhookUrl = `${HOTTRACK_API_URL}/api/webhook/telegram/${id}`;
        const telegramApiUrl = `https://api.telegram.org/bot${token}/setWebhook?url=${webhookUrl}`;
        
        const response = await axios.get(telegramApiUrl);

        if (response.data.ok) {
            res.status(200).json({ message: 'Webhook configurado com sucesso!' });
        } else {
            throw new Error(response.data.description);
        }
    } catch (error) {
        console.error("Erro ao configurar webhook:", error);
        if (error.isAxiosError && error.response) {
            const status = error.response.status;
            const telegramMessage = error.response.data?.description || 'Resposta inválida do Telegram.';
            if (status === 401 || status === 404) {
                return res.status(400).json({ message: `O Telegram rejeitou seu token: "${telegramMessage}". Verifique se o token está correto.` });
            }
            return res.status(500).json({ message: `Erro de comunicação com o Telegram: ${telegramMessage}` });
        }
        res.status(500).json({ message: `Erro interno no servidor: ${error.message}` });
    }
});

app.post('/api/bots/test-connection', authenticateJwt, async (req, res) => {
    const { bot_id } = req.body;
    if (!bot_id) return res.status(400).json({ message: 'ID do bot é obrigatório.' });

    try {
        const [bot] = await sqlTx`SELECT bot_token, bot_name FROM telegram_bots WHERE id = ${bot_id} AND seller_id = ${req.user.id}`;
        if (!bot) {
            return res.status(404).json({ message: 'Bot não encontrado ou não pertence a este usuário.' });
        }
        if (!bot.bot_token) {
            return res.status(400).json({ message: 'Token do bot não configurado. Impossível testar.'})
        }

        const response = await axios.get(`https://api.telegram.org/bot${bot.bot_token}/getMe`);
        
        if (response.data.ok) {
            res.status(200).json({ 
                message: `Conexão com o bot @${response.data.result.username} bem-sucedida!`,
                bot_info: response.data.result
            });
        } else {
            throw new Error('A API do Telegram retornou um erro.');
        }

    } catch (error) {
        console.error(`[BOT TEST ERROR] Bot ID: ${bot_id} - Erro:`, error.response?.data || error.message);
        let errorMessage = 'Falha ao conectar com o bot. Verifique o token e tente novamente.';
        if (error.response?.status === 401) {
            errorMessage = 'Token inválido. Verifique se o token do bot foi copiado corretamente do BotFather.';
        } else if (error.response?.status === 404) {
            errorMessage = 'Bot não encontrado. O token pode estar incorreto ou o bot foi deletado.';
        }
        res.status(500).json({ message: errorMessage });
    }
});

app.get('/api/bots/users', authenticateJwt, async (req, res) => {
    const { botIds } = req.query; 

    if (!botIds) {
        return res.status(400).json({ message: 'IDs dos bots são obrigatórios.' });
    }
    const botIdArray = botIds.split(',').map(id => parseInt(id.trim(), 10));

    try {
        const users = await sqlTx`
            SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username 
            FROM telegram_chats 
            WHERE bot_id = ANY(${botIdArray}) AND seller_id = ${req.user.id};
        `;
        res.status(200).json({ total_users: users.length });
    } catch (error) {
        console.error("Erro ao buscar contagem de usuários do bot:", error);
        res.status(500).json({ message: 'Erro interno ao buscar usuários.' });
    }
});
app.post('/api/pressels', authenticateJwt, async (req, res) => {
    const { name, bot_id, white_page_url, pixel_ids, utmify_integration_id, traffic_type, deploy_to_netlify, netlify_site_name } = req.body;
    
    if (!name || !bot_id || !white_page_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) return res.status(400).json({ message: 'Todos os campos são obrigatórios.' });
    
    // Validação do nome do site Netlify
    if (deploy_to_netlify && netlify_site_name) {
        const siteName = netlify_site_name.trim();
        
        // Verificar se tem caracteres válidos (apenas alfanuméricos e hífens)
        const validNameRegex = /^[a-zA-Z0-9-]+$/;
        if (!validNameRegex.test(siteName)) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify deve conter apenas letras, números e hífens.' 
            });
        }
        
        // Verificar limite de 37 caracteres
        if (siteName.length > 37) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify deve ter no máximo 37 caracteres.' 
            });
        }
        
        // Verificar se não começa ou termina com hífen
        if (siteName.startsWith('-') || siteName.endsWith('-')) {
            return res.status(400).json({ 
                message: 'Nome do site Netlify não pode começar ou terminar com hífen.' 
            });
        }
    }
    
    try {
        const numeric_bot_id = parseInt(bot_id, 10);
        const numeric_pixel_ids = pixel_ids.map(id => parseInt(id, 10));

        const botResult = await sqlTx`SELECT bot_name FROM telegram_bots WHERE id = ${numeric_bot_id} AND seller_id = ${req.user.id}`;
        if (botResult.length === 0) {
            return res.status(404).json({ message: 'Bot não encontrado.' });
        }
        const bot_name = botResult[0].bot_name;

        const result = await sqlTx.begin(async sql => {
            const [newPressel] = await sql`
                INSERT INTO pressels (seller_id, name, bot_id, bot_name, white_page_url, utmify_integration_id, traffic_type, netlify_url) 
                VALUES (${req.user.id}, ${name}, ${numeric_bot_id}, ${bot_name}, ${white_page_url}, ${utmify_integration_id || null}, ${traffic_type || 'both'}, NULL) 
                RETURNING *;
            `;
            
            for (const pixelId of numeric_pixel_ids) {
                await sql`INSERT INTO pressel_pixels (pressel_id, pixel_config_id) VALUES (${newPressel.id}, ${pixelId})`;
            }
            
            let netlifyUrl = null;
            
            // Deploy opcional para Netlify
            if (deploy_to_netlify) {
                try {
                    // Buscar configurações do Netlify
                    const [seller] = await sql`SELECT netlify_access_token, netlify_site_id FROM sellers WHERE id = ${req.user.id}`;
                    
                    if (seller?.netlify_access_token) {
                        // Gerar HTML da pressel
                        const htmlContent = await generatePresselHTML(newPressel, numeric_pixel_ids);

                        if (deploy_to_netlify) {
                            // Sempre criar site exclusivo por pressel
                            const siteName = (netlify_site_name && netlify_site_name.trim()
                                ? netlify_site_name.trim()
                                : `pressel-${newPressel.id}-${Date.now()}`)
                                .toLowerCase()
                                .replace(/[^a-z0-9-]/g, '-');

                            const siteResult = await createNetlifySite(seller.netlify_access_token, siteName);

                            if (siteResult.success) {
                                // Deploy no novo site (não tocar em sellers.netlify_site_id)
                                const deployResult = await deployToNetlify(seller.netlify_access_token, siteResult.site.id, htmlContent, 'index.html');

                                if (deployResult.success) {
                                    netlifyUrl = deployResult.url;

                                    // Atualizar campo netlify_url na tabela pressels
                                    await sql`UPDATE pressels SET netlify_url = ${netlifyUrl} WHERE id = ${newPressel.id}`;

                                    // Adicionar domínio automaticamente
                                    const domain = deployResult.url.replace('https://', '');
                                    await sql`INSERT INTO pressel_allowed_domains (pressel_id, domain) VALUES (${newPressel.id}, ${domain})`;

                                    console.log(`[Netlify] Site exclusivo criado e pressel ${newPressel.id} deployada: ${netlifyUrl}`);
                                }
                            } else {
                                console.error(`[Netlify] Erro ao criar site exclusivo para pressel ${newPressel.id}:`, siteResult.error);
                            }
                        }
                    } else {
                        console.warn(`[Netlify] Token Netlify não configurado para vendedor ${req.user.id}`);
                    }
                } catch (netlifyError) {
                    console.error(`[Netlify] Erro no deploy da pressel ${newPressel.id}:`, netlifyError);
                    // Não falha a criação da pressel se o deploy falhar
                }
            }
            
            // Se o deploy via Netlify foi solicitado, a URL é obrigatória
            if (deploy_to_netlify && !netlifyUrl) {
                throw new Error('Falha no deploy Netlify: URL não gerada.');
            }

            return { newPressel, netlifyUrl };
        });

        res.status(201).json({ 
            ...result.newPressel, 
            pixel_ids: numeric_pixel_ids, 
            bot_name,
            netlify_url: result.netlifyUrl
        });
    } catch (error) {
        console.error("Erro ao salvar pressel:", error);
        res.status(500).json({ message: 'Erro ao salvar a pressel.' });
    }
});
app.delete('/api/pressels/:id', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        
        // Buscar informações da pressel antes de excluir
        const [pressel] = await sqlTx`
            SELECT id, seller_id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Buscar configurações do Netlify do seller
        const [seller] = await sqlTx`
            SELECT netlify_access_token, netlify_site_id 
            FROM sellers 
            WHERE id = ${req.user.id}
        `;
        
        // Excluir site do Netlify se existir
        if (seller?.netlify_access_token && seller?.netlify_site_id) {
            try {
                const deleteResult = await deleteNetlifySite(seller.netlify_access_token, seller.netlify_site_id);
                if (deleteResult.success) {
                    console.log(`[Netlify] Site ${seller.netlify_site_id} excluído com sucesso`);
                } else {
                    console.warn(`[Netlify] Site não encontrado ou já excluído:`, deleteResult.error);
                }
                
                // Sempre limpar netlify_site_id do seller (mesmo se o site não existir)
                await sqlTx`UPDATE sellers SET netlify_site_id = NULL WHERE id = ${req.user.id}`;
            } catch (netlifyError) {
                console.warn(`[Netlify] Erro ao excluir site da pressel ${presselId}:`, netlifyError);
                // Limpar netlify_site_id mesmo se houver erro
                await sqlTx`UPDATE sellers SET netlify_site_id = NULL WHERE id = ${req.user.id}`;
            }
        }
        
        // Excluir a pressel
        await sqlTx`DELETE FROM pressels WHERE id = ${presselId} AND seller_id = ${req.user.id}`;
        
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir pressel:", error);
        res.status(500).json({ message: 'Erro ao excluir a pressel.' });
    }
});

// ==========================================================
//          ROTAS PARA GERENCIAR DOMÍNIOS PERMITIDOS
// ==========================================================

// Buscar domínios permitidos para uma pressel
app.get('/api/pressels/:id/domains', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sqlTx`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        const domains = await sqlTx`
            SELECT id, domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        res.json(domains);
    } catch (error) {
        console.error("Erro ao buscar domínios permitidos:", error);
        res.status(500).json({ message: 'Erro ao buscar domínios.' });
    }
});

// Adicionar domínio permitido para uma pressel
app.post('/api/pressels/:id/domains', authenticateJwt, async (req, res) => {
    try {
        const presselId = req.params.id;
        const { domain } = req.body;
        
        if (!domain || typeof domain !== 'string' || domain.trim().length === 0) {
            return res.status(400).json({ message: 'Domínio é obrigatório.' });
        }
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sqlTx`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Normalizar domínio (remover protocolo se presente)
        const normalizedDomain = domain.trim().replace(/^https?:\/\//, '');
        
        // Verificar se já existe
        const [existing] = await sqlTx`
            SELECT id FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} AND domain = ${normalizedDomain}
        `;
        
        if (existing) {
            return res.status(400).json({ message: 'Domínio já está cadastrado para esta pressel.' });
        }
        
        // Inserir domínio
        const [newDomain] = await sqlTx`
            INSERT INTO pressel_allowed_domains (pressel_id, domain) 
            VALUES (${presselId}, ${normalizedDomain}) 
            RETURNING id, domain, created_at
        `;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(201).json(newDomain);
    } catch (error) {
        console.error("Erro ao adicionar domínio:", error);
        res.status(500).json({ message: 'Erro ao adicionar domínio.' });
    }
});

// Remover domínio permitido
app.delete('/api/pressels/:presselId/domains/:domainId', authenticateJwt, async (req, res) => {
    try {
        const { presselId, domainId } = req.params;
        
        // Verificar se a pressel pertence ao seller
        const [pressel] = await sqlTx`
            SELECT id FROM pressels 
            WHERE id = ${presselId} AND seller_id = ${req.user.id}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Verificar se o domínio pertence à pressel
        const [domain] = await sqlTx`
            SELECT id FROM pressel_allowed_domains 
            WHERE id = ${domainId} AND pressel_id = ${presselId}
        `;
        
        if (!domain) {
            return res.status(404).json({ message: 'Domínio não encontrado.' });
        }
        
        // Remover domínio
        await sqlTx`DELETE FROM pressel_allowed_domains WHERE id = ${domainId}`;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao remover domínio:", error);
        res.status(500).json({ message: 'Erro ao remover domínio.' });
    }
});

// Micro painel público para publishers registrarem domínios
app.get('/api/pressel-domains/:presselId', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        
        // Verificar se a pressel existe
        const [pressel] = await sqlTx`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        const domains = await sqlTx`
            SELECT domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        res.json({
            pressel: { id: pressel.id, name: pressel.name },
            domains
        });
    } catch (error) {
        console.error("Erro ao buscar domínios da pressel:", error);
        res.status(500).json({ message: 'Erro ao buscar domínios.' });
    }
});

// Registrar domínio via micro painel (sem autenticação)
app.post('/api/pressel-domains/:presselId/register', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        const { domain, verification_code } = req.body;
        
        if (!domain || typeof domain !== 'string' || domain.trim().length === 0) {
            return res.status(400).json({ message: 'Domínio é obrigatório.' });
        }
        
        // Verificar se a pressel existe
        const [pressel] = await sqlTx`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).json({ message: 'Pressel não encontrada.' });
        }
        
        // Normalizar domínio
        const normalizedDomain = domain.trim().replace(/^https?:\/\//, '');
        
        // Verificar se já existe
        const [existing] = await sqlTx`
            SELECT id FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} AND domain = ${normalizedDomain}
        `;
        
        if (existing) {
            return res.status(400).json({ message: 'Domínio já está cadastrado para esta pressel.' });
        }
        
        // TODO: Implementar verificação de domínio (DNS, arquivo de verificação, etc.)
        // Por enquanto, aceitar automaticamente
        
        // Inserir domínio
        const [newDomain] = await sqlTx`
            INSERT INTO pressel_allowed_domains (pressel_id, domain) 
            VALUES (${presselId}, ${normalizedDomain}) 
            RETURNING id, domain, created_at
        `;
        
        // Limpar cache
        allowedDomainsCache.clear();
        
        res.status(201).json({
            message: 'Domínio registrado com sucesso!',
            domain: newDomain
        });
    } catch (error) {
        console.error("Erro ao registrar domínio:", error);
        res.status(500).json({ message: 'Erro ao registrar domínio.' });
    }
});

// Micro painel público para publishers registrarem domínios
app.get('/pressel-domains/:presselId', async (req, res) => {
    try {
        const presselId = req.params.presselId;
        
        // Verificar se a pressel existe
        const [pressel] = await sqlTx`
            SELECT id, name FROM pressels WHERE id = ${presselId}
        `;
        
        if (!pressel) {
            return res.status(404).send(`
                <!DOCTYPE html>
                <html lang="pt-BR">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Pressel não encontrada</title>
                    <style>
                        body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f0f2f5; }
                        .container { max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                        h1 { color: #e74c3c; }
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>❌ Pressel não encontrada</h1>
                        <p>Esta pressel não existe ou foi removida.</p>
                    </div>
                </body>
                </html>
            `);
        }
        
        const domains = await sqlTx`
            SELECT domain, created_at 
            FROM pressel_allowed_domains 
            WHERE pressel_id = ${presselId} 
            ORDER BY created_at DESC
        `;
        
        const html = `
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gerenciador de Domínios - ${pressel.name}</title>
    <style>
        body { 
            margin: 0; 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, Cantarell, "Open Sans", "Helvetica Neue", sans-serif; 
            background-color: #f0f2f5; 
            color: #1c1e21; 
            padding: 20px;
        }
        .container { 
            max-width: 800px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 8px; 
            padding: 30px; 
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 1px solid #e1e5e9;
        }
        .header h1 {
            color: #1877f2;
            margin: 0 0 10px 0;
        }
        .header p {
            color: #606770;
            margin: 0;
        }
        .form-group {
            margin-bottom: 20px;
        }
        .form-group label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #1c1e21;
        }
        .form-group input {
            width: 100%;
            padding: 12px;
            border: 1px solid #dadde1;
            border-radius: 6px;
            font-size: 16px;
            box-sizing: border-box;
        }
        .form-group input:focus {
            outline: none;
            border-color: #1877f2;
            box-shadow: 0 0 0 2px rgba(24, 119, 242, 0.2);
        }
        .btn {
            background: #1877f2;
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: background-color 0.2s;
        }
        .btn:hover {
            background: #166fe5;
        }
        .btn:disabled {
            background: #dadde1;
            cursor: not-allowed;
        }
        .domains-list {
            margin-top: 30px;
        }
        .domains-list h3 {
            margin-bottom: 15px;
            color: #1c1e21;
        }
        .domain-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 12px;
            background: #f8f9fa;
            border-radius: 6px;
            margin-bottom: 8px;
        }
        .domain-name {
            font-family: monospace;
            color: #1877f2;
            font-weight: 600;
        }
        .domain-date {
            color: #606770;
            font-size: 14px;
        }
        .alert {
            padding: 12px;
            border-radius: 6px;
            margin-bottom: 20px;
        }
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        .alert-error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        .loading {
            display: none;
            text-align: center;
            padding: 20px;
        }
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #1877f2;
            border-radius: 50%;
            width: 20px;
            height: 20px;
            animation: spin 1s linear infinite;
            margin: 0 auto 10px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🌐 Gerenciador de Domínios</h1>
            <p>Registre os domínios que podem usar a pressel: <strong>${pressel.name}</strong></p>
        </div>

        <div id="alert-container"></div>

        <form id="domain-form">
            <div class="form-group">
                <label for="domain">Domínio:</label>
                <input 
                    type="text" 
                    id="domain" 
                    name="domain" 
                    placeholder="exemplo.com ou https://exemplo.com"
                    required
                />
            </div>
            <button type="submit" class="btn" id="submit-btn">
                Adicionar Domínio
            </button>
        </form>

        <div class="loading" id="loading">
            <div class="spinner"></div>
            <p>Carregando domínios...</p>
        </div>

        <div class="domains-list" id="domains-list" style="display: none;">
            <h3>Domínios Registrados:</h3>
            <div id="domains-container"></div>
        </div>
    </div>

    <script>
        const API_BASE_URL = '${process.env.API_BASE_URL}';
        const PRESSEL_ID = ${presselId};

        // Função para mostrar alertas
        function showAlert(message, type = 'success') {
            const alertContainer = document.getElementById('alert-container');
            alertContainer.innerHTML = \`
                <div class="alert alert-\${type}">
                    \${message}
                </div>
            \`;
            
            // Remover alerta após 5 segundos
            setTimeout(() => {
                alertContainer.innerHTML = '';
            }, 5000);
        }

        // Função para carregar domínios
        async function loadDomains() {
            try {
                document.getElementById('loading').style.display = 'block';
                document.getElementById('domains-list').style.display = 'none';
                
                const response = await fetch(\`\${API_BASE_URL}/api/pressel-domains/\${PRESSEL_ID}\`);
                const data = await response.json();
                
                if (response.ok) {
                    displayDomains(data.domains);
                    document.getElementById('domains-list').style.display = 'block';
                } else {
                    showAlert('Erro ao carregar domínios: ' + data.message, 'error');
                }
            } catch (error) {
                console.error('Erro ao carregar domínios:', error);
                showAlert('Erro ao carregar domínios. Tente novamente.', 'error');
            } finally {
                document.getElementById('loading').style.display = 'none';
            }
        }

        // Função para exibir domínios
        function displayDomains(domains) {
            const container = document.getElementById('domains-container');
            
            if (domains.length === 0) {
                container.innerHTML = '<p style="color: #606770; text-align: center; padding: 20px;">Nenhum domínio registrado ainda.</p>';
                return;
            }
            
            container.innerHTML = domains.map(domain => \`
                <div class="domain-item">
                    <div>
                        <div class="domain-name">\${domain.domain}</div>
                        <div class="domain-date">Registrado em: \${new Date(domain.created_at).toLocaleString('pt-BR')}</div>
                    </div>
                </div>
            \`).join('');
        }

        // Função para adicionar domínio
        async function addDomain(domain) {
            try {
                const response = await fetch(\`\${API_BASE_URL}/api/pressel-domains/\${PRESSEL_ID}/register\`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ domain })
                });
                
                const data = await response.json();
                
                if (response.ok) {
                    showAlert('Domínio registrado com sucesso!', 'success');
                    document.getElementById('domain').value = '';
                    loadDomains(); // Recarregar lista
                } else {
                    showAlert('Erro: ' + data.message, 'error');
                }
            } catch (error) {
                console.error('Erro ao adicionar domínio:', error);
                showAlert('Erro ao adicionar domínio. Tente novamente.', 'error');
            }
        }

        // Event listeners
        document.getElementById('domain-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const domainInput = document.getElementById('domain');
            const submitBtn = document.getElementById('submit-btn');
            const domain = domainInput.value.trim();
            
            if (!domain) {
                showAlert('Por favor, insira um domínio válido.', 'error');
                return;
            }
            
            submitBtn.disabled = true;
            submitBtn.textContent = 'Adicionando...';
            
            try {
                await addDomain(domain);
            } finally {
                submitBtn.disabled = false;
                submitBtn.textContent = 'Adicionar Domínio';
            }
        });

        // Carregar domínios ao inicializar
        loadDomains();
    </script>
</body>
</html>
        `;
        
        res.send(html);
    } catch (error) {
        console.error("Erro ao servir micro painel:", error);
        res.status(500).send(`
            <!DOCTYPE html>
            <html lang="pt-BR">
            <head>
                <meta charset="UTF-8">
                <title>Erro</title>
                <style>
                    body { font-family: Arial, sans-serif; text-align: center; padding: 50px; background: #f0f2f5; }
                    .container { max-width: 500px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                    h1 { color: #e74c3c; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>❌ Erro interno</h1>
                    <p>Ocorreu um erro ao carregar o painel. Tente novamente mais tarde.</p>
                </div>
            </body>
            </html>
        `);
    }
});
app.post('/api/checkouts', authenticateJwt, async (req, res) => {
    const { name, product_name, redirect_url, value_type, fixed_value_cents, pixel_ids } = req.body;

    if (!name || !product_name || !redirect_url || !Array.isArray(pixel_ids) || pixel_ids.length === 0) {
        return res.status(400).json({ message: 'Nome, Nome do Produto, URL de Redirecionamento e ao menos um Pixel são obrigatórios.' });
    }
    if (value_type === 'fixed' && (!fixed_value_cents || fixed_value_cents <= 0)) {
        return res.status(400).json({ message: 'Para valor fixo, o valor em centavos deve ser maior que zero.' });
    }

    try {
        const newCheckout = await sqlTx.begin(async sql => {
            const [checkout] = await sql`
                INSERT INTO checkouts (seller_id, name, product_name, redirect_url, value_type, fixed_value_cents)
                VALUES (${req.user.id}, ${name}, ${product_name}, ${redirect_url}, ${value_type}, ${value_type === 'fixed' ? fixed_value_cents : null})
                RETURNING *;
            `;

            for (const pixelId of pixel_ids) {
                await sql`INSERT INTO checkout_pixels (checkout_id, pixel_config_id) VALUES (${checkout.id}, ${pixelId})`;
            }
            
            return checkout;
        });

        res.status(201).json({ ...newCheckout, pixel_ids: pixel_ids.map(id => parseInt(id)) });
    } catch (error) {
        console.error("Erro ao salvar checkout:", error);
        res.status(500).json({ message: 'Erro interno ao salvar o checkout.' });
    }
});
// EXCLUIR CHECKOUT
app.delete('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;

    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }

    try {
        // IMPORTANT: First, delete associated clicks to avoid foreign key constraint errors
        await sqlTx`DELETE FROM clicks WHERE checkout_id = ${checkoutId} AND seller_id = ${sellerId}`;

        // Now, delete the checkout itself
        const result = await sqlTx`
            DELETE FROM hosted_checkouts
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Return ID to confirm deletion
        `;

        if (result.length === 0) {
            console.warn(`Tentativa de excluir checkout não encontrado ou não pertencente ao seller: ${checkoutId}, Seller: ${sellerId}`);
            // Still return success as the end state (checkout doesn't exist) is achieved
        }

        res.status(200).json({ message: 'Checkout excluído com sucesso!' }); // Use 200 with message

    } catch (error) {
        console.error(`Erro ao excluir checkout ${checkoutId}:`, error);
        // Specifically handle foreign key violations if pix_transactions block deletion
        if (error.code === '23503') { // PostgreSQL foreign key violation error code
             console.error(`Erro de chave estrangeira ao excluir checkout ${checkoutId}. Pode haver transações PIX associadas.`);
             return res.status(409).json({ message: 'Não é possível excluir este checkout pois existem transações PIX associadas a ele através de cliques. Contacte o suporte se necessário.' });
        }
        res.status(500).json({ message: 'Erro interno ao excluir o checkout.' });
    }
});
app.post('/api/settings/pix', authenticateJwt, async (req, res) => {
    const { 
        pushinpay_token, cnpay_public_key, cnpay_secret_key, oasyfy_public_key, oasyfy_secret_key,
        wiinpay_api_key, pixup_client_id, pixup_client_secret,
        syncpay_client_id, syncpay_client_secret,
        brpix_secret_key, brpix_company_id,
        paradise_secret_key, paradise_product_hash,
        pix_provider_primary, pix_provider_secondary, pix_provider_tertiary
    } = req.body;
    try {
        await sqlTx`UPDATE sellers SET 
            pushinpay_token = ${pushinpay_token || null}, 
            cnpay_public_key = ${cnpay_public_key || null}, 
            cnpay_secret_key = ${cnpay_secret_key || null}, 
            oasyfy_public_key = ${oasyfy_public_key || null}, 
            oasyfy_secret_key = ${oasyfy_secret_key || null},
            wiinpay_api_key = ${wiinpay_api_key || null},
            pixup_client_id = ${pixup_client_id || null},
            pixup_client_secret = ${pixup_client_secret || null},
            syncpay_client_id = ${syncpay_client_id || null},
            syncpay_client_secret = ${syncpay_client_secret || null},
            brpix_secret_key = ${brpix_secret_key || null},
            brpix_company_id = ${brpix_company_id || null},
            paradise_secret_key = ${paradise_secret_key || null},
            paradise_product_hash = ${paradise_product_hash || null},
            pix_provider_primary = ${pix_provider_primary || 'pushinpay'},
            pix_provider_secondary = ${pix_provider_secondary || null},
            pix_provider_tertiary = ${pix_provider_tertiary || null}
            WHERE id = ${req.user.id}`;
        res.status(200).json({ message: 'Configurações de PIX salvas com sucesso.' });
    } catch (error) {
        console.error("Erro ao salvar configurações de PIX:", error);
        res.status(500).json({ message: 'Erro ao salvar as configurações.' });
    }
});

// ==========================================================
//          ROTAS DE INTEGRAÇÃO NETLIFY
// ==========================================================

app.post('/api/netlify/validate-token', authenticateJwt, async (req, res) => {
    const { access_token } = req.body;
    
    if (!access_token) {
        return res.status(400).json({ message: 'Token de acesso é obrigatório.' });
    }
    
    try {
        const result = await validateNetlifyToken(access_token);
        
        if (result.success) {
            // Salva o token se for válido
            await sqlTx`UPDATE sellers SET netlify_access_token = ${access_token} WHERE id = ${req.user.id}`;
            res.json({ 
                success: true, 
                message: 'Token válido! Configuração salva.',
                user: result.user
            });
        } else {
            res.status(400).json({ 
                success: false, 
                message: 'Token inválido: ' + result.error 
            });
        }
    } catch (error) {
        console.error("Erro ao validar token Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.get('/api/netlify/sites', authenticateJwt, async (req, res) => {
    try {
        const [seller] = await sqlTx`SELECT netlify_access_token FROM sellers WHERE id = ${req.user.id}`;
        
        if (!seller?.netlify_access_token) {
            return res.status(400).json({ message: 'Token Netlify não configurado.' });
        }
        
        const result = await getNetlifySites(seller.netlify_access_token);
        
        if (result.success) {
            res.json({ sites: result.sites });
        } else {
            res.status(400).json({ message: 'Erro ao buscar sites: ' + result.error });
        }
    } catch (error) {
        console.error("Erro ao buscar sites Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
app.post('/api/netlify/create-site', authenticateJwt, async (req, res) => {
    const { site_name } = req.body;
    
    if (!site_name) {
        return res.status(400).json({ message: 'Nome do site é obrigatório.' });
    }
    
    try {
        const [seller] = await sqlTx`SELECT netlify_access_token FROM sellers WHERE id = ${req.user.id}`;
        
        if (!seller?.netlify_access_token) {
            return res.status(400).json({ message: 'Token Netlify não configurado.' });
        }
        
        const result = await createNetlifySite(seller.netlify_access_token, site_name);
        
        if (result.success) {
            // Salva o site_id no banco
            await sqlTx`UPDATE sellers SET netlify_site_id = ${result.site.id} WHERE id = ${req.user.id}`;
            
            res.json({ 
                success: true, 
                site: result.site,
                url: result.url,
                message: 'Site criado com sucesso!'
            });
        } else {
            res.status(400).json({ 
                success: false, 
                message: 'Erro ao criar site: ' + result.error 
            });
        }
    } catch (error) {
        console.error("Erro ao criar site Netlify:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
app.get('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    try {
        const integrations = await sqlTx`
            SELECT id, account_name, created_at 
            FROM utmify_integrations 
            WHERE seller_id = ${req.user.id} 
            ORDER BY created_at DESC
        `;
        res.status(200).json(integrations);
    } catch (error) {
        console.error("Erro ao buscar integrações Utmify:", error);
        res.status(500).json({ message: 'Erro ao buscar integrações.' });
    }
});
app.post('/api/integrations/utmify', authenticateJwt, async (req, res) => {
    const { account_name, api_token } = req.body;
    if (!account_name || !api_token) {
        return res.status(400).json({ message: 'Nome da conta e token da API são obrigatórios.' });
    }
    try {
        const [newIntegration] = await sqlTx`
            INSERT INTO utmify_integrations (seller_id, account_name, api_token) 
            VALUES (${req.user.id}, ${account_name}, ${api_token}) 
            RETURNING id, account_name, created_at
        `;
        res.status(201).json(newIntegration);
    } catch (error) {
        console.error("Erro ao adicionar integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao salvar integração.' });
    }
});
app.delete('/api/integrations/utmify/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        await sqlTx`
            DELETE FROM utmify_integrations 
            WHERE id = ${id} AND seller_id = ${req.user.id}
        `;
        res.status(204).send();
    } catch (error) {
        console.error("Erro ao excluir integração Utmify:", error);
        res.status(500).json({ message: 'Erro ao excluir integração.' });
    }
});
// ROTA /api/registerClick CORRIGIDA PARA BUSCAR SELLER_ID PELO PRESSSEL_ID OU CHECKOUT_ID

app.post('/api/registerClick', rateLimitMiddleware, logApiRequest, async (req, res) => {
    // REMOVIDO: sellerApiKey
    const { presselId, checkoutId, referer, fbclid, fbp, fbc, user_agent, utm_source, utm_campaign, utm_medium, utm_content, utm_term } = req.body;

    // MODIFICADO: Validação inicial
    if (!presselId && !checkoutId) {
        // Agora verifica apenas se pelo menos um ID está presente
        return res.status(400).json({ message: 'É necessário fornecer presselId ou checkoutId.' });
    }

    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    // Validação de domínio para pressels (mantida)
    if (presselId) {
        const origin = req.headers.origin;
        if (origin) {
            const isDomainAllowed = await isDomainAllowedForPressel(presselId, origin);
            if (!isDomainAllowed) {
                console.log(`[SECURITY] Tentativa de acesso de domínio não autorizado: ${origin} para pressel ${presselId}`);
                return res.status(403).json({
                    message: 'Domínio não autorizado para esta pressel.',
                    hint: `Verifique o painel de domínios ou registre seu domínio.` // Mensagem genérica
                });
            }
        } else {
             console.warn(`[SECURITY] Não foi possível verificar a origem (cabeçalho Origin ausente) para pressel ${presselId}. Permitindo acesso.`);
             // Considerar se deve bloquear ou permitir requisições sem Origin. Permitir pode ser necessário para alguns cenários, mas menos seguro.
        }
    }

    try {
        let sellerId = null;

        // --- INÍCIO DA LÓGICA PARA ENCONTRAR seller_id ---
        if (presselId) {
            const [pressel] = await sqlTx`SELECT seller_id FROM pressels WHERE id = ${presselId}`;
            if (!pressel) {
                return res.status(404).json({ message: 'Pressel não encontrada.' });
            }
            sellerId = pressel.seller_id;
        } else if (checkoutId) {
            const [checkout] = await sqlTx`SELECT seller_id FROM hosted_checkouts WHERE id = ${checkoutId}`;
            if (!checkout) {
                return res.status(404).json({ message: 'Checkout não encontrado.' });
            }
            sellerId = checkout.seller_id;
        }

        // Se, por algum motivo, não encontramos o sellerId
        if (!sellerId) {
             console.error(`[registerClick] ERRO CRÍTICO: Não foi possível determinar o seller_id para presselId=${presselId} ou checkoutId=${checkoutId}`);
             return res.status(500).json({ message: 'Erro ao identificar o vendedor associado.' });
        }
        // --- FIM DA LÓGICA PARA ENCONTRAR seller_id ---

        // MODIFICADO: Query INSERT usa o sellerId encontrado
        const result = await sqlTx`INSERT INTO clicks (
            seller_id, pressel_id, checkout_id, ip_address, user_agent, referer, fbclid, fbp, fbc,
            utm_source, utm_campaign, utm_medium, utm_content, utm_term
        ) VALUES (
            ${sellerId}, ${presselId || null}, ${checkoutId || null}, ${ip_address}, ${user_agent}, ${referer}, ${fbclid}, ${fbp}, ${fbc},
            ${utm_source || null}, ${utm_campaign || null}, ${utm_medium || null}, ${utm_content || null}, ${utm_term || null}
        ) RETURNING *;`; // Não precisamos mais do JOIN com sellers aqui

        // O resto da lógica permanece o mesmo...
        const newClick = result[0];
        const click_record_id = newClick.id;
        // Gera um click_id único e amigável, mesmo que já exista um no banco (para consistência)
        const clean_click_id = `lead${click_record_id.toString().padStart(6, '0')}`;
        const db_click_id = `/start ${clean_click_id}`;

        await sqlTx`UPDATE clicks SET click_id = ${db_click_id} WHERE id = ${click_record_id}`;

        // Retorna o click_id limpo para o frontend/JS da pressel
        res.status(200).json({ status: 'success', click_id: clean_click_id });

        // Tarefas assíncronas (Geolocalização e Evento Meta)
        (async () => {
            try {
                let city = 'Desconhecida', state = 'Desconhecido';
                // Adiciona verificação para IPs locais comuns
                const isLocalIp = ip_address === '::1' || ip_address === '127.0.0.1' || ip_address.startsWith('192.168.') || ip_address.startsWith('10.');
                if (ip_address && !isLocalIp) {
                    try {
                        const geo = await axios.get(`http://ip-api.com/json/${ip_address}?fields=status,city,regionName`);
                        if (geo.data.status === 'success') {
                            city = geo.data.city || city;
                            state = geo.data.regionName || state;
                        } else {
                             logger.warn(`[GEO] Falha ao obter geolocalização para IP ${ip_address}: ${geo.data.message || 'Status não foi success'}`);
                        }
                    } catch (geoError) {
                         logger.error(`[GEO] Erro na API de geolocalização para IP ${ip_address}:`, geoError.message);
                    }
                } else if (isLocalIp) {
                     logger.debug(`[GEO] IP ${ip_address} é local. Pulando geolocalização.`);
                     city = 'Local'; // Ou mantenha Desconhecida
                     state = 'Local';
                }
                await sqlTx`UPDATE clicks SET city = ${city}, state = ${state} WHERE id = ${click_record_id}`;
                logger.debug(`[BACKGROUND] Geolocalização atualizada para o clique ${click_record_id} -> Cidade: ${city}, Estado: ${state}.`);



            } catch (backgroundError) {
                logger.error("Erro em tarefa de segundo plano (registerClick):", backgroundError.message);
            }
        })();

    } catch (error) {
        logger.error("Erro ao registrar clique:", error);
        if (!res.headersSent) {
            res.status(500).json({ message: 'Erro interno do servidor.' });
        }
    }
});
app.post('/api/click/info', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id } = req.body;
    if (!apiKey || !click_id) return res.status(400).json({ message: 'API Key e click_id são obrigatórios.' });
    
    try {
        const sellerResult = await sqlTx`SELECT id, email FROM sellers WHERE api_key = ${apiKey}`;
        if (sellerResult.length === 0) {
            console.warn(`[CLICK INFO] Tentativa de consulta com API Key inválida: ${apiKey}`);
            return res.status(401).json({ message: 'API Key inválida.' });
        }
        
        const seller_id = sellerResult[0].id;
        const seller_email = sellerResult[0].email;
        
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        
        const clickResult = await sqlTx`SELECT city, state FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller_id}`;
        
        if (clickResult.length === 0) {
            console.warn(`[CLICK INFO NOT FOUND] Vendedor (ID: ${seller_id}, Email: ${seller_email}) tentou consultar o click_id "${click_id}", mas não foi encontrado.`);
            return res.status(404).json({ message: 'Click ID não encontrado para este vendedor.' });
        }
        
        const clickInfo = clickResult[0];
        res.status(200).json({ status: 'success', city: clickInfo.city, state: clickInfo.state });

    } catch (error) {
        console.error("Erro ao consultar informações do clique:", error);
        res.status(500).json({ message: 'Erro interno ao consultar informações do clique.' });
    }
});
app.get('/api/dashboard/metrics', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        let { startDate, endDate } = req.query;
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        const totalClicksQuery = hasDateFilter
            ? sqlTx`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId} AND created_at BETWEEN ${startDate} AND ${endDate}`
            : sqlTx`SELECT COUNT(*) FROM clicks WHERE seller_id = ${sellerId}`;

        const pixGeneratedQuery = hasDateFilter
            ? sqlTx`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.created_at BETWEEN ${startDate} AND ${endDate}`
            : sqlTx`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId}`;

        const pixPaidQuery = hasDateFilter
            ? sqlTx`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate}`
            : sqlTx`SELECT COUNT(pt.id) AS total, COALESCE(SUM(pt.pix_value), 0) AS revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid'`;

        const botsPerformanceQuery = hasDateFilter
            ? sqlTx`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON (c.pressel_id = p.id OR c.bot_id = tb.id) AND c.seller_id = ${sellerId} AND c.created_at BETWEEN ${startDate} AND ${endDate} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`
            : sqlTx`SELECT tb.bot_name, COUNT(c.id) AS total_clicks, COUNT(pt.id) FILTER (WHERE pt.status = 'paid') AS total_pix_paid, COALESCE(SUM(pt.pix_value) FILTER (WHERE pt.status = 'paid'), 0) AS paid_revenue FROM telegram_bots tb LEFT JOIN pressels p ON p.bot_id = tb.id LEFT JOIN clicks c ON (c.pressel_id = p.id OR c.bot_id = tb.id) AND c.seller_id = ${sellerId} LEFT JOIN pix_transactions pt ON pt.click_id_internal = c.id WHERE tb.seller_id = ${sellerId} GROUP BY tb.bot_name ORDER BY paid_revenue DESC, total_clicks DESC`;

        const clicksByStateQuery = hasDateFilter
             ? sqlTx`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' AND c.created_at BETWEEN ${startDate} AND ${endDate} GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`
             : sqlTx`SELECT c.state, COUNT(c.id) AS total_clicks FROM clicks c WHERE c.seller_id = ${sellerId} AND c.state IS NOT NULL AND c.state != 'Desconhecido' GROUP BY c.state ORDER BY total_clicks DESC LIMIT 10`;

        const userTimezone = 'America/Sao_Paulo'; 
        const dailyRevenueQuery = hasDateFilter
             ? sqlTx`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' AND pt.paid_at BETWEEN ${startDate} AND ${endDate} GROUP BY 1 ORDER BY 1 ASC`
             : sqlTx`SELECT DATE(pt.paid_at AT TIME ZONE ${userTimezone}) as date, COALESCE(SUM(pt.pix_value), 0) as revenue FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id WHERE c.seller_id = ${sellerId} AND pt.status = 'paid' GROUP BY 1 ORDER BY 1 ASC`;
        
        const [
               totalClicksResult, pixGeneratedResult, pixPaidResult, botsPerformance,
               clicksByState, dailyRevenue
        ] = await Promise.all([
              totalClicksQuery, pixGeneratedQuery, pixPaidQuery, botsPerformanceQuery,
              clicksByStateQuery, dailyRevenueQuery
        ]);

        const totalClicks = totalClicksResult[0].count;
        const totalPixGenerated = pixGeneratedResult[0].total;
        const totalRevenue = pixGeneratedResult[0].revenue;
        const totalPixPaid = pixPaidResult[0].total;
        const paidRevenue = pixPaidResult[0].revenue;
        
        res.status(200).json({
            total_clicks: parseInt(totalClicks),
            total_pix_generated: parseInt(totalPixGenerated),
            total_pix_paid: parseInt(totalPixPaid),
            total_revenue: parseFloat(totalRevenue),
            paid_revenue: parseFloat(paidRevenue),
            bots_performance: botsPerformance.map(b => ({ ...b, total_clicks: parseInt(b.total_clicks), total_pix_paid: parseInt(b.total_pix_paid), paid_revenue: parseFloat(b.paid_revenue) })),
            clicks_by_state: clicksByState.map(s => ({ ...s, total_clicks: parseInt(s.total_clicks) })),
            daily_revenue: dailyRevenue.filter(d => d.date).map(d => ({ date: d.date.toISOString().split('T')[0], revenue: parseFloat(d.revenue) }))
        });
    } catch (error) {
        console.error("Erro ao buscar métricas do dashboard:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});
// ########## ROTA DE TRANSAÇÕES CORRIGIDA ##########
app.get('/api/transactions', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const { startDate, endDate } = req.query; // Pega as datas da query string
        const hasDateFilter = startDate && endDate && startDate !== '' && endDate !== '';

        // 1. Iniciar a string da query e o array de parâmetros
        let queryString = `
            SELECT
                pt.status,
                pt.pix_value,
                COALESCE(tb_pressel.bot_name, tb_organic.bot_name, hc.config->'content'->>'main_title', 'Origem Desconhecida') as source_name,
                pt.provider,
                pt.created_at
            FROM pix_transactions pt
            JOIN clicks c ON pt.click_id_internal = c.id
            -- Join para bots via Pressel
            LEFT JOIN pressels p ON c.pressel_id = p.id
            LEFT JOIN telegram_bots tb_pressel ON p.bot_id = tb_pressel.id
            -- Join para bots via Tráfego Orgânico (direto do clique)
            LEFT JOIN telegram_bots tb_organic ON c.bot_id = tb_organic.id
            -- Join para Checkouts Hospedados
            LEFT JOIN hosted_checkouts hc ON c.checkout_id = hc.id
            WHERE c.seller_id = $1
        `;
        const queryParams = [sellerId]; // $1 é sellerId

        if (hasDateFilter) {
            // 2. Adicionar o filtro de data à string e os parâmetros ao array
            queryString += ` AND pt.created_at BETWEEN $2 AND $3`;
            queryParams.push(startDate); // $2 é startDate
            queryParams.push(endDate);   // $3 é endDate
        }

        // 3. Adicionar paginação (obrigatória para evitar sobrecarga)
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 100, 1000); // Máximo 1000 por página
        const offset = (page - 1) * limit;
        
        queryString += ` ORDER BY pt.created_at DESC LIMIT $${queryParams.length + 1} OFFSET $${queryParams.length + 2};`;
        queryParams.push(limit, offset);

        // 4. Executar a consulta usando a sintaxe compatível com o cliente postgres
        const transactions = await sqlTx.unsafe(queryString, queryParams);
        
        // 5. Buscar total para paginação
        let countQuery = `
            SELECT COUNT(*) as total
            FROM pix_transactions pt
            JOIN clicks c ON pt.click_id_internal = c.id
            WHERE c.seller_id = $1
        `;
        const countParams = [sellerId];
        if (hasDateFilter) {
            countQuery += ` AND pt.created_at BETWEEN $2 AND $3`;
            countParams.push(startDate, endDate);
        }
        const [countResult] = await sqlTx.unsafe(countQuery, countParams);
        const total = parseInt(countResult.total);

        res.status(200).json({
            transactions,
            pagination: {
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit)
            }
        });
    } catch (error) {
        console.error("Erro ao buscar transações:", error); // Loga o erro completo no console
        res.status(500).json({ message: 'Erro ao buscar dados das transações.' });
    }
});

app.post('/api/pix/generate', logApiRequest, async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { click_id, value_cents, customer, product } = req.body;

    if (!apiKey || !click_id || !value_cents) return res.status(400).json({ message: 'API Key, click_id e value_cents são obrigatórios.' });

    try {
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) return res.status(401).json({ message: 'API Key inválida.' });

        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        const [click] = await sqlTx`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller.id}`;
        if (!click) return res.status(404).json({ message: 'Click ID não encontrado.' });

        const ip_address = click.ip_address;

        // *** SUBSTITUIÇÃO DA LÓGICA DO LOOP PELA NOVA FUNÇÃO ***
        const pixResult = await generatePixWithFallback(seller, value_cents, req.headers.host, apiKey, ip_address, click.id); // Passa click.id

        // O INSERT já foi feito dentro de generatePixWithFallback
        // Apenas continue com os eventos pós-geração

        if (click.pressel_id || click.checkout_id) { // Verifica se veio de pressel ou checkout
             await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: value_cents / 100 }, null);
        }

        const customerDataForUtmify = customer || { name: "Cliente Interessado", email: "cliente@email.com" };
        const productDataForUtmify = product || { id: "prod_1", name: "Produto Ofertado" };
        await sendEventToUtmify('waiting_payment', click, { provider_transaction_id: pixResult.transaction_id, pix_value: value_cents / 100, created_at: new Date() }, seller, customerDataForUtmify, productDataForUtmify);

        // Remove o internal_transaction_id antes de retornar para a API externa
        const { internal_transaction_id, ...apiResponse } = pixResult;
        return res.status(200).json(apiResponse);

    } catch (error) { // O catch agora pega o erro lançado por generatePixWithFallback se todos falharem
        console.error(`[PIX GENERATE ERROR] Erro na rota /api/pix/generate:`, error.message);
        // Retorna a mensagem de erro específica lançada pela função de fallback
        res.status(500).json({ message: error.message || 'Erro interno ao processar a geração de PIX.' });
    }
});

app.get('/api/pix/status/:transaction_id', async (req, res) => {
    const apiKey = req.headers['x-api-key'];
    const { transaction_id } = req.params; // Pode ser ID do provedor ou nosso pix_id

    if (!apiKey) return res.status(401).json({ message: 'API Key não fornecida.' });
    if (!transaction_id) return res.status(400).json({ message: 'ID da transação é obrigatório.' });

    try {
        // Valida API Key e obtém o seller
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE api_key = ${apiKey}`;
        if (!seller) {
            return res.status(401).json({ message: 'API Key inválida.' });
        }

        // Busca a transação e dados do clique associado (inclui checkout_id e click_id originais)
        const [transaction] = await sqlTx`
            SELECT pt.*, c.checkout_id, c.click_id
            FROM pix_transactions pt JOIN clicks c ON pt.click_id_internal = c.id
            WHERE (pt.provider_transaction_id = ${transaction_id} OR pt.pix_id = ${transaction_id})
              AND c.seller_id = ${seller.id}`;

        if (!transaction) {
            return res.status(404).json({ status: 'not_found', message: 'Transação não encontrada.' });
        }

        let currentStatus = transaction.status;
        let customerData = {};

        // Se ainda pendente e passível de consulta, tenta confirmar no provedor
        if (currentStatus === 'pending' && transaction.provider !== 'oasyfy' && transaction.provider !== 'cnpay' && transaction.provider !== 'brpix') {
            try {
                let providerStatus;
                if (transaction.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                        headers: { 'Authorization': `Bearer ${syncPayToken}` }
                    });
                    providerStatus = response.data.status;
                    customerData = response.data.payer;
                } else if (transaction.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    // CORREÇÃO: Campo correto é 'payer_national_registration' conforme documentação PushinPay
                    customerData = { name: response.data.payer_name, document: response.data.payer_national_registration };
                } else if (transaction.provider === 'wiinpay') {
                    const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                    if (wiinpayApiKey) {
                        const now = Date.now();
                        const last = wiinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                        if (now - last >= 60_000) {
                            const result = await getWiinpayPaymentStatus(transaction.provider_transaction_id, wiinpayApiKey);
                            providerStatus = result.status;
                            customerData = result.customer || {};
                            wiinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                        }
                    } else {
                        console.warn(`[PIX Status] Seller ${seller.id} sem chave WiinPay configurada para consulta.`);
                    }
                } else if (transaction.provider === 'paradise') {
                    const paradiseSecretKey = seller.paradise_secret_key;
                    if (paradiseSecretKey) {
                        const now = Date.now();
                        const last = paradiseLastCheckAt.get(transaction.provider_transaction_id) || 0;
                        if (now - last >= 60_000) {
                            const result = await getParadisePaymentStatus(transaction.provider_transaction_id, paradiseSecretKey);
                            providerStatus = result.status;
                            customerData = result.customer || {};
                            paradiseLastCheckAt.set(transaction.provider_transaction_id, now);
                        }
                    } else {
                        console.warn(`[PIX Status] Seller ${seller.id} sem chave Paradise configurada para consulta.`);
                    }
                }

                const normalizedProviderStatus = String(providerStatus || '').toLowerCase();
                const paidStatuses = new Set(['paid', 'completed', 'approved', 'success', 'received']);

                if (paidStatuses.has(normalizedProviderStatus)) {
                    await handleSuccessfulPayment(transaction.id, customerData);
                    currentStatus = 'paid';
                }
            } catch (providerError) {
                if (!providerError.response || providerError.response.status !== 404) {
                    console.error(`Falha ao consultar o provedor (${transaction.provider}) para a transação ${transaction.id}:`, providerError.message);
                }
                // Continua retornando pending se não conseguiu confirmar
            }
        }

        // Se já está paga, verificar se eventos foram enviados antes de retornar
        if (currentStatus === 'paid' && !transaction.meta_event_id) {
            // Transação está paga mas eventos não foram enviados - tentar enviar
            console.log(`[PIX Status] Transação ${transaction.id} está paga mas sem meta_event_id. Tentando enviar eventos...`);
            
            // Se não temos customerData ainda, tentar buscar do provider
            if (Object.keys(customerData).length === 0 && transaction.provider !== 'oasyfy' && transaction.provider !== 'cnpay' && transaction.provider !== 'brpix') {
                try {
                    if (transaction.provider === 'syncpay') {
                        const syncPayToken = await getSyncPayAuthToken(seller);
                        const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                            headers: { 'Authorization': `Bearer ${syncPayToken}` }
                        });
                        customerData = response.data.payer || {};
                    } else if (transaction.provider === 'pushinpay') {
                        const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                        customerData = { name: response.data.payer_name, document: response.data.payer_national_registration };
                    } else if (transaction.provider === 'wiinpay') {
                        const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                        if (wiinpayApiKey) {
                            const result = await getWiinpayPaymentStatus(transaction.provider_transaction_id, wiinpayApiKey);
                            customerData = result.customer || {};
                        }
                    } else if (transaction.provider === 'paradise') {
                        const paradiseSecretKey = seller.paradise_secret_key;
                        if (paradiseSecretKey) {
                            const result = await getParadisePaymentStatus(transaction.provider_transaction_id, paradiseSecretKey);
                            customerData = result.customer || {};
                        }
                    }
                    
                    // Tentar enviar eventos mesmo que a transação já esteja paga
                    if (Object.keys(customerData).length > 0) {
                        await handleSuccessfulPayment(transaction.id, customerData);
                    }
                } catch (error) {
                    console.error(`[PIX Status] Erro ao tentar enviar eventos para transação já paga:`, error.message);
                }
            } else if (Object.keys(customerData).length > 0) {
                // Temos customerData do bloco anterior, tentar enviar eventos
                await handleSuccessfulPayment(transaction.id, customerData);
            }
        }

        if (currentStatus === 'paid') {
            let redirectUrl = null;
            // Se veio de checkout hospedado, tenta buscar URL de sucesso no config
            if (transaction.checkout_id && String(transaction.checkout_id).startsWith('cko_')) {
                const [checkoutConfig] = await sqlTx`SELECT config FROM hosted_checkouts WHERE id = ${transaction.checkout_id}`;
                let checkoutConfigJson;
                try {
                    checkoutConfigJson = parseJsonField(checkoutConfig?.config, `hosted_checkouts:${transaction.checkout_id}`);
                } catch {
                    checkoutConfigJson = null;
                }
                redirectUrl = checkoutConfigJson?.redirects?.success_url || null;
                if (!redirectUrl) {
                    console.warn(`[PIX Status] Checkout ${transaction.checkout_id} sem redirects.success_url configurado.`);
                }
            }

            return res.status(200).json({
                status: 'paid',
                redirectUrl: redirectUrl,
                click_id: transaction.click_id
            });
        }

        return res.status(200).json({ status: currentStatus });

    } catch (error) {
        console.error("Erro ao consultar status da transação:", error);
        res.status(500).json({ message: 'Erro interno ao consultar o status.' });
    }
});
app.post('/api/pix/test-provider', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { provider } = req.body;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;

    if (!provider) {
        return res.status(400).json({ message: 'O nome do provedor é obrigatório.' });
    }

    try {
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        const value_cents = 3333;
        
        const startTime = Date.now();
        const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
        const endTime = Date.now();
        const responseTime = ((endTime - startTime) / 1000).toFixed(2);

        res.status(200).json({
            provider: provider.toUpperCase(),
            acquirer: pixResult.acquirer,
            responseTime: responseTime,
            qr_code_text: pixResult.qr_code_text
        });

    } catch (error) {
        console.error(`[PIX TEST ERROR] Seller ID: ${sellerId}, Provider: ${provider} - Erro:`, error.response?.data || error.message);
        res.status(500).json({ 
            message: `Falha ao gerar PIX de teste com ${provider.toUpperCase()}. Verifique as credenciais.`, 
            details: error.response?.data ? JSON.stringify(error.response.data) : error.message 
        });
    }
});
app.post('/api/pix/test-priority-route', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
    let testLog = [];

    try {
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        const providerOrder = [
            { name: seller.pix_provider_primary, position: 'Primário' },
            { name: seller.pix_provider_secondary, position: 'Secundário' },
            { name: seller.pix_provider_tertiary, position: 'Terciário' }
        ].filter(p => p.name); 

        if (providerOrder.length === 0) {
            return res.status(400).json({ message: 'Nenhuma ordem de prioridade de provedores foi configurada.' });
        }

        const value_cents = 3333;

        for (const providerInfo of providerOrder) {
            const provider = providerInfo.name;
            const position = providerInfo.position;
            
            try {
                const startTime = Date.now();
                const pixResult = await generatePixForProvider(provider, seller, value_cents, req.headers.host, seller.api_key, ip_address);
                const endTime = Date.now();
                const responseTime = ((endTime - startTime) / 1000).toFixed(2);

                testLog.push(`SUCESSO com Provedor ${position} (${provider.toUpperCase()}).`);
                return res.status(200).json({
                    success: true, position: position, provider: provider.toUpperCase(),
                    acquirer: pixResult.acquirer, responseTime: responseTime,
                    qr_code_text: pixResult.qr_code_text, log: testLog
                });

            } catch (error) {
                let errorMessage = error.message;
                if (error.response && error.response.data) {
                    errorMessage = JSON.stringify(error.response.data);
                }
                console.error(`Falha no provedor ${position} (${provider}):`, error.response?.data || error.message);
                testLog.push(`FALHA com Provedor ${position} (${provider.toUpperCase()}): ${errorMessage}`);
            }
        }

        console.error("Todos os provedores na rota de prioridade falharam.");
        return res.status(500).json({
            success: false, message: 'Todos os provedores configurados na sua rota de prioridade falharam.',
            log: testLog
        });

    } catch (error) {
        console.error(`[PIX PRIORITY TEST ERROR] Erro geral:`, error.message);
        res.status(500).json({ 
            success: false, message: 'Ocorreu um erro inesperado ao testar a rota de prioridade.',
            log: testLog
        });
    }
});

// ==========================================================
//          MOTOR DE FLUXO E WEBHOOK DO TELEGRAM (VERSÃO FINAL)
// ==========================================================
function findNextNode(currentNodeId, handleId, edges) {
    const edge = edges.find(edge => edge.source === currentNodeId && (edge.sourceHandle === handleId || !edge.sourceHandle || handleId === null));
    return edge ? edge.target : null;
}

async function sendTypingAction(chatId, botToken) {
    try {
        await axios.post(`https://api.telegram.org/bot${botToken}/sendChatAction`, {
            chat_id: chatId,
            action: 'typing',
        });
    } catch (error) {
        console.warn(`[Flow Engine] Falha ao enviar ação 'typing' para ${chatId}:`, error.response?.data || error.message);
    }
}

// Envia a ação de digitação continuamente durante um período
async function showTypingForDuration(chatId, botToken, durationMs) {
    const endTime = Date.now() + durationMs;
    while (Date.now() < endTime) {
        await sendTypingAction(chatId, botToken);
        const remaining = endTime - Date.now();
        const wait = Math.min(5000, remaining);
        await new Promise(resolve => setTimeout(resolve, wait));
    }
}

async function sendMessage(chatId, text, botToken, sellerId, botId, showTyping, variables = {}) {
    if (!text || text.trim() === '') return;
    try {
        if (showTyping) {
            await sendTypingAction(chatId, botToken);
            let typingDuration = Math.max(500, Math.min(2000, text.length * 50));
            await new Promise(resolve => setTimeout(resolve, typingDuration));
        }
        const response = await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, { chat_id: chatId, text: text, parse_mode: 'HTML' });
        if (response.data.ok) {
            const sentMessage = response.data.result;
            // CORREÇÃO FINAL: Salva NULL para os dados do usuário quando o remetente é o bot.
            await sqlTx`
                INSERT INTO telegram_chats (seller_id, bot_id, chat_id, message_id, user_id, first_name, last_name, username, message_text, sender_type, click_id)
                VALUES (${sellerId}, ${botId}, ${chatId}, ${sentMessage.message_id}, ${sentMessage.from.id}, NULL, NULL, NULL, ${text}, 'bot', ${variables.click_id || null})
                ON CONFLICT (chat_id, message_id) DO NOTHING;
            `;
        }
    } catch (error) {
        logger.error(`[Flow Engine] Erro ao enviar/salvar mensagem:`, error.response?.data || error.message);
    }
}
/**
 * [REATORADO] Executa uma lista de ações sequencialmente.
 * Esta função é chamada pelo processFlow para rodar as ações DENTRO de um nó.
 * @returns {string} Retorna 'paid', 'pending', 'flow_forwarded', ou 'completed' para que o processFlow decida a navegação.
 */
async function processActions(actions, chatId, botId, botToken, sellerId, variables, logPrefix = '[Actions]') {
    logger.debug(`${logPrefix} Iniciando processamento de ${actions.length} ações aninhadas para chat ${chatId}`);


    
    const normalizeChatIdentifier = (value) => {
        if (value === null || value === undefined) return null;
        const trimmed = String(value).trim();
        if (!trimmed) return null;
        if (/^-?\d+$/.test(trimmed)) {
            const numericId = Number(trimmed);
            if (Number.isSafeInteger(numericId)) {
                return numericId;
            }
        }
        return trimmed;
    };

    for (let i = 0; i < actions.length; i++) {
        const action = actions[i];
        const actionData = action.data || {}; // Garante que actionData exista
        logger.debug(`${logPrefix} [${i + 1}/${actions.length}] Processando ação: ${action.type}`);



        switch (action.type) {
            case 'message':
                try {
                    let textToSend = await replaceVariables(actionData.text, variables);
                    
                    // Validação do tamanho do texto (limite do Telegram: 4096 caracteres)
                    if (textToSend.length > 4096) {
                        logger.warn(`${logPrefix} [Flow Message] Texto excede limite de 4096 caracteres. Truncando...`);
                        textToSend = textToSend.substring(0, 4093) + '...';
                    }
                    
                    // Verifica se tem botão para anexar
                    if (actionData.buttonUrl) {
                        // Trata buttonText vazio, null, undefined ou string vazia
                        let rawBtnText = actionData.buttonText;
                        if (!rawBtnText || (typeof rawBtnText === 'string' && rawBtnText.trim() === '')) {
                            rawBtnText = 'Clique aqui';
                            logger.warn(`${logPrefix} [Flow Message] Botão sem texto informado. Aplicando texto padrão '${rawBtnText}'.`);
                        }
                        
                        let btnText = await replaceVariables(rawBtnText, variables);
                        // Garante que após replaceVariables ainda tenha texto válido
                        btnText = btnText && btnText.trim() !== '' ? btnText : 'Clique aqui';

                        let btnUrl = await replaceVariables(actionData.buttonUrl, variables);
                        
                        // Se a URL for um checkout ou thank you page e tivermos click_id, adiciona como parâmetro
                        if (variables.click_id && (btnUrl.includes('/oferta/') || btnUrl.includes('/obrigado/'))) {
                            try {
                                // Adiciona protocolo se não existir
                                const urlWithProtocol = btnUrl.startsWith('http') ? btnUrl : `https://${btnUrl}`;
                                const urlObj = new URL(urlWithProtocol);
                                // Remove prefixo '/start ' se existir
                                const cleanClickId = variables.click_id.replace('/start ', '');
                                urlObj.searchParams.set('click_id', cleanClickId);
                                btnUrl = urlObj.toString();
                                
                                // Log apropriado baseado no tipo de URL
                                const urlType = btnUrl.includes('/obrigado/') ? 'thank you page' : 'checkout hospedado';
                                logger.debug(`${logPrefix} [Flow Message] Adicionando click_id ${cleanClickId} ao botão de ${urlType}`);
                            } catch (urlError) {
                                logger.error(`${logPrefix} [Flow Message] Erro ao processar URL: ${urlError.message}`);
                            }
                        }
                        
                        // Envia com botão inline
                        const payload = { 
                            chat_id: chatId, 
                            text: textToSend, 
                            parse_mode: 'HTML',
                            reply_markup: { 
                                inline_keyboard: [[{ text: btnText, url: btnUrl }]] 
                            }
                        };
                        
                        const response = await sendTelegramRequest(botToken, 'sendMessage', payload);
                        if (response && response.ok) {
                            await saveMessageToDb(sellerId, botId, response.result, 'bot');
                        }
                    } else {
                        // Envia mensagem normal sem botão
                        await sendMessage(chatId, textToSend, botToken, sellerId, botId, false, variables);
                    }
                } catch (error) {
                    logger.error(`${logPrefix} [Flow Message] Erro ao enviar mensagem: ${error.message}`);

                }
                break;

            case 'image':
            case 'video':
            case 'audio': {
                try {
                    let caption = await replaceVariables(actionData.caption, variables);
                    
                    // Validação do tamanho da legenda (limite do Telegram: 1024 caracteres)
                    if (caption && caption.length > 1024) {
                        logger.warn(`${logPrefix} [Flow Media] Legenda excede limite de 1024 caracteres. Truncando...`);
                        caption = caption.substring(0, 1021) + '...';
                    }
                    
                    const response = await handleMediaNode(action, botToken, chatId, caption); // Passa a ação inteira

                    if (response && response.ok) {
                        await saveMessageToDb(sellerId, botId, response.result, 'bot');
                    }
                } catch (e) {
                    logger.error(`${logPrefix} [Flow Media] Erro ao enviar mídia (ação ${action.type}) para o chat ${chatId}: ${e.message}`);

                }
                break;
            }

            case 'delay':
                const delaySeconds = actionData.delayInSeconds || 1;
                logger.debug(`${logPrefix} [Delay] Aguardando ${delaySeconds} segundos...`);
                await new Promise(resolve => setTimeout(resolve, delaySeconds * 1000));
                logger.debug(`${logPrefix} [Delay] Delay de ${delaySeconds}s concluído.`);
                break;
            
            case 'typing_action':
                if (actionData.durationInSeconds && actionData.durationInSeconds > 0) {
                    await showTypingForDuration(chatId, botToken, actionData.durationInSeconds * 1000);
                }
                break;
            
            case 'action_pix':
                try {
                    logger.debug(`${logPrefix} Executando action_pix para chat ${chatId}`);
                    const valueInCents = actionData.valueInCents;
                    if (!valueInCents) throw new Error("Valor do PIX não definido na ação do fluxo.");
    
                    const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
                    if (!seller) throw new Error(`${logPrefix} Vendedor ${sellerId} não encontrado.`);
    
                    let click_id_from_vars = variables.click_id;
                    if (!click_id_from_vars) {
                        const [recentClick] = await sqlTx`
                            SELECT click_id FROM telegram_chats 
                            WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL 
                            ORDER BY created_at DESC LIMIT 1`;
                        if (recentClick?.click_id) {
                            click_id_from_vars = recentClick.click_id;
                        }
                    }
    
                    if (!click_id_from_vars) {
                        throw new Error(`${logPrefix} Click ID não encontrado para gerar PIX.`);
                    }
    
                    const db_click_id = click_id_from_vars.startsWith('/start ') ? click_id_from_vars : `/start ${click_id_from_vars}`;
                    const [click] = await sqlTx`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${sellerId}`;
                    if (!click) throw new Error(`${logPrefix} Click ID não encontrado para este vendedor.`);

                    const ip_address = click.ip_address;
                    const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';
                    
                    // Gera PIX e salva no banco
                    const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);
                    logger.debug(`${logPrefix} PIX gerado com sucesso. Transaction ID: ${pixResult.transaction_id}`);
                    
                    // Atualiza as variáveis do fluxo (IMPORTANTE)
                    variables.last_transaction_id = pixResult.transaction_id;
                    console.log(
                        '[PIX][action_pix] seller=%s click=%s transaction_id=%s',
                        sellerId,
                        click.id,
                        variables.last_transaction_id
                    );
    
                    let messageText = await replaceVariables(actionData.pixMessageText || "", variables);
                    
                    // Validação do tamanho do texto da mensagem do PIX (limite de 1024 caracteres)
                    if (messageText && messageText.length > 1024) {
                        logger.warn(`${logPrefix} [PIX] Texto da mensagem excede limite de 1024 caracteres. Truncando...`);
                        messageText = messageText.substring(0, 1021) + '...';
                    }
                    
                    const buttonText = await replaceVariables(actionData.pixButtonText || "📋 Copiar", variables);
                    const pixToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;
    
                    // CRÍTICO: Tenta enviar o PIX para o usuário
                    const sentMessage = await sendTelegramRequest(botToken, 'sendMessage', {
                        chat_id: chatId, text: pixToSend, parse_mode: 'HTML',
                        reply_markup: { inline_keyboard: [[{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]] }
                    });
    
                    // Verifica se o envio foi bem-sucedido
                    if (!sentMessage.ok) {
                        // Cancela a transação PIX no banco se não conseguiu enviar ao usuário
                        logger.error(`${logPrefix} FALHA ao enviar PIX. Cancelando transação ${pixResult.transaction_id}. Motivo: ${sentMessage.description || 'Desconhecido'}`);
                        
                        await sqlTx`
                            UPDATE pix_transactions 
                            SET status = 'canceled' 
                            WHERE provider_transaction_id = ${pixResult.transaction_id}
                        `;
                        
                        throw new Error(`Não foi possível enviar PIX ao usuário. Motivo: ${sentMessage.description || 'Erro desconhecido'}. Transação cancelada.`);
                    }
                    
                    // Salva a mensagem no banco
                    await saveMessageToDb(sellerId, botId, sentMessage.result, 'bot');
                    logger.debug(`${logPrefix} PIX enviado com sucesso ao usuário ${chatId}`);
                    
                    // Envia eventos para Utmify e Meta SOMENTE APÓS confirmação de entrega ao usuário
                    const customerDataForUtmify = { name: variables.nome_completo || "Cliente Bot", email: "bot@email.com" };
                    const productDataForUtmify = { id: "prod_bot", name: "Produto (Fluxo Bot)" };
                    await sendEventToUtmify(
                        'waiting_payment', 
                        click, 
                        { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() }, 
                        seller, customerDataForUtmify, productDataForUtmify
                    );
                    logger.debug(`${logPrefix} Evento 'waiting_payment' enviado para Utmify para o clique ${click.id}.`);

                    // Envia InitiateCheckout para Meta se o click veio de pressel ou checkout
                    if (click.pressel_id || click.checkout_id) {
                        await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: valueInCents / 100 }, null);
                        logger.debug(`${logPrefix} Evento 'InitiateCheckout' enviado para Meta para o clique ${click.id}.`);
                    }
                } catch (error) {
                    logger.error(`${logPrefix} Erro no nó action_pix para chat ${chatId}:`, error.message);
                    // Re-lança o erro para que o fluxo seja interrompido
                    throw error;
                }
                break;

            case 'action_check_pix':
                try {
                    const transactionId = variables.last_transaction_id;
                    console.log(
                        '[PIX][action_check_pix] seller=%s chat=%s buscando transaction_id=%s',
                        sellerId,
                        chatId,
                        transactionId
                    );
                    if (!transactionId) throw new Error("Nenhum ID de transação PIX nas variáveis.");
                    
                    const [transaction] = await sqlTx`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId}`;
                    if (!transaction) throw new Error(`Transação ${transactionId} não encontrada.`);

                    // Tenta consultar o provedor
                    const paidStatuses = new Set(['paid', 'completed', 'approved', 'success', 'received']);
                    let providerStatus = null;
                    let customerData = {};
                    const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;

                    // Se já está paga, verificar se eventos foram enviados
                    if (transaction.status === 'paid') {
                        // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                        if (!transaction.meta_event_id) {
                            console.log(`[PIX][action_check_pix] Transação ${transactionId} está paga mas sem meta_event_id. Tentando buscar dados do provider e enviar eventos...`);
                            
                            // Tentar buscar dados do cliente do provider
                            try {
                                if (transaction.provider === 'pushinpay') {
                                    const last = pushinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                                    const now = Date.now();
                                    if (now - last >= 60_000) {
                                        const resp = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`,
                                            { headers: { Authorization: `Bearer ${seller.pushinpay_token}`, Accept: 'application/json', 'Content-Type': 'application/json' } });
                                        customerData = { name: resp.data.payer_name, document: resp.data.payer_national_registration };
                                        pushinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                                    }
                                } else if (transaction.provider === 'syncpay') {
                                    const syncPayToken = await getSyncPayAuthToken(seller);
                                    const resp = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`,
                                        { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                                    customerData = resp.data.payer || {};
                                } else if (transaction.provider === 'wiinpay') {
                                    const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                                    if (wiinpayApiKey) {
                                        const now = Date.now();
                                        const last = wiinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                                        if (now - last >= 60_000) {
                                            const result = await getWiinpayPaymentStatus(transaction.provider_transaction_id, wiinpayApiKey);
                                            customerData = result.customer || {};
                                            wiinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                                        }
                                    }
                                } else if (transaction.provider === 'paradise') {
                                    const paradiseSecretKey = seller.paradise_secret_key;
                                    if (paradiseSecretKey) {
                                        const now = Date.now();
                                        const last = paradiseLastCheckAt.get(transaction.provider_transaction_id) || 0;
                                        if (now - last >= 60_000) {
                                            const result = await getParadisePaymentStatus(transaction.provider_transaction_id, paradiseSecretKey);
                                            customerData = result.customer || {};
                                            paradiseLastCheckAt.set(transaction.provider_transaction_id, now);
                                        }
                                    }
                                }
                                
                                // Tentar enviar eventos mesmo que a transação já esteja paga
                                await handleSuccessfulPayment(transaction.id, customerData);
                                console.log(`[PIX][action_check_pix] Eventos enviados para transação ${transactionId} que já estava paga.`);
                            } catch (error) {
                                console.error(`[PIX][action_check_pix] Erro ao tentar enviar eventos para transação já paga:`, error.message);
                            }
                        }
                        return 'paid'; // Sinaliza para 'processFlow' seguir pelo handle 'a'
                    }

                    // Se não está paga, consultar o provider normalmente
                    if (transaction.provider === 'pushinpay') {
                        const last = pushinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                        const now = Date.now();
                        if (now - last >= 60_000) { // Limite de 1 min
                            const resp = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`,
                                { headers: { Authorization: `Bearer ${seller.pushinpay_token}`, Accept: 'application/json', 'Content-Type': 'application/json' } });
                            providerStatus = String(resp.data.status || '').toLowerCase();
                            customerData = { name: resp.data.payer_name, document: resp.data.payer_national_registration };
                            pushinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                        }
                    } else if (transaction.provider === 'syncpay') {
                        const syncPayToken = await getSyncPayAuthToken(seller);
                        const resp = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`,
                            { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                        providerStatus = String(resp.data.status || '').toLowerCase();
                        customerData = resp.data.payer || {};
                    } else if (transaction.provider === 'wiinpay') {
                        const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                        if (wiinpayApiKey) {
                            const now = Date.now();
                            const last = wiinpayLastCheckAt.get(transaction.provider_transaction_id) || 0;
                            if (now - last >= 60_000) {
                                const result = await getWiinpayPaymentStatus(transaction.provider_transaction_id, wiinpayApiKey);
                                providerStatus = result.status || null;
                                customerData = result.customer || {};
                                wiinpayLastCheckAt.set(transaction.provider_transaction_id, now);
                            }
                        } else {
                            console.warn(`${logPrefix} Seller ${sellerId} sem chave WiinPay configurada.`);
                        }
                    } else if (transaction.provider === 'paradise') {
                        const paradiseSecretKey = seller.paradise_secret_key;
                        if (paradiseSecretKey) {
                            const now = Date.now();
                            const last = paradiseLastCheckAt.get(transaction.provider_transaction_id) || 0;
                            if (now - last >= 60_000) {
                                const result = await getParadisePaymentStatus(transaction.provider_transaction_id, paradiseSecretKey);
                                providerStatus = result.status || null;
                                customerData = result.customer || {};
                                paradiseLastCheckAt.set(transaction.provider_transaction_id, now);
                            }
                        } else {
                            console.warn(`${logPrefix} Seller ${sellerId} sem chave Paradise configurada.`);
                        }
                    }
                    
                    if (providerStatus && paidStatuses.has(providerStatus)) {
                        await handleSuccessfulPayment(transaction.id, customerData); // Atualiza o DB
                        return 'paid'; // Sinaliza para 'processFlow' seguir pelo handle 'a'
                    } else {
                        return 'pending'; // Sinaliza para 'processFlow' seguir pelo handle 'b'
                    }
                } catch (error) {
                    logger.error(`${logPrefix} Erro ao consultar PIX:`, error);
                    return 'pending'; // Em caso de erro, assume pendente e segue pelo handle 'b'
                }
            case 'forward_flow':
                const targetFlowId = actionData.targetFlowId;
                if (!targetFlowId) {
                    logger.error(`${logPrefix} 'forward_flow' action não tem targetFlowId. Action completa:`, JSON.stringify(action, null, 2));
                    break;
                }

                // Garante que targetFlowId seja um número para a query SQL
                const targetFlowIdNum = parseInt(targetFlowId, 10);
                if (isNaN(targetFlowIdNum)) {
                    logger.error(`${logPrefix} 'forward_flow' targetFlowId inválido: ${targetFlowId}`);
                    break;
                }

                logger.debug(`${logPrefix} Encaminhando para o fluxo ${targetFlowIdNum} para o chat ${chatId}`);

                // Cancela qualquer tarefa de timeout pendente antes de encaminhar para o novo fluxo
                try {
                    const [stateToCancel] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    if (stateToCancel && stateToCancel.scheduled_message_id) {
                        try {
                            await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                            logger.debug(`${logPrefix} [Forward Flow] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                        } catch (e) {
                            logger.warn(`${logPrefix} [Forward Flow] Falha ao cancelar QStash msg ${stateToCancel.scheduled_message_id}:`, e.message);
                        }
                    }
                } catch (e) {
                    logger.error(`${logPrefix} [Forward Flow] Erro ao verificar tarefas pendentes:`, e.message);
                }

                // Carrega o fluxo de destino e descobre o primeiro nó depois do trigger
                const [targetFlow] = await sqlTx`SELECT * FROM flows WHERE id = ${targetFlowIdNum} AND bot_id = ${botId}`;
                if (!targetFlow || !targetFlow.nodes) {
                    logger.error(`${logPrefix} Fluxo de destino ${targetFlowIdNum} não encontrado.`);
                    break;
                }
                const targetFlowData = typeof targetFlow.nodes === 'string' ? JSON.parse(targetFlow.nodes) : targetFlow.nodes;
                const targetNodes = targetFlowData.nodes || [];
                const targetEdges = targetFlowData.edges || [];
                const targetStartNode = targetNodes.find(n => n.type === 'trigger');
                if (!targetStartNode) {
                    logger.error(`${logPrefix} Fluxo de destino ${targetFlowIdNum} não tem nó de 'trigger'.`);
                    break;
                }
                
                // Encontra o primeiro nó válido (não trigger) após o trigger inicial
                let currentNodeId = findNextNode(targetStartNode.id, 'a', targetEdges);
                let attempts = 0;
                const maxAttempts = 20; // Proteção contra loops infinitos
                
                // Limpa o estado atual antes de iniciar o novo fluxo
                await sqlTx`UPDATE user_flow_states 
                          SET waiting_for_input = false, scheduled_message_id = NULL 
                          WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                
                // Pula nós do tipo 'trigger' até encontrar um nó válido
                while (currentNodeId && attempts < maxAttempts) {
                    const currentNode = targetNodes.find(n => n.id === currentNodeId);
                    if (!currentNode) {
                        logger.error(`${logPrefix} Nó ${currentNodeId} não encontrado no fluxo de destino.`);
                        break;
                    }
                    
                    if (currentNode.type !== 'trigger') {
                        // Encontrou um nó válido (não é trigger)
                        logger.debug(`${logPrefix} Encontrado nó válido para iniciar: ${currentNodeId} (tipo: ${currentNode.type})`);
                        // Passa os dados do fluxo de destino para o processFlow recursivo
                        await processFlow(chatId, botId, botToken, sellerId, currentNodeId, variables, targetNodes, targetEdges);
                        break;
                    }
                    
                    // Se for trigger, continua procurando o próximo nó
                    logger.debug(`${logPrefix} Pulando nó trigger ${currentNodeId}, procurando próximo nó...`);
                    currentNodeId = findNextNode(currentNodeId, 'a', targetEdges);
                    attempts++;
                }
                
                if (!currentNodeId || attempts >= maxAttempts) {
                    if (attempts >= maxAttempts) {
                        logger.error(`${logPrefix} Limite de tentativas atingido ao procurar nó válido no fluxo ${targetFlowIdNum}.`);
                    } else {
                        logger.debug(`${logPrefix} Fluxo de destino ${targetFlowIdNum} está vazio (sem nó válido após o trigger).`);
                    }
                }
                return 'flow_forwarded';
                
            case 'action_create_invite_link':
                try {
                    logger.debug(`${logPrefix} Executando action_create_invite_link para chat ${chatId}`);
                    
                    // Buscar o supergroup_id do bot
                    const [botInvite] = await sqlTx`
                        SELECT telegram_supergroup_id 
                        FROM telegram_bots 
                        WHERE id = ${botId}
                    `;
                    
                    if (!botInvite?.telegram_supergroup_id) {
                        throw new Error('Supergrupo não configurado para este bot');
                    }
                    
                    const normalizedChatId = normalizeChatIdentifier(botInvite.telegram_supergroup_id);
                    if (!normalizedChatId) {
                        throw new Error('ID do supergrupo inválido para criação de convite');
                    }
                    
                    const userToUnban = actionData.userId || chatId;
                    const normalizedUserId = normalizeChatIdentifier(userToUnban);
                    try {
                        const unbanResponse = await sendTelegramRequest(
                            botToken,
                            'unbanChatMember',
                            {
                                chat_id: normalizedChatId,
                                user_id: normalizedUserId,
                                only_if_banned: true
                            }
                        );
                        if (unbanResponse?.ok) {
                            logger.debug(`${logPrefix} Usuário ${userToUnban} desbanido antes da criação do convite.`);
                        } else if (unbanResponse && !unbanResponse.ok) {
                            const desc = (unbanResponse.description || '').toLowerCase();
                            if (desc.includes("can't remove chat owner")) {
                                logger.debug(`${logPrefix} Tentativa de desbanir o proprietário do grupo ignorada.`);
                            } else {
                                logger.warn(`${logPrefix} Não foi possível desbanir usuário ${userToUnban}: ${unbanResponse.description}`);
                            }
                        }
                    } catch (unbanError) {
                        const message = (unbanError?.message || '').toLowerCase();
                        if (message.includes("can't remove chat owner")) {
                            logger.debug(`${logPrefix} Tentativa de desbanir o proprietário do grupo ignorada.`);
                        } else {
                            logger.warn(`${logPrefix} Erro ao tentar desbanir usuário ${userToUnban}:`, unbanError.message);
                        }
                    }
                    
                    const expireDate = actionData.expireMinutes 
                        ? Math.floor(Date.now() / 1000) + (actionData.expireMinutes * 60)
                        : undefined;
                    
                    const inviteNameRaw = (actionData.linkName || `Convite_${chatId}_${Date.now()}`).toString().trim();
                    const inviteName = inviteNameRaw ? inviteNameRaw.slice(0, 32) : `Convite_${Date.now()}`;

                    const invitePayload = {
                        chat_id: normalizedChatId,
                        name: inviteName,
                        member_limit: 1,
                        creates_join_request: false
                    };
                    
                    if (expireDate) {
                        invitePayload.expire_date = expireDate;
                    }
                    
                    const inviteResponse = await sendTelegramRequest(
                        botToken, 
                        'createChatInviteLink', 
                        invitePayload
                    );
                    
                    if (inviteResponse.ok) {
                        // Salvar link nas variáveis
                        variables.invite_link = inviteResponse.result.invite_link;
                        variables.invite_link_name = inviteResponse.result.name;
                        variables.invite_link_single_use = true;
                        variables.user_was_banned = false;
                        variables.banned_user_id = undefined;
                        
                        const buttonText = (actionData.buttonText || DEFAULT_INVITE_BUTTON_TEXT).trim() || DEFAULT_INVITE_BUTTON_TEXT;
                        const template = (actionData.messageText || actionData.text || DEFAULT_INVITE_MESSAGE).trim() || DEFAULT_INVITE_MESSAGE;
                        const messageText = await replaceVariables(template, variables);

                        const payload = {
                            chat_id: chatId,
                            text: messageText,
                            parse_mode: 'HTML',
                            reply_markup: {
                                inline_keyboard: [[{ text: buttonText, url: inviteResponse.result.invite_link }]]
                            }
                        };

                        const messageResponse = await sendTelegramRequest(botToken, 'sendMessage', payload);
                        if (messageResponse?.ok) {
                            await saveMessageToDb(sellerId, botId, messageResponse.result, 'bot');
                        } else {
                            throw new Error(messageResponse?.description || 'Falha ao enviar mensagem do convite.');
                        }
                        
                        logger.debug(`${logPrefix} Link de convite criado com sucesso: ${inviteResponse.result.invite_link}`);
                    } else {
                        throw new Error(`Falha ao criar link de convite: ${inviteResponse.description}`);
                    }
                } catch (error) {
                    logger.error(`${logPrefix} Erro ao criar link de convite:`, error.message);
                    throw error;
                }
                break;
                
            case 'action_remove_user_from_group':
                try {
                    logger.debug(`${logPrefix} Executando action_remove_user_from_group para chat ${chatId}`);
                    
                    const [bot] = await sqlTx`
                        SELECT telegram_supergroup_id 
                        FROM telegram_bots 
                        WHERE id = ${botId}
                    `;
                    
                    if (!bot?.telegram_supergroup_id) {
                        throw new Error('Supergrupo não configurado para este bot');
                    }
                    
                    const normalizedChatId = normalizeChatIdentifier(bot.telegram_supergroup_id);
                    if (!normalizedChatId) {
                        throw new Error('ID do supergrupo inválido para banimento');
                    }

                                        
                    const handleOwnerBanRestriction = () => {
                        logger.debug(`${logPrefix} Tentativa de banir o proprietário do grupo ignorada.`);
                        variables.user_was_banned = false;
                        variables.banned_user_id = undefined;
                    };
                    
                    // Usar o chat_id do usuário atual ou um ID específico
                    const userToRemove = actionData.userId || chatId;
                    const normalizedUserId = normalizeChatIdentifier(userToRemove);
                    
                    let banResponse;
                    try {
                        banResponse = await sendTelegramRequest(
                            botToken,
                            'banChatMember',
                            {
                                chat_id: normalizedChatId,
                                user_id: normalizedUserId,
                                revoke_messages: actionData.deleteMessages || false
                            }
                        );
                    } catch (banError) {
                        const errorDesc =
                            banError?.response?.data?.description ||
                            banError?.description ||
                            banError?.message ||
                            '';
                        if (errorDesc.toLowerCase().includes("can't remove chat owner")) {
                            handleOwnerBanRestriction();
                            break;
                        }
                        logger.error(`${logPrefix} Erro ao remover usuário do grupo:`, banError.message);
                        throw banError;
                    }

                    if (banResponse.ok) {
                        logger.debug(`${logPrefix} Usuário ${userToRemove} removido e banido do grupo`);
                        variables.user_was_banned = true;
                        variables.banned_user_id = userToRemove;
                        variables.last_ban_at = new Date().toISOString();

                        const linkToRevoke = actionData.inviteLink || variables.invite_link;
                        if (linkToRevoke) {
                            try {
                                const revokeResponse = await sendTelegramRequest(
                                    botToken,
                                    'revokeChatInviteLink',
                                    {
                                        chat_id: normalizedChatId,
                                        invite_link: linkToRevoke
                                    }
                                );
                                if (revokeResponse.ok) {
                                    logger.debug(`${logPrefix} Link de convite revogado após banimento: ${linkToRevoke}`);
                                    variables.invite_link_revoked = true;
                                    delete variables.invite_link;
                                    delete variables.invite_link_name;
                                } else {
                                    logger.warn(`${logPrefix} Falha ao revogar link ${linkToRevoke}: ${revokeResponse.description}`);
                                }
                            } catch (revokeError) {
                                logger.warn(`${logPrefix} Erro ao tentar revogar link ${linkToRevoke}:`, revokeError.message);
                            }
                        }
                        
                        if (actionData.sendMessage) {
                            const messageText = await replaceVariables(
                                actionData.messageText || 'Você foi removido do grupo.',
                                variables
                            );
                            await sendMessage(chatId, messageText, botToken, sellerId, botId, false, variables);
                        }
                    } else {
                        const desc =
                            (banResponse?.description ||
                                banResponse?.result?.description ||
                                '').
                                toLowerCase();
                        if (desc.includes("can't remove chat owner")) {
                            handleOwnerBanRestriction();
                        } else {
                            throw new Error(`Falha ao remover usuário: ${banResponse.description}`);
                        }
                    }
                } catch (error) {
                    const message = (error?.response?.data?.description ||
                        error?.description ||
                        error?.message ||
                        '').toLowerCase();
                    if (message.includes("can't remove chat owner")) {
                        handleOwnerBanRestriction();
                    } else {
                        logger.error(`${logPrefix} Erro ao remover usuário do grupo:`, error.message);
                        throw error;
                    }
                }
                break;

            default:
                logger.warn(`${logPrefix} Tipo de ação aninhada desconhecida: ${action.type}. Ignorando.`);
                break;
        }
        
    }

    // Se o loop terminar normalmente (sem 'return' condicional)
    return 'completed';
}
/**
 * [REATORADO] Processa o fluxo principal, navegando entre os nós.
 * Esta função agora lida apenas com a lógica de NAVEGAÇÃO.
 * Ela chama 'processActions' para EXECUTAR o conteúdo de cada nó.
 */
// Função helper para incrementar contador de execução de node
async function incrementNodeExecutionCount(flowId, nodeId) {
    if (!flowId || !nodeId) return; // Não rastreia se não tiver flow_id ou node_id
    
    try {
        // Busca o valor atual e incrementa
        const [current] = await sqlTx`
            SELECT COALESCE(node_execution_counts, '{}'::jsonb) as counts 
            FROM flows 
            WHERE id = ${flowId}
        `;
        
        if (!current) return;
        
        const currentCount = current.counts[nodeId] || 0;
        const newCount = parseInt(currentCount) + 1;
        
        // Usa jsonb_set com array de texto para o caminho
        await sqlTx`
            UPDATE flows 
            SET node_execution_counts = jsonb_set(
                COALESCE(node_execution_counts, '{}'::jsonb),
                ARRAY[${nodeId}],
                ${newCount}::text::jsonb
            )
            WHERE id = ${flowId}
        `;
    } catch (error) {
        logger.warn(`[Flow Engine] Erro ao incrementar contador de execução do node ${nodeId} no flow ${flowId}:`, error.message);
        // Não interrompe o fluxo se houver erro no rastreamento
    }
}

async function processFlow(chatId, botId, botToken, sellerId, startNodeId = null, initialVariables = {}, flowNodes = null, flowEdges = null) {
    const logPrefix = startNodeId ? '[WORKER]' : '[MAIN]';
    logger.debug(`${logPrefix} [Flow Engine] Iniciando processo para ${chatId}. Nó inicial: ${startNodeId || 'Padrão'}`);

    // ==========================================================
    // PASSO 1: CARREGAR VARIÁVEIS DO USUÁRIO E DO CLIQUE
    // ==========================================================
    let variables = { ...initialVariables };
    let currentFlowId = null; // Armazena o ID do fluxo atual para rastreamento

    const [user] = await sqlTx`
        SELECT first_name, last_name 
        FROM telegram_chats 
        WHERE chat_id = ${chatId} AND bot_id = ${botId} AND sender_type = 'user'
        ORDER BY created_at DESC LIMIT 1`;

    if (user) {
        variables.primeiro_nome = user.first_name || '';
        variables.nome_completo = `${user.first_name || ''} ${user.last_name || ''}`.trim();
    }

    if (variables.click_id) {
        const db_click_id = variables.click_id.startsWith('/start ') ? variables.click_id : `/start ${variables.click_id}`;
        const [click] = await sqlTx`SELECT city, state FROM clicks WHERE click_id = ${db_click_id}`;
        if (click) {
            variables.cidade = click.city || '';
            variables.estado = click.state || '';
        }
        else{
            variables.cidade = '';
            variables.estado = '';
        }
    }
    // ==========================================================
    // FIM DO PASSO 1
    // ==========================================================

    // Se os dados do fluxo foram fornecidos (ex: forward_flow), usa eles. Caso contrário, busca do banco.
    let nodes, edges;
    if (flowNodes && flowEdges) {
        // Usa os dados do fluxo fornecido (do forward_flow)
        nodes = flowNodes;
        edges = flowEdges;
        logger.debug(`${logPrefix} [Flow Engine] Usando dados do fluxo fornecido (${nodes.length} nós, ${edges.length} arestas).`);
    } else {
        // Busca o fluxo ativo do banco
        const [flow] = await sqlTx`
            SELECT * FROM flows 
            WHERE bot_id = ${botId} AND is_active = TRUE
            ORDER BY updated_at DESC LIMIT 1`;
        if (!flow || !flow.nodes) {
            logger.info(`${logPrefix} [Flow Engine] Nenhum fluxo ativo encontrado para o bot ID ${botId}.`);
            return;
        }

        currentFlowId = flow.id; // Armazena o ID do fluxo para rastreamento
        const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
        nodes = flowData.nodes || [];
        edges = flowData.edges || [];
    }

    let currentNodeId = startNodeId;
    const isStartCommand = initialVariables.click_id && initialVariables.click_id.startsWith('/start');

    if (!currentNodeId) {
        if (isStartCommand) {
            logger.debug(`${logPrefix} [Flow Engine] Comando /start detectado. Reiniciando fluxo.`);
            
            // Cancela tarefa de timeout pendente
            const [stateToCancel] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (stateToCancel && stateToCancel.scheduled_message_id) {
                try {
                    await qstashClient.messages.delete(stateToCancel.scheduled_message_id);
                    logger.debug(`[Flow Engine] Tarefa de timeout pendente ${stateToCancel.scheduled_message_id} cancelada.`);
                } catch (e) {
                    logger.warn(`[Flow Engine] Falha ao cancelar QStash msg ${stateToCancel.scheduled_message_id}:`, e.message);
                }
            }

            await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            const startNode = nodes.find(node => node.type === 'trigger');
            currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;

        } else {
            // Não é /start, verifica se está esperando resposta
            const [userState] = await sqlTx`SELECT * FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            if (userState && userState.waiting_for_input) {
                logger.debug(`${logPrefix} [Flow Engine] Usuário respondeu. Continuando do nó ${userState.current_node_id} (handle 'a').`);
                currentNodeId = findNextNode(userState.current_node_id, 'a', edges); // 'a' = Com Resposta
                
                // Carrega variáveis salvas no estado
                let parsedVariables = {};
                try {
                    parsedVariables = JSON.parse(userState.variables);
                } catch (e) { parsedVariables = userState.variables; }
                variables = { ...variables, ...parsedVariables }; // Mescla variáveis

            } else {
                // Nova conversa sem /start (ou estado expirado), reinicia
                logger.debug(`${logPrefix} [Flow Engine] Nova conversa. Iniciando do gatilho.`);
                await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                const startNode = nodes.find(node => node.type === 'trigger');
                currentNodeId = startNode ? findNextNode(startNode.id, 'a', edges) : null;
            }
        }
    }

    if (!currentNodeId) {
        logger.debug(`${logPrefix} [Flow Engine] Nenhum nó para processar. Fim do fluxo.`);
        await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        return;
    }

    // ==========================================================
    // PASSO 3: O NOVO LOOP DE NAVEGAÇÃO
    // ==========================================================
    let safetyLock = 0;
    while (currentNodeId && safetyLock < 20) {
        safetyLock++;
        let currentNode = nodes.find(node => node.id === currentNodeId);
        
        if (!currentNode) {
            logger.error(`${logPrefix} [Flow Engine] Erro: Nó ${currentNodeId} não encontrado.`);
            break;
        }

        logger.debug(`${logPrefix} [Flow Engine] Processando Nó: ${currentNode.id} (Tipo: ${currentNode.type})`);

        // Incrementa contador de execução do node (apenas se tiver flow_id)
        if (currentFlowId && currentNode.id !== 'start') {
            await incrementNodeExecutionCount(currentFlowId, currentNode.id);
        }

        // Salva o estado atual (não está esperando input... ainda)
        await sqlTx`
            INSERT INTO user_flow_states (chat_id, bot_id, current_node_id, variables, waiting_for_input, scheduled_message_id)
            VALUES (${chatId}, ${botId}, ${currentNodeId}, ${JSON.stringify(variables)}, false, NULL)
            ON CONFLICT (chat_id, bot_id)
            DO UPDATE SET 
                current_node_id = EXCLUDED.current_node_id, 
                variables = EXCLUDED.variables, 
                waiting_for_input = false, 
                scheduled_message_id = NULL;
        `;

        if (currentNode.type === 'trigger') {
            // Nó de 'trigger' é apenas um ponto de partida, executa ações aninhadas (se houver) e segue
            if (currentNode.data.actions && currentNode.data.actions.length > 0) {
                 await processActions(currentNode.data.actions, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`);
                 // Persiste variáveis caso as ações do gatilho tenham modificado algo (ex: action_pix no gatilho)
                 await sqlTx`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            }
            currentNodeId = findNextNode(currentNode.id, 'a', edges);
            continue;
        }

        if (currentNode.type === 'action') {
            // 1. Prepara as ações para execução
            const allActions = currentNode.data.actions || [];
            const willBeScheduled = currentNode.data.waitForReply === true;
            
            // Se o nó será agendado (waitForReply), separa ações de mídia
            let actionsToExecuteNow = allActions;
            let scheduledMediaActions = [];
            
            if (willBeScheduled) {
                const mediaTypes = ['image', 'video', 'audio'];
                scheduledMediaActions = allActions.filter(a => mediaTypes.includes(a.type));
                actionsToExecuteNow = allActions.filter(a => !mediaTypes.includes(a.type));
                
                if (scheduledMediaActions.length > 0) {
                    logger.debug(`${logPrefix} [Flow Engine] Nó será agendado. Identificadas ${scheduledMediaActions.length} ação(ões) de mídia.`);
                }
            }
            
            logger.debug(`${logPrefix} [Flow Engine] Processando nó 'action' com ${actionsToExecuteNow.length} ação(ões): ${actionsToExecuteNow.map(a => a.type).join(', ')}`);
            
            let actionResult;
            try {
                actionResult = await processActions(actionsToExecuteNow, chatId, botId, botToken, sellerId, variables, `[FlowNode ${currentNode.id}]`);
                
                // 2. Persiste as variáveis (caso 'action_pix' tenha atualizado 'last_transaction_id')
                await sqlTx`UPDATE user_flow_states SET variables = ${JSON.stringify(variables)} WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            } catch (actionError) {
                // Erro crítico durante execução de ação (ex: PIX não enviado por bot bloqueado)
                logger.error(`${logPrefix} [Flow Engine] Erro CRÍTICO ao processar ações do nó ${currentNode.id}:`, actionError.message);
                
                // Limpa o estado do usuário para evitar fluxo preso
                await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                logger.debug(`${logPrefix} [Flow Engine] Estado do usuário limpo devido a erro crítico.`);
                
                // Re-lança o erro para ser capturado pelo try-catch do webhook
                throw actionError;
            }

            // 3. Verifica se uma ação 'forward_flow' foi executada
            if (actionResult === 'flow_forwarded') {
                logger.debug(`${logPrefix} [Flow Engine] Fluxo encaminhado. Encerrando o fluxo atual.`);
                currentNodeId = null; // Para o loop atual
                break;
            }

            // 4. Verifica se o NÓ está configurado para 'waitForReply'
            if (currentNode.data.waitForReply) {
                // 4.1. Se houver mídias agendadas, envia ANTES de pausar
                if (scheduledMediaActions.length > 0) {
                    logger.debug(`${logPrefix} [Flow Engine] Enviando ${scheduledMediaActions.length} mídia(s) antes de aguardar resposta.`);
                    
                    try {
                        await processActions(
                            scheduledMediaActions,
                            chatId,
                            botId,
                            botToken,
                            sellerId,
                            variables,
                            `${logPrefix} [Scheduled Media]`
                        );
                        
                        logger.debug(`${logPrefix} [Flow Engine] Mídias enviadas antes de aguardar resposta.`);
                    } catch (mediaError) {
                        logger.error(`${logPrefix} [Flow Engine] Erro ao enviar mídias agendadas:`, mediaError);
                        // Continua mesmo se falhar
                    }
                }
                
                const noReplyNodeId = findNextNode(currentNode.id, 'b', edges); // 'b' = Sem Resposta
                const timeoutMinutes = currentNode.data.replyTimeout || 5;

                try {
                    // Agenda o worker de timeout com uma única chamada
                    const response = await qstashClient.publishJSON({
                        url: `${process.env.HOTTRACK_API_URL}/api/worker/process-timeout`,
                        body: { 
                            chat_id: chatId, 
                            bot_id: botId, 
                            target_node_id: noReplyNodeId,
                            variables: variables,
                            timestamp: Date.now() // 👈 ADICIONE ISSO para evitar deduplicação
                        },
                        delay: `${timeoutMinutes}m`,
                        contentBasedDeduplication: false, // 👈 MUDE DE true PARA false
                        method: "POST"
                    });
                    
                    // Salva o estado como "esperando" e armazena o ID da tarefa agendada
                    await sqlTx`
                        UPDATE user_flow_states 
                        SET waiting_for_input = true, scheduled_message_id = ${response.messageId} 
                        WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    
                    logger.debug(`${logPrefix} [Flow Engine] Fluxo pausado no nó ${currentNode.id}. Esperando ${timeoutMinutes} min. Tarefa QStash: ${response.messageId}`);
                
                } catch (error) {
                    logger.error(`${logPrefix} [Flow Engine] Erro CRÍTICO ao agendar timeout no QStash:`, error);
                }

                currentNodeId = null; // PARA o loop
                break;
            }
            
            // 5. Verifica se o resultado foi de um 'action_check_pix'
            if (actionResult === 'paid') {
                logger.debug(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pago. Seguindo handle 'a'.`);
                currentNodeId = findNextNode(currentNode.id, 'a', edges); // 'a' = Pago
                continue;
            }
            if (actionResult === 'pending') {
                logger.debug(`${logPrefix} [Flow Engine] Resultado do Nó: PIX Pendente. Seguindo handle 'b'.`);
                currentNodeId = findNextNode(currentNode.id, 'b', edges); // 'b' = Pendente
                continue;
            }
            
            // 6. Se nada acima aconteceu, é um nó de ação simples. Segue pelo handle 'a'.
            currentNodeId = findNextNode(currentNode.id, 'a', edges);
            continue;
        }


        // Tipo de nó desconhecido
        logger.warn(`${logPrefix} [Flow Engine] Tipo de nó desconhecido: ${currentNode.type}. Encerrando fluxo.`);
        currentNodeId = null;
    }
    // ==========================================================
    // FIM DO PASSO 3
    // ==========================================================

    // Limpeza final: Se o fluxo terminou (não está esperando input), limpa o estado.
    if (!currentNodeId) {
        const [state] = await sqlTx`SELECT 1 FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId} AND waiting_for_input = true`;
        if (!state) {
            logger.debug(`${logPrefix} [Flow Engine] Fim do fluxo para ${chatId}. Limpando estado.`);
            await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        } else {
            logger.debug(`${logPrefix} [Flow Engine] Fluxo pausado (waiting for input). Estado preservado para ${chatId}.`);
        }
    }
}

app.post('/api/webhook/telegram/:botId', webhookRateLimitMiddleware, async (req, res) => {
    const { botId } = req.params;
    // CORREÇÃO 1: Extrai o objeto 'message' do corpo da requisição logo no início.
    const { message } = req.body; 
    
    // Responde imediatamente ao Telegram para evitar timeouts.
    res.sendStatus(200);

    try {
        // Validação mais robusta da estrutura da mensagem
        if (!message) {
            logger.debug('[Webhook] Requisição ignorada: objeto message ausente.');
            return;
        }
        
        if (!message.chat) {
            logger.debug('[Webhook] Requisição ignorada: objeto chat ausente na mensagem.');
            return;
        }
        
        if (!message.chat.id) {
            logger.debug('[Webhook] Requisição ignorada: chat.id ausente na mensagem.');
            return;
        }
        
        // Verifica se é uma mensagem válida (não callback_query, etc.)
        if (message.message_id === undefined) {
            logger.debug('[Webhook] Requisição ignorada: message_id ausente (pode ser callback_query ou outro tipo).');
            return;
        }
        const chatId = message.chat.id;
        const text = message.text || '';
        const isStartCommand = text.startsWith('/start');

        const [bot] = await sqlTx`SELECT seller_id, bot_token FROM telegram_bots WHERE id = ${botId}`;
        if (!bot) {
            logger.warn(`[Webhook] Bot ID ${botId} não encontrado.`);
            return;
        }
        const { seller_id: sellerId, bot_token: botToken } = bot;

        // Salva a mensagem do usuário (seja /start ou resposta)
        await saveMessageToDb(sellerId, botId, message, 'user');

        // PRIORIDADE 1: Comando /start reinicia tudo.
        if (isStartCommand) {
     
                  const parts = text.split(' ');
                 
                  // VERIFICA SE É UM /start COM ID (ex: /start lead... ou /start bot_org...)
                  if (parts.length > 1 && parts[1].trim() !== '') {
                    const clickIdValue = text;
                    logger.debug(`[Webhook] Click ID de campanha detectado: ${clickIdValue}. Reiniciando fluxo para o chat ${chatId}.`);
            
                    // Cancela qualquer tarefa pendente e deleta o estado antigo.
                    const [existingState] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    if (existingState && existingState.scheduled_message_id) {
                      try {
                        await qstashClient.messages.delete(existingState.scheduled_message_id);
                        logger.debug(`[Webhook] Tarefa de timeout antiga cancelada devido ao /start.`);
                      } catch (e) { /* Ignora erros se a tarefa já foi executada */ }
                    }
                    await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                   
                    // Inicia o fluxo do zero.
                    await processFlow(chatId, botId, botToken, sellerId, null, { click_id: clickIdValue });
                 
                  } else {
                    // É um /start orgânico (sozinho), que você quer ignorar.
                    logger.debug(`[Webhook] Comando /start (orgânico) recebido para o chat ${chatId}. Nenhuma ação tomada.`);
                    // Não faz nada e apenas termina a execução.
                  }
                 
                  return; // Finaliza a execução (seja por iniciar o fluxo ou por ignorar)
                }

        // PRIORIDADE 2: Se não for /start, trata como uma resposta normal.
        const [userState] = await sqlTx`SELECT current_node_id, variables, scheduled_message_id, waiting_for_input FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        
        // LOG: Estado encontrado
        logger.debug(`[Webhook] Estado encontrado para ${chatId}:`, userState ? {
            current_node_id: userState.current_node_id,
            waiting_for_input: userState.waiting_for_input,
            has_scheduled_message: !!userState.scheduled_message_id,
            variables_type: typeof userState.variables
        } : 'NENHUM');
        
        if (userState && userState.waiting_for_input) {
            logger.debug(`[Webhook] Usuário ${chatId} respondeu. Processando continuação do fluxo...`);
            
            // Cancela o timeout, pois o usuário respondeu.
            if (userState.scheduled_message_id) {
                 try {
                    await qstashClient.messages.delete(userState.scheduled_message_id);
                    logger.debug(`[Webhook] Tarefa de timeout cancelada pela resposta do usuário.`);
                } catch (e) { 
                    logger.warn(`[Webhook] Erro ao cancelar timeout (pode já ter sido executado):`, e.message);
                }
            }

            // IMPORTANTE: Limpa o estado de espera ANTES de continuar o fluxo
            await sqlTx`
                UPDATE user_flow_states 
                SET waiting_for_input = false, scheduled_message_id = NULL 
                WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
            
            logger.debug(`[Webhook] Estado atualizado: waiting_for_input = false`);

            // Continua o fluxo a partir do próximo nó.
            const [flow] = await sqlTx`SELECT nodes FROM flows WHERE bot_id = ${botId} AND is_active = TRUE`;
            logger.debug(`[Webhook] Flow encontrado:`, flow ? 'SIM' : 'NÃO');
            
            if (flow && flow.nodes) {
                logger.debug(`[Webhook] Flow.nodes tipo:`, typeof flow.nodes);
                
                // IMPORTANTE: Parse correto do flow.nodes (pode ser string ou objeto)
                const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;
                const nodes = flowData.nodes || [];
                const edges = flowData.edges || [];
                logger.debug(`[Webhook] Nodes: ${nodes.length}, Edges: ${edges.length}`);
                
                const nextNodeId = findNextNode(userState.current_node_id, 'a', edges);
                logger.debug(`[Webhook] Próximo nó (handle 'a'):`, nextNodeId || 'NENHUM');
                logger.debug(`[Webhook] Current node:`, userState.current_node_id);
                logger.debug(`[Webhook] Edges disponíveis:`, edges.filter(e => e.source === userState.current_node_id));

                if (nextNodeId) {
                    // Parse das variáveis se necessário
                    let parsedVariables = userState.variables;
                    if (typeof parsedVariables === 'string') {
                        try {
                            parsedVariables = JSON.parse(parsedVariables);
                            logger.debug(`[Webhook] Variáveis parseadas com sucesso:`, parsedVariables);
                        } catch (e) {
                            logger.error('[Webhook] Erro ao fazer parse das variáveis:', e);
                            parsedVariables = {};
                        }
                    } else {
                        logger.debug(`[Webhook] Variáveis já são objeto:`, parsedVariables);
                    }
                    
                    logger.debug(`[Webhook] Chamando processFlow com nextNodeId: ${nextNodeId}`);
                    
                    try {
                        await processFlow(chatId, botId, botToken, sellerId, nextNodeId, parsedVariables);
                        logger.debug(`[Webhook] Fluxo continuado com sucesso para ${chatId}`);
                    } catch (error) {
                        logger.error(`[Webhook] Erro ao continuar fluxo para ${chatId}:`, error);
                        logger.error(`[Webhook] Stack trace:`, error.stack);
                        // Limpa estado corrompido
                        await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                    }
                } else {
                    logger.debug(`[Webhook] Fim do fluxo após resposta do usuário (sem próximo nó).`);
                    await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
                }
            } else {
                 logger.error(`[Webhook] Fluxo para o bot ${botId} não encontrado ao tentar continuar.`);
            }
        } else {
            logger.debug(`[Webhook] Mensagem de ${chatId} ignorada (não é /start e não há fluxo esperando resposta).`);
        }
    
    } catch (error) {
        // Este catch agora só pegará erros realmente inesperados no fluxo principal.
        console.error("Erro GERAL e INESPERADO ao processar webhook do Telegram:", error);
    }
});


app.get('/api/dispatches', authenticateJwt, async (req, res) => {
    try {
        const dispatches = await sqlTx`SELECT * FROM mass_sends WHERE seller_id = ${req.user.id} ORDER BY sent_at DESC;`;
        res.status(200).json(dispatches);
    } catch (error) {
        console.error("Erro ao buscar histórico de disparos:", error);
        res.status(500).json({ message: 'Erro ao buscar histórico.' });
    }
});
app.get('/api/dispatches/:id', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    try {
        // Adicionar paginação para evitar sobrecarga
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 100, 1000); // Máximo 1000 por página
        const offset = (page - 1) * limit;
        
        const details = await sqlTx`
            SELECT d.*, u.first_name, u.username
            FROM mass_send_details d
            LEFT JOIN telegram_chats u ON d.chat_id = u.chat_id
            WHERE d.mass_send_id = ${id}
            ORDER BY d.sent_at
            LIMIT ${limit} OFFSET ${offset};
        `;
        
        // Buscar total para paginação
        const [countResult] = await sqlTx`
            SELECT COUNT(*) as total
            FROM mass_send_details
            WHERE mass_send_id = ${id}
        `;
        const total = parseInt(countResult.total);
        
        res.status(200).json({
            details,
            pagination: {
                page,
                limit,
                total,
                totalPages: Math.ceil(total / limit)
            }
        });
    } catch (error) {
        console.error("Erro ao buscar detalhes do disparo:", error);
        res.status(500).json({ message: 'Erro ao buscar detalhes.' });
    }
});
app.post('/api/bots/mass-send', authenticateJwt, async (req, res) => {
      const sellerId = req.user.id;
      const { botIds, disparoFlowId, campaignName } = req.body;
    
      if (!botIds || !Array.isArray(botIds) || botIds.length === 0 || !disparoFlowId || !campaignName) {
        return res.status(400).json({ message: 'Nome da campanha, IDs dos Bots e ID do fluxo de disparo são obrigatórios.' });
      }
    
        let historyId; 
    
      try {
            // 1. Buscar o fluxo de disparo
        const [disparoFlow] = await sqlWithRetry(
            'SELECT * FROM disparo_flows WHERE id = $1 AND seller_id = $2',
            [disparoFlowId, sellerId]
        );
        
        if (!disparoFlow) {
            return res.status(404).json({ message: 'Fluxo de disparo não encontrado.' });
        }
        
        // Parse do fluxo
        const flowData = typeof disparoFlow.nodes === 'string' ? JSON.parse(disparoFlow.nodes) : disparoFlow.nodes;
        const flowNodes = flowData.nodes || [];
        const flowEdges = flowData.edges || [];
        
        if (flowNodes.length === 0) {
            return res.status(400).json({ message: 'O fluxo de disparo está vazio. Adicione pelo menos uma ação.' });
        }
        
        // Verificar se há pelo menos um nó de ação (não trigger)
        const actionNodes = flowNodes.filter(n => n.type === 'action');
        if (actionNodes.length === 0) {
            return res.status(400).json({ message: 'O fluxo de disparo deve ter pelo menos um nó de ação.' });
        }
        
            // 2. Verificar quais bots são válidos primeiro (otimização)
        const validBotIds = [];
        for (const botId of botIds) {
            const [botCheck] = await sqlWithRetry(
                'SELECT id FROM telegram_bots WHERE id = $1 AND seller_id = $2',
                [botId, sellerId]
            );
            if (botCheck) {
                validBotIds.push(botId);
            }
        }
        
        if (validBotIds.length === 0) {
            return res.status(404).json({ message: 'Nenhum bot válido encontrado.' });
        }
        
        // 3. Buscar todos os contatos únicos em uma única query (otimização)
        const allContacts = new Map();
        const contacts = await sqlWithRetry(
            sqlTx`SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username, click_id, bot_id
                  FROM telegram_chats 
                  WHERE bot_id = ANY(${validBotIds}) AND seller_id = ${sellerId}
                  ORDER BY chat_id, created_at DESC`
        );
        
        contacts.forEach(c => {
            if (!allContacts.has(c.chat_id)) {
                allContacts.set(c.chat_id, { 
                    chat_id: c.chat_id,
                    first_name: c.first_name,
                    last_name: c.last_name,
                    username: c.username,
                    click_id: c.click_id,
                    bot_id_source: c.bot_id 
                });
            }
        });
        const uniqueContacts = Array.from(allContacts.values());
    
        if (uniqueContacts.length === 0) {
          return res.status(404).json({ message: 'Nenhum contato encontrado para os bots selecionados.' });
        }
        
        // Limitar número máximo de contatos para evitar sobrecarga
        const MAX_CONTACTS_PER_CAMPAIGN = 10000; // Máximo 10k contatos por campanha
        if (uniqueContacts.length > MAX_CONTACTS_PER_CAMPAIGN) {
            return res.status(400).json({ 
                message: `Número máximo de contatos excedido. Máximo permitido: ${MAX_CONTACTS_PER_CAMPAIGN}. Encontrados: ${uniqueContacts.length}` 
            });
        }
    
            // --- MUDANÇA PRINCIPAL AQUI ---
            // 4. Calcular o total de trabalhos (1 trabalho por contato - o fluxo completo será processado)
            const total_jobs_to_queue = uniqueContacts.length;
            if (total_jobs_to_queue === 0) {
                return res.status(400).json({ message: 'Nenhum trabalho a ser agendado (0 contatos).' });
            }
    
        // 5. Criar o registro mestre da campanha com os totais corretos
        const [history] = await sqlWithRetry(
          sqlTx`INSERT INTO disparo_history (
                    seller_id, campaign_name, bot_ids, disparo_flow_id, 
                    status, total_sent, failure_count, 
                    total_jobs, processed_jobs
                   ) 
                   VALUES (
                    ${sellerId}, ${campaignName}, ${JSON.stringify(botIds)}, ${disparoFlowId}, 
                    'PENDING', ${uniqueContacts.length}, 0, 
                    ${total_jobs_to_queue}, 0
                   ) 
                   RETURNING id`
        );
        historyId = history.id;
            // --- FIM DA MUDANÇA ---
        
        // 5. Retornar resposta HTTP imediatamente e processar em background
        res.status(202).json({ message: `Disparo "${campaignName}" para ${uniqueContacts.length} contatos agendado com sucesso! O processo ocorrerá em segundo plano.` });
        
        // 6. Processar publicação ao QStash em background (não bloqueia resposta HTTP)
        (async () => {
            try {
                let messageCounter = 0;
                const delayBetweenMessages = 1; // 1 segundo de atraso entre cada contato
                const qstashPromises = [];
                
                // Processar contatos em chunks para evitar sobrecarga de memória
                const CONTACTS_CHUNK_SIZE = 500; // Processar 500 contatos por vez
                const contactChunks = [];
                for (let i = 0; i < uniqueContacts.length; i += CONTACTS_CHUNK_SIZE) {
                    contactChunks.push(uniqueContacts.slice(i, i + CONTACTS_CHUNK_SIZE));
                }
                
                console.log(`[DISPARO] Processando ${uniqueContacts.length} contatos em ${contactChunks.length} chunks de ${CONTACTS_CHUNK_SIZE}`);
        
                // Encontrar o trigger (nó inicial do disparo)
                let startNodeId = null;
                const triggerNode = flowNodes.find(node => node.type === 'trigger');
                
                if (triggerNode) {
                    // Se tem trigger, começar do trigger (ele passará para o próximo nó automaticamente)
                    startNodeId = triggerNode.id;
                } else {
                    // Fallback: procurar primeiro nó de ação (compatibilidade com fluxos antigos)
                    const actionNode = flowNodes.find(node => node.type === 'action');
                    if (actionNode) {
                        startNodeId = actionNode.id;
                    }
                }
                
                if (!startNodeId) {
                    console.error('[DISPARO] Nenhum nó inicial encontrado no fluxo!');
                    await sqlWithRetry('UPDATE disparo_history SET status = $1 WHERE id = $2', ['FAILED', historyId]);
                    return;
                }
        
                for (const contactChunk of contactChunks) {
                    for (const contact of contactChunk) {
                        const userVariables = {
                            primeiro_nome: contact.first_name || '',
                            nome_completo: `${contact.first_name || ''} ${contact.last_name || ''}`.trim(),
                            click_id: contact.click_id ? contact.click_id.replace('/start ', '') : null
                        };
                        
                        const payload = {
                            history_id: historyId,
                            chat_id: contact.chat_id,
                            bot_id: contact.bot_id_source,
                            flow_nodes: JSON.stringify(flowNodes),
                            flow_edges: JSON.stringify(flowEdges),
                            start_node_id: startNodeId,
                            variables_json: JSON.stringify(userVariables)
                        };
            
                        const totalDelaySeconds = messageCounter * delayBetweenMessages;
            
                        qstashPromises.push(
                            qstashClient.publishJSON({
                                url: `${process.env.HOTTRACK_API_URL}/api/worker/process-disparo`, 
                                body: payload,
                                delay: `${totalDelaySeconds}s`, 
                                retries: 2,
                                // Limitar concorrência no QStash para evitar sobrecarga
                                headers: {
                                    'Upstash-Concurrency': '5' // Máximo 5 requisições simultâneas
                                }
                            })
                        );
                       
                        messageCounter++; 
                    }
                    
                    // Processar promises deste chunk antes de continuar para o próximo
                    // Isso evita acumular milhões de promises em memória
                    if (qstashPromises.length > 0) {
                        console.log(`[DISPARO] Processando chunk com ${qstashPromises.length} promises...`);
                        await publishQStashInBatches(qstashPromises, 10, 500);
                        qstashPromises.length = 0; // Limpar array para liberar memória
                    }
                }
        
                // Processar promises restantes (se houver)
                if (qstashPromises.length > 0) {
                    await publishQStashInBatches(qstashPromises, 10, 500);
                }
        
                // Atualizar o status da campanha para "RUNNING"
                await sqlWithRetry(
                    sqlTx`UPDATE disparo_history SET status = 'RUNNING' WHERE id = ${historyId}`
                );
            } catch (bgError) {
                console.error("Erro no processamento em background do disparo:", bgError);
                // Atualizar status para erro se possível
                try {
                    await sqlWithRetry(
                        sqlTx`UPDATE disparo_history SET status = 'FAILED' WHERE id = ${historyId}`
                    );
                } catch (updateError) {
                    console.error("Erro ao atualizar status para FAILED:", updateError);
                }
            }
        })();
    
      } catch (error) {
        console.error("Erro crítico no agendamento do disparo:", error);
        if(historyId) {
                await sqlWithRetry('DELETE FROM disparo_history WHERE id = $1', [historyId]).catch(e => console.error("Falha ao limpar histórico órfão:", e));
        }
        if (!res.headersSent) {
          res.status(500).json({ message: 'Erro interno ao agendar o disparo.' });
        }
      }
    });

//          WEBHOOKS DE PROVEDORES DE PAGAMENTO PIX
// ==========================================================

app.post('/api/webhook/pushinpay', async (req, res) => {
    const { id, status, payer_name, payer_national_registration } = req.body; 
    console.log(`[Webhook PushinPay] Recebido webhook - ID: ${id}, Status: ${status}`);
    
    const normalized = String(status || '').toLowerCase();
    const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
    const canceledStatuses = new Set(['canceled', 'cancelled', 'expired', 'failed']);
    
    // Processar PIX pago
    if (paidStatuses.has(normalized)) {
      try {
        const [tx] = await sqlTx`
                  SELECT pt.id, pt.status, pt.provider_transaction_id, pt.meta_event_id, c.seller_id 
                  FROM pix_transactions pt 
                  JOIN clicks c ON pt.click_id_internal = c.id 
                  WHERE LOWER(pt.provider_transaction_id) = LOWER(${id}) AND pt.provider = 'pushinpay'
              `;
        
        if (!tx) {
            console.error(`[Webhook PushinPay] ERRO: Transação não encontrada! provider_transaction_id='${id}', provider='pushinpay'`);
            const countResult = await sqlTx`SELECT COUNT(*) as total FROM pix_transactions WHERE provider = 'pushinpay'`;
            console.error(`[Webhook PushinPay] Total de transações PushinPay no banco: ${countResult[0].total}`);
            return res.sendStatus(200);
        }
        
        // Se já está paga, verificar se eventos foram enviados
        if (tx.status === 'paid') {
            // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
            if (!tx.meta_event_id) {
                console.log(`[Webhook PushinPay] Transação ${id} (interna: ${tx.id}) está paga mas sem meta_event_id. Tentando enviar eventos...`);
                await handleSuccessfulPayment(tx.id, { name: payer_name, document: payer_national_registration });
            } else {
                console.log(`[Webhook PushinPay] Transação ${id} (interna: ${tx.id}) já processada e eventos enviados. Ignorando webhook duplicado.`);
            }
            return res.sendStatus(200);
        }
        
        console.log(`[Webhook PushinPay] Transação ${id} (interna: ${tx.id}) encontrada. Status atual: '${tx.status}'. Processando pagamento...`);
        
        await handleSuccessfulPayment(tx.id, { name: payer_name, document: payer_national_registration });
        console.log(`[Webhook PushinPay] ✓ Transação ${id} (interna: ${tx.id}) processada com sucesso!`);
        
      } catch (error) { 
              console.error(`[Webhook PushinPay] ERRO CRÍTICO:`, error.response?.data || error.message);
              console.error(`[Webhook PushinPay] Stack:`, error.stack);
          }
    } 
    // Processar PIX cancelado/expirado
    else if (canceledStatuses.has(normalized)) {
        try {
            console.log(`[Webhook PushinPay] PIX cancelado/expirado - ID: ${id}, Status: ${status}`);
            
            const [tx] = await sqlTx`
                UPDATE pix_transactions 
                SET status = 'expired', updated_at = NOW() 
                WHERE LOWER(provider_transaction_id) = LOWER(${id}) AND provider = 'pushinpay' AND status = 'pending'
                RETURNING id, status`;
            
            if (tx) {
                console.log(`[Webhook PushinPay] Transação ${id} (interna: ${tx.id}) marcada como expirada.`);
            }
        } catch (error) {
            console.error(`[Webhook PushinPay] Erro ao processar cancelamento:`, error.message);
        }
    } 
    else {
        console.log(`[Webhook PushinPay] Status '${status}' não processado. Ignorando.`);
    }
    res.sendStatus(200);
  });

app.post('/api/webhook/pixup', async (req, res) => {
    // Responde imediatamente ao Pixup para evitar timeouts
    res.sendStatus(200);

    try {
        // O webhook do Pixup envia os dados dentro de requestBody
        const requestBody = req.body?.requestBody || req.body;
        
        if (!requestBody) {
            console.warn('[Webhook Pixup] Payload sem requestBody:', JSON.stringify(req.body));
            return;
        }

        const { transactionId, status, external_id, creditParty } = requestBody;
        
        if (!transactionId && !external_id) {
            console.warn('[Webhook Pixup] Payload sem identificador de pagamento:', JSON.stringify(requestBody));
            return;
        }

        const identifier = transactionId || external_id;
        const normalized = String(status || '').toUpperCase();
        const paidStatuses = new Set(['PAID', 'CONFIRMED', 'COMPLETED', 'APPROVED', 'SUCCESS']);
        const canceledStatuses = new Set(['EXPIRED', 'CANCELLED', 'CANCELED', 'FAILED', 'REJECTED']);

        // Processar PIX pago
        if (paidStatuses.has(normalized)) {
            try {
                const [tx] = await sqlTx`
                    SELECT pt.id, pt.status, pt.provider_transaction_id, pt.meta_event_id, c.seller_id 
                    FROM pix_transactions pt 
                    JOIN clicks c ON pt.click_id_internal = c.id 
                    WHERE (LOWER(pt.provider_transaction_id) = LOWER(${identifier}) OR pt.provider_transaction_id = ${external_id}) AND pt.provider = 'pixup'
                `;

                if (!tx) {
                    console.error(`[Webhook Pixup] Transação não encontrada para ID ${identifier}.`);
                    return;
                }

                // creditParty contém os dados do pagador (quem pagou)
                // Documentação: creditParty { name, email, taxId }
                const payerName = creditParty?.name || '';
                const payerDocument = creditParty?.taxId || '';
                const payerEmail = creditParty?.email || '';
                const customerData = { 
                    name: payerName, 
                    document: payerDocument,
                    email: payerEmail 
                };

                // Se já está paga, verificar se eventos foram enviados
                if (tx.status === 'paid') {
                    // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                    if (!tx.meta_event_id) {
                        console.log(`[Webhook Pixup] Transação ${tx.id} está paga mas sem meta_event_id. Tentando enviar eventos...`);
                        await handleSuccessfulPayment(tx.id, customerData);
                    } else {
                        console.log(`[Webhook Pixup] Transação ${tx.id} já está paga e eventos enviados. Ignorando duplicata.`);
                    }
                    return;
                }

                console.log(`[Webhook Pixup] Processando transação ${identifier} (interna: ${tx.id}) com status '${normalized}'.`);
                
                await handleSuccessfulPayment(tx.id, customerData);
                console.log(`[Webhook Pixup] ✓ Transação ${identifier} (interna: ${tx.id}) processada com sucesso!`);
            } catch (error) {
                console.error('[Webhook Pixup] Erro ao processar pagamento:', error);
            }
        }
        // Processar PIX cancelado/expirado
        else if (canceledStatuses.has(normalized)) {
            try {
                console.log(`[Webhook Pixup] PIX cancelado/expirado - ID: ${identifier}, Status: ${status}`);
                
                const [tx] = await sqlTx`
                    UPDATE pix_transactions 
                    SET status = 'expired', updated_at = NOW() 
                    WHERE (LOWER(provider_transaction_id) = LOWER(${identifier}) OR provider_transaction_id = ${external_id}) 
                      AND provider = 'pixup' 
                      AND status = 'pending'
                    RETURNING id, status
                `;
                
                if (tx) {
                    console.log(`[Webhook Pixup] Transação ${identifier} (interna: ${tx.id}) marcada como expirada.`);
                }
            } catch (error) {
                console.error('[Webhook Pixup] Erro ao marcar transação como expirada:', error.message);
            }
        } else {
            console.log(`[Webhook Pixup] Status '${normalized}' não tratado. Payload:`, JSON.stringify(requestBody));
        }
    } catch (error) {
        console.error('[Webhook Pixup] Erro inesperado ao processar payload:', error);
    }
});

app.post('/api/webhook/cnpay', async (req, res) => {

  const transactionData = req.body.transaction;
  const customer = req.body.client;

  if (!transactionData || !transactionData.status) {
    console.log("[Webhook CNPay] Webhook ignorado: objeto 'transaction' ou 'status' ausente.");
    return res.sendStatus(200);
  }

  const { id: transactionId, status } = transactionData;
  const normalized = String(status || '').toLowerCase();
  const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
  
  if (paidStatuses.has(normalized)) {
    try {
      console.log(`[Webhook CNPay] Processando pagamento para transactionId: ${transactionId}`);
            
      const [tx] = await sqlTx`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'cnpay'`;
      
      if (!tx) {
          console.error(`[Webhook CNPay] ERRO: Transação não encontrada! provider_transaction_id='${transactionId}', provider='cnpay'`);
          const countResult = await sqlTx`SELECT COUNT(*) as total FROM pix_transactions WHERE provider = 'cnpay'`;
          console.error(`[Webhook CNPay] Total de transações CNPay no banco: ${countResult[0].total}`);
          return res.sendStatus(200);
      }
      
      // Se já está paga, verificar se eventos foram enviados
      if (tx.status === 'paid') {
        // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
        if (!tx.meta_event_id) {
          console.log(`[Webhook CNPay] Transação ${tx.id} está paga mas sem meta_event_id. Tentando enviar eventos...`);
          await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
        } else {
          console.log(`[Webhook CNPay] Transação ${tx.id} já está marcada como 'paga' e eventos enviados. Ignorando webhook duplicado.`);
        }
        return res.sendStatus(200);
      }
      
      console.log(`[Webhook CNPay] Transação ${tx.id} encontrada. Status atual: '${tx.status}'. Atualizando para PAGO...`);
      await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
      console.log(`[Webhook CNPay] ✓ Transação ${transactionId} (interna: ${tx.id}) processada com sucesso!`);
      
    } catch (error) { 
      console.error(`[Webhook CNPay] ERRO CRÍTICO:`, error);
      console.error(`[Webhook CNPay] Stack:`, error.stack);
    }
  } else {
      console.log(`[Webhook CNPay] Status '${status}' não é considerado pago. Ignorando.`);
  }
  res.sendStatus(200);
});

app.post('/api/webhook/oasyfy', async (req, res) => {
    
    const transactionData = req.body.transaction;
    const customer = req.body.client;
    
    if (!transactionData || !transactionData.status) {
        console.log("[Webhook Oasy.fy] Webhook ignorado: objeto 'transaction' ou 'status' ausente.");
        return res.sendStatus(200);
    }
    
    const { id: transactionId, status } = transactionData;
    const normalized = String(status || '').toLowerCase();
    const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
    
    if (paidStatuses.has(normalized)) {
        try {
            console.log(`[Webhook Oasy.fy] Processando pagamento para transactionId: ${transactionId}`);
            
            const [tx] = await sqlTx`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'oasyfy'`;
            
            if (!tx) {
                console.error(`[Webhook Oasy.fy] ERRO: Transação não encontrada! provider_transaction_id='${transactionId}', provider='oasyfy'`);
                const countResult = await sqlTx`SELECT COUNT(*) as total FROM pix_transactions WHERE provider = 'oasyfy'`;
                console.error(`[Webhook Oasy.fy] Total de transações Oasyfy no banco: ${countResult[0].total}`);
                return res.sendStatus(200);
            }
            
            // Se já está paga, verificar se eventos foram enviados
            if (tx.status === 'paid') {
                // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                if (!tx.meta_event_id) {
                    console.log(`[Webhook Oasy.fy] Transação ${transactionId} (interna: ${tx.id}) está paga mas sem meta_event_id. Tentando enviar eventos...`);
                    await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
                } else {
                    console.log(`[Webhook Oasy.fy] Transação ${transactionId} (interna: ${tx.id}) já está marcada como 'paid' e eventos enviados. Ignorando webhook duplicado.`);
                }
                return res.sendStatus(200);
            }
            
            console.log(`[Webhook Oasy.fy] Transação encontrada. ID interno: ${tx.id}, Status atual: '${tx.status}'. Processando...`);
            await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.cpf });
            console.log(`[Webhook Oasy.fy] ✓ Transação ${transactionId} (interna: ${tx.id}) processada com sucesso!`);
            
        } catch (error) { 
            console.error(`[Webhook Oasy.fy] ERRO CRÍTICO:`, error);
            console.error(`[Webhook Oasy.fy] Stack:`, error.stack);
        }
    } else {
        console.log(`[Webhook Oasy.fy] Recebido webhook com status '${status}', não identificado como pago. Ignorando.`);
    }
    res.sendStatus(200);
});

app.post('/api/webhook/wiinpay', async (req, res) => {
    try {
        const parsed = parseWiinpayPayment(req.body || {});
        if (!parsed.id) {
            console.warn('[Webhook WiinPay] Payload sem identificador de pagamento:', JSON.stringify(req.body));
            return res.sendStatus(200);
        }

        const normalizedStatus = String(parsed.status || '').toLowerCase();
        const paidStatuses = new Set(['paid', 'completed', 'approved', 'success', 'received']);
        const canceledStatuses = new Set(['canceled', 'cancelled', 'expired', 'failed', 'refused']);

        if (paidStatuses.has(normalizedStatus)) {
            try {
                const [tx] = await sqlTx`
                    SELECT * FROM pix_transactions 
                    WHERE LOWER(provider_transaction_id) = LOWER(${parsed.id}) AND provider = 'wiinpay'
                `;

                if (!tx) {
                    console.error(`[Webhook WiinPay] Transação não encontrada para ID ${parsed.id}.`);
                    return res.sendStatus(200);
                }

                // Se já está paga, verificar se eventos foram enviados
                if (tx.status === 'paid') {
                    // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                    if (!tx.meta_event_id) {
                        console.log(`[Webhook WiinPay] Transação ${tx.id} está paga mas sem meta_event_id. Tentando enviar eventos...`);
                        await handleSuccessfulPayment(tx.id, parsed.customer || {});
                    } else {
                        console.log(`[Webhook WiinPay] Transação ${tx.id} já está paga e eventos enviados. Ignorando duplicata.`);
                    }
                    return res.sendStatus(200);
                }

                console.log(`[Webhook WiinPay] Processando transação ${parsed.id} (interna: ${tx.id}) com status '${normalizedStatus}'.`);
                await handleSuccessfulPayment(tx.id, parsed.customer || {});
            } catch (error) {
                console.error('[Webhook WiinPay] Erro ao processar pagamento:', error);
            }
        } else if (canceledStatuses.has(normalizedStatus)) {
            try {
                const [tx] = await sqlTx`
                    UPDATE pix_transactions 
                    SET status = 'expired', updated_at = NOW() 
                    WHERE LOWER(provider_transaction_id) = LOWER(${parsed.id}) 
                      AND provider = 'wiinpay' 
                      AND status = 'pending'
                    RETURNING id
                `;
                if (tx) {
                    console.log(`[Webhook WiinPay] Transação ${parsed.id} marcada como expirada (interna: ${tx.id}).`);
                }
            } catch (error) {
                console.error('[Webhook WiinPay] Erro ao marcar transação como expirada:', error.message);
            }
        } else {
            console.log(`[Webhook WiinPay] Status '${normalizedStatus}' não tratado. Payload:`, JSON.stringify(req.body));
        }
    } catch (error) {
        console.error('[Webhook WiinPay] Erro inesperado ao processar payload:', error);
    }
    res.sendStatus(200);
});
async function sendEventToUtmify(status, clickData, pixData, sellerData, customerData, productData) {
    console.log(`[Utmify] Iniciando envio de evento '${status}' para o clique ID: ${clickData.id}`);
    try {
        let integrationId = null;

        if (clickData.pressel_id) {
            console.log(`[Utmify] Clique originado da Pressel ID: ${clickData.pressel_id}`);
            const [pressel] = await sqlTx`SELECT utmify_integration_id FROM pressels WHERE id = ${clickData.pressel_id}`;
            if (pressel) {
                integrationId = pressel.utmify_integration_id;
            }
        } else if (clickData.checkout_id) {
            console.log(`[Utmify] Clique originado do Checkout ID: ${clickData.checkout_id}. Lógica de associação não implementada para checkouts.`);
        }

        if (!integrationId) {
            console.log(`[Utmify] Nenhuma conta Utmify vinculada à origem do clique ${clickData.id}. Abortando envio.`);
            return;
        }

        const [integration] = await sqlTx`
            SELECT api_token FROM utmify_integrations 
            WHERE id = ${integrationId} AND seller_id = ${sellerData.id}
        `;

        if (!integration || !integration.api_token) {
            console.error(`[Utmify] ERRO: Token não encontrado para a integração ID ${integrationId} do vendedor ${sellerData.id}.`);
            return;
        }

        const utmifyApiToken = integration.api_token;
        
        const createdAt = (pixData.created_at || new Date()).toISOString().replace('T', ' ').substring(0, 19);
        const approvedDate = status === 'paid' ? (pixData.paid_at || new Date()).toISOString().replace('T', ' ').substring(0, 19) : null;
        const payload = {
            orderId: pixData.provider_transaction_id, platform: "HotTrack", paymentMethod: 'pix',
            status: status, createdAt: createdAt, approvedDate: approvedDate, refundedAt: null,
            customer: { name: customerData?.name || "Não informado", email: customerData?.email || "naoinformado@email.com", phone: customerData?.phone || null, document: customerData?.document || null, },
            products: [{ id: productData?.id || "default_product", name: productData?.name || "Produto Digital", planId: null, planName: null, quantity: 1, priceInCents: Math.round(pixData.pix_value * 100) }],
            trackingParameters: { src: null, sck: null, utm_source: clickData.utm_source, utm_campaign: clickData.utm_campaign, utm_medium: clickData.utm_medium, utm_content: clickData.utm_content, utm_term: clickData.utm_term },
            commission: { totalPriceInCents: Math.round(pixData.pix_value * 100), gatewayFeeInCents: Math.round(pixData.pix_value * 100 * (sellerData.commission_rate || 0.0500)), userCommissionInCents: Math.round(pixData.pix_value * 100 * (1 - (sellerData.commission_rate || 0.0500))) },
            isTest: false
        };

        await axios.post('https://api.utmify.com.br/api-credentials/orders', payload, { headers: { 'x-api-token': utmifyApiToken } });
        console.log(`[Utmify] SUCESSO: Evento '${status}' do pedido ${payload.orderId} enviado para a conta Utmify (Integração ID: ${integrationId}).`);

    } catch (error) {
        console.error(`[Utmify] ERRO CRÍTICO ao enviar evento '${status}':`, error.response?.data || error.message);
    }
}
async function sendMetaEvent(eventName, clickData, transactionData, customerData = null) {
    try {
        let presselPixels = [];
        if (clickData.pressel_id) {
            presselPixels = await sqlTx`SELECT pixel_config_id FROM pressel_pixels WHERE pressel_id = ${clickData.pressel_id}`;
        } else if (clickData.checkout_id) {
            // Verificar se é um checkout antigo (integer) ou hosted_checkout (UUID)
            const checkoutId = clickData.checkout_id;
            
            // Se começa com 'cko_', é um hosted_checkout (UUID)
            if (typeof checkoutId === 'string' && checkoutId.startsWith('cko_')) {
                // Buscar pixel_id do config do hosted_checkout
                const [hostedCheckout] = await sqlTx`
                    SELECT config->'tracking'->>'pixel_id' as pixel_id 
                    FROM hosted_checkouts 
                    WHERE id = ${checkoutId}
                `;
                
                if (hostedCheckout?.pixel_id) {
                    // Converter pixel_id para o formato esperado
                    presselPixels = [{ pixel_config_id: parseInt(hostedCheckout.pixel_id) }];
                }
            } else {
                // É um checkout antigo (integer), usar a tabela checkout_pixels
                presselPixels = await sqlTx`SELECT pixel_config_id FROM checkout_pixels WHERE checkout_id = ${checkoutId}`;
            }
        }

        if (presselPixels.length === 0) {
            console.log(`Nenhum pixel configurado para o evento ${eventName} do clique ${clickData.id}.`);
            return;
        }

        const userData = {
            fbp: clickData.fbp || undefined,
            fbc: clickData.fbc || undefined,
            external_id: clickData.click_id ? clickData.click_id.replace('/start ', '') : undefined
        };

        if (clickData.ip_address && clickData.ip_address !== '::1' && !clickData.ip_address.startsWith('127.0.0.1')) {
            userData.client_ip_address = clickData.ip_address;
        }
        if (clickData.user_agent && clickData.user_agent.length > 10) { 
            userData.client_user_agent = clickData.user_agent;
        }

        if (customerData?.name) {
            const nameParts = customerData.name.trim().split(' ');
            const firstName = nameParts[0].toLowerCase();
            const lastName = nameParts.length > 1 ? nameParts[nameParts.length - 1].toLowerCase() : undefined;
            userData.fn = crypto.createHash('sha256').update(firstName).digest('hex');
            if (lastName) {
                userData.ln = crypto.createHash('sha256').update(lastName).digest('hex');
            }
        }

        const city = clickData.city && clickData.city !== 'Desconhecida' ? clickData.city.toLowerCase().replace(/[^a-z]/g, '') : null;
        const state = clickData.state && clickData.state !== 'Desconhecido' ? clickData.state.toLowerCase().replace(/[^a-z]/g, '') : null;
        if (city) userData.ct = crypto.createHash('sha256').update(city).digest('hex');
        if (state) userData.st = crypto.createHash('sha256').update(state).digest('hex');

        Object.keys(userData).forEach(key => userData[key] === undefined && delete userData[key]);
        
        for (const { pixel_config_id } of presselPixels) {
            const [pixelConfig] = await sqlTx`SELECT pixel_id, meta_api_token FROM pixel_configurations WHERE id = ${pixel_config_id}`;
            if (pixelConfig) {
                const { pixel_id, meta_api_token } = pixelConfig;
                const event_id = `${eventName}.${transactionData.id || clickData.id}.${pixel_id}`;
                
                const payload = {
                    data: [{
                        event_name: eventName,
                        event_time: Math.floor(Date.now() / 1000),
                        event_id,
                        user_data: userData,
                        custom_data: {
                            currency: 'BRL',
                            value: transactionData.pix_value
                        },
                    }]
                };
                
                if (eventName !== 'Purchase') {
                    delete payload.data[0].custom_data.value;
                }

                console.log(`[Meta Pixel] Enviando payload para o pixel ${pixel_id}:`, JSON.stringify(payload, null, 2));
                await axios.post(`https://graph.facebook.com/v19.0/${pixel_id}/events`, payload, { params: { access_token: meta_api_token } });
                console.log(`Evento '${eventName}' enviado para o Pixel ID ${pixel_id}.`);

                if (eventName === 'Purchase') {
                     await sqlTx`UPDATE pix_transactions SET meta_event_id = ${event_id} WHERE id = ${transactionData.id}`;
                }
            }
        }
    } catch (error) {
        console.error(`Erro ao enviar evento '${eventName}' para a Meta. Detalhes:`, error.response?.data || error.message);
    }
}
async function checkPendingTransactions() {
    try {
        const pendingTransactions = await sqlTx`
            SELECT id, provider, provider_transaction_id, click_id_internal, status
            FROM pix_transactions WHERE status = 'pending' AND created_at > NOW() - INTERVAL '30 minutes'`;

        if (pendingTransactions.length === 0) return;
        
        for (const tx of pendingTransactions) {
            if (tx.provider === 'oasyfy' || tx.provider === 'cnpay' || tx.provider === 'brpix') {
                continue;
            }

            try {
                const [seller] = await sqlTx`
                    SELECT *
                    FROM sellers s JOIN clicks c ON c.seller_id = s.id
                    WHERE c.id = ${tx.click_id_internal}`;
                if (!seller) continue;

                let providerStatus, customerData = {};
                if (tx.provider === 'syncpay') {
                    const syncPayToken = await getSyncPayAuthToken(seller);
                    const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${tx.provider_transaction_id}`, { headers: { 'Authorization': `Bearer ${syncPayToken}` } });
                    providerStatus = response.data.status;
                    customerData = response.data.payer;
                } else if (tx.provider === 'pushinpay') {
                    const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${tx.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                    providerStatus = response.data.status;
                    // CORREÇÃO: Campo correto é 'payer_national_registration' conforme documentação PushinPay
                    customerData = { name: response.data.payer_name, document: response.data.payer_national_registration };
                } else if (tx.provider === 'wiinpay') {
                    const wiinpayApiKey = getSellerWiinpayApiKey(seller);
                    if (wiinpayApiKey) {
                        const now = Date.now();
                        const last = wiinpayLastCheckAt.get(tx.provider_transaction_id) || 0;
                        if (now - last >= 60_000) {
                            const result = await getWiinpayPaymentStatus(tx.provider_transaction_id, wiinpayApiKey);
                            providerStatus = result.status;
                            customerData = result.customer || {};
                            wiinpayLastCheckAt.set(tx.provider_transaction_id, now);
                        }
                    } else {
                        console.warn(`[checkPendingTransactions] Seller ${seller.id} sem chave WiinPay configurada para consulta.`);
                    }
                } else if (tx.provider === 'paradise') {
                    const paradiseSecretKey = seller.paradise_secret_key;
                    if (paradiseSecretKey) {
                        const now = Date.now();
                        const last = paradiseLastCheckAt.get(tx.provider_transaction_id) || 0;
                        if (now - last >= 60_000) {
                            const result = await getParadisePaymentStatus(tx.provider_transaction_id, paradiseSecretKey);
                            providerStatus = result.status;
                            customerData = result.customer || {};
                            paradiseLastCheckAt.set(tx.provider_transaction_id, now);
                        }
                    } else {
                        console.warn(`[checkPendingTransactions] Seller ${seller.id} sem chave Paradise configurada para consulta.`);
                    }
                }
                
                const normalizedProviderStatus = String(providerStatus || '').toLowerCase();
                const paidStatuses = new Set(['paid', 'completed', 'approved', 'success', 'received']);

                if (paidStatuses.has(normalizedProviderStatus) && tx.status !== 'paid') {
                     await handleSuccessfulPayment(tx.id, customerData);
                }
            } catch (error) {
                if (!error.response || error.response.status !== 404) {
                    console.error(`Erro ao verificar transação ${tx.id} (${tx.provider}):`, error.response?.data || error.message);
                }
            }
            await new Promise(resolve => setTimeout(resolve, 200)); 
        }
    } catch (error) {
        console.error("Erro na rotina de verificação geral:", error.message);
    }
}

app.post('/api/webhook/syncpay', async (req, res) => {
    try {
        const notification = req.body;
        console.log('[Webhook SyncPay] Notificação recebida:', JSON.stringify(notification, null, 2));

        if (!notification.data) {
            console.log('[Webhook SyncPay] Webhook ignorado: formato inesperado, objeto "data" não encontrado.');
            return res.sendStatus(200);
        }

        const transactionData = notification.data;
        const transactionId = transactionData.id;
        const identifier = transactionData.identifier; // CORREÇÃO: SyncPay pode usar 'identifier'
        const status = transactionData.status;
        const customer = transactionData.client;

        if (!transactionId || !status) {
            console.log('[Webhook SyncPay] Ignorado: "id" ou "status" não encontrados dentro do objeto "data".');
            return res.sendStatus(200);
        }

        const normalized = String(status || '').toLowerCase();
        const paidStatuses = new Set(['paid', 'completed', 'approved', 'success']);
        
        if (paidStatuses.has(normalized)) {
            console.log(`[Webhook SyncPay] Processando pagamento - ID: ${transactionId}, Identifier: ${identifier}`);
            
            // CORREÇÃO: Buscar por 'id' OU 'identifier' (SyncPay pode enviar um ou outro)
            const [tx] = await sqlTx`
                SELECT * FROM pix_transactions 
                WHERE (provider_transaction_id = ${transactionId} OR provider_transaction_id = ${identifier}) 
                AND provider = 'syncpay'
            `;

            if (!tx) {
                console.error(`[Webhook SyncPay] ERRO: Transação não encontrada! Tentou buscar id='${transactionId}' ou identifier='${identifier}', provider='syncpay'`);
                const countResult = await sqlTx`SELECT COUNT(*) as total FROM pix_transactions WHERE provider = 'syncpay'`;
                console.error(`[Webhook SyncPay] Total de transações SyncPay no banco: ${countResult[0].total}`);
                // Logar todas as transações SyncPay recentes para debug
                const recentTx = await sqlTx`SELECT provider_transaction_id FROM pix_transactions WHERE provider = 'syncpay' ORDER BY created_at DESC LIMIT 5`;
                console.error(`[Webhook SyncPay] Últimas 5 transaction IDs no banco:`, recentTx.map(t => t.provider_transaction_id));
                return res.sendStatus(200);
            }
            
            // Se já está paga, verificar se eventos foram enviados
            if (tx.status === 'paid') {
                // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                if (!tx.meta_event_id) {
                    console.log(`[Webhook SyncPay] Transação ${transactionId} (interna: ${tx.id}) está paga mas sem meta_event_id. Tentando enviar eventos...`);
                    await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document });
                } else {
                    console.log(`[Webhook SyncPay] Transação ${transactionId} (interna: ${tx.id}) já está marcada como 'paga' e eventos enviados. Ignorando webhook duplicado.`);
                }
                return res.sendStatus(200);
            }
            
            console.log(`[Webhook SyncPay] Transação ${tx.id} encontrada. Status atual: '${tx.status}'. Atualizando para PAGO...`);
            await handleSuccessfulPayment(tx.id, { name: customer?.name, document: customer?.document });
            console.log(`[Webhook SyncPay] ✓ Transação ${transactionId} (interna: ${tx.id}) processada com sucesso!`);
        } else {
            console.log(`[Webhook SyncPay] Status '${status}' não é considerado pago. Ignorando.`);
        }
        
        res.sendStatus(200);
    
    } catch (error) {
        console.error(`[Webhook SyncPay] ERRO CRÍTICO:`, error);
        console.error(`[Webhook SyncPay] Stack:`, error.stack);
        res.sendStatus(500);
    }
});

app.post('/api/webhook/brpix', async (req, res) => {
    const payload = req.body || {};
    const event = payload.event;
    const data = payload.data || {};
    const customer = data.customer || {};
    const transactionId = data.transaction_id || data.id;

    console.log('[Webhook BRPix] Notificação recebida:', JSON.stringify({ event, transactionId, status: data.status }, null, 2));

    if (!event || !transactionId) {
        console.warn('[Webhook BRPix] Payload inválido: campos "event" ou "transaction_id" ausentes.');
        return res.sendStatus(200);
    }

    const normalizedEvent = String(event).toLowerCase();

    try {
        const [tx] = await sqlTx`SELECT * FROM pix_transactions WHERE provider_transaction_id = ${transactionId} AND provider = 'brpix'`;

        if (!tx) {
            console.error(`[Webhook BRPix] ERRO: Transação não encontrada! provider_transaction_id='${transactionId}', provider='brpix'`);
            const countResult = await sqlTx`SELECT COUNT(*) as total FROM pix_transactions WHERE provider = 'brpix'`;
            console.error(`[Webhook BRPix] Total de transações BRPix no banco: ${countResult[0].total}`);
            return res.sendStatus(200);
        }

        if (normalizedEvent === 'transaction.paid') {
            const customerDocument = customer?.document?.number || customer?.document || customer?.cpf || null;
            const customerData = { name: customer?.name, document: customerDocument, email: customer?.email };
            
            // Se já está paga, verificar se eventos foram enviados
            if (tx.status === 'paid') {
                // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                if (!tx.meta_event_id) {
                    console.log(`[Webhook BRPix] Transação ${transactionId} (interna: ${tx.id}) está paga mas sem meta_event_id. Tentando enviar eventos...`);
                    await handleSuccessfulPayment(tx.id, customerData);
                } else {
                    console.log(`[Webhook BRPix] Transação ${transactionId} (interna: ${tx.id}) já está marcada como 'paga' e eventos enviados. Ignorando webhook duplicado.`);
                }
                return res.sendStatus(200);
            }

            console.log(`[Webhook BRPix] Transação ${tx.id} encontrada. Status atual: '${tx.status}'. Atualizando para PAGO...`);
            await handleSuccessfulPayment(tx.id, customerData);
            console.log(`[Webhook BRPix] ✓ Transação ${transactionId} (interna: ${tx.id}) processada com sucesso!`);
        } else if (normalizedEvent === 'transaction.created') {
            if (tx.status === 'paid') {
                console.log(`[Webhook BRPix] Evento 'created' ignorado: transação ${transactionId} já está paga.`);
            } else {
                const [updated] = await sqlTx`UPDATE pix_transactions SET status = 'pending', updated_at = NOW() WHERE id = ${tx.id} RETURNING id`;
                if (updated) {
                    console.log(`[Webhook BRPix] Transação ${transactionId} (interna: ${tx.id}) marcada/confirmada como 'pending'.`);
                }
            }
        } else if (normalizedEvent === 'transaction.failed' || normalizedEvent === 'transaction.expired' || normalizedEvent === 'transaction.refunded') {
            if (tx.status === 'paid') {
                console.warn(`[Webhook BRPix] Evento '${event}' ignorado: transação ${transactionId} já está paga.`);
            } else {
                const statusMap = {
                    'transaction.failed': 'failed',
                    'transaction.expired': 'expired',
                    'transaction.refunded': 'refunded'
                };
                const newStatus = statusMap[normalizedEvent] || 'failed';
                const [updated] = await sqlTx`UPDATE pix_transactions SET status = ${newStatus}, updated_at = NOW() WHERE id = ${tx.id} RETURNING id, status`;
                if (updated) {
                    console.log(`[Webhook BRPix] Transação ${transactionId} (interna: ${tx.id}) atualizada para '${newStatus}'.`);
                }
            }
        } else {
            console.log(`[Webhook BRPix] Evento '${event}' não tratado. Nenhuma ação executada.`);
        }
    } catch (error) {
        console.error(`[Webhook BRPix] ERRO CRÍTICO ao processar evento '${event}' para transactionId '${transactionId}':`, error);
        console.error(`[Webhook BRPix] Stack:`, error.stack);
    }

    res.sendStatus(200);
});

app.post('/api/webhook/paradise', async (req, res) => {
    // Responde imediatamente ao Paradise para evitar timeouts
    res.sendStatus(200);

    try {
        const payload = req.body;
        
        if (!payload) {
            console.warn('[Webhook Paradise] Payload vazio:', JSON.stringify(req.body));
            return;
        }

        const { transaction_id, external_id, status, customer } = payload;
        
        // Paradise pode enviar transaction_id (numérico) ou external_id (reference)
        const identifier = transaction_id || external_id;
        
        if (!identifier) {
            console.warn('[Webhook Paradise] Payload sem identificador de pagamento:', JSON.stringify(payload));
            return;
        }

        const normalized = String(status || '').toLowerCase();
        const paidStatuses = new Set(['approved', 'paid', 'completed', 'success']);
        const canceledStatuses = new Set(['failed', 'expired', 'cancelled', 'canceled', 'refunded']);

        // Processar PIX pago
        if (paidStatuses.has(normalized)) {
            try {
                const [tx] = await sqlTx`
                    SELECT pt.id, pt.status, pt.provider_transaction_id, pt.meta_event_id, c.seller_id 
                    FROM pix_transactions pt 
                    JOIN clicks c ON pt.click_id_internal = c.id 
                    WHERE (pt.provider_transaction_id = ${String(identifier)} OR pt.provider_transaction_id = ${external_id || ''}) AND pt.provider = 'paradise'
                `;

                if (!tx) {
                    console.error(`[Webhook Paradise] Transação não encontrada para ID ${identifier}.`);
                    return;
                }

                // Extrair dados do cliente do webhook
                const payerName = customer?.name || '';
                const payerDocument = customer?.document || '';
                const payerEmail = customer?.email || '';
                const payerPhone = customer?.phone || '';
                const customerData = { 
                    name: payerName, 
                    document: payerDocument,
                    email: payerEmail,
                    phone: payerPhone
                };

                // Se já está paga, verificar se eventos foram enviados
                if (tx.status === 'paid') {
                    // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                    if (!tx.meta_event_id) {
                        console.log(`[Webhook Paradise] Transação ${tx.id} está paga mas sem meta_event_id. Tentando enviar eventos...`);
                        await handleSuccessfulPayment(tx.id, customerData);
                    } else {
                        console.log(`[Webhook Paradise] Transação ${tx.id} já está paga e eventos enviados. Ignorando duplicata.`);
                    }
                    return;
                }

                console.log(`[Webhook Paradise] Processando transação ${identifier} (interna: ${tx.id}) com status '${normalized}'.`);
                
                await handleSuccessfulPayment(tx.id, customerData);
                console.log(`[Webhook Paradise] ✓ Transação ${identifier} (interna: ${tx.id}) processada com sucesso!`);
            } catch (error) {
                console.error('[Webhook Paradise] Erro ao processar pagamento:', error);
            }
        }
        // Processar PIX cancelado/expirado/falhado
        else if (canceledStatuses.has(normalized)) {
            try {
                console.log(`[Webhook Paradise] PIX cancelado/expirado/falhado - ID: ${identifier}, Status: ${status}`);
                
                const [tx] = await sqlTx`
                    UPDATE pix_transactions 
                    SET status = 'expired', updated_at = NOW() 
                    WHERE (provider_transaction_id = ${String(identifier)} OR provider_transaction_id = ${external_id || ''}) 
                      AND provider = 'paradise' 
                      AND status = 'pending'
                    RETURNING id, status
                `;
                
                if (tx) {
                    console.log(`[Webhook Paradise] Transação ${identifier} (interna: ${tx.id}) marcada como expirada.`);
                }
            } catch (error) {
                console.error('[Webhook Paradise] Erro ao marcar transação como expirada:', error.message);
            }
        } else {
            console.log(`[Webhook Paradise] Status '${normalized}' não tratado. Payload:`, JSON.stringify(payload));
        }
    } catch (error) {
        console.error('[Webhook Paradise] Erro inesperado ao processar payload:', error);
    }
});

// ==========================================================
//          ENDPOINTS ADICIONAIS DO ARQUIVO 1
// ==========================================================

// Endpoint 1: Configurações HotTrack
app.put('/api/settings/hottrack-key', authenticateJwt, async (req, res) => {
    const { apiKey } = req.body;
    if (typeof apiKey === 'undefined') return res.status(400).json({ message: 'O campo apiKey é obrigatório.' });
    try {
        await sqlWithRetry('UPDATE sellers SET api_key = $1 WHERE id = $2', [apiKey, req.user.id]);
        res.status(200).json({ message: 'Chave de API do HotTrack salva com sucesso!' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao salvar a chave.' });
    }
});

// Endpoint 2: Contagem de contatos
app.post('/api/bots/contacts-count', authenticateJwt, async (req, res) => {
    const { botIds, excludeChatIds } = req.body;
    const sellerId = req.user.id;

    if (!botIds || !Array.isArray(botIds) || botIds.length === 0) {
        return res.status(200).json({ count: 0 });
    }

    try {
        let query = `SELECT COUNT(DISTINCT chat_id) FROM telegram_chats WHERE seller_id = $1 AND bot_id = ANY($2::int[])`;
        let params = [sellerId, botIds];

        if (excludeChatIds && Array.isArray(excludeChatIds) && excludeChatIds.length > 0) {
            query += ` AND chat_id NOT IN (${excludeChatIds.map((_, i) => `$${i + 3}`).join(',')})`;
            params.push(...excludeChatIds);
        }

        const result = await sqlWithRetry(query, params);
        res.status(200).json({ count: parseInt(result[0].count, 10) });
    } catch (error) {
        console.error("Erro ao contar contatos:", error);
        res.status(500).json({ message: 'Erro interno ao contar contatos.' });
    }
});

// Endpoint 3: Validação de contatos
app.post('/api/bots/validate-contacts', authenticateJwt, async (req, res) => {
    const { botId } = req.body;
    const sellerId = req.user.id;

    if (!botId) {
        return res.status(400).json({ message: 'ID do bot é obrigatório.' });
    }

    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [botId, sellerId]);
        if (!bot || !bot.bot_token) {
            return res.status(404).json({ message: 'Bot não encontrado ou sem token configurado.' });
        }

        const contacts = await sqlWithRetry('SELECT DISTINCT ON (chat_id) chat_id, first_name, last_name, username FROM telegram_chats WHERE bot_id = $1', [botId]);
        if (contacts.length === 0) {
            return res.status(200).json({ inactive_contacts: [], message: 'Nenhum contato para validar.' });
        }

        const inactiveContacts = [];
        const BATCH_SIZE = 30; 

        for (let i = 0; i < contacts.length; i += BATCH_SIZE) {
            const batch = contacts.slice(i, i + BATCH_SIZE);
            const promises = batch.map(contact => 
                sendTelegramRequest(bot.bot_token, 'sendChatAction', { chat_id: contact.chat_id, action: 'typing' })
                    .catch(error => {
                        if (error.response && (error.response.status === 403 || error.response.status === 400)) {
                            inactiveContacts.push(contact);
                        }
                    })
            );

            await Promise.all(promises);

            if (i + BATCH_SIZE < contacts.length) {
                await new Promise(resolve => setTimeout(resolve, 1100));
            }
        }

        res.status(200).json({ inactive_contacts: inactiveContacts });

    } catch (error) {
        console.error("Erro ao validar contatos:", error);
        res.status(500).json({ message: 'Erro interno ao validar contatos.' });
    }
});

// Endpoint 4: Remoção de contatos
app.post('/api/bots/remove-contacts', authenticateJwt, async (req, res) => {
    const { botId, chatIds } = req.body;
    const sellerId = req.user.id;

    if (!botId || !Array.isArray(chatIds) || chatIds.length === 0) {
        return res.status(400).json({ message: 'ID do bot e uma lista de chat_ids são obrigatórios.' });
    }

    try {
        const result = await sqlWithRetry('DELETE FROM telegram_chats WHERE bot_id = $1 AND seller_id = $2 AND chat_id = ANY($3::bigint[])', [botId, sellerId, chatIds]);
        res.status(200).json({ message: `${result.count} contatos inativos foram removidos com sucesso.` });
    } catch (error) {
        console.error("Erro ao remover contatos:", error);
        res.status(500).json({ message: 'Erro interno ao remover contatos.' });
    }
});

// Endpoint 5: Envio de mídia (base64)
app.post('/api/chats/:botId/send-media', authenticateJwt, json70mb, async (req, res) => {
    const { chatId, fileData, fileType, fileName } = req.body;
    if (!chatId || !fileData || !fileType || !fileName) {
        return res.status(400).json({ message: 'Dados incompletos.' });
    }


    try {
        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1 AND seller_id = $2', [req.params.botId, req.user.id]);
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado.' });
        const buffer = Buffer.from(fileData, 'base64');
        try { validateTelegramSize(buffer, fileType); } catch (e) { return res.status(413).json({ message: e.message }); }
        const formData = new FormData();
        formData.append('chat_id', chatId);
        let method, field;
        if (fileType.startsWith('image/')) {
            method = 'sendPhoto';
            field = 'photo';
        } else if (fileType.startsWith('video/')) {
            method = 'sendVideo';
            field = 'video';
        } else if (fileType.startsWith('audio/')) {
            method = 'sendVoice';
            field = 'voice';
        } else {
            return res.status(400).json({ message: 'Tipo de arquivo não suportado.' });
        }
        formData.append(field, buffer, { filename: fileName });
        const response = await sendTelegramRequest(bot.bot_token, method, formData, { headers: formData.getHeaders() });
        if (response.ok) {
            await saveMessageToDb(req.user.id, req.params.botId, response.result, 'operator');
        }
        res.status(200).json({ message: 'Mídia enviada!' });
    } catch (error) {

        const msg = error.message?.includes('excede') ? error.message : 'Não foi possível enviar a mídia.';
        res.status(500).json({ message: msg });
 
        
    }
});

// Endpoint 6: Deletar conversa
app.delete('/api/chats/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        await sqlWithRetry('DELETE FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND seller_id = $3', [req.params.botId, req.params.chatId, req.user.id]);
        res.status(200).json({ message: 'Conversa deletada.' });
    } catch (error) { res.status(500).json({ message: 'Erro ao deletar a conversa.' }); }
});

// Endpoint 7: Gerar PIX manual
app.post('/api/chats/generate-pix', authenticateJwt, async (req, res) => {
    const { botId, chatId, click_id, valueInCents, pixMessage, pixButtonText } = req.body;
    try {
        if (!click_id) return res.status(400).json({ message: "Usuário não tem um Click ID para gerar PIX." });
        
        const [seller] = await sqlWithRetry('SELECT * FROM sellers WHERE id = $1', [req.user.id]);
        if (!seller || !seller.api_key) return res.status(400).json({ message: "API Key não configurada." });

        // Busca dados do click
        const db_click_id = click_id.startsWith('/start ') ? click_id : `/start ${click_id}`;
        const [click] = await sqlTx`SELECT * FROM clicks WHERE click_id = ${db_click_id} AND seller_id = ${seller.id}`;
        if (!click) return res.status(404).json({ message: 'Click ID não encontrado.' });

        const ip_address = click.ip_address;
        const hostPlaceholder = process.env.HOTTRACK_API_URL ? new URL(process.env.HOTTRACK_API_URL).host : 'localhost';

        // Gera PIX SEM enviar eventos ainda
        const pixResult = await generatePixWithFallback(seller, valueInCents, hostPlaceholder, seller.api_key, ip_address, click.id);
        console.log(`[Manual PIX] PIX gerado com sucesso. Transaction ID: ${pixResult.transaction_id}`);

        await sqlWithRetry(`UPDATE telegram_chats SET last_transaction_id = $1 WHERE bot_id = $2 AND chat_id = $3`, [pixResult.transaction_id, botId, chatId]);

        const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1', [botId]);
        
        const messageText = pixMessage || '';
        const buttonText = pixButtonText || '📋 Copiar Código PIX';
        const textToSend = `<pre>${pixResult.qr_code_text}</pre>\n\n${messageText}`;

        // CRÍTICO: Tenta enviar o PIX para o usuário
        const sentMessage = await sendTelegramRequest(bot.bot_token, 'sendMessage', {
            chat_id: chatId,
            text: textToSend,
            parse_mode: 'HTML',
            reply_markup: {
                inline_keyboard: [
                    [{ text: buttonText, copy_text: { text: pixResult.qr_code_text } }]
                ]
            }
        });

        // Verifica se o envio foi bem-sucedido
        if (!sentMessage.ok) {
            // Cancela a transação PIX no banco se não conseguiu enviar ao usuário
            console.error(`[Manual PIX] FALHA ao enviar PIX. Cancelando transação ${pixResult.transaction_id}. Motivo: ${sentMessage.description || 'Desconhecido'}`);
            
            await sqlTx`
                UPDATE pix_transactions 
                SET status = 'canceled' 
                WHERE provider_transaction_id = ${pixResult.transaction_id}
            `;
            
            return res.status(500).json({ 
                message: `Não foi possível enviar PIX ao usuário. Motivo: ${sentMessage.description || 'Erro desconhecido'}. Transação cancelada.` 
            });
        }

        // Salva a mensagem enviada
        await saveMessageToDb(req.user.id, botId, sentMessage.result, 'operator');
        console.log(`[Manual PIX] PIX enviado com sucesso ao usuário ${chatId}`);

        // Envia eventos para Utmify e Meta SOMENTE APÓS confirmação de entrega
        const customerDataForUtmify = { name: "Cliente Bot", email: "bot@email.com" };
        const productDataForUtmify = { id: "prod_manual", name: "PIX Manual" };
        
        await sendEventToUtmify(
            'waiting_payment', 
            click, 
            { provider_transaction_id: pixResult.transaction_id, pix_value: valueInCents / 100, created_at: new Date() }, 
            seller, 
            customerDataForUtmify, 
            productDataForUtmify
        );

        console.log(`[Manual PIX] Evento 'waiting_payment' enviado para Utmify para transação ${pixResult.transaction_id}`);

        // Envia InitiateCheckout para Meta se o click veio de pressel ou checkout
        if (click.pressel_id || click.checkout_id) {
            await sendMetaEvent('InitiateCheckout', click, { id: pixResult.internal_transaction_id, pix_value: valueInCents / 100 }, null);
            console.log(`[Manual PIX] Evento 'InitiateCheckout' enviado para Meta para transação ${pixResult.transaction_id}`);
        }

        res.status(200).json({ message: 'PIX enviado ao usuário com sucesso.' });
    } catch (error) {
        console.error('[Manual PIX] Erro:', error.message);
        res.status(500).json({ message: error.message || 'Erro ao gerar PIX.' });
    }
});

// Endpoint 8: Verificar status do PIX (modificado)
app.get('/api/chats/check-pix/:botId/:chatId', authenticateJwt, async (req, res) => {
    try {
        const { botId, chatId } = req.params;
        // 1) Descobrir a última transação do chat
        const [chat] = await sqlWithRetry(
            'SELECT last_transaction_id FROM telegram_chats WHERE bot_id = $1 AND chat_id = $2 AND last_transaction_id IS NOT NULL ORDER BY created_at DESC LIMIT 1',
            [botId, chatId]
        );
        if (!chat || !chat.last_transaction_id) {
            return res.status(404).json({ message: 'Nenhuma transação PIX recente encontrada para este usuário.' });
        }

        // 2) Obter a API Key do seller logado
        const [seller] = await sqlWithRetry('SELECT api_key FROM sellers WHERE id = $1', [req.user.id]);
        if (!seller || !seller.api_key) {
            return res.status(400).json({ message: 'API Key não configurada.' });
        }

        // 3) Delegar para o endpoint central de status
        const baseApiUrl = process.env.HOTTRACK_API_URL;
        if (!baseApiUrl) {
            return res.status(500).json({ message: 'HOTTRACK_API_URL não configurada no servidor.' });
        }

        const response = await axios.get(`${baseApiUrl}/api/pix/status/${encodeURIComponent(chat.last_transaction_id)}`, {
            headers: { 'x-api-key': seller.api_key }
        });

        return res.status(200).json(response.data);
    } catch (error) {
        const status = error.response?.status || 500;
        const payload = error.response?.data || { message: 'Erro ao consultar PIX.' };
        return res.status(status).json(payload);
    }
});
// Endpoint 9: Iniciar fluxo manualmente
app.post('/api/chats/start-flow', authenticateJwt, async (req, res) => {
    const { botId, chatId, flowId } = req.body;
    const sellerId = req.user.id;

    try {
        const [bot] = await sqlTx`SELECT bot_token FROM telegram_bots WHERE id = ${botId} AND seller_id = ${sellerId}`;
        if (!bot) return res.status(404).json({ message: 'Bot não encontrado ou não pertence a você.' });

        const [flow] = await sqlTx`SELECT nodes FROM flows WHERE id = ${flowId} AND bot_id = ${botId}`;
        if (!flow || !flow.nodes) return res.status(404).json({ message: 'Fluxo não encontrado ou não pertence a este bot.' });

        // Parse correto do flow.nodes (pode ser string ou objeto)
        const flowData = typeof flow.nodes === 'string' ? JSON.parse(flow.nodes) : flow.nodes;

        // --- LÓGICA DE LIMPEZA ---
        console.log(`[Manual Flow Start] Iniciando limpeza para o chat ${chatId} antes de iniciar o fluxo ${flowId}.`);
        const [existingState] = await sqlTx`SELECT scheduled_message_id FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        if (existingState && existingState.scheduled_message_id) {
            try {
                await qstashClient.messages.delete(existingState.scheduled_message_id);
                console.log(`[Manual Flow Start] Tarefa de timeout antiga cancelada.`);
            } catch (e) { /* Ignora erro se a tarefa já foi executada */ }
        }
        await sqlTx`DELETE FROM user_flow_states WHERE chat_id = ${chatId} AND bot_id = ${botId}`;
        console.log(`[Manual Flow Start] Estado de fluxo antigo deletado.`);
        // --- FIM DA LÓGICA DE LIMPEZA ---

        // Encontra o ponto de partida do fluxo
        const startNode = flowData.nodes?.find(node => node.type === 'trigger');
        const firstNodeId = findNextNode(startNode.id, null, flowData.edges);

        if (!firstNodeId) {
            return res.status(400).json({ message: 'O fluxo selecionado não tem um nó inicial configurado após o gatilho.' });
        }

        // Busca o click_id mais recente para preservar o contexto
        const [chatContext] = await sqlTx`SELECT click_id FROM telegram_chats WHERE chat_id = ${chatId} AND bot_id = ${botId} AND click_id IS NOT NULL ORDER BY created_at DESC LIMIT 1`;
        let initialVars = {};
        if (chatContext?.click_id) {
            initialVars.click_id = chatContext.click_id;
        }

        // Inicia o fluxo para o usuário
        await processFlow(chatId, botId, bot.bot_token, sellerId, firstNodeId, initialVars);

        res.status(200).json({ message: 'Fluxo iniciado para o usuário com sucesso!' });
    } catch (error) {
        console.error("Erro ao iniciar fluxo manualmente:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

app.get('/api/media/preview/:bot_id/:file_id', async (req, res) => {
      try {
        const { bot_id, file_id } = req.params;
        let token;
        if (bot_id === 'storage') {
          token = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
        } else {
          const [bot] = await sqlWithRetry('SELECT bot_token FROM telegram_bots WHERE id = $1', [bot_id]);
          token = bot?.bot_token;
        }
    
        if (!token) return res.status(404).send('Bot não encontrado.');
    
        const fileInfoResponse = await sendTelegramRequest(token, 'getFile', { file_id });
        if (!fileInfoResponse.ok || !fileInfoResponse.result?.file_path) {
          return res.status(404).send('Arquivo não encontrado no Telegram.');
        }
    
        const fileUrl = `https://api.telegram.org/file/bot${token}/${fileInfoResponse.result.file_path}`;
    
        // ***** A CORREÇÃO ESTÁ AQUI *****
        // Adiciona 'httpsAgent' para reutilizar a conexão
        const response = await axios.get(fileUrl, { 
          responseType: 'stream',
          httpsAgent: httpsAgent // <<-- ADICIONE ESTA LINHA
        });
        // *********************************
    
        res.setHeader('Content-Type', response.headers['content-type']);
        response.data.pipe(res);
    
      } catch (error) {
        console.error("Erro no preview:", error.message);
        res.status(500).send('Erro ao buscar o arquivo.');
      }
    });

// Endpoint 11: Listar biblioteca de mídia
app.get('/api/media', authenticateJwt, async (req, res) => {
    try {
        const mediaFiles = await sqlWithRetry('SELECT id, file_name, file_id, file_type, thumbnail_file_id FROM media_library WHERE seller_id = $1 ORDER BY created_at DESC', [req.user.id]);
        res.status(200).json(mediaFiles);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar a biblioteca de mídia.' });
    }
});
// Endpoint 12: Upload para biblioteca de mídia
app.post('/api/media/upload', authenticateJwt, json70mb, async (req, res) => {
    const { fileName, fileData, fileType } = req.body;
    if (!fileName || !fileData || !fileType) return res.status(400).json({ message: 'Dados do ficheiro incompletos.' });
    try {
        const storageBotToken = process.env.TELEGRAM_STORAGE_BOT_TOKEN;
        const storageChannelId = process.env.TELEGRAM_STORAGE_CHANNEL_ID;
        if (!storageBotToken || !storageChannelId) throw new Error('Credenciais do bot de armazenamento não configuradas.');
        const buffer = Buffer.from(fileData, 'base64');

        // fileType aqui é 'image' | 'video' | 'audio'. Transformamos em um hint MIME para validar.
        const mimeHint = fileType === 'image' ? 'image/' : (fileType === 'video' ? 'video/' : (fileType === 'audio' ? 'audio/' : ''));
        if (!mimeHint) return res.status(400).json({ message: 'Tipo de ficheiro não suportado.' });
        try { validateTelegramSize(buffer, mimeHint); } catch (e) { return res.status(413).json({ message: e.message }); }
        const formData = new FormData();
        formData.append('chat_id', storageChannelId);
        let telegramMethod = '', fieldName = '';
        if (fileType === 'image') {
            telegramMethod = 'sendPhoto';
            fieldName = 'photo';
        } else if (fileType === 'video') {
            telegramMethod = 'sendVideo';
            fieldName = 'video';
        } else if (fileType === 'audio') {
            telegramMethod = 'sendVoice';
            fieldName = 'voice';
        } else {
            return res.status(400).json({ message: 'Tipo de ficheiro não suportado.' });
        }
        formData.append(fieldName, buffer, { filename: fileName });
        
        // Adicionar timeout maior para uploads grandes (120 segundos)
        const response = await sendTelegramRequest(
            storageBotToken, 
            telegramMethod, 
            formData, 
            { 
                headers: formData.getHeaders(),
                timeout: 120000 // 120 segundos para uploads grandes
            }
        );
        
        if (!response?.ok || !response.result) {
            const errorMsg = response?.description || 'Resposta inválida do Telegram ao enviar mídia.';
            console.error('[Media Upload] Resposta inválida:', errorMsg);
            if (!res.headersSent) {
                return res.status(500).json({ message: `Erro ao enviar mídia: ${errorMsg}` });
            }
            return;
        }
        
        const result = response.result; // Mensagem retornada pelo Telegram

        let fileId, thumbnailFileId = null;
        if (fileType === 'image') {
            const photos = Array.isArray(result.photo) ? result.photo : [];
            fileId = photos.length > 0 ? photos[photos.length - 1].file_id : null;
            thumbnailFileId = photos.length > 0 ? photos[0].file_id : null;
        } else if (fileType === 'video') {
            // Pode vir como result.video ou como result.document com mime de vídeo
            const videoObj = result.video || (result.document && result.document.mime_type?.startsWith('video/') ? result.document : null);
            fileId = videoObj?.file_id || null;
            thumbnailFileId = videoObj?.thumbnail?.file_id || videoObj?.thumb?.file_id || null;
        } else {
            // audio -> enviamos como voice
            fileId = result.voice?.file_id || null;
        }
        
        if (!fileId) {
            console.error('[Media Upload] Resposta Telegram inesperada (sem file_id):', JSON.stringify(result).slice(0, 1000));
            if (!res.headersSent) {
                return res.status(500).json({ message: 'Não foi possível obter o file_id do Telegram.' });
            }
            return;
        }
        
        const [newMedia] = await sqlWithRetry(`
            INSERT INTO media_library (seller_id, file_name, file_id, file_type, thumbnail_file_id)
            VALUES ($1, $2, $3, $4, $5) RETURNING id, file_name, file_id, file_type, thumbnail_file_id;
        `, [req.user.id, fileName, fileId, fileType, thumbnailFileId]);
        
        if (!res.headersSent) {
            return res.status(201).json(newMedia);
        }
    } catch (error) {
        console.error('[Media Upload] Erro:', error);
        
        // Verificar se resposta já foi enviada
        if (res.headersSent) {
            return;
        }
        
        // Mensagens de erro mais específicas
        let errorMessage = 'Erro ao fazer upload da mídia.';
        if (error.message?.includes('timeout')) {
            errorMessage = 'Timeout ao enviar mídia. O arquivo pode ser muito grande ou a conexão está lenta.';
        } else if (error.message?.includes('ECONNRESET') || error.message?.includes('socket')) {
            errorMessage = 'Conexão perdida durante o upload. Tente novamente.';
        } else if (error.message) {
            errorMessage = `Erro ao fazer upload: ${error.message}`;
        }
        
        res.status(500).json({ message: errorMessage });
    }
});

// Endpoint 13: Deletar mídia da biblioteca
app.delete('/api/media/:id', authenticateJwt, async (req, res) => {
    try {
        // Primeiro verificar se a mídia existe e pertence ao usuário
        const [existingMedia] = await sqlWithRetry('SELECT id FROM media_library WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        
        if (!existingMedia) {
            return res.status(404).json({ message: 'Mídia não encontrada.' });
        }
        
        // Se existe, deletar
        await sqlWithRetry('DELETE FROM media_library WHERE id = $1 AND seller_id = $2', [req.params.id, req.user.id]);
        res.status(204).send();
    } catch (error) {
        res.status(500).json({ message: 'Erro ao excluir a mídia.' });
    }
});

// Endpoint 14: Compartilhar fluxo
app.post('/api/flows/:id/share', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const { name, description, allowReshare, bundleLinkedFlows, bundleMedia } = req.body;
    const sellerId = req.user.id;
    try {
        // Buscar apenas nodes do fluxo original (flags vêm do body, definidas pelo usuário)
        const [flow] = await sqlWithRetry(
            `SELECT nodes FROM flows WHERE id = $1 AND seller_id = $2`, 
            [id, sellerId]
        );
        if (!flow) return res.status(404).json({ message: 'Fluxo não encontrado.' });

        const [seller] = await sqlWithRetry('SELECT name FROM sellers WHERE id = $1', [sellerId]);
        if (!seller) return res.status(404).json({ message: 'Vendedor não encontrado.' });
        
        // Inserir em shared_flows usando flags definidas pelo usuário no body (não do fluxo original)
        await sqlWithRetry(`
            INSERT INTO shared_flows (
                name, description, original_flow_id, seller_id, seller_name, nodes,
                share_bundle_linked_flows, share_bundle_media, share_allow_reshare
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [
                name, 
                description, 
                id, 
                sellerId, 
                seller.name, 
                flow.nodes,
                !!bundleLinkedFlows,  // Flag definida pelo usuário no momento do compartilhamento
                !!bundleMedia,        // Flag definida pelo usuário no momento do compartilhamento
                !!allowReshare        // Flag definida pelo usuário no momento do compartilhamento
            ]
        );
        await sqlWithRetry('UPDATE flows SET is_shared = TRUE WHERE id = $1', [id]);
        res.status(201).json({ message: 'Fluxo compartilhado com sucesso!' });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao compartilhar fluxo: ' + error.message });
    }
});

// Endpoint 15: Listar fluxos compartilhados
app.get('/api/shared-flows', authenticateJwt, async (req, res) => {
    try {
        const sharedFlows = await sqlWithRetry('SELECT id, name, description, seller_name, import_count, created_at FROM shared_flows ORDER BY import_count DESC, created_at DESC');
        res.status(200).json(sharedFlows);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar fluxos da comunidade.' });
    }
});

// Endpoint 16: Importar fluxo compartilhado
app.post('/api/shared-flows/:id/import', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const { botId } = req.body;
    const sellerId = req.user.id;
    try {
        if (!botId) return res.status(400).json({ message: 'É necessário selecionar um bot para importar.' });
        
        // Buscar shared_flow incluindo flags de compartilhamento e seller_id original
        const [sharedFlow] = await sqlWithRetry(
            `SELECT name, nodes, seller_id, 
                    share_bundle_linked_flows, share_bundle_media, share_allow_reshare
             FROM shared_flows WHERE id = $1`, 
            [id]
        );
        if (!sharedFlow) return res.status(404).json({ message: 'Fluxo compartilhado não encontrado.' });

        console.log(`[Import Shared Flow] Configurações: bundle_linked_flows=${sharedFlow.share_bundle_linked_flows}, bundle_media=${sharedFlow.share_bundle_media}, allow_reshare=${sharedFlow.share_allow_reshare}`);

        // Deep clone para evitar mutar o original
        let processedNodes = JSON.parse(JSON.stringify(sharedFlow.nodes));
        
        // Verificar se a estrutura tem o array de nós
        const nodesArray = processedNodes?.nodes || [];
        console.log(`[Import Shared Flow] Estrutura de nós: ${nodesArray.length} nós encontrados`);
        
        // Flags independentes
        const shouldCopyLinkedFlows = sharedFlow.share_bundle_linked_flows === true;
        const shouldCopyMedia = sharedFlow.share_bundle_media === true;
        
        // Copiar fluxos anexados se configurado
        if (shouldCopyLinkedFlows) {
            console.log('[Import Shared Flow] Copiando fluxos anexados...');
            const flowIdMapping = await copyLinkedFlows(
                nodesArray,
                sharedFlow.seller_id, // seller_id original do fluxo compartilhado
                sellerId,
                botId,
                shouldCopyMedia, // Passar flag para limpar mídias dos fluxos anexados se necessário
                sharedFlow.share_allow_reshare || false // Herdar share_allow_reshare do fluxo compartilhado
            );
            processedNodes.nodes = updateNodeReferences(nodesArray, flowIdMapping);
        } else {
            console.log('[Import Shared Flow] NÃO copiando fluxos anexados (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanLinkedFlowReferences(nodesArray);
        }
        
        // Copiar mídias se configurado
        if (shouldCopyMedia) {
            console.log('[Import Shared Flow] Copiando mídias...');
            await copyMediaFiles(
                nodesArray,
                sharedFlow.seller_id, // seller_id original do fluxo compartilhado
                sellerId
            );
        } else {
            console.log('[Import Shared Flow] NÃO copiando mídias (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanMediaReferences(processedNodes.nodes || nodesArray);
        }

        const newFlowName = `${sharedFlow.name} (Importado)`;
        const [newFlow] = await sqlWithRetry(
            `INSERT INTO flows (seller_id, bot_id, name, nodes, share_allow_reshare) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
            [sellerId, botId, newFlowName, processedNodes, sharedFlow.share_allow_reshare || false]
        );
        
        await sqlWithRetry('UPDATE shared_flows SET import_count = import_count + 1 WHERE id = $1', [id]);
        
        console.log(`[Import Shared Flow] Fluxo importado com sucesso (allow_reshare=${sharedFlow.share_allow_reshare})`);
        res.status(201).json(newFlow);
    } catch (error) {
        console.error("Erro ao importar fluxo compartilhado:", error);
        res.status(500).json({ message: 'Erro ao importar fluxo: ' + error.message });
    }
});

// Endpoint 17: Gerar link de compartilhamento
app.post('/api/flows/:id/generate-share-link', authenticateJwt, async (req, res) => {
    const { id } = req.params;
    const sellerId = req.user.id;
    const { priceInCents, allowReshare, bundleLinkedFlows, bundleMedia } = req.body;

    console.log(`[Generate Share Link] Flow ID: ${id}, Seller: ${sellerId}`);
    console.log(`[Generate Share Link] Params: price=${priceInCents}, reshare=${allowReshare}, flows=${bundleLinkedFlows}, media=${bundleMedia}`);

    try {
        // Verificar se o fluxo existe e pertence ao vendedor
        const [flow] = await sqlWithRetry(
            'SELECT id FROM flows WHERE id = $1 AND seller_id = $2',
            [id, sellerId]
        );
        
        if (!flow) {
            console.log(`[Generate Share Link] Fluxo não encontrado: id=${id}, seller=${sellerId}`);
            return res.status(404).json({ message: 'Fluxo não encontrado.' });
        }
        
        // O dono sempre pode gerar link de compartilhamento
        // O valor de share_allow_reshare será definido pelo parâmetro allowReshare que vem do frontend
        const shareId = uuidv4();
        console.log(`[Generate Share Link] Generated UUID: ${shareId}`);
        
        const [updatedFlow] = await sqlWithRetry(
            `UPDATE flows SET 
                shareable_link_id = $1,
                share_price_cents = $2,
                share_allow_reshare = $3,
                share_bundle_linked_flows = $4,
                share_bundle_media = $5
             WHERE id = $6 AND seller_id = $7 RETURNING shareable_link_id`,
            [shareId, priceInCents || 0, !!allowReshare, !!bundleLinkedFlows, !!bundleMedia, id, sellerId]
        );
        
        console.log(`[Generate Share Link] Sucesso! Link: ${updatedFlow.shareable_link_id}`);
        res.status(200).json({ shareable_link_id: updatedFlow.shareable_link_id });
    } catch (error) {
        console.error("Erro ao gerar link de compartilhamento:", error);
        console.error("Stack:", error.stack);
        res.status(500).json({ message: 'Erro ao gerar link de compartilhamento: ' + error.message });
    }
});

// Endpoint 18: Detalhes do link compartilhado
app.get('/api/share/details/:shareId', async (req, res) => {
    try {
        const { shareId } = req.params;
        const [flow] = await sqlWithRetry(`
            SELECT name, share_price_cents, share_allow_reshare, share_bundle_linked_flows, share_bundle_media
            FROM flows WHERE shareable_link_id = $1
        `, [shareId]);

        if (!flow) {
            return res.status(404).json({ message: 'Link de compartilhamento inválido ou não encontrado.' });
        }

        res.status(200).json(flow);
    } catch (error) {
        console.error("Erro ao buscar detalhes do compartilhamento:", error);
        res.status(500).json({ message: 'Erro interno do servidor.' });
    }
});

/**
 * Copia fluxos anexados (linked flows) referenciados nos nós
 */
async function copyLinkedFlows(nodes, originalSellerId, newSellerId, newBotId, shouldCopyMedia, inheritAllowReshare = false) {
    const flowIdMapping = {};
    if (!nodes || !Array.isArray(nodes)) {
        console.log('[Copy Linked Flows] Nós inválidos ou vazios');
        return flowIdMapping;
    }
    
    const linkedFlowIds = new Set();
    for (const node of nodes) {
        // Verificar dentro de actions (estrutura atual)
        if (node.data?.actions && Array.isArray(node.data.actions)) {
            for (const action of node.data.actions) {
                if (action.type === 'forward_flow' && action.data?.targetFlowId) {
                    linkedFlowIds.add(action.data.targetFlowId);
                    console.log(`[Copy Linked Flows] Detectado targetFlowId: ${action.data.targetFlowId} (${action.data.targetFlowName}) em action do nó ${node.id}`);
                }
            }
        }
    }
    
    if (linkedFlowIds.size === 0) return flowIdMapping;
    console.log(`[Copy Linked Flows] Encontrados ${linkedFlowIds.size} fluxos anexados para copiar (inheritAllowReshare=${inheritAllowReshare})`);
    
    for (const oldFlowId of linkedFlowIds) {
        try {
            const [linkedFlow] = await sqlWithRetry(
                'SELECT name, nodes, share_allow_reshare FROM flows WHERE id = $1 AND seller_id = $2',
                [oldFlowId, originalSellerId]
            );
            
            if (linkedFlow) {
                let linkedFlowNodes = JSON.parse(JSON.stringify(linkedFlow.nodes));
                
                // Se não deve copiar mídias, limpar mídias do fluxo anexado também
                if (!shouldCopyMedia) {
                    const linkedNodesArray = linkedFlowNodes?.nodes || [];
                    if (linkedNodesArray.length > 0) {
                        console.log(`[Copy Linked Flows] Removendo mídias do fluxo anexado ${oldFlowId}`);
                        linkedFlowNodes.nodes = cleanMediaReferences(linkedNodesArray);
                    }
                }
                
                // Decidir qual valor de share_allow_reshare usar
                // Se inheritAllowReshare=true, usa o valor herdado; senão usa o do fluxo anexado original
                const allowReshare = inheritAllowReshare || (linkedFlow.share_allow_reshare || false);
                
                const newFlowName = `${linkedFlow.name} (Anexado)`;
                const [newFlow] = await sqlWithRetry(
                    'INSERT INTO flows (seller_id, bot_id, name, nodes, share_allow_reshare) VALUES ($1, $2, $3, $4, $5) RETURNING id',
                    [newSellerId, newBotId, newFlowName, linkedFlowNodes, allowReshare]
                );
                flowIdMapping[oldFlowId] = newFlow.id;
                console.log(`[Copy Linked Flows] Fluxo ${oldFlowId} copiado para ${newFlow.id} (allow_reshare=${allowReshare})`);
            }
        } catch (error) {
            console.error(`[Copy Linked Flows] Erro ao copiar fluxo ${oldFlowId}:`, error.message);
        }
    }
    
    return flowIdMapping;
}

/**
 * Copia mídias referenciadas nos nós
 */
async function copyMediaFiles(nodes, originalSellerId, newSellerId) {
    const mediaMapping = {};
    if (!nodes || !Array.isArray(nodes)) {
        console.log('[Copy Media] Nós inválidos ou vazios');
        return mediaMapping;
    }
    
    const mediaFileIds = new Set();
    const mediaFields = ['image', 'imageUrl', 'video', 'videoUrl', 'audio', 'audioUrl', 'file_id'];
    
    for (const node of nodes) {
        const data = node.data || {};
        
        // Verificar campos dentro de actions
        if (data.actions && Array.isArray(data.actions)) {
            for (const action of data.actions) {
                if (action.data) {
                    for (const field of mediaFields) {
                        if (action.data[field]) {
                            mediaFileIds.add(action.data[field]);
                            console.log(`[Copy Media] Detectado ${field}: ${action.data[field]} em action do nó ${node.id}`);
                        }
                    }
                }
            }
        }
    }
    
    if (mediaFileIds.size === 0) return mediaMapping;
    console.log(`[Copy Media] Encontrados ${mediaFileIds.size} arquivos de mídia para copiar`);
    
    for (const fileId of mediaFileIds) {
        try {
            const [originalMedia] = await sqlWithRetry(
                'SELECT file_name, file_id, file_type, thumbnail_file_id FROM media_library WHERE file_id = $1 AND seller_id = $2',
                [fileId, originalSellerId]
            );
            
            if (originalMedia) {
                const [existingMedia] = await sqlWithRetry(
                    'SELECT id FROM media_library WHERE file_id = $1 AND seller_id = $2',
                    [originalMedia.file_id, newSellerId]
                );
                
                if (!existingMedia) {
                    const newFileName = `${originalMedia.file_name} (Importado)`;
                    await sqlWithRetry(`
                        INSERT INTO media_library (seller_id, file_name, file_id, file_type, thumbnail_file_id)
                        VALUES ($1, $2, $3, $4, $5)
                    `, [newSellerId, newFileName, originalMedia.file_id, originalMedia.file_type, originalMedia.thumbnail_file_id]);
                    
                    console.log(`[Copy Media] Copiado registro de mídia ${fileId}`);
                }
                mediaMapping[fileId] = fileId;
            }
        } catch (error) {
            console.error(`[Copy Media] Erro ao copiar mídia ${fileId}:`, error.message);
        }
    }
    
    return mediaMapping;
}

/**
 * Atualiza referências de fluxos anexados nos nós
 */
function updateNodeReferences(nodes, flowIdMapping) {
    if (!nodes || !Array.isArray(nodes) || Object.keys(flowIdMapping).length === 0) {
        return nodes;
    }
    
    return nodes.map(node => {
        const updatedData = { ...node.data };
        let updated = false;
        
        // Atualizar targetFlowId dentro de actions
        if (updatedData.actions && Array.isArray(updatedData.actions)) {
            updatedData.actions = updatedData.actions.map(action => {
                if (action.type === 'forward_flow' && action.data?.targetFlowId) {
                    const oldId = action.data.targetFlowId;
                    if (flowIdMapping[oldId]) {
                        console.log(`[Update Refs] Atualizado targetFlowId de ${oldId} para ${flowIdMapping[oldId]} em action do nó ${node.id}`);
                        updated = true;
                        return {
                            ...action,
                            data: {
                                ...action.data,
                                targetFlowId: flowIdMapping[oldId]
                            }
                        };
                    }
                }
                return action;
            });
        }
        
        return updated ? { ...node, data: updatedData } : node;
    });
}
/**
 * Remove referências de mídia dos nós
 */
function cleanMediaReferences(nodes) {
    if (!nodes || !Array.isArray(nodes)) {
        console.log('[Clean Media] Nós inválidos ou vazios');
        return nodes;
    }
    
    console.log(`[Clean Media] Iniciando limpeza em ${nodes.length} nós...`);
    
    return nodes.map(node => {
        const cleanedData = { ...node.data };
        let cleaned = false;
        
        // Limpar campos de mídia dentro de actions
        if (cleanedData.actions && Array.isArray(cleanedData.actions)) {
            cleanedData.actions = cleanedData.actions.map(action => {
                if (action.type === 'image' || action.type === 'video' || action.type === 'audio') {
                    console.log(`[Clean Media] Removendo action de mídia (${action.type}) do nó ${node.id}`);
                    cleaned = true;
                    return null;
                }
                
                // Limpar campos de mídia dentro do action.data
                if (action.data) {
                    const cleanedActionData = { ...action.data };
                    const mediaFields = ['image', 'imageUrl', 'video', 'videoUrl', 'audio', 'audioUrl', 'file_id'];
                    for (const field of mediaFields) {
                        if (cleanedActionData[field]) {
                            console.log(`[Clean Media] Removendo ${field} de action em nó ${node.id}: ${cleanedActionData[field]}`);
                            delete cleanedActionData[field];
                            cleaned = true;
                        }
                    }
                    return { ...action, data: cleanedActionData };
                }
                
                return action;
            }).filter(action => action !== null);
        }
        
        if (cleaned) {
            console.log(`[Clean Media] Nó ${node.id} limpo`);
        }
        
        return {
            ...node,
            data: cleanedData
        };
    });
}

/**
 * Remove referências de fluxos anexados dos nós
 */
function cleanLinkedFlowReferences(nodes) {
    if (!nodes || !Array.isArray(nodes)) {
        return nodes;
    }
    
    console.log('[Clean Linked Flows] Removendo referências de fluxos anexados dos nós...');
    
    return nodes.map(node => {
        const cleanedData = { ...node.data };
        let cleaned = false;
        
        // Remover actions de forward_flow
        if (cleanedData.actions && Array.isArray(cleanedData.actions)) {
            const originalLength = cleanedData.actions.length;
            cleanedData.actions = cleanedData.actions.filter(action => {
                if (action.type === 'forward_flow') {
                    console.log(`[Clean Linked Flows] Removendo action forward_flow do nó ${node.id}`);
                    cleaned = true;
                    return false;
                }
                return true;
            });
            
            if (cleanedData.actions.length !== originalLength) {
                cleaned = true;
            }
        }
        
        if (cleaned) {
            console.log(`[Clean Linked Flows] Referências removidas do nó ${node.id}`);
        }
        
        return {
            ...node,
            data: cleanedData
        };
    });
}

// Endpoint 19: Importar fluxo por link (GRATUITO)
app.post('/api/flows/import-from-link', authenticateJwt, async (req, res) => {
    const { shareableLinkId, botId } = req.body;
    const sellerId = req.user.id;
    try {
        if (!botId || !shareableLinkId) return res.status(400).json({ message: 'ID do link e ID do bot são obrigatórios.' });
        
        // Buscar fluxo original com configurações de compartilhamento
        const [originalFlow] = await sqlWithRetry(`
            SELECT name, nodes, share_price_cents, seller_id,
                   share_bundle_linked_flows, share_bundle_media, share_allow_reshare
            FROM flows WHERE shareable_link_id = $1
        `, [shareableLinkId]);
        
        if (!originalFlow) return res.status(404).json({ message: 'Link de compartilhamento inválido ou expirado.' });

        if (originalFlow.share_price_cents > 0) {
            return res.status(400).json({ message: 'Este fluxo é pago e não pode ser importado por esta via.' });
        }

        console.log(`[Import Free Flow] Configurações: bundle_linked_flows=${originalFlow.share_bundle_linked_flows}, bundle_media=${originalFlow.share_bundle_media}`);

        // Deep clone para evitar mutar o original
        let processedNodes = JSON.parse(JSON.stringify(originalFlow.nodes));
        
        // Verificar se a estrutura tem o array de nós
        const nodesArray = processedNodes?.nodes || [];
        console.log(`[Import Free Flow] Estrutura de nós: ${nodesArray.length} nós encontrados`);
        
        // Flags independentes
        const shouldCopyLinkedFlows = originalFlow.share_bundle_linked_flows === true;
        const shouldCopyMedia = originalFlow.share_bundle_media === true;
        
        // Copiar fluxos anexados se configurado
        if (shouldCopyLinkedFlows) {
            console.log('[Import Free Flow] Copiando fluxos anexados...');
            const flowIdMapping = await copyLinkedFlows(
                nodesArray,
                originalFlow.seller_id,
                sellerId,
                botId,
                shouldCopyMedia, // Passar flag para limpar mídias dos fluxos anexados se necessário
                originalFlow.share_allow_reshare || false // Herdar share_allow_reshare do fluxo principal
            );
            processedNodes.nodes = updateNodeReferences(nodesArray, flowIdMapping);
        } else {
            console.log('[Import Free Flow] NÃO copiando fluxos anexados (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanLinkedFlowReferences(nodesArray);
        }
        
        // Copiar mídias se configurado
        if (shouldCopyMedia) {
            console.log('[Import Free Flow] Copiando mídias...');
            await copyMediaFiles(
                nodesArray,
                originalFlow.seller_id,
                sellerId
            );
        } else {
            console.log('[Import Free Flow] NÃO copiando mídias (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanMediaReferences(processedNodes.nodes || nodesArray);
        }

        const newFlowName = `${originalFlow.name} (Importado)`;
        const [newFlow] = await sqlWithRetry(
            `INSERT INTO flows (seller_id, bot_id, name, nodes, share_allow_reshare) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
            [sellerId, botId, newFlowName, processedNodes, originalFlow.share_allow_reshare || false]
        );
        
        console.log(`[Import Free Flow] Fluxo importado com sucesso (allow_reshare=${originalFlow.share_allow_reshare})`);
        res.status(201).json(newFlow);
    } catch (error) {
        console.error("Erro ao importar fluxo por link:", error);
        res.status(500).json({ message: 'Erro ao importar fluxo por link: ' + error.message });
    }
});

// Endpoint 21.1: Gerar PIX para importar fluxo pago
app.post('/api/flows/generate-pix-import', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { shareableLinkId } = req.body;

    console.log(`[Generate PIX Import] Seller: ${sellerId}, Link: ${shareableLinkId}`);

    try {
        // Buscar dados do fluxo compartilhado
        const [sharedFlow] = await sqlWithRetry(
            'SELECT name, share_price_cents FROM flows WHERE shareable_link_id = $1',
            [shareableLinkId]
        );

        if (!sharedFlow) {
            return res.status(404).json({ message: 'Link de compartilhamento inválido.' });
        }

        if (sharedFlow.share_price_cents <= 0) {
            return res.status(400).json({ message: 'Este fluxo é gratuito. Use a importação gratuita.' });
        }

        // Buscar dados do vendedor (quem está importando)
        const [seller] = await sqlWithRetry(
            'SELECT * FROM sellers WHERE id = $1',
            [sellerId]
        );

        if (!seller) {
            return res.status(404).json({ message: 'Vendedor não encontrado.' });
        }

        // Gerar PIX usando a função de fallback
        const host = req.headers.host || 'localhost';
        const ipAddress = req.ip || req.headers['x-forwarded-for'] || 'unknown';
        
        // Criar um click temporário para tracking (necessário pela constraint NOT NULL)
        console.log(`[Generate PIX Import] Criando click temporário...`);
        const [click] = await sqlWithRetry(
            `INSERT INTO clicks (seller_id, ip_address, user_agent) 
             VALUES ($1, $2, $3) 
             RETURNING id`,
            [sellerId, ipAddress, req.headers['user-agent'] || 'Flow Import']
        );
        console.log(`[Generate PIX Import] Click criado: ${click.id}`);
        
        console.log(`[Generate PIX Import] Gerando PIX com fallback...`);
        const pixResult = await generatePixWithFallback(
            seller, 
            sharedFlow.share_price_cents, 
            host, 
            seller.api_key, 
            ipAddress, 
            click.id
        );
        console.log(`[Generate PIX Import] PIX gerado com sucesso. Pix Transaction ID: ${pixResult.internal_transaction_id}`);

        // Criar registro na tabela flow_purchase_transactions
        const [purchaseTransaction] = await sqlWithRetry(
            `INSERT INTO flow_purchase_transactions (
                buyer_id, flow_share_link_id, flow_name, price_cents, pix_value,
                qr_code_text, qr_code_base64, provider, provider_transaction_id,
                pix_transaction_id, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id`,
            [
                sellerId,
                shareableLinkId,
                sharedFlow.name,
                sharedFlow.share_price_cents,
                sharedFlow.share_price_cents / 100,
                pixResult.qr_code_text,
                pixResult.qr_code_base64,
                pixResult.provider,
                pixResult.transaction_id,
                pixResult.internal_transaction_id,
                'pending'
            ]
        );
        
        console.log(`[Generate PIX Import] Purchase transaction criada: ${purchaseTransaction.id}`);

        res.status(200).json({
            qr_code_text: pixResult.qr_code_text,
            qr_code_base64: pixResult.qr_code_base64,
            purchase_transaction_id: purchaseTransaction.id,
            value: sharedFlow.share_price_cents / 100,
            flow_name: sharedFlow.name
        });

    } catch (error) {
        console.error('[Generate PIX Import] Erro:', error);
        res.status(500).json({ message: error.message || 'Erro ao gerar PIX para importação.' });
    }
});

// Endpoint 21.2: Verificar status do pagamento (polling)
app.get('/api/flows/check-payment/:purchaseTransactionId', authenticateJwt, async (req, res) => {
    const { purchaseTransactionId } = req.params;
    const sellerId = req.user.id;

    try {
        const [purchase] = await sqlWithRetry(
            `SELECT fpt.*, pt.status as pix_status, pt.paid_at
             FROM flow_purchase_transactions fpt
             LEFT JOIN pix_transactions pt ON fpt.pix_transaction_id = pt.id
             WHERE fpt.id = $1`,
            [purchaseTransactionId]
        );

        if (!purchase) {
            return res.status(404).json({ message: 'Transação não encontrada.' });
        }

        // Verificar se a transação pertence ao vendedor correto
        if (purchase.buyer_id !== sellerId) {
            return res.status(403).json({ message: 'Acesso negado a esta transação.' });
        }

        // Atualizar status se PIX foi pago mas purchase ainda está pending
        if (purchase.pix_status === 'paid' && purchase.status === 'pending') {
            await sqlWithRetry(
                `UPDATE flow_purchase_transactions 
                 SET status = 'paid', paid_at = NOW() 
                 WHERE id = $1`,
                [purchaseTransactionId]
            );
            purchase.status = 'paid';
            purchase.paid_at = new Date();
        }

        res.status(200).json({
            status: purchase.status,
            paid_at: purchase.paid_at,
            is_paid: purchase.status === 'paid' || purchase.status === 'imported'
        });

    } catch (error) {
        console.error('[Check Payment] Erro:', error);
        res.status(500).json({ message: 'Erro ao verificar status do pagamento.' });
    }
});
// Endpoint 21.3: Importar fluxo pago após confirmação de pagamento
app.post('/api/flows/import-paid', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const { purchaseTransactionId, botId } = req.body;

    console.log(`[Import Paid Flow] Purchase Transaction: ${purchaseTransactionId}, Bot: ${botId}, Seller: ${sellerId}`);

    try {
        // Verificar se o pagamento foi confirmado
        const [purchase] = await sqlWithRetry(
            `SELECT * FROM flow_purchase_transactions WHERE id = $1`,
            [purchaseTransactionId]
        );

        if (!purchase) {
            return res.status(404).json({ message: 'Transação de compra não encontrada.' });
        }

        if (purchase.buyer_id !== sellerId) {
            return res.status(403).json({ message: 'Acesso negado a esta transação.' });
        }

        if (purchase.status !== 'paid' && purchase.status !== 'imported') {
            return res.status(400).json({ message: 'Pagamento ainda não foi confirmado.' });
        }

        // Se já foi importado, retornar sucesso
        if (purchase.status === 'imported') {
            console.log(`[Import Paid Flow] Fluxo já foi importado anteriormente`);
            return res.status(200).json({ message: 'Fluxo já foi importado com sucesso.' });
        }

        const shareableLinkId = purchase.flow_share_link_id;

        // Buscar fluxo original
        const [originalFlow] = await sqlWithRetry(
            'SELECT * FROM flows WHERE shareable_link_id = $1',
            [shareableLinkId]
        );

        if (!originalFlow) {
            return res.status(404).json({ message: 'Fluxo compartilhado não encontrado.' });
        }

        // Copiar fluxo (mesma lógica do endpoint gratuito)
        // Deep clone para evitar mutar o original
        let processedNodes = JSON.parse(JSON.stringify(originalFlow.nodes));
        
        // Verificar se a estrutura tem o array de nós
        const nodesArray = processedNodes?.nodes || [];
        console.log(`[Import Paid Flow] Estrutura de nós: ${nodesArray.length} nós encontrados`);
        
        // Flags independentes
        const shouldCopyLinkedFlows = originalFlow.share_bundle_linked_flows === true;
        const shouldCopyMedia = originalFlow.share_bundle_media === true;
        
        console.log(`[Import Paid Flow] Configurações: bundle_linked_flows=${shouldCopyLinkedFlows}, bundle_media=${shouldCopyMedia}`);

        // Copiar fluxos anexados se configurado
        if (shouldCopyLinkedFlows) {
            console.log('[Import Paid Flow] Copiando fluxos anexados...');
            const flowIdMapping = await copyLinkedFlows(
                nodesArray,
                originalFlow.seller_id,
                sellerId,
                botId,
                shouldCopyMedia,
                originalFlow.share_allow_reshare || false // Herdar share_allow_reshare do fluxo principal
            );
            console.log(`[Import Paid Flow] Fluxos copiados. Mapeamento:`, flowIdMapping);
            processedNodes.nodes = updateNodeReferences(nodesArray, flowIdMapping);
        } else {
            console.log('[Import Paid Flow] NÃO copiando fluxos anexados (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanLinkedFlowReferences(nodesArray);
        }
        
        // Copiar mídias se configurado
        if (shouldCopyMedia) {
            console.log('[Import Paid Flow] Copiando mídias...');
            await copyMediaFiles(
                nodesArray,
                originalFlow.seller_id,
                sellerId
            );
        } else {
            console.log('[Import Paid Flow] NÃO copiando mídias (flag desabilitada) - removendo referências');
            processedNodes.nodes = cleanMediaReferences(processedNodes.nodes || nodesArray);
        }

        const newFlowName = `${originalFlow.name} (Importado)`;
        const [newFlow] = await sqlWithRetry(
            `INSERT INTO flows (seller_id, bot_id, name, nodes, share_allow_reshare) VALUES ($1, $2, $3, $4, $5) RETURNING *`,
            [sellerId, botId, newFlowName, processedNodes, originalFlow.share_allow_reshare || false]
        );

        // Atualizar purchase transaction para status 'imported'
        await sqlWithRetry(
            `UPDATE flow_purchase_transactions 
             SET status = 'imported', imported_at = NOW() 
             WHERE id = $1`,
            [purchaseTransactionId]
        );

        console.log(`[Import Paid Flow] Fluxo importado com sucesso após pagamento confirmado`);
        res.status(201).json(newFlow);

    } catch (error) {
        console.error('[Import Paid Flow] Erro:', error);
        res.status(500).json({ message: 'Erro ao importar fluxo pago: ' + error.message });
    }
});

// Endpoint 22: Histórico de disparos
app.get('/api/disparos/history', authenticateJwt, async (req, res) => {
    try {
        const history = await sqlWithRetry(`
            SELECT 
                h.*,
                (SELECT COUNT(*) FROM disparo_log WHERE history_id = h.id AND status = 'CONVERTED') as conversions
            FROM 
                disparo_history h
            WHERE 
                h.seller_id = $1
            ORDER BY 
                h.created_at DESC;
        `, [req.user.id]);
        res.status(200).json(history);
    } catch (error) {
        res.status(500).json({ message: 'Erro ao buscar histórico de disparos.' });
    }
});
// Endpoint 23: Verificar conversões de disparos (modificado)
app.post('/api/disparos/check-conversions/:historyId', authenticateJwt, async (req, res) => {
    const { historyId } = req.params;
    const sellerId = req.user.id;

    try {
        const [seller] = await sqlWithRetry('SELECT * FROM sellers WHERE id = $1', [sellerId]);
        if (!seller) {
            return res.status(400).json({ message: "Vendedor não encontrado." });
        }

        const logs = await sqlWithRetry(
            `SELECT id, transaction_id FROM disparo_log WHERE history_id = $1 AND status != 'CONVERTED' AND transaction_id IS NOT NULL`,
            [historyId]
        );
        
        let updatedCount = 0;
        for (const log of logs) {
            try {
                // Buscar a transação no banco
                const [transaction] = await sqlWithRetry('SELECT * FROM pix_transactions WHERE provider_transaction_id = $1 OR pix_id = $1', [log.transaction_id]);
                if (!transaction) {
                    continue;
                }

                // Se já está paga, verificar se eventos foram enviados
                if (transaction.status === 'paid') {
                    // Se não tem meta_event_id, eventos podem não ter sido enviados - tentar enviar
                    if (!transaction.meta_event_id) {
                        console.log(`[Check Conversions] Transação ${transaction.id} está paga mas sem meta_event_id. Tentando buscar dados do provider e enviar eventos...`);
                        
                        // Tentar buscar dados do cliente do provider
                        let customerData = {};
                        try {
                            if (transaction.provider === 'syncpay') {
                                const syncPayToken = await getSyncPayAuthToken(seller);
                                const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                                    headers: { 'Authorization': `Bearer ${syncPayToken}` }
                                });
                                customerData = response.data.payer || {};
                            } else if (transaction.provider === 'pushinpay') {
                                const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                                customerData = { name: response.data.payer_name, document: response.data.payer_national_registration };
                            }
                            
                            // Tentar enviar eventos mesmo que a transação já esteja paga
                            if (Object.keys(customerData).length > 0) {
                                await handleSuccessfulPayment(transaction.id, customerData);
                            }
                        } catch (error) {
                            console.error(`[Check Conversions] Erro ao tentar enviar eventos para transação já paga:`, error.message);
                        }
                    }
                    
                    await sqlWithRetry(`UPDATE disparo_log SET status = 'CONVERTED' WHERE id = $1`, [log.id]);
                    updatedCount++;
                    continue;
                }

                // Se não está paga, verificar no provedor
                let providerStatus, customerData = {};
                try {
                    if (transaction.provider === 'syncpay') {
                        const syncPayToken = await getSyncPayAuthToken(seller);
                        const response = await axios.get(`${SYNCPAY_API_BASE_URL}/api/partner/v1/transaction/${transaction.provider_transaction_id}`, {
                            headers: { 'Authorization': `Bearer ${syncPayToken}` }
                        });
                        providerStatus = response.data.status;
                        customerData = response.data.payer;
                    } else if (transaction.provider === 'pushinpay') {
                        const response = await axios.get(`https://api.pushinpay.com.br/api/transactions/${transaction.provider_transaction_id}`, { headers: { Authorization: `Bearer ${seller.pushinpay_token}` } });
                        providerStatus = response.data.status;
                        customerData = { name: response.data.payer_name, document: response.data.payer_national_registration };
                    } else if (transaction.provider === 'oasyfy' || transaction.provider === 'cnpay' || transaction.provider === 'brpix') {
                        // Não consultamos, depende do webhook
                        continue;
                    }
                } catch (providerError) {
                    console.error(`Falha ao consultar o provedor para a transação ${transaction.id}:`, providerError.message);
                    continue;
                }

                if (providerStatus === 'paid' || providerStatus === 'COMPLETED') {
                    await handleSuccessfulPayment(transaction.id, customerData);
                    await sqlWithRetry(`UPDATE disparo_log SET status = 'CONVERTED' WHERE id = $1`, [log.id]);
                    updatedCount++;
                }
            } catch(e) {
                // Ignora erros de PIX não encontrado, etc.
            }
            await new Promise(resolve => setTimeout(resolve, 200)); // Rate limiting
        }

        res.status(200).json({ message: `Verificação concluída. ${updatedCount} novas conversões encontradas.` });
    } catch (error) {
        res.status(500).json({ message: 'Erro ao verificar conversões.' });
    }
});

// Endpoint 24: CRON para processar fila de disparos (modificado)
// [REMOVIDO] Rota de CRON de disparos (não utilizada)

// Health check endpoint para Render
app.get('/api/health', (req, res) => {
    res.status(200).json({ 
        status: 'OK', 
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV || 'development',
        port: PORT
    });
});


// ==========================================================
// ROTAS CHECKOUTS HOSPEDADOS E PÁGINAS DE OBRIGADO
// ==========================================================

// LISTAR CHECKOUTS
app.get('/api/checkouts', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        // Select the ID, extract the main title from the config JSON, and the creation date
        const checkouts = await sqlTx`
            SELECT
                id,
                config->'content'->>'main_title' as name, -- Extracts 'main_title' from the 'content' object within 'config'
                created_at
            FROM hosted_checkouts
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(checkouts);
    } catch (error) {
        console.error("Erro ao listar checkouts hospedados:", error);
        res.status(500).json({ message: 'Erro ao buscar seus checkouts.' });
    }
});

// EDITAR (ATUALIZAR) CHECKOUT
app.put('/api/checkouts/:checkoutId', authenticateJwt, async (req, res) => {
    const { checkoutId } = req.params;
    const sellerId = req.user.id;
    const newConfig = req.body; // Expects the full updated config object from the frontend

    // Basic validation
    if (!checkoutId.startsWith('cko_')) {
        return res.status(400).json({ message: 'ID de checkout inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }

    try {
        // Update the config JSON and updated_at timestamp for the specific checkout ID and seller ID
        const result = await sqlTx`
            UPDATE hosted_checkouts
            SET config = ${sqlTx.json(newConfig)}, updated_at = NOW()
            WHERE id = ${checkoutId} AND seller_id = ${sellerId}
            RETURNING id; -- Confirma a atualização
        `;

        // Check if any row was updated
        if (result.length === 0) {
            return res.status(404).json({ message: 'Checkout não encontrado ou você não tem permissão para editá-lo.' });
        }

        res.status(200).json({ message: 'Checkout atualizado com sucesso!', checkoutId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar checkout ${checkoutId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar o checkout.' });
    }
});


// ROTA CRIAÇÃO CHECKOUT HOSPEDADO
app.post('/api/checkouts/create-hosted', authenticateJwt, async (req, res) => {
    const sellerId = req.user.id;
    const config = req.body; // Expects the full config object from the frontend

    // Generate a unique ID for the new checkout, prefixed for easy identification
    const checkoutId = `cko_${uuidv4()}`;

    try {
        // Insert into the hosted_checkouts table
        await sqlTx`
            INSERT INTO hosted_checkouts (id, seller_id, config)
            VALUES (${checkoutId}, ${sellerId}, ${sqlTx.json(config)});
        `;

        // Return the generated ID to the frontend
        res.status(201).json({
            message: 'Checkout hospedado criado com sucesso!',
            checkoutId: checkoutId
        });

    } catch (error) {
        console.error("Erro ao criar checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno ao criar o checkout.' });
    }
});

// ROTA PÁGINA DE OFERTA
app.get('/api/oferta/:checkoutId', async (req, res) => {
    const { checkoutId } = req.params;
    const { click_id, cid } = req.query; // Captura IDs de clique da URL

    try {
        // 1. Busca a configuração do checkout e o ID do vendedor
        const [checkout] = await sqlTx`
            SELECT seller_id, config FROM hosted_checkouts WHERE id = ${checkoutId}
        `;

        if (!checkout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        let finalClickId = click_id || cid || null;

        // 2. Lógica para tráfego orgânico (nenhum ID de clique na URL)
        if (!finalClickId) {
            console.log(`[Organic Traffic] Nenhum click_id encontrado para checkout ${checkoutId}. Gerando um novo.`);
            
            const ip_address = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
            const user_agent = req.headers['user-agent'];

            // Insere um novo clique para a visita orgânica
            const [newClick] = await sqlTx`
                INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent, is_organic)
                VALUES (${checkout.seller_id}, ${checkoutId}, ${ip_address}, ${user_agent}, TRUE)
                RETURNING id;
            `;

            // Gera um click_id único e amigável no formato /start
            finalClickId = `org_${newClick.id.toString().padStart(7, '0')}`;

            // Atualiza o registro com o novo click_id gerado (com prefixo /start para consistência)
            await sqlTx`
                UPDATE clicks SET click_id = ${`/start ${finalClickId}`} WHERE id = ${newClick.id}
            `;
             console.log(`[Organic Traffic] Novo click_id gerado e associado: ${finalClickId}`);
        }

        // 3. Retorna a configuração e o click_id final para o frontend
        let parsedConfig;
        try {
            parsedConfig = parseJsonField(checkout.config, `hosted_checkouts:${checkoutId}`);
        } catch {
            return res.status(500).json({ message: 'Configuração inválida do checkout.' });
        }
        res.status(200).json({
            config: parsedConfig,
            click_id: finalClickId // Envia o ID existente ou o novo ID orgânico
        });

    } catch (error) {
        console.error("Erro ao buscar dados do checkout ou processar tráfego orgânico:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});

app.post('/api/oferta/generate-pix', async (req, res) => {
    const { checkoutId, value_cents, click_id, customer, product } = req.body;

    if (!checkoutId || !value_cents) {
        return res.status(400).json({ message: 'Dados insuficientes para gerar o PIX.' });
    }

    try {
        // 1) Validar checkout e obter seller
        const [hostedCheckout] = await sqlTx`
            SELECT seller_id, config 
            FROM hosted_checkouts 
            WHERE id = ${checkoutId}
        `;
        if (!hostedCheckout) {
            return res.status(404).json({ message: 'Checkout não encontrado.' });
        }

        const sellerId = hostedCheckout.seller_id;
        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller || !seller.api_key) {
            return res.status(400).json({ message: 'API Key não configurada para o vendedor.' });
        }

        // 2) Garantir que há um click_id associado e buscar dados do click
        const requestIp = req.headers['x-forwarded-for']?.split(',')[0].trim() || req.socket.remoteAddress;
        const user_agent = req.headers['user-agent'];
        let finalClickId = click_id;
        let clickRecord = null;

        if (finalClickId) {
            const cleanClickId = finalClickId.replace('/start ', '');
            const dbClickId = cleanClickId.startsWith('/start ') ? cleanClickId : `/start ${cleanClickId}`;
            const [existingClick] = await sqlTx`
                SELECT * FROM clicks 
                WHERE click_id = ${dbClickId} AND seller_id = ${sellerId}
            `;

            if (!existingClick) {
                console.log(`[Checkout PIX] Click_id ${cleanClickId} não encontrado, criando novo registro para checkout`);
                const [newClick] = await sqlTx`
                    INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent, click_id, is_organic)
                    VALUES (${sellerId}, ${checkoutId}, ${requestIp}, ${user_agent}, ${dbClickId}, FALSE)
                    RETURNING *;
                `;
                finalClickId = cleanClickId;
                clickRecord = newClick;
            } else {
                finalClickId = cleanClickId;
                clickRecord = existingClick;
                if (!clickRecord.checkout_id) {
                    await sqlTx`UPDATE clicks SET checkout_id = ${checkoutId} WHERE id = ${clickRecord.id}`;
                    clickRecord = { ...clickRecord, checkout_id: checkoutId };
                }
                console.log(`[Checkout PIX] Usando click_id existente: ${cleanClickId}`);
            }
        } else {
            console.log(`[Checkout PIX] Nenhum click_id fornecido, gerando orgânico`);
            const [newClick] = await sqlTx`
                INSERT INTO clicks (seller_id, checkout_id, ip_address, user_agent, is_organic)
                VALUES (${sellerId}, ${checkoutId}, ${requestIp}, ${user_agent}, TRUE)
                RETURNING *;
            `;
            finalClickId = `org_${newClick.id.toString().padStart(7, '0')}`;
            await sqlTx`UPDATE clicks SET click_id = ${`/start ${finalClickId}`} WHERE id = ${newClick.id}`;
            clickRecord = { ...newClick, click_id: `/start ${finalClickId}` };
            console.log(`[Checkout PIX] Click_id orgânico gerado: ${finalClickId}`);
        }

        if (!clickRecord) {
            return res.status(500).json({ message: 'Não foi possível criar ou localizar o clique.' });
        }

        const [click] = await sqlTx`SELECT * FROM clicks WHERE id = ${clickRecord.id}`;
        if (!click) {
            return res.status(500).json({ message: 'Clique associado não encontrado após criação.' });
        }

        const hostForPix = (() => {
            if (req.headers.host) return req.headers.host;
            if (process.env.HOTTRACK_API_URL) {
                try {
                    return new URL(process.env.HOTTRACK_API_URL).host;
                } catch (_) {
                    return 'localhost';
                }
            }
            return 'localhost';
        })();

        const pixIpAddress = click.ip_address || requestIp;
        const pixResult = await generatePixWithFallback(
            seller,
            value_cents,
            hostForPix,
            seller.api_key,
            pixIpAddress,
            click.id
        );

        // 3) Disparar eventos pós-geração de PIX
        const customerDataForUtmify = customer || { name: "Cliente Interessado", email: "cliente@email.com" };

        let checkoutConfigJson = null;
        if (hostedCheckout.config) {
            try {
                checkoutConfigJson = parseJsonField(hostedCheckout.config, `hosted_checkouts:${checkoutId}`);
            } catch (configError) {
                console.warn(`[Checkout PIX] Configuração inválida para checkout ${checkoutId}:`, configError.message);
                checkoutConfigJson = null;
            }
        }

        const productDataForUtmify = product || {
            id: checkoutConfigJson?.product?.id || "prod_1",
            name: checkoutConfigJson?.content?.main_title || checkoutConfigJson?.product?.name || "Produto Ofertado"
        };

        const transactionDataForEvents = {
            provider_transaction_id: pixResult.transaction_id,
            pix_value: value_cents / 100,
            created_at: new Date()
        };

        await sendMetaEvent(
            'InitiateCheckout',
            { ...click, checkout_id: checkoutId },
            { id: pixResult.internal_transaction_id, pix_value: value_cents / 100 },
            customer || null
        );

        await sendEventToUtmify(
            'waiting_payment',
            { ...click, checkout_id: checkoutId },
            transactionDataForEvents,
            seller,
            customerDataForUtmify,
            productDataForUtmify
        );

        const { internal_transaction_id, ...apiResponse } = pixResult;
        return res.status(200).json(apiResponse);
    } catch (error) {
        console.error('[Checkout PIX] Erro geral ao gerar PIX:', error.response?.data || error.message);
        const status = error.response?.status || 500;
        const message = error.response?.data?.message || error.message || 'Não foi possível gerar o PIX no momento.';
        return res.status(status).json({ message });
    }
});

// ==========================================================
// ROTAS PÁGINAS DE OBRIGADO
// ==========================================================
// ROTA CRIAÇÃO CHECKOUT HOSPEDADO (create-hosted)
app.post('/api/checkouts/create-hosted', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects the full config object from the frontend

    // Generate a unique ID for the new checkout, prefixed for easy identification
    const checkoutId = `cko_${uuidv4()}`;

    try {
        // Insert into the hosted_checkouts table
        await sqlTx`
            INSERT INTO hosted_checkouts (id, seller_id, config)
            VALUES (${checkoutId}, ${sellerId}, ${sqlTx.json(config)});
        `;

        // Return the generated ID to the frontend
        res.status(201).json({
            message: 'Checkout hospedado criado com sucesso!',
            checkoutId: checkoutId
        });

    } catch (error) {
        console.error("Erro ao criar checkout hospedado:", error);
        res.status(500).json({ message: 'Erro interno ao criar o checkout.' });
    }
});

// ATUALIZAR UMA PÁGINA DE OBRIGADO
app.put('/api/thank-you-pages/:pageId', authenticateApiKey, async (req, res) => { // Usando ApiKey para consistência com create
    const { pageId } = req.params;
    const sellerId = req.sellerId;
    const newConfig = req.body; // Espera o objeto config atualizado

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }
    if (!newConfig || typeof newConfig !== 'object') {
        return res.status(400).json({ message: 'Configuração inválida fornecida.' });
    }
     // Valida campos essenciais no newConfig
    if (!newConfig.page_name || !newConfig.purchase_value || !newConfig.pixel_id || !newConfig.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para atualizar a página.' });
    }

    try {
        const result = await sqlTx`
            UPDATE thank_you_pages
            SET config = ${sqlTx.json(newConfig)}, updated_at = NOW()
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
            return res.status(404).json({ message: 'Página não encontrada ou você não tem permissão para editá-la.' });
        }
        res.status(200).json({ message: 'Página de obrigado atualizada com sucesso!', pageId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar a página.' });
    }
});

app.post('/api/thank-you-pages/create', authenticateApiKey, async (req, res) => {
    const sellerId = req.sellerId;
    const config = req.body; // Expects config object { page_name, purchase_value, pixel_id, redirect_url, utmify_integration_id? }

    // Validate essential fields
    if (!config.page_name || !config.purchase_value || !config.pixel_id || !config.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para criar a página.' });
    }

    // Generate unique ID for the page
    const pageId = `ty_${uuidv4()}`;

    try {
        // Insert the configuration into the database
        await sqlTx`
            INSERT INTO thank_you_pages (id, seller_id, config)
            VALUES (${pageId}, ${sellerId}, ${sqlTx.json(config)});
        `;

        res.status(201).json({
            message: 'Página de obrigado criada com sucesso!',
            pageId: pageId // Return the generated ID
        });

    } catch (error) {
        console.error("Erro ao criar página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno ao criar a página.' });
    }
});

// Fetch configuration for a specific Thank You Page
app.get('/api/obrigado/:pageId', async (req, res) => {
    const { pageId } = req.params;

    try {
        const [page] = await sqlTx`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada.' });
        }

        let parsedConfig;
        try {
            parsedConfig = parseJsonField(page.config, `thank_you_pages:${pageId}`);
        } catch {
            return res.status(500).json({ message: 'Configuração inválida da página de obrigado.' });
        }

        res.status(200).json({
            config: parsedConfig,
        });

    } catch (error) {
        console.error("Erro ao buscar dados da página de obrigado:", error);
        res.status(500).json({ message: 'Erro interno no servidor.' });
    }
});
// Trigger Utmify event from the Thank You Page frontend
app.post('/api/thank-you-pages/fire-utmify', async (req, res) => {
    const { pageId, trackingParameters, customerData } = req.body;

    try {
        const [page] = await sqlTx`
            SELECT seller_id, config FROM thank_you_pages WHERE id = ${pageId}
        `;

        let parsedConfig;
        try {
            parsedConfig = parseJsonField(page?.config, `thank_you_pages:${pageId}`);
        } catch {
            return res.status(500).json({ message: 'Configuração inválida da página de obrigado.' });
        }

        if (!page || !parsedConfig?.utmify_integration_id) {
            return res.status(404).json({ message: 'Página ou integração Utmify não configurada.' });
        }

        const sellerId = page.seller_id;
        const utmifyIntegrationId = parsedConfig.utmify_integration_id;

        const [seller] = await sqlTx`SELECT * FROM sellers WHERE id = ${sellerId}`;
        if (!seller) {
            return res.status(404).json({ message: 'Vendedor não encontrado.' });
        }

        const utmifyData = {
            integration_id: utmifyIntegrationId,
            event_name: 'Purchase',
            customer_data: {
                email: customerData.email || '',
                phone: customerData.phone || '',
                name: customerData.name || '',
                ...trackingParameters
            },
            purchase_data: {
                value: parsedConfig.purchase_value,
                currency: 'BRL'
            }
        };

        const utmifyResponse = await fetch('https://api.utmify.com.br/v1/events', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${seller.utmify_token}`
            },
            body: JSON.stringify(utmifyData)
        });

        if (!utmifyResponse.ok) {
            throw new Error(`Utmify API error: ${utmifyResponse.status}`);
        }

        res.status(200).json({ message: 'Evento enviado para Utmify com sucesso!' });

    } catch (error) {
        console.error(`[Utmify TY Page Error]`, error.response?.data || error.message);
        res.status(500).json({ message: 'Erro ao enviar evento para Utmify.' });
    }
});

// LISTAR PÁGINAS DE OBRIGADO DO VENDEDOR
app.get('/api/thank-you-pages', authenticateJwt, async (req, res) => {
    try {
        const sellerId = req.user.id;
        const pages = await sqlTx`
            SELECT
                id,
                config->>'page_name' as name,
                created_at
            FROM thank_you_pages
            WHERE seller_id = ${sellerId}
            ORDER BY created_at DESC;
        `;
        res.status(200).json(pages);
    } catch (error) {
        console.error("Erro ao listar páginas de obrigado:", error);
        res.status(500).json({ message: 'Erro ao buscar suas páginas de obrigado.' });
    }
});

// BUSCAR UMA PÁGINA DE OBRIGADO ESPECÍFICA
app.get('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;
    try {
        const [page] = await sqlTx`
            SELECT id, config FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId};
        `;
        if (!page) {
            return res.status(404).json({ message: 'Página de obrigado não encontrada ou não pertence a você.' });
        }
        let parsedConfig;
        try {
            parsedConfig = parseJsonField(page.config, `thank_you_pages:${pageId}`);
        } catch {
            return res.status(500).json({ message: 'Configuração inválida da página de obrigado.' });
        }
        res.status(200).json({ ...page, config: parsedConfig });
    } catch (error) {
        console.error(`Erro ao buscar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao buscar a página.' });
    }
});

// ATUALIZAR UMA PÁGINA DE OBRIGADO
app.put('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;
    const newConfig = req.body;

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }

    if (!newConfig.page_name || !newConfig.purchase_value || !newConfig.pixel_id || !newConfig.redirect_url) {
        return res.status(400).json({ message: 'Dados insuficientes para atualizar a página.' });
    }

    try {
        const result = await sqlTx`
            UPDATE thank_you_pages
            SET config = ${sqlTx.json(newConfig)}, updated_at = NOW()
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
            return res.status(404).json({ message: 'Página não encontrada ou você não tem permissão para editá-la.' });
        }
        res.status(200).json({ message: 'Página de obrigado atualizada com sucesso!', pageId: result[0].id });
    } catch (error) {
        console.error(`Erro ao atualizar página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao atualizar a página.' });
    }
});

// DELETAR UMA PÁGINA DE OBRIGADO
app.delete('/api/thank-you-pages/:pageId', authenticateJwt, async (req, res) => {
    const { pageId } = req.params;
    const sellerId = req.user.id;

    if (!pageId.startsWith('ty_')) {
        return res.status(400).json({ message: 'ID de página inválido.' });
    }

    try {
        const result = await sqlTx`
            DELETE FROM thank_you_pages
            WHERE id = ${pageId} AND seller_id = ${sellerId}
            RETURNING id;
        `;
        if (result.length === 0) {
             console.warn(`Tentativa de excluir página TY não encontrada ou não pertencente ao seller: ${pageId}, Seller: ${sellerId}`);
        }
        res.status(200).json({ message: 'Página de obrigado excluída com sucesso!' });
    } catch (error) {
        console.error(`Erro ao excluir página de obrigado ${pageId}:`, error);
        res.status(500).json({ message: 'Erro interno ao excluir a página.' });
    }
});

// ==========================================================
//          SERVIÇO DE ARQUIVOS ESTÁTICOS (FRONTEND & ADMIN)
// ==========================================================


const isProduction = process.env.NODE_ENV === 'production';

// Define os caminhos baseados no ambiente
const frontendPath = isProduction ? 'frontend' : '../frontend';
const adminFrontendPath = isProduction ? 'admin-frontend' : '../admin-frontend';

// Rota específica para verificação de email - DEVE VIR ANTES DO EXPRESS.STATIC
app.get('/verify-email', (req, res) => {
    console.log('Servindo página de verificação de email');
    res.sendFile(path.join(__dirname, frontendPath, 'verify-email.html'));
});

// Servir frontend estático
app.use(express.static(path.join(__dirname, frontendPath)));

// Servir admin estático em /admin
app.use('/admin', express.static(path.join(__dirname, adminFrontendPath)));

// Rota catch-all para SPA - DEVE SER A ÚLTIMA ROTA
app.get('*', (req, res) => {
  if (req.path.startsWith('/api')) {
    // API routes - não servir arquivos estáticos
    return res.status(404).json({ error: 'API route not found' });
  } else if (req.path.startsWith('/admin')) {
    // Admin routes
    return res.sendFile(path.join(__dirname, adminFrontendPath, 'index.html'));
  } else {
    // Frontend routes
    return res.sendFile(path.join(__dirname, frontendPath, 'index.html'));
  }
});

// Error handler global para requisições abortadas e outros erros
app.use((err, req, res, next) => {
    // Ignorar erros de requisição abortada silenciosamente
    if (err.message?.includes('request aborted') || 
        err.message?.includes('aborted') ||
        req.aborted ||
        err.code === 'ECONNRESET' ||
        err.code === 'EPIPE') {
        // Cliente fechou conexão - não é um erro real do servidor
        if (!res.headersSent) {
            return res.status(499).end();
        }
        return;
    }
    
    // Logar outros erros normalmente
    console.error('Erro não tratado:', err.message);
    if (!res.headersSent) {
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Inicialização do servidor
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 Servidor HotTrack rodando na porta ${PORT}`);
    console.log(`📱 API disponível em: http://localhost:${PORT}/api`);
    console.log(`🏥 Health check: http://localhost:${PORT}/api/health`);
    console.log(`🌍 Ambiente: ${process.env.NODE_ENV || 'development'}`);
});

module.exports = app;