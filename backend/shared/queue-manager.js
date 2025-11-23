const { Queue, Worker, QueueEvents } = require('bullmq');
const { getRedisConnection } = require('./redis');

class QueueManager {
    constructor() {
        this.queues = new Map();
        this.workers = new Map();
        this.connection = null;
    }

    /**
     * Inicializar conexão Redis (lazy loading)
     */
    _getConnection() {
        if (!this.connection) {
            this.connection = getRedisConnection();
        }
        return this.connection;
    }

    /**
     * Criar ou obter uma fila
     */
    getQueue(queueName) {
        if (!this.queues.has(queueName)) {
            const queue = new Queue(queueName, {
                connection: this._getConnection(),
                defaultJobOptions: {
                    attempts: 3,
                    backoff: {
                        type: 'exponential',
                        delay: 2000, // 2s, 4s, 8s
                    },
                    removeOnComplete: {
                        age: 24 * 3600, // Manter por 24 horas
                        count: 1000, // Manter últimos 1000 jobs
                    },
                    removeOnFail: {
                        age: 7 * 24 * 3600, // Manter falhas por 7 dias
                    },
                },
            });

            this.queues.set(queueName, queue);
            console.log(`[QUEUE] Fila ${queueName} criada`);
        }

        return this.queues.get(queueName);
    }

    /**
     * Adicionar tarefa à fila
     */
    async addJob(queueName, jobName, data, options = {}) {
        const queue = this.getQueue(queueName);
        
        const job = await queue.add(jobName, data, {
            ...options,
            // Se tiver delay, garantir que está em milissegundos
            ...(options.delay && {
                delay: typeof options.delay === 'number' 
                    ? options.delay 
                    : this.parseDelay(options.delay)
            }),
        });

        console.log(`[QUEUE] Job ${job.id} adicionado à fila ${queueName} (delay: ${options.delay || 0}ms)`);
        return job;
    }

    /**
     * Criar worker para processar jobs
     */
    createWorker(queueName, processor, options = {}) {
        if (this.workers.has(queueName)) {
            console.warn(`[QUEUE] Worker para ${queueName} já existe`);
            return this.workers.get(queueName);
        }

        const worker = new Worker(
            queueName,
            async (job) => {
                console.log(`[WORKER] Processando job ${job.id} da fila ${queueName}`);
                try {
                    const result = await processor(job);
                    console.log(`[WORKER] Job ${job.id} concluído com sucesso`);
                    return result;
                } catch (error) {
                    console.error(`[WORKER] Erro ao processar job ${job.id}:`, error);
                    throw error; // BullMQ cuida do retry automaticamente
                }
            },
            {
                connection: this._getConnection(),
                concurrency: options.concurrency || 10, // Processar 10 jobs simultaneamente
                limiter: options.limiter || {
                    max: 50, // Máximo 50 jobs
                    duration: 1000, // Por segundo
                },
                ...options,
            }
        );

        worker.on('completed', (job) => {
            console.log(`[WORKER] ✅ Job ${job.id} completado`);
        });

        worker.on('failed', (job, err) => {
            console.error(`[WORKER] ❌ Job ${job?.id} falhou:`, err.message);
        });

        worker.on('error', (err) => {
            console.error(`[WORKER] Erro no worker ${queueName}:`, err);
        });

        this.workers.set(queueName, worker);
        console.log(`[WORKER] Worker criado para fila ${queueName} (concurrency: ${options.concurrency || 10})`);
        
        return worker;
    }

    /**
     * Cancelar job
     */
    async cancelJob(queueName, jobId) {
        try {
            const queue = this.getQueue(queueName);
            const job = await queue.getJob(jobId);
            
            if (job) {
                await job.remove();
                console.log(`[QUEUE] Job ${jobId} cancelado da fila ${queueName}`);
                return true;
            }
            
            console.log(`[QUEUE] Job ${jobId} não encontrado na fila ${queueName}`);
            return false;
        } catch (error) {
            console.error(`[QUEUE] Erro ao cancelar job ${jobId}:`, error.message);
            return false;
        }
    }

    /**
     * Converter delay string (ex: "2s", "5m") para milissegundos
     */
    parseDelay(delayString) {
        if (typeof delayString === 'number') {
            return delayString;
        }

        const match = String(delayString).match(/^(\d+)([smhd])$/);
        if (!match) return 0;

        const value = parseInt(match[1]);
        const unit = match[2];

        const multipliers = {
            s: 1000,
            m: 60 * 1000,
            h: 60 * 60 * 1000,
            d: 24 * 60 * 60 * 1000,
        };

        return value * multipliers[unit];
    }

    /**
     * Fechar todas as conexões (útil para graceful shutdown)
     */
    async close() {
        console.log('[QUEUE] Fechando workers e filas...');
        
        // Fechar workers
        for (const [name, worker] of this.workers.entries()) {
            await worker.close();
            console.log(`[WORKER] Worker ${name} fechado`);
        }
        
        // Fechar filas
        for (const [name, queue] of this.queues.entries()) {
            await queue.close();
            console.log(`[QUEUE] Fila ${name} fechada`);
        }
        
        // Fechar conexão Redis
        if (this.connection) {
            await this.connection.quit();
            console.log('[REDIS] Conexão fechada');
        }
    }
}

// Exportar instância singleton
module.exports = new QueueManager();

