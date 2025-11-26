// Cloudflare R2 Storage Module
const { S3Client, PutObjectCommand, HeadObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');

class R2Storage {
    constructor() {
        if (!process.env.R2_ENDPOINT || !process.env.R2_ACCESS_KEY_ID || !process.env.R2_SECRET_ACCESS_KEY) {
            console.warn('[R2 Storage] Credenciais não configuradas. Storage externo desabilitado.');
            this.enabled = false;
            return;
        }

        this.client = new S3Client({
            endpoint: process.env.R2_ENDPOINT,
            region: 'auto',
            credentials: {
                accessKeyId: process.env.R2_ACCESS_KEY_ID,
                secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
            },
        });

        this.bucketName = process.env.R2_BUCKET_NAME;
        this.publicUrl = process.env.R2_PUBLIC_URL; // URL pública via CDN ou custom domain
        this.enabled = true;

        if (!this.bucketName || !this.publicUrl) {
            console.warn('[R2 Storage] R2_BUCKET_NAME ou R2_PUBLIC_URL não configurado. Storage externo desabilitado.');
            this.enabled = false;
        }
    }

    getContentType(fileType) {
        const types = {
            'image': 'image/jpeg',
            'video': 'video/mp4',
            'audio': 'audio/ogg'
        };
        return types[fileType] || 'application/octet-stream';
    }

    async uploadFile(buffer, fileName, fileType, sellerId) {
        if (!this.enabled) {
            throw new Error('R2 Storage não está habilitado.');
        }

        const timestamp = Date.now();
        const sanitizedFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
        const storageKey = `seller_${sellerId}/${timestamp}_${sanitizedFileName}`;

        try {
            await this.client.send(new PutObjectCommand({
                Bucket: this.bucketName,
                Key: storageKey,
                Body: buffer,
                ContentType: this.getContentType(fileType),
                CacheControl: 'public, max-age=31536000',
            }));

            const publicUrl = `${this.publicUrl}/${storageKey}`;

            return {
                storageKey,
                publicUrl,
                contentType: this.getContentType(fileType)
            };
        } catch (error) {
            console.error('[R2 Storage] Erro ao fazer upload:', error);
            throw error;
        }
    }

    async uploadThumbnail(buffer, fileName, sellerId) {
        if (!this.enabled) {
            throw new Error('R2 Storage não está habilitado.');
        }

        const timestamp = Date.now();
        const sanitizedFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
        const storageKey = `seller_${sellerId}/thumbnails/${timestamp}_${sanitizedFileName}`;

        try {
            await this.client.send(new PutObjectCommand({
                Bucket: this.bucketName,
                Key: storageKey,
                Body: buffer,
                ContentType: 'image/jpeg',
                CacheControl: 'public, max-age=31536000',
            }));

            const publicUrl = `${this.publicUrl}/${storageKey}`;

            return {
                storageKey,
                publicUrl
            };
        } catch (error) {
            console.error('[R2 Storage] Erro ao fazer upload de thumbnail:', error);
            throw error;
        }
    }

    async fileExists(storageKey) {
        if (!this.enabled) return false;

        try {
            await this.client.send(new HeadObjectCommand({
                Bucket: this.bucketName,
                Key: storageKey,
            }));
            return true;
        } catch (error) {
            if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
                return false;
            }
            throw error;
        }
    }

    async deleteFile(storageKey) {
        if (!this.enabled) {
            console.warn('[R2 Storage] Tentativa de deletar arquivo com R2 desabilitado:', storageKey);
            return false;
        }

        try {
            await this.client.send(new DeleteObjectCommand({
                Bucket: this.bucketName,
                Key: storageKey,
            }));
            return true;
        } catch (error) {
            console.error('[R2 Storage] Erro ao deletar arquivo:', error);
            return false;
        }
    }
}

module.exports = new R2Storage();

