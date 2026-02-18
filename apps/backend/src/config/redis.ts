import { createClient } from 'redis';
import dotenv from 'dotenv';
import path from 'path';
import { logger } from '../utils/logger';

dotenv.config({ path: path.join(__dirname, '../../../.env') });

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6491';

export const redisClient = createClient({
    url: redisUrl
});

redisClient.on('error', (err) => logger.error(`Redis client error: ${String(err)}`));
redisClient.on('connect', () => logger.info('Redis client connected'));

export const subscriber = redisClient.duplicate();
subscriber.on('error', (err) => logger.error(`Redis subscriber error: ${String(err)}`));
subscriber.on('connect', () => logger.info('Redis subscriber connected'));

export const connectRedis = async () => {
    if (!redisClient.isOpen) {
        await redisClient.connect();
    }
    if (!subscriber.isOpen) {
        await subscriber.connect();
    }
};
