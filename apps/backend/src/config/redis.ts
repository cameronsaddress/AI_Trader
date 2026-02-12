import { createClient } from 'redis';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.join(__dirname, '../../../.env') });

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6491';

export const redisClient = createClient({
    url: redisUrl
});

redisClient.on('error', (err) => console.log('Redis Client Error', err));
redisClient.on('connect', () => console.log('Redis Client Connected'));

export const subscriber = redisClient.duplicate();
subscriber.on('error', (err) => console.log('Redis Subscriber Error', err));
subscriber.on('connect', () => console.log('Redis Subscriber Connected'));

export const connectRedis = async () => {
    if (!redisClient.isOpen) {
        await redisClient.connect();
    }
    if (!subscriber.isOpen) {
        await subscriber.connect();
    }
};
