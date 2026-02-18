import { Sequelize } from 'sequelize';
import dotenv from 'dotenv';
import path from 'path';
import { logger } from '../utils/logger';

dotenv.config({ path: path.join(__dirname, '../../../.env') }); // Re-resolve root .env if needed, though docker handles this

const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5544/ai_trader';

export const sequelize = new Sequelize(dbUrl, {
    dialect: 'postgres',
    logging: process.env.NODE_ENV === 'development'
        ? (message) => logger.debug(`[Sequelize] ${message}`)
        : false,
    pool: {
        max: 20,
        min: 0,
        acquire: 30000,
        idle: 10000
    },
    define: {
        timestamps: true,
        underscored: true
    }
});

export const connectDB = async () => {
    let retries = 5;
    while (retries > 0) {
        try {
            await sequelize.authenticate();
            logger.info('PostgreSQL connection established');

            // Sync models - In production, use migrations instead of sync()
            if (process.env.NODE_ENV !== 'production') {
                await sequelize.sync({ alter: true });
                logger.info('PostgreSQL schema sync complete (alter=true)');
            }
            return;
        } catch (error) {
            logger.error(`Unable to connect to the database (retries left: ${retries}): ${String(error)}`);
            retries -= 1;
            await new Promise(res => setTimeout(res, 5000));
        }
    }
    logger.error('Failed to connect to database after multiple attempts.');
    process.exit(1);
};
