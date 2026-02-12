import { Sequelize } from 'sequelize';
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.join(__dirname, '../../../.env') }); // Re-resolve root .env if needed, though docker handles this

const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5544/ai_trader';

export const sequelize = new Sequelize(dbUrl, {
    dialect: 'postgres',
    logging: process.env.NODE_ENV === 'development' ? console.log : false,
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
            console.log('PostgreSQL Connection has been established successfully.');

            // Sync models - In production, use migrations instead of sync()
            if (process.env.NODE_ENV !== 'production') {
                await sequelize.sync({ alter: true });
                console.log('Database synced (alter: true)');
            }
            return;
        } catch (error) {
            console.error(`Unable to connect to the database (retries left: ${retries}):`, error);
            retries -= 1;
            await new Promise(res => setTimeout(res, 5000));
        }
    }
    console.error('Failed to connect to database after multiple attempts.');
    process.exit(1);
};
