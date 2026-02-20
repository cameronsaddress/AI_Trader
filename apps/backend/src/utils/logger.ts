import winston from 'winston';

const levels = {
    error: 0,
    warn: 1,
    info: 2,
    http: 3,
    debug: 4,
};

const colors = {
    error: 'red',
    warn: 'yellow',
    info: 'green',
    http: 'magenta',
    debug: 'white',
};

winston.addColors(colors);

const format = winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss:ms' }),
    winston.format.colorize({ all: true }),
    winston.format.printf(
        (info) => `${info.timestamp} ${info.level}: ${info.message}`,
    ),
);

const logLevel = (
    process.env.LOG_LEVEL
    || (process.env.NODE_ENV === 'development' ? 'debug' : 'info')
).toLowerCase();
const maxSizeBytes = Math.max(1_000_000, Number(process.env.LOG_MAX_SIZE_BYTES || `${20 * 1024 * 1024}`));
const maxFiles = Math.max(1, Number(process.env.LOG_MAX_FILES || '10'));

const transports = [
    new winston.transports.Console(),
    new winston.transports.File({
        filename: 'logs/error.log',
        level: 'error',
        maxsize: maxSizeBytes,
        maxFiles,
    }),
    new winston.transports.File({
        filename: 'logs/all.log',
        maxsize: maxSizeBytes,
        maxFiles,
    }),
];

export const logger = winston.createLogger({
    level: logLevel,
    levels,
    format,
    transports,
});
