import { Model, DataTypes } from 'sequelize';
import { sequelize } from '../config/database';

export class Trade extends Model {
    public id!: string;
    public accountId!: string;
    public modelId!: string; // Which AI made the decision
    public symbol!: string;
    public side!: 'BUY' | 'SELL';
    public quantity!: number;
    public price!: number;
    public fee!: number;
    public realizedPnl?: number;

    // Audit Trail
    public reasoning!: string; // LLM's explanation
    public confidence!: number; // 0-1 score

    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;
}

Trade.init({
    id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true
    },
    accountId: {
        type: DataTypes.UUID,
        allowNull: false,
        references: {
            model: 'accounts',
            key: 'id'
        }
    },
    modelId: {
        type: DataTypes.UUID, // Using string UUID even if not strictly enforced at DB level for logs
        allowNull: true
    },
    symbol: {
        type: DataTypes.STRING,
        allowNull: false
    },
    side: {
        type: DataTypes.ENUM('BUY', 'SELL'),
        allowNull: false
    },
    quantity: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false
    },
    price: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false
    },
    fee: {
        type: DataTypes.DECIMAL(20, 8),
        defaultValue: 0
    },
    realizedPnl: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: true
    },
    reasoning: {
        type: DataTypes.TEXT,
        allowNull: true
    },
    confidence: {
        type: DataTypes.FLOAT,
        defaultValue: 0
    }
}, {
    sequelize,
    modelName: 'Trade',
    tableName: 'trades'
});
