import { Model, DataTypes } from 'sequelize';
import { sequelize } from '../config/database';

export class TradingModel extends Model {
    public id!: string;
    public name!: string;
    public provider!: string; // 'openai', 'anthropic', etc.
    public modelVersion!: string; // 'gpt-4', 'claude-3-opus'
    public type!: 'LLM' | 'ML' | 'Arbitrage';
    public active!: boolean;
    public config!: object; // JSON config for prompt setup, risk parameters

    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;
}

TradingModel.init({
    id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true
    },
    name: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: true
    },
    provider: {
        type: DataTypes.STRING,
        allowNull: false
    },
    modelVersion: {
        type: DataTypes.STRING,
        allowNull: false
    },
    type: {
        type: DataTypes.ENUM('LLM', 'ML', 'Arbitrage'),
        defaultValue: 'LLM'
    },
    active: {
        type: DataTypes.BOOLEAN,
        defaultValue: true
    },
    config: {
        type: DataTypes.JSONB,
        defaultValue: {}
    }
}, {
    sequelize,
    modelName: 'TradingModel',
    tableName: 'trading_models'
});
