import { Model, DataTypes } from 'sequelize';
import { sequelize } from '../config/database';

export class Account extends Model {
    public id!: string;
    public userId!: string;
    public modelId?: string; // Optional linkage to specific model
    public exchange!: 'coinbase' | 'polymarket' | 'simulation';
    public balance!: number;
    public currency!: string;
    public isSimulated!: boolean;

    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;
}

Account.init({
    id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true
    },
    userId: {
        type: DataTypes.UUID,
        allowNull: false,
        references: {
            model: 'users',
            key: 'id'
        }
    },
    modelId: {
        type: DataTypes.UUID,
        allowNull: true,
        references: {
            model: 'trading_models',
            key: 'id'
        }
    },
    exchange: {
        type: DataTypes.ENUM('coinbase', 'polymarket', 'simulation'),
        allowNull: false
    },
    balance: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false,
        defaultValue: 0
    },
    currency: {
        type: DataTypes.STRING,
        allowNull: false,
        defaultValue: 'USD'
    },
    isSimulated: {
        type: DataTypes.BOOLEAN,
        defaultValue: true
    }
}, {
    sequelize,
    modelName: 'Account',
    tableName: 'accounts'
});
