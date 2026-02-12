import { Model, DataTypes } from 'sequelize';
import { sequelize } from '../config/database';

export class Position extends Model {
    public id!: string;
    public accountId!: string;
    public symbol!: string;
    public side!: 'LONG' | 'SHORT';
    public quantity!: number;
    public entryPrice!: number;
    public currentPrice!: number;
    public unrealizedPnl!: number;
    public status!: 'OPEN' | 'CLOSED';

    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;
}

Position.init({
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
    symbol: {
        type: DataTypes.STRING,
        allowNull: false
    },
    side: {
        type: DataTypes.ENUM('LONG', 'SHORT'),
        allowNull: false
    },
    quantity: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false
    },
    entryPrice: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false
    },
    currentPrice: {
        type: DataTypes.DECIMAL(20, 8),
        allowNull: false
    },
    unrealizedPnl: {
        type: DataTypes.DECIMAL(20, 8),
        defaultValue: 0
    },
    status: {
        type: DataTypes.ENUM('OPEN', 'CLOSED'),
        defaultValue: 'OPEN'
    }
}, {
    sequelize,
    modelName: 'Position',
    tableName: 'positions',
    indexes: [
        {
            unique: true,
            fields: ['account_id', 'symbol'],
            where: {
                status: 'OPEN'
            }
        }
    ]
});
