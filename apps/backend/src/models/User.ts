import { Model, DataTypes } from 'sequelize';
import { sequelize } from '../config/database';

export class User extends Model {
    public id!: string;
    public username!: string;
    public passwordHash!: string;
    public role!: 'admin' | 'trader' | 'viewer';
    public apiKey?: string;

    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;
}

User.init({
    id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true
    },
    username: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: true
    },
    passwordHash: {
        type: DataTypes.STRING,
        allowNull: false
    },
    role: {
        type: DataTypes.ENUM('admin', 'trader', 'viewer'),
        defaultValue: 'trader'
    },
    apiKey: {
        type: DataTypes.STRING,
        allowNull: true,
        unique: true
    }
}, {
    sequelize,
    modelName: 'User',
    tableName: 'users'
});
