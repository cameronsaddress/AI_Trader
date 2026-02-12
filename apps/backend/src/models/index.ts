import { User } from './User';
import { Account } from './Account';
import { TradingModel } from './TradingModel';
import { Position } from './Position';
import { Trade } from './Trade';

// Associations
User.hasMany(Account, { foreignKey: 'userId' });
Account.belongsTo(User, { foreignKey: 'userId' });

TradingModel.hasMany(Account, { foreignKey: 'modelId' }); // Account dedicated to a model
Account.belongsTo(TradingModel, { foreignKey: 'modelId' });

Account.hasMany(Position, { foreignKey: 'accountId' });
Position.belongsTo(Account, { foreignKey: 'accountId' });

Account.hasMany(Trade, { foreignKey: 'accountId' });
Trade.belongsTo(Account, { foreignKey: 'accountId' });

TradingModel.hasMany(Trade, { foreignKey: 'modelId' });
Trade.belongsTo(TradingModel, { foreignKey: 'modelId' });

export {
    User,
    Account,
    TradingModel,
    Position,
    Trade
};
