-- Create extension for UUID generation if not exists
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table (if not exists via Sequelize sync, but good for reference)
-- This script runs on container startup if /var/lib/postgresql/data is empty
-- However, Sequelize sync({ alter: true }) will handle schema creation.
-- We can use this for initial data seeding or specific strict SQL requirements not handled by ORM.

-- Example: Create a default admin user (password: admin123 - should be changed)
-- Ideally this is done via a seeder script in the app, but SQL init is robust.
-- INSERT INTO users (id, username, password_hash, role, created_at, updated_at)
-- VALUES (uuid_generate_v4(), 'admin', '$2b$10$EpOd/something...', 'admin', NOW(), NOW())
-- ON CONFLICT DO NOTHING;
