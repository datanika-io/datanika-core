-- Enable pg_stat_statements extension (requires shared_preload_libraries set in postgres command).
-- This runs once on container first start via /docker-entrypoint-initdb.d/.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
