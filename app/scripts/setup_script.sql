CREATE APPLICATION ROLE IF NOT EXISTS app_user;

CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_user;

CREATE OR ALTER VERSIONED SCHEMA app_public;
GRANT USAGE ON SCHEMA app_public TO APPLICATION ROLE app_user;

EXECUTE IMMEDIATE FROM '/scripts/setup_procs.sql';
EXECUTE IMMEDIATE FROM '/scripts/setup_ui.sql';
EXECUTE IMMEDIATE FROM '/scripts/setup_backend.sql';
