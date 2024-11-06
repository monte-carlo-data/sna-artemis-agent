-- role used for read-only operations like getting logs, checking service status, etc.
CREATE APPLICATION ROLE IF NOT EXISTS app_user;

-- role used for administrative operations like starting/stopping the service, configuring the
-- external access integrations, etc.
CREATE APPLICATION ROLE IF NOT EXISTS app_admin;

CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_user;

CREATE SCHEMA IF NOT EXISTS app_admin;
GRANT USAGE ON SCHEMA app_admin TO APPLICATION ROLE app_admin;

CREATE OR ALTER VERSIONED SCHEMA app_public;
GRANT USAGE ON SCHEMA app_public TO APPLICATION ROLE app_user;

EXECUTE IMMEDIATE FROM '/scripts/setup_procs.sql';
EXECUTE IMMEDIATE FROM '/scripts/setup_ui.sql';
EXECUTE IMMEDIATE FROM '/scripts/setup_secrets.sql';
EXECUTE IMMEDIATE FROM '/scripts/setup_stage.sql';

