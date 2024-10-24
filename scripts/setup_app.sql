-- set mc_application_name='mc_app';

-- to remove:
-- set mc_warehouse_size='XSMALL';
-- set mc_warehouse_name='MONTE_CARLO_WH';
-- set sf_host_name='RNB23277.snowflakecomputing.com';
-- end to remove

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION mc_backend_egress_access_integration
  ALLOWED_NETWORK_RULES = (mc_app.core.mc_backend_egress_access)
  ENABLED = true;

GRANT USAGE ON INTEGRATION mc_backend_egress_access_integration TO APPLICATION mc_app;

-- CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($mc_warehouse_name) WAREHOUSE_SIZE=$mc_warehouse_size INITIALLY_SUSPENDED=TRUE
--     AUTO_SUSPEND = 5 AUTO_RESUME = TRUE;
--
-- -- Grant permissions to use the new warehouse
-- GRANT OPERATE, USAGE, MONITOR ON WAREHOUSE IDENTIFIER($mc_warehouse_name) TO APPLICATION IDENTIFIER($mc_application_name);

-- Grant privileges to allow access to query history
-- GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE" TO APPLICATION IDENTIFIER($mc_application_name);
-- GRANT CREATE COMPUTE POOL ON ACCOUNT TO APPLICATION IDENTIFIER($mc_application_name);
--GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION IDENTIFIER($mc_application_name);

-- Start the app
-- TODO: backend host and warehouse name as arguments
-- USE IDENTIFIER($mc_application_name);
-- CALL app_public.start_app();
