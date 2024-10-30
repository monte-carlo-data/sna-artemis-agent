-- Script used to create a stored procedure that the MC Application uses to execute queries.
-- The stored procedure runs as the owner, which is the `MONTE_CARLO_APP_ROLE` role, configured
-- to have access to the databases that can be monitored.
-- The application is then granted access to the stored procedure and the database where it is defined.

-- Configuration
set mc_app_role_name='MONTE_CARLO_APP_ROLE';
set mc_helper_db_name='MC_APP_HELPER';
set mc_app_name='MC_APP';
set mc_app_user_role=$mc_app_name || '.APP_USER';

-- Set role for grants
USE ROLE ACCOUNTADMIN;

-- Create the role Monte Carlo will use to execute queries
CREATE ROLE IF NOT EXISTS identifier($mc_app_role_name);

-- Grant the new role to ACCOUNTADMIN
GRANT ROLE identifier($mc_app_role_name) TO ROLE ACCOUNTADMIN;

-- Grant privileges to allow access to query history
GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE" TO ROLE identifier($mc_app_role_name);

-- Grant access to the app
GRANT APPLICATION ROLE identifier($mc_app_user_role) TO ROLE identifier($mc_app_role_name);


-- Create a database to define the helper stored procedure
CREATE DATABASE IF NOT EXISTS identifier($mc_helper_db_name);
GRANT OWNERSHIP ON DATABASE identifier($mc_helper_db_name) TO ROLE identifier($mc_app_role_name);

USE ROLE identifier($mc_app_role_name);
USE DATABASE identifier($mc_helper_db_name);
CREATE SCHEMA IF NOT EXISTS MC_APP;
USE SCHEMA MC_APP;

CREATE OR REPLACE PROCEDURE MC_APP_EXECUTE_QUERY(query STRING)
    RETURNS TABLE()
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    LET rs RESULTSET := (EXECUTE IMMEDIATE :query);
    RETURN TABLE(rs);
END;

GRANT USAGE ON DATABASE identifier($mc_helper_db_name) TO APPLICATION identifier($mc_app_name);
GRANT USAGE ON SCHEMA MC_APP TO APPLICATION identifier($mc_app_name);
GRANT USAGE ON PROCEDURE MC_APP_EXECUTE_QUERY(STRING) TO APPLICATION identifier($mc_app_name);
