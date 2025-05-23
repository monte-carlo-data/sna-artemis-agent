-- Script used to create a stored procedure that the MC Application uses to execute queries.
-- The stored procedure runs as the owner, which is the `MONTE_CARLO_APP_ROLE` role, configured
-- to have access to the databases that can be monitored.
-- The application is then granted access to the stored procedure and the database where it is defined.

-- Configuration
-- Role name to use to execute queries, can be changed to any other name
set mcd_agent_role_name='MCD_AGENT_ROLE';
-- Database name used to create the stored procedure, can be changed to any other name
set mcd_helper_db_name='MCD_AGENT_HELPER';

-- Set role for grants
USE ROLE ACCOUNTADMIN;

-- Create the role Monte Carlo will use to execute queries
CREATE ROLE IF NOT EXISTS identifier($mcd_agent_role_name);

-- Grant the new role to ACCOUNTADMIN
GRANT ROLE identifier($mcd_agent_role_name) TO ROLE ACCOUNTADMIN;

-- Grant privileges to allow access to query history
GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE" TO ROLE identifier($mcd_agent_role_name);

-- Create a database to define the helper stored procedure
CREATE DATABASE IF NOT EXISTS identifier($mcd_helper_db_name);
GRANT OWNERSHIP ON DATABASE identifier($mcd_helper_db_name) TO ROLE identifier($mcd_agent_role_name);

USE ROLE identifier($mcd_agent_role_name);
USE DATABASE identifier($mcd_helper_db_name);
CREATE SCHEMA IF NOT EXISTS MCD_AGENT;
USE SCHEMA MCD_AGENT;

CREATE OR REPLACE PROCEDURE MCD_AGENT_EXECUTE_QUERY(query STRING)
    RETURNS TABLE()
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    LET rs RESULTSET := (EXECUTE IMMEDIATE :query);
    RETURN TABLE(rs);
END;
