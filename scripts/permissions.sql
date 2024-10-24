-- Configuration
set database_to_monitor='<your_database>';
set mc_application_name='MC_APP';

-- Set role for grants
USE ROLE ACCOUNTADMIN;

GRANT USAGE,MONITOR ON DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);
GRANT USAGE,MONITOR ON ALL SCHEMAS IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);

USE DATABASE identifier($database_to_monitor);
CREATE OR REPLACE PROCEDURE GRANT_REFERENCES_TO_MONTE_CARLO_APP()
    RETURNS VARCHAR
    LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
// If a Snowflake account has only database future grants, applying schema level future grants can break existing roles!
//
// "When future grants are defined at both the database and schema level, the schema level grants take precedence over
// the database level grants, and the database level grants are ignored. An important point to note here is that as
// long as there is a SCHEMA level future grants, ALL DATABASE levels will be ignored, even for the roles that are
// NOT defined in the SCHEMA level future grants."
// See: https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html#considerations
//
// This is why the following script checks if there are any SCHEMA level future grants before creating new SCHEMA level
// grants. If there aren't any we assume you're using DATABASE level future grants, and create the new grants on the
// DATABASE level instead.
//
// Please see here for more information: https://community.snowflake.com/s/article/DB-Level-Future-Grants-Overridden-by-Schema-Level-Future-Grants
//
snowflake.createStatement({sqlText: `use database identifier($database_to_monitor)`}).execute();
var show_future_grants = snowflake.createStatement({sqlText: `SHOW FUTURE GRANTS IN DATABASE identifier($database_to_monitor)`}).execute();
var schema_future_grants = snowflake.createStatement({sqlText: `select * from TABLE(RESULT_SCAN('${show_future_grants.getQueryId()}')) where "grant_on" = 'SCHEMA'`}).execute();
if (schema_future_grants.getRowCount() > 0) {
    var schemas_to_grant = snowflake.createStatement({ sqlText:`select * from information_schema.SCHEMATA where SCHEMA_NAME <> 'INFORMATION_SCHEMA'`}).execute();
    var granted_schemas = "";
    while(schemas_to_grant.next()) {
      table_schema = schemas_to_grant.getColumnValue("SCHEMA_NAME");

      snowflake.createStatement({ sqlText:`GRANT REFERENCES ON ALL TABLES IN SCHEMA ${table_schema} TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
      snowflake.createStatement({ sqlText:`GRANT REFERENCES ON ALL VIEWS IN SCHEMA ${table_schema} TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
      snowflake.createStatement({ sqlText:`GRANT REFERENCES ON ALL EXTERNAL TABLES IN SCHEMA ${table_schema} TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
      snowflake.createStatement({ sqlText:`GRANT MONITOR ON ALL DYNAMIC TABLES IN SCHEMA ${table_schema} TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();

      granted_schemas += table_schema + "; "
    }
    return `Granted references for schemas ${granted_schemas}`;
}
snowflake.createStatement({ sqlText: `GRANT REFERENCES ON ALL TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
snowflake.createStatement({ sqlText: `GRANT REFERENCES ON ALL VIEWS IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
snowflake.createStatement({ sqlText: `GRANT REFERENCES ON ALL EXTERNAL TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();
snowflake.createStatement({ sqlText: `GRANT MONITOR ON ALL DYNAMIC TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name)`}).execute();

return `Granted references for database to application`;
$$;
CALL GRANT_REFERENCES_TO_MONTE_CARLO_APP();

-- Grant read-only privileges to database to be monitored
GRANT SELECT ON ALL TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);
GRANT SELECT ON ALL VIEWS IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);
GRANT SELECT ON ALL EXTERNAL TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);
GRANT SELECT ON ALL STREAMS IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);
GRANT SELECT ON ALL DYNAMIC TABLES IN DATABASE identifier($database_to_monitor) TO APPLICATION IDENTIFIER($mc_application_name);

