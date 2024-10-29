CREATE OR REPLACE PROCEDURE app_public.start_app(
       min_nodes INT DEFAULT 2,
       max_nodes INT DEFAULT 2,
       family VARCHAR DEFAULT 'CPU_X64_XS',
       wh_size VARCHAR DEFAULT 'XSMALL',
       wh_auto_suspend INT DEFAULT 5
   )
   RETURNS string
   LANGUAGE sql
   AS
$$
BEGIN
   LET pool_name := (SELECT CURRENT_DATABASE()) || '_compute_pool';
   LET wh_name := (SELECT CURRENT_DATABASE()) || '_wh';

   -- using execute immediate because parameters like AUTO_SUSPEND or MIN_NODES
   -- doesn't support being assigned to parameters, it needs to be a fixed value in the query
   EXECUTE IMMEDIATE ('ALTER WAREHOUSE IF EXISTS ' || :wh_name || ' SET WAREHOUSE_SIZE = ' || :wh_size);
   EXECUTE IMMEDIATE ('ALTER WAREHOUSE IF EXISTS ' || :wh_name || ' SET AUTO_SUSPEND = ' || :wh_auto_suspend);
   LET create_wh_sql VARCHAR := 'CREATE WAREHOUSE IF NOT EXISTS ' || :wh_name
       || ' WAREHOUSE_SIZE=' || :wh_size
       || ' INITIALLY_SUSPENDED = true '
       || ' AUTO_SUSPEND = ' || :wh_auto_suspend
       || ' AUTO_RESUME = true';
   EXECUTE IMMEDIATE :create_wh_sql;

   EXECUTE IMMEDIATE ('ALTER COMPUTE POOL IF EXISTS ' || :pool_name || ' SET MIN_NODES = ' || :min_nodes);
   EXECUTE IMMEDIATE ('ALTER COMPUTE POOL IF EXISTS ' || :pool_name || ' SET MAX_NODES = ' || :max_nodes);
   LET create_pool_sql VARCHAR := 'CREATE COMPUTE POOL IF NOT EXISTS ' || :pool_name
       || ' MIN_NODES = ' || :min_nodes
       || ' MAX_NODES = ' || :max_nodes
       || ' INSTANCE_FAMILY = ' || :family
       || ' AUTO_RESUME = true';
   EXECUTE IMMEDIATE :create_pool_sql;

   CREATE SERVICE IF NOT EXISTS core.mc_app_service
      IN COMPUTE POOL identifier(:pool_name)
      EXTERNAL_ACCESS_INTEGRATIONS=(reference('monte_carlo_external_access'))
      FROM spec='service/mc_app_spec.yaml';

   ALTER SERVICE IF EXISTS core.mc_app_service SET EXTERNAL_ACCESS_INTEGRATIONS=(reference('monte_carlo_external_access'));
   ALTER SERVICE IF EXISTS core.mc_app_service FROM spec='service/mc_app_spec.yaml';

   CREATE OR REPLACE FUNCTION core.push_metrics ()
      RETURNS varchar
      SERVICE=core.mc_app_service
      ENDPOINT='mc-app-endpoint'
      AS '/api/v1/test/metrics';

   CREATE OR REPLACE FUNCTION core.query_completed(OP_ID VARCHAR, QUERY_ID VARCHAR)
      RETURNS varchar
      SERVICE=core.mc_app_service
      ENDPOINT='mc-app-endpoint'
      AS '/api/v1/agent/execute/snowflake/query_completed';

   CREATE OR REPLACE FUNCTION core.query_failed(OP_ID VARCHAR, CODE INT, MSG VARCHAR, ST VARCHAR)
      RETURNS varchar
      SERVICE=core.mc_app_service
      ENDPOINT='mc-app-endpoint'
      AS '/api/v1/agent/execute/snowflake/query_failed';

   CREATE OR REPLACE FUNCTION core.health_check()
      RETURNS varchar
      SERVICE=core.mc_app_service
      ENDPOINT='mc-app-endpoint'
      AS '/api/v1/test/health';

  CREATE OR REPLACE FUNCTION core.reachability_test()
      RETURNS varchar
      SERVICE=core.mc_app_service
      ENDPOINT='mc-app-endpoint'
      AS '/api/v1/test/reachability';

   RETURN 'Service successfully created or updated';
END;
$$;

GRANT USAGE ON PROCEDURE app_public.start_app(INT, INT, VARCHAR, VARCHAR, INT) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE app_public.service_status()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
   DECLARE
         service_status VARCHAR;
   BEGIN
         CALL SYSTEM$GET_SERVICE_STATUS('core.mc_app_service') INTO :service_status;
         RETURN PARSE_JSON(:service_status)[0]['status']::VARCHAR || ': ' || PARSE_JSON(:service_status)[0]['message']::VARCHAR;
   END;
$$;

GRANT USAGE ON PROCEDURE app_public.service_status() TO APPLICATION ROLE app_user;

CREATE OR REPLACE PROCEDURE app_public.service_logs(limit int DEFAULT 100)
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
   DECLARE
         service_logs VARCHAR;
   BEGIN
         CALL SYSTEM$GET_SERVICE_LOGS('core.mc_app_service', '0', 'mc-app', :limit) INTO :service_logs;
         LET results RESULTSET := (SELECT value as log_line FROM TABLE(SPLIT_TO_TABLE(:service_logs, '\n')) WHERE log_line NOT LIKE '%/healthcheck%');
         RETURN TABLE(results);
   END;
$$;

GRANT USAGE ON PROCEDURE app_public.service_logs(int) TO APPLICATION ROLE app_user;

CREATE OR REPLACE PROCEDURE app_admin.get_config_for_reference(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS $$
    BEGIN
        CASE (ref_name)
            WHEN 'MONTE_CARLO_EXTERNAL_ACCESS' THEN
                RETURN '{
                    "type": "CONFIGURATION",
                    "payload":{
                      "host_ports": ["mcd-orchestrator-test-nlb-9b478a23917fbdf9.elb.us-east-1.amazonaws.com:80"]
                    }
                }';
        END CASE;
        RETURN '';
    END;
$$;

GRANT USAGE ON PROCEDURE app_admin.get_config_for_reference(string) TO APPLICATION ROLE app_admin;

CREATE OR REPLACE PROCEDURE app_admin.register_single_reference(ref_name STRING, operation STRING, ref_or_alias STRING)
  RETURNS STRING
  LANGUAGE SQL
  AS $$
    BEGIN
      CASE (operation)
        WHEN 'ADD' THEN
          SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
          SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
          SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
      ELSE
        RETURN 'unknown operation: ' || operation;
      END CASE;
      RETURN NULL;
    END;
  $$;

GRANT USAGE ON PROCEDURE app_admin.register_single_reference(STRING, STRING, STRING) TO APPLICATION ROLE app_admin;