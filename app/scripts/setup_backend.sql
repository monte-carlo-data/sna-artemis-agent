BEGIN
    LET backend_service_url := 'mcd-orchestrator-test-nlb-9b478a23917fbdf9.elb.us-east-1.amazonaws.com:80';

    CREATE SECRET IF NOT EXISTS core.mc_app_token
        TYPE=generic_string
        SECRET_STRING='{}';

    CREATE OR REPLACE NETWORK RULE core.mc_backend_egress_access
      MODE = EGRESS
      TYPE = HOST_PORT
      VALUE_LIST = (:backend_service_url);

    GRANT USAGE ON NETWORK RULE core.mc_backend_egress_access TO APPLICATION ROLE app_user;
END;