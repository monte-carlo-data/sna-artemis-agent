QUERY_EXECUTE_QUERY_WITH_HELPER = """
WITH RUN_QUERY AS PROCEDURE(op_id VARCHAR, query STRING)
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
    BEGIN
        BEGIN
            ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS={timeout};
            CALL MCD_AGENT_HELPER.MCD_AGENT.MCD_AGENT_EXECUTE_QUERY(:query);
            SELECT * FROM TABLE(RESULT_SCAN(:SQLID));
            SELECT mcd_agent.core.query_completed(:op_id, :SQLID);
        EXCEPTION
            WHEN OTHER THEN BEGIN
                SELECT mcd_agent.core.query_failed(:op_id, :sqlcode, :sqlerrm, :sqlstate);
            END;
        END;
    END;
    $$
CALL RUN_QUERY(?, ?);
"""

QUERY_SET_STATEMENT_TIMEOUT = "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS={timeout}"

QUERY_EXECUTE_QUERY_WITH_HELPER_SYNC = (
    "CALL MCD_AGENT_HELPER.MCD_AGENT.MCD_AGENT_EXECUTE_QUERY(?)"
)
