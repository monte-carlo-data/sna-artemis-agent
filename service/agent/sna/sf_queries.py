QUERY_EXECUTE_QUERY_WITH_HELPER = """
WITH RUN_QUERY AS PROCEDURE(op_id VARCHAR, query STRING)
    RETURNS VARCHAR
    LANGUAGE SQL
    AS
    $$
    BEGIN
        BEGIN
            ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS={timeout};
            CALL mcd_agent.core.execute_helper_query(:query);
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

QUERY_EXECUTE_QUERY_WITH_HELPER_SYNC = "CALL MCD_AGENT.CORE.EXECUTE_HELPER_QUERY(?)"

QUERY_LOAD_CONFIG = "SELECT key, value FROM {table}"

QUERY_UPDATE_CONFIG = """
MERGE INTO {table} C
USING (SELECT ? AS key) S
ON S.key=C.key 
WHEN MATCHED THEN UPDATE SET value=? 
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (?, ?)
"""

QUERY_RESTART_SERVICE = """
BEGIN
    CALL SYSTEM$WAIT(5);
    CALL MCD_AGENT.APP_PUBLIC.RESTART_SERVICE();
END;
"""
