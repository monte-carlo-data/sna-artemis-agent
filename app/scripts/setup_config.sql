CREATE SCHEMA IF NOT EXISTS CONFIG;

-- table used to store settings for the application, for example how to execute queries or
-- size of thread pools
CREATE TABLE IF NOT EXISTS CONFIG.APP_CONFIG (
    KEY VARCHAR(255) PRIMARY KEY,
    VALUE VARCHAR(2000)
);

-- Set default configuration values (won't overwrite existing values)
MERGE INTO CONFIG.APP_CONFIG C
USING (SELECT 'METRICS_TIMER_ENABLED' AS key, 'true' AS value) S
ON S.key = C.key
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (S.key, S.value);

MERGE INTO CONFIG.APP_CONFIG C
USING (SELECT 'PUBLISHER_THREAD_COUNT' AS key, '3' AS value) S
ON S.key = C.key
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (S.key, S.value);

MERGE INTO CONFIG.APP_CONFIG C
USING (SELECT 'OPS_RUNNER_THREAD_COUNT' AS key, '5' AS value) S
ON S.key = C.key
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (S.key, S.value);
