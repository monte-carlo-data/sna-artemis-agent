CREATE SCHEMA IF NOT EXISTS CONFIG;

-- table used to store settings for the application, for example how to execute queries or
-- size of thread pools
CREATE TABLE IF NOT EXISTS CONFIG.APP_CONFIG (
    KEY VARCHAR(255) PRIMARY KEY,
    VALUE VARCHAR(2000)
);
