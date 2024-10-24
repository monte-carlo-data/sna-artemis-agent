-- run after setup_user.sql
USE ROLE mc_app_role;
USE DATABASE mc_app_data;

CREATE SCHEMA IF NOT EXISTS mc_app;
CREATE IMAGE REPOSITORY IF NOT EXISTS mc_app_repository;
CREATE STAGE IF NOT EXISTS mc_app_stage
  DIRECTORY = ( ENABLE = true );
