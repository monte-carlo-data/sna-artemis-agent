CREATE SCHEMA IF NOT EXISTS UI;
CREATE OR REPLACE STREAMLIT UI.MONTE_CARLO_AGENT
     FROM '/streamlit'
     TITLE = 'Monte Carlo Agent'
     MAIN_FILE = '/streamlit_app.py';

GRANT USAGE ON SCHEMA UI TO APPLICATION ROLE app_user;
GRANT USAGE ON STREAMLIT UI.MONTE_CARLO_AGENT TO APPLICATION ROLE app_user;