-- secret used to store the token used to connect to Monte Carlo, updated by the Streamlit app
CREATE SECRET IF NOT EXISTS core.mcd_app_token
    TYPE=generic_string
    SECRET_STRING='{}';
