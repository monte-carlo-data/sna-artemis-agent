import json

import pandas as pd
import streamlit as st

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

st.session_state.logs_table = []


def restart_container(token_updated: bool = False):
    session: Session = get_active_session()
    result = session.sql(
        f"CALL app_public.start_app();",
    ).collect()
    if token_updated:
        st.success(f"Token updated and container started: {result[0][0]}")
    else:
        st.success(f"Container restarted: {result[0][0]}")


def get_container_status():
    session: Session = get_active_session()
    result = session.sql(
        "CALL app_public.service_status();",
    ).collect()
    st.success(result[0][0])


def setup_connection():
    key_id = st.session_state.key_input_id
    key_secret = st.session_state.key_input_secret
    key_json = {"mcd_id": key_id, "mcd_token": key_secret}
    session: Session = get_active_session()
    session.sql(
        f"ALTER SECRET MCD_AGENT.CORE.MCD_AGENT_TOKEN SET SECRET_STRING=?;",
        params=[json.dumps(key_json)],
    ).collect()
    restart_container(True)


def push_metrics():
    _execute_function("push_metrics")


def health_check():
    _execute_function("health_check")


def reachability_test():
    _execute_function("reachability_test")


def _execute_function(name: str):
    session: Session = get_active_session()
    result = session.sql(
        f"SELECT core.{name}();",
    ).collect()
    st.success(result[0][0])


def fetch_logs():
    pass


def main():
    session: Session = get_active_session()
    st.header("Monte Carlo Agent")

    setup_tab, adv_tab = st.tabs(["Initial Setup", "Advanced"])
    with setup_tab:
        st.write("Welcome to the Monte Carlo Agent!")
        st.write(
            "Make sure you follow the steps documented [here](https://docs.getmontecarlo.com) "
            "before proceeding."
        )
        st.write("")
        with st.form("setup_form"):
            st.text_input("Key Id", key="key_input_id")
            st.text_input("Key Secret", key="key_input_secret", type="password")
            _ = st.form_submit_button("Configure", on_click=setup_connection)
    with adv_tab:
        with st.form("adv_form"):
            _ = st.form_submit_button("Restart Container", on_click=restart_container)
            _ = st.form_submit_button("Container Status", on_click=get_container_status)
            _ = st.form_submit_button("Push Metrics", on_click=push_metrics)
            _ = st.form_submit_button("Health Check", on_click=health_check)
            _ = st.form_submit_button("Reachability Test", on_click=reachability_test)

        with st.form("logs_form"):
            _ = st.form_submit_button("Fetch Logs", on_click=fetch_logs)
        try:
            logs_table = session.sql("CALL app_public.service_logs(1000)").collect()
        except Exception:
            logs_table = []
        st.dataframe(pd.DataFrame(reversed(logs_table)))


if __name__ == "__main__":
    main()
