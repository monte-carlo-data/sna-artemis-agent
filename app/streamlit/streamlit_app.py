import json
import pandas as pd
import streamlit as st

import snowflake.permissions as permissions
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

if not permissions.get_reference_associations("mcd_agent_helper_execute_query"):
    permissions.request_reference("mcd_agent_helper_execute_query")


def restart_container(token_updated: bool = False):
    session: Session = get_active_session()
    result = session.sql(
        f"CALL app_public.start_app();",
    ).collect()
    if token_updated:
        st.success(
            f"Token updated and container restarted, status: ({_get_container_status_text()})"
        )
    else:
        st.success(f"Container restarted: {result[0][0]}")


def get_container_status():
    st.info(_get_container_status_text())


def _get_container_status_text() -> str:
    session: Session = get_active_session()
    result = session.sql(
        "CALL app_public.service_status();",
    ).collect()
    return result[0][0]


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
    st.info(result[0][0])


def logs_panel():
    logs_table = _fetch_logs()
    return st.dataframe(pd.DataFrame(reversed(logs_table)), width=1000, height=500)


def _fetch_logs():
    session: Session = get_active_session()
    try:
        return session.sql("CALL app_public.service_logs(1000)").collect()
    except Exception:
        return []


def update_token_panel(status_container=None):
    def setup_connection_wrapper():
        if status_container:
            with status_container:
                setup_connection()
        else:
            setup_connection()

    with st.form("update_token_form"):
        st.text_input("Key Id", key="key_input_id")
        st.text_input("Key Secret", key="key_input_secret", type="password")
        st.form_submit_button("Update Token", on_click=setup_connection_wrapper)


def main():
    st.header("Monte Carlo Agent")

    st.write(
        "Welcome to the Monte Carlo SNA Agent, see documentation [here](https://docs.getmontecarlo.com) for details."
    )
    buttons_container = st.container(border=True)
    status_container = st.container()

    with buttons_container:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("Container Status"):
                with status_container:
                    get_container_status()
        with col2:
            if st.button("Reachability Test"):
                with status_container:
                    reachability_test()
        with col3:
            if st.button("Update Token"):
                with status_container:
                    update_token_panel(status_container)
        with col4:
            if st.button("Fetch Logs"):
                with status_container:
                    logs_panel()
    st.markdown(
        """
        <style>
            div[data-testid="column"] * {
                min-width: 130px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
