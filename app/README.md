# Monte Carlo Data Collector - Snowflake Artemis Agent - Streamlit application

## Local development
### Pre-requisites
- Python 3.12 or later
- Snowflake [CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) installed and configured with access to the Snowflake account where the agent will be deployed.

### Test Streamlit application changes
After making local changes to the Streamlit application, you can test them by running the following command from the root folder (this command assumes the Snowflake connection name is `mc_app_dev`):
```shell
snow app run -c mc_app_dev
```

### Remove Streamlit application
Run:
```shell
snow app teardown -c mc_app_dev --force 
```
