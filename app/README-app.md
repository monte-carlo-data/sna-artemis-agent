# Monte Carlo Data Collector - Snowflake Artemis Agent - Streamlit application

Monte Carlo's [SNA agent](https://hub.docker.com/r/montecarlodata/sna-agent).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative deployment options.

## Configuration
### Setup helper resources
- As `ACCOUNTADMIN` execute the SQL script available [here](https://github.com/monte-carlo-data/apollo-agent/blob/main/scripts/setup_app.sql), this script will:
  - Create a new database: `MCD_AGENT_HELPER`.
  - Define a new role: `MCD_AGENT_ROLE`.
  - Grants imported privileges on database "SNOWFLAKE" to the new role, this is required to collect query logs.
  - Define a new stored procedure: `MCD_AGENT_EXECUTE_QUERY` which runs `as owner` (the role defined above) and is used by the application to execute queries.
  - You'll later grant this role access to the databases you'd like to monitor with Monte Carlo.

### Grant access to monitored databases
- Edit the SQL script available [here](https://github.com/monte-carlo-data/apollo-agent/blob/main/scripts/permissions.sql):
  - Update the value of the `database_to_monitor` session variable to match the name of the database to monitor.
- As `ACCOUNTADMIN` execute the script, this script:
  - Grants `MCD_AGENT_ROLE` the required read-only permissions for Monte Carlo to monitor the specified database.
  - Uses `FUTURE GRANTS` to ensure that any new objects created in the database are automatically accessible to Monte Carlo.

### Configure the agent
Once the scripts described in the previous steps were executed you can proceed to setup the application itself.
The only configuration required is the authentication credentials that will be used to connect to Monte Carlo:
- Execute the Snowflake Application.
- Enter the Key ID and Secret you got from Monte Carlo and click on "Configure".
- This will save the credentials in a Snowflake secret and start the Snowpark Container Service used by the application.
- Switch to the "Advanced" tab and click on "Container Status", it will take a few minutes for the container to be available.
- Once the container status is "Ready", the agent is ready to be used.
- You can use the "Reachability Test" button to check connectivity with Monte Carlo cloud services using the provided credentials.

