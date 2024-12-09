# Monte Carlo Data Collector - Snowflake Artemis Agent - Streamlit application

Monte Carlo's [SNA agent](https://docs.getmontecarlo.com/docs/sna-agent-deployment).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative deployment options.

## Permissions
In the setup guide, you'll be asked to grant additional privileges from your account.

### Account level privileges

`CREATE COMPUTE POOL`
A compute pool is required to run a Snowpark container service with the agent, this service
is used to execute queries on the monitored databases.

`CREATE WAREHOUSE`
As part of the setup process, the agent will create a warehouse to run queries on the monitored databases.

### Privileges to objects

The application will request access to:
- Monte Carlo backend services at `https://artemis.getmontecarlo.com`.
- Stored Procedure deployed as part of the setup process in the helper database, see steps [here](#setup-code).

Additionally, for each monitored database, you'll need to execute [this script](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/permissions.sql), which grants the following privileges to the role `MCD_AGENT_ROLE`:

`IMPORTED PRIVILEGES` on **DATABASE**
To access query history.

`USAGE`,`MONITOR`on **DATABASE**
To access metadata.

`USAGE`,`MONITOR`on **ALL SCHEMAS IN DATABASE**
To access metadata.

`REFERENCES` on **ALL TABLES IN DATABASE**, **ALL VIEWS IN DATABASE**, **ALL EXTERNAL TABLES IN DATABASE**
To access metadata.

`MONITOR` on **ALL DYNAMIC TABLES IN DATABASE**
To access metadata.

`REFERENCES` on **FUTURE TABLES IN DATABASE**, **FUTURE VIEWS IN DATABASE**, **FUTURE EXTERNAL TABLES IN DATABASE**
To access metadata for future objects.

`MONITOR` on **FUTURE DYNAMIC TABLES IN DATABASE**
To access metadata for future dynamic tables.

`USAGE`,`MONITOR`on **FUTURE SCHEMAS IN DATABASE**
To access metadata for future schemas.

`SELECT` on **ALL TABLES IN DATABASE**, **ALL VIEWS IN DATABASE**, **ALL EXTERNAL TABLES IN DATABASE**, **ALL DYNAMIC TABLES IN DATABASE**, **ALL STREAMS IN DATABASE**
Read-only access to query objects.

`SELECT` on **FUTURE TABLES IN DATABASE**, **FUTURE VIEWS IN DATABASE**, **FUTURE EXTERNAL TABLES IN DATABASE**, **FUTURE DYNAMIC TABLES IN DATABASE**, **FUTURE STREAMS IN DATABASE**
Read-only access to query future objects.

- --

## Object creation

In the setup guide, you'll be asked to create the following object(s) in your account.

You'll need to execute [this script](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/setup_app.sql), which will create the following objects:
- `MCD_AGENT_ROLE`: role used to execute queries.
- `MCD_AGENT_HELPER` database owned by `MCD_AGENT_ROLE`, which includes:
    - `MCD_AGENT_EXECUTE_QUERY` stored procedure used to execute queries on behalf of `MCD_AGENT_ROLE`.
    - This stored procedure is used to avoid a limitation with Snowflake Applications (not being able to grant `FUTURE GRANTS` to them).
    - Accessing future objects and retain existing permissions on re-created objects is critical for Monte Carlo’s observability features.
    - For more information, see [FAQ for the Snowflake Agent](https://docs.getmontecarlo.com/docs/sna-agent-deployment#faqs).

Additionally, the following objects will be created in the application's database:
- `CORE.MCD_AGENT_TOKEN`: Secret object used to store the authentication token to Monte Carlo platform.
- `CORE.DATA_STORE`: Internal stage used to store data when the response exceeds a given threshold or for some Monte Carlo features like data sampling.
- `CORE.EXECUTE_HELPER_QUERY`: A stored procedure used to invoke the helper stored procedure using references.
- `APP_PUBLIC.SUSPEND_SERVICE`, `APP_PUBLIC.RESUME_SERVICE`, `APP_PUBLIC.RESTART_SERVICE`: Stored procedures used to suspend, resume and restart the service.
- `APP_PUBLIC.UPDATE_TOKEN`: Stored procedure used to update the token in the secret.
- `APP_PUBLIC.SERVICE_STATUS`: Used to get the status of the service.
- `APP_PUBLIC.SERVICE_LOGS`: Used to retrieve the logs for the service.
- `APP_ADMIN.GET_CONFIG_FOR_REFERENCE`, `APP_ADMIN.REGISTER_SINGLE_REFERENCE`: Used for references registration.
- `MCD_AGENT_COMPUTE_POOL`: Compute pool used to host the service.
- `MCD_AGENT_WH`: Warehouse used to execute queries.
- `CORE.MCD_AGENT_SERVICE`: The service itself.
- UDFs:
  - `CORE.REACHABILITY_TEST`: Used to test access (reachability) to the Monte Carlo platform from the agent.
  - `CORE.QUERY_COMPLETED`: Used to notify the container that an async query has completed successfully.
  - `CORE.QUERY_FAILED`: Used to notify the container that an async query has completed unsuccessfully. 

Please note the source code for the application is public, so you can inspect the scripts used to the create all these objects here: https://github.com/monte-carlo-data/sna-artemis-agent

- --

## Setup code
As described in the next section, you'll need to execute the following SQL scripts to set up the agent:
- Setup helper resources: [script](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/setup_app.sql).
- Grant access to monitored databases: [script](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/permissions.sql).

## Setup instructions

### Setup helper resources
- As `ACCOUNTADMIN` execute the SQL script available [here](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/setup_app.sql), this script will:
  - Create a new database: `MCD_AGENT_HELPER`.
  - Define a new role: `MCD_AGENT_ROLE`.
  - Grants imported privileges on database "SNOWFLAKE" to the new role, this is required to collect query logs.
  - Define a new stored procedure: `MCD_AGENT_EXECUTE_QUERY` which runs `as owner` (the role defined above) and is used by the application to execute queries.
  - You'll later grant this role access to the databases you'd like to monitor with Monte Carlo.

### Grant access to monitored databases
- Edit the SQL script available [here](https://raw.githubusercontent.com/monte-carlo-data/sna-artemis-agent/refs/heads/main/scripts/permissions.sql):
  - Update the value of the `database_to_monitor` session variable to match the name of the database to monitor.
- As `ACCOUNTADMIN` execute the script, this script:
  - Grants `MCD_AGENT_ROLE` the required read-only permissions for Monte Carlo to monitor the specified database.
  - Uses `FUTURE GRANTS` to ensure that any new objects created in the database are automatically accessible to Monte Carlo.

### Configure the agent
Once the scripts described in the previous steps were executed you can proceed to setup the application itself.
The only configuration required is the authentication credentials that will be used to connect to Monte Carlo:
- Execute the Snowflake Application:
  - Grant the required permissions.
  - Grant access to the external access integration.
  - Launch the application
- Click "Update Token":
  - Enter the Key ID and Secret you got from Monte Carlo and click on "Update Token".
  - This will save the credentials in a Snowflake secret and start the Snowpark Container Service used by the application.
- Click on "Container Status", it will take a few minutes for the container to be available.
- Once the container status is "Ready", the agent is ready to be used.
- You can use the "Reachability Test" button to check connectivity with Monte Carlo cloud services using the provided credentials.

## Usage Snippets
### Restart the Service:
```sql
CALL MCD_AGENT.APP_PUBLIC.RESTART_SERVICE();
```

### Update the token
```sql
CALL MCD_AGENT.APP_PUBLIC.UPDATE_TOKEN('KEY_ID', 'KEY_SECRET');
```

### Update the warehouse size
```sql
CALL MCD_AGENT.APP_PUBLIC.SETUP_APP(wh_size => 'small')
```

### Get service logs
```sql
CALL MCD_AGENT.APP_PUBLIC.SERVICE_LOGS(500);
```

