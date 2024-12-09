# Monte Carlo Data Collector - Snowflake Artemis Agent

Monte Carlo's [SNA agent](https://docs.getmontecarlo.com/docs/sna-agent-deployment).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative
deployment options.

# Project description
This project is a Monte Carlo Data egress-only agent for Snowflake, it runs as a Snowflake Native Application using Snowpark Container Services.
The UI in the Snowflake Application is used only for configuration and troubleshooting, the agent itself runs in the container.
The agent starts automatically when the container starts, connects to the Monte Carlo Cloud services, and listens for requests to execute Snowflake queries to collect metadata and other resources like query logs.

# Project structure
This project consists of:
- [service](./service/README.md): The Snowpark Container Service that runs the agent.
- [app](./app/README.md): The Streamlit application with the UI for the Snowflake Native Application

# App Configuration
The [README.md](./app/README.md) file for the UI application includes the required steps to configure the application once installed.

# License

See [LICENSE](./LICENSE.md) for more information.

# Security

See [SECURITY](./SECURITY.md) for more information.
