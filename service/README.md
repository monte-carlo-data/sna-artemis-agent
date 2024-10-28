# Monte Carlo Data Collector - Artemis Agent - Snowflake Container Service

Monte Carlo's [SNA agent](https://hub.docker.com/r/montecarlodata/sna-agent).
See [here](https://docs.getmontecarlo.com/docs/platform-architecture) for architecture details and alternative deployment options.

## Local development environment
### Pre-requisites
- Python 3.12 or later

### Prepare your local environment
- Create a virtual env in the parent directory (`sna-artemis-agent`), for example: `python -m venv .venv` and activate it: `. .venv/bin/activate`
  - If you don't use the virtual env in `.venv` you must create a symbolic link: `ln -s VENV_DIR .venv` because pyright requires the virtual env to be in `.venv` directory in the repository root folder.
- Install the required libraries: `pip install -r requirements-build.txt -r requirements.txt -r requirements-dev.txt`
- Install the pre-commit hooks (fro the root folder): `pre-commit install`

### Tests execution
- To run tests, use `pytest` (the configuration for pytest in `pyproject.toml` configures `.` as the `pythonpath` and `./tests` as the test folder).

### Local application execution
- Artemis SNA Agent uses a Flask application
- To run it: `python -m agent.main`
- The server will listen in port `8081` and you can call the `health` endpoint by accessing: http://localhost:8081/api/v1/test/health:
  ```shell
  curl http://localhost:8081/api/v1/test/health | jq
  ```

#### Local execution using Docker
You can also execute the agent building and running a Docker image:
```shell
docker build -t local_agent --platform=linux/amd64 .;\
docker run --rm --name local_agent -p8081:8081 -ePORT=8081 -it local_agent
```
Or running the latest dev image from DockerHub:
```shell
docker run --rm --name dev_agent -p8081:8081 -ePORT=8081 -it montecarlodata/prerelease-sna-agent:latest
```

And you can run the unit tests in Docker:
```shell
docker build -t test_agent --target tests --platform=linux/amd64 --build-arg CACHEBUST="`date`" --progress=plain .
```
**Note**: `CACHEBUST` is used as a way to skip the cached layer for the tests execution and force them to run again.

## License

See [LICENSE](https://github.com/monte-carlo-data/sna-artemis-agent/blob/main/LICENSE.md) for more information.

## Security

See [SECURITY](https://github.com/monte-carlo-data/apollo-agent/blob/main/SECURITY.md) for more information.