# enable/disable connection pool
CONFIG_USE_CONNECTION_POOL = "USE_CONNECTION_POOL"
# connection pool size
CONFIG_CONNECTION_POOL_SIZE = "CONNECTION_POOL_SIZE"
# number of threads used to execute queries
CONFIG_QUERIES_RUNNER_THREAD_COUNT = "QUERIES_RUNNER_THREAD_COUNT"
# number of threads used to execute operations (like storage, health, etc.)
CONFIG_OPS_RUNNER_THREAD_COUNT = "OPS_RUNNER_THREAD_COUNT"
# number of threads used to publish results to MC backend
CONFIG_PUBLISHER_THREAD_COUNT = "PUBLISHER_THREAD_COUNT"
# how to run queries, by default we run them asynchronously, set this to `true` to run them synchronously
# you should update the size of the connection pool and the number of query runners accordingly
CONFIG_USE_SYNC_QUERIES = "USE_SYNC_QUERIES"
# name of the stage to use to store files
CONFIG_STAGE_NAME = "STAGE_NAME"
