import os

# API Endpoints
CLUSTER_ENDPOINT = "/api/2.0/clusters/list"
CONTEXT_ENDPOINT = "/api/1.2/contexts/create"
CONTEXT_DESTROY_ENDPOINT = "/api/1.2/contexts/destroy"
COMMAND_ENDPOINT = "/api/1.2/commands/execute"
STATUS_ENDPOINT = "/api/1.2/commands/status"
EXECUTE_QUERY_ENDPOINT = "/api/2.0/sql/statements/"
QUERY_STATUS_ENDPOINT = "/api/2.0/sql/statements/{statement_id}"
CANCEL_QUERY_ENDPOINT_CLUSTER = "/api/1.2/commands/cancel"
CANCEL_QUERY_ENDPOINT_DBSQL = "/api/2.0/sql/statements/{statement_id}/cancel"
GET_RUN_ENDPOINT = "/api/2.0/jobs/runs/get"
RUN_SUBMIT_ENDPOINT = "/api/2.0/jobs/runs/submit"
EXECUTE_JOB_ENDPOINT = "/api/2.0/jobs/run-now"
GET_JOB_ENDPOINT = "/api/2.0/jobs/get"
CANCEL_JOB_RUN_ENDPOINT = "/api/2.0/jobs/runs/cancel"
AAD_TOKEN_ENDPOINT = "https://login.microsoftonline.com/{}/oauth2/v2.0/token"
WAREHOUSE_STATUS_ENDPOINT = "/api/2.0/sql/warehouses"
WAREHOUSE_START_ENDPOINT = "/api/2.0/sql/warehouses/{}/start"
SPECIFIC_WAREHOUSE_STATUS_ENDPOINT = "/api/2.0/sql/warehouses/{}"

# Azure Databricks scope
SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

# App Name
APP_NAME = __file__.split(os.sep)[-3]

# Command execution configs
COMMAND_SLEEP_INTERVAL_IN_SECONDS = 3

SPLUNK_SEARCH_STATUS_CHECK_INTERVAL = 10

MINIMUM_COMMAND_TIMEOUT_VALUE = 30

MINIMUM_QUERY_ROW_LIMIT = 1

USER_AGENT_CONST = "Databricks-AddOnFor-Splunk-1.4.2"

VERIFY_SSL = True
INTERNAL_VERIFY_SSL = False
RETRIES = 3
BACKOFF_FACTOR = 60
TIMEOUT = 300
STATUS_FORCELIST = [429, 500, 502, 503, 504]

# Error codes and message
ERROR_CODE = {
    "700016": "Invalid Client ID provided.",
    "900023": "Invalid Tenant ID provided.",
    "7000215": "Invalid Client Secret provided.",
    "400": "Bad request. The request is malformed.",
    "401": "Unauthorized. Access token may be invalid or expired.",
    "403": "Client secret may have expired. Please configure a valid Client secret.",
    "404": "Invalid API endpoint.",
    "429": "API limit exceeded. Please try again after some time.",
    "500": "Internal server error.",
    "invalid_client": "Invalid OAuth Client ID or Client Secret provided.",
    "unauthorized_client": "Service principal is not authorized for this workspace.",
    "invalid_grant": "The provided OAuth credentials are invalid or expired.",
}
