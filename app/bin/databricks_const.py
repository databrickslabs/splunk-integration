import os

# API Endpoints
CLUSTER_ENDPOINT = "/api/2.0/clusters/list"
CONTEXT_ENDPOINT = "/api/1.2/contexts/create"
CONTEXT_DESTROY_ENDPOINT = "/api/1.2/contexts/destroy"
COMMAND_ENDPOINT = "/api/1.2/commands/execute"
STATUS_ENDPOINT = "/api/1.2/commands/status"
GET_RUN_ENDPOINT = "/api/2.0/jobs/runs/get"
RUN_SUBMIT_ENDPOINT = "/api/2.0/jobs/runs/submit"
EXECUTE_JOB_ENDPOINT = "/api/2.0/jobs/run-now"
GET_JOB_ENDPOINT = "/api/2.0/jobs/get"
AAD_TOKEN_ENDPOINT = "https://login.microsoftonline.com/{}/oauth2/v2.0/token"

# Azure Databricks scope
SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

# App Name
APP_NAME = __file__.split(os.sep)[-3]

# KV Store collection name
KV_COLLECTION_NAME_SUBMIT_RUN = "databricks_submit_run_log"
KV_COLLECTION_NAME_EXECUTE_JOB = "databricks_execute_job_log"

REQUIRED_ROLES = ['databricks_user', 'databricks_admin']

# Command execution configs
COMMAND_TIMEOUT_IN_SECONDS = 300
COMMAND_SLEEP_INTERVAL_IN_SECONDS = 3

USER_AGENT_CONST = "Databricks-AddOnFor-Splunk-1.2.0"

VERIFY_SSL = True
INTERNAL_VERIFY_SSL = False
RETRIES = 3
BACKOFF_FACTOR = 60
TIMEOUT = 300
STATUS_FORCELIST = [429, 500, 502, 503, 504]

# Error codes and message
ERROR_CODE = {
    '700016': 'Invalid Client ID provided.',
    '900023': 'Invalid Tenant ID provided.',
    '7000215': 'Invalid Client Secret provided.',
    '403': 'Client secret may have expired. Please configure a valid Client secret.',
    '404': 'Invalid API endpoint.',
    '500': 'Internal server error.',
    '400': 'Bad request. The request is malformed.',
    '429': 'API limit exceeded. Please try again after some time.'
}
