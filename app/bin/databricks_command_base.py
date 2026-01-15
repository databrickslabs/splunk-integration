"""Base class for Databricks commands that submit runs and ingest data to Splunk."""

import ta_databricks_declare  # noqa: F401
import json
import time
import traceback

import databricks_common_utils as utils
import databricks_const as const
from log_manager import setup_logging


def create_command_info(username, account_name, args, uid, extra_fields=None):
    """
    Create the base info dictionary for command processing.

    :param username: The Splunk username executing the command
    :param account_name: The Databricks account name
    :param args: Search command arguments
    :param uid: Unique identifier for this execution
    :param extra_fields: Optional dict of additional fields to include
    :return: Dictionary with command processing info
    """
    info = {
        "user": username,
        "account_name": account_name,
        "created_time": time.time(),
        "param": args,
        "run_id": "-",
        "run_execution_status": "-",
        "output_url": "-",
        "result_url": "-",
        "command_submission_status": "Failed",
        "error": "-",
        "uid": uid
    }
    if extra_fields:
        info.update(extra_fields)
    return info


def update_info_on_success(info_to_process, response, output_url):
    """
    Update info dictionary after successful run submission.

    :param info_to_process: The info dictionary to update
    :param response: API response from Databricks
    :param output_url: The run page URL from Databricks
    """
    info_to_process.update(response)
    if output_url:
        result_url = output_url.rstrip("/") + "/resultsOnly"
        info_to_process["output_url"] = output_url
        info_to_process["result_url"] = result_url
        info_to_process["command_submission_status"] = "Success"
        info_to_process["run_execution_status"] = "Initiated"


def ingest_command_data(info_to_process, session_key, index, sourcetype, logger):
    """
    Ingest command data into Splunk.

    :param info_to_process: The data to ingest
    :param session_key: Splunk session key
    :param index: Target Splunk index
    :param sourcetype: Sourcetype for the ingested data
    :param logger: Logger instance for logging
    """
    try:
        logger.info(f"Ingesting the data into Splunk index: {index}")
        indented_json = json.dumps(info_to_process, indent=4)
        logger.info(f"Data to be ingested in Splunk:\n{indented_json}")
        utils.ingest_data_to_splunk(info_to_process, session_key, index, sourcetype)
        logger.info(f"Successfully ingested the data into Splunk index: {index}.")
    except Exception:
        logger.error(f"Error occurred while ingesting data into Splunk. Error: {traceback.format_exc()}")


def handle_command_error(exception, info_to_process, command, logger):
    """
    Handle command errors by logging and updating info dictionary.

    :param exception: The exception that occurred
    :param info_to_process: The info dictionary to update with error
    :param command: The GeneratingCommand instance for writing errors
    :param logger: Logger instance for logging
    """
    logger.error(exception)
    logger.error(traceback.format_exc())
    info_to_process["error"] = str(exception)
    command.write_error(str(exception))


class QueryPollingResult:
    """Result of a polling operation."""

    def __init__(self, response, status, timed_out=False, error_message=None):
        self.response = response
        self.status = status
        self.timed_out = timed_out
        self.error_message = error_message

    @property
    def is_success(self):
        return not self.timed_out and self.error_message is None


def poll_query_status(client, status_endpoint, args, timeout_seconds,
                      success_states, failure_states, logger,
                      sleep_interval=None, get_status_func=None):
    """
    Generic polling mechanism with timeout for query execution.

    :param client: DatabricksClient instance
    :param status_endpoint: API endpoint to check status
    :param args: Arguments to pass to the status endpoint
    :param timeout_seconds: Maximum time to wait in seconds
    :param success_states: Set/list of states indicating success
    :param failure_states: Set/list of states indicating failure
    :param logger: Logger instance
    :param sleep_interval: Time to sleep between polls (default from const)
    :param get_status_func: Function to extract status from response (default: response.get("status"))
    :return: QueryPollingResult
    """
    if sleep_interval is None:
        sleep_interval = const.COMMAND_SLEEP_INTERVAL_IN_SECONDS

    if get_status_func is None:
        get_status_func = lambda r: r.get("status")

    total_wait_time = 0
    response = None
    status = None

    while total_wait_time <= timeout_seconds:
        response = client.databricks_api("get", status_endpoint, args=args)
        status = get_status_func(response)
        logger.info(f"Query execution status: {status}.")

        if status in failure_states:
            return QueryPollingResult(response, status, error_message=f"Query failed with status: {status}")

        if status in success_states:
            return QueryPollingResult(response, status)

        # Calculate remaining time and sleep appropriately
        seconds_to_timeout = timeout_seconds - total_wait_time

        if seconds_to_timeout < sleep_interval:
            if not seconds_to_timeout:
                total_wait_time += 1
                continue

            logger.info(f"Query execution in progress, will retry after {seconds_to_timeout} seconds.")
            time.sleep(seconds_to_timeout)
            total_wait_time += seconds_to_timeout
        else:
            logger.info(f"Query execution in progress, will retry after {sleep_interval} seconds.")
            time.sleep(sleep_interval)
            total_wait_time += sleep_interval

    # Timeout occurred
    return QueryPollingResult(
        response, status, timed_out=True,
        error_message=f"Command execution timed out. Last status: {status}."
    )
