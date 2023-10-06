import ta_databricks_declare  # noqa: F401
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET

import databricks_com as com
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunklib.searchcommands import (
    dispatch,
    GeneratingCommand,
    Configuration,
    Option,
    validators,
)
from solnlib.splunkenv import get_splunkd_uri
from splunk import rest

UID = str(uuid.uuid4())
_LOGGER = setup_logging("ta_databricksquery_command", UID)


@Configuration(type="reporting")
class DatabricksQueryCommand(GeneratingCommand):
    """Custom Command of databricksquery."""

    # Take input from user using parameters
    warehouse_id = Option(require=False)
    query = Option(require=True)
    account_name = Option(require=True)
    command_timeout = Option(require=False, validate=validators.Integer(minimum=1))
    limit = Option(require=False, validate=validators.Integer(minimum=1))

    def cancel_query(self, search_sid, session_key, client, statement_id):
        """Method to cancel query execution based on splunk search status."""
        while True:
            try:
                URL = "{}/services/search/jobs/{}".format(get_splunkd_uri(), search_sid)
                _, content = rest.simpleRequest(
                    URL, sessionKey=session_key, method="GET", raiseAllErrors=True, getargs=None
                )
                namespaces = {
                    "s": "http://dev.splunk.com/ns/rest",
                }
                root = ET.fromstring(content)
                dispatch_state = root.find(".//s:key[@name='dispatchState']", namespaces).text
                is_finalized = root.find(".//s:key[@name='isFinalized']", namespaces).text

                if dispatch_state == "FINALIZING" and is_finalized in [1, "1"]:
                    _LOGGER.info(
                        "Stop button of Splunk search has been clicked by User. Canceling the query execution."
                    )
                    response, status_code = client.databricks_api(
                        "post", const.CANCEL_QUERY_ENDPOINT.format(statement_id=statement_id)
                    )
                    if status_code == 200:
                        _LOGGER.info("Successfully canceled the query execution.")
                        break
                else:
                    time.sleep(const.SPLUNK_SEARCH_STATUS_CHECK_INTERVAL)
            except Exception as e:
                _LOGGER.error("Error while attempting to cancel query execution. Error: {}".format(str(e)))

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksquery command.")
        _LOGGER.info("Warehouse ID: {}".format(self.warehouse_id))
        _LOGGER.info("Query: {}".format(self.query))
        _LOGGER.info("Command Timeout: {}".format(self.command_timeout))
        _LOGGER.info("Limit: {}".format(self.limit))

        # Get session key and sid
        session_key = self._metadata.searchinfo.session_key
        search_sid = self._metadata.searchinfo.sid

        try:
            if self.command_timeout and self.command_timeout < const.MINIMUM_COMMAND_TIMEOUT_VALUE:
                self.write_error(
                    "Command Timeout value must be greater than or equal to {} seconds.".format(
                        const.MINIMUM_COMMAND_TIMEOUT_VALUE
                    )
                )
                _LOGGER.warning(
                    "Command Timeout value must be greater than or equal to {} seconds."
                    " Exiting the command.".format(const.MINIMUM_COMMAND_TIMEOUT_VALUE)
                )
                sys.exit(0)

            if self.limit and self.limit < const.MINIMUM_QUERY_ROW_LIMIT:
                self.write_error(
                    "Limit value must be greater than or equal to {} rows.".format(const.MINIMUM_QUERY_ROW_LIMIT)
                )
                _LOGGER.warning(
                    "Limit value must be greater than or equal to {} rows."
                    " Exiting the command.".format(const.MINIMUM_QUERY_ROW_LIMIT)
                )
                sys.exit(0)

            # Fetching TA configurations
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                self.write_error(
                    "Account '{}' not found. Please provide valid Databricks account.".format(self.account_name)
                )
                _LOGGER.error(
                    "Account '{}' not found. Please provide valid Databricks account."
                    " Exiting the command.".format(self.account_name)
                )
                sys.exit(0)

            # Fetching timeout value
            admin_com_timeout = databricks_configs.get("admin_command_timeout")
            if (self.command_timeout and self.command_timeout > int(admin_com_timeout)) or not self.command_timeout:
                command_timeout_in_seconds = int(admin_com_timeout)
            else:
                command_timeout_in_seconds = self.command_timeout
            if self.command_timeout and self.command_timeout > int(admin_com_timeout):
                _LOGGER.warning(
                    "Provided value of Command Timeout ({} seconds) by the user is greater than the maximum"
                    " allowed/permitted value. Using the maximum allowed/permitted value ({} seconds).".format(
                        self.command_timeout, int(admin_com_timeout)
                    )
                )
                self.write_warning(
                    "Setting Command Timeout to maximum allowed/permitted value ({} seconds) as a"
                    " greater value has been specified ({} seconds) in search.".format(
                        admin_com_timeout, self.command_timeout
                    )
                )
            else:
                if self.command_timeout:
                    _LOGGER.info(
                        "Provided value of Command Timeout ({} seconds) by the user is within the maximum"
                        " allowed/permitted value ({} seconds).".format(self.command_timeout, int(admin_com_timeout))
                    )
                else:
                    _LOGGER.info(
                        "No value for Command Timeout is provided. "
                        "Using the maximum allowed value ({} seconds).".format(admin_com_timeout)
                    )
            _LOGGER.info("Setting Command Timeout to {} seconds.".format(command_timeout_in_seconds))

            # Fetching limit value
            query_result_limit = databricks_configs.get("query_result_limit")
            if not self.limit or self.limit > int(query_result_limit):
                row_limit = int(query_result_limit)
            else:
                row_limit = self.limit
            if self.limit and self.limit > int(query_result_limit):
                _LOGGER.warning(
                    "Provided value of Result Limit ({} rows) by the user is greater than the maximum"
                    " allowed/permitted value. Using the maximum allowed/permitted value ({} rows).".format(
                        self.limit, int(query_result_limit)
                    )
                )
                self.write_warning(
                    "Setting Result Limit to maximum allowed/permitted value ({} rows) as a"
                    " greater value has been specified ({} rows) in search.".format(query_result_limit, self.limit)
                )
            else:
                if self.limit:
                    _LOGGER.info(
                        "Provided value of Result Limit ({} rows) by the user is within the maximum"
                        " allowed/permitted value ({} rows).".format(self.limit, int(query_result_limit))
                    )
                else:
                    _LOGGER.info(
                        "No value for Result Limit is provided. "
                        "Using the maximum allowed value ({} rows).".format(query_result_limit)
                    )
            _LOGGER.info("Setting Result Limit to {} rows.".format(row_limit))

            # Fetching warehouse id
            self.warehouse_id = self.warehouse_id or databricks_configs.get("warehouse_id")
            if not self.warehouse_id:
                raise Exception(
                    "Databricks warehouse_id is required to execute this custom command. "
                    "Provide a warehouse_id parameter or configure the Warehouse ID in the TA's configuration page."
                )

            client = com.DatabricksClient(self.account_name, session_key)

            # SQL statement execution payload
            payload = {
                "warehouse_id": self.warehouse_id,
                "statement": self.query,
                "schema": "tpch",
                "disposition": "EXTERNAL_LINKS",
                "format": "JSON_ARRAY",
                "row_limit": row_limit,
            }

            # Request to execute statement
            _LOGGER.info("Submitting SQL query for execution.")
            response = client.databricks_api("post", const.EXECUTE_QUERY_ENDPOINT, data=payload)

            statement_id = response.get("statement_id")
            _LOGGER.info("Query submitted, statement id: {}.".format(statement_id))

            # Check for Splunk search cancellation
            cancel_method_thread = threading.Thread(
                target=self.cancel_query,
                args=(search_sid, session_key, client, statement_id),
                name="cancel_method_thread",
            )
            cancel_method_thread.start()

            # Pulling mechanism
            _LOGGER.info("Fetching query execution status.")
            status = None

            total_wait_time = 0
            while total_wait_time <= command_timeout_in_seconds:
                response = client.databricks_api("get", const.QUERY_STATUS_ENDPOINT.format(statement_id=statement_id))
                status = response.get("status", {}).get("state")
                _LOGGER.info("Query execution status: {}.".format(status))

                if status in ("CANCELED", "CLOSED", "FAILED"):
                    err_message = "Could not complete the query execution. Status: {}.".format(status)
                    if status == "FAILED":
                        err_message += " Error: {}".format(response["status"].get("error", {}).get("message"))
                    raise Exception(err_message)

                elif status == "SUCCEEDED":
                    _LOGGER.info("Query execution successful. Preparing data.")

                    if response["manifest"].get("truncated"):
                        _LOGGER.info("Result row limit exceeded, hence results are truncated.")
                        self.write_warning("Result limit exceeded, hence results are truncated.")

                    total_row_count = response["manifest"]["total_row_count"]
                    _LOGGER.info("Total number of rows obtained in query's result: {}".format(total_row_count))

                    # Prepare list of Headers
                    headers = response["manifest"]["schema"]["columns"]
                    schema = []
                    for header in headers:
                        field = header.get("name")
                        schema.append(field)

                    _LOGGER.info("Result table schema: {}".format(schema))

                    # Method to fetch data of every chunk
                    def fetch_data(external_links, schema):
                        chunk_index = external_links[0]["chunk_index"]
                        external_link = external_links[0]["external_link"]

                        # Fetch Data
                        response = client.external_api("get", external_link)
                        _LOGGER.info(
                            "Total number of rows obtained in chunk-{} of query result: {}".format(
                                chunk_index, len(response)
                            )
                        )
                        for row in response:
                            yield dict(zip(schema, row))

                    # Get external link of first chunk
                    external_links = response["result"].get("external_links")
                    if not external_links:
                        raise Exception("No data returned from execution of this query.")
                    next_chunk_internal_link = external_links[0].get("next_chunk_internal_link")

                    # Fetch data of first chunk
                    yield from fetch_data(external_links, schema)

                    while next_chunk_internal_link:
                        # Get external links of next chunk
                        response = client.databricks_api("get", next_chunk_internal_link)
                        external_links = response["external_links"]
                        next_chunk_internal_link = external_links[0].get("next_chunk_internal_link")

                        # Fetch data of nth chunk
                        yield from fetch_data(external_links, schema)

                    _LOGGER.info("Data parsed successfully.")
                    break

                # If statement execution is in ["RUNNING", "PENDING"] state
                seconds_to_timeout = command_timeout_in_seconds - total_wait_time

                if seconds_to_timeout < const.COMMAND_SLEEP_INTERVAL_IN_SECONDS:

                    if not seconds_to_timeout:
                        total_wait_time += 1
                        continue

                    _LOGGER.info(
                        "Query execution in progress, will retry after {} seconds.".format(str(seconds_to_timeout))
                    )
                    time.sleep(seconds_to_timeout)
                    total_wait_time += seconds_to_timeout
                    continue

                _LOGGER.info(
                    "Query execution in progress, will retry after {} seconds.".format(
                        str(const.COMMAND_SLEEP_INTERVAL_IN_SECONDS)
                    )
                )
                time.sleep(const.COMMAND_SLEEP_INTERVAL_IN_SECONDS)
                total_wait_time += const.COMMAND_SLEEP_INTERVAL_IN_SECONDS
            else:
                # Timeout scenario
                msg = "Command execution timed out. Last status: {}.".format(status)
                _LOGGER.info(msg)
                _LOGGER.info("Canceling the query execution")
                resp_, status_code = client.databricks_api(
                    "post", const.CANCEL_QUERY_ENDPOINT.format(statement_id=statement_id)
                )
                if status_code == 200:
                    _LOGGER.info("Successfully canceled the query execution.")
                    self.write_error("Canceled the execution as command execution timed out")

            _LOGGER.info("Successfully executed databricksquery command.")

        except Exception as e:
            if str(e) == "Search Canceled!":
                _LOGGER.info("Query execution has been canceled!")
            else:
                _LOGGER.exception(e)
            self.write_error(str(e))


dispatch(DatabricksQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
