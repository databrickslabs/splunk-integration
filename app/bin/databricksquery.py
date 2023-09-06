import ta_databricks_declare  # noqa: F401
import sys
import traceback
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
    cluster = Option(require=False)
    query = Option(require=True)
    account_name = Option(require=True)
    command_timeout = Option(require=False, validate=validators.Integer(minimum=1))

    def cancel_query(self, search_sid, session_key, client, call_args):
        """Method to cancel query execution based on splunk search status."""
        while True:
            try:
                URL = "{}/services/search/jobs/{}".format(get_splunkd_uri(), search_sid)
                _, content = rest.simpleRequest(
                    URL, sessionKey=session_key, method="GET", raiseAllErrors=True, getargs=None)
                namespaces = {'s': 'http://dev.splunk.com/ns/rest', }
                root = ET.fromstring(content)
                dispatch_state = root.find(".//s:key[@name='dispatchState']", namespaces).text
                is_finalized = root.find(".//s:key[@name='isFinalized']", namespaces).text

                if dispatch_state == "FINALIZING" and is_finalized in [1, "1"]:
                    _LOGGER.info("Stop button of Splunk search has been clicked by User. "
                                 "Canceling the query execution.")
                    response, status_code = client.databricks_api("post", const.CANCEL_QUERY_ENDPOINT, data=call_args)
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
        _LOGGER.info("Cluster: {}".format(self.cluster))
        _LOGGER.info("Query: {}".format(self.query))
        _LOGGER.info("Command Timeout: {}".format(self.command_timeout))

        # Get session key and sid
        session_key = self._metadata.searchinfo.session_key
        search_sid = self._metadata.searchinfo.sid

        try:
            if self.command_timeout and self.command_timeout < const.MINIMUM_COMMAND_TIMEOUT_VALUE:
                self.write_error("Command Timeout value must be greater than or equal to {} seconds."
                                 .format(const.MINIMUM_COMMAND_TIMEOUT_VALUE))
                _LOGGER.warning("Command Timeout value must be greater than or equal to {} seconds."
                                " Exiting the command.".format(const.MINIMUM_COMMAND_TIMEOUT_VALUE))
                sys.exit(0)
            # Fetching timeout value
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                self.write_error("Account '{}' not found. Please provide valid Databricks account."
                                 .format(self.account_name))
                _LOGGER.error("Account '{}' not found. Please provide valid Databricks account."
                              " Exiting the command.".format(self.account_name))
                sys.exit(0)
            admin_com_timeout = databricks_configs.get("admin_command_timeout")
            if ((self.command_timeout and self.command_timeout > int(admin_com_timeout)) or not self.command_timeout):
                command_timeout_in_seconds = int(admin_com_timeout)
            else:
                command_timeout_in_seconds = self.command_timeout
            if (self.command_timeout and self.command_timeout > int(admin_com_timeout)):
                _LOGGER.warning("Provided value of Command Timeout ({} seconds) by the user is greater than the maximum"
                                " allowed/permitted value. Using the maximum allowed/permitted value ({} seconds)."
                                .format(self.command_timeout, int(admin_com_timeout)))
                self.write_warning("Setting Command Timeout to maximum allowed/permitted value ({} seconds) as a"
                                   " greater value has been specified ({} seconds) in search."
                                   .format(admin_com_timeout, self.command_timeout))
            else:
                if self.command_timeout:
                    _LOGGER.info("Provided value of Command Timeout ({} seconds) by the user is within the maximum"
                                 " allowed/permitted value ({} seconds)."
                                 .format(self.command_timeout, int(admin_com_timeout)))
                else:
                    _LOGGER.info("No value for Command Timeout is provided. "
                                 "Using the maximum allowed value ({} seconds).".format(admin_com_timeout))
            _LOGGER.info("Setting Command Timeout to {} seconds.".format(command_timeout_in_seconds))

            # Fetching cluster name
            self.cluster = self.cluster or databricks_configs.get("cluster_name")
            if not self.cluster:
                raise Exception(
                    "Databricks cluster is required to execute this custom command. "
                    "Provide a cluster parameter or configure the cluster in the TA's configuration page."
                )

            client = com.DatabricksClient(self.account_name, session_key)

            # Request to get cluster ID
            _LOGGER.info("Requesting cluster ID for cluster: {}.".format(self.cluster))
            cluster_id = client.get_cluster_id(self.cluster)
            _LOGGER.info("Cluster ID received: {}.".format(cluster_id))

            # Request to create context
            _LOGGER.info("Creating Context in cluster.")
            payload = {"language": "sql", "clusterId": cluster_id}
            response = client.databricks_api("post", const.CONTEXT_ENDPOINT, data=payload)

            context_id = response.get("id")
            _LOGGER.info("Context created: {}.".format(context_id))

            # Request to execute command
            _LOGGER.info("Submitting SQL query for execution.")
            payload["contextId"] = context_id
            payload["command"] = self.query
            response = client.databricks_api("post", const.COMMAND_ENDPOINT, data=payload)

            command_id = response.get("id")
            _LOGGER.info("Query submitted, command id: {}.".format(command_id))

            # pulling mechanism
            _LOGGER.info("Fetching query execution status.")
            status = None
            args = {
                "clusterId": cluster_id,
                "contextId": context_id,
                "commandId": command_id,
            }

            cancel_method_thread = threading.Thread(
                target=self.cancel_query, args=(search_sid, session_key, client, args), name="cancel_method_thread")
            cancel_method_thread.start()

            total_wait_time = 0
            while total_wait_time <= command_timeout_in_seconds:
                response = client.databricks_api("get", const.STATUS_ENDPOINT, args=args)
                status = response.get("status")
                _LOGGER.info("Query execution status: {}.".format(status))

                if status in ("Canceled", "Cancelled", "Error"):
                    raise Exception(
                        "Could not complete the query execution. Status: {}.".format(status)
                    )

                elif status == "Finished":
                    if response["results"]["resultType"] == "error":
                        if response["results"].get("cause") and \
                                "CommandCancelledException" in response["results"]["cause"]:
                            raise Exception("Search Canceled!")
                        msg = response["results"].get(
                            "summary", "Error encountered while executing query."
                        )
                        raise Exception(str(msg))

                    if response["results"]["resultType"] != "table":
                        raise Exception(
                            "Encountered unknown result type, terminating the execution."
                        )

                    if response["results"].get("truncated"):
                        _LOGGER.info("Results are truncated due to Databricks API limitations.")
                        self.write_warning(
                            "Results are truncated due to Databricks API limitations."
                        )

                    _LOGGER.info("Query execution successful. Preparing data.")

                    # Prepare list of Headers
                    headers = response["results"]["schema"]
                    schema = []
                    for header in headers:
                        field = header.get("name")
                        schema.append(field)

                    # Fetch Data
                    data = response["results"]["data"]
                    count_of_result = len(data) if data else 0
                    _LOGGER.info("Total number of rows obtained in query's result: {}".format(count_of_result))
                    for d in data:
                        yield dict(zip(schema, d))

                    _LOGGER.info("Data parsed successfully.")
                    break

                seconds_to_timeout = command_timeout_in_seconds - total_wait_time

                if seconds_to_timeout < const.COMMAND_SLEEP_INTERVAL_IN_SECONDS:

                    if not seconds_to_timeout:
                        total_wait_time += 1
                        continue

                    _LOGGER.info(
                        "Query execution in progress, will retry after {} seconds.".format(
                            str(seconds_to_timeout)
                        )
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
                resp_, status_code = client.databricks_api("post", const.CANCEL_QUERY_ENDPOINT, data=args)
                if status_code == 200:
                    _LOGGER.info("Successfully canceled the query execution.")
                    self.write_error("Canceled the execution as command execution timed out")

            # Destroy the context to free-up space in Databricks
            if context_id:
                _LOGGER.info("Deleting context.")
                payload = {"contextId": context_id, "clusterId": cluster_id}
                _ = client.databricks_api("post", const.CONTEXT_DESTROY_ENDPOINT, data=payload)
                _LOGGER.info("Context deleted successfully.")
            _LOGGER.info("Successfully executed databricksquery command.")

        except Exception as e:
            if str(e) == "Search Canceled!":
                _LOGGER.info("Query execution has been canceled!")
            else:
                _LOGGER.error(e)
                _LOGGER.error(traceback.format_exc())
            self.write_error(str(e))


dispatch(DatabricksQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
