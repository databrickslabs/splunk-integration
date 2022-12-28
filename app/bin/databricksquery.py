import ta_databricks_declare  # noqa: F401
import sys
import traceback
import time

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

_LOGGER = setup_logging("ta_databricksquery_command")


@Configuration(type="events")
class DatabricksQueryCommand(GeneratingCommand):
    """Custom Command of databricksquery."""

    # Take input from user using parameters
    cluster = Option(require=False)
    query = Option(require=True)
    account_name = Option(require=True)
    command_timeout = Option(require=False, validate=validators.Integer(minimum=1))

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksquery command")
        command_timeout_in_seconds = self.command_timeout or const.COMMAND_TIMEOUT_IN_SECONDS
        _LOGGER.info("Setting command timeout to {} seconds.".format(command_timeout_in_seconds))

        # Get session key
        session_key = self._metadata.searchinfo.session_key

        try:
            # Check User role
            if not utils.check_user_roles(session_key):
                error_msg = ('Lack of "databricks_user" role for the current user.'
                             ' Refer "Provide Required Access" section in the Intro page.')
                _LOGGER.error(error_msg)
                raise Exception(error_msg)

            # Fetching cluster name
            self.cluster = self.cluster or utils.get_databricks_configs(
                session_key, self.account_name
            ).get("cluster_name")
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

            total_wait_time = 0
            while total_wait_time <= command_timeout_in_seconds:
                response = client.databricks_api("get", const.STATUS_ENDPOINT, args=args)

                status = response.get("status")
                _LOGGER.info("Query execution status: {}.".format(status))

                if status in ("Cancelled", "Error"):
                    raise Exception(
                        "Could not complete the query execution. Status: {}.".format(status)
                    )

                elif status == "Finished":
                    if response["results"]["resultType"] == "error":
                        msg = response["results"].get(
                            "summary", "Error encountered while executing query."
                        )
                        raise Exception(str(msg))

                    if response["results"]["resultType"] != "table":
                        raise Exception(
                            "Encountered unknown result type, terminating the execution."
                        )

                    if response["results"].get("truncated", True):
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
                self.write_error(msg)

            # Destroy the context to free-up space in Databricks
            if context_id:
                _LOGGER.info("Deleting context.")
                payload = {"contextId": context_id, "clusterId": cluster_id}
                _ = client.databricks_api("post", const.CONTEXT_DESTROY_ENDPOINT, data=payload)
                _LOGGER.info("Context deleted successfully.")

        except Exception as e:
            _LOGGER.error(e)
            _LOGGER.error(traceback.format_exc())
            self.write_error(str(e))


dispatch(DatabricksQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
