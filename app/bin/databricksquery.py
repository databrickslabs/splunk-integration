import ta_databricks_declare  # noqa: F401
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

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
    cluster = Option(require=False)
    query = Option(require=True)
    account_name = Option(require=True)
    command_timeout = Option(require=False, validate=validators.Integer(minimum=1))
    limit = Option(require=False, validate=validators.Integer(minimum=1))

    def cancel_query(self, search_sid, session_key, client, cancel_endpoint, data_for_cancelation):
        """Method to cancel query execution based on splunk search status."""
        while True:
            try:
                URL = f"{get_splunkd_uri()}/services/search/jobs/{search_sid}"
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
                        "post", cancel_endpoint, data=data_for_cancelation
                    )
                    if status_code == 200:
                        _LOGGER.info("Successfully canceled the query execution.")
                    else:
                        _LOGGER.error(f"Error while attempting to cancel the query execution."
                                      f" Response returned from API : {response}")
                    break
                else:
                    time.sleep(const.SPLUNK_SEARCH_STATUS_CHECK_INTERVAL)
            except Exception as e:
                if "unknown sid" in str(e).lower():
                    _LOGGER.debug(f"Query execution can not be canceled anymore as Splunk's search "
                                  f"ID does not exist. Error: {e}")
                else:
                    _LOGGER.debug(f"Unknown error occured. Error: {e}")
                break

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksquery command.")
        _LOGGER.info(f"Warehouse ID: {self.warehouse_id}")
        _LOGGER.info(f"Cluster: {self.cluster}")
        _LOGGER.info(f"Query: {self.query}")
        _LOGGER.info(f"Command Timeout: {self.command_timeout}")
        _LOGGER.info(f"Limit: {self.limit}")

        # Get session key and sid
        session_key = self._metadata.searchinfo.session_key
        search_sid = self._metadata.searchinfo.sid

        try:
            if self.command_timeout and self.command_timeout < const.MINIMUM_COMMAND_TIMEOUT_VALUE:
                self.write_error(
                    f"Command Timeout value must be greater than or equal to {const.MINIMUM_COMMAND_TIMEOUT_VALUE} seconds."
                )
                _LOGGER.warning(
                    f"Command Timeout value must be greater than or equal to {const.MINIMUM_COMMAND_TIMEOUT_VALUE} seconds."
                    " Exiting the command."
                )
                sys.exit(0)

            def handle_invalid_limit_value():
                if self.limit and self.limit < const.MINIMUM_QUERY_ROW_LIMIT:
                    self.write_error(
                        f"Limit value must be greater than or equal to {const.MINIMUM_QUERY_ROW_LIMIT} rows."
                    )
                    _LOGGER.error(
                        f"Limit value must be greater than or equal to {const.MINIMUM_QUERY_ROW_LIMIT} rows."
                        " Exiting the command."
                    )
                    sys.exit(0)

            # Fetching TA configurations
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                self.write_error(
                    f"Account '{self.account_name}' not found. Please provide valid Databricks account."
                )
                _LOGGER.error(
                    f"Account '{self.account_name}' not found. Please provide valid Databricks account."
                    " Exiting the command."
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
                    f"Provided value of Command Timeout ({self.command_timeout} seconds) by the user is greater than the maximum"
                    f" allowed/permitted value. Using the maximum allowed/permitted value ({int(admin_com_timeout)} seconds)."
                )
                self.write_warning(
                    f"Setting Command Timeout to maximum allowed/permitted value ({admin_com_timeout} seconds) as a"
                    f" greater value has been specified ({self.command_timeout} seconds) in search."
                )
            else:
                if self.command_timeout:
                    _LOGGER.info(
                        f"Provided value of Command Timeout ({self.command_timeout} seconds) by the user is within the maximum"
                        f" allowed/permitted value ({int(admin_com_timeout)} seconds)."
                    )
                else:
                    _LOGGER.info(
                        f"No value for Command Timeout is provided. "
                        f"Using the maximum allowed value ({admin_com_timeout} seconds)."
                    )
            _LOGGER.info(f"Setting Command Timeout to {command_timeout_in_seconds} seconds.")

            def fetch_limit_value():
                # Fetching limit value
                query_result_limit = databricks_configs.get("query_result_limit")
                if not self.limit or self.limit > int(query_result_limit):
                    row_limit = int(query_result_limit)
                else:
                    row_limit = self.limit
                if self.limit and self.limit > int(query_result_limit):
                    _LOGGER.warning(
                        f"Provided value of Result Limit ({self.limit} rows) by the user is greater than the maximum"
                        f" allowed/permitted value. Using the maximum allowed/permitted value ({int(query_result_limit)} rows)."
                    )
                    self.write_warning(
                        f"Setting Result Limit to maximum allowed/permitted value ({query_result_limit} rows) as a"
                        f" greater value has been specified ({self.limit} rows) in search."
                    )
                else:
                    if self.limit:
                        _LOGGER.info(
                            f"Provided value of Result Limit ({self.limit} rows) by the user is within the maximum"
                            f" allowed/permitted value ({int(query_result_limit)} rows)."
                        )
                    else:
                        _LOGGER.info(
                            f"No value for Result Limit is provided. "
                            f"Using the maximum allowed value ({query_result_limit} rows)."
                        )
                _LOGGER.info(f"Setting Result Limit to {row_limit} rows.")
                return row_limit

            client = com.DatabricksClient(self.account_name, session_key)

            def handle_cluster_method():
                # Request to get cluster ID
                _LOGGER.info(f"Requesting cluster ID for cluster: {self.cluster}.")
                cluster_id = client.get_cluster_id(self.cluster)
                _LOGGER.info(f"Cluster ID received: {cluster_id}.")

                # Request to create context
                _LOGGER.info("Creating Context in cluster.")
                payload = {"language": "sql", "clusterId": cluster_id}
                response = client.databricks_api("post", const.CONTEXT_ENDPOINT, data=payload)

                context_id = response.get("id")
                _LOGGER.info(f"Context created: {context_id}.")

                # Request to execute command
                _LOGGER.info("Submitting SQL query for execution.")
                payload["contextId"] = context_id
                payload["command"] = self.query
                response = client.databricks_api("post", const.COMMAND_ENDPOINT, data=payload)

                command_id = response.get("id")
                _LOGGER.info(f"Query submitted, command id: {command_id}.")

                # pulling mechanism
                _LOGGER.info("Fetching query execution status.")
                status = None
                args = {
                    "clusterId": cluster_id,
                    "contextId": context_id,
                    "commandId": command_id,
                }
                cancel_endpoint = const.CANCEL_QUERY_ENDPOINT_CLUSTER
                cancel_method_thread = threading.Thread(
                    target=self.cancel_query,
                    args=(search_sid, session_key, client, cancel_endpoint, args),
                    name="cancel_method_thread"
                )
                cancel_method_thread.start()

                total_wait_time = 0
                while total_wait_time <= command_timeout_in_seconds:
                    response = client.databricks_api("get", const.STATUS_ENDPOINT, args=args)
                    status = response.get("status")
                    _LOGGER.info(f"Query execution status: {status}.")

                    if status in ("Canceled", "Cancelled", "Error"):
                        raise Exception(
                            f"Could not complete the query execution. Status: {status}."
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
                        _LOGGER.info(f"Total number of rows obtained in query's result: {count_of_result}")
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
                            f"Query execution in progress, will retry after {seconds_to_timeout} seconds."
                        )
                        time.sleep(seconds_to_timeout)
                        total_wait_time += seconds_to_timeout
                        continue

                    _LOGGER.info(
                        f"Query execution in progress, will retry after {const.COMMAND_SLEEP_INTERVAL_IN_SECONDS} seconds."
                    )
                    time.sleep(const.COMMAND_SLEEP_INTERVAL_IN_SECONDS)
                    total_wait_time += const.COMMAND_SLEEP_INTERVAL_IN_SECONDS
                else:
                    # Timeout scenario
                    _LOGGER.info(f"Command execution timed out. Last status: {status}.")
                    _LOGGER.info("Canceling the query execution")
                    resp_, status_code = client.databricks_api("post", const.CANCEL_QUERY_ENDPOINT_CLUSTER, data=args)
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

            def handle_dbsql_method(row_limit, thread_count):

                def ensure_warehouse_running(id_of_warehouse):
                    """Ensure warehouse is in RUNNING state, starting it if necessary.
                    
                    Handles all warehouse states:
                    - RUNNING: Returns immediately
                    - STARTING: Waits for startup to complete
                    - STOPPING: Waits for stop to complete, then starts
                    - STOPPED: Starts the warehouse and waits
                    - Other states (DELETED, DELETING, etc.): Raises an error
                    
                    Args:
                        id_of_warehouse: The warehouse ID to ensure is running
                    """
                    start_was_requested = False
                    
                    while True:
                        warehouse_resp = client.databricks_api(
                            "get",
                            const.SPECIFIC_WAREHOUSE_STATUS_ENDPOINT.format(id_of_warehouse)
                        )
                        current_state = warehouse_resp.get("state", "").lower()
                        
                        if current_state == "running":
                            if start_was_requested:
                                _LOGGER.info("Warehouse started successfully.")
                            else:
                                _LOGGER.info("Warehouse is already running.")
                            break
                        elif current_state == "starting":
                            _LOGGER.info("Warehouse is in STARTING state, waiting...")
                            time.sleep(3)
                        elif current_state == "stopping":
                            _LOGGER.info("Warehouse is in STOPPING state, waiting for it to stop...")
                            time.sleep(3)
                        elif current_state == "stopped":
                            if start_was_requested:
                                # After calling start API, warehouse may briefly still show as STOPPED
                                # before transitioning to STARTING. Wait and retry.
                                _LOGGER.info("Warehouse still in STOPPED state after start request, "
                                             "waiting for state transition...")
                                time.sleep(3)
                            else:
                                _LOGGER.info("Warehouse is in STOPPED state. Starting the warehouse.")
                                client.databricks_api(
                                    "post", const.WAREHOUSE_START_ENDPOINT.format(id_of_warehouse)
                                )
                                start_was_requested = True
                        else:
                            raise Exception(f"Warehouse cannot be started. Current SQL warehouse state is {warehouse_resp.get('state')}.")

                # Check whether SQL Warehouse exists. If yes, ensure it's running.
                warehouse_exist = False
                list_of_links = []
                list_of_chunk_number = []
                resp = client.databricks_api("get", const.WAREHOUSE_STATUS_ENDPOINT)
                response = resp.get("warehouses")
                for res in response:
                    if res.get("id") == self.warehouse_id:
                        warehouse_exist = True
                        if res.get("state").lower() != "running":
                            ensure_warehouse_running(self.warehouse_id)
                        break
                if not warehouse_exist:
                    raise Exception(f"No SQL warehouse found with ID: {self.warehouse_id}. Provide a valid SQL warehouse ID.")

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
                _LOGGER.info(f"Query submitted, statement id: {statement_id}.")

                cancel_endpoint = const.CANCEL_QUERY_ENDPOINT_DBSQL.format(statement_id=statement_id)

                # Check for Splunk search cancellation
                cancel_method_thread = threading.Thread(
                    target=self.cancel_query,
                    args=(search_sid, session_key, client, cancel_endpoint, None),
                    name="cancel_method_thread",
                )
                cancel_method_thread.start()

                # Pulling mechanism
                _LOGGER.info("Fetching query execution status.")
                status = None

                total_wait_time = 0
                while total_wait_time <= command_timeout_in_seconds:
                    response = client.databricks_api(
                        "get",
                        const.QUERY_STATUS_ENDPOINT.format(statement_id=statement_id)
                    )
                    status = response.get("status", {}).get("state")
                    _LOGGER.info(f"Query execution status: {status}.")

                    if status in ("CANCELED", "CLOSED", "FAILED"):
                        err_message = f"Could not complete the query execution. Status: {status}."
                        if status == "FAILED":
                            err_message += f" Error: {response['status'].get('error', {}).get('message')}"
                        raise Exception(err_message)

                    elif status == "SUCCEEDED":
                        _LOGGER.info("Query execution successful. Preparing data.")

                        if response["manifest"].get("truncated"):
                            _LOGGER.info("Result row limit exceeded, hence results are truncated.")
                            self.write_warning("Result limit exceeded, hence results are truncated.")

                        total_row_count = response["manifest"]["total_row_count"]
                        _LOGGER.info(f"Total number of rows obtained in query's result: {total_row_count}")
                        if int(total_row_count) == 0:
                            _LOGGER.info("Successfully executed databricksquery command.")
                            sys.exit(0)

                        # Prepare list of Headers
                        headers = response["manifest"]["schema"]["columns"]
                        schema = []
                        for header in headers:
                            field = header.get("name")
                            schema.append(field)

                        _LOGGER.info(f"Result table schema: {schema}")

                        # Method to fetch data of every chunk
                        def fetch_data_executor(args):
                            external_link, chunk_index = args
                            # Fetch Data
                            response = client.external_api("get", external_link)

                            _LOGGER.info(
                                f"Total number of rows obtained in chunk-{chunk_index} of query result: {len(response)}"
                            )
                            return response

                        def parse_data(response, schema):
                            for row in response:
                                yield dict(zip(schema, row))

                        # Get external link of first chunk
                        external_links = response["result"].get("external_links")
                        if not external_links:
                            raise Exception("No data returned from execution of this query.")
                        next_chunk_internal_link = external_links[0].get("next_chunk_internal_link")

                        list_of_links.append(external_links[0]["external_link"])
                        list_of_chunk_number.append(external_links[0]["chunk_index"])

                        while next_chunk_internal_link:
                            response = client.databricks_api("get", next_chunk_internal_link)
                            external_links = response["external_links"]
                            next_chunk_internal_link = external_links[0].get("next_chunk_internal_link")
                            list_of_links.append(external_links[0]["external_link"])
                            list_of_chunk_number.append(external_links[0]["chunk_index"])

                        combined_args = zip(list_of_links, list_of_chunk_number)
                        with ThreadPoolExecutor(max_workers=int(thread_count)) as executor:
                            results = executor.map(fetch_data_executor, combined_args)

                        for res in results:
                            yield from parse_data(res, schema)

                        _LOGGER.info("Data parsed successfully.")
                        break

                    # If statement execution is in ["RUNNING", "PENDING"] state
                    seconds_to_timeout = command_timeout_in_seconds - total_wait_time

                    if seconds_to_timeout < const.COMMAND_SLEEP_INTERVAL_IN_SECONDS:

                        if not seconds_to_timeout:
                            total_wait_time += 1
                            continue

                        _LOGGER.info(
                            f"Query execution in progress, will retry after {seconds_to_timeout} seconds."
                        )
                        time.sleep(seconds_to_timeout)
                        total_wait_time += seconds_to_timeout
                        continue

                    _LOGGER.info(
                        f"Query execution in progress, will retry after {const.COMMAND_SLEEP_INTERVAL_IN_SECONDS} seconds."
                    )
                    time.sleep(const.COMMAND_SLEEP_INTERVAL_IN_SECONDS)
                    total_wait_time += const.COMMAND_SLEEP_INTERVAL_IN_SECONDS
                else:
                    # Timeout scenario
                    _LOGGER.info(f"Command execution timed out. Last status: {status}.")
                    _LOGGER.info("Canceling the query execution")
                    resp_, status_code = client.databricks_api(
                        "post", const.CANCEL_QUERY_ENDPOINT_DBSQL.format(statement_id=statement_id)
                    )
                    if status_code == 200:
                        _LOGGER.info("Successfully canceled the query execution.")
                        self.write_error("Canceled the execution as command execution timed out")

                _LOGGER.info("Successfully executed databricksquery command.")

            if not self.cluster and not self.warehouse_id:
                dbquery_type = databricks_configs.get("config_for_dbquery")
                if dbquery_type == "dbsql":
                    handle_invalid_limit_value()
                    self.warehouse_id = databricks_configs.get("warehouse_id")
                    if not self.warehouse_id:
                        raise Exception(
                            "Databricks warehouse_id is required to execute this custom command. "
                            "Provide a warehouse_id parameter or configure the Warehouse ID "
                            "in the TA's configuration page."
                        )
                    row_limit = fetch_limit_value()
                    for event in handle_dbsql_method(row_limit, databricks_configs.get("thread_count")):
                        yield event
                elif (
                    dbquery_type == "interactive_cluster"
                    or (dbquery_type is None and databricks_configs.get("cluster_name"))
                ):
                    self.cluster = databricks_configs.get("cluster_name")
                    if not self.cluster:
                        raise Exception(
                            "Databricks cluster is required to execute this custom command. "
                            "Provide a cluster parameter or configure the cluster in the TA's configuration page."
                        )
                    for event in handle_cluster_method():
                        yield event
                else:
                    msg = (
                        "No configuration found for Cluster Name or Warehouse ID on the TA's configuration page. "
                        "Provide Cluster Name or Warehouse ID on TA's Configuration page or in Search."
                    )
                    raise Exception(msg)

            elif self.cluster and self.warehouse_id:
                _LOGGER.error("Provide only one of Cluster or Warehouse ID. Exiting the script.")
                raise Exception("Provide only one of Cluster or Warehouse ID")
            elif self.cluster and not self.warehouse_id:
                for event in handle_cluster_method():
                    yield event
            elif self.warehouse_id and not self.cluster:
                handle_invalid_limit_value()
                row_limit = fetch_limit_value()
                for event in handle_dbsql_method(row_limit, databricks_configs.get("thread_count")):
                    yield event

        except Exception as e:
            if str(e) == "Search Canceled!":
                _LOGGER.info("Query execution has been canceled!")
            else:
                _LOGGER.exception(e)
            self.write_error(str(e))


dispatch(DatabricksQueryCommand, sys.argv, sys.stdin, sys.stdout, __name__)
