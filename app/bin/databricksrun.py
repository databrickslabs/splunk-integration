import ta_databricks_declare  # noqa: F401
import sys
import time
import traceback
import json
import uuid

import databricks_com as com
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunklib.searchcommands import (
    dispatch,
    GeneratingCommand,
    Configuration,
    Option,
)

APP_NAME = const.APP_NAME
UID = str(uuid.uuid4())
_LOGGER = setup_logging("ta_databricksrun_command", UID)


@Configuration(type="reporting")
class DatabricksRunCommand(GeneratingCommand):
    """Custom Command of databricksrun."""

    # Take input from user using parameters
    notebook_path = Option(require=True)
    run_name = Option(require=False)
    account_name = Option(require=True)
    cluster = Option(require=False)
    revision_timestamp = Option(require=False)
    notebook_params = Option(require=False)
    identifier = Option(require=False)

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksrun command.")
        _LOGGER.info("Notebook Path: {}".format(self.notebook_path if self.notebook_path else None))
        _LOGGER.info("Notebook Revision Timestamp: {}"
                     .format(self.revision_timestamp if self.revision_timestamp else None))
        _LOGGER.info("Run Name: {}".format(self.run_name if self.run_name else None))
        _LOGGER.info("Cluster: {}".format(self.cluster if self.cluster else None))
        _LOGGER.info("Notebook Params: {}".format(self.notebook_params if self.notebook_params else None))
        _LOGGER.info("Identifier: {}".format(self.identifier if self.identifier else None))

        info_to_process = {
            "user": self._metadata.searchinfo.username,
            "account_name": self.account_name,
            "created_time": time.time(),
            "param": self._metadata.searchinfo.args,
            "run_id": "-",
            "run_execution_status": "-",
            "output_url": "-",
            "result_url": "-",
            "command_submission_status": "Failed",
            "error": "-",
            "identifier": "-",
            "uid": UID
        }
        if not (self.notebook_path and self.notebook_path.strip()):
            self.write_error('Please provide value for the parameter "notebook_path"')
            exit(1)
        if self.identifier and self.identifier.strip():
            info_to_process["identifier"] = self.identifier.strip()

        session_key = self._metadata.searchinfo.session_key
        self.run_name = self.run_name or const.APP_NAME

        try:
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                ERR_MSG = \
                    "Account '{}' not found. Please provide valid Databricks account.".format(self.account_name)
                raise Exception(ERR_MSG)
            provided_index = databricks_configs.get("index")
            # Fetching cluster name
            self.cluster = (self.cluster and self.cluster.strip()) or databricks_configs.get("cluster_name")
            if not self.cluster:
                raise Exception(
                    "Databricks cluster is required to execute this custom command. "
                    "Provide a cluster parameter or configure the cluster in the TA's configuration page."
                )

            client = com.DatabricksClient(self.account_name, session_key)

            # Request to get cluster ID
            _LOGGER.info("Requesting cluster ID for cluster: {}".format(self.cluster))
            cluster_id = client.get_cluster_id(self.cluster)
            _LOGGER.info("Cluster ID received: {}".format(cluster_id))

            # Request to submit the run
            _LOGGER.info("Preparing request body for execution")
            notebook_task = {"notebook_path": self.notebook_path}
            if self.revision_timestamp and self.revision_timestamp.strip():
                notebook_task["revision_timestamp"] = self.revision_timestamp
            if self.notebook_params and self.notebook_params.strip():
                notebook_task["base_parameters"] = utils.format_to_json_parameters(
                    self.notebook_params
                )

            payload = {
                "run_name": self.run_name,
                "existing_cluster_id": cluster_id,
                "notebook_task": notebook_task,
            }

            _LOGGER.info("Submitting the run")
            response = client.databricks_api("post", const.RUN_SUBMIT_ENDPOINT, data=payload)

            info_to_process.update(response)
            run_id = response["run_id"]
            if run_id:
                _LOGGER.info("run ID returned: {}".format(run_id))
            _LOGGER.info("Successfully submitted the run with ID: {}".format(run_id))

            # Request to get the run_id details
            _LOGGER.info("Fetching details for run ID: {}".format(run_id))
            args = {"run_id": run_id}
            response = client.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)

            output_url = response.get("run_page_url")
            if output_url:
                result_url = output_url.rstrip("/") + "/resultsOnly"
                info_to_process["output_url"] = output_url
                info_to_process["result_url"] = result_url
                info_to_process["command_submission_status"] = "Success"
                info_to_process["run_execution_status"] = "Initiated"
                _LOGGER.info("Output url returned: {}".format(output_url))

            _LOGGER.info("Successfully executed databricksrun command.")

        except Exception as e:
            _LOGGER.error(e)
            _LOGGER.error(traceback.format_exc())
            info_to_process["error"] = str(e)
            self.write_error(str(e))
            exit(1)

        finally:
            try:
                _LOGGER.info("Ingesting the data into Splunk index: {}".format(provided_index))
                indented_json = json.dumps(info_to_process, indent=4)
                _LOGGER.info("Data to be ingested in Splunk:\n{}".format(indented_json))
                utils.ingest_data_to_splunk(
                    info_to_process, session_key, provided_index, "databricks:databricksrun"
                )
                _LOGGER.info("Successfully ingested the data into Splunk index: {}.".format(provided_index))
            except Exception:
                _LOGGER.error("Error occured while ingesting data into Splunk. Error: {}"
                              .format(traceback.format_exc()))
        yield info_to_process


dispatch(DatabricksRunCommand, sys.argv, sys.stdin, sys.stdout, __name__)
