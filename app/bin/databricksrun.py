import ta_databricks_declare  # noqa: F401
import sys
import time
import uuid

import databricks_com as com
import databricks_const as const
import databricks_common_utils as utils
from databricks_command_base import (
    create_command_info,
    update_info_on_success,
    ingest_command_data,
    handle_command_error,
)
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
        _LOGGER.info(f"Notebook Path: {self.notebook_path or None}")
        _LOGGER.info(f"Notebook Revision Timestamp: {self.revision_timestamp or None}")
        _LOGGER.info(f"Run Name: {self.run_name or None}")
        _LOGGER.info(f"Cluster: {self.cluster or None}")
        _LOGGER.info(f"Notebook Params: {self.notebook_params or None}")
        _LOGGER.info(f"Identifier: {self.identifier or None}")

        info_to_process = create_command_info(
            self._metadata.searchinfo.username,
            self.account_name,
            self._metadata.searchinfo.args,
            UID,
            extra_fields={"identifier": "-"}
        )

        if not (self.notebook_path and self.notebook_path.strip()):
            self.write_error('Please provide value for the parameter "notebook_path"')
            exit(1)
        if self.identifier and self.identifier.strip():
            info_to_process["identifier"] = self.identifier.strip()

        session_key = self._metadata.searchinfo.session_key
        self.run_name = self.run_name or const.APP_NAME
        provided_index = None

        try:
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                raise Exception(
                    f"Account '{self.account_name}' not found. Please provide valid Databricks account."
                )
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
            _LOGGER.info(f"Requesting cluster ID for cluster: {self.cluster}")
            cluster_id = client.get_cluster_id(self.cluster)
            _LOGGER.info(f"Cluster ID received: {cluster_id}")

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

            # Update info with response from POST (contains run_id)
            info_to_process.update(response)
            run_id = response["run_id"]
            if run_id:
                _LOGGER.info(f"run ID returned: {run_id}")
            _LOGGER.info(f"Successfully submitted the run with ID: {run_id}")

            # Request to get the run_id details
            _LOGGER.info(f"Fetching details for run ID: {run_id}")
            args = {"run_id": run_id}
            response = client.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)

            output_url = response.get("run_page_url")
            update_info_on_success(info_to_process, response, output_url)
            if output_url:
                _LOGGER.info(f"Output url returned: {output_url}")

            _LOGGER.info("Successfully executed databricksrun command.")

        except Exception as e:
            handle_command_error(e, info_to_process, self, _LOGGER)
            exit(1)

        finally:
            if provided_index:
                ingest_command_data(
                    info_to_process, session_key, provided_index,
                    "databricks:databricksrun", _LOGGER
                )

        yield info_to_process


dispatch(DatabricksRunCommand, sys.argv, sys.stdin, sys.stdout, __name__)
