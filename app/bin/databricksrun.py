import ta_databricks_declare  # noqa: F401
import sys
import time
import traceback

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

_LOGGER = setup_logging("ta_databricksrun_command")


@Configuration(type="events")
class DatabricksRunCommand(GeneratingCommand):
    """Custom Command of databricksrun."""

    # Take input from user using parameters
    notebook_path = Option(require=True)
    run_name = Option(require=False)
    cluster = Option(require=False)
    revision_timestamp = Option(require=False)
    notebook_params = Option(require=False)
    identifier = Option(require=False)

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksrun command")
        kv_log_info = {
            "user": self._metadata.searchinfo.username,
            "created_time": time.time(),
            "param": self._metadata.searchinfo.args,
            "run_id": "-",
            "output_url": "-",
            "result_url": "-",
            "command_status": "Failed",
            "error": "-",
            "identifier": "-"
        }
        if not (self.notebook_path and self.notebook_path.strip()):
            self.write_error("Please provide value for the parameter \"notebook_path\"")
            exit(1)
        if self.identifier and self.identifier.strip():
            kv_log_info["identifier"] = self.identifier.strip()

        session_key = self._metadata.searchinfo.session_key
        self.run_name = self.run_name or const.APP_NAME

        try:
            # Fetching cluster name
            self.cluster = (self.cluster and self.cluster.strip()) or utils.get_databricks_configs().get("cluster_name")
            if not self.cluster:
                raise Exception(
                    "Databricks cluster is required to execute this custom command. "
                    "Provide a cluster parameter or configure the cluster in the TA's configuration page."
                )

            client = com.DatabricksClient(session_key)

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
                notebook_task["base_parameters"] = utils.format_to_json_parameters(self.notebook_params)

            payload = {
                "run_name": self.run_name,
                "existing_cluster_id": cluster_id,
                "notebook_task": notebook_task,
            }

            _LOGGER.info("Submitting the run")
            response = client.databricks_api(
                "post", const.RUN_SUBMIT_ENDPOINT, data=payload
            )

            kv_log_info.update(response)
            run_id = response["run_id"]
            _LOGGER.info("Successfully submitted the run with ID: {}".format(run_id))

            # Request to get the run_id details
            _LOGGER.info("Fetching details for run ID: {}".format(run_id))
            args = {"run_id": run_id}
            response = client.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)

            output_url = response.get("run_page_url")
            if output_url:
                result_url = output_url.rstrip("/") + "/resultsOnly"
                kv_log_info["output_url"] = output_url
                kv_log_info["result_url"] = result_url
                kv_log_info["command_status"] = "Success"
                _LOGGER.info("Output url returned: {}".format(output_url))

        except Exception as e:
            _LOGGER.error(e)
            _LOGGER.error(traceback.format_exc())
            kv_log_info["error"] = str(e)
            self.write_error(str(e))
            exit(1)

        finally:
            updated_kv_info = utils.update_kv_store_collection(
                self._metadata.searchinfo.splunkd_uri,
                const.KV_COLLECTION_NAME_SUBMIT_RUN,
                session_key,
                kv_log_info,
            )

        yield updated_kv_info


dispatch(DatabricksRunCommand, sys.argv, sys.stdin, sys.stdout, __name__)
