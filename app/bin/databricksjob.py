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
    validators,
)

_LOGGER = setup_logging("ta_databricksjob_command")


@Configuration(type="events")
class DatabricksJobCommand(GeneratingCommand):
    """Custom Command of databricksjob."""

    # Take input from user using parameters
    job_id = Option(require=True, validate=validators.Integer(0))
    notebook_params = Option(require=False)

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksjob command")
        kv_log_info = {
            "user": self._metadata.searchinfo.username,
            "created_time": time.time(),
            "param": self._metadata.searchinfo.args,
            "run_id": "-",
            "output_url": "-",
            "result_url": "-",
            "command_status": "Failed",
            "error": "-",
        }

        session_key = self._metadata.searchinfo.session_key

        try:
            # Get job details
            client = com.DatabricksClient(session_key)

            payload = {
                "job_id": self.job_id,
            }

            _LOGGER.info("Fetching job details before submitting the execution.")
            response = client.databricks_api("get", const.GET_JOB_ENDPOINT, args=payload)

            job_settings = response["settings"]
            tasks_list = list(set(job_settings.keys()))

            if "notebook_task" not in tasks_list:
                raise Exception("Given job does not contains the notebook task. Hence terminating the execution.")
            if (
                "spark_jar_task" in tasks_list
                or "spark_python_task" in tasks_list
                or "spark_submit_task" in tasks_list
            ):
                raise Exception(
                    "Given job contains one of the following task in addition to the notebook task. "
                    "(spark_jar_task, spark_python_task and spark_submit_task) "
                    "Hence terminating the execution."
                )

            # Request for executing the job
            _LOGGER.info("Preparing request body for execution.")
            payload["notebook_params"] = utils.format_to_json_parameters(self.notebook_params)

            _LOGGER.info("Submitting job for execution.")
            response = client.databricks_api(
                "post", const.EXECUTE_JOB_ENDPOINT, data=payload
            )

            kv_log_info.update(response)
            run_id = response["run_id"]
            _LOGGER.info("Successfully executed the job with ID: {}.".format(self.job_id))

            # Request to get the run_id details
            _LOGGER.info("Fetching details for run ID: {}.".format(run_id))
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
                const.KV_COLLECTION_NAME_EXECUTE_JOB,
                session_key,
                kv_log_info,
            )

        yield updated_kv_info


dispatch(DatabricksJobCommand, sys.argv, sys.stdin, sys.stdout, __name__)
