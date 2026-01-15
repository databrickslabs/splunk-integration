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
    validators,
)

APP_NAME = const.APP_NAME
UID = str(uuid.uuid4())
_LOGGER = setup_logging("ta_databricksjob_command", UID)


@Configuration(type="reporting")
class DatabricksJobCommand(GeneratingCommand):
    """Custom Command of databricksjob."""

    # Take input from user using parameters
    job_id = Option(require=True, validate=validators.Integer(0))
    account_name = Option(require=True)
    notebook_params = Option(require=False)

    def generate(self):
        """Generating custom command."""
        _LOGGER.info("Initiating databricksjob command.")
        _LOGGER.info(f"Job ID: {self.job_id}")
        _LOGGER.info(f"Notebook Params: {self.notebook_params}")

        info_to_process = create_command_info(
            self._metadata.searchinfo.username,
            self.account_name,
            self._metadata.searchinfo.args,
            UID
        )

        session_key = self._metadata.searchinfo.session_key
        provided_index = None

        try:
            databricks_configs = utils.get_databricks_configs(session_key, self.account_name)
            if not databricks_configs:
                raise Exception(
                    f"Account '{self.account_name}' not found. Please provide valid Databricks account."
                )
            provided_index = databricks_configs.get("index")

            # Get job details
            client = com.DatabricksClient(self.account_name, session_key)
            payload = {"job_id": self.job_id}

            _LOGGER.info("Fetching job details before submitting the execution.")
            response = client.databricks_api("get", const.GET_JOB_ENDPOINT, args=payload)

            job_settings = response["settings"]
            tasks_list = list(set(job_settings.keys()))

            if "notebook_task" not in tasks_list:
                raise Exception(
                    "Given job does not contains the notebook task. Hence terminating the execution."
                )
            if any(task in tasks_list for task in ["spark_jar_task", "spark_python_task", "spark_submit_task"]):
                raise Exception(
                    "Given job contains one of the following task in addition to the notebook task. "
                    "(spark_jar_task, spark_python_task and spark_submit_task) "
                    "Hence terminating the execution."
                )

            # Request for executing the job
            _LOGGER.info("Preparing request body for execution.")
            payload["notebook_params"] = utils.format_to_json_parameters(self.notebook_params)

            _LOGGER.info("Submitting job for execution.")
            response = client.databricks_api("post", const.EXECUTE_JOB_ENDPOINT, data=payload)

            # Update info with response from POST (contains run_id)
            info_to_process.update(response)
            run_id = response["run_id"]
            if run_id:
                _LOGGER.info(f"run ID returned: {run_id}")
            _LOGGER.info(f"Successfully executed the job with job ID: {self.job_id}.")

            # Request to get the run_id details
            _LOGGER.info(f"Fetching details for run ID: {run_id}.")
            args = {"run_id": run_id}
            response = client.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)

            output_url = response.get("run_page_url")
            update_info_on_success(info_to_process, response, output_url)
            if output_url:
                _LOGGER.info(f"Output url returned: {output_url}")

            _LOGGER.info("Successfully executed databricksjob command.")

        except Exception as e:
            handle_command_error(e, info_to_process, self, _LOGGER)
            exit(1)

        finally:
            if provided_index:
                ingest_command_data(
                    info_to_process, session_key, provided_index,
                    "databricks:databricksjob", _LOGGER
                )

        yield info_to_process


dispatch(DatabricksJobCommand, sys.argv, sys.stdin, sys.stdout, __name__)
