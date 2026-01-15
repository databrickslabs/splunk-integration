import ta_databricks_declare  # noqa: F401

import traceback
import sys
import time

import splunklib.results as Results  # noqa
import splunk.Intersplunk

import databricks_com as com
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

APP_NAME = const.APP_NAME
_LOGGER = setup_logging("ta_databricksrunstatus_command")


if __name__ == "__main__":
    try:
        _LOGGER.info("Initiating databricksrunstatus command.")
        results, d_results, settings = splunk.Intersplunk.getOrganizedResults()
        session_key = str(settings.get('sessionKey'))
        if len(results) == 0:
            _LOGGER.info("No data found to update.")
            sys.exit(0)

        for each in results:
            try:
                to_ingest = False
                if each.get("param"):
                    each["param"] = each["param"].split('\n')
                run_id = each['run_id']
                account_name = each['account_name']
                index = each["index"]
                sourcetype = each["sourcetype"]
                if sourcetype == "databricks:databricksjob":
                    each.pop("identifier", None)
                each.pop("index", None)
                each.pop("sourcetype", None)
                APPEND_RUN_ID_IN_LOG = f"[UID: {each.get('uid', '-')}] Run ID: {run_id}."
                client_ = com.DatabricksClient(account_name, session_key)
                try:
                    run_id = int(run_id)
                except ValueError:
                    continue
                args = {"run_id": run_id}
                response = client_.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)
                if response and response["state"]["life_cycle_state"] == "RUNNING":
                    _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution is in Running state.")
                    if each["run_execution_status"] != "Running":
                        to_ingest = True
                        each["run_execution_status"] = "Running"

                elif response and response["state"]["life_cycle_state"] == "PENDING":
                    _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution is in Pending state.")
                    if each["run_execution_status"] != "Pending":
                        to_ingest = True
                        each["run_execution_status"] = "Pending"

                elif response and response["state"]["life_cycle_state"] == "TERMINATED":
                    if response["state"]["result_state"] == "SUCCESS":
                        _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution is successfully completed.")
                        each["run_execution_status"] = "Success"
                        to_ingest = True
                    elif response["state"]["result_state"] == "FAILED":
                        _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution is failed.")
                        each["run_execution_status"] = "Failed"
                        to_ingest = True
                    elif response["state"]["result_state"] == "CANCELED":
                        _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution is canceled.")
                        each["run_execution_status"] = "Canceled"
                        to_ingest = True
                else:
                    if response:
                        res_state = response["state"]["result_state"]
                        _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Execution status is {res_state}.")
                        if each["run_execution_status"] != res_state:
                            to_ingest = True
                            each["run_execution_status"] = res_state

                if to_ingest:
                    each["created_time"] = time.time()
                    utils.ingest_data_to_splunk(dict(each), session_key, index, sourcetype)
                    _LOGGER.info(f"{APPEND_RUN_ID_IN_LOG} Updated Execution details successfully ingested into Splunk.")
            except Exception:
                _LOGGER.error(f"{APPEND_RUN_ID_IN_LOG} Error: {traceback.format_exc()}")
                continue
        _LOGGER.info("Completed execution of databricksrunstatus command.")
    except Exception:
        _LOGGER.error(f"Error occurred in executing databricksrunstatus command. Error: {traceback.format_exc()}")
        sys.exit(0)
