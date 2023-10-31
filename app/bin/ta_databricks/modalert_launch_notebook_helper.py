
# encoding = utf-8
from solnlib.splunkenv import get_splunkd_access_info
from splunklib import client
from databricks_const import APP_NAME

import time
import traceback
import logging


def process_event(helper, *args, **kwargs):
    """Process events."""
    # Using message() instead of log_info() to prevent the status of Adaptive action to be set
    # before completion of Adaptive Response.
    helper.message("Alert action launch_notebook started.", level=logging.INFO)
    start_time = time.time()
    notebook_path = helper.get_param("notebook_path")
    revision_timestamp = helper.get_param("revision_timestamp")
    notebook_parameters = helper.get_param("notebook_parameters")
    cluster_name = helper.get_param("cluster_name")
    account_name = helper.get_param("account_name")
    run_name = helper.get_param("run_name")

    if not (notebook_path and notebook_path.strip()):
        helper.log_error("Notebook path is a required parameter which is not provided.")
        exit(1)
    if not (account_name):
        helper.log_error("Databricks Account is a required parameter which is not provided.")
        exit(1)
    search_string = "|databricksrun account_name=\"" + account_name + "\"  notebook_path=\"" + \
        notebook_path.strip() + "\""
    if revision_timestamp:
        search_string = search_string + " revision_timestamp=\"" + revision_timestamp.strip() + "\""
    if notebook_parameters:
        search_string = search_string + " notebook_params=\"" + notebook_parameters.strip() + "\""
    if cluster_name:
        search_string = search_string + " cluster=\"" + cluster_name.strip() + "\""
    if run_name:
        search_string = search_string + " run_name=\"" + run_name.strip() + "\""
    try:
        if helper.action_mode == "adhoc":
            sid = helper.orig_sid
            rid = helper.orig_rid
        else:
            sid = helper.sid
            rid = helper.settings.get("rid")
        identifier = "{}:{}".format(rid, sid)
        search_string = search_string + " identifier=\"" + identifier + "\""
        # Using message() instead of log_info() to prevent the status of Adaptive action to be set
        # before completion of Adaptive Response.
        helper.message("Search query: {}".format(search_string), level=logging.INFO)
        _, host, mgmt_port = get_splunkd_access_info()
        service = client.connect(host=host,
                                 port=mgmt_port,
                                 app=APP_NAME,
                                 token=helper.session_key)
        search_kwargs = {"exec_mode": "blocking"}
        search_job = service.jobs.create(search_string, **search_kwargs)  # noqa F841

    except Exception as e:
        helper.log_error(e)
        helper.log_error("Error occured for launch_notebook alert action. {}".format(traceback.format_exc()))
        exit(1)

    helper.log_info("Exiting alert action. Time taken: {} seconds.".format(time.time() - start_time))
    return 0
