## Minimal set of standard modules to import
import csv  ## Result set is in CSV format
import gzip  ## Result set is gzipped
import json  ## Payload comes in JSON format
import logging  ## For specifying log levels
import sys  ## For appending the library path

## Standard modules specific to this action
import requests  ## For making http based API calls
import urllib  ## For url encoding
import time  ## For rate limiting


import re, os
import traceback

## Importing the cim_actions.py library
## A.  Import make_splunkhome_path
## B.  Append your library path to sys.path
## C.  Import ModularAction from cim_actions
## D.  Import ModularActionTimer from cim_actions
from splunk.clilib.bundle_paths import make_splunkhome_path

sys.path.append(make_splunkhome_path(["etc", "apps", "Splunk_SA_CIM", "lib"]))
from cim_actions import ModularAction, ModularActionTimer

## Retrieve a logging instance from ModularAction
## It is required that this endswith _modalert
logger = ModularAction.setup_logger("databricks_modalert")

## Subclass ModularAction for purposes of implementing
## a script specific dowork() method
class NotebookModularAction(ModularAction):

    ## This method will initialize NotebookModularAction
    def __init__(self, settings, logger, action_name=None):
        ## Call ModularAction.__init__
        super(NotebookModularAction, self).__init__(settings, logger, action_name)
        ## Initialize param.limit
        try:
            self.limit = int(self.configuration.get("limit", 1))
            if self.limit < 1 or self.limit > 30:
                self.limit = 30
        except (ValueError, TypeError):
            self.limit = 1

    ## This method will handle validation
    def validate(self, result):
        ## outer validation
        pass
        # if len(self.rids)<=1:
        #     ## Validate param.url
        #     if not self.configuration.get('url'):
        #         raise Exception('Invalid URL requested')
        #     ## Validate param.service
        #     if (self.configuration.get('service', '')
        #        not in NotebookModularAction.VALID_SERVICES):
        #         raise Exception('Invalid service requested')
        #     ## Validate param.parameter_field
        #     if self.configuration.get('parameter_field', '') not in result:
        #         raise Exception('Parameter field does not exist in result')

    ## This method will do the actual work itself

    def dowork(self, result):
        # ## get parameter value
        # parameter  = result[self.configuration.get('parameter_field')]
        # ## get service
        # service    = self.configuration.get('service', '')
        ## build sourcetype
        sourcetype = "databricks:notebook"
        self.message("Successfully started Databricks Notebook Action", status="success")
        # self.message(f'Settings: {self.settings}', status='success')
        # self.message(f'Configuration: {self.configuration}', status='success')

        paramOne = self.configuration.get("paramone")
        paramTwo = None
        try:
            paramTwo = self.configuration.get("paramtwo")
        except (KeyError, AttributeError):
            pass
        notebook = self.configuration.get("notebook")
        params = {}
        if paramOne in result:
            params[paramOne] = result[paramOne]
        if paramTwo and paramTwo != "" and paramTwo in result:
            params[paramTwo] = result[paramTwo]

        rid = ""
        try:
            if "orig_rid" in result:
                rid = result["orig_rid"]
            elif "rid" in result:
                rid = result["rid"]
            elif self.settings.get("rid"):
                rid = self.settings.get("rid")
            elif self.settings.get("orig_rid"):
                rid = self.settings.get("orig_rid")
        except (KeyError, AttributeError):
            pass

        sid = ""
        try:
            if "orig_sid" in result:
                sid = result["orig_sid"]
            elif "sid" in result:
                sid = result["sid"]
            elif self.settings.get("sid"):
                sid = self.settings.get("sid")
            elif self.settings.get("orig_sid"):
                sid = self.settings.get("orig_sid")
        except (KeyError, AttributeError):
            pass

        try:
            cluster_id = com.get_cluster_id(self.cluster_name)
            self.message(
                f"Cluster ID received: {cluster_id}", status="working"
            )  # , level=logging.INFO)

            # Request to submit the run
            self.message(
                "Preparing request body for execution", status="working"
            )  # , level=logging.INFO)
            notebook_task = {"notebook_path": notebook}
            notebook_task["base_parameters"] = params

            payload = {
                # "run_name": self.run_name,
                "existing_cluster_id": cluster_id,
                "notebook_task": notebook_task,
            }

            self.message("Submitting the run", status="working")  # , level=logging.INFO)
            response = com.databricks_api("post", const.RUN_SUBMIT_ENDPOINT, data=payload)

            # info_to_process.update(response)
            run_id = response["run_id"]
            self.message(
                f"Successfully submitted the run with ID: {run_id}"
            )  # , status="working", level=logging.INFO)

            # Request to get the run_id details
            args = {"run_id": run_id}
            response = com.databricks_api("get", const.GET_RUN_ENDPOINT, args=args)
            result_url = ""
            output_url = response.get("run_page_url")
            if output_url:
                result_url = output_url.rstrip("/") + "/resultsOnly"
            self.message(
                f'Start result_url="{result_url}" output_url="{output_url}" End', status="success"
            )
            if sid != "" and rid != "":
                self.addevent(
                    json.dumps(
                        {
                            "_time": time.time(),
                            "sid": sid,
                            "rid": rid,
                            "result_url": result_url,
                            "output_url": output_url,
                            "response": response,
                            "request_params": params,
                            "databricks_instance": self.databricks_instance,
                            "cluster_name": self.cluster_name,
                            "notebook": notebook,
                        }
                    ),
                    sourcetype=sourcetype,
                )
        except Exception as e:
            modaction.message(
                f"Failure during job submission: {traceback.format_exc()}",
                status="failure",
                level=logging.CRITICAL,
            )


if __name__ == "__main__":
    ## This is standard chrome for validating that
    ## the script is being executed by splunkd accordingly
    if len(sys.argv) < 2 or sys.argv[1] != "--execute":
        print("FATAL Unsupported execution mode (expected --execute flag)", file=sys.stderr)
        sys.exit(1)

    ## The entire execution is wrapped in an outer try/except
    try:
        ## Retrieve an instanced of NotebookModularAction and name it modaction
        ## pass the payload (sys.stdin) and logging instance
        stdindata = sys.stdin.read()
        modaction = NotebookModularAction(stdindata, logger, "notebook")
        logger.debug(modaction.settings)
        modaction.message(
            "About to start trying to import the Databricks code",
            status="starting",
            level=logging.CRITICAL,
        )
        try:
            import databricks_com as com
            import databricks_const as const
            import databricks_common_utils as utils
        except Exception as e:
            modaction.message(
                f"Failure on importing Databricks libs: {traceback.format_exc()}",
                status="failure",
                level=logging.CRITICAL,
            )

        modaction.session_key = json.loads(stdindata)["session_key"]
        modaction.account_name = json.loads(stdindata).get("configuration").get("account_name")
        com = com.DatabricksClient(modaction.account_name, modaction.session_key)
        try:
            databricks_config = utils.get_databricks_configs(
                modaction.session_key, modaction.account_name
            )
            modaction.cluster_name = databricks_config.get("cluster_name")
            modaction.databricks_instance = databricks_config.get("databricks_instance")
        except Exception as e:
            modaction.message(
                f"Failure getting cluster name config: {traceback.format_exc()}",
                status="failure",
                level=logging.CRITICAL,
            )

        ## Add a duration message for the "main" component using modaction.start_timer as
        ## the start time
        with ModularActionTimer(modaction, "main", modaction.start_timer):
            ## Process the result set by opening results_file with gzip
            with gzip.open(modaction.results_file, "rt") as fh:
                ## Iterate the result set using a dictionary reader
                ## We also use enumerate which provides "num" which
                ## can be used as the result ID (rid)
                modaction.message(
                    f"Got a file: {modaction.results_file}",
                    status="working",
                    level=logging.CRITICAL,
                )
                for num, result in enumerate(csv.DictReader(fh)):
                    ## results limiting
                    if num >= modaction.limit:
                        break
                    ## Set rid to row # (0->n) if unset
                    result.setdefault("rid", str(num))
                    ## Update the ModularAction instance
                    ## with the current result.  This sets
                    ## orig_sid/rid/orig_rid accordingly.
                    modaction.update(result)
                    ## Generate an invocation message for each result.
                    ## Tells splunkd that we are about to perform the action
                    ## on said result.
                    modaction.invoke()
                    ## Validate the invocation
                    modaction.validate(result)
                    ## This is where we do the actual work.  In this case
                    ## we are calling out to an external API and creating
                    ## events based on the information returned
                    modaction.dowork(result)
                    ## rate limiting
                    time.sleep(1.6)

            ## Once we're done iterating the result set and making
            ## the appropriate API calls we will write out the events
            modaction.writeevents(index="cim_modactions", source="databricks:modalert")

    ## This is standard chrome for outer exception handling
    except Exception as e:
        ## adding additional logging since adhoc search invocations do not write to stderr
        try:
            modaction.message(traceback.format_exc(), status="failure", level=logging.CRITICAL)
        except NameError:
            logger.critical(e)
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(3)
