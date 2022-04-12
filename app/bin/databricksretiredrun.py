import ta_databricks_declare  # noqa: F401

import sys
import traceback
import json

import databricks_const as const

from datetime import datetime, timedelta
from log_manager import setup_logging
from splunklib.client import connect
from solnlib.splunkenv import get_splunkd_access_info
from splunklib.searchcommands import (
    dispatch,
    GeneratingCommand,
    Configuration,
    Option,
    validators,
)

_LOGGER = setup_logging("ta_databricksretiredrun_command")


@Configuration()
class DatabricksRetiredRunCommand(GeneratingCommand):
    """Custom Command of databricksretiredrun."""

    days = Option(
        name='days', require=False, validate=validators.Integer(minimum=1)
    )

    run_id = Option(
        name='run_id', require=False
    )

    user = Option(
        name='user', require=False
    )

    def generate(self):
        """Generate method of Generating Command."""
        try:
            _LOGGER.info("Initiating databricksretiredrun command.")
            if False:
                yield
            current_time = datetime.utcnow()
            if not any((self.days, self.run_id, self.user)):
                msg = "No parameters provided. Please provide at least one of the parameters"
                self.write_error(msg)
                _LOGGER.error(msg)
                exit(1)

            conditions_list = []

            if self.days:
                lastcreatedtime = (
                    current_time
                    - timedelta(
                        days=self.days,
                        hours=current_time.hour,
                        minutes=current_time.minute,
                        seconds=current_time.second,
                        microseconds=current_time.microsecond,
                    )
                ).timestamp()
                lastcreated_dict = {"created_time": {"$lt": lastcreatedtime}}
                conditions_list.append(lastcreated_dict)

            if self.run_id and self.run_id.strip():
                run_id_dict = {"run_id": self.run_id.strip()}
                conditions_list.append(run_id_dict)

            if self.user and self.user.strip():
                user_dict = {"user": self.user.strip()}
                conditions_list.append(user_dict)

            endpoint_query = {"$and": conditions_list}
            _, host, mgmt_port = get_splunkd_access_info()
            session_key = self.search_results_info.auth_token
            service = connect(app=const.APP_NAME, owner="nobody",
                              port=mgmt_port, token=session_key)
            if const.KV_COLLECTION_NAME_SUBMIT_RUN not in self.service.kvstore:
                msg = "Could not find collection {}.".format(const.KV_COLLECTION_NAME_SUBMIT_RUN)
                self.write_error(msg)
                raise Exception(msg)
            collection = service.kvstore[const.KV_COLLECTION_NAME_SUBMIT_RUN]
            _LOGGER.info("Deleting retired run details from databricks_submit_run_log...")
            # Responsible to delete data from the databricks_submit_run_log lookup
            query = json.dumps(endpoint_query)
            collection.data.delete(query)

        except Exception as e:
            _LOGGER.error(e)
            _LOGGER.error(traceback.format_exc())
        _LOGGER.info("Time taken - {} seconds.".format((datetime.utcnow() - current_time).total_seconds()))
        _LOGGER.info("Completed the execution of databricksretiredrun command")

    def __init__(self):
        """Initialize custom command class."""
        super(DatabricksRetiredRunCommand, self).__init__()


dispatch(DatabricksRetiredRunCommand, sys.argv, sys.stdin, sys.stdout, __name__)
