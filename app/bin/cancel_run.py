"""This module contain class and method related to updating the finding state."""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..')))

import ta_databricks_declare  # noqa: F401, E402
import json  # noqa: E402
import databricks_com as com  # noqa: E402
from splunk.persistconn.application import PersistentServerConnectionApplication  # noqa: E402
import databricks_const as const  # noqa: E402
from log_manager import setup_logging  # noqa: E402

APP_NAME = const.APP_NAME
_LOGGER = setup_logging("ta_databricks_cancel_run")


class CancelRunningExecution(PersistentServerConnectionApplication):
    """Run Cancelation Handler."""

    def __init__(self, _command_line, _command_arg):
        """Initialize object with given parameters."""
        self.run_id = None
        self.account_name = None
        self.uid = None
        self.payload = {}
        self.status = None
        self.session_key = None
        super(PersistentServerConnectionApplication, self).__init__()

    # Handle a synchronous from splunkd.
    def handle(self, in_string):
        """
        After user clicks on Cancel Run button, Called for a simple synchronous request.

        @param in_string: request data passed in
        @rtype: string or dict
        @return: String to return in response.  If a dict was passed in,
                 it will automatically be JSON encoded before being returned.
        """
        try:
            req_data = json.loads(in_string)
            form_data = dict(req_data.get("form"))
            self.run_id = form_data.get("run_id")
            self.account_name = form_data.get("account_name")
            self.uid = form_data.get("uid")
            LOG_PREFIX = "[UID: {}] Run ID: {}.".format(self.uid, self.run_id)
            _LOGGER.info("{} Initiating cancelation request.".format(LOG_PREFIX))
            session = dict(req_data.get("session"))
            self.session_key = session.get("authtoken")
            client_ = com.DatabricksClient(self.account_name, self.session_key)
            payload = {
                "run_id": self.run_id,
            }
            try:
                resp, status_code = client_.databricks_api("post", const.CANCEL_JOB_RUN_ENDPOINT, data=payload)
                if status_code == 200:
                    _LOGGER.info("{} Successfully canceled.".format(LOG_PREFIX))
                    _LOGGER.info("{} An updated event with canceled execution status will be ingested in Splunk "
                                 "in few minutes.".format(LOG_PREFIX))
                    self.payload['canceled'] = "Success"
                    self.status = 200
                else:
                    _LOGGER.info("{} Unable to cancel. Response returned from API: {}. Status Code: {}"
                                 .format(LOG_PREFIX, resp, status_code))
                    self.payload['canceled'] = "Failed"
                    self.status = 500
            except Exception as e:
                _LOGGER.error("{} Error while canceling. Error: {}".format(LOG_PREFIX, str(e)))
                self.payload['canceled'] = "Failed"
                self.status = 500

        except Exception as err:
            _LOGGER.error("{} Error while canceling. Error: {}".format(LOG_PREFIX, str(err)))
            self.payload['canceled'] = "Failed"
            self.status = 500
        return {'payload': self.payload, 'status': self.status}

    def handleStream(self, handle, in_string):
        """For future use."""
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """Virtual method which can be optionally overridden to receive a callback after the request completes."""
        pass
