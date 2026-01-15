
# encoding = utf-8
# Always put this line at the beginning of this file
import ta_databricks_declare

import os
import sys
import traceback

from alert_actions_base import ModularAlertBase
import modalert_launch_notebook_helper


class AlertActionWorkerlaunch_notebook(ModularAlertBase):

    def __init__(self, ta_name, alert_name):
        super(AlertActionWorkerlaunch_notebook, self).__init__(ta_name, alert_name)

    def validate_params(self):

        if not self.get_param("notebook_path"):
            self.log_error('notebook_path is a mandatory parameter, but its value is None.')
            return False
        return True

    def process_event(self, *args, **kwargs):
        status = 0
        try:
            if not self.validate_params():
                return 3
            status = modalert_launch_notebook_helper.process_event(self, *args, **kwargs)
        except (AttributeError, TypeError) as ae:
            self.log_error(f"Error: {ae}. Please double check spelling and also verify that a compatible version of Splunk_SA_CIM is installed.")
            return 4
        except Exception as e:
            if e:
                self.log_error(f"Unexpected error: {e}.")
            else:
                self.log_error(f"Unexpected error: {traceback.format_exc()}.")
            return 5
        return status


if __name__ == "__main__":
    exitcode = AlertActionWorkerlaunch_notebook("TA-Databricks", "launch_notebook").run(sys.argv)
    sys.exit(exitcode)
