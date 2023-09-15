import declare
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import json
from importlib import import_module

mocked_modules = {}

def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        "log_manager",
        "splunk",
        "splunk.persistconn.application",
        "splunk.rest",
        "splunk.admin",
        "splunk.clilib",
        "splunk.clilib.cli_common"
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        if module == "splunk.persistconn.application":
            magicmock.PersistentServerConnectionApplication = object
        patch.dict("sys.modules", **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestCancelRunningExecution(unittest.TestCase):
    """Test Cancel Running Execution."""

    def test_handle_cancel_run_success(self):
        input_data = {
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        d_com = import_module("databricks_com")
        c_r = import_module("cancel_run")
        d_com._LOGGER = MagicMock()
        client_instance = MagicMock(spec=d_com.DatabricksClient)
        client_instance.databricks_api.return_value = ("Response", 200)
        with patch.object(d_com, 'DatabricksClient', return_value=client_instance):

            cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
            result = cancel_run_obj.handle(input_string)
            expected_result = {
                'payload': {'canceled': 'Success'},
                'status': 200
            }
            self.assertEqual(result, expected_result)

    def test_handle_cancel_run_failure(self):
        input_data = {
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        d_com = import_module("databricks_com")
        d_com._LOGGER = MagicMock()
        c_r = import_module("cancel_run")
        c_r._LOGGER = MagicMock()
        client_instance = MagicMock(spec=d_com.DatabricksClient)
        client_instance.databricks_api.side_effect = Exception("Error while canceling")
        with patch.object(d_com, 'DatabricksClient', return_value=client_instance):
            cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
            result = cancel_run_obj.handle(input_string)
            c_r._LOGGER.error.assert_called_with("[UID: test_uid] Run ID: 12345. Error while canceling. Error: Error while canceling")

            expected_result = {
                'payload': {'canceled': 'Failed'},
                'status': 500
            }
            self.assertEqual(result, expected_result)
    
    def test_handle_cancel_run_non_200_response(self):
        input_data = {
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        d_com = import_module("databricks_com")
        d_com._LOGGER = MagicMock()
        c_r = import_module("cancel_run")
        c_r._LOGGER = MagicMock()
        client_instance = MagicMock(spec=d_com.DatabricksClient)
        client_instance.databricks_api.return_value = ("Response", 404)
        with patch.object(d_com, 'DatabricksClient', return_value=client_instance):
            cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
            result = cancel_run_obj.handle(input_string)

        c_r._LOGGER.info.assert_called_with("[UID: test_uid] Run ID: 12345. Unable to cancel. Response returned from API: Response. Status Code: 404")

        expected_result = {
            'payload': {'canceled': 'Failed'},
            'status': 500
        }
        self.assertEqual(result, expected_result)

    def test_handle_cancel_run_exception(self):
        input_data = {
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        d_com = import_module("databricks_com")
        d_com._LOGGER = MagicMock()
        c_r = import_module("cancel_run")
        c_r._LOGGER = MagicMock()
        client_instance = MagicMock(spec=d_com.DatabricksClient)
        client_instance.databricks_api.side_effect = Exception("Error while canceling")
        with patch.object(d_com, 'DatabricksClient', return_value=client_instance):
            cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
            result = cancel_run_obj.handle(input_string)

        c_r._LOGGER.error.assert_called_with("[UID: test_uid] Run ID: 12345. Error while canceling. Error: Error while canceling")

        expected_result = {
            'payload': {'canceled': 'Failed'},
            'status': 500
        }
        self.assertEqual(result, expected_result)
    
    def test_handle_cancel_run_missing_required_field(self):
        # Missing run_id in form data
        input_data = {
            "form": {
                "account_name": "test_account",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)
        c_r = import_module("cancel_run")
        c_r._LOGGER = MagicMock()
        cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
        result = cancel_run_obj.handle(input_string)

        expected_result = {
            'payload': {'canceled': 'Failed'},
            'status': 500
        }
        self.assertEqual(result, expected_result)

        # Missing account_name in form data
        input_data = {
            "form": {
                "run_id": "12345",
                "uid": "test_uid"
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
        result = cancel_run_obj.handle(input_string)

        expected_result = {
            'payload': {'canceled': 'Failed'},
            'status': 500
        }
        self.assertEqual(result, expected_result)

        # Missing uid in form data
        input_data = {
            "form": {
                "run_id": "12345",
                "account_name": "test_account",
            },
            "session": {
                "authtoken": "dummy_token"
            }
        }
        input_string = json.dumps(input_data)

        cancel_run_obj = c_r.CancelRunningExecution("command_line", "command_args")
        result = cancel_run_obj.handle(input_string)

        expected_result = {
            'payload': {'canceled': 'Failed'},
            'status': 500
        }
        self.assertEqual(result, expected_result)
