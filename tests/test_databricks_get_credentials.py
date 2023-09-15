import declare
import os
import sys
import unittest
from utility import Response
import json
import traceback
import base64
from importlib import import_module
from mock import patch, MagicMock


mocked_modules = {}


def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        "log_manager",
        "splunk",
        "splunk.persistconn.application",
        "splunk.rest"
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        if module == "splunk.persistconn.application":
            magicmock.PersistentServerConnectionApplication = object
        patch.dict("sys.modules", **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()


class TestDatabricksGetCredentials(unittest.TestCase):
    """Test Databricks Get Credentials."""

    def test_get_credentials_object(self):
        db_cm = import_module("databricks_get_credentials")
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        self.assertIsInstance(obj1, db_cm.DatabricksGetCredentials)

    def test_handle_save_access_token_success(self):
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token"
            }
        })
        
        # mock the CredentialManager to raise an exception when set_password is called
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.return_value = "Saved AAD access token successfully."
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Saved AAD access token successfully."
        assert result["status"] == 200

    def test_handle_save_access_token_failure(self):
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token"
            }
        })

        # mock the CredentialManager to raise an exception when set_password is called
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.side_effect = Exception("Failed to save AAD access token")
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Databricks Error: Exception while saving AAD access token: Failed to save AAD access token"
        assert result["status"] == 500

    @patch("databricks_get_credentials.rest.simpleRequest")
    def test_handle_retrieve_config_success(self, mock_request):
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": None,
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token"
            }
        })
        mock_request.return_value = (200, json.dumps({"entry":[{"content":{"auth_type":"PAT", "databricks_instance":"http", "cluster_name":"test"}},"test"]}))
        result = obj1.handle(input_string)
        db_cm._LOGGER.debug.assert_called_with("Account configurations read successfully from account.conf .")
       
    
    @patch("databricks_get_credentials.rest.simpleRequest")
    @patch("databricks_get_credentials.CredentialManager")
    def test_handle_retrieve_config_success_till_end(self, mock_credential_manager, mock_request):
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": None,
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token"
            }
        })
        mock_request.return_value = (200, json.dumps({"entry":[{"content":{"auth_type":"PAT", "databricks_instance":"http", "cluster_name":"test"}},"test"]}))
        mock_credential_manager.return_value.get_password.return_value = json.dumps({"aad_client_secret": "client_secret", "aad_access_token": "access_token"})
        result = obj1.handle(input_string)
        db_cm._LOGGER.debug.assert_called_with("Additional parameters configurations read successfully from settings.conf")
