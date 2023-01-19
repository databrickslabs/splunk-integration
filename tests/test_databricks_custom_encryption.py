import declare
import os
import sys
import requests
import unittest
import json
import traceback
import base64
import random
import string
from utility import Response
from importlib import import_module
from mock import patch, MagicMock

mocked_modules = {}


def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        "log_manager",
        "splunk",
        "splunk.persistconn.application",
        "splunk.rest",
        "Crypto.Cipher"
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}
    for module, magicmock in mocked_modules.items():
        if module == "splunk.persistconn.application":
            magicmock.PersistentServerConnectionApplication = object
        patch.dict("sys.modules", **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()


class TestDatabricksCustomEncryption(unittest.TestCase):
    """Test Databricks Custom Encryption."""

    def test_get_custom_encryption_object(self):
        db_cm = import_module("databricks_custom_encryption")
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        self.assertIsInstance(obj1, db_cm.DatabricksCustomEncryption)

    @patch("databricks_custom_encryption.DatabricksCustomEncryption.databricks_configuration_encrypt")
    def test_handle_pat(self, mock_encrypt):
        db_cm = import_module("databricks_custom_encryption")
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "auth_type", "PAT" ], [ "client_secret", "" ], [ "databricks_access_token", "access_token" ], [ "databricks_instance", "databricks_instance" ], [ "name", "PAT_account2" ], [ "edit", "edit called" ] ] }'
        response = {
            "payload": {
                "success": "Databricks Configuration Custom Encryption completed successfully."
            },
            "status": 200
        }
        self.assertEqual(response, obj1.handle(in_string))
        
    @patch("databricks_custom_encryption.DatabricksCustomEncryption.databricks_configuration_encrypt")
    def test_handle_pat_error(self, mock_encrypt):
        db_cm = import_module("databricks_custom_encryption")
        
        err_message = "error message"
        mock_encrypt.side_effect = Exception(err_message)
        
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "auth_type", "PAT" ], [ "client_secret", "" ], [ "databricks_access_token", "access_token" ], [ "databricks_instance", "databricks_instance" ], [ "name", "" ], [ "edit", "edit called" ] ] }'
        
        response = {
            "payload": {
                "error": "Databricks Error : Error occured while performing custom encryption - {}".format(err_message)
            },
            "status": 500
        }
        self.assertEqual(response, obj1.handle(in_string))
        
    @patch("databricks_custom_encryption.DatabricksCustomEncryption.databricks_configuration_encrypt")
    def test_handle_aad(self, mock_encrypt):
        db_cm = import_module("databricks_custom_encryption")
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "auth_type", "AAD" ], [ "client_secret", "client_secret" ], [ "access_token", "access_token" ], [ "databricks_instance", "databricks_instance" ], [ "name", "PAT_account2" ], [ "edit", "edit called" ] ] }'
        response = {
            "payload": {
                "success": "Databricks Configuration Custom Encryption completed successfully."
            },
            "status": 200
        }
        self.assertEqual(response, obj1.handle(in_string))
    
    @patch("databricks_custom_encryption.DatabricksCustomEncryption.databricks_proxy_encrypt")
    def test_handle_proxy(self, mock_encrypt):
        db_cm = import_module("databricks_custom_encryption")
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "proxy_password", "proxy_password" ] ] }'
        response = {
            "payload": {
                "success": "Databricks Proxy Custom Encryption completed successfully."
            },
            "status": 200
        }
        self.assertEqual(response, obj1.handle(in_string))
        
        
    @patch("databricks_custom_encryption.DatabricksCustomEncryption.databricks_proxy_encrypt")
    def test_handle_proxy_error(self, mock_encrypt):
        db_cm = import_module("databricks_custom_encryption")
        err_message = "error message"
        mock_encrypt.side_effect = Exception(err_message)
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "proxy_password", "proxy_password" ] ] }'
        response = {
            "payload": {
                "error": "Databricks Error : Error occured while performing custom encryption - {}".format(err_message)
            },
            "status": 500
        }
        self.assertEqual(response, obj1.handle(in_string))

    @patch("databricks_custom_encryption.AES.new", return_value=MagicMock())
    @patch("databricks_custom_encryption.AES.encrypt")
    @patch("splunk.rest.simpleRequest")
    @patch("base64.b64encode")
    def test_databricks_configuration_encrypt(self, mock_base, mock_rest, mock_aes_encrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_encryption")
        db_cm._LOGGER = MagicMock()
        mock_aes_encrypt.return_value ="encrypted_access_token"
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        obj1.auth_type = "PAT"
        obj1.databricks_pat = "access_token"
        obj1.databricks_configuration_encrypt()
        db_cm._LOGGER.debug.assert_called_with("Databricks Configuration Custom Encryption completed successfully.")
        
    @patch("databricks_custom_encryption.AES.new", return_value=MagicMock())
    @patch("databricks_custom_encryption.AES.encrypt")
    @patch("splunk.rest.simpleRequest")
    @patch("base64.b64encode")
    def test_databricks_configuration_encrypt_edit(self, mock_base, mock_rest, mock_aes_encrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_encryption")
        db_cm._LOGGER = MagicMock()
        mock_aes_encrypt.return_value ="encrypted_access_token"
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        obj1.auth_type = "PAT"
        obj1.edit = "edit"
        obj1.databricks_pat = "access_token"
        obj1.databricks_configuration_encrypt()
        db_cm._LOGGER.debug.assert_called_with("Databricks Configuration Custom Encryption completed successfully.")
    
    @patch("databricks_custom_encryption.AES.new", return_value=MagicMock())
    @patch("databricks_custom_encryption.AES.encrypt")
    @patch("splunk.rest.simpleRequest")
    @patch("base64.b64encode")
    def test_databricks_proxy_encrypt(self, mock_base, mock_rest, mock_aes_encrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_encryption")
        db_cm._LOGGER = MagicMock()
        mock_aes_encrypt.return_value ="encrypted_access_token"
        obj1 = db_cm.DatabricksCustomEncryption("command_line", "command_args")
        obj1.proxy_password = "proxy_password"
        obj1.databricks_proxy_encrypt()
        db_cm._LOGGER.debug.assert_called_with("Databricks Proxy Custom Encryption completed successfully.")