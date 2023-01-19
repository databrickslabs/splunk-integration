import declare
import os
import sys
import unittest
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


class TestDatabricksCustomDecryption(unittest.TestCase):
    """Test Databricks Custom Encryption."""

    def test_get_custom_decryption_object(self):
        db_cm = import_module("databricks_custom_decryption")
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        self.assertIsInstance(obj1, db_cm.DatabricksCustomDecryption)
    
    
    @patch("databricks_custom_decryption.DatabricksCustomDecryption.perform_config_decryption")
    @patch("databricks_custom_decryption.DatabricksCustomDecryption.get_account_configs")
    @patch("splunk.rest.simpleRequest")
    def test_handle_config(self, mock_rest, mock_account_configs , mock_decrypt):
        db_cm = import_module("databricks_custom_decryption")
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "name", "PAT_account2" ] ] }'
        mock_account_configs.return_value = {'auth_type':'PAT'}
        mock_rest.return_value = '_', '{"entry":[{"content":{"key":"key"}}]}'
        response = {
            "payload": {},
            "status": 200
        }
        self.assertEqual(response, obj1.handle(in_string))
        
    @patch("databricks_custom_decryption.DatabricksCustomDecryption.perform_proxy_decryption")
    @patch("splunk.rest.simpleRequest")
    def test_handle_proxy(self, mock_rest, mock_decrypt):
        db_cm = import_module("databricks_custom_decryption")
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "proxy", "password" ] ] }'
        mock_rest.return_value = '_', '{"entry":[{"content":{"key":"key"}}]}'
        response = {
            "payload": {},
            "status": 200
        }
        self.assertEqual(response, obj1.handle(in_string))
    
    @patch("databricks_custom_decryption.DatabricksCustomDecryption.perform_config_decryption")
    @patch("databricks_custom_decryption.DatabricksCustomDecryption.get_account_configs")
    @patch("splunk.rest.simpleRequest")
    @patch("traceback.format_exc")
    def test_handle_error(self, mock_traceback, mock_rest, mock_account_configs , mock_decrypt):
        db_cm = import_module("databricks_custom_decryption")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        in_string = '{ "session": { "authtoken": "Authtoken" }, "form": [ [ "name", "PAT_account2" ] ] }'
        mock_account_configs.return_value = {'auth_type':'PAT'}
        mock_rest.return_value = '_', '{"entry":[{"content":{"key":"key"}}]}'
        err_message = "error_message"
        mock_traceback.return_value = err_message
        mock_decrypt.side_effect = Exception(err_message)
        response = {
            "payload": "Databricks Error : Error occured while performing custom decryption - {}".format(err_message),
            "status": 500
        }
        self.assertEqual(response, obj1.handle(in_string))
        
    @patch("splunk.rest.simpleRequest")
    def test_get_account_configs(self, mock_rest):
        db_cm = import_module("databricks_custom_decryption")
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        obj1.account_name = "abc"
        obj1.session_key = "session_key"
        mock_rest.return_value = "_" , '{"entry":[{"content":{"sample":"sample"}}]}'
        response = obj1.get_account_configs()
        self.assertEqual(response, {"sample":"sample"})
    
    @patch("databricks_custom_encryption.AES.new")
    @patch("databricks_custom_encryption.AES.decrypt")
    @patch("base64.b64decode")
    def test_perform_config_decryption_pat(self, mock_base, mock_aes_decrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_decryption")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        mock_aes_new.return_value.decrypt.return_value.decode.return_value = "decrypted_access_token"
        # mock_base.return_value = b'encrypted_access_token'
        # mock_aes_decrypt.return_value = b"decrypted_access_token"
        obj1.auth_type = "PAT"
        configs = {"key":"key", "nonce":"nonce", "databricks_pat":"access_token"}
        obj1.perform_config_decryption(configs)
        self.assertEqual(obj1.payload["databricks_pat"], "decrypted_access_token")
        
    @patch("databricks_custom_encryption.AES.new")
    @patch("databricks_custom_encryption.AES.decrypt")
    @patch("base64.b64decode")
    def test_perform_config_decryption_aad(self, mock_base, mock_aes_decrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_decryption")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        mock_aes_new.return_value.decrypt.return_value.decode.return_value = "decrypted_access_token"
        obj1.auth_type = "AAD"
        configs = {"key":"key", "nonce":"nonce", "aad_access_token":"access_token", "aad_client_secret":"client_secret"}
        obj1.perform_config_decryption(configs)
        self.assertEqual(obj1.payload["aad_access_token"], "decrypted_access_token")
    
    @patch("databricks_custom_encryption.AES.new")
    @patch("databricks_custom_encryption.AES.decrypt")
    @patch("base64.b64decode")
    def test_perform_proxy_decryption(self, mock_base, mock_aes_decrypt, mock_aes_new):
        db_cm = import_module("databricks_custom_decryption")
        db_cm._LOGGER = MagicMock()
        obj1 = db_cm.DatabricksCustomDecryption("command_line", "command_args")
        mock_aes_new.return_value.decrypt.return_value.decode.return_value = "proxy_password"
        configs = {"proxy_key":"key", "proxy_nonce":"nonce", "proxy_password":"proxy_password"}
        obj1.perform_proxy_decryption(configs)
        self.assertEqual(obj1.payload["proxy_password"], "proxy_password")
        