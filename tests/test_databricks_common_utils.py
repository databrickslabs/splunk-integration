import declare
import os
import sys
import requests
import unittest
import json
from utility import Response

from importlib import import_module
from mock import patch, MagicMock


mocked_modules = {}
def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        'log_manager',
        'splunk',
        'splunk.rest',
        'splunk.clilib',
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks utils."""

    def test_get_databricks_configs(self):
        db_utils = import_module('databricks_common_utils')
        stanza_return_value = {"databricks_instance" : "123", "databricks_access_token" : "pat123"}
        db_utils.cli.getConfStanza.return_value = stanza_return_value
        response = db_utils.get_databricks_configs()
        self.assertEqual(response, stanza_return_value)

    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_save_databricks_aad_access_token(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        db_utils.save_databricks_aad_access_token("session_key", "access_token", "client_secret")
        self.assertEqual(db_utils._LOGGER.info.call_count, 2)
        db_utils._LOGGER.info.assert_called_with("Saved AAD access token successfully.")
    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_save_databricks_aad_access_token_exception(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_manager.return_value.set_password.side_effect = Exception("test")
        with self.assertRaises(Exception) as context:
            db_utils.save_databricks_aad_access_token("session_key", "access_token", "client_secret")
        self.assertEqual(db_utils._LOGGER.error.call_count, 1)
        db_utils._LOGGER.error.assert_called_with("Exception while saving AAD access token: test")
        self.assertEqual(
            "Exception while saving AAD access token.", str(context.exception))
    
    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_clear_token_aad(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        mock_manager.return_value.get_password.return_value = json.dumps({"access_token":"aad_access_token"})
        clear_token = db_utils.get_clear_token("session_key", "AAD")
        self.assertEqual(clear_token, "aad_access_token")
    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_clear_token_pat(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        mock_manager.return_value.get_password.return_value = json.dumps({"databricks_access_token":"PAT token"})
        clear_token = db_utils.get_clear_token("session_key", "PAT")
        self.assertEqual(clear_token, "PAT token")

    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_clear_token_exception(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_manager.return_value.get_password.side_effect = Exception("test")
        clear_token = db_utils.get_clear_token("session_key", "AAD")
        db_utils._LOGGER.error.assert_called_with("Error while fetching Databricks instance access token: test")
        self.assertEqual(clear_token, None)
    

    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_clear_client_secret(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        mock_manager.return_value.get_password.return_value = json.dumps({"client_secret":"client_secret_value"})
        cl_sec = db_utils.get_clear_client_secret("session_key")
        self.assertEqual(cl_sec, "client_secret_value")
    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_clear_client_secret_exception(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_manager.return_value.get_password.side_effect = Exception("test")
        cl_sec = db_utils.get_clear_client_secret("session_key")
        db_utils._LOGGER.error.assert_called_with("Error while fetching client secret: test")
        self.assertEqual(cl_sec, None)

    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_proxy_clear_password(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        mock_manager.return_value.get_password.return_value = json.dumps({"proxy_password":"psswd"})
        pwd = db_utils.get_proxy_clear_password("session_key")
        self.assertEqual(pwd, "psswd")
    
    @patch("databricks_common_utils.rest.simpleRequest", autospec=True)
    def test_get_proxy_configuration(self, mock_rest):
        db_utils = import_module('databricks_common_utils')
        mock_rest.return_value = (200, json.dumps({"entry":[{"content":{"proxy_ip":"ip", "proxy_port":"port"}},"test"]}))
        prxy_settings = db_utils.get_proxy_configuration("session_key")
        self.assertEqual(prxy_settings, {"proxy_ip":"ip", "proxy_port":"port"})
    
    @patch("databricks_common_utils.get_proxy_configuration", autospec=True)
    @patch("databricks_common_utils.get_proxy_clear_password", autospec=True)
    def test_get_proxy_uri(self, mock_pwd, mock_conf):
        db_utils = import_module('databricks_common_utils')
        mock_conf.return_value = {"proxy_enabled": 1, "proxy_type": "http", "proxy_url": "proxy_url", "proxy_port": 8000, "proxy_username": "proxy_usr"}
        mock_pwd.return_value = "proxy_pwd"
        proxy_uri = db_utils.get_proxy_uri("session_key")
        self.assertEqual(proxy_uri, {'http': 'http://proxy_usr:proxy_pwd@proxy_url:8000', 'https': 'http://proxy_usr:proxy_pwd@proxy_url:8000'})
        

    @patch("databricks_common_utils.get_proxy_configuration", autospec=True)
    @patch("databricks_common_utils.get_proxy_clear_password", autospec=True)
    def test_get_proxy_uri_disabled(self, mock_pwd, mock_conf):
        db_utils = import_module('databricks_common_utils')
        mock_conf.return_value = {"proxy_enabled": 0, "proxy_type": "http", "proxy_url": "proxy_url", "proxy_port": 8000, "proxy_username": "proxy_usr"}
        mock_pwd.return_value = "proxy_pwd"
        proxy_uri = db_utils.get_proxy_uri("session_key")
        self.assertEqual(proxy_uri, None)

    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_update_kv_store_collection_if(self, mock_post):
        db_utils = import_module('databricks_common_utils')
        mock_post.return_value.status_code =  200
        kv_resp = db_utils.update_kv_store_collection("splunk_uri", "run_collection","session_key", {})
        self.assertEqual(kv_resp, {"kv_status": "KV Store updated successfully"})
    
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_update_kv_store_collection_else(self, mock_post):
        db_utils = import_module('databricks_common_utils')
        mock_post.return_value.status_code =  400
        kv_resp = db_utils.update_kv_store_collection("splunk_uri", "run_collection","session_key", {})
        self.assertEqual(kv_resp, {"kv_status": "Error occurred while updating KV Store"})
    
    def test_format_to_json_parameters(self):
        db_utils = import_module('databricks_common_utils')
        params = db_utils.format_to_json_parameters("a=1||b=2")
        self.assertEqual(params, {'a': '1', 'b': '2'})
    
    def test_format_to_json_parameters_exception(self):
        db_utils = import_module('databricks_common_utils')
        with self.assertRaises(Exception) as context:
            params = db_utils.format_to_json_parameters("a||b=2")
        self.assertEqual(
            "Invalid format for parameter notebook_params. Provide the value in 'param1=val1||param2=val2' format.", str(context.exception))

    @patch("databricks_common_utils.get_proxy_uri", autospec=True)        
    @patch("databricks_common_utils.get_databricks_configs", autospec=True)        
    @patch("databricks_common_utils.get_clear_client_secret", autospec=True)
    @patch("databricks_common_utils.save_databricks_aad_access_token", autospec=True)
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_get_aad_access_token(self, mock_post, mock_save, mock_secret, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.side_effect = MagicMock
        mock_secret.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent")
        self.assertEqual (return_val, "123")

    @patch("databricks_common_utils.get_proxy_uri", autospec=True)        
    @patch("databricks_common_utils.get_databricks_configs", autospec=True)        
    @patch("databricks_common_utils.get_clear_client_secret", autospec=True)
    @patch("databricks_common_utils.save_databricks_aad_access_token", autospec=True)
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_get_aad_access_token(self, mock_post, mock_save, mock_secret, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_secret.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent")
        self.assertEqual (return_val, "123")

    
    @patch("databricks_common_utils.get_proxy_uri", autospec=True)        
    @patch("databricks_common_utils.get_databricks_configs", autospec=True)        
    @patch("databricks_common_utils.get_clear_client_secret", autospec=True)
    @patch("databricks_common_utils.save_databricks_aad_access_token", autospec=True)
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_get_aad_access_token_200(self, mock_post, mock_save, mock_secret, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_secret.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent")
        self.assertEqual (return_val, "123")
    
    @patch("databricks_common_utils.get_proxy_uri", autospec=True)        
    @patch("databricks_common_utils.get_databricks_configs", autospec=True)        
    @patch("databricks_common_utils.get_clear_client_secret", autospec=True)
    @patch("databricks_common_utils.save_databricks_aad_access_token", autospec=True)
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_get_aad_access_token_200(self, mock_post, mock_save, mock_secret, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_secret.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent")
        self.assertEqual (return_val, "123")

    
    @patch("databricks_common_utils.get_proxy_uri", autospec=True)        
    @patch("databricks_common_utils.get_databricks_configs", autospec=True)        
    @patch("databricks_common_utils.get_clear_client_secret", autospec=True)
    @patch("databricks_common_utils.save_databricks_aad_access_token", autospec=True)
    @patch("databricks_common_utils.requests.post", autospec=True)
    def test_get_aad_access_token_403(self, mock_post, mock_save, mock_secret, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_secret.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.side_effect = MagicMock()
        mock_post.side_effect = [Response(403), Response(403), Response(403)]
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", retry=3)
        self.assertEqual (return_val, ("Client secret may have expired. Please configure a valid Client secret.", False))
        self.assertEqual(mock_post.call_count, 3)



    
    