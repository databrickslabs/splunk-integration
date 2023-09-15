import declare
import os
import sys
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
        'splunk.clilib.cli_common',
        'splunklib',
        'splunklib.binding',
        'splunklib.client',
        'splunklib.results',
        'splunk.admin'
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks utils."""
    
    @patch("databricks_common_utils.get_current_user")
    def test_get_user_agent(self, mock_user):
        db_utils = import_module('databricks_common_utils')
        response = db_utils.get_user_agent()
        self.assertEqual(response, "Databricks-AddOnFor-Splunk-1.3.0")
    
    @patch("databricks_common_utils.client.connect")
    @patch("databricks_common_utils.client.connect.jobs.oneshot")
    @patch("databricks_common_utils.results.JSONResultsReader")
    @patch("databricks_common_utils.get_mgmt_port")
    def test_get_current_user(self, mock_common, mock_json, mock_jobs, mock_client):
        db_utils = import_module('databricks_common_utils')
        mock_common.return_value = 8089
        mock_client.retrun_value = MagicMock()
        mock_jobs.retrun_value = '[{"username": "db_admin"}]'
        mock_json.return_value = [{"username": "db_admin"}]
        response = db_utils.get_current_user("session_key")
        self.assertEqual(response, "db_admin")
    
    @patch("splunk.rest.simpleRequest")
    def test_get_mgmt_port(self, mock_rest):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_rest.return_value = '_', '{"entry":[{"content":{"mgmtHostPort": "127.0.0.1:8089"}}]}'
        response = db_utils.get_mgmt_port("session_key",MagicMock())
        self.assertEqual(response, '8089')
    

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_databricks_configs(self, mock_request):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_request.return_value = (200, json.dumps({"databricks_instance" : "123", "databricks_access_token" : "pat123", "auth_type":"PAT"}))
        response = db_utils.get_databricks_configs("session_key", "account_name")
        self.assertEqual(response, {"databricks_instance" : "123", "databricks_access_token" : "pat123", "auth_type":"PAT"})

    
    @patch("databricks_common_utils.rest.simpleRequest")
    def test_save_databricks_aad_access_token(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        db_utils.save_databricks_aad_access_token("account_name", "session_key", "access_token", "client_secret")
        self.assertEqual(db_utils._LOGGER.info.call_count, 2)
        db_utils._LOGGER.info.assert_called_with("Saved AAD access token successfully.")
    
    @patch("databricks_common_utils.rest.simpleRequest")
    def test_save_databricks_aad_access_token_exception(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_manager.side_effect = Exception("test")
        with self.assertRaises(Exception) as context:
            db_utils.save_databricks_aad_access_token("account_name", "session_key", "access_token", "client_secret")
        self.assertEqual(db_utils._LOGGER.error.call_count, 1)
        db_utils._LOGGER.error.assert_called_with("Exception while saving AAD access token: test")
        self.assertEqual(
            "Exception while saving AAD access token.", str(context.exception))
    
    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_proxy_clear_password(self, mock_manager):
        db_utils = import_module('databricks_common_utils')
        mock_manager.return_value.get_password.return_value = json.dumps({"proxy_password":"psswd"})
        pwd = db_utils.get_proxy_clear_password("session_key")
        self.assertEqual(pwd, "psswd")
    
    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_proxy_configuration(self, mock_rest):
        db_utils = import_module('databricks_common_utils')
        mock_rest.return_value = (200, json.dumps({"entry":[{"content":{"proxy_ip":"ip", "proxy_port":"port"}},"test"]}))
        prxy_settings = db_utils.get_proxy_configuration("session_key")
        self.assertEqual(prxy_settings, {"proxy_ip":"ip", "proxy_port":"port"})
    
    @patch("databricks_common_utils.get_proxy_configuration")
    @patch("databricks_common_utils.get_proxy_clear_password")
    def test_get_proxy_uri(self, mock_pwd, mock_conf):
        db_utils = import_module('databricks_common_utils')
        mock_conf.return_value = {"proxy_enabled": 1, "proxy_type": "http", "proxy_url": "proxy_url", "proxy_port": 8000, "proxy_username": "proxy_usr", "use_for_oauth": "1"}
        mock_pwd.return_value = "proxy_pwd"
        proxy_uri = db_utils.get_proxy_uri("session_key")
        self.assertEqual(proxy_uri, {'http': 'http://proxy_usr:proxy_pwd@proxy_url:8000', 'https': 'http://proxy_usr:proxy_pwd@proxy_url:8000', 'use_for_oauth': '1'})

    @patch("databricks_common_utils.get_proxy_configuration")
    @patch("databricks_common_utils.get_proxy_clear_password")
    def test_get_proxy_uri_disabled(self, mock_pwd, mock_conf):
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_conf.return_value = {"proxy_enabled": 0, "proxy_type": "http", "proxy_url": "proxy_url", "proxy_port": 8000, "proxy_username": "proxy_usr"}
        mock_pwd.return_value = "proxy_pwd"
        proxy_uri = db_utils.get_proxy_uri("session_key")
        db_utils._LOGGER.info.assert_called_with("Proxy is disabled. Skipping proxy mechanism.")
        self.assertEqual(proxy_uri, None)
    
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

    @patch("databricks_common_utils.get_proxy_uri")        
    @patch("databricks_common_utils.get_databricks_configs")        
    @patch("databricks_common_utils.save_databricks_aad_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token(self, mock_post, mock_save, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.side_effect = MagicMock
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret")
        self.assertEqual (return_val, "123")
    
    @patch("databricks_common_utils.get_proxy_uri")        
    @patch("databricks_common_utils.get_databricks_configs")        
    @patch("databricks_common_utils.save_databricks_aad_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_200(self, mock_post, mock_save, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret")
        self.assertEqual (return_val, "123")
    
    @patch("databricks_common_utils.get_proxy_uri")        
    @patch("databricks_common_utils.get_databricks_configs")        
    @patch("databricks_common_utils.save_databricks_aad_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_200(self, mock_post, mock_save, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123"}
        mock_post.return_value.status_code =  200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret")
        self.assertEqual (return_val, "123")

    
    @patch("databricks_common_utils.get_proxy_uri")        
    @patch("databricks_common_utils.get_databricks_configs")        
    @patch("databricks_common_utils.save_databricks_aad_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_403(self, mock_post, mock_save, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_conf. return_value = MagicMock()
        mock_proxy.side_effect = MagicMock()
        mock_post.side_effect = [Response(403), Response(403), Response(403)]
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret", retry=3)
        self.assertEqual (return_val, ("Client secret may have expired. Please configure a valid Client secret.", False))
        self.assertEqual(mock_post.call_count, 3)

    @patch("splunk.clilib.cli_common.getMgmtUri")
    @patch("databricks_common_utils.client.connect")
    @patch("databricks_common_utils.GetSessionKey")
    def test_create_service(self, mock_get_session_key, mock_client_connect, mock_get_mgmt_uri):
        mock_get_session_key.return_value.session_key = "test_session_key"
        mock_get_mgmt_uri.return_value = "https://localhost:8089"
        mock_client = MagicMock()
        mock_client_connect.return_value = mock_client

        db_utils = import_module('databricks_common_utils')

        result = db_utils.create_service()

        mock_get_mgmt_uri.assert_called_once()
        mock_get_session_key.assert_called_once()
        mock_client_connect.assert_called_once_with(port="8089", token="test_session_key", app="TA-Databricks")
        self.assertEqual(result, mock_client)
    


    @patch("databricks_common_utils.get_mgmt_port")
    @patch("databricks_common_utils.client.connect")
    def test_ingest_data_to_splunk(self, mock_connect, mock_get_mgmt_port):
        data = {
            "user": "test",
            "created_time": "1234567890",
            "param": "a=1||b=2",
            "run_id": "123",
            "output_url": "/test/resultsOnly",
            "result_url": "/result_url",
            "command_status": "success",
            "error": "-",
            "identifier": "id1"
        }
        session_key = "some_session_key"
        provided_index = "test_index"
        sourcetype = "test_sourcetype"
        mock_get_mgmt_port.return_value = "8089"

        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        db_utils.ingest_data_to_splunk(data, session_key, provided_index, sourcetype)

        json_string = json.dumps(data, ensure_ascii=False).replace('"', '\\"')
        searchquery = '| makeresults | eval _raw="{}" | collect index={} sourcetype={}'\
            .format(json_string, provided_index, sourcetype)

        mock_get_mgmt_port.assert_called_once_with(session_key, db_utils._LOGGER)
        mock_connect.assert_called_once_with(
            host="localhost",
            port="8089",
            scheme="https",
            app="TA-Databricks",
            token=session_key
        )
        mock_connect.return_value.jobs.oneshot.assert_called_once_with(searchquery)

    
    @patch("databricks_common_utils.create_service")
    def test_update_macros(self, mock_create_service):
        service_mock = MagicMock()
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_create_service.return_value = service_mock
        macro_manager = db_utils.IndexMacroManager()

        macro_name = "test_macro"
        index_string = "index_string"
        macro_manager.update_macros(service_mock, macro_name, index_string)
        service_mock.post.assert_called_once_with("properties/macros/{}".format(macro_name), definition=index_string)
    

    def test_validate_success(self):
        """Test the validate method with a validation failure."""
        db_utils = import_module('databricks_common_utils')
        macro_manager = db_utils.IndexMacroManager()
        value = "abc"
        data = {"index": "main"}
        result = macro_manager.validate(value, data)
        self.assertTrue(result)