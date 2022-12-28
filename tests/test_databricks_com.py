import declare
import os
import sys
import unittest
import json

from utility import Response
from importlib import import_module
from mock import patch, MagicMock

CLUSTER_LIST = {"clusters": [{"cluster_name": "test1", "cluster_id": "123","state":"running"}, {"cluster_name": "test2", "cluster_id": "345","state":"pending"}]}

mocked_modules = {}
def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        'log_manager',
        'splunk',
        'splunk.rest',
        'splunk.clilib',
        'solnlib.server_info',
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks utils."""
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version):
        db_com = import_module('databricks_com')
        db_com._LOGGER = MagicMock()
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        self.assertIsInstance(obj,db_com.DatabricksClient)
        db_com._LOGGER.info.assert_called_with("Proxy is configured. Using proxy to execute the request.")

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value=None)
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object_error(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123","auth_type" : "PAT"}
        with self.assertRaises(Exception) as context:
            obj = db_com.DatabricksClient("account_name", "session_key")
        self.assertEqual(
            "Addon is not configured. Navigate to addon's configuration page to configure the addon.", str(context.exception))

    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_id(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        cluster_id = obj.get_cluster_id("test1")
        self.assertEqual(cluster_id, "123")
    
    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_pending(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        with self.assertRaises(Exception) as context:
            cluster_id = obj.get_cluster_id("test2")
        self.assertEqual(
            "Ensure that the cluster is in running state. Current cluster state is pending.", str(context.exception))
    

    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_none(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        with self.assertRaises(Exception) as context:
            cluster_id = obj.get_cluster_id("test3")
        self.assertEqual(
            "No cluster found with name test3. Provide a valid cluster name.", str(context.exception))
    
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_get(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.return_value = Response(200)
        resp = obj.databricks_api("get", "endpoint", args="123")
        self.assertEqual(obj.session.get.call_count, 1)
        self.assertEqual(resp, {"status_code": 200})

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_post(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(200)
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 1)
        self.assertEqual(resp, {"status_code": 200})
    
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_429(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(429)
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 1)
        self.assertEqual(
            "API limit exceeded. Please try again after some time.", str(context.exception))


    @patch("databricks_com.utils.get_aad_access_token", return_value="new_access_token")
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_refresh_token(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version, mock_refresh):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "AAD"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(200)]
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(resp, {"status_code": 200})
        
    
    @patch("databricks_com.utils.get_aad_access_token", return_value="new_token")
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.utils.get_clear_token", return_value="token")
    @patch("databricks_com.utils.get_proxy_uri", return_value="{}")
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_refresh_token_error(self, mock_conf, mock_session, mock_proxy, mock_token, mock_version, mock_refresh):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "AAD"}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(403)]
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(
            "Invalid access token. Please enter the valid access token.", str(context.exception))



    
    