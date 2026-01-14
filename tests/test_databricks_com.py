import declare
import unittest

from utility import Response
from importlib import import_module
from mock import patch, MagicMock

# Import shared test utilities
from conftest import (
    create_module_mocks,
    teardown_module_mocks,
    DATABRICKS_COM_MODULES,
    CLUSTER_LIST,
    get_pat_config,
    get_aad_config,
    get_oauth_config,
)


mocked_modules = {}


def setUpModule():
    global mocked_modules
    mocked_modules = create_module_mocks(DATABRICKS_COM_MODULES)


def tearDownModule():
    teardown_module_mocks()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks utils."""

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        db_com._LOGGER = MagicMock()
        mock_conf.return_value = get_pat_config(proxy_uri={"use_for_oauth": '0', 'http': 'uri'})
        obj = db_com.DatabricksClient("account_name", "session_key")
        self.assertIsInstance(obj, db_com.DatabricksClient)
        db_com._LOGGER.info.assert_called_with("Proxy is configured. Using proxy to execute the request.")

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_skipping_proxy(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        db_com._LOGGER = MagicMock()
        mock_conf.return_value = get_pat_config(proxy_uri={"use_for_oauth": '1', 'http': 'uri'})
        obj = db_com.DatabricksClient("account_name", "session_key")
        self.assertIsInstance(obj, db_com.DatabricksClient)
        db_com._LOGGER.info.assert_called_with("Skipping the usage of proxy for running query as 'Use Proxy for OAuth' parameter is checked.")

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object_error(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = get_pat_config(token=None)
        with self.assertRaises(Exception) as context:
            obj = db_com.DatabricksClient("account_name", "session_key")
        self.assertEqual(
            "Addon is not configured. Navigate to addon's configuration page to configure the addon.", str(context.exception))

    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_id(self, mock_conf, mock_session, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        cluster_id = obj.get_cluster_id("test1")
        self.assertEqual(cluster_id, "123")
    
    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_pending(self, mock_conf, mock_session, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        with self.assertRaises(Exception) as context:
            cluster_id = obj.get_cluster_id("test2")
        self.assertEqual(
            "Ensure that the cluster is in running state. Current cluster state is pending.", str(context.exception))
    
    @patch("databricks_com.DatabricksClient.databricks_api", return_value=CLUSTER_LIST) 
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_cluster_none(self, mock_conf, mock_session, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        with self.assertRaises(Exception) as context:
            cluster_id = obj.get_cluster_id("test3")
        self.assertEqual(
            "No cluster found with name test3. Provide a valid cluster name.", str(context.exception))

    @patch("databricks_com.DatabricksClient.databricks_api", return_value={"clusters": None})
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_empty_cluster_response(self, mock_conf, mock_session, mock_version, mock_response):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        with self.assertRaises(Exception) as context:
            cluster_id = obj.get_cluster_id("test4")
        self.assertEqual(
            "No cluster found with name test4. Provide a valid cluster name.", str(context.exception))
    
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_get(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.return_value = Response(200)
        resp = obj.databricks_api("get", "endpoint", args="123")
        self.assertEqual(obj.session.get.call_count, 1)
        self.assertEqual(resp, {"status_code": 200})

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_post(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(200)
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 1)
        self.assertEqual(resp, {"status_code": 200})
    
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_429(self, mock_conf, mock_session, mock_version):
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance" : "123", "auth_type" : "PAT", "databricks_pat" : "token", "proxy_uri" : None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(429)
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 1)
        self.assertEqual(
            "API limit exceeded. Please try again after some time.", str(context.exception))


    @patch("databricks_com.utils.get_aad_access_token", return_value=("new_access_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_refresh_token(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test AAD token refresh on 403 response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(200)]
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(resp, {"status_code": 200})
        
    
    @patch("databricks_com.utils.get_aad_access_token", return_value=("new_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_refresh_token_error(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test AAD token refresh when second request still fails."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(403)]
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(
            "Invalid access token. Please enter the valid access token.", str(context.exception))

    # =========================================================================
    # OAuth M2M Token Refresh Tests
    # =========================================================================

    @patch("databricks_com.utils.get_oauth_access_token", return_value=("new_oauth_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_oauth_refresh_token(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test OAuth M2M token refresh on 403 response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(200)]
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(resp, {"status_code": 200})

    @patch("databricks_com.utils.get_oauth_access_token", return_value=("Token refresh failed", False))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_oauth_refresh_token_failure(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test OAuth M2M token refresh failure."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403)]
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual("Token refresh failed", str(context.exception))

    @patch("databricks_com.utils.get_oauth_access_token", return_value=("new_oauth_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_oauth_refresh_still_fails(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test OAuth M2M token refresh when second request still fails."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403), Response(403)]
        with self.assertRaises(Exception) as context:
            resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual("Invalid access token. Please enter the valid access token.", str(context.exception))

    @patch("databricks_com.utils.get_oauth_access_token", return_value=("new_oauth_token", 300))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    @patch("time.time", return_value=1000000.0)
    def test_proactive_oauth_token_refresh(self, mock_time, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test proactive OAuth token refresh when token is about to expire."""
        db_com = import_module('databricks_com')
        # Token expires in 4 minutes (240 seconds) - should trigger refresh
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "1000240.0",  # 240 seconds from now
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(200)
        
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        
        # Token should have been refreshed proactively
        mock_refresh.assert_called_once()
        self.assertEqual(resp, {"status_code": 200})

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("time.time", return_value=1000000.0)
    def test_should_refresh_oauth_token_within_threshold(self, mock_time, mock_conf, mock_session, mock_version):
        """Test should_refresh_oauth_token returns True when token expires within 5 minutes."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "1000200.0",  # 200 seconds from now (< 5 min)
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertTrue(obj.should_refresh_oauth_token())

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("time.time", return_value=1000000.0)
    def test_should_refresh_oauth_token_outside_threshold(self, mock_time, mock_conf, mock_session, mock_version):
        """Test should_refresh_oauth_token returns False when token has > 5 minutes validity."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "1003700.0",  # 3700 seconds from now (> 5 min)
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertFalse(obj.should_refresh_oauth_token())

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_should_refresh_oauth_token_not_oauth(self, mock_conf, mock_session, mock_version):
        """Test should_refresh_oauth_token returns False for non-OAuth auth types."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "PAT", 
            "databricks_pat": "token",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        # PAT auth doesn't have oauth_token_expiration attribute
        self.assertFalse(obj.should_refresh_oauth_token())

    # =========================================================================
    # Connection Error Handling Tests
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_connection_error(self, mock_conf, mock_session, mock_version):
        """Test handling of connection errors."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.side_effect = Exception("Connection refused")
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("get", "endpoint")
        
        self.assertEqual("Connection refused", str(context.exception))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_400_error(self, mock_conf, mock_session, mock_version):
        """Test handling of 400 Bad Request response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(400, {"message": "Invalid parameter"})
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("post", "endpoint", data={"p1": "v1"})
        
        self.assertEqual("Invalid parameter", str(context.exception))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_500_error(self, mock_conf, mock_session, mock_version):
        """Test handling of 500 Internal Server Error response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.return_value = Response(500, {"error": "Database connection failed"})
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("get", "endpoint")
        
        self.assertEqual("Database connection failed", str(context.exception))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_404_error(self, mock_conf, mock_session, mock_version):
        """Test handling of 404 Not Found response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.return_value = Response(404, {})
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("get", "endpoint")
        
        self.assertEqual("Invalid API endpoint.", str(context.exception))

    # =========================================================================
    # Cancel Endpoint Tests
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_cancel_endpoint(self, mock_conf, mock_session, mock_version):
        """Test cancel endpoint returns tuple with status code."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(200, {"cancelled": True})
        
        resp, status_code = obj.databricks_api("post", "/api/cancel", data={"run_id": "123"})
        
        self.assertEqual(resp, {"cancelled": True})
        self.assertEqual(status_code, 200)

    # =========================================================================
    # OAuth M2M Client Initialization Tests
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object_oauth_m2m(self, mock_conf, mock_session, mock_version):
        """Test DatabricksClient initialization with OAuth M2M auth."""
        db_com = import_module('databricks_com')
        db_com._LOGGER = MagicMock()
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "OAUTH_M2M", 
            "oauth_access_token": "oauth_token",
            "oauth_client_id": "client_id",
            "oauth_client_secret": "client_secret",
            "oauth_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertIsInstance(obj, db_com.DatabricksClient)
        self.assertEqual(obj.auth_type, "OAUTH_M2M")
        self.assertEqual(obj.databricks_token, "oauth_token")
        self.assertEqual(obj.oauth_client_id, "client_id")
        self.assertEqual(obj.oauth_client_secret, "client_secret")

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object_account_not_found(self, mock_conf, mock_session, mock_version):
        """Test DatabricksClient initialization when account not found."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = None
        
        with self.assertRaises(Exception) as context:
            obj = db_com.DatabricksClient("nonexistent_account", "session_key")
        
        self.assertEqual(
            "Account 'nonexistent_account' not found. Please provide valid Databricks account.", 
            str(context.exception)
        )

    # =========================================================================
    # External API Tests
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_external_api_get(self, mock_conf, mock_session, mock_version):
        """Test external API GET request."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.external_session.get.return_value = Response(200, {"data": "test"})
        
        resp = obj.external_api("get", "https://external.api.com/endpoint")
        
        self.assertEqual(obj.external_session.get.call_count, 1)
        self.assertEqual(resp, {"data": "test"})

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_external_api_post(self, mock_conf, mock_session, mock_version):
        """Test external API POST request."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.external_session.post.return_value = Response(200, {"result": "success"})
        
        resp = obj.external_api("post", "https://external.api.com/endpoint", data={"key": "value"})
        
        self.assertEqual(obj.external_session.post.call_count, 1)
        self.assertEqual(resp, {"result": "success"})

    # =========================================================================
    # AAD Token Expiration Tests
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock())
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_object_aad_with_expiration(self, mock_conf, mock_session, mock_version):
        """Test DatabricksClient initialization with AAD auth and token expiration."""
        db_com = import_module('databricks_com')
        db_com._LOGGER = MagicMock()
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "aad_token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertIsInstance(obj, db_com.DatabricksClient)
        self.assertEqual(obj.auth_type, "AAD")
        self.assertEqual(obj.databricks_token, "aad_token")
        self.assertEqual(obj.aad_client_id, "client_id")
        self.assertEqual(obj.aad_tenant_id, "tenant_id")
        self.assertEqual(obj.aad_token_expiration, 9999999999.0)

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("time.time", return_value=1000000.0)
    def test_should_refresh_aad_token_within_threshold(self, mock_time, mock_conf, mock_session, mock_version):
        """Test should_refresh_aad_token returns True when token expires within 5 minutes."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "1000200.0",  # 200 seconds from now (< 5 min)
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertTrue(obj.should_refresh_aad_token())

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("time.time", return_value=1000000.0)
    def test_should_refresh_aad_token_outside_threshold(self, mock_time, mock_conf, mock_session, mock_version):
        """Test should_refresh_aad_token returns False when token has > 5 minutes validity."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "1003700.0",  # 3700 seconds from now (> 5 min)
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertFalse(obj.should_refresh_aad_token())

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_should_refresh_aad_token_no_expiration(self, mock_conf, mock_session, mock_version):
        """Test should_refresh_aad_token returns False when no expiration is set."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": None,  # No expiration set
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        # Should return False when expiration is 0 (not set)
        self.assertFalse(obj.should_refresh_aad_token())

    @patch("databricks_com.utils.get_aad_access_token", return_value=("new_aad_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    @patch("time.time", return_value=1000000.0)
    def test_proactive_aad_token_refresh(self, mock_time, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test proactive AAD token refresh when token is about to expire."""
        db_com = import_module('databricks_com')
        # Token expires in 4 minutes (240 seconds) - should trigger refresh
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "1000240.0",  # 240 seconds from now
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.return_value = Response(200)
        
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        
        # Token should have been refreshed proactively
        mock_refresh.assert_called_once()
        self.assertEqual(resp, {"status_code": 200})

    # =========================================================================
    # Token Expired Response Detection Tests (_is_token_expired_response)
    # =========================================================================

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_is_token_expired_response_403(self, mock_conf, mock_session, mock_version):
        """Test _is_token_expired_response returns True for 403 status."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        response = Response(403)
        self.assertTrue(obj._is_token_expired_response(response))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_is_token_expired_response_401(self, mock_conf, mock_session, mock_version):
        """Test _is_token_expired_response returns True for 401 status."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        response = Response(401)
        self.assertTrue(obj._is_token_expired_response(response))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_is_token_expired_response_200(self, mock_conf, mock_session, mock_version):
        """Test _is_token_expired_response returns False for 200 status."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        response = Response(200)
        self.assertFalse(obj._is_token_expired_response(response))

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_is_token_expired_response_none(self, mock_conf, mock_session, mock_version):
        """Test _is_token_expired_response returns False for None response."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        
        self.assertFalse(obj._is_token_expired_response(None))

    # =========================================================================
    # 401 Status Code Handling Tests
    # =========================================================================

    @patch("databricks_com.utils.get_aad_access_token", return_value=("new_token", 3600))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_401_triggers_refresh(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test 401 response triggers AAD token refresh."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(401), Response(200)]
        
        resp = obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        
        self.assertEqual(obj.session.post.call_count, 2)
        self.assertEqual(resp, {"status_code": 200})
        mock_refresh.assert_called_once()

    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    def test_get_api_response_401_error_message(self, mock_conf, mock_session, mock_version):
        """Test 401 response returns correct error message for PAT auth."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {"databricks_instance": "123", "auth_type": "PAT", "databricks_pat": "token", "proxy_uri": None}
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.get.return_value = Response(401, {})
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("get", "endpoint")
        
        self.assertEqual("Unauthorized. Access token may be invalid or expired.", str(context.exception))

    @patch("databricks_com.utils.get_aad_access_token", return_value=("Token refresh failed", False))
    @patch("solnlib.server_info", return_value=MagicMock()) 
    @patch("databricks_com.DatabricksClient.get_requests_retry_session", return_value=MagicMock())
    @patch("databricks_com.utils.get_databricks_configs", autospec=True)
    @patch("databricks_com.utils.get_proxy_uri", return_value=None)
    def test_get_api_response_aad_refresh_failure(self, mock_proxy, mock_conf, mock_session, mock_version, mock_refresh):
        """Test AAD token refresh failure returns proper error."""
        db_com = import_module('databricks_com')
        mock_conf.return_value = {
            "databricks_instance": "123", 
            "auth_type": "AAD", 
            "aad_access_token": "token",
            "aad_client_id": "client_id",
            "aad_tenant_id": "tenant_id",
            "aad_client_secret": "client_secret",
            "aad_token_expiration": "9999999999.0",
            "proxy_uri": None
        }
        obj = db_com.DatabricksClient("account_name", "session_key")
        obj.session.post.side_effect = [Response(403)]
        
        with self.assertRaises(Exception) as context:
            obj.databricks_api("post", "endpoint", args="123", data={"p1": "v1"})
        
        self.assertEqual("Token refresh failed", str(context.exception))



    
    
