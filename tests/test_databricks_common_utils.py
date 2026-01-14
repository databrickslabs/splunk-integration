import declare
import unittest
import json
from utility import Response
from importlib import import_module
from mock import patch, MagicMock

# Import shared test utilities
from conftest import (
    create_module_mocks,
    teardown_module_mocks,
    COMMON_UTILS_MODULES,
)


mocked_modules = {}


def setUpModule():
    global mocked_modules
    mocked_modules = create_module_mocks(COMMON_UTILS_MODULES)


def tearDownModule():
    teardown_module_mocks()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks utils."""
    
    @patch("databricks_common_utils.get_current_user")
    def test_get_user_agent(self, mock_user):
        db_utils = import_module('databricks_common_utils')
        response = db_utils.get_user_agent()
        self.assertEqual(response, "Databricks-AddOnFor-Splunk-1.4.2")
    
    @patch("databricks_common_utils.client.connect")
    @patch("databricks_common_utils.client.connect.jobs.oneshot")
    @patch("databricks_common_utils.results.JSONResultsReader")
    @patch("databricks_common_utils.get_mgmt_port")
    def test_get_current_user(self, mock_common, mock_json, mock_jobs, mock_client):
        db_utils = import_module('databricks_common_utils')
        mock_common.return_value = 8089
        mock_client.return_value = MagicMock()
        mock_jobs.return_value = '[{"username": "db_admin"}]'
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
        # stanza_return_value = {"databricks_instance" : "123", "databricks_access_token" : "pat123"}
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
    def test_get_aad_access_token_200(self, mock_post, mock_save, mock_conf, mock_proxy):
        """Test successful AAD token acquisition returns tuple (token, expires_in)."""
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_conf.return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.return_value.json.return_value = {"access_token": "123", "expires_in": 3600}
        mock_post.return_value.status_code = 200
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret")
        # Now returns tuple (access_token, expires_in)
        self.assertEqual(return_val, ("123", 3600))

    
    @patch("databricks_common_utils.get_proxy_uri")
    @patch("databricks_common_utils.get_databricks_configs")
    @patch("databricks_common_utils.save_databricks_aad_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_403(self, mock_post, mock_save, mock_conf, mock_proxy):
        db_utils = import_module('databricks_common_utils')
        mock_save.return_value = MagicMock()
        mock_conf.return_value = MagicMock()
        mock_proxy.return_value = MagicMock()
        mock_post.side_effect = [Response(403), Response(403), Response(403)]
        return_val = db_utils.get_aad_access_token("session_key", "user_agent", "account_name", "aad_client_id", "aad_client_secret", retry=3)
        self.assertEqual(return_val, ("Client secret may have expired. Please configure a valid Client secret.", False))
        self.assertEqual(mock_post.call_count, 3)

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_save_databricks_oauth_access_token(self, mock_request):
        """Test saving OAuth access token successfully."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_request.return_value = (200, '{}')

        db_utils.save_databricks_oauth_access_token("account_name", "session_key", "access_token", 3600, "client_secret")

        self.assertEqual(db_utils._LOGGER.info.call_count, 2)
        db_utils._LOGGER.info.assert_called_with("Saved OAuth access token successfully.")

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_save_databricks_oauth_access_token_exception(self, mock_request):
        """Test saving OAuth access token with exception."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_request.side_effect = Exception("test error")

        with self.assertRaises(Exception) as context:
            db_utils.save_databricks_oauth_access_token("account_name", "session_key", "access_token", 3600, "client_secret")

        self.assertEqual(db_utils._LOGGER.error.call_count, 1)
        db_utils._LOGGER.error.assert_called_with("Exception while saving OAuth access token: test error")
        self.assertEqual("Exception while saving OAuth access token.", str(context.exception))

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_success(self, mock_post, mock_user):
        """Test successful OAuth M2M token acquisition."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "oauth_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        token, expires_in = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertEqual(token, "oauth_token_123")
        self.assertEqual(expires_in, 3600)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.save_databricks_oauth_access_token")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_with_conf_update(self, mock_post, mock_save, mock_user):
        """Test OAuth token acquisition with configuration update."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"
        mock_save.return_value = None

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "oauth_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        token, expires_in = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret", conf_update=True
        )

        self.assertEqual(token, "oauth_token_123")
        mock_save.assert_called_once()

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_with_proxy_use_for_oauth_true(self, mock_post, mock_user):
        """Test OAuth token acquisition skips proxy when use_for_oauth is true."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "oauth_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        proxy_settings = {"http": "http://proxy:8080", "https": "http://proxy:8080", "use_for_oauth": "1"}

        token, expires_in = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret", proxy_settings=proxy_settings
        )

        # Should be called with None proxy when use_for_oauth is true
        call_args = mock_post.call_args
        self.assertIsNone(call_args[1]['proxies'])

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_with_proxy_use_for_oauth_false(self, mock_post, mock_user):
        """Test OAuth token acquisition uses proxy when use_for_oauth is false."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "oauth_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        proxy_settings = {"http": "http://proxy:8080", "https": "http://proxy:8080", "use_for_oauth": "0"}

        token, expires_in = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret", proxy_settings=proxy_settings
        )

        # Should be called with proxy settings (without use_for_oauth key)
        call_args = mock_post.call_args
        self.assertEqual(call_args[1]['proxies'], {"http": "http://proxy:8080", "https": "http://proxy:8080"})

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_invalid_client(self, mock_post, mock_user):
        """Test OAuth token acquisition with invalid_client error."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"error": "invalid_client"}
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = Exception("Invalid client")
        mock_post.return_value = mock_response

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertEqual(msg, "Invalid OAuth Client ID or Client Secret provided.")
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_unauthorized_client(self, mock_post, mock_user):
        """Test OAuth token acquisition with unauthorized_client error."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"error": "unauthorized_client"}
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = Exception("Unauthorized")
        mock_post.return_value = mock_response

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertEqual(msg, "Service principal is not authorized for this workspace.")
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_status_code_error(self, mock_post, mock_user):
        """Test OAuth token acquisition with status code based error."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = Exception("Too many requests")
        mock_post.return_value = mock_response

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertEqual(msg, "API limit exceeded. Please try again after some time.")
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_generic_error(self, mock_post, mock_user):
        """Test OAuth token acquisition with generic HTTP error."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.status_code = 502
        mock_response.raise_for_status.side_effect = Exception("Bad Gateway")
        mock_post.return_value = mock_response

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertIn("Unable to validate OAuth credentials", msg)
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_connection_error(self, mock_post, mock_user):
        """Test OAuth token acquisition with connection error (no response)."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_post.side_effect = Exception("Connection refused")

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret"
        )

        self.assertIn("Unable to request Databricks instance", msg)
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_oauth_access_token_retry_mechanism(self, mock_post, mock_user):
        """Test OAuth token acquisition retry mechanism."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_post.side_effect = Exception("Connection timeout")

        msg, status = db_utils.get_oauth_access_token(
            "session_key", "account_name", "databricks.instance.com",
            "client_id", "client_secret", retry=3
        )

        self.assertEqual(mock_post.call_count, 3)
        self.assertFalse(status)

    @patch("databricks_common_utils.get_proxy_configuration")
    @patch("databricks_common_utils.get_proxy_clear_password")
    def test_get_proxy_uri_with_special_chars(self, mock_pwd, mock_conf):
        """Test proxy URI formatting with special characters in username/password."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()

        mock_conf.return_value = {
            "proxy_enabled": 1,
            "proxy_type": "http",
            "proxy_url": "proxy.example.com",
            "proxy_port": 8080,
            "proxy_username": "user@domain.com",
            "use_for_oauth": "0"
        }
        mock_pwd.return_value = "p@ssw:rd!"

        proxy_uri = db_utils.get_proxy_uri("session_key")

        # Verify that special characters are URL encoded
        self.assertIn("user%40domain.com", proxy_uri['http'])
        self.assertIn("p%40ssw%3Ard%21", proxy_uri['http'])
        self.assertEqual(proxy_uri['use_for_oauth'], "0")

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_databricks_configs_with_proxy_and_credentials(self, mock_request):
        """Test get_databricks_configs with proxy enabled and credentials."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()

        config_data = {
            "databricks_instance": "test.databricks.com",
            "auth_type": "OAuth",
            "proxy_enabled": "1",
            "proxy_url": "proxy.example.com",
            "proxy_port": "8080",
            "proxy_type": "http",
            "proxy_username": "proxyuser",
            "proxy_password": "proxypass",
            "use_for_oauth": "1"
        }

        mock_request.return_value = (200, json.dumps(config_data))

        response = db_utils.get_databricks_configs("session_key", "account_name")

        self.assertIn("proxy_uri", response)
        self.assertIn("proxyuser", response["proxy_uri"]["http"])
        self.assertIn("proxypass", response["proxy_uri"]["http"])
        self.assertEqual(response["proxy_uri"]["use_for_oauth"], "1")

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_databricks_configs_with_proxy_special_chars(self, mock_request):
        """Test get_databricks_configs with special characters in proxy credentials."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()

        config_data = {
            "databricks_instance": "test.databricks.com",
            "auth_type": "OAuth",
            "proxy_enabled": "1",
            "proxy_url": "proxy.example.com",
            "proxy_port": "3128",
            "proxy_type": "https",
            "proxy_username": "admin@corp",
            "proxy_password": "p@ss:123",
            "use_for_oauth": "0"
        }

        mock_request.return_value = (200, json.dumps(config_data))

        response = db_utils.get_databricks_configs("session_key", "account_name")

        # Verify special characters are URL encoded
        self.assertIn("admin%40corp", response["proxy_uri"]["http"])
        self.assertIn("p%40ss%3A123", response["proxy_uri"]["http"])

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_databricks_configs_with_proxy_no_credentials(self, mock_request):
        """Test get_databricks_configs with proxy but no credentials."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()

        config_data = {
            "databricks_instance": "test.databricks.com",
            "auth_type": "PAT",
            "proxy_enabled": "1",
            "proxy_url": "proxy.example.com",
            "proxy_port": "8080",
            "proxy_type": "http",
            "use_for_oauth": "0"
        }

        mock_request.return_value = (200, json.dumps(config_data))

        response = db_utils.get_databricks_configs("session_key", "account_name")

        self.assertIn("proxy_uri", response)
        # Should not contain @ symbol (no credentials)
        self.assertNotIn("@", response["proxy_uri"]["http"])
        self.assertIn("proxy.example.com:8080", response["proxy_uri"]["http"])

    @patch("databricks_common_utils.rest.simpleRequest")
    def test_get_databricks_configs_exception(self, mock_request):
        """Test get_databricks_configs with exception."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()

        mock_request.side_effect = Exception("Connection error")

        response = db_utils.get_databricks_configs("session_key", "account_name")

        self.assertIsNone(response)
        self.assertEqual(db_utils._LOGGER.error.call_count, 1)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_with_conf_update(self, mock_post, mock_user):
        """Test AAD token acquisition with configuration update returns tuple."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "aad_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        with patch("databricks_common_utils.save_databricks_aad_access_token") as mock_save:
            result = db_utils.get_aad_access_token(
                "session_key", "account_name", "tenant_id",
                "client_id", "client_secret", conf_update=True
            )

            # Now returns tuple (access_token, expires_in)
            self.assertEqual(result, ("aad_token_123", 3600))
            mock_save.assert_called_once()

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_with_proxy_settings(self, mock_post, mock_user):
        """Test AAD token acquisition with proxy settings removes use_for_oauth key."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "aad_token_123", "expires_in": 3600}
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        proxy_settings = {"http": "http://proxy:8080", "https": "http://proxy:8080", "use_for_oauth": "1"}

        result = db_utils.get_aad_access_token(
            "session_key", "account_name", "tenant_id",
            "client_id", "client_secret", proxy_settings=proxy_settings
        )

        # Verify use_for_oauth key is removed from proxy_settings
        call_args = mock_post.call_args
        self.assertNotIn("use_for_oauth", call_args[1]['proxies'])
        # Now returns tuple (access_token, expires_in)
        self.assertEqual(result, ("aad_token_123", 3600))

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_error_code_handling(self, mock_post, mock_user):
        """Test AAD token acquisition with error_codes in response."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_response = MagicMock()
        mock_response.json.return_value = {"error_codes": [700016]}
        mock_response.status_code = 400
        mock_response.raise_for_status.side_effect = Exception("Invalid client ID")
        mock_post.return_value = mock_response

        msg, status = db_utils.get_aad_access_token(
            "session_key", "account_name", "tenant_id",
            "client_id", "client_secret"
        )

        self.assertEqual(msg, "Invalid Client ID provided.")
        self.assertFalse(status)

    @patch("databricks_common_utils.get_current_user")
    @patch("databricks_common_utils.requests.post")
    def test_get_aad_access_token_connection_error(self, mock_post, mock_user):
        """Test AAD token acquisition with connection error (no response object)."""
        db_utils = import_module('databricks_common_utils')
        db_utils._LOGGER = MagicMock()
        mock_user.return_value = "test_user"

        mock_post.side_effect = Exception("Connection refused")

        msg, status = db_utils.get_aad_access_token(
            "session_key", "account_name", "tenant_id",
            "client_id", "client_secret"
        )

        self.assertIn("Unable to request Databricks instance", msg)
        self.assertFalse(status)

    @patch("databricks_common_utils.CredentialManager", autospec=True)
    def test_get_proxy_clear_password_not_exist(self, mock_manager):
        """Test get_proxy_clear_password when credential doesn't exist."""
        db_utils = import_module('databricks_common_utils')
        from solnlib.credentials import CredentialNotExistException

        mock_manager.return_value.get_password.side_effect = CredentialNotExistException("Not found")

        pwd = db_utils.get_proxy_clear_password("session_key")

        self.assertIsNone(pwd)




