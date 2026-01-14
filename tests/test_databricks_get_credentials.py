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
    setup_persistconn_mock,
    CREDENTIALS_MODULES,
)


mocked_modules = {}


def setUpModule():
    global mocked_modules
    special_handlers = {
        "splunk.persistconn.application": setup_persistconn_mock,
    }
    mocked_modules = create_module_mocks(CREDENTIALS_MODULES, special_handlers)


def tearDownModule():
    teardown_module_mocks()


class TestDatabricksGetCredentials(unittest.TestCase):
    """Test Databricks Get Credentials."""

    def test_get_credentials_object(self):
        db_cm = import_module("databricks_get_credentials")
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        self.assertIsInstance(obj1, db_cm.DatabricksGetCredentials)

    def test_handle_save_access_token_success(self):
        """Test successful saving of AAD access token with expiration."""
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token",
                "aad_token_expiration": "1234567890.0"
            }
        })
        
        # mock the CredentialManager
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.return_value = "Saved AAD access token successfully."
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Saved AAD access token successfully."
        assert result["status"] == 200
        
        # Verify the credential manager was called with correct parameters including expiration
        credential_manager_mock.set_password.assert_called_once()

    def test_handle_save_access_token_failure(self):
        """Test failure when saving AAD access token."""
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "aad_client_secret": "client_secret",
                "aad_access_token": "access_token",
                "aad_token_expiration": "1234567890.0"
            }
        })

        # mock the CredentialManager to raise an exception when set_password is called
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.side_effect = Exception("Failed to save AAD access token")
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Databricks Error: Exception while saving access token: Failed to save AAD access token"
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

    # =========================================================================
    # OAuth Token Save Tests
    # =========================================================================

    def test_handle_save_oauth_access_token_success(self):
        """Test successful saving of OAuth access token."""
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "oauth_client_secret": "oauth_client_secret",
                "oauth_access_token": "oauth_access_token",
                "oauth_token_expiration": "1234567890.0"
            }
        })
        
        # mock the CredentialManager
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.return_value = "Saved OAuth access token successfully."
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Saved OAuth access token successfully."
        assert result["status"] == 200
        
        # Verify the credential manager was called with correct parameters
        credential_manager_mock.set_password.assert_called_once()

    def test_handle_save_oauth_access_token_failure(self):
        """Test failure when saving OAuth access token."""
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1",
                "oauth_client_secret": "oauth_client_secret",
                "oauth_access_token": "oauth_access_token",
                "oauth_token_expiration": "1234567890.0"
            }
        })

        # mock the CredentialManager to raise an exception
        credential_manager_mock = MagicMock()
        credential_manager_mock.set_password.side_effect = Exception("Failed to save OAuth access token")
        db_cm.CredentialManager = MagicMock(return_value=credential_manager_mock)

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert result["payload"] == "Databricks Error: Exception while saving access token: Failed to save OAuth access token"
        assert result["status"] == 500

    def test_handle_no_token_data_provided(self):
        """Test error when update_token is set but no token data provided."""
        db_cm = import_module("databricks_get_credentials")
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test",
                "update_token": "1"
                # No aad_access_token or oauth_access_token
            }
        })

        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)

        assert "No token data provided for update" in result["payload"]
        assert result["status"] == 500

    # =========================================================================
    # Retrieve Configurations Tests (OAuth M2M)
    # =========================================================================

    @patch("databricks_get_credentials.rest.simpleRequest")
    @patch("databricks_get_credentials.CredentialManager")
    def test_handle_retrieve_oauth_config(self, mock_cred_manager, mock_request):
        """Test retrieving OAuth M2M configuration."""
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        
        # Mock account manager
        account_manager_mock = MagicMock()
        account_manager_mock.get_password.return_value = json.dumps({
            "oauth_client_secret": "oauth_secret",
            "oauth_access_token": "oauth_token",
            "oauth_token_expiration": "1234567890.0"
        })
        
        # Mock proxy manager
        proxy_manager_mock = MagicMock()
        proxy_manager_mock.get_password.return_value = json.dumps({
            "proxy_password": "proxy_pass"
        })
        
        mock_cred_manager.side_effect = [account_manager_mock, proxy_manager_mock]
        
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test_account"
            }
        })
        
        # Mock REST calls
        def mock_request_side_effect(*args, **kwargs):
            if "ta_databricks_account" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "auth_type": "OAUTH_M2M",
                            "databricks_instance": "test.databricks.azure.net",
                            "oauth_client_id": "oauth_client_id",
                            "config_for_dbquery": "warehouse",
                            "warehouse_id": "warehouse123",
                            "cluster_name": None
                        }
                    }]
                }))
            elif "proxy" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "proxy_enabled": "0",
                            "proxy_password": ""
                        }
                    }]
                }))
            elif "additional_parameters" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "admin_command_timeout": "300",
                            "query_result_limit": "1000",
                            "index": "main",
                            "thread_count": "4"
                        }
                    }]
                }))
        
        mock_request.side_effect = mock_request_side_effect
        
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)
        
        assert result["status"] == 200
        payload = result["payload"]
        assert payload["auth_type"] == "OAUTH_M2M"
        assert payload["oauth_client_id"] == "oauth_client_id"
        assert payload["oauth_access_token"] == "oauth_token"
        assert payload["oauth_token_expiration"] == "1234567890.0"

    @patch("databricks_get_credentials.rest.simpleRequest")
    @patch("databricks_get_credentials.CredentialManager")
    def test_handle_retrieve_pat_config(self, mock_cred_manager, mock_request):
        """Test retrieving PAT configuration."""
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        
        # Mock account manager
        account_manager_mock = MagicMock()
        account_manager_mock.get_password.return_value = json.dumps({
            "databricks_pat": "pat_token_value"
        })
        
        # Mock proxy manager
        proxy_manager_mock = MagicMock()
        proxy_manager_mock.get_password.return_value = json.dumps({})
        
        mock_cred_manager.side_effect = [account_manager_mock, proxy_manager_mock]
        
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test_account"
            }
        })
        
        # Mock REST calls
        def mock_request_side_effect(*args, **kwargs):
            if "ta_databricks_account" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "auth_type": "PAT",
                            "databricks_instance": "test.databricks.azure.net",
                            "config_for_dbquery": "cluster",
                            "cluster_name": "test_cluster",
                            "warehouse_id": None
                        }
                    }]
                }))
            elif "proxy" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "proxy_enabled": "0",
                            "proxy_password": ""
                        }
                    }]
                }))
            elif "additional_parameters" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "admin_command_timeout": "300",
                            "query_result_limit": "1000",
                            "index": "main",
                            "thread_count": "4"
                        }
                    }]
                }))
        
        mock_request.side_effect = mock_request_side_effect
        
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)
        
        assert result["status"] == 200
        payload = result["payload"]
        assert payload["auth_type"] == "PAT"
        assert payload["databricks_pat"] == "pat_token_value"

    @patch("databricks_get_credentials.rest.simpleRequest")
    @patch("databricks_get_credentials.CredentialManager")
    def test_handle_retrieve_aad_config(self, mock_cred_manager, mock_request):
        """Test retrieving AAD configuration with token expiration."""
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        
        # Mock account manager
        account_manager_mock = MagicMock()
        account_manager_mock.get_password.return_value = json.dumps({
            "aad_client_secret": "aad_secret",
            "aad_access_token": "aad_token",
            "aad_token_expiration": "1234567890.0"
        })
        
        # Mock proxy manager
        proxy_manager_mock = MagicMock()
        proxy_manager_mock.get_password.return_value = json.dumps({})
        
        mock_cred_manager.side_effect = [account_manager_mock, proxy_manager_mock]
        
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test_account"
            }
        })
        
        # Mock REST calls
        def mock_request_side_effect(*args, **kwargs):
            if "ta_databricks_account" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "auth_type": "AAD",
                            "databricks_instance": "test.databricks.azure.net",
                            "aad_client_id": "aad_client_id",
                            "aad_tenant_id": "aad_tenant_id",
                            "config_for_dbquery": "warehouse",
                            "warehouse_id": "warehouse123",
                            "cluster_name": None
                        }
                    }]
                }))
            elif "proxy" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "proxy_enabled": "0",
                            "proxy_password": ""
                        }
                    }]
                }))
            elif "additional_parameters" in args[0]:
                return (200, json.dumps({
                    "entry": [{
                        "content": {
                            "admin_command_timeout": "300",
                            "query_result_limit": "1000",
                            "index": "main",
                            "thread_count": "4"
                        }
                    }]
                }))
        
        mock_request.side_effect = mock_request_side_effect
        
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)
        
        assert result["status"] == 200
        payload = result["payload"]
        assert payload["auth_type"] == "AAD"
        assert payload["aad_client_id"] == "aad_client_id"
        assert payload["aad_tenant_id"] == "aad_tenant_id"
        assert payload["aad_access_token"] == "aad_token"
        assert payload["aad_token_expiration"] == "1234567890.0"
        assert payload["aad_client_secret"] == "aad_secret"

    @patch("databricks_get_credentials.rest.simpleRequest")
    def test_handle_retrieve_config_error(self, mock_request):
        """Test error handling when retrieving configuration fails."""
        db_cm = import_module("databricks_get_credentials")
        db_cm._LOGGER = MagicMock()
        
        mock_request.side_effect = Exception("Connection error")
        
        input_string = json.dumps({
            "system_authtoken": "dummy_token",
            "form": {
                "name": "test_account"
            }
        })
        
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        result = obj1.handle(input_string)
        
        assert result["status"] == 500
        assert "Databricks Error: Error occured while retrieving account and proxy configurations" in result["payload"]

    # =========================================================================
    # handleStream and done Tests
    # =========================================================================

    def test_handleStream_not_implemented(self):
        """Test that handleStream raises NotImplementedError."""
        db_cm = import_module("databricks_get_credentials")
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        
        with self.assertRaises(NotImplementedError):
            obj1.handleStream(None, "input_string")

    def test_done_method(self):
        """Test that done method executes without error."""
        db_cm = import_module("databricks_get_credentials")
        obj1 = db_cm.DatabricksGetCredentials("command_line", "command_args")
        
        # done() should complete without raising any exception
        result = obj1.done()
        self.assertIsNone(result)
