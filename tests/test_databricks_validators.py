import declare
import unittest

from utility import Response
from importlib import import_module
from mock import patch, MagicMock

# Import shared test utilities
from conftest import (
    create_module_mocks,
    teardown_module_mocks,
    VALIDATOR_MODULES,
)


mocked_modules = {}


def setUpModule():
    global mocked_modules
    mocked_modules = create_module_mocks(VALIDATOR_MODULES)


def tearDownModule():
    teardown_module_mocks()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks Validators."""
    
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_pat(self, mock_aad, mock_pat, mock_validator, mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        mock_pat.return_value = True
        db_val_obj.validate("PAT",{"auth_type": "PAT", "databricks_pat": "pat_token"})
        self.assertEqual(mock_pat.call_count, 1)
    
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_pat_error(self, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("PAT",{"auth_type": "PAT"})
        mock_put.assert_called_once_with("Field Databricks Access Token is required")

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_aad(self, mock_aad, mock_pat, mock_validator,mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        mock_aad.return_value = True
        db_val_obj.validate("PAT",{"auth_type": "AAD", "aad_client_id": "cl_id", "aad_client_secret": "cl_sec", "aad_tenant_id":"tn_id"})
        self.assertEqual(mock_aad.call_count, 1)

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_aad_client_id_error(self, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("PAT",{"auth_type": "AAD"})
        mock_put.assert_called_once_with("Field Client Id is required")
    
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_aad_tenant_error(self, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("PAT",{"auth_type": "AAD", "aad_client_id": "cl_id"})
        mock_put.assert_called_once_with("Field Tenant Id is required")

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    def test_validate_aad_client_secret_error(self, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("PAT",{"auth_type": "AAD", "aad_client_id": "cl_id", "aad_tenant_id": "tn_id"})
        mock_put.assert_called_once_with("Field Client Secret is required")
    
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_pat_function(self, mock_valid_inst, mock_validator):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        mock_valid_inst.return_value = True
        db_val_obj.validate_pat({"auth_type": "PAT", "databricks_pat": "pat_token", "databricks_instance": "db_instance"})
        mock_valid_inst.assert_called_once_with(db_val_obj, "db_instance", "pat_token")

    @patch("databricks_validators.utils.get_aad_access_token", return_value=("access_token", 3600))
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_aad_function(self, mock_valid_inst, mock_validator, mock_access):
        """Test successful AAD validation flow with token expiration."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = True
        
        data = {
            "auth_type": "AAD", 
            "aad_client_id": "cl_id", 
            "aad_tenant_id": "tenant_id", 
            "aad_client_secret": "client_secret", 
            "databricks_instance": "db_instance",
            "name": "test_account"
        }
        result = db_val_obj.validate_aad(data)
        
        mock_valid_inst.assert_called_once_with(db_val_obj, "db_instance", "access_token")
        self.assertTrue(result)
        self.assertEqual(data["aad_access_token"], "access_token")
        self.assertEqual(data["databricks_pat"], "")
        # Verify aad_token_expiration is set
        self.assertIn("aad_token_expiration", data)
        self.assertTrue(float(data["aad_token_expiration"]) > 0)
    
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.utils.get_aad_access_token", return_value=("test", False))
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance")
    def test_validate_aad_function_error(self, mock_valid_inst, mock_access, mock_put):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = True
        db_val_obj.validate_aad({"auth_type": "AAD", "aad_client_id": "cl_id", "aad_tenant_id": "tenant_id", "aad_client_secret": "client_secret", "databricks_instance": "db_instance"})
        mock_put.assert_called_once_with("test")
        self.assertEqual(mock_valid_inst.call_count, 0)
    
    @patch("databricks_validators.utils.get_proxy_uri", return_value=None)
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("requests.get", return_value=Response(500))
    @patch("databricks_common_utils.get_user_agent")
    def test_validate_instance_false(self, mock_user_agent, mock_get, mock_put, mock_proxy):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj.current_user = "current_user"
        ret_val = db_val_obj.validate_db_instance("instance", "token")
        mock_put.assert_called_once_with("Internal server error. Cannot verify Databricks instance.")
        self.assertEqual(ret_val, False)

    # =========================================================================
    # OAuth M2M Validation Tests
    # =========================================================================

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_oauth")
    def test_validate_oauth_auth_type(self, mock_oauth, mock_validator, mock_conf, mock_proxy, mock_session):
        """Test that validate() calls validate_oauth for OAUTH_M2M auth type."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        mock_oauth.return_value = True
        db_val_obj.validate("OAUTH_M2M", {
            "auth_type": "OAUTH_M2M", 
            "oauth_client_id": "client_id", 
            "oauth_client_secret": "client_secret",
            "databricks_instance": "test.databricks.azure.net"
        })
        self.assertEqual(mock_oauth.call_count, 1)

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    def test_validate_oauth_client_id_error(self, mock_put, mock_conf, mock_proxy, mock_session):
        """Test validation error when OAuth client ID is missing."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("OAUTH_M2M", {"auth_type": "OAUTH_M2M"})
        mock_put.assert_called_once_with("Field OAuth Client ID is required")

    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    def test_validate_oauth_client_secret_error(self, mock_put, mock_conf, mock_proxy, mock_session):
        """Test validation error when OAuth client secret is missing."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj.validate("OAUTH_M2M", {"auth_type": "OAUTH_M2M", "oauth_client_id": "client_id"})
        mock_put.assert_called_once_with("Field OAuth Client Secret is required")

    @patch("databricks_validators.utils.get_oauth_access_token", return_value=("access_token", 3600))
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_oauth_function_success(self, mock_valid_inst, mock_validator, mock_access):
        """Test successful OAuth M2M validation flow."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = True
        
        data = {
            "auth_type": "OAUTH_M2M", 
            "oauth_client_id": "cl_id", 
            "oauth_client_secret": "cl_secret", 
            "databricks_instance": "db_instance",
            "name": "test_account"
        }
        result = db_val_obj.validate_oauth(data)
        
        mock_valid_inst.assert_called_once_with(db_val_obj, "db_instance", "access_token")
        self.assertTrue(result)
        self.assertEqual(data["oauth_access_token"], "access_token")
        self.assertEqual(data["databricks_pat"], "")
        self.assertEqual(data["aad_access_token"], "")

    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("databricks_validators.utils.get_oauth_access_token", return_value=("Token retrieval failed", False))
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance")
    def test_validate_oauth_function_token_error(self, mock_valid_inst, mock_access, mock_put):
        """Test OAuth validation when token retrieval fails."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._proxy_settings = {}
        
        data = {
            "auth_type": "OAUTH_M2M", 
            "oauth_client_id": "cl_id", 
            "oauth_client_secret": "cl_secret", 
            "databricks_instance": "db_instance",
            "name": "test_account"
        }
        result = db_val_obj.validate_oauth(data)
        
        mock_put.assert_called_once_with("Token retrieval failed")
        self.assertEqual(mock_valid_inst.call_count, 0)
        self.assertFalse(result)

    @patch("databricks_validators.utils.get_oauth_access_token", return_value=("access_token", 3600))
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_oauth_function_instance_validation_failure(self, mock_valid_inst, mock_validator, mock_access):
        """Test OAuth validation when instance validation fails."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = False
        
        data = {
            "auth_type": "OAUTH_M2M", 
            "oauth_client_id": "cl_id", 
            "oauth_client_secret": "cl_secret", 
            "databricks_instance": "db_instance",
            "name": "test_account"
        }
        result = db_val_obj.validate_oauth(data)
        
        mock_valid_inst.assert_called_once()
        self.assertFalse(result)

    # =========================================================================
    # Additional AAD Validation Tests  
    # =========================================================================

    @patch("databricks_validators.utils.get_aad_access_token", return_value=("access_token", 3600))
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_aad_function_instance_failure(self, mock_valid_inst, mock_validator, mock_access):
        """Test AAD validation when instance validation fails."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = False
        
        result = db_val_obj.validate_aad({
            "auth_type": "AAD", 
            "aad_client_id": "cl_id", 
            "aad_tenant_id": "tenant_id", 
            "aad_client_secret": "client_secret", 
            "databricks_instance": "db_instance",
            "name": "test_account"
        })
        
        mock_valid_inst.assert_called_once()
        self.assertFalse(result)

    # =========================================================================
    # Proxy Configuration Tests
    # =========================================================================

    @patch("databricks_validators.utils.get_proxy_uri")
    @patch("databricks_validators.utils.get_current_user", return_value="test_user")
    @patch("requests.get")
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    def test_validate_db_instance_with_proxy_skip_oauth(self, mock_put, mock_get, mock_user, mock_proxy):
        """Test instance validation skips proxy when use_for_oauth is set."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        # Proxy settings with use_for_oauth set to skip proxy
        mock_proxy.return_value = {"use_for_oauth": "1", "http": "http://proxy:8080"}
        mock_get.return_value = Response(200, {"clusters": []})
        
        result = db_val_obj.validate_db_instance("instance", "token")
        
        # Verify proxy was skipped (set to None)
        self.assertTrue(result)
        self.assertIsNone(db_val_obj._proxy_settings)

    @patch("databricks_validators.utils.get_proxy_uri")
    @patch("databricks_validators.utils.get_current_user", return_value="test_user")
    @patch("requests.get")
    def test_validate_db_instance_with_proxy_enabled(self, mock_get, mock_user, mock_proxy):
        """Test instance validation uses proxy when configured."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        # Proxy settings with use_for_oauth=0 means use proxy
        mock_proxy.return_value = {"use_for_oauth": "0", "http": "http://proxy:8080"}
        mock_get.return_value = Response(200, {"clusters": []})
        
        result = db_val_obj.validate_db_instance("instance", "token")
        
        self.assertTrue(result)
        # Verify proxy is configured (use_for_oauth key removed)
        self.assertEqual(db_val_obj._proxy_settings, {"http": "http://proxy:8080"})

    @patch("databricks_validators.utils.get_proxy_uri", return_value=None)
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("requests.get", return_value=Response(403))
    @patch("databricks_common_utils.get_user_agent")
    def test_validate_instance_invalid_token(self, mock_user_agent, mock_get, mock_put, mock_proxy):
        """Test instance validation with invalid access token (403 response)."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        ret_val = db_val_obj.validate_db_instance("instance", "invalid_token")
        
        mock_put.assert_called_once_with("Invalid access token. Please enter the valid access token.")
        self.assertFalse(ret_val)

    @patch("databricks_validators.utils.get_proxy_uri", return_value=None)
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("requests.get", return_value=Response(404))
    @patch("databricks_common_utils.get_user_agent")
    def test_validate_instance_not_found(self, mock_user_agent, mock_get, mock_put, mock_proxy):
        """Test instance validation with 404 response."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        ret_val = db_val_obj.validate_db_instance("instance", "token")
        
        mock_put.assert_called_once_with("Please validate the provided details.")
        self.assertFalse(ret_val)

    @patch("databricks_validators.utils.get_proxy_uri", return_value=None)
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("requests.get", return_value=Response(400))
    @patch("databricks_common_utils.get_user_agent")
    def test_validate_instance_invalid_instance(self, mock_user_agent, mock_get, mock_put, mock_proxy):
        """Test instance validation with 400 response."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        ret_val = db_val_obj.validate_db_instance("instance", "token")
        
        mock_put.assert_called_once_with("Invalid Databricks instance.")
        self.assertFalse(ret_val)

    @patch("databricks_validators.utils.get_proxy_uri", return_value=None)
    @patch("databricks_validators.utils.get_current_user", return_value="test_user")
    @patch("requests.get")
    def test_validate_instance_success(self, mock_get, mock_user, mock_proxy):
        """Test successful instance validation."""
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        
        mock_get.return_value = Response(200, {"clusters": []})
        
        ret_val = db_val_obj.validate_db_instance("instance", "valid_token")
        
        self.assertTrue(ret_val)
