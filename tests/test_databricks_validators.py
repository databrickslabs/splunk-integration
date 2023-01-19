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
        'solnlib.server_info',
        'splunk_aoblib',
        'splunk_aoblib.rest_migration'
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestDatabricksUtils(unittest.TestCase):
    """Test Databricks Validators."""
    
    @patch("databricks_validators.Validator")
    @patch("splunk.rest.simpleRequest")
    def test_perform_encryption(self, mock_rest, mock_validator):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        mock_rest.return_value = [{"status":"200"},{"dummy":"dummy"}]
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        response = db_val_obj.perform_encryption({
            "databricks_access_token":"databricks_access_token",
            "client_secret":"client_secret",
            "access_token":"access_token",
            "name":"name",
            "edit":"edit"
        })
        self.assertEqual(response, True)
    
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_logging(self, mock_user, mock_seesion_key, mock_validator):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.LoggingValidator()
        response = db_val_obj.validate("log",{"log_level": "log_level"})
        self.assertEqual(response, True)
    
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_common_utils.check_user_roles")
    @patch("splunk.rest.simpleRequest")
    def test_validate_proxy(self, mock_rest, mock_user, mock_seesion_key, mock_validator):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ProxyEncryption()
        response = db_val_obj.validate("Proxy",{"proxy_password": "proxy_password"})
        self.assertEqual(response, True)
    
    @patch("databricks_validators.SessionKeyProvider", return_value=MagicMock())
    @patch("databricks_validators.utils.get_proxy_uri", return_value="{}")
    @patch("splunk_aoblib.rest_migration.ConfigMigrationHandler")
    @patch("databricks_validators.Validator")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_pat")
    @patch("databricks_validators.ValidateDatabricksInstance.validate_aad")
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_pat(self, mock_user, mock_aad, mock_pat, mock_validator, mock_conf, mock_proxy, mock_session):
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
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_pat_error(self, mock_user, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
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
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_aad(self,  mock_user_role, mock_aad, mock_pat, mock_validator,mock_conf, mock_proxy, mock_session):
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
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_aad_client_id_error(self, mock_user, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
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
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_aad_tenant_error(self, mock_user, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
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
    @patch("databricks_common_utils.check_user_roles")
    def test_validate_aad_client_secret_error(self, mock_user, mock_aad, mock_pat, mock_put, mock_conf, mock_proxy, mock_session):
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

    @patch("databricks_validators.utils.get_aad_access_token", return_value="access_token")
    @patch("databricks_validators.Validator", autospec=True)
    @patch("databricks_validators.ValidateDatabricksInstance.validate_db_instance", autospec=True)
    def test_validate_aad_function(self, mock_valid_inst, mock_validator, mock_access):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._proxy_settings = {}
        mock_valid_inst.return_value = True
        db_val_obj.validate_aad({"auth_type": "AAD", "aad_client_id": "cl_id", "aad_tenant_id": "tenant_id", "aad_client_secret": "client_secret", "databricks_instance": "db_instance"})
        mock_valid_inst.assert_called_once_with(db_val_obj, "db_instance", "access_token")
    
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
    
    @patch("databricks_validators.Validator.put_msg", return_value=MagicMock())
    @patch("requests.get", return_value=Response(500))
    @patch("databricks_common_utils.get_user_agent")
    def test_validate_instance_false(self, mock_user_agent, mock_get, mock_put):
        db_val = import_module('databricks_validators')
        db_val._LOGGER = MagicMock()
        db_val_obj = db_val.ValidateDatabricksInstance()
        db_val_obj._splunk_version = "splunk_version"
        db_val_obj._splunk_session_key = "session_key"
        db_val_obj._proxy_settings = {}
        db_val_obj.current_user = "current_user"
        ret_val = db_val_obj.validate_db_instance("instance", "token")
        mock_put.assert_called_once_with("Internal server error. Cannot verify Databricks instance.")
        self.assertEqual(ret_val, False)


    
    