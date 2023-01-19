import ta_databricks_declare
from databricks_validators import ValidateDatabricksInstance
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    SingleModel,
)
from splunktaucclib.rest_handler import admin_external, util
from splunk_aoblib.rest_migration import ConfigMigrationHandler
import splunk.rest as rest
from xml.etree import cElementTree as ET
from log_manager import setup_logging
import traceback

logger = setup_logging("ta_databricks_rh_account")

util.remove_http_proxy_env_vars()

class SessionKeyProvider(ConfigMigrationHandler):
    """
    Provides Splunk session key to custom validator.
    """

    def __init__(self):
        """
        Save session key in class instance.
        """
        self.session_key = self.getSessionKey()

class CustomConfigMigrationHandler(ConfigMigrationHandler):
    """
    Manage the Rest Handler for server.

    :param ConfigMigrationHandler: inhereting ConfigMigrationHandler
    """
    def handleCreate(self, confInfo):
        self.payload["name"] = self.callerArgs.id
        try:
            super().handleCreate(confInfo)
        except Exception as e:
            logger.info("Databricks Error: Error Occured while creating Databricks Account {}".format(
                e
            ))
            logger.debug("Databricks Error: Error Occured while creating Databricks Account {}".format(
                traceback.format_exc()
            ))
            raise Exception(e)
    
    def handleEdit(self, confInfo):
        self.payload["name"] = self.callerArgs.id
        self.payload["edit"] = "edit called"
        try:
            ConfigMigrationHandler.handleEdit(self, confInfo)
        except Exception as e:
            logger.info("Databricks Error: Error Occured while updating Databricks Account {}".format(
                e
            ))
            logger.debug("Databricks Error: Error Occured while updating Databricks Account {}".format(
                traceback.format_exc()
            ))
            raise Exception(e)
        
    def handleRemove(self, confInfo):
        
        try:
            response = rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_account/{}".format(self.callerArgs.id),
                    method='DELETE',
                    sessionKey=SessionKeyProvider().session_key,
                    raiseAllErrors=True,
                    rawResult=True,
                )
            if int(response[0].get("status")) == 403:
                raise Exception("Lack of 'databricks_admin' role for the current user." \
                    " Refer 'Provide Required Access' section in the Intro page." \
                    " Response Status Code - {}.".format(response[0].get("status")))
            elif int(response[0].get("status")) not in [200,201]:
                raise Exception("Something Went Wrong." \
                    " Failed to authorized server. Connection closed." \
                    " Response Status Code - {}.".format(response[0].get("status")))
        except Exception as e:
            logger.info("Databricks Error: Error Occured while deleting Databricks Account {}".format(
                e
            ))
            logger.debug("Databricks Error: Error Occured while deleting Databricks Account {}".format(
                traceback.format_exc()
            ))
            raise Exception(e)
        logger.info("{} account removed".format(self.callerArgs.id))
        
        try:    
            response = rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords/{}".format(self.callerArgs.id),
                    method='DELETE',
                    sessionKey=SessionKeyProvider().session_key,
                    raiseAllErrors=True,
                    rawResult=True,
                )
            
            if int(response[0].get("status")) == 403:
                raise Exception("Lack of 'databricks_admin' role for the current user." \
                    " Refer 'Provide Required Access' section in the Intro page." \
                    " Response Status Code - {}.".format(response[0].get("status")))
            elif int(response[0].get("status")) == 404:
                raise Exception("Password for this account already removed.")
            if int(response[0].get("status")) not in [200,201]:
                raise Exception("Something Went Wrong." \
                    " Failed to authorized server. Connection closed." \
                    " Response Status Code - {}.".format(response[0].get("status")))
        except Exception as e:
            logger.info("Databricks Error: Error Occured while deleting Databricks Password {}".format(
                e
            ))
            logger.debug("Databricks Error: Error Occured while deleting Databricks Password {}".format(
                traceback.format_exc()
            ))
            raise Exception(e)
        logger.info("password removed.")
fields = [
    field.RestField(
        'databricks_instance',
        required=True,
        encrypted=False,
        default='',
        validator=validator.String(
            min_len=0,
            max_len=500,
        )
    ),
    field.RestField(
        'auth_type',
        required=True,
        encrypted=False,
        default='',
        validator=ValidateDatabricksInstance()
    ),
    field.RestField(
        'aad_client_id',
        required=False,
        encrypted=False,
        default='',
        validator=validator.String(
            min_len=0,
            max_len=500,
        )
    ),
    field.RestField(
        'aad_tenant_id',
        required=False,
        encrypted=False,
        default='',
        validator=validator.String(
            min_len=0,
            max_len=500,
        )
    ),
    field.RestField(
        'aad_client_secret',
        required=False,
        encrypted=False,
        default='',
        validator=None
    ),
    field.RestField(
        'databricks_pat',
        required=False,
        encrypted=False,
        default='',
        validator=None
    ),
    field.RestField(
        'cluster_name',
        required=False,
        encrypted=False,
        default='',
        validator=validator.String(
            min_len=0,
            max_len=500,
        )
    ),
    field.RestField(
        'aad_access_token',
        required=False,
        encrypted=False
    )
]
model_databricks_credentials = RestModel(fields, name=None)

endpoint = SingleModel(
    'ta_databricks_account',
    model_databricks_credentials,
)

if __name__ == '__main__':
    admin_external.handle(
        endpoint,
        handler=CustomConfigMigrationHandler,
    )