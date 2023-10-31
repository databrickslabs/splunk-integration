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
        encrypted=True,
        default='',
        validator=None
    ),
    field.RestField(
        'databricks_pat',
        required=False,
        encrypted=True,
        default='',
        validator=None
    ),
    field.RestField(
        'config_for_dbquery',
        required=True,
        encrypted=False,
        default='dbsql',
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
        'warehouse_id',
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
        encrypted=True
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
        handler=ConfigMigrationHandler,
    )