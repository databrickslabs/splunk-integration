
from email.policy import default
import ta_databricks_declare
from databricks_validators import ValidateDatabricksInstance, ProxyEncryption, LoggingValidator
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    MultipleModel,
)
from splunktaucclib.rest_handler import admin_external, util
from splunk_aoblib.rest_migration import ConfigMigrationHandler

util.remove_http_proxy_env_vars()


fields_proxy = [
    field.RestField(
        'proxy_enabled',
        required=False,
        encrypted=False,
        default=None,
        validator=None
    ),
    field.RestField(
        'proxy_type',
        required=False,
        encrypted=False,
        default='http',
        validator=None
    ),
    field.RestField(
        'proxy_url',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            min_len=0,
            max_len=4096,
        )
    ),
    field.RestField(
        'proxy_port',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.Number(
            min_val=1,
            max_val=65535,
        )
    ),
    field.RestField(
        'proxy_username',
        required=False,
        encrypted=False,
        default=None,
        validator=validator.String(
            min_len=0,
            max_len=50,
        )
    ),
    field.RestField(
        'proxy_password',
        required=False,
        encrypted=False,
        default=None,
        validator=ProxyEncryption()
    ),
    field.RestField(
        'proxy_rdns',
        required=False,
        encrypted=False,
        default=None,
        validator=None
    )
]
model_proxy = RestModel(fields_proxy, name='proxy')


fields_logging = [
    field.RestField(
        'loglevel',
        required=False,
        encrypted=False,
        default='INFO',
        validator=LoggingValidator()
    )
]
model_logging = RestModel(fields_logging, name='logging')


fields_databricks_credentials = [
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
model_databricks_credentials = RestModel(fields_databricks_credentials, name='databricks_credentials')


endpoint = MultipleModel(
    'ta_databricks_settings',
    models=[
        model_proxy,
        model_logging
    ],
)


if __name__ == '__main__':
    admin_external.handle(
        endpoint,
        handler=ConfigMigrationHandler,
    )
