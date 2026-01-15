import ta_databricks_declare
from splunktaucclib.rest_handler.endpoint.validator import Validator
from databricks_common_utils import IndexMacroManager
from splunktaucclib.rest_handler.endpoint import (
    field,
    validator,
    RestModel,
    MultipleModel,
)
from splunktaucclib.rest_handler import admin_external, util
from splunk_aoblib.rest_migration import ConfigMigrationHandler
import os

util.remove_http_proxy_env_vars()

class ValidateThread(Validator):
    def validate(self, value, data):
        thread_count_value = data.get("thread_count")
        cpu_core = os.cpu_count()
        max_threads = 2 * int(cpu_core)
        if int(thread_count_value) > max_threads:
            self.put_msg(f'Suggested Value for Max Thread Count is within twice of CPU Count. CPU Count is {cpu_core}. Please enter a value equal to or lesser than {max_threads}.')
            return False
        return True

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
        encrypted=True,
        default=None,
        validator=validator.String(
            min_len=0,
            max_len=8192,
        )
    ),
    field.RestField(
        'proxy_rdns',
        required=False,
        encrypted=False,
        default=None,
        validator=None
    ),
    field.RestField(
        'use_for_oauth',
        required=False,
        encrypted=False,
        default=0,
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
        validator=None
    )
]
model_logging = RestModel(fields_logging, name='logging')

fields_additional_parameters = [
    field.RestField(
        'admin_command_timeout',
        required=True,
        default=300,
        validator=None
    ),
    field.RestField(
        'query_result_limit',
        required=True,
        default=10000,
        validator=None
    ),
    field.RestField(
        'index',
        required=True,
        default='main',
        encrypted=False,
        validator=IndexMacroManager()
    ),
    field.RestField(
        'thread_count',
        required=True,
        default=5,
        encrypted=False,
        validator=ValidateThread()
    )
]
model_additional_parameters = RestModel(fields_additional_parameters, name='additional_parameters')

endpoint = MultipleModel(
    'ta_databricks_settings',
    models=[
        model_proxy,
        model_logging,
        model_additional_parameters
    ],
)


if __name__ == '__main__':
    admin_external.handle(
        endpoint,
        handler=ConfigMigrationHandler,
    )
