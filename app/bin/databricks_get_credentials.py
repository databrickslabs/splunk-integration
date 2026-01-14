"""This module contain class and method related to updating the finding state."""
import sys
import os
import json
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..')))

import splunk.rest as rest  # noqa: E402
import ta_databricks_declare  # noqa: E402 F401
import databricks_const as const  # noqa: E402
import traceback  # noqa: E402
from solnlib.credentials import CredentialManager  # noqa: E402
from splunk.persistconn.application import PersistentServerConnectionApplication  # noqa: E402
from log_manager import setup_logging  # noqa: E402

APP_NAME = const.APP_NAME
_LOGGER = setup_logging("ta_databricks_get_credentials")


class DatabricksGetCredentials(PersistentServerConnectionApplication):
    """Custom Encryption Handler."""

    def __init__(self, _command_line, _command_arg):
        """Initialize object with given parameters."""
        self.auth_type = None
        self.key = None
        self.proxy_key = None
        self.session_key = None
        self.admin_session_key = None
        self.payload = {}
        self.status = None
        super(PersistentServerConnectionApplication, self).__init__()

    # Handle a synchronous from splunkd.
    def handle(self, in_string):
        """
        For using any custom command, Called for a simple synchronous request.

        @param in_string: request data passed in
        @rtype: string or dict
        @return: String to return in response.  If a dict was passed in,
                 it will automatically be JSON encoded before being returned.
        """
        req_data = json.loads(in_string)
        self.admin_session_key = req_data.get('system_authtoken', None)
        form_data = dict(req_data.get("form"))
        self.account_name = form_data.get("name")

        # Saving Configurations
        if form_data.get('update_token'):
            try:
                # Determine which credentials to save based on what's provided
                if form_data.get("aad_access_token"):
                    _LOGGER.info("Saving databricks AAD access token.")
                    client_sec = form_data.get("aad_client_secret")
                    access_token = form_data.get("aad_access_token")
                    token_expiration = form_data.get("aad_token_expiration")
                    new_creds = json.dumps({
                        "aad_client_secret": client_sec,
                        "aad_access_token": access_token,
                        "aad_token_expiration": token_expiration
                    })
                    success_msg = 'Saved AAD access token successfully.'
                elif form_data.get("oauth_access_token"):
                    _LOGGER.info("Saving databricks OAuth access token.")
                    client_sec = form_data.get("oauth_client_secret")
                    access_token = form_data.get("oauth_access_token")
                    token_expiration = form_data.get("oauth_token_expiration")
                    new_creds = json.dumps({
                        "oauth_client_secret": client_sec,
                        "oauth_access_token": access_token,
                        "oauth_token_expiration": token_expiration
                    })
                    success_msg = 'Saved OAuth access token successfully.'
                else:
                    raise Exception("No token data provided for update.")

                manager = CredentialManager(
                    self.admin_session_key,
                    app=APP_NAME,
                    realm="__REST_CREDENTIAL__#{0}#{1}".format(APP_NAME, "configs/conf-ta_databricks_account"),
                )
                manager.set_password(self.account_name, new_creds)
                _LOGGER.info(success_msg)
                return {
                    'payload': success_msg,
                    'status': 200
                }
            except Exception as e:
                error_msg = "Databricks Error: Exception while saving access token: {}".format(str(e))
                _LOGGER.error(error_msg)
                _LOGGER.debug(traceback.format_exc())
                return {
                    'payload': error_msg,
                    'status': 500
                }

        # Retrieve Configurations
        config_dict = {
            'databricks_instance': None,
            'aad_client_id': None,
            'aad_tenant_id': None,
            'aad_client_secret': None,
            'aad_access_token': None,
            'aad_token_expiration': None,
            'oauth_client_id': None,
            'oauth_client_secret': None,
            'oauth_access_token': None,
            'oauth_token_expiration': None,
            'config_for_dbquery': None,
            'cluster_name': None,
            'warehouse_id': None,
            'databricks_pat': None,
            'auth_type': None,
            'proxy_enabled': None,
            'proxy_type': None,
            'proxy_url': None,
            'proxy_port': None,
            'proxy_username': None,
            'proxy_password': None,
            'proxy_rdns': None,
            'use_for_oauth': None,
            'admin_command_timeout': None,
            'query_result_limit': None,
            'index': None,
            'thread_count': None
        }
        try:
            _LOGGER.info("Retrieving account and settings configurations.")

            # Get account settings from conf
            _, account_response_content = rest.simpleRequest(
                "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_account/{}".format(
                    self.account_name
                ),
                sessionKey=self.admin_session_key,
                getargs={"output_mode": "json"},
                raiseAllErrors=True,
            )
            account_config_json = json.loads(account_response_content)
            account_config = account_config_json.get("entry")[0].get("content")
            _LOGGER.debug("Account configurations read successfully from account.conf .")

            config_dict['auth_type'] = account_config.get('auth_type')
            config_dict['databricks_instance'] = account_config.get('databricks_instance')
            config_dict['config_for_dbquery'] = account_config.get('config_for_dbquery')
            config_dict['cluster_name'] = account_config.get('cluster_name')
            config_dict['warehouse_id'] = account_config.get('warehouse_id')

            # Get clear account password from passwords.conf
            account_manager = CredentialManager(
                self.admin_session_key,
                app=APP_NAME,
                realm="__REST_CREDENTIAL__#{}#configs/conf-ta_databricks_account".format(APP_NAME),
            )
            account_password = json.loads(account_manager.get_password(self.account_name))
            _LOGGER.debug("Clear account password read successfully from passwords.conf.")

            if config_dict['auth_type'] == 'PAT':
                config_dict['databricks_pat'] = account_password.get('databricks_pat')
            elif config_dict['auth_type'] == 'AAD':
                config_dict['aad_client_id'] = account_config.get('aad_client_id')
                config_dict['aad_tenant_id'] = account_config.get('aad_tenant_id')
                config_dict['aad_client_secret'] = account_password.get('aad_client_secret')
                config_dict['aad_access_token'] = account_password.get('aad_access_token')
                config_dict['aad_token_expiration'] = account_password.get('aad_token_expiration')
            elif config_dict['auth_type'] == 'OAUTH_M2M':
                config_dict['oauth_client_id'] = account_config.get('oauth_client_id')
                config_dict['oauth_client_secret'] = account_password.get('oauth_client_secret')
                config_dict['oauth_access_token'] = account_password.get('oauth_access_token')
                config_dict['oauth_token_expiration'] = account_password.get('oauth_token_expiration')

            # Get proxy settings from conf
            _, proxy_response_content = rest.simpleRequest(
                "/servicesNS/nobody/{}/TA_Databricks_settings/proxy".format(
                    APP_NAME
                ),
                sessionKey=self.admin_session_key,
                getargs={"output_mode": "json"},
                raiseAllErrors=True,
            )
            proxy_config_json = json.loads(proxy_response_content)
            proxy_config = proxy_config_json.get("entry")[0].get("content")
            _LOGGER.debug("Proxy configurations read successfully from settings.conf")

            if proxy_config.get('proxy_password'):
                # Get clear proxy password from passwords.conf
                proxy_manager = CredentialManager(
                    self.admin_session_key,
                    app=APP_NAME,
                    realm="__REST_CREDENTIAL__#{}#configs/conf-ta_databricks_settings".format(APP_NAME),
                )
                proxy_password = json.loads(proxy_manager.get_password('proxy'))
                config_dict['proxy_password'] = proxy_password.get('proxy_password')
                _LOGGER.debug("Clear proxy password read successfully from passwords.conf.")

            config_dict['proxy_enabled'] = proxy_config.get('proxy_enabled')
            config_dict['proxy_type'] = proxy_config.get('proxy_type')
            config_dict['proxy_url'] = proxy_config.get('proxy_url')
            config_dict['proxy_port'] = proxy_config.get('proxy_port')
            config_dict['proxy_username'] = proxy_config.get('proxy_username')
            config_dict['proxy_rdns'] = proxy_config.get('proxy_rdns')
            config_dict['use_for_oauth'] = proxy_config.get('use_for_oauth')

            # Get additional settings from conf
            _, additional_settings_response_content = rest.simpleRequest(
                "/servicesNS/nobody/{}/TA_Databricks_settings/additional_parameters".format(
                    APP_NAME
                ),
                sessionKey=self.admin_session_key,
                getargs={"output_mode": "json"},
                raiseAllErrors=True,
            )
            additional_settings_json = json.loads(additional_settings_response_content)
            additional_settings_config = additional_settings_json.get("entry")[0].get("content")
            _LOGGER.debug("Additional parameters configurations read successfully from settings.conf")
            config_dict['admin_command_timeout'] = additional_settings_config.get("admin_command_timeout")
            config_dict['query_result_limit'] = additional_settings_config.get("query_result_limit")
            config_dict['index'] = additional_settings_config.get("index")
            config_dict['thread_count'] = additional_settings_config.get("thread_count")

            self.status = 200
            return {
                'payload': config_dict,
                'status': self.status
            }
        except Exception:
            error_msg = "Databricks Error: Error occured while retrieving account and proxy configurations - {}".format(
                traceback.format_exc())
            _LOGGER.error(error_msg)
            return {
                'payload': error_msg,
                'status': 500
            }

    def handleStream(self, handle, in_string):
        """For future use."""
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """Virtual method which can be optionally overridden to receive a callback after the request completes."""
        pass
