import requests
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunktaucclib.rest_handler.endpoint.validator import Validator
from splunk_aoblib.rest_migration import ConfigMigrationHandler
from solnlib.utils import is_true
import traceback

_LOGGER = setup_logging("ta_databricks_validator")


class SessionKeyProvider(ConfigMigrationHandler):
    """
    Provides Splunk session key to custom validator.
    """

    def __init__(self):
        """
        Save session key in class instance.
        """
        self.session_key = self.getSessionKey()


class ValidateDatabricksInstance(Validator):
    """
    Validator for Databricks instance and token.
    """

    def validate_pat(self, data):
        """
        Validation flow if the user opts for Personal Access Token.

        :param data: Dictionary containing values from configuration UI.
        :return: Boolean depending on the sucess of the connection
        """
        databricks_instance = data.get("databricks_instance").strip("/")
        databricks_pat = data.get("databricks_pat")
        return self.validate_db_instance(databricks_instance, databricks_pat)

    def validate_aad(self, data):
        """
        Validation flow if the user opts for Azure Active Directory.

        :param data: Dictionary containing values from configuration UI.
        :return: Boolean depending on the sucess of the connection
        """
        import time
        _LOGGER.info('Obtaining Azure Active Directory access token')
        aad_client_id = data.get("aad_client_id").strip()
        client_sec = data.get("aad_client_secret").strip()
        aad_tenant_id = data.get("aad_tenant_id").strip()
        account_name = data.get("name")
        result = utils.get_aad_access_token(
            self._splunk_session_key, account_name,
            aad_tenant_id, aad_client_id, client_sec, self._proxy_settings)

        if isinstance(result, tuple) and result[1] == False:
            _LOGGER.error(result[0])
            self.put_msg(result[0])
            return False

        access_token, expires_in = result
        _LOGGER.info('Obtained Azure Active Directory access token Successfully.')
        databricks_instance = data.get("databricks_instance").strip("/")
        valid_instance = self.validate_db_instance(databricks_instance, access_token)
        if valid_instance:
            data["aad_access_token"] = access_token
            data["aad_token_expiration"] = str(time.time() + expires_in)
            data["databricks_pat"] = ""
            return True
        else:
            return False

    def validate_oauth(self, data):
        """
        Validation flow if the user opts for OAuth M2M authentication.

        :param data: Dictionary containing values from configuration UI.
        :return: Boolean depending on the success of the connection
        """
        import time
        _LOGGER.info('Obtaining OAuth M2M access token')
        oauth_client_id = data.get("oauth_client_id").strip()
        oauth_client_secret = data.get("oauth_client_secret").strip()
        databricks_instance = data.get("databricks_instance").strip("/")
        account_name = data.get("name")

        result = utils.get_oauth_access_token(
            self._splunk_session_key,
            account_name,
            databricks_instance,
            oauth_client_id,
            oauth_client_secret,
            self._proxy_settings
        )

        if isinstance(result, tuple) and result[1] == False:
            _LOGGER.error(result[0])
            self.put_msg(result[0])
            return False

        access_token, expires_in = result
        _LOGGER.info('Obtained OAuth M2M access token successfully.')

        valid_instance = self.validate_db_instance(databricks_instance, access_token)
        if valid_instance:
            data["oauth_access_token"] = access_token
            data["oauth_token_expiration"] = str(time.time() + expires_in)
            data["databricks_pat"] = ""
            data["aad_access_token"] = ""
            return True
        else:
            return False

    def validate_db_instance(self, instance_url, access_token):
        """
        Method to validate databricks instance.

        :param instance_url: Databricks instance
        :param access_token: AAD access token | Personal access token
        """
        _LOGGER.info('Validating Databricks instance')
        req_url = f"https://{instance_url}{const.CLUSTER_ENDPOINT}"
        self._proxy_settings = utils.get_proxy_uri(self._splunk_session_key)
        if self._proxy_settings:
            if is_true(self._proxy_settings.get("use_for_oauth")):
                _LOGGER.info(
                    "Skipping the usage of proxy for validation as 'Use Proxy for OAuth' parameter is checked."
                )
                self._proxy_settings = None
            else:
                self._proxy_settings.pop("use_for_oauth")

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "User-Agent": const.USER_AGENT_CONST
        }
        _LOGGER.debug(
            f"Request made to the Databricks from Splunk user: {utils.get_current_user(self._splunk_session_key)}"
        )
        try:
            resp = requests.get(
                req_url,
                headers=headers,
                proxies=self._proxy_settings,
                verify=const.VERIFY_SSL,
                timeout=const.TIMEOUT
            )
            resp.raise_for_status()
            _ = resp.json()
            _LOGGER.info('Validated Databricks instance sucessfully.')
            return True
        except requests.exceptions.SSLError as sslerror:
            self.put_msg("SSL certificate validation failed. Please verify the SSL certificate.")
            _LOGGER.error(f"Databricks Error: SSL certificate validation failed. Please verify the SSL certificate: {sslerror}")
            _LOGGER.debug(f"Databricks Error: SSL certificate validation failed. Please verify the SSL certificate: {traceback.format_exc()}")
            return False
        except Exception as e:
            if "resp" in locals() and resp.status_code == 403:
                msg = "Invalid access token. Please enter the valid access token."
            elif "resp" in locals() and resp.status_code == 404:
                msg = "Please validate the provided details."
            elif "resp" in locals() and resp.status_code == 500:
                msg = "Internal server error. Cannot verify Databricks instance."
            elif "resp" in locals() and resp.status_code == 400:
                msg = "Invalid Databricks instance."
            elif "_ssl.c" in str(e):
                msg = "SSL certificate verification failed. Please add a valid " \
                    "SSL certificate."
            else:
                msg = "Unable to request Databricks instance. "\
                    "Please validate the provided Databricks and "\
                    "Proxy configurations or check the network connectivity."
            _LOGGER.error(str(e))
            _LOGGER.error(msg)
            self.put_msg(msg)
            return False

    def validate(self, value, data):
        """
        Check if the given value is valid.

        :param value: value to validate.
        :param data: whole payload in request.
        :return True or False
        """
        _LOGGER.info("Initiating configuration validation.")
        self._splunk_session_key = SessionKeyProvider().session_key

        auth_type = data.get("auth_type")
        if auth_type == "PAT":
            if (not (data.get("databricks_pat", None)
                     and data.get("databricks_pat").strip())
                    ):
                self.put_msg('Field Databricks Access Token is required')
                return False
        elif auth_type == "AAD":
            if (not (data.get("aad_client_id", None)
                     and data.get("aad_client_id").strip())
                    ):
                self.put_msg('Field Client Id is required')
                return False
            elif (not (data.get("aad_tenant_id", None)
                  and data.get("aad_tenant_id").strip())
                  ):
                self.put_msg('Field Tenant Id is required')
                return False
            elif (not (data.get("aad_client_secret", None)
                  and data.get("aad_client_secret").strip())
                  ):
                self.put_msg('Field Client Secret is required')
                return False
        elif auth_type == "OAUTH_M2M":
            if (not (data.get("oauth_client_id", None)
                     and data.get("oauth_client_id").strip())
                    ):
                self.put_msg('Field OAuth Client ID is required')
                return False
            elif (not (data.get("oauth_client_secret", None)
                  and data.get("oauth_client_secret").strip())
                  ):
                self.put_msg('Field OAuth Client Secret is required')
                return False
        _LOGGER.info("Reading proxy and user data.")
        try:
            self._proxy_settings = utils.get_proxy_uri(self._splunk_session_key)
        except Exception as e:
            if "_ssl.c" in str(e):
                self.put_msg("SSL certificate verification failed. Please add a valid SSL certificate.")
                _LOGGER.error(f"Databricks Error: SSL certificate validation failed. Please verify the SSL certificate: {e}")
                _LOGGER.debug(f"Databricks Error: SSL certificate validation failed. Please verify the SSL certificate: {traceback.format_exc()}")
                return False
            else:
                self.put_msg("Unexpected error occurred. Check *databricks*.log file for more details.")
                _LOGGER.error(f"Databricks Error: Unexpected error occurred: {e}")
                _LOGGER.debug(f"Databricks Error: Unexpected error occurred: {traceback.format_exc()}")
                return False
        if auth_type == "PAT":
            return self.validate_pat(data)
        elif auth_type == "AAD":
            return self.validate_aad(data)
        elif auth_type == "OAUTH_M2M":
            return self.validate_oauth(data)