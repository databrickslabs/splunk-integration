import ta_databricks_declare  # noqa: F401
import requests
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunktaucclib.rest_handler.endpoint.validator import Validator
from splunk_aoblib.rest_migration import ConfigMigrationHandler

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
        databricks_access_token = data.get("databricks_access_token")
        return self.validate_db_instance(databricks_instance, databricks_access_token)

    def validate_aad(self, data):
        """
        Validation flow if the user opts for Azure Active Directory.

        :param data: Dictionary containing values from configuration UI.
        :return: Boolean depending on the sucess of the connection
        """
        _LOGGER.info('Obtaining Azure Active Directory access token')
        client_id = data.get("client_id").strip()
        client_sec = data.get("client_secret").strip()
        tenant_id = data.get("tenant_id").strip()
        access_token = utils.get_aad_access_token(
            self._splunk_session_key, const.USER_AGENT_CONST,
            self._proxy_settings, tenant_id, client_id, client_sec)
        if isinstance(access_token, tuple):
            _LOGGER.error(access_token[0])
            self.put_msg(access_token[0])
            return False
        _LOGGER.info('Obtained Azure Active Directory access token Successfully.')
        databricks_instance = data.get("databricks_instance").strip("/")
        valid_instance = self.validate_db_instance(databricks_instance, access_token)
        if valid_instance:
            data["access_token"] = access_token
            data["databricks_access_token"] = ""
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
        req_url = "{}{}{}".format(
            "https://", instance_url, const.CLUSTER_ENDPOINT
        )

        headers = {
            "Authorization": "Bearer {}".format(access_token),
            "Content-Type": "application/json",
            "User-Agent": const.USER_AGENT_CONST,
        }
        try:
            resp = requests.get(req_url, headers=headers, proxies=self._proxy_settings, verify=const.VERIFY_SSL)
            resp.raise_for_status()
            _ = resp.json()
            _LOGGER.info('Validated Databricks instance sucessfully.')
            return True
        except Exception as e:
            if "resp" in locals() and resp.status_code == 403:
                msg = "Invalid access token. Please enter the valid access token."
            elif "resp" in locals() and resp.status_code == 404:
                msg = "Please validate the provided details."
            elif "resp" in locals() and resp.status_code == 500:
                msg = "Internal server error. Cannot verify Databricks instance."
            elif "resp" in locals() and resp.status_code == 400:
                msg = "Invalid Databricks instance."
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
        auth_type = data.get("auth_type")
        if auth_type == "PAT":
            if (not (data.get("databricks_access_token", None)
                     and data.get("databricks_access_token").strip())
                    ):
                self.put_msg('Field Databricks Access Token is required')
                return False
        else:
            if (not (data.get("client_id", None)
                     and data.get("client_id").strip())
                    ):
                self.put_msg('Field Client Id is required')
                return False
            elif (not (data.get("tenant_id", None)
                  and data.get("tenant_id").strip())
                  ):
                self.put_msg('Field Tenant Id is required')
                return False
            elif (not (data.get("client_secret", None)
                  and data.get("client_secret").strip())
                  ):
                self.put_msg('Field Client Secret is required')
                return False
        _LOGGER.info("Reading proxy and user data.")
        self._splunk_session_key = SessionKeyProvider().session_key
        self._proxy_settings = utils.get_proxy_uri(self._splunk_session_key)
        if auth_type == "PAT":
            return self.validate_pat(data)
        else:
            return self.validate_aad(data)
