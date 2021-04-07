import ta_databricks_declare  # noqa: F401
import requests
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunktaucclib.rest_handler.endpoint.validator import Validator
from splunk_aoblib.rest_migration import ConfigMigrationHandler

_LOGGER = setup_logging("databricks_validator")


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

    def validate(self, value, data):
        """
        Check if the given value is valid.

        :param value: value to validate.
        :param data: whole payload in request.
        :return True or False
        """
        _LOGGER.info("Initiating configuration validation.")
        _LOGGER.info("Reading proxy and user data.")

        # Get Splunk Session Key
        splunk_session_key = SessionKeyProvider().session_key

        # Get proxy settings information
        proxy_settings = utils.get_proxy_uri(splunk_session_key)

        # Set parameters
        databricks_instance = data.get("databricks_instance").strip("/")
        databricks_access_token = data.get("databricks_access_token")
        _LOGGER.info("Validating the provided configurations.")

        req_url = "{}{}{}".format(
            "https://", databricks_instance, const.CLUSTER_ENDPOINT
        )

        headers = {
            "Authorization": "Bearer {}".format(databricks_access_token),
            "Content-Type": "application/json",
            "User-Agent": const.USER_AGENT_CONST,
        }

        # Create Connection
        try:
            resp = requests.get(req_url, headers=headers, proxies=proxy_settings)
            resp.raise_for_status()
            _ = resp.json()
            _LOGGER.info("Configurations validated successfully.")
            return True
        except Exception as e:
            if "resp" in locals() and resp.status_code == 403:
                msg = "Invalid access token. Please enter the valid access token."
            elif "resp" in locals() and resp.status_code == 404:
                msg = "Please validate the provided details."
            elif "resp" in locals() and resp.status_code == 500:
                msg = "Internal server error. Cannot verify Databricks instance."
            else:
                msg = "Unable to request Databricks instance. "\
                      "Please validate the provided Databricks and "\
                      "Proxy configurations or check the network connectivity."

            _LOGGER.error(str(e))
            _LOGGER.error(msg)
            self.put_msg(msg)
            return False
