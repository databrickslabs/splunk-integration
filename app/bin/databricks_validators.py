import requests
import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

from splunktaucclib.rest_handler.endpoint.validator import Validator
from splunk_aoblib.rest_migration import ConfigMigrationHandler
import splunk.rest as rest
import traceback
import json

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


class LoggingValidator(Validator):
    """Logging Validator Class."""

    def validate(self, value, data):
        """Validates user role before saving log level."""
        try:
            self._splunk_session_key = SessionKeyProvider().session_key
            if not utils.check_user_roles(self._splunk_session_key, validate="validate_user"):
                self.put_msg("Lack of 'databricks_admin' role for the current user. \
                    Refer 'Provide Required Access' section in the Intro page")
                return False
            return True
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while validating user for saving log level {}".format(e))
            return False


class ProxyEncryption(Validator):
    """
    Proxy Encryption Class.
    """

    def validate(self, value, data):
        """
        Check if the given value is valid.

        :param value: value to validate.
        :param data: whole payload in request.
        :return True or False
        """
        try:
            self._splunk_session_key = SessionKeyProvider().session_key
            if not utils.check_user_roles(self._splunk_session_key, validate="validate_user"):
                self.put_msg("Lack of 'databricks_admin' role for the current user. \
                    Refer 'Provide Required Access' section in the Intro page")
                return False
            rest.simpleRequest(
                "/databricks_custom_encryption",
                method='POST',
                postargs=data,
                sessionKey=self._splunk_session_key,
                raiseAllErrors=True,
                rawResult=True
            )

            if data.get("proxy_password"):
                data["proxy_password"] = "*****"
            return True
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing custom proxy encryption {}".format(e))
            self.put_msg(e)
            return False


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
        _LOGGER.info('Obtaining Azure Active Directory access token')
        aad_client_id = data.get("aad_client_id").strip()
        client_sec = data.get("aad_client_secret").strip()
        aad_tenant_id = data.get("aad_tenant_id").strip()
        account_name = data.get("name")
        aad_access_token = utils.get_aad_access_token(
            self._splunk_session_key, account_name,
            self._proxy_settings, aad_tenant_id, aad_client_id, client_sec)
        if isinstance(aad_access_token, tuple):
            _LOGGER.error(aad_access_token[0])
            self.put_msg(aad_access_token[0])
            return False
        _LOGGER.info('Obtained Azure Active Directory access token Successfully.')
        databricks_instance = data.get("databricks_instance").strip("/")
        valid_instance = self.validate_db_instance(databricks_instance, aad_access_token)
        if valid_instance:
            data["aad_access_token"] = aad_access_token
            data["databricks_pat"] = ""
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
            "User-Agent": "{}".format(const.USER_AGENT_CONST)
        }
        _LOGGER.debug(
            "Request made to the Databricks from Splunk user: {}".format(
                utils.get_current_user(self._splunk_session_key)
            )
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
            _LOGGER.error("Databricks Error : SSL certificate validation failed. Please verify the SSL"
                          " certificate: {}".format(sslerror))
            _LOGGER.debug("Databricks Error : SSL certificate validation failed. Please verify the SSL"
                          " certificate: {}".format(traceback.format_exc()))
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

        # Check User role
        if not utils.check_user_roles(self._splunk_session_key, validate="validate_user"):
            self.put_msg("Lack of 'databricks_admin' role for the current user. \
                    Refer 'Provide Required Access' section in the Intro page")
            return False

        auth_type = data.get("auth_type")
        if auth_type == "PAT":
            if (not (data.get("databricks_pat", None)
                     and data.get("databricks_pat").strip())
                    ):
                self.put_msg('Field Databricks Access Token is required')
                return False
        else:
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
        _LOGGER.info("Reading proxy and user data.")
        try:
            self._proxy_settings = utils.get_proxy_uri(self._splunk_session_key)
        except Exception as e:
            if "_ssl.c" in str(e):
                self.put_msg("SSL certificate verification failed. Please add a valid SSL certificate.")
                _LOGGER.error("Databricks Error : SSL certificate validation failed. Please verify the SSL"
                              " certificate: {}".format(e))
                _LOGGER.debug("Databricks Error : SSL certificate validation failed. Please verify the SSL"
                              " certificate: {}".format(traceback.format_exc()))
                return False
            else:
                self.put_msg("Unexpected error occured. check *databricks*.log file for more detail. ")
                _LOGGER.error("Databricks Error : Unexpected error occured: {}".format(e))
                _LOGGER.debug("Databricks Error : Unexpected error occured: {}".format(traceback.format_exc()))
                return False
        if auth_type == "PAT":
            if self.validate_pat(data):
                return self.perform_encryption(data)
        else:
            if self.validate_aad(data):
                return self.perform_encryption(data)

    def perform_encryption(self, data):
        """
        Method to perform custom encryption if creds are valid.

        :param data: whole payload in request.
        :return True or False
        """
        try:
            mask = "*****"
            try:
                response = rest.simpleRequest(
                    "/databricks_custom_encryption",
                    method='POST',
                    postargs=data,
                    sessionKey=self._splunk_session_key,
                    raiseAllErrors=True,
                    rawResult=True
                )
                if int(response[0].get("status")) not in [200, 201]:
                    if int(response[0].get("status")) == 500 and json.loads(response[1]).get("error"):
                        raise Exception(json.loads(response[1]).get("error"))
                    else:
                        raise Exception("Something went wrong.")

            except Exception as e:
                _LOGGER.info("Databricks Error: Error Occured while encrypting Databricks Creds {}".format(
                    e
                ))
                _LOGGER.debug("Databricks Error: Error Occured while encrypting Databricks Creds {}".format(
                    traceback.format_exc()
                ))
                raise Exception(e)
            if data.get("databricks_pat"):
                data["databricks_pat"] = mask
            if data.get("aad_client_secret"):
                data["aad_client_secret"] = mask
            if data.get("aad_access_token"):
                data["aad_access_token"] = mask
            del data["name"]
            if data.get("edit"):
                del data["edit"]
            return True
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing custom encryption {}".format(e))
            self.put_msg(e)
            return False
