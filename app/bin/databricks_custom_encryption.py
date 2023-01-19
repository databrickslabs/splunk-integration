"""This module contain class and method related to updating the finding state."""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..')))

import ta_databricks_declare  # noqa: E402 F401
import json  # noqa: E402
import traceback  # noqa: E402
import base64  # noqa: E402
import random  # noqa: E402
import string  # noqa: E402
from splunk.persistconn.application import PersistentServerConnectionApplication  # noqa: E402
import splunk.rest as rest  # noqa: E402
from log_manager import setup_logging  # noqa: E402
from Crypto.Cipher import AES  # noqa: E402

_LOGGER = setup_logging("ta_databrickscustom_encryption")


class DatabricksCustomEncryption(PersistentServerConnectionApplication):
    """Custom Encryption Handler."""

    def __init__(self, _command_line, _command_arg):
        """Initialize object with given parameters."""
        self.account_name = None
        self.auth_type = None
        self.edit = None
        self.aad_client_secret = None
        self.databricks_pat = None
        self.aad_access_token = None
        self.proxy_password = None
        self.payload = {}
        self.status = None
        self.session_key = None
        super(PersistentServerConnectionApplication, self).__init__()

    def handle(self, in_string):
        """
        After user clicks on Save, Called for a simple synchronous request.

        @param in_string: request data passed in
        @rtype: string or dict
        @return: String to return in response.  If a dict was passed in,
                 it will automatically be JSON encoded before being returned.
        """
        try:
            req_data = json.loads(in_string)
            session = dict(req_data.get("session"))
            self.session_key = session.get("authtoken")
            form_data = dict(req_data.get("form"))
            self.account_name = form_data.get("name")
            if form_data.get("edit"):
                self.edit = form_data.get("edit")
            if form_data.get("auth_type") or form_data.get("databricks_pat") or form_data.get("aad_client_secret"):
                self.auth_type = form_data.get("auth_type")

                if self.auth_type == "PAT" or form_data.get("databricks_pat"):
                    self.databricks_pat = form_data.get("databricks_pat")
                elif self.auth_type == "AAD" or form_data.get("aad_client_secret"):
                    self.aad_client_secret = form_data.get("aad_client_secret")
                    self.aad_access_token = form_data.get("aad_access_token")

                self.databricks_configuration_encrypt()

                self.payload["success"] = "Databricks Configuration Custom Encryption completed successfully."
                self.status = 200
            else:
                self.proxy_password = form_data.get("proxy_password")
                self.databricks_proxy_encrypt()

                self.payload["success"] = "Databricks Proxy Custom Encryption completed successfully."
                self.status = 200

        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing custom encryption {}".format(e))
            self.payload["error"] = "Databricks Error : Error occured while performing custom encryption - {}".format(e)
            self.status = 500

        return {'payload': self.payload,
                'status': self.status
            }

    def databricks_configuration_encrypt(self):
        """Peforms configuration custom encryption and creates a custom conf."""
        try:
            _LOGGER.debug("Performing configuration custom encryption.")

            # this key will be used for encryption
            key = ''.join(random.choices(string.ascii_uppercase
                          + string.digits, k=32))

            # modify the key
            modified_key = ''.join(map(lambda x: chr(ord(x) + 1), key))

            cipher = AES.new(key.encode(), AES.MODE_EAX)

            if self.auth_type == "PAT":
                # encrypted api key
                encrypted_pat_access_token = cipher.encrypt(self.databricks_pat.encode())

                # nonce is a random value generated each time we instantiate the cipher using new()
                # creating a dict to write in custom passwords conf file
                value = {
                    'name': self.account_name,
                    'key': base64.b64encode(modified_key.encode()).decode(),
                    'nonce': base64.b64encode(cipher.nonce).decode(),
                    'databricks_pat': base64.b64encode(encrypted_pat_access_token).decode()
                }
            else:
                encrypted_aad_client_secret = cipher.encrypt(self.aad_client_secret.encode())
                encrypted_aad_access_token = cipher.encrypt(self.aad_access_token.encode())

                # nonce is a random value generated each time we instantiate the cipher using new()
                # creating a dict to write in custom passwords conf file
                value = {
                    'name': self.account_name,
                    'key': base64.b64encode(modified_key.encode()).decode(),
                    'nonce': base64.b64encode(cipher.nonce).decode(),
                    'aad_client_secret': base64.b64encode(encrypted_aad_client_secret).decode(),
                    'aad_access_token': base64.b64encode(encrypted_aad_access_token).decode()
                }
            if not self.edit:
                rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords",
                    method='POST',
                    postargs=value,
                    sessionKey=self.session_key,
                    raiseAllErrors=True,
                    rawResult=True,
                )
            else:
                del value["name"]
                response = rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords/{}"
                    .format(self.account_name),
                    method='POST',
                    postargs=value,
                    sessionKey=self.session_key,
                    raiseAllErrors=True,
                    rawResult=True,
                )
                if int(response[0].get("status")) == 404:
                    raise Exception("Databricks Passwords Conf file not found.")
            _LOGGER.debug("Databricks Configuration Custom Encryption completed successfully.")
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while creating a custom conf file - {}".format(
                traceback.format_exc()))
            raise Exception(e)

    def databricks_proxy_encrypt(self):
        """Peforms proxy custom encryption and creates a custom conf."""
        try:
            _LOGGER.debug("Performing proxy custom encryption.")

            # this key will be used for encryption
            proxy_key = ''.join(random.choices(string.ascii_uppercase
                                + string.digits, k=32))

            # modify the key
            modified_proxy_key = ''.join(map(lambda x: chr(ord(x) + 1), proxy_key))

            proxy_cipher = AES.new(proxy_key.encode(), AES.MODE_EAX)

            # encrypted api key
            encrypted_proxy_password = proxy_cipher.encrypt(self.proxy_password.encode())
            # creating a dict to write in custom proxy conf file
            value = {
                'proxy_key': base64.b64encode(modified_proxy_key.encode()).decode(),
                'proxy_nonce': base64.b64encode(proxy_cipher.nonce).decode(),
                'proxy_password': base64.b64encode(encrypted_proxy_password).decode()
            }
            response = rest.simpleRequest(
                "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords/proxy_password",
                method='POST',
                postargs=value,
                sessionKey=self.session_key,
                raiseAllErrors=True
            )
            if int(response[0].get("status")) == 404:
                raise Exception("Databricks Passwords Conf file not found.")
            _LOGGER.debug("Databricks Proxy Custom Encryption completed successfully.")
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while creating a custom conf file - {}".format(
                traceback.format_exc()))
            raise Exception(e)

    def handleStream(self, handle, in_string):
        """For future use."""
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """Virtual method which can be optionally overridden to receive a callback after the request completes."""
        pass
