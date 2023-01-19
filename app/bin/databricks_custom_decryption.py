"""This module contain class and method related to updating the finding state."""
import sys
import os
import json
import splunk.rest as rest
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..')))

import ta_databricks_declare  # noqa: E402 F401
import traceback  # noqa: E402
import base64  # noqa: E402
from splunk.persistconn.application import PersistentServerConnectionApplication  # noqa: E402
from log_manager import setup_logging  # noqa: E402
from Crypto.Cipher import AES  # noqa: E402

_LOGGER = setup_logging("ta_databrickscustom_decryption")


class DatabricksCustomDecryption(PersistentServerConnectionApplication):
    """Custom Encryption Handler."""

    def __init__(self, _command_line, _command_arg):
        """Initialize object with given parameters."""
        self.auth_type = None
        self.key = None
        self.proxy_key = None
        self.session_key = None
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
        # Retrieve configurations
        try:
            _LOGGER.debug("Performing custom decryption.")
            req_data = json.loads(in_string)
            session = dict(req_data.get("session"))
            self.session_key = session.get("authtoken")
            form_data = dict(req_data.get("form"))

            if form_data.get("name"):
                self.account_name = form_data.get("name")

                self.settings_configs = self.get_account_configs()
                self.auth_type = self.settings_configs.get("auth_type")

                _, response_content = rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords/{}"
                    .format(self.account_name),
                    sessionKey=self.session_key,
                    getargs={"output_mode": "json"},
                    raiseAllErrors=True
                )
                configs = json.loads(response_content)
                configs = configs.get("entry")[0].get("content")
                self.perform_config_decryption(configs)
            else:
                _, response_content = rest.simpleRequest(
                    "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_passwords/proxy_password",
                    sessionKey=self.session_key,
                    getargs={"output_mode": "json"},
                    raiseAllErrors=True
                )
                configs = json.loads(response_content)
                configs = configs.get("entry")[0].get("content")
                self.perform_proxy_decryption(configs)

            self.status = 200
            _LOGGER.debug("Databricks Custom Decryption is successful.")
            return {
                'payload': self.payload,
                'status': self.status
            }
        except Exception:
            error_msg = "Databricks Error : Error occured while performing custom decryption - {}".format(
                traceback.format_exc())
            _LOGGER.error(error_msg)
            return {
                'payload': error_msg,
                'status': 500
            }

    def get_account_configs(self):
        """Gets account information."""
        try:
            _, response_content = rest.simpleRequest(
                "/servicesNS/nobody/TA-Databricks/configs/conf-ta_databricks_account/{}"
                .format(self.account_name),
                sessionKey=self.session_key,
                getargs={"output_mode": "json"},
                raiseAllErrors=True
            )
            settings_configs = json.loads(response_content)
            return settings_configs.get("entry")[0].get("content")
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while fetching account information - {}".format(e))
            _LOGGER.debug("Databricks Error : Error occured while fetching account information - {}".format(
                traceback.format_exc()))

    def perform_config_decryption(self, configs):
        """Performing custom configuration decryption."""
        try:
            _LOGGER.debug("Peforming custom config decryption.")
            modified_key = configs.get("key")
            encoded_nonce = configs.get("nonce")

            # decode the key
            decoded_key = base64.b64decode(modified_key).decode()
            # original key
            self.key = ''.join(map(lambda x: chr(ord(x) - 1), decoded_key))

            # decode the nonce
            decoded_nonce = base64.b64decode(encoded_nonce.encode())

            decrypt_cipher = AES.new(self.key.encode(), AES.MODE_EAX, nonce=decoded_nonce)

            if self.auth_type == "PAT":
                self.decrypt_and_set_payload(configs, decrypt_cipher, "databricks_pat")
            else:
                self.decrypt_and_set_payload(configs, decrypt_cipher, "aad_client_secret")
                self.decrypt_and_set_payload(configs, decrypt_cipher, "aad_access_token")

        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing custom config decryption - {}".format(e))
            _LOGGER.debug("Databricks Error : Error occured while performing custom config decryption - {}".format(
                traceback.format_exc()))

    def perform_proxy_decryption(self, configs):
        """Performing custom proxy decryption."""
        try:
            _LOGGER.debug("Performing custom proxy decryption.")
            proxy_key = configs.get("proxy_key")
            proxy_nonce = configs.get("proxy_nonce")

            # decode the key
            decoded_proxy_key = base64.b64decode(proxy_key).decode()
            # decode the nonce
            decoded_proxy_nonce = base64.b64decode(proxy_nonce.encode())

            # original key
            self.proxy_key = ''.join(map(lambda x: chr(ord(x) - 1), decoded_proxy_key))

            decrypt_proxy_cipher = AES.new(self.proxy_key.encode(), AES.MODE_EAX, nonce=decoded_proxy_nonce)
            self.decrypt_and_set_payload(configs, decrypt_proxy_cipher, "proxy_password")

        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing custom proxy decryption - {}".format(e))
            _LOGGER.debug("Databricks Error : Error occured while performing custom proxy decryption - {}".format(
                traceback.format_exc()))

    def decrypt_and_set_payload(self, configs, decrypt_cipher, field):
        """Method to do base64 decode, peform decryption and set payload."""
        try:
            encrypted_value = base64.b64decode(configs.get(field).encode())
            # decrypt the key
            decrypted_value = decrypt_cipher.decrypt(encrypted_value)
            try:
                self.payload[field] = decrypted_value.decode()
            except UnicodeDecodeError:
                _LOGGER.debug("Performing str based decoding for {}.".format(field))
                self.payload[field] = str(decrypted_value)[2:-1]
        except Exception as e:
            _LOGGER.error("Databricks Error : Error occured while performing \
                base64 decoding, decryption and setting payload  - {}".format(e))
            _LOGGER.debug("Databricks Error : Error occured while performing \
                base64 decoding, decryption and setting payload - {}".format(traceback.format_exc()))

    def handleStream(self, handle, in_string):
        """For future use."""
        raise NotImplementedError("PersistentServerConnectionApplication.handleStream")

    def done(self):
        """Virtual method which can be optionally overridden to receive a callback after the request completes."""
        pass
