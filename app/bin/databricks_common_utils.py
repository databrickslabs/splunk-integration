import ta_databricks_declare  # noqa: F401
import json
import requests
import os
import traceback
import re
from urllib.parse import urlencode
import databricks_const as const
from log_manager import setup_logging, log_exception

import splunklib.client as client_
from splunktaucclib.rest_handler.endpoint.validator import Validator
from splunktaucclib.rest_handler.endpoint import (
    validator
)
import splunk.admin as admin
import splunk.clilib.cli_common
import splunk.rest as rest
from six.moves.urllib.parse import quote
from splunklib.binding import HTTPError
from solnlib.utils import is_true
from solnlib.credentials import CredentialManager, CredentialNotExistException
import splunklib.results as results
import splunklib.client as client

_LOGGER = setup_logging("ta_databricks_utils")
APP_NAME = const.APP_NAME


def build_proxy_uri(proxy_settings):
    """
    Build proxy URI from settings dict.

    :param proxy_settings: Dict with proxy_url, proxy_port, proxy_username,
                          proxy_password, proxy_type, proxy_enabled keys
    :return: Dict with http/https URIs and use_for_oauth flag, or None if disabled
    """
    if not proxy_settings:
        return None

    if not all([
        is_true(proxy_settings.get("proxy_enabled")),
        proxy_settings.get("proxy_url"),
        proxy_settings.get("proxy_type"),
    ]):
        return None

    http_uri = proxy_settings["proxy_url"]

    if proxy_settings.get("proxy_port"):
        http_uri = f"{http_uri}:{proxy_settings['proxy_port']}"

    if proxy_settings.get("proxy_username") and proxy_settings.get("proxy_password"):
        http_uri = "{}:{}@{}".format(
            quote(proxy_settings["proxy_username"], safe=""),
            quote(proxy_settings["proxy_password"], safe=""),
            http_uri,
        )

    http_uri = f"{proxy_settings['proxy_type']}://{http_uri}"

    return {
        "http": http_uri,
        "https": http_uri,
        "use_for_oauth": proxy_settings.get("use_for_oauth")
    }


def get_databricks_configs(session_key, account_name):
    """
    Get configuration details from ta_databricks_settings.conf.

    :return: dictionary with Databricks fields and values
    """
    _LOGGER.info("Reading configuration file.")
    configs_dict = None
    value = {"name": account_name}
    try:
        _, response_content = rest.simpleRequest(
            "/databricks_get_credentials",
            sessionKey=session_key,
            postargs=value,
            raiseAllErrors=True,
        )
        configs_dict = json.loads(response_content)

        # Setting proxy uri using consolidated builder
        proxy_uri = build_proxy_uri(configs_dict)
        if proxy_uri:
            configs_dict["proxy_uri"] = proxy_uri

    except Exception as e:
        log_exception(_LOGGER, "Databricks Error: Error occurred while fetching databricks account and proxy configs", e)
    return configs_dict


def _save_databricks_token(account_name, session_key, token_type, access_token, client_secret, expires_in=None):
    """
    Store access token with expiration timestamp.

    :param account_name: Account name
    :param session_key: Splunk session key
    :param token_type: 'aad' or 'oauth'
    :param access_token: Access token
    :param client_secret: Client secret
    :param expires_in: Token lifetime in seconds (optional, defaults to 3600)
    :return: None
    """
    import time
    if expires_in is None:
        expires_in = 3600  # Default to 1 hour if not provided

    # Preserve original display names for log messages
    display_names = {'aad': 'AAD', 'oauth': 'OAuth'}
    token_display_name = display_names.get(token_type, token_type)

    if token_type == 'aad':
        new_creds = {
            "name": account_name,
            "aad_client_secret": client_secret,
            "aad_access_token": access_token,
            "aad_token_expiration": str(time.time() + expires_in),
            "update_token": True
        }
    elif token_type == 'oauth':
        new_creds = {
            "name": account_name,
            "oauth_client_secret": client_secret,
            "oauth_access_token": access_token,
            "oauth_token_expiration": str(time.time() + expires_in),
            "update_token": True
        }
    else:
        raise ValueError(f"Unknown token_type: {token_type}")

    try:
        _LOGGER.info(f"Saving databricks {token_display_name} access token.")
        rest.simpleRequest(
            "/databricks_get_credentials",
            sessionKey=session_key,
            postargs=new_creds,
            raiseAllErrors=True,
        )
        _LOGGER.info(f"Saved {token_display_name} access token successfully.")
    except Exception as e:
        _LOGGER.error(f"Exception while saving {token_display_name} access token: {str(e)}")
        _LOGGER.debug(traceback.format_exc())
        raise Exception(f"Exception while saving {token_display_name} access token.")


def save_databricks_aad_access_token(account_name, session_key, access_token, client_sec, expires_in=None):
    """Store new AAD access token with expiration timestamp."""
    _save_databricks_token(account_name, session_key, 'aad', access_token, client_sec, expires_in)


def save_databricks_oauth_access_token(account_name, session_key, access_token, expires_in, client_secret):
    """Store new OAuth access token with expiration timestamp."""
    _save_databricks_token(account_name, session_key, 'oauth', access_token, client_secret, expires_in)


def get_oauth_access_token(
    session_key,
    account_name,
    databricks_instance,
    oauth_client_id,
    oauth_client_secret,
    proxy_settings=None,
    retry=1,
    conf_update=False,
):
    """
    Method to acquire OAuth M2M access token for Databricks service principal.

    :param session_key: Splunk session key
    :param account_name: Account name for configuration storage
    :param databricks_instance: Databricks workspace instance URL
    :param oauth_client_id: OAuth client ID from service principal
    :param oauth_client_secret: OAuth client secret from service principal
    :param proxy_settings: Proxy configuration dict
    :param retry: Number of retry attempts
    :param conf_update: If True, store token in configuration
    :return: tuple (access_token, expires_in) or (error_message, False)
    """
    import time
    from requests.auth import HTTPBasicAuth

    token_url = f"https://{databricks_instance}/oidc/v1/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": const.USER_AGENT_CONST,
    }
    _LOGGER.debug(f"Request made to the Databricks from Splunk user: {get_current_user(session_key)}")
    data_dict = {"grant_type": "client_credentials", "scope": "all-apis"}
    data_encoded = urlencode(data_dict)

    # Handle proxy settings for OAuth M2M
    # Note: "use_for_oauth" means "use proxy ONLY for AAD token generation"
    # Since OAuth M2M endpoint is on the Databricks instance (not AAD),
    # we should skip proxy when use_for_oauth is true
    if proxy_settings:
        if is_true(proxy_settings.get("use_for_oauth")):
            _LOGGER.info(
                "Skipping the usage of proxy for OAuth M2M as 'Use Proxy for OAuth' parameter is checked."
            )
            proxy_settings_copy = None
        else:
            proxy_settings_copy = proxy_settings.copy()
            proxy_settings_copy.pop("use_for_oauth", None)
    else:
        proxy_settings_copy = None

    while retry:
        try:
            resp = requests.post(
                token_url,
                headers=headers,
                data=data_encoded,
                auth=HTTPBasicAuth(oauth_client_id, oauth_client_secret),
                proxies=proxy_settings_copy,
                verify=const.VERIFY_SSL,
                timeout=const.TIMEOUT
            )
            resp.raise_for_status()
            response = resp.json()
            oauth_access_token = response.get("access_token")
            expires_in = response.get("expires_in", 3600)
            if conf_update:
                save_databricks_oauth_access_token(
                    account_name, session_key, oauth_access_token, expires_in, oauth_client_secret
                )
            return oauth_access_token, expires_in
        except Exception as e:
            retry -= 1
            if "resp" in locals():
                error_code = resp.json().get("error")
                if error_code and error_code in list(const.ERROR_CODE.keys()):
                    msg = const.ERROR_CODE[error_code]
                elif str(resp.status_code) in list(const.ERROR_CODE.keys()):
                    msg = const.ERROR_CODE[str(resp.status_code)]
                elif resp.status_code not in (200, 201):
                    msg = (
                        f"Response status: {resp.status_code}. Unable to validate OAuth credentials. "
                        "Check logs for more details."
                    )
            else:
                msg = (
                    "Unable to request Databricks instance. "
                    "Please validate the provided Databricks and "
                    "Proxy configurations or check the network connectivity."
                )
                _LOGGER.error(f"Error while trying to generate OAuth access token: {e}")
                _LOGGER.debug(traceback.format_exc())
            _LOGGER.error(msg)
            if retry == 0:
                return msg, False


def get_proxy_clear_password(session_key):
    """
    Get clear password from splunk passwords.conf.

    :return: str/None: proxy password if available else None.
    """
    try:
        manager = CredentialManager(
            session_key,
            app=APP_NAME,
            realm=f"__REST_CREDENTIAL__#{APP_NAME}#configs/conf-ta_databricks_settings",
        )
        return json.loads(manager.get_password("proxy")).get("proxy_password")
    except CredentialNotExistException:
        return None


def get_proxy_configuration(session_key):
    """
    Get proxy configuration settings.

    :return: proxy configuration dict.
    """
    rest_endpoint = f"/servicesNS/nobody/{APP_NAME}/TA_Databricks_settings/proxy"

    _, content = rest.simpleRequest(
        rest_endpoint,
        sessionKey=session_key,
        method="GET",
        getargs={"output_mode": "json"},
        raiseAllErrors=True,
    )

    return json.loads(content)["entry"][0]["content"]


def get_proxy_uri(session_key):
    """
    Generate proxy uri from provided configurations.

    :param session_key: Splunk Session Key
    :return: if proxy configuration available returns uri dict else None.
    """
    _LOGGER.info("Reading proxy configurations from file.")

    proxy_settings = get_proxy_configuration(session_key)

    if proxy_settings.get("proxy_username"):
        proxy_settings["proxy_password"] = get_proxy_clear_password(session_key)

    proxy_data = build_proxy_uri(proxy_settings)

    if proxy_data:
        _LOGGER.info("Proxy is enabled. Returning proxy configurations.")
    else:
        _LOGGER.info("Proxy is disabled. Skipping proxy mechanism.")

    return proxy_data


def format_to_json_parameters(params):
    """
    Split the provided string by `||` and make dictionary of that splitted key-value pair string.

    :params: String in the form of "key1=val1||key2=val2"
    :return: dictionary created on the basis of given string
    """
    output_json = {}

    try:
        if params:
            lst = params.split("||")
            for item in lst:
                kv = item.split("=")
                output_json[kv[0].strip()] = kv[1].strip()
    except Exception:
        raise Exception(
            "Invalid format for parameter notebook_params. Provide the value in 'param1=val1||param2=val2' format."
        )

    return output_json


def get_mgmt_port(session_key, logger):
    """Get Management Port."""
    try:
        _, content = rest.simpleRequest(
            "/services/configs/conf-web/settings",
            method="GET",
            sessionKey=session_key,
            getargs={"output_mode": "json"},
            raiseAllErrors=True,
        )
    except Exception as e:
        log_exception(logger, "Databricks Get Management Port Error: Error while making request to read web.conf file", e)
    # Parse Result
    try:
        content = json.loads(content)
        content = re.findall(r':(\d+)', content["entry"][0]["content"]["mgmtHostPort"])[0]
        logger.info("Databricks Info: Get management port from web.conf is {} ".format(content))
    except Exception as e:
        log_exception(logger, "Databricks Error: Error while parsing web.conf file", e)
    return content


def get_current_user(session_key):
    """Get current logged in user."""
    kwargs_oneshot = {"output_mode": "json"}
    searchquery_oneshot = (
        "| rest /services/authentication/current-context splunk_server=local | table username"
    )
    try:
        service = client.connect(port=get_mgmt_port(session_key, _LOGGER), token=session_key)
    except Exception as e:
        log_exception(_LOGGER, "Databricks Error: Error while connecting to splunklib client", e)

    try:
        oneshotsearch_results = service.jobs.oneshot(searchquery_oneshot, **kwargs_oneshot)

        # Get the results and display them using the JSONResultsReader
        reader = results.JSONResultsReader(oneshotsearch_results)
        for item in reader:
            if isinstance(item, dict) and item.get("username"):
                return item.get("username", None)
        raise Exception("No username found.")
    except Exception as e:
        log_exception(_LOGGER, "Databricks Error: Error while fetching logged in username", e)


def get_aad_access_token(
    session_key,
    account_name,
    aad_tenant_id,
    aad_client_id,
    aad_client_secret,
    proxy_settings=None,
    retry=1,
    conf_update=False,
):
    """
    Method to acquire a new AAD access token.

    :param session_key: Splunk session key
    :param account_name: Account name for configuration storage
    :param aad_tenant_id: Azure AD tenant ID
    :param aad_client_id: Azure AD client ID
    :param aad_client_secret: Azure AD client secret
    :param proxy_settings: Proxy configuration dict
    :param retry: Number of retry attempts
    :param conf_update: If True, store token in configuration
    :return: tuple (access_token, expires_in) or (error_message, False)
    """
    token_url = const.AAD_TOKEN_ENDPOINT.format(aad_tenant_id)
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": const.USER_AGENT_CONST,
    }
    _LOGGER.debug(f"Request made to the Databricks from Splunk user: {get_current_user(session_key)}")
    data_dict = {"grant_type": "client_credentials", "scope": const.SCOPE}

    data_dict["client_id"] = aad_client_id
    data_dict["client_secret"] = aad_client_secret
    data_encoded = urlencode(data_dict)

    if proxy_settings:
        proxy_settings.pop("use_for_oauth")

    while retry:
        try:
            resp = requests.post(
                token_url,
                headers=headers,
                data=data_encoded,
                proxies=proxy_settings,
                verify=const.VERIFY_SSL,
                timeout=const.TIMEOUT
            )
            resp.raise_for_status()
            response = resp.json()
            aad_access_token = response.get("access_token")
            expires_in = response.get("expires_in", 3600)  # Default to 1 hour
            if conf_update:
                save_databricks_aad_access_token(
                    account_name, session_key, aad_access_token, aad_client_secret, expires_in
                )
            return aad_access_token, expires_in
        except Exception as e:
            retry -= 1
            if "resp" in locals():
                error_code = resp.json().get("error_codes")
                if error_code:
                    error_code = str(error_code[0])
                if error_code in list(const.ERROR_CODE.keys()):
                    msg = const.ERROR_CODE[error_code]
                elif str(resp.status_code) in list(const.ERROR_CODE.keys()):
                    msg = const.ERROR_CODE[str(resp.status_code)]
                elif resp.status_code not in (200, 201):
                    msg = (
                        f"Response status: {resp.status_code}. Unable to validate Azure Active Directory Credentials. "
                        "Check logs for more details."
                    )
            else:
                msg = (
                    "Unable to request Databricks instance. "
                    "Please validate the provided Databricks and "
                    "Proxy configurations or check the network connectivity."
                )
                _LOGGER.error(f"Error while trying to generate AAD access token: {e}")
                _LOGGER.debug(traceback.format_exc())
            _LOGGER.error(msg)
            if retry == 0:
                return msg, False


def get_user_agent():
    """Method to get user agent."""
    return const.USER_AGENT_CONST


class GetSessionKey(admin.MConfigHandler):
    """To get Splunk session key."""

    def __init__(self):
        """Initialize."""
        self.session_key = self.getSessionKey()


def create_service(sessionkey=None):
    """Create Service to communicate with splunk."""
    mgmt_port = splunk.clilib.cli_common.getMgmtUri().split(":")[-1]
    if not sessionkey:
        sessionkey = GetSessionKey().session_key
    service = client.connect(port=mgmt_port, token=sessionkey, app=APP_NAME)
    return service


class IndexMacroManager(Validator):
    """Class provides methods for handling Macros."""

    def __init__(self, *args, **kwargs):
        """Initialize the parameters."""
        super(IndexMacroManager, self).__init__(*args, **kwargs)
        self._validator = validator
        self._args = args
        self._kwargs = kwargs
        self.path = os.path.abspath(__file__)

    def update_macros(self, service, macro_name, index_string):
        """Update macro with the selected index."""
        service.post(f"properties/macros/{macro_name}", definition=index_string)
        _LOGGER.info(f"Macro: {macro_name} is updated Successfully with definition: {index_string}.")

    def validate(self, value, data):
        """Update the macros with the selected index."""
        try:
            service = create_service()
            selected_index = data.get("index")
            response_string = f"index IN ({selected_index})"
            self.update_macros(service, "databricks_index_macro", response_string)
            return True
        except HTTPError:
            _LOGGER.error(f"Error while updating Macros: {traceback.format_exc()}")
            self.put_msg("Error while updating Macros. Kindly check log file for more details.")
            return False
        except Exception as e:
            msg = f"Unrecognized error: {e}"
            _LOGGER.error(msg)
            self.put_msg(msg)
            _LOGGER.error(traceback.format_exc())
            return False


def ingest_data_to_splunk(data, session_key, provided_index, sourcetype):
    """Method to ingest data to Splunk."""
    json_string = json.dumps(data, ensure_ascii=False).replace('"', '\\"')
    port = get_mgmt_port(session_key, _LOGGER)
    searchquery = f'| makeresults | eval _raw="{json_string}" | collect index={provided_index} sourcetype={sourcetype}'
    service = client_.connect(
        host="localhost",
        port=port,
        scheme="https",
        app=APP_NAME,
        token=session_key
    )
    service.jobs.oneshot(searchquery)
