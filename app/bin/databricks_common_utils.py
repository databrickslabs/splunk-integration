import ta_databricks_declare  # noqa: F401
import json
import requests
import traceback
from urllib.parse import urlencode
import databricks_const as const
from log_manager import setup_logging

import splunk.rest as rest
from six.moves.urllib.parse import quote
from splunk.clilib import cli_common as cli
from solnlib.credentials import CredentialManager, CredentialNotExistException
from solnlib.utils import is_true

_LOGGER = setup_logging("ta_databricks_utils")
APP_NAME = const.APP_NAME


def get_databricks_configs():
    """
    Get configuration details from ta_databricks_settings.conf.

    :return: dictionary with Databricks fields and values
    """
    _LOGGER.info("Reading configuration file.")
    configs = cli.getConfStanza("ta_databricks_settings", "databricks_credentials")
    return configs


def save_databricks_aad_access_token(session_key, access_token, client_sec):
    """
    Method to store new AAD access token.

    :return: None
    """
    try:
        _LOGGER.info("Saving databricks AAD access token.")
        manager = CredentialManager(
            session_key,
            app=APP_NAME,
            realm="__REST_CREDENTIAL__#{0}#{1}".format(APP_NAME, "configs/conf-ta_databricks_settings"),
        )
        new_creds = json.dumps({"client_secret": client_sec, "access_token": access_token})
        manager.set_password("databricks_credentials", new_creds)
        _LOGGER.info("Saved AAD access token successfully.")
    except Exception as e:
        _LOGGER.error("Exception while saving AAD access token: {}".format(str(e)))
        _LOGGER.debug(traceback.format_exc())
        raise Exception("Exception while saving AAD access token.")


def get_clear_token(session_key, auth_type):
    """
    Get either access token or personal access token.

    :return: Access token
    """
    access_token = None
    try:
        manager = CredentialManager(
            session_key,
            app=APP_NAME,
            realm="__REST_CREDENTIAL__#{0}#{1}".format(APP_NAME, "configs/conf-ta_databricks_settings"),
        )

        if auth_type == "AAD":
            access_token = json.loads(manager.get_password("databricks_credentials")).get(
                "access_token"
            )
        else:
            access_token = json.loads(manager.get_password("databricks_credentials")).get(
                "databricks_access_token"
            )
    except Exception as e:
        _LOGGER.error("Error while fetching Databricks instance access token: {}".format(str(e)))
        _LOGGER.debug(traceback.format_exc())

    return access_token


def get_clear_client_secret(session_key):
    """
    Get clear client secret from passwords.conf.

    :return: str/None: Client Secret | None.
    """
    client_secret = None
    try:
        manager = CredentialManager(
            session_key,
            app=APP_NAME,
            realm="__REST_CREDENTIAL__#{0}#{1}".format(APP_NAME, "configs/conf-ta_databricks_settings"),
        )

        client_secret = json.loads(manager.get_password("databricks_credentials")).get(
            "client_secret"
        )
    except Exception as e:
        _LOGGER.error("Error while fetching client secret: {}".format(str(e)))
        _LOGGER.debug(traceback.format_exc())

    return client_secret


def get_proxy_clear_password(session_key):
    """
    Get clear password from splunk passwords.conf.

    :return: str/None: proxy password if available else None.
    """
    try:
        manager = CredentialManager(
            session_key,
            app=APP_NAME,
            realm="__REST_CREDENTIAL__#{0}#{1}".format(
                APP_NAME, "configs/conf-ta_databricks_settings"
            ),
        )
        return json.loads(manager.get_password("proxy")).get("proxy_password")
    except CredentialNotExistException:
        return None


def get_proxy_configuration(session_key):
    """
    Get proxy configuration settings.

    :return: proxy configuration dict.
    """
    rest_endpoint = "/servicesNS/nobody/{}/TA_Databricks_settings/proxy".format(APP_NAME)

    _, content = rest.simpleRequest(
        rest_endpoint,
        sessionKey=session_key,
        method="GET",
        getargs={"output_mode": "json"},
        raiseAllErrors=True,
    )

    return json.loads(content)["entry"][0]["content"]


def get_proxy_uri(session_key, proxy_settings=None):
    """
    Generate proxy uri from provided configurations.

    :param session_key: Splunk Session Key
    :param proxy_settings: Proxy configuration dict. Defaults to None.
    :return: if proxy configuration available returns uri string else None.
    """
    _LOGGER.info("Reading proxy configurations from file.")

    if not proxy_settings:
        proxy_settings = get_proxy_configuration(session_key)

    if proxy_settings.get("proxy_username"):
        proxy_settings["proxy_password"] = get_proxy_clear_password(session_key)

    if all(
        [
            proxy_settings,
            is_true(proxy_settings.get("proxy_enabled")),
            proxy_settings.get("proxy_url"),
            proxy_settings.get("proxy_type"),
        ]
    ):
        http_uri = proxy_settings["proxy_url"]

        if proxy_settings.get("proxy_port"):
            http_uri = "{}:{}".format(http_uri, proxy_settings.get("proxy_port"))

        if proxy_settings.get("proxy_username") and proxy_settings.get(
            "proxy_password"
        ):
            http_uri = "{}:{}@{}".format(
                quote(proxy_settings["proxy_username"], safe=""),
                quote(proxy_settings["proxy_password"], safe=""),
                http_uri,
            )

        http_uri = "{}://{}".format(proxy_settings['proxy_type'], http_uri)

        proxy_data = {"http": http_uri, "https": http_uri}

        _LOGGER.info("Returning proxy configurations.")

        return proxy_data
    else:
        return None


def update_kv_store_collection(splunkd_uri, kv_collection_name, session_key, kv_log_info):
    """
    Create and update KV store collection.

    :param splunkd_uri: Splunk management URI
    :param kv_collection_name: KV Store collection to create/update
    :param session_key: Splunk Session Key
    :param kv_log_info: Information that needs to be updated
    :return: Dictionary with updated value of KV Store update status
    """
    header = {
        "Authorization": "Bearer {}".format(session_key),
        "Content-Type": "application/json",
    }

    # Add the log of record into the KV Store
    _LOGGER.info(
        "Adding the command log info to KV Store. Command Log Info: {}".format(kv_log_info)
    )

    kv_update_url = "{}/servicesNS/nobody/{}/storage/collections/data/{}".format(
        splunkd_uri,
        const.APP_NAME,
        kv_collection_name,
    )

    _LOGGER.info(
        "Executing REST call, URL: {}, Payload: {}.".format(kv_update_url, str(kv_log_info))
    )
    response = requests.post(
        kv_update_url, headers=header, data=json.dumps(kv_log_info), verify=False
    )

    if response.status_code in {200, 201}:
        _LOGGER.info("KV Store updated successfully.")
        kv_log_info.update({"kv_status": "KV Store updated successfully"})
    else:
        _LOGGER.info("Error occurred while updating KV Store.")
        kv_log_info.update({"kv_status": "Error occurred while updating KV Store"})

    return kv_log_info


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


def get_aad_access_token(session_key, user_agent, proxy_settings=None,
                         tenant_id=None, client_id=None, client_secret=None,
                         retry=1
):
    """
    Method to acquire a new AAD access token.

    :param session_key: Splunk session key
    :param user_agent: Splunk user agent
    :return: access token
    """
    tenant_id = get_databricks_configs().get("tenant_id") if not tenant_id else tenant_id
    token_url = const.AAD_TOKEN_ENDPOINT.format(tenant_id)
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'User-Agent': user_agent}
    data_dict = {"grant_type": "client_credentials",
                 "scope": const.SCOPE}
    client_id = get_databricks_configs().get("client_id").strip() if not client_id else client_id
    client_sec = get_clear_client_secret(session_key) if not client_secret else client_secret
    data_dict["client_id"] = client_id
    data_dict["client_secret"] = client_sec
    data_encoded = urlencode(data_dict)
    while retry:
        try:
            resp = requests.post(token_url, headers=headers, data=data_encoded,
                                 proxies=proxy_settings, verify=const.VERIFY_SSL)
            resp.raise_for_status()
            response = resp.json()
            access_token = response.get("access_token")
            if not all([tenant_id, client_id, client_secret]):
                save_databricks_aad_access_token(session_key, access_token, client_sec)
            return access_token
        except Exception as e:
            retry -= 1
            error_code = resp.json().get('error_codes')
            if error_code:
                error_code = str(error_code[0])
            if error_code == '700016':
                msg = 'Invalid Client ID provided.'
            elif error_code == '90002':
                msg = 'Invalid Tenant ID provided.'
            elif error_code == '7000215':
                msg = 'Invalid Client Secret provided.'
            elif resp.status_code == 403:
                msg = "Client secret may have expired. Please configure a valid Client secret."
            elif resp.status_code == 404:
                msg = "Invalid API endpoint."
            elif resp.status_code == 500:
                msg = "Internal server error."
            elif resp.status_code == 400:
                msg = resp.json().get("message", "Bad request. The request is malformed.")
            elif resp.status_code == 429:
                msg = "API limit exceeded. Please try again after some time.",
            elif resp.status_code not in (200, 201):
                msg = 'Response status: {}. Unable to validate Azure Active Directory Credentials.'\
                    'Check logs for more details.'.format(str(resp.status_code))
            else:
                msg = 'Unable to validate Azure Active Directory Credentials. Check logs for more details.'
                _LOGGER.error("Error while trying to generate AAD access token: {}".format(str(e)))
                _LOGGER.debug(traceback.format_exc())
            _LOGGER.error(msg)
            if retry == 0:
                return msg, False
