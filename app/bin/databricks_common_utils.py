import ta_databricks_declare  # noqa: F401
import json
import requests
import databricks_const as const
from log_manager import setup_logging

import splunk.rest as rest
from six.moves.urllib.parse import quote
from splunk.clilib import cli_common as cli
from solnlib.credentials import CredentialManager, CredentialNotExistException
from solnlib.utils import is_true

_LOGGER = setup_logging("databricks_utils")
APP_NAME = const.APP_NAME


def get_databricks_configs():
    """
    Get configuration details from ta_databricks_settings.conf.

    :return: dictionary with Databricks fields and values
    """
    _LOGGER.info("Reading configuration file.")
    configs = cli.getConfStanza("ta_databricks_settings", "databricks_credentials")
    return configs


def get_databricks_clear_token(session_key):
    """
    Get unencrypted access token from passwords.conf.

    :return: Access token in clear text
    """
    _LOGGER.info("Reading Databricks personal access token.")
    manager = CredentialManager(
        session_key,
        app=APP_NAME,
        realm="__REST_CREDENTIAL__#{0}#{1}".format(APP_NAME, "configs/conf-ta_databricks_settings"),
    )

    access_token = None

    try:
        access_token = json.loads(manager.get_password("databricks_credentials")).get(
            "databricks_access_token"
        )
    except Exception as e:
        _LOGGER.error(str(e))

    return access_token


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
    except CredentialNotExistException:
        return None
    else:
        return json.loads(manager.get_password("proxy")).get("proxy_password")


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
