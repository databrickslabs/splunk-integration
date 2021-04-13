import ta_databricks_declare  # noqa: F401
import requests
import json
import traceback

import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging

_LOGGER = setup_logging("databricks_com")


def databricks_api(method, endpoint, splunk_session_key, data=None, args=None):
    """
    Common method to hit the API of Databricks instance.

    :param method: "get" or "post"
    :param endpoint: Endpoint to get the data from e.g. /api/1.0/sample/endpoint
    :param splunk_session_key: Session key of Splunk Instance
    :param data: Payload to be send over post call
    :param args: Arguments to be add into the url
    :return: response in the form of dictionary
    """
    # Get user configuration
    databricks_instance = utils.get_databricks_configs().get("databricks_instance")
    databricks_token = utils.get_databricks_clear_token(splunk_session_key)

    if not all([databricks_instance, databricks_token]):
        raise Exception("Addon is not configured. Navigate to addon's configuration page to configure the addon.")

    # Prepare URL
    request_url = "{}{}{}".format("https://", databricks_instance.strip("/"), endpoint)

    # Getting proxy settings
    proxy_settings = utils.get_proxy_uri(splunk_session_key)

    if proxy_settings:
        _LOGGER.info("Proxy is configured. Using proxy to execute the request.")

    headers = {
        "Authorization": "Bearer {}".format(databricks_token),
        "Content-Type": "application/json",
        "User-Agent": const.USER_AGENT_CONST,
    }

    try:

        if method.lower() == "get":
            _LOGGER.info("Executing REST call: {}.".format(request_url))
            response = requests.get(
                request_url, params=args, headers=headers, proxies=proxy_settings
            )
        elif method.lower() == "post":
            _LOGGER.info("Executing REST call: {} Payload: {}.".format(request_url, str(data)))
            response = requests.post(
                request_url,
                params=args,
                headers=headers,
                json=data,
                proxies=proxy_settings,
            )

        response.raise_for_status()

        return response.json()
    except Exception as e:
        msg = "Unable to request Databricks instance. "\
              "Please validate the provided Databricks and Proxy configurations or check the network connectivity."
        if "response" in locals():
            status_code_messages = {
                400: response.json().get("message", "Bad request. The request is malformed."),
                403: "Invalid access token. Please enter the valid access token.",
                404: "Invalid API endpoint.",
                429: "API limit exceeded. Please try again after some time.",
                500: response.json().get("error", "Internal server error."),
            }

            msg = status_code_messages.get(response.status_code, msg)

        _LOGGER.error(str(e))
        _LOGGER.error(traceback.format_exc())
        raise Exception(msg)


def get_cluster_id(splunk_session_key, cluster_name):
    """
    Method to get the cluster id on the basis of cluster name.

    :param splunk_session_key: Session Key of Splunk
    :param cluster_name: Name of the cluster to get ID of
    :return: return the ID of cluster in the form of String
    """
    cluster_id = None
    resp = databricks_api("get", const.CLUSTER_ENDPOINT, splunk_session_key)
    response = resp.get("clusters")

    for r in response:

        if r.get("cluster_name") == cluster_name:

            if r.get("state").lower() == "running":
                cluster_id = r.get("cluster_id")
                return cluster_id

            raise Exception(
                "Ensure that the cluster is in running state. Current cluster state is {}.".format(
                    r.get("state")
                )
            )
    raise Exception("No cluster found with name {}. Provide a valid cluster name.".format(cluster_name))
