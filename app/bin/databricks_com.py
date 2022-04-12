import ta_databricks_declare  # noqa: F401
import requests
import traceback

import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

_LOGGER = setup_logging("ta_databricks_com")


class DatabricksClient(object):
    """A class to establish connection with Databricks and get data using REST API."""

    def __init__(self, session_key):
        """Intialize DatabricksClient object to get data from Databricks platform.

        Args:
            session_key (object): Splunk session key
        """
        databricks_configs = utils.get_databricks_configs()
        databricks_instance = databricks_configs.get("databricks_instance")
        self.auth_type = databricks_configs.get("auth_type")
        self.session_key = session_key
        self.session = self.get_requests_retry_session()
        self.session.proxies = utils.get_proxy_uri(session_key)
        self.session.verify = const.VERIFY_SSL
        self.databricks_token = utils.get_clear_token(self.session_key, self.auth_type)
        if not all([databricks_instance, self.databricks_token]):
            raise Exception("Addon is not configured. Navigate to addon's configuration page to configure the addon.")
        self.databricks_instance_url = "{}{}".format("https://", databricks_instance.strip("/"))
        self.request_headers = {
            "Authorization": "Bearer {}".format(self.databricks_token),
            "Content-Type": "application/json",
            "User-Agent": const.USER_AGENT_CONST
        }
        self.session.headers.update(self.request_headers)
        if self.session.proxies:
            _LOGGER.info("Proxy is configured. Using proxy to execute the request.")

    def get_requests_retry_session(self):
        """
        Create and return a session object with retry mechanism.

        :param retries: Maximum number of retries to attempt
        :param backoff_factor: Backoff factor used to calculate time between retries. e.g. For 10 - 5, 10, 20, 40,...
        :param status_forcelist: A tuple containing the response status codes that should trigger a retry.
        :param method_whiltelist: HTTP methods on which retry will be performed.

        :return: Session Object
        """
        session = requests.Session()
        retry = Retry(
            total=const.RETRIES,
            backoff_factor=const.BACKOFF_FACTOR,
            status_forcelist=const.STATUS_FORCELIST,
            method_whitelist=["POST", "GET"]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def databricks_api(self, method, endpoint, data=None, args=None):
        """
        Common method to hit the API of Databricks instance.

        :param method: "get" or "post"
        :param endpoint: Endpoint to get the data from e.g. /api/1.0/sample/endpoint
        :param data: Payload to be send over post call
        :param args: Arguments to be add into the url
        :return: response in the form of dictionary
        """
        run_again = True
        request_url = "{}{}".format(self.databricks_instance_url, endpoint)
        try:
            while True:
                if method.lower() == "get":
                    _LOGGER.info("Executing REST call: {}.".format(endpoint))
                    response = self.session.get(
                        request_url, params=args
                    )
                elif method.lower() == "post":
                    _LOGGER.info("Executing REST call: {} Payload: {}.".format(endpoint, str(data)))
                    response = self.session.post(
                        request_url,
                        params=args,
                        json=data
                    )
                status_code = response.status_code
                if status_code == 403 and self.auth_type == 'AAD' and run_again:
                    response = None
                    run_again = False
                    _LOGGER.info("Refreshing AAD token.")
                    db_token = utils.get_aad_access_token(
                        self.session_key, self.request_headers['User-Agent'], self.session.proxies, retry=const.RETRIES)
                    if isinstance(db_token, tuple):
                        raise Exception(db_token[0])
                    else:
                        self.databricks_token = db_token
                    self.request_headers["Authorization"] = "Bearer {}".format(self.databricks_token)
                    self.session.headers.update(self.request_headers)
                elif status_code != 200:
                    response.raise_for_status()
                else:
                    break
            return response.json()
        except Exception as e:
            msg = "Unable to request Databricks instance. "\
                "Please validate the provided Databricks and Proxy configurations or check the network connectivity."
            if "response" in locals() and response is not None:
                status_code_messages = {
                    400: response.json().get("message", "Bad request. The request is malformed."),
                    403: "Invalid access token. Please enter the valid access token.",
                    404: "Invalid API endpoint.",
                    429: "API limit exceeded. Please try again after some time.",
                    500: response.json().get("error", "Internal server error."),
                }

                msg = status_code_messages.get(response.status_code, msg)
            else:
                msg = str(e)
            _LOGGER.error(str(e))
            _LOGGER.error(traceback.format_exc())
            raise Exception(msg)

    def get_cluster_id(self, cluster_name):
        """
        Method to get the cluster id on the basis of cluster name.

        :param cluster_name: Name of the cluster to get ID of
        :return: return the ID of cluster in the form of String
        """
        cluster_id = None
        resp = self.databricks_api("get", const.CLUSTER_ENDPOINT)
        response = resp.get("clusters")

        for r in response:

            if r.get("cluster_name") == cluster_name:

                if r.get("state").lower() in ["running", "resizing"]:
                    cluster_id = r.get("cluster_id")
                    return cluster_id

                raise Exception(
                    "Ensure that the cluster is in running state. Current cluster state is {}.".format(
                        r.get("state")
                    )
                )
        raise Exception("No cluster found with name {}. Provide a valid cluster name.".format(cluster_name))
