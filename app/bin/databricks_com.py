import ta_databricks_declare  # noqa: F401
import requests
import time
import traceback

import databricks_const as const
import databricks_common_utils as utils
from log_manager import setup_logging
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from solnlib.utils import is_true

_LOGGER = setup_logging("ta_databricks_com")

# Token refresh buffer in seconds (5 minutes)
TOKEN_REFRESH_BUFFER = 300


class DatabricksClient(object):
    """A class to establish connection with Databricks and get data using REST API."""

    def __init__(self, account_name, session_key):
        """Intialize DatabricksClient object to get data from Databricks platform.

        Args:
            session_key (object): Splunk session key
        """
        databricks_configs = utils.get_databricks_configs(session_key, account_name)
        if not databricks_configs:
            raise Exception(f"Account '{account_name}' not found. Please provide valid Databricks account.")
        self.account_name = account_name
        databricks_instance = databricks_configs.get("databricks_instance")
        self.auth_type = databricks_configs.get("auth_type")
        self.session_key = session_key
        self.session = self.get_requests_retry_session()
        self.session.proxies = databricks_configs.get("proxy_uri")
        if self.session.proxies:
            if is_true(self.session.proxies.get("use_for_oauth")):
                _LOGGER.info(
                    "Skipping the usage of proxy for running query as 'Use Proxy for OAuth' parameter is checked."
                )
                self.session.proxies = None
            else:
                self.session.proxies.pop("use_for_oauth")

        self.session.verify = const.VERIFY_SSL
        self.session.timeout = const.TIMEOUT
        if self.auth_type == "PAT":
            self.databricks_token = databricks_configs.get("databricks_pat")
        elif self.auth_type == "AAD":
            self.databricks_token = databricks_configs.get("aad_access_token")
            self.aad_client_id = databricks_configs.get("aad_client_id")
            self.aad_tenant_id = databricks_configs.get("aad_tenant_id")
            self.aad_client_secret = databricks_configs.get("aad_client_secret")
            aad_token_expiration_str = databricks_configs.get("aad_token_expiration")
            self.aad_token_expiration = float(aad_token_expiration_str) if aad_token_expiration_str else 0
        elif self.auth_type == "OAUTH_M2M":
            self.databricks_token = databricks_configs.get("oauth_access_token")
            self.oauth_client_id = databricks_configs.get("oauth_client_id")
            self.oauth_client_secret = databricks_configs.get("oauth_client_secret")
            oauth_token_expiration_str = databricks_configs.get("oauth_token_expiration")
            self.oauth_token_expiration = float(oauth_token_expiration_str) if oauth_token_expiration_str else 0

        if not all([databricks_instance, self.databricks_token]):
            raise Exception("Addon is not configured. Navigate to addon's configuration page to configure the addon.")
        self.databricks_instance_url = f"https://{databricks_instance.strip('/')}"
        self.request_headers = {
            "Authorization": f"Bearer {self.databricks_token}",
            "Content-Type": "application/json",
            "User-Agent": const.USER_AGENT_CONST,
        }
        _LOGGER.debug(
            f"Request made to the Databricks from Splunk user: {utils.get_current_user(self.session_key)}"
        )

        # Separate session to call external APIs
        self.external_session = self.get_requests_retry_session()
        self.external_session.proxies = self.session.proxies
        self.external_session.verify = self.session.verify and False
        # Setting timeout in session does not work but kept here for sake of
        # consistency. Reference: https://requests.readthedocs.io/en/latest/api/#sessionapi
        self.external_session.timeout = self.session.timeout

        # Set session headers with auth tokens
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
            allowed_methods=["POST", "GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _should_refresh_token(self, expiration_attr):
        """
        Check if token should be refreshed proactively.

        :param expiration_attr: Name of the expiration attribute to check
        :return: Boolean - True if token expires within TOKEN_REFRESH_BUFFER seconds
        """
        expiration = getattr(self, expiration_attr, None)
        if expiration is None or expiration == 0:
            return False
        return (expiration - time.time()) < TOKEN_REFRESH_BUFFER

    def should_refresh_aad_token(self):
        """Check if AAD token should be refreshed proactively."""
        return self._should_refresh_token('aad_token_expiration')

    def should_refresh_oauth_token(self):
        """Check if OAuth token should be refreshed proactively."""
        return self._should_refresh_token('oauth_token_expiration')

    def _is_token_expired_response(self, response):
        """
        Check if the API response indicates an expired token.

        Databricks API may return different status codes for expired tokens:
        - 403 Forbidden (legacy)
        - 401 Unauthorized with "Token is expired"
        - 400 Bad Request with "ExpiredJwtException"

        :param response: requests.Response object
        :return: Boolean - True if response indicates expired token
        """
        if response is None:
            return False

        status_code = response.status_code

        # 403 Forbidden - legacy expired token response
        if status_code == 403:
            return True

        # 401 Unauthorized - check for token expiry message
        if status_code == 401:
            try:
                response_text = response.text.lower()
                if "token is expired" in response_text or "expired" in response_text:
                    return True
            except Exception:
                pass
            return True  # Treat all 401s as potentially expired tokens

        # 400 Bad Request - check for ExpiredJwtException
        if status_code == 400:
            try:
                response_text = response.text.lower()
                if "expiredjwtexception" in response_text or "expired" in response_text:
                    return True
            except Exception:
                pass

        return False

    def _refresh_aad_token(self):
        """Refresh AAD access token and update session headers."""
        databricks_configs = utils.get_databricks_configs(self.session_key, self.account_name)
        proxy_config = databricks_configs.get("proxy_uri")

        result = utils.get_aad_access_token(
            self.session_key,
            self.account_name,
            self.aad_tenant_id,
            self.aad_client_id,
            self.aad_client_secret,
            proxy_config,
            retry=const.RETRIES,
            conf_update=True
        )

        if isinstance(result, tuple) and result[1] == False:
            raise Exception(result[0])

        access_token, expires_in = result
        self._update_token(access_token, expires_in, 'aad_token_expiration')

    def _refresh_oauth_token(self):
        """Refresh OAuth M2M access token and update session headers."""
        databricks_configs = utils.get_databricks_configs(self.session_key, self.account_name)
        proxy_config = databricks_configs.get("proxy_uri")

        result = utils.get_oauth_access_token(
            self.session_key,
            self.account_name,
            self.databricks_instance_url.replace("https://", ""),
            self.oauth_client_id,
            self.oauth_client_secret,
            proxy_config,
            retry=const.RETRIES,
            conf_update=True
        )

        if isinstance(result, tuple) and result[1] == False:
            raise Exception(result[0])

        access_token, expires_in = result
        self._update_token(access_token, expires_in, 'oauth_token_expiration')

    def _update_token(self, access_token, expires_in, expiration_attr):
        """Update token and session headers after refresh."""
        self.databricks_token = access_token
        setattr(self, expiration_attr, time.time() + expires_in)
        self.request_headers["Authorization"] = f"Bearer {self.databricks_token}"
        self.session.headers.update(self.request_headers)

    def databricks_api(self, method, endpoint, data=None, args=None):
        """
        Common method to hit the API of Databricks instance.

        :param method: "get" or "post"
        :param endpoint: Endpoint to get the data from e.g. /api/1.0/sample/endpoint
        :param data: Payload to be send over post call
        :param args: Arguments to be add into the url
        :return: response in the form of dictionary
        """
        # Proactive token refresh
        if self.auth_type == "AAD" and self.should_refresh_aad_token():
            _LOGGER.info("AAD token expiring soon, refreshing proactively.")
            self._refresh_aad_token()
        elif self.auth_type == "OAUTH_M2M" and self.should_refresh_oauth_token():
            _LOGGER.info("OAuth token expiring soon, refreshing proactively.")
            self._refresh_oauth_token()

        run_again = True
        request_url = f"{self.databricks_instance_url}{endpoint}"
        try:
            while True:
                if method.lower() == "get":
                    _LOGGER.info(f"Executing REST call: {endpoint}.")
                    response = self.session.get(request_url, params=args, timeout=self.session.timeout)
                elif method.lower() == "post":
                    _LOGGER.info(f"Executing REST call: {endpoint} Payload: {data}.")
                    response = self.session.post(request_url, params=args, json=data, timeout=self.session.timeout)
                status_code = response.status_code
                # Check if token is expired and needs refresh (handles 403, 401, 400 with expiry messages)
                if self._is_token_expired_response(response) and self.auth_type == "AAD" and run_again:
                    _LOGGER.info(f"Token expired (status: {status_code}). Refreshing AAD token.")
                    response = None
                    run_again = False
                    self._refresh_aad_token()
                elif self._is_token_expired_response(response) and self.auth_type == "OAUTH_M2M" and run_again:
                    _LOGGER.info(f"Token expired (status: {status_code}). Refreshing OAuth M2M token.")
                    response = None
                    run_again = False
                    self._refresh_oauth_token()
                elif status_code != 200:
                    response.raise_for_status()
                else:
                    break
            if "cancel" in endpoint:
                return response.json(), status_code
            else:
                return response.json()
        except Exception as e:
            msg = (
                "Unable to request Databricks instance. "
                "Please validate the provided Databricks and Proxy configurations or check the network connectivity."
            )
            if "response" in locals() and response is not None:
                status_code_messages = {
                    400: response.json().get("message", "Bad request. The request is malformed."),
                    401: "Unauthorized. Access token may be invalid or expired.",
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
        if response is None:
            raise Exception(f"No cluster found with name {cluster_name}. Provide a valid cluster name.")

        for r in response:
            if r.get("cluster_name") == cluster_name:
                if r.get("state").lower() in ["running", "resizing"]:
                    cluster_id = r.get("cluster_id")
                    return cluster_id

                raise Exception(
                    f"Ensure that the cluster is in running state. Current cluster state is {r.get('state')}."
                )
        raise Exception(f"No cluster found with name {cluster_name}. Provide a valid cluster name.")

    def external_api(self, method, url, data=None, args=None):
        """
        Common method to request data from external APIs.

        :param method: "get" or "post"
        :param url: URL to get the data from
        :param data: Payload to be send over post call
        :param args: Arguments to be add into the url
        :return: response in the form of dictionary
        """
        # Request arguments
        kwargs = {
            "timeout": self.external_session.timeout,
        }
        if args:
            kwargs["params"] = args
        if data:
            kwargs["json"] = data

        # Call APIs
        if method.lower() == "get":
            _LOGGER.info(f"Executing REST call: {url}.")
            response = self.external_session.get(url, **kwargs)
        elif method.lower() == "post":
            _LOGGER.info(f"Executing REST call: {url} Payload: {data}.")
            response = self.external_session.post(url, **kwargs)
        response.raise_for_status()

        return response.json()
