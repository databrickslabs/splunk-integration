"""
Shared pytest fixtures and test configuration for Databricks Add-on tests.

This module provides common mocking utilities and test fixtures to reduce
code duplication across test files.
"""
import pytest
from mock import patch, MagicMock


# =============================================================================
# Module Mock Lists - Predefined sets of modules to mock for different tests
# =============================================================================

# Core Splunk modules needed by most tests
CORE_SPLUNK_MODULES = [
    'log_manager',
    'splunk',
    'splunk.rest',
    'splunk.admin',
    'splunk.clilib',
    'splunk.clilib.cli_common',
]

# Modules for DatabricksClient and API tests
DATABRICKS_COM_MODULES = CORE_SPLUNK_MODULES + [
    'solnlib.server_info',
]

# Modules for validator tests
VALIDATOR_MODULES = CORE_SPLUNK_MODULES + [
    'solnlib.server_info',
    'splunk_aoblib',
    'splunk_aoblib.rest_migration',
]

# Modules for command/query tests
COMMAND_MODULES = CORE_SPLUNK_MODULES + [
    'solnlib.server_info',
    'splunk_aoblib',
    'splunk_aoblib.rest_migration',
]

# Modules for credentials handler tests
CREDENTIALS_MODULES = [
    'log_manager',
    'splunk',
    'splunk.persistconn.application',
    'splunk.rest',
]

# Modules for common utils tests
COMMON_UTILS_MODULES = CORE_SPLUNK_MODULES + [
    'splunklib.client',
    'splunklib.results',
]

# Modules for run status tests
RUN_STATUS_MODULES = CORE_SPLUNK_MODULES + [
    'solnlib',
    'solnlib.server_info',
    'solnlib.utils',
    'solnlib.credentials',
]

# Modules for alert tests
ALERT_MODULES = [
    'splunk',
    'splunk.rest',
    'splunk.clilib',
    'solnlib.server_info',
    'splunk_aoblib',
    'splunk_aoblib.rest_migration',
    'solnlib.splunkenv',
    'splunklib',
]


# =============================================================================
# Module Mocking Functions
# =============================================================================

def create_module_mocks(modules_to_mock, special_handlers=None):
    """
    Create MagicMock objects for a list of modules and patch sys.modules.
    
    Args:
        modules_to_mock: List of module names to mock
        special_handlers: Dict mapping module names to special setup functions
                         e.g., {'splunk.persistconn.application': setup_persistconn}
    
    Returns:
        Dict of mocked modules
    """
    mocked_modules = {module: MagicMock() for module in modules_to_mock}
    
    # Apply special handlers if provided
    if special_handlers:
        for module_name, handler in special_handlers.items():
            if module_name in mocked_modules:
                handler(mocked_modules[module_name])
    
    # Patch sys.modules
    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()
    
    return mocked_modules


def setup_persistconn_mock(mock_module):
    """Setup special mock for splunk.persistconn.application."""
    mock_module.PersistentServerConnectionApplication = object


def teardown_module_mocks():
    """Stop all patches - call in tearDownModule."""
    patch.stopall()


# =============================================================================
# Common Test Configurations
# =============================================================================

def get_pat_config(instance="123", token="token", proxy_uri=None):
    """Get a PAT authentication configuration dict."""
    return {
        "databricks_instance": instance,
        "auth_type": "PAT",
        "databricks_pat": token,
        "proxy_uri": proxy_uri,
    }


def get_aad_config(
    instance="123",
    token="token",
    client_id="client_id",
    tenant_id="tenant_id",
    client_secret="client_secret",
    token_expiration="9999999999.0",
    proxy_uri=None
):
    """Get an AAD authentication configuration dict."""
    return {
        "databricks_instance": instance,
        "auth_type": "AAD",
        "aad_access_token": token,
        "aad_client_id": client_id,
        "aad_tenant_id": tenant_id,
        "aad_client_secret": client_secret,
        "aad_token_expiration": token_expiration,
        "proxy_uri": proxy_uri,
    }


def get_oauth_config(
    instance="123",
    token="token",
    client_id="client_id",
    client_secret="client_secret",
    token_expiration="9999999999.0",
    proxy_uri=None
):
    """Get an OAuth M2M authentication configuration dict."""
    return {
        "databricks_instance": instance,
        "auth_type": "OAUTH_M2M",
        "oauth_access_token": token,
        "oauth_client_id": client_id,
        "oauth_client_secret": client_secret,
        "oauth_token_expiration": token_expiration,
        "proxy_uri": proxy_uri,
    }


def get_proxy_config(
    http="http://proxy:8080",
    https=None,
    use_for_oauth="0"
):
    """Get a proxy configuration dict."""
    config = {"http": http, "use_for_oauth": use_for_oauth}
    if https:
        config["https"] = https
    return config


# =============================================================================
# Common Test Data
# =============================================================================

CLUSTER_LIST = {
    "clusters": [
        {"cluster_name": "test1", "cluster_id": "123", "state": "running"},
        {"cluster_name": "test2", "cluster_id": "345", "state": "pending"},
    ]
}


# =============================================================================
# Pytest Fixtures (optional - for pytest-native tests)
# =============================================================================

@pytest.fixture
def mock_logger():
    """Fixture to provide a mocked logger."""
    return MagicMock()


@pytest.fixture
def pat_config():
    """Fixture for PAT configuration."""
    return get_pat_config()


@pytest.fixture
def aad_config():
    """Fixture for AAD configuration."""
    return get_aad_config()


@pytest.fixture
def oauth_config():
    """Fixture for OAuth M2M configuration."""
    return get_oauth_config()
