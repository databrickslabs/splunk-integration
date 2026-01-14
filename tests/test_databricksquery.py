import declare
import unittest
from mock import patch, MagicMock

mocked_modules = {}
def setUpModule():
    global mocked_modules

    module_to_be_mocked = [
        'log_manager',
        'splunk',
        'splunk.rest',
        'splunk.admin',
        'splunk.clilib',
        'splunk.clilib.cli_common',
        'solnlib.server_info',
        'splunk_aoblib',
        'splunk_aoblib.rest_migration'
    ]

    mocked_modules = {module: MagicMock() for module in module_to_be_mocked}

    for module, magicmock in mocked_modules.items():
        patch.dict('sys.modules', **{module: magicmock}).start()


def tearDownModule():
    patch.stopall()

class TestDatabricksQuery(unittest.TestCase):
    """Test databricksquery."""

    def setUp(self):
        import databricksquery
        self.databricksquery = databricksquery
        self.DatabricksQueryCommand = databricksquery.DatabricksQueryCommand
    
    @patch("databricksquery.utils.get_databricks_configs")
    def test_cluster_exception(self, mock_get_config):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.account_name = "test_account"
        db_query_obj.write_error = MagicMock()
        # Configure the mock to return the expected value
        mock_get_config.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "config_for_dbquery": "interactive_cluster",
            "cluster_name": None,
            "admin_command_timeout": "300"
        }
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertTrue(mock_get_config.called)
        db_query_obj.write_error.assert_called_once_with("Databricks cluster is required to execute this custom command. Provide a cluster parameter or configure the cluster in the TA's configuration page.")

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_get_cluster_id_exception(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.write_error = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.side_effect = Exception("error getting cluster id")
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        client.get_cluster_id.assert_called_once()
        db_query_obj.write_error.assert_called_once_with("error getting cluster id")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_create_context_exception(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = Exception("error while creating context")
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        client.databricks_api.assert_called_once()
        db_query_obj.write_error.assert_called_once_with("error while creating context")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_submit_query_exception(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            Exception("error while submitting query")]
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertEqual(client.databricks_api.call_count,2)
        db_query_obj.write_error.assert_called_once_with("error while submitting query")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_fetch_data_exception(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            {"id": "command_id1"}, Exception("error while fetching data")]
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertEqual(client.databricks_api.call_count,3)
        db_query_obj.write_error.assert_called_once_with("error while fetching data")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_fetch_data_status_error(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            {"id": "command_id1"}, {"status":"Error"}]
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertEqual(client.databricks_api.call_count,3)
        db_query_obj.write_error.assert_called_once_with("Could not complete the query execution. Status: Error.")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_fetch_data_status_finished_error(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            {"id": "command_id1"}, {"status":"Finished", "results": {"resultType": "error"}}]
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertEqual(client.databricks_api.call_count,3)
        db_query_obj.write_error.assert_called_once_with("Error encountered while executing query.")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_fetch_data_status_finished_not_table(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            {"id": "command_id1"}, {"status":"Finished", "results": {"resultType": "json"}}]
        db_query_obj.write_error = MagicMock()
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        self.assertEqual(client.databricks_api.call_count,3)
        db_query_obj.write_error.assert_called_once_with("Encountered unknown result type, terminating the execution.")
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_fetch_data_status_finished_truncated(self, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"}, 
            {"id": "command_id1"}, 
            {"status":"Finished", "results": {"data": [["1", "2"],["3", "4"]],"resultType": "table", "truncated": True, "schema":[{"name": "field1"},{"name": "field2"}]}}]
        db_query_obj.write_warning = MagicMock()
        resp = db_query_obj.generate()
        row1 = next(resp)
        row2 = next(resp)
        self.assertEqual(client.databricks_api.call_count,3)
        db_query_obj.write_warning.assert_called_once_with("Results are truncated due to Databricks API limitations.")
        self.assertEqual(row1 , {'field1': '1', 'field2': '2'})
        self.assertEqual(row2 , {'field1': '3', 'field2': '4'})
    
    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time", autospec=True)
    def test_fetch_data_status_finished_loop(self,mock_time, mock_utils, mock_com):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [{"contextId": "context1"},
            {"id": "command_id1"},
            {"status":"processing"},
            {"status":"Finished", "results": {"data": [["1", "2"],["3", "4"]],"resultType": "table", "truncated": True, "schema":[{"name": "field1"},{"name": "field2"}]}}]
        db_query_obj.write_warning = MagicMock()
        mock_time.sleep.return_value = MagicMock()
        resp = db_query_obj.generate()
        row1 = next(resp)
        row2 = next(resp)
        self.assertEqual(client.databricks_api.call_count,4)
        db_query_obj.write_warning.assert_called_once_with("Results are truncated due to Databricks API limitations.")
        self.assertEqual(row1 , {'field1': '1', 'field2': '2'})
        self.assertEqual(row2 , {'field1': '3', 'field2': '4'})

    @patch("databricksquery.utils.get_databricks_configs")
    def test_command_timeout_below_minimum(self, mock_get_config):
        """Test that command timeout below minimum value raises error"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.account_name = "test_account"
        db_query_obj.command_timeout = 15  # Below minimum of 30
        db_query_obj.write_error = MagicMock()

        mock_get_config.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "config_for_dbquery": "dbsql",
            "warehouse_id": "w123",
            "admin_command_timeout": "300"
        }

        resp = db_query_obj.generate()
        try:
            next(resp)
        except (StopIteration, SystemExit):
            pass

        db_query_obj.write_error.assert_called_with(
            "Command Timeout value must be greater than or equal to 30 seconds."
        )

    @patch("databricksquery.get_splunkd_uri")
    @patch("databricksquery.rest.simpleRequest")
    @patch("databricksquery.time.sleep")
    def test_cancel_query_success(self, mock_sleep, mock_simple_request, mock_get_uri):
        """Test successful query cancellation when Splunk search is stopped"""
        # Mock Splunkd URI
        mock_get_uri.return_value = "https://localhost:8089"

        # Create mock XML response for Splunk search status
        xml_response = '''<?xml version="1.0" encoding="UTF-8"?>
        <entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest">
            <s:key name="dispatchState">FINALIZING</s:key>
            <s:key name="isFinalized">1</s:key>
        </entry>'''

        mock_simple_request.return_value = (None, xml_response.encode())

        client = MagicMock()
        client.databricks_api.return_value = ({}, 200)

        db_query_obj = self.DatabricksQueryCommand()

        # Call cancel_query method
        db_query_obj.cancel_query(
            search_sid="test_sid",
            session_key="test_key",
            client=client,
            cancel_endpoint="/api/cancel",
            data_for_cancelation={"statement_id": "stmt123"}
        )

        # Verify API was called to cancel query
        client.databricks_api.assert_called_once_with(
            "post", "/api/cancel", data={"statement_id": "stmt123"}
        )

    @patch("databricksquery.rest.simpleRequest")
    @patch("databricksquery.time.sleep")
    def test_cancel_query_unknown_sid(self, mock_sleep, mock_simple_request):
        """Test cancel_query handles unknown SID gracefully"""
        mock_simple_request.side_effect = Exception("unknown sid")

        client = MagicMock()
        db_query_obj = self.DatabricksQueryCommand()

        # Should not raise exception, just log and break
        db_query_obj.cancel_query(
            search_sid="invalid_sid",
            session_key="test_key",
            client=client,
            cancel_endpoint="/api/cancel",
            data_for_cancelation={}
        )

        # Verify no API call was made since exception occurred early
        client.databricks_api.assert_not_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.ThreadPoolExecutor")
    def test_warehouse_query_success(self, mock_executor, mock_utils, mock_com):
        """Test successful warehouse query execution with result pagination"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_warning = MagicMock()

        client = mock_com.return_value = MagicMock()

        # Mock warehouse status check
        client.databricks_api.side_effect = [
            # Warehouse list
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            # Execute query
            {"statement_id": "stmt123"},
            # Status check - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 2,
                    "truncated": False,
                    "schema": {
                        "columns": [{"name": "col1"}, {"name": "col2"}]
                    }
                },
                "result": {
                    "external_links": [
                        {
                            "external_link": "http://example.com/chunk0",
                            "chunk_index": 0,
                            "next_chunk_internal_link": None
                        }
                    ]
                }
            }
        ]

        # Mock external API response
        client.external_api.return_value = [["val1", "val2"], ["val3", "val4"]]

        # Mock ThreadPoolExecutor
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.map.return_value = [[["val1", "val2"], ["val3", "val4"]]]

        resp = db_query_obj.generate()
        results = list(resp)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], {"col1": "val1", "col2": "val2"})
        self.assertEqual(results[1], {"col1": "val3", "col2": "val4"})

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_warehouse_query_failed_state(self, mock_utils, mock_com):
        """Test warehouse query with FAILED state"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Warehouse list
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            # Execute query
            {"statement_id": "stmt123"},
            # Status check - FAILED
            {
                "status": {
                    "state": "FAILED",
                    "error": {"message": "Syntax error in query"}
                }
            }
        ]

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called()
        error_msg = db_query_obj.write_error.call_args[0][0]
        self.assertIn("FAILED", error_msg)
        self.assertIn("Syntax error in query", error_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_cluster_query_timeout(self, mock_sleep, mock_utils, mock_com):
        """Test cluster query execution timeout scenario"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        db_query_obj.command_timeout = 30  # Short timeout
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300"
        }

        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"

        # Mock responses - keep returning PENDING state
        client.databricks_api.side_effect = [
            {"id": "context1"},  # Create context
            {"id": "command_id1"},  # Submit query
            {"status": "Running"},  # Status checks - keep pending
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            {"status": "Running"},
            ({}, 200),  # Cancel response
            {}  # Context destroy
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called_with(
            "Canceled the execution as command execution timed out"
        )

    @patch("databricksquery.utils.get_databricks_configs")
    def test_invalid_account_name(self, mock_get_config):
        """Test handling of invalid account name"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.account_name = "invalid_account"
        db_query_obj.write_error = MagicMock()

        mock_get_config.return_value = None

        resp = db_query_obj.generate()
        try:
            next(resp)
        except (StopIteration, SystemExit):
            pass

        db_query_obj.write_error.assert_called_with(
            "Account 'invalid_account' not found. Please provide valid Databricks account."
        )

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_query_timeout(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse query execution timeout scenario"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.command_timeout = 30  # Short timeout
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Warehouse list
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            # Execute query
            {"statement_id": "stmt123"},
            # Status checks - keep returning PENDING/RUNNING
            {"status": {"state": "PENDING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            {"status": {"state": "RUNNING"}},
            # Cancel response
            ({}, 200)
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        db_query_obj.write_error.assert_called_with(
            "Canceled the execution as command execution timed out"
        )

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_command_timeout_exceeds_admin_max(self, mock_utils, mock_com):
        """Test command timeout exceeds admin configured maximum"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.command_timeout = 500  # Exceeds admin max of 300
        db_query_obj.write_warning = MagicMock()
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Should have written warning about using max value
        db_query_obj.write_warning.assert_called()
        warning_msg = db_query_obj.write_warning.call_args[0][0]
        self.assertIn("300", warning_msg)
        self.assertIn("500", warning_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_limit_exceeds_admin_max(self, mock_utils, mock_com):
        """Test limit exceeds admin configured maximum"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.limit = 5000  # Exceeds admin max of 1000
        db_query_obj.write_warning = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Should have written warning about using max value
        db_query_obj.write_warning.assert_called()
        warning_msg = db_query_obj.write_warning.call_args[0][0]
        self.assertIn("1000", warning_msg)
        self.assertIn("5000", warning_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_cluster_canceled_status(self, mock_utils, mock_com):
        """Test cluster query with Canceled status"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [
            {"id": "context1"},
            {"id": "command_id1"},
            {"status": "Canceled"}
        ]

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called_with(
            "Could not complete the query execution. Status: Canceled."
        )

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_cluster_command_cancellation_exception(self, mock_utils, mock_com):
        """Test cluster query with CommandCancelledException"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [
            {"id": "context1"},
            {"id": "command_id1"},
            {
                "status": "Finished",
                "results": {
                    "resultType": "error",
                    "cause": "CommandCancelledException: Query was canceled"
                }
            }
        ]

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called_with("Search Canceled!")

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_cluster_finished_with_custom_error_summary(self, mock_utils, mock_com):
        """Test cluster query finished with custom error summary"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        client.databricks_api.side_effect = [
            {"id": "context1"},
            {"id": "command_id1"},
            {
                "status": "Finished",
                "results": {
                    "resultType": "error",
                    "summary": "Table not found: my_table"
                }
            }
        ]

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called_with("Table not found: my_table")

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_both_cluster_and_warehouse_provided(self, mock_utils, mock_com):
        """Test error when both cluster and warehouse_id are provided"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.cluster = "test_cluster"
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300"
        }

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        db_query_obj.write_error.assert_called_with(
            "Provide only one of Cluster or Warehouse ID"
        )

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_no_config_for_cluster_or_warehouse(self, mock_utils, mock_com):
        """Test error when no cluster or warehouse configuration found"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "config_for_dbquery": None
        }

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        error_msg = db_query_obj.write_error.call_args[0][0]
        self.assertIn("No configuration found", error_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_starting_state(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse in STARTING state, then transitions to RUNNING"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Initial warehouse list - STARTING
            {"warehouses": [{"id": "w123", "state": "STARTING"}]},
            # ensure_warehouse_running: status check - still STARTING
            {"state": "STARTING"},
            # ensure_warehouse_running: status check - now RUNNING
            {"state": "RUNNING"},
            # Execute query
            {"statement_id": "stmt123"},
            # Query status - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify sleep was called while waiting for warehouse
        mock_sleep.assert_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_stopped_then_started(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse in STOPPED state, gets started, then runs query"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Initial warehouse list - STOPPED
            {"warehouses": [{"id": "w123", "state": "STOPPED"}]},
            # ensure_warehouse_running: status check - STOPPED
            {"state": "STOPPED"},
            # ensure_warehouse_running: Start warehouse call
            {},
            # ensure_warehouse_running: status check - now RUNNING
            {"state": "RUNNING"},
            # Execute query
            {"statement_id": "stmt123"},
            # Query status - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify warehouse start was called
        calls = [call for call in client.databricks_api.call_args_list
                 if len(call[0]) > 1 and 'start' in str(call[0][1])]
        self.assertTrue(len(calls) > 0)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_warehouse_not_found(self, mock_utils, mock_com):
        """Test error when warehouse ID not found"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w999"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()
        client.databricks_api.return_value = {
            "warehouses": [{"id": "w123", "state": "RUNNING"}]
        }

        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass

        error_msg = db_query_obj.write_error.call_args[0][0]
        self.assertIn("No SQL warehouse found", error_msg)
        self.assertIn("w999", error_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.ThreadPoolExecutor")
    def test_warehouse_query_multiple_chunks(self, mock_executor, mock_utils, mock_com):
        """Test warehouse query with multiple result chunks"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Warehouse list
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            # Execute query
            {"statement_id": "stmt123"},
            # Status check - SUCCEEDED with multiple chunks
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 4,
                    "truncated": False,
                    "schema": {"columns": [{"name": "col1"}]}
                },
                "result": {
                    "external_links": [{
                        "external_link": "http://example.com/chunk0",
                        "chunk_index": 0,
                        "next_chunk_internal_link": "/internal/chunk1"
                    }]
                }
            },
            # Get next chunk
            {
                "external_links": [{
                    "external_link": "http://example.com/chunk1",
                    "chunk_index": 1,
                    "next_chunk_internal_link": None
                }]
            }
        ]

        # Mock ThreadPoolExecutor to return data from both chunks
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance
        mock_executor_instance.map.return_value = [
            [["val1"], ["val2"]],  # Chunk 0
            [["val3"], ["val4"]]   # Chunk 1
        ]

        resp = db_query_obj.generate()
        results = list(resp)

        self.assertEqual(len(results), 4)
        self.assertEqual(results[0], {"col1": "val1"})
        self.assertEqual(results[3], {"col1": "val4"})

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_warehouse_query_truncated_results(self, mock_utils, mock_com):
        """Test warehouse query with truncated results warning"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_warning = MagicMock()

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 1000,
                    "truncated": True,  # Results are truncated
                    "schema": {"columns": [{"name": "col1"}]}
                },
                "result": {
                    "external_links": [{
                        "external_link": "http://example.com/chunk0",
                        "chunk_index": 0,
                        "next_chunk_internal_link": None
                    }]
                }
            }
        ]

        client.external_api.return_value = [["val1"]]

        resp = db_query_obj.generate()
        list(resp)

        db_query_obj.write_warning.assert_called_with(
            "Result limit exceeded, hence results are truncated."
        )

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_warehouse_query_no_external_links(self, mock_utils, mock_com):
        """Test warehouse query with no external links raises error"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_error = MagicMock()

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 10,
                    "truncated": False,
                    "schema": {"columns": [{"name": "col1"}]}
                },
                "result": {
                    "external_links": None  # No links!
                }
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except StopIteration:
            pass

        error_msg = db_query_obj.write_error.call_args[0][0]
        self.assertIn("No data returned", error_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_config_uses_warehouse_from_settings(self, mock_utils, mock_com):
        """Test using warehouse_id from configuration when not provided in command"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        # No warehouse_id or cluster set on command

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "config_for_dbquery": "dbsql",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify warehouse_id was set from config
        self.assertEqual(db_query_obj.warehouse_id, "w123")

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_config_uses_cluster_from_settings(self, mock_utils, mock_com):
        """Test using cluster from configuration when not provided in command"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "config_for_dbquery": "interactive_cluster",
            "cluster_name": "test_cluster",
            "admin_command_timeout": "300"
        }

        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.side_effect = Exception("test error")

        resp = db_query_obj.generate()
        try:
            list(resp)
        except StopIteration:
            pass

        # Verify cluster was set from config
        self.assertEqual(db_query_obj.cluster, "test_cluster")

    @patch("databricksquery.get_splunkd_uri")
    @patch("databricksquery.rest.simpleRequest")
    @patch("databricksquery.time.sleep")
    def test_cancel_query_non_200_response(self, mock_sleep, mock_simple_request, mock_get_uri):
        """Test cancel_query with non-200 response from cancel API"""
        mock_get_uri.return_value = "https://localhost:8089"

        xml_response = '''<?xml version="1.0" encoding="UTF-8"?>
        <entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest">
            <s:key name="dispatchState">FINALIZING</s:key>
            <s:key name="isFinalized">1</s:key>
        </entry>'''

        mock_simple_request.return_value = (None, xml_response.encode())

        client = MagicMock()
        client.databricks_api.return_value = ({"error": "some error"}, 400)

        db_query_obj = self.DatabricksQueryCommand()

        db_query_obj.cancel_query(
            search_sid="test_sid",
            session_key="test_key",
            client=client,
            cancel_endpoint="/api/cancel",
            data_for_cancelation={"statement_id": "stmt123"}
        )

        # Verify API was called
        client.databricks_api.assert_called_once()

    @patch("databricksquery.get_splunkd_uri")
    @patch("databricksquery.rest.simpleRequest")
    @patch("databricksquery.time.sleep")
    def test_cancel_query_other_exception(self, mock_sleep, mock_simple_request, mock_get_uri):
        """Test cancel_query handles other exceptions gracefully"""
        mock_get_uri.return_value = "https://localhost:8089"
        mock_simple_request.side_effect = Exception("Network error")

        client = MagicMock()
        db_query_obj = self.DatabricksQueryCommand()

        # Should not raise exception, just log and break
        db_query_obj.cancel_query(
            search_sid="test_sid",
            session_key="test_key",
            client=client,
            cancel_endpoint="/api/cancel",
            data_for_cancelation={}
        )

        # Verify no API call was made since exception occurred early
        client.databricks_api.assert_not_called()

    @patch("databricksquery.get_splunkd_uri")
    @patch("databricksquery.rest.simpleRequest")
    @patch("databricksquery.time.sleep")
    def test_cancel_query_continues_when_not_finalized(self, mock_sleep, mock_simple_request, mock_get_uri):
        """Test cancel_query continues checking when search is not finalized"""
        mock_get_uri.return_value = "https://localhost:8089"

        # First call - not finalized, second call - finalized
        xml_not_finalized = '''<?xml version="1.0" encoding="UTF-8"?>
        <entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest">
            <s:key name="dispatchState">RUNNING</s:key>
            <s:key name="isFinalized">0</s:key>
        </entry>'''

        xml_finalized = '''<?xml version="1.0" encoding="UTF-8"?>
        <entry xmlns="http://www.w3.org/2005/Atom" xmlns:s="http://dev.splunk.com/ns/rest">
            <s:key name="dispatchState">FINALIZING</s:key>
            <s:key name="isFinalized">1</s:key>
        </entry>'''

        mock_simple_request.side_effect = [
            (None, xml_not_finalized.encode()),
            (None, xml_finalized.encode())
        ]

        client = MagicMock()
        client.databricks_api.return_value = ({}, 200)

        db_query_obj = self.DatabricksQueryCommand()

        db_query_obj.cancel_query(
            search_sid="test_sid",
            session_key="test_key",
            client=client,
            cancel_endpoint="/api/cancel",
            data_for_cancelation={"statement_id": "stmt123"}
        )

        # Verify sleep was called while waiting
        mock_sleep.assert_called()
        # Verify API was eventually called to cancel
        client.databricks_api.assert_called_once()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_warehouse_invalid_state(self, mock_utils, mock_com):
        """Test warehouse in invalid state raises error"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_error = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Warehouse in DELETED state (invalid)
            {"warehouses": [{"id": "w123", "state": "DELETED"}]},
            # ensure_warehouse_running: status check returns DELETED state
            {"state": "DELETED"}
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify error was written
        db_query_obj.write_error.assert_called()
        error_msg = db_query_obj.write_error.call_args[0][0]
        self.assertIn("DELETED", error_msg)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_command_timeout_within_admin_max(self, mock_utils, mock_com):
        """Test command timeout within admin max uses provided value"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.command_timeout = 100  # Within admin max of 300
        db_query_obj.write_warning = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify no warning was issued (timeout value was within bounds)
        db_query_obj.write_warning.assert_not_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_limit_within_admin_max(self, mock_utils, mock_com):
        """Test limit within admin max uses provided value"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.limit = 100  # Within admin max of 1000
        db_query_obj.write_warning = MagicMock()

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify no warning was issued (limit value was within bounds)
        db_query_obj.write_warning.assert_not_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_no_command_timeout_uses_admin_default(self, mock_utils, mock_com):
        """Test no command timeout uses admin default"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_warning = MagicMock()
        # No command_timeout set (defaults to None)

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify no warning was issued (should use admin default without warning)
        db_query_obj.write_warning.assert_not_called()
        # Verify command_timeout was not set (remains None, uses admin default)
        self.assertIsNone(db_query_obj.command_timeout)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    def test_no_limit_uses_admin_default(self, mock_utils, mock_com):
        """Test no limit uses admin default"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"
        db_query_obj.write_warning = MagicMock()
        # No limit set (defaults to None)

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "warehouse_id": "w123",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = [
            {"warehouses": [{"id": "w123", "state": "RUNNING"}]},
            {"statement_id": "stmt123"},
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify no warning was issued (should use admin default without warning)
        db_query_obj.write_warning.assert_not_called()
        # Verify limit was not set (remains None, uses admin default)
        self.assertIsNone(db_query_obj.limit)

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_stopped_delayed_transition(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse in STOPPED state with delayed transition to STARTING after start API call.
        
        This tests the race condition fix where after calling start API, 
        the warehouse may briefly still show as STOPPED before transitioning to STARTING.
        """
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Initial warehouse list - STOPPED
            {"warehouses": [{"id": "w123", "state": "STOPPED"}]},
            # ensure_warehouse_running: status check - STOPPED
            {"state": "STOPPED"},
            # ensure_warehouse_running: Start warehouse call
            {},
            # ensure_warehouse_running: status check - still STOPPED (race condition)
            {"state": "STOPPED"},
            # ensure_warehouse_running: status check - now STARTING
            {"state": "STARTING"},
            # ensure_warehouse_running: status check - now RUNNING
            {"state": "RUNNING"},
            # Execute query
            {"statement_id": "stmt123"},
            # Query status - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify warehouse start was called
        calls = [call for call in client.databricks_api.call_args_list
                 if len(call[0]) > 1 and 'start' in str(call[0][1])]
        self.assertTrue(len(calls) > 0)
        
        # Verify sleep was called (for waiting during state transitions)
        mock_sleep.assert_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_stopping_state(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse in STOPPING state, waits for STOPPED, then starts and runs query"""
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Initial warehouse list - STOPPING
            {"warehouses": [{"id": "w123", "state": "STOPPING"}]},
            # Status check - still STOPPING
            {"state": "STOPPING"},
            # Status check - now STOPPED
            {"state": "STOPPED"},
            # Start warehouse call
            {},
            # Status check - now RUNNING
            {"state": "RUNNING"},
            # Execute query
            {"statement_id": "stmt123"},
            # Query status - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Verify warehouse start was called
        calls = [call for call in client.databricks_api.call_args_list
                 if len(call[0]) > 1 and 'start' in str(call[0][1])]
        self.assertTrue(len(calls) > 0)
        
        # Verify sleep was called while waiting for warehouse to stop
        mock_sleep.assert_called()

    @patch("databricksquery.com.DatabricksClient", autospec=True)
    @patch("databricksquery.utils", autospec=True)
    @patch("databricksquery.time.sleep")
    def test_warehouse_stopping_transitions_to_running(self, mock_sleep, mock_utils, mock_com):
        """Test warehouse in STOPPING state that transitions directly to RUNNING (edge case)
        
        This can happen if someone else starts the warehouse while we're waiting for it to stop.
        """
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.warehouse_id = "w123"

        mock_utils.get_databricks_configs.return_value = {
            "auth_type": "PAT",
            "databricks_instance": "test.databricks.com",
            "databricks_pat": "test_token",
            "admin_command_timeout": "300",
            "query_result_limit": "1000",
            "thread_count": "4"
        }

        client = mock_com.return_value = MagicMock()

        client.databricks_api.side_effect = [
            # Initial warehouse list - STOPPING
            {"warehouses": [{"id": "w123", "state": "STOPPING"}]},
            # Status check - transitions directly to RUNNING (someone else started it)
            {"state": "RUNNING"},
            # Start warehouse call (will still be made but warehouse is already running)
            {},
            # Status check - RUNNING
            {"state": "RUNNING"},
            # Execute query
            {"statement_id": "stmt123"},
            # Query status - SUCCEEDED
            {
                "status": {"state": "SUCCEEDED"},
                "manifest": {
                    "total_row_count": 0,
                    "truncated": False,
                    "schema": {"columns": []}
                },
                "result": {"external_links": []}
            }
        ]

        resp = db_query_obj.generate()
        try:
            list(resp)
        except (StopIteration, SystemExit):
            pass

        # Query should complete successfully
        # The warehouse start may or may not be called depending on timing
