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
        'splunk.clilib',
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

    @classmethod
    def setUp(cls):
        import databricksquery
        cls.databricksquery = databricksquery
        cls.DatabricksQueryCommand = databricksquery.DatabricksQueryCommand
    
    @patch("databricksquery.utils", autospec=True)
    def test_cluster_exception(self, mock_utils):
        db_query_obj = self.DatabricksQueryCommand()
        db_query_obj._metadata = MagicMock()
        db_query_obj.write_error = MagicMock()
        mock_utils.get_databricks_configs.return_value = {"type": "pat"}
        resp = db_query_obj.generate()
        try:
            next(resp)
        except StopIteration:
            pass
        mock_utils.get_databricks_configs.assert_called_once()
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