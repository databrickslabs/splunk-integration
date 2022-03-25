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

class TestDatabricksrun(unittest.TestCase):
    """Test databricksrun."""

    @classmethod
    def setUp(cls):
        import databricksrun
        cls.databricksrun = databricksrun
        cls.DatabricksRunCommand = databricksrun.DatabricksRunCommand

    def test_notebook_value_exception(self):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "   "
        db_run_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        db_run_obj.write_error.assert_called_once()
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksrun.utils", autospec=True)
    def test_cluster_exception(self, mock_utils):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "   "
        mock_utils.get_databricks_configs.return_value = {"type": "pat"}
        db_run_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        mock_utils.get_databricks_configs.assert_called_once()
        db_run_obj.write_error.assert_called_once_with("Databricks cluster is required to execute this custom command. Provide a cluster parameter or configure the cluster in the TA's configuration page.")
        self.assertEqual(cm.exception.code, 1)

    @patch("databricksrun.com.DatabricksClient", autospec=True)
    @patch("databricksrun.utils", autospec=True)
    def test_get_cluster_id_exception(self, mock_utils, mock_com):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "test_cluster"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.side_effect = Exception("error getting cluster id")
        db_run_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        client.get_cluster_id.assert_called_once()
        db_run_obj.write_error.assert_called_once_with("error getting cluster id")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksrun.com.DatabricksClient", autospec=True)
    @patch("databricksrun.utils", autospec=True)
    def test_parse_params_exception(self, mock_utils, mock_com):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "test_cluster"
        db_run_obj.revision_timestamp = "123457890"
        db_run_obj.notebook_params = "a:1||b=2"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        mock_utils.format_to_json_parameters.side_effect = Exception("error parsing param")
        db_run_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        mock_utils.format_to_json_parameters.assert_called_once()
        db_run_obj.write_error.assert_called_once_with("error parsing param")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksrun.com.DatabricksClient", autospec=True)
    @patch("databricksrun.utils", autospec=True)
    def test_submit_run_exception(self, mock_utils, mock_com):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "test_cluster"
        db_run_obj.revision_timestamp = "123457890"
        db_run_obj.notebook_params = "a=1||b=2"
        db_run_obj.run_name = "abc"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        mock_utils.format_to_json_parameters.return_value = {"a": 1, "b":2}
        db_run_obj.write_error = MagicMock()
        client.databricks_api.side_effect = Exception("error while submitting run")
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        client.databricks_api.assert_called_once()
        db_run_obj.write_error.assert_called_once_with("error while submitting run")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksrun.com.DatabricksClient", autospec=True)
    @patch("databricksrun.utils", autospec=True)
    def test_fetch_data_exception(self, mock_utils, mock_com):
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "test_cluster"
        db_run_obj.revision_timestamp = "123457890"
        db_run_obj.notebook_params = "a=1||b=2"
        db_run_obj.run_name = "abc"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        mock_utils.format_to_json_parameters.return_value = {"a": 1, "b":2}
        db_run_obj.write_error = MagicMock()
        client.databricks_api.side_effect = [{"run_id": "123"}, Exception("error while fetching data")]
        with self.assertRaises(SystemExit) as cm:
            resp = db_run_obj.generate()
            next(resp)
        self.assertEqual(client.databricks_api.call_count,2)
        db_run_obj.write_error.assert_called_once_with("error while fetching data")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksrun.com.DatabricksClient", autospec=True)
    @patch("databricksrun.utils", autospec=True)
    def test_fetch_data(self, mock_utils, mock_com):
        ret_val = {"user": "test",
            "created_time": "1234567890",
            "param": "a=1||b=2",
            "run_id": "123",
            "output_url": "/test/resultsOnly",
            "result_url": "/result_url",
            "command_status": "success",
            "error": "-",
            "identifier": "id1"
        }
        db_run_obj = self.DatabricksRunCommand()
        db_run_obj._metadata = MagicMock()
        db_run_obj.notebook_path = "/test"
        db_run_obj.identifier = "id1"
        db_run_obj.cluster = "test_cluster"
        db_run_obj.revision_timestamp = "123457890"
        db_run_obj.notebook_params = "a=1||b=2"
        db_run_obj.run_name = "abc"
        client = mock_com.return_value = MagicMock()
        client.get_cluster_id.return_value = "c1"
        mock_utils.format_to_json_parameters.return_value = {"a": 1, "b":2}
        db_run_obj.write_error = MagicMock()
        client.databricks_api.side_effect = [{"run_id": "123"},{"run_page_url": "/test/", "result_url": "/result_url"}]
        mock_utils.update_kv_store_collection.return_value =ret_val
        resp = db_run_obj.generate()
        return_val  = next(resp)
        self.assertEqual(client.databricks_api.call_count,2)
        mock_utils.update_kv_store_collection.assert_called_once()
        assert return_val == ret_val