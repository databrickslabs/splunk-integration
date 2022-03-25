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

class TestDatabricksjob(unittest.TestCase):
    """Test databricksjob."""

    @classmethod
    def setUp(cls):
        import databricksjob
        cls.databricksjob = databricksjob
        cls.DatabricksJobCommand = databricksjob.DatabricksJobCommand
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_fetch_job_details_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        client = mock_com.return_value = MagicMock()
        client.databricks_api.side_effect = Exception("error while fetching job details")
        db_job_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        client.databricks_api.assert_called_once()
        db_job_obj.write_error.assert_called_once_with("error while fetching job details")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_job_details_notebook_task_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.return_value = {"settings":{"spark_submit_task": "some task"}}
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        client.databricks_api.assert_called_once()
        db_job_obj.write_error.assert_called_once_with("Given job does not contains the notebook task. Hence terminating the execution.")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_job_details_spark_task_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.return_value = {"settings": {"notebook_task": "test", "spark_submit_task": "some task"}}
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        client.databricks_api.assert_called_once()
        db_job_obj.write_error.assert_called_once_with("Given job contains one of the following task in addition to the notebook task. (spark_jar_task, spark_python_task and spark_submit_task) Hence terminating the execution.")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_parse_param_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        db_job_obj.notebook_params = "a=1||b=2"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.return_value = {"settings": {"notebook_task": "test"}}
        mock_utils.format_to_json_parameters.side_effect = Exception("error while parsing params")
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        client.databricks_api.assert_called_once()
        db_job_obj.write_error.assert_called_once_with("error while parsing params")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_parse_param_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        db_job_obj.notebook_params = "a=1||b=2"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.side_effect = [{"settings": {"notebook_task": "test"}}, Exception("error while executing job")]
        mock_utils.format_to_json_parameters.return_value = {"a":"1","b":"2"}
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        self.assertEqual(client.databricks_api.call_count,2)
        db_job_obj.write_error.assert_called_once_with("error while executing job")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_fetch_job_exception(self, mock_utils, mock_com):
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        db_job_obj.notebook_params = "a=1||b=2"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.side_effect = [{"settings": {"notebook_task": "test"}}, {"run_id": "1234"}, Exception("error while fetching job response")]
        mock_utils.format_to_json_parameters.return_value = {"a":"1","b":"2"}
        with self.assertRaises(SystemExit) as cm:
            resp = db_job_obj.generate()
            next(resp)
        self.assertEqual(client.databricks_api.call_count,3)
        db_job_obj.write_error.assert_called_once_with("error while fetching job response")
        self.assertEqual(cm.exception.code, 1)
    
    @patch("databricksjob.com.DatabricksClient", autospec=True)
    @patch("databricksjob.utils", autospec=True)
    def test_fetch_job_data(self, mock_utils, mock_com):
        ret_val = {"user": "test",
            "created_time": "1234567890",
            "param": "a=1||b=2",
            "run_id": "123",
            "output_url": "/test/resultsOnly",
            "result_url": "/result_url",
            "command_status": "success",
            "error": "-"
        }
        db_job_obj = self.DatabricksJobCommand()
        db_job_obj._metadata = MagicMock()
        db_job_obj.job_id = "123"
        db_job_obj.notebook_params = "a=1||b=2"
        client = mock_com.return_value = MagicMock()
        db_job_obj.write_error = MagicMock()
        client.databricks_api.side_effect = [{"settings": {"notebook_task": "test"}}, {"run_id": "1234"}, {"run_page_url": "/test/", "result_url": "/result_url"}]
        mock_utils.format_to_json_parameters.return_value = {"a":"1","b":"2"}
        mock_utils.update_kv_store_collection.return_value =ret_val
        resp = db_job_obj.generate()
        return_val  = next(resp)
        self.assertEqual(client.databricks_api.call_count,3)
        mock_utils.update_kv_store_collection.assert_called_once()
        assert return_val == ret_val
    


    
    