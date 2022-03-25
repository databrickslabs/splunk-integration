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

class TestDatabricksRetiredRunCommand(unittest.TestCase):
    """Test databricksretiredrun."""

    @classmethod
    def setUp(cls):
        import databricksretiredrun
        cls.databricksretiredrun = databricksretiredrun
        cls.DatabricksRetiredRunCommand = databricksretiredrun.DatabricksRetiredRunCommand

    def test_option_values_exception(self):
        db_retired_run_obj = self.DatabricksRetiredRunCommand()
        db_retired_run_obj.write_error = MagicMock()
        with self.assertRaises(SystemExit) as cm:
            resp = db_retired_run_obj.generate()
            next(resp)
        self.assertEqual(cm.exception.code, 1)
        db_retired_run_obj.write_error.assert_called_once_with("No parameters provided. Please provide at least one of the parameters")
    