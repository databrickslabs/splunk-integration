# encode = utf-8

"""
This module is used to filter and reload PATH.
This file is genrated by Splunk add-on builder
"""

import os
import sys
import re

py_version = "aob_py3"

ta_name = 'TA-Databricks'
ta_lib_name = 'ta_databricks'
pattern = re.compile(r"[\\/]etc[\\/]apps[\\/][^\\/]+[\\/]bin[\\/]?$")

platform = sys.platform
if platform.startswith("win32"):
    platfrom_folder = "windows_x86_64"
elif platform.startswith("darwin"):
    platfrom_folder = "darwin_x86_64"
else:
    platfrom_folder = "linux_x86_64"

new_paths = [path for path in sys.path if not pattern.search(path) or ta_name in path]
new_paths.insert(0, os.path.sep.join([os.path.dirname(__file__), ta_lib_name]))
new_paths.insert(0, os.path.sep.join([os.path.dirname(__file__), ta_lib_name, py_version]))
new_paths.insert(0, os.path.sep.join([os.path.dirname(__file__), ta_lib_name, py_version, "3rdparty", platfrom_folder]))
sys.path = new_paths
