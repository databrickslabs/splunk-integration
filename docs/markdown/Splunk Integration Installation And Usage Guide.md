Databricks Add-on for Splunk
==========================

This is an add-on powered by the Splunk Add-on Builder.

# OVERVIEW
The Databricks Add-on for Splunk is used to query Databricks data, and execute Databricks notebooks from Splunk.

* Author - Databricks, Inc.
* Version - 1.0.0
* Creates Index - False
* Prerequisites - This application requires appropriate credentials to query data from Databricks platform. For Details refer to Configuration > Add Databricks Credentials section.
* Compatible with:
    * Splunk Enterprise version: 8.0.x and 8.1.x
    * REST API: 1.2 and 2.0
    * OS: Platform independent
    * Browser: Safari, Chrome and Firefox

# RELEASE NOTES VERSION 1.0.0
* First release

# RECOMMENDED SYSTEM CONFIGURATION
* Standard Splunk configuration

# TOPOLOGY AND SETTING UP SPLUNK ENVIRONMENT
* This app can be set up in two ways:
    
    1. **Standalone Mode**:
        * Install the Databricks Add-on for Splunk.
    2. **Distributed Environment**:
        * Install the Databricks Add-on for Splunk on the search head and configure an account to use the custom commands.
        * In case of deployment in the search head cluster environment use a deployer to push the apps. Follow the below steps to push the apps to search head cluster members:
            * On the deployer node, extract the app at `$SPLUNK_HOME$/etc/shcluster/apps`.
            * Create a `shclustering` stanza at path `$SPLUNK_HOME$/etc/shcluster/apps/TA-Databricks/local/server.conf` and add following information to the stanza: `conf_replication_include.ta_databricks_settings = true` as shown below.
              ```
              [shclustering]
              conf_replication_include.ta_databricks_settings = true
              ```
            * Push the bundle to search head members


# INSTALLATION
Databricks Add-on for Splunk can be installed through UI using "Manage Apps" > "Install app from file" or by extracting tarball directly into `$SPLUNK_HOME/etc/apps/` folder.

# CONFIGURATION
Users will be required to have `admin_all_objects` capability in order to configure Databricks Add-on for Splunk. This integration allows a user to configure only one pair of Databricks Instance, Databricks Access Token and Databricks Cluster Name at a time. In case a user is using the integration in search head cluster environment, configuration on all the search cluster nodes will be overwritten as and when a user changes some configuration on any one of the search head cluster members. Hence a user should configure the integration on only one of the search head cluster members. Once the installation is done successfully, follow the below steps to configure.

## 1. Add Databricks Credentials
To configure Databricks Add-on for Splunk, navigate to Databricks Add-on for Splunk, click on "Configuration", go to "Databricks Credentials" tab, fill in the details asked and click "Save". Field descriptions are as below:

| Field Name                | Field Description                                                                                                                                                                                                                                  |
| ------------------------  | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Databricks Instance\*     | Databricks Instance URL.                                                                                                                                                                                                                           |
| Databricks Access Token\* | Databricks personal access token to use for authentication. Refer [Generate Databricks Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token) document to generate the access token. |                                                                                              |
| Databricks Cluster Name   | Name of the Databricks cluster to use for query and notebook execution. A user can override this value while executing the custom command.                                                                                                         |

**Note**: `*` denotes required fields

## 2. Configure Proxy (Required only if the requests should go via proxy server)
Navigate to Databricks Add-on for Splunk, click on "Configuration", go to the "Proxy" tab, fill in the details asked and click "Save". Field descriptions are as below:

| Field Name            | Field Description                                                              |
| -------------------   | ------------------------------------------------------------------------------ |
| Enable                | Enable/Disable proxy                                                           |
| Proxy Type\*          | Type of proxy                                                                  |
| Host\*                | Hostname/IP Address of the proxy                                               |
| Port\*                | Port of proxy                                                                  |
| Username              | Username for proxy authentication (Username and Password are inclusive fields) |
| Password              | Password for proxy authentication (Username and Password are inclusive fields) |
| Remote DNS resolution | Whether to resolve DNS or not                                                  |

**Note**: `*` denotes required fields

After enabling proxy, re-visit the "Databricks Credentials" tab, fill in the details and click on "Save" to verify if the proxy is working.

## 3. Configure Logging (Optional)
Navigate to Databricks Add-on for Splunk, click on "Configuration", go to the "Logging" tab, select the preferred "Log level" value from the dropdown and click "Save".

# CUSTOM COMMANDS
Users will be required to have `admin_all_objects` or `list_storage_passwords` capability in order to execute the custom command. Once the user configures Databricks Add-on for Splunk successfully, they can execute custom commands. With custom commands, users can:

* Query their data present in the Databricks table from Splunk.
* Execute Databricks notebooks from Splunk.

Currently, Databricks Add-on for Splunk provides three custom commands. Users can open the Splunk search bar and can execute the commands. Below are the command details.

## 1. databricksquery
This custom command helps users to query their data present in the Databricks table from Splunk.

* Command Parameters

| Parameter         | Required | Overview                                                         |
| ---------------   | -------- | ---------------------------------------------------------------- |
| `query`           | Yes      | SQL query to get data from Databricks delta table.               |
| `cluster`         | No       | Name of cluster to use for execution.                            |
| `command_timeout` | No       | Time to wait in seconds for query completion. Default value: 300 |

### Syntax

`| databricksquery cluster="<cluster_name>" query="<SQL_query>" command_timeout=<timeout_in_seconds> | table *`

### Output

The command gives output of the query in tabular format. It will return an error message in case any error occurs in query execution.

### Example

`| databricksquery query="SELECT * FROM default.people WHERE age>30" cluster="test_cluster" command_timeout=60 | table *`

## 2. databricksrun

This custom command helps users to submit a one-time run without creating a job.

### Command Parameters

| Parameter            | Required | Overview                                                                                                    |
| ------------------   | -------- | ----------------------------------------------------------------------------------------------------------- |
| `notebook_path`      | Yes      | The absolute path of the notebook to be run in the Databricks workspace. This path must begin with a slash. |
| `run_name`           | No       | Name of the submitted run.                                                                                  |
| `cluster`            | No       | Name of cluster to use for execution.                                                                       |
| `revision_timestamp` | No       | The epoch timestamp of the revision of the notebook.                                                        |
| `notebook_params`    | No       | Parameters to pass while executing the run. Refer below example to view the format.                         |

### Syntax

`| databricksrun notebook_path="<path_to_notebook>" run_name="<run_name>" cluster="<cluster_name>" revision_timestamp=<revision_timestamp> notebook_params="<params_for_job_execution>" | table *`

### Output

The command will give the details about the executed run through job.

### Example 1

`| databricksrun notebook_path="/path/to/test_notebook" run_name="run_comm" cluster="test_cluster" revision_timestamp=1609146477 notebook_params="key1=value1||key2=value2" | table *`

### Example 2

`| databricksrun notebook_path="/path/to/test_notebook" run_name="run_comm" cluster="test_cluster" revision_timestamp=1609146477 notebook_params="key1=value with \"double quotes\" in it||key2=value2" | table *`

## 3. databricksjob  

This custom command helps users to run an already created job now from Splunk.

### Command Parameters

| Parameter         | Required | Overview                                                                                   |
| ---------------   | -------- | ------------------------------------------------------------------------------------------ |
| `job_id`          | Yes      | Job ID of your existing job in Databricks.                                                 |
| `notebook_params` | No       | Parameters to pass while executing the job. Refer below example to view the format.        |

### Syntax

`| databricksjob job_id=<job_id> notebook_params="<params_for_job_execution>" | table *`

### Output

The command will give the details about the executed run through job.

### Example 1

`| databricksjob job_id=2 notebook_params="key1=value1||key2=value2" | table *`

### Example 2

`| databricksjob job_id=2 notebook_params="key1=value with \"double quotes\" in it||key2=value2" | table *`

# DASHBOARDS 
Users can check the status of command execution in the dashboard. Dashboard will be accessible to all the users. A user with admin_all_objects capability can navigate to `<splunk_instance_host_or_ip>:<splunk_web_port>/en-US/app/TA-Databricks/dashboards` to modify the permissions for “Databricks Job Execution Details” dashboard. This app contains the following dashboard:

* Databricks Job Execution Details: The dashboard provides the details about the one-time runs and jobs executed using `databricksrun` and `databricksjob` custom commands respectively. 

# OPEN SOURCE COMPONENTS AND LICENSES
Some of the components included in "Databricks Add-on for Splunk" are licensed under free or open source licenses. We wish to thank the contributors to those projects.

* requests version 2.22.0 https://pypi.org/project/requests (LICENSE https://github.com/requests/requests/blob/master/LICENSE)

# KNOWN ISSUES
* When the commands fail, sometimes an indistinct/unclear error message is displayed in the UI, not giving a precise reason for the failure. To troubleshoot such cases, please check the logs at `$SPLUNK_HOME/var/log/TA-Databricks/<command_name>_command.log` to get the precise reason for the failure.

# LIMITATIONS
* The Databricks API used in the `databricksquery` custom command has a limit on the number of results to be returned. Hence, sometimes the results obtained from this custom command may not be complete. 

# TROUBLESHOOTING
* Authentication Failure: Check the network connectivity and verify that the configuration details provided are correct.
* For any other unknown failure, please check the `$SPLUNK_HOME/var/log/TA-Databricks` folder to get more details on the issue.


**Note**: `$SPLUNK_HOME` denotes the path where Splunk is installed. Ex: `/opt/splunk`

# UNINSTALL & CLEANUP STEPS

* Remove `$SPLUNK_HOME/etc/apps/TA-Databricks/`
* Remove `$SPLUNK_HOME/var/log/TA-Databricks/`
* Remove `$SPLUNK_HOME/var/log/splunk/ta_databricks.log`
* To reflect the cleanup changes in UI, restart Splunk instance. Refer https://docs.splunk.com/Documentation/Splunk/8.0.6/Admin/StartSplunk documentation to get information on how to restart Splunk.

**Note**: `$SPLUNK_HOME` denotes the path where Splunk is installed. Ex: `/opt/splunk`

# SUPPORT
* This app is not officially supported by Databricks. Please send email to [cybersecurity@databricks.com](mailto:cybersecurity@databricks.com) for help.

# COPYRIGHT
© Databricks 2020. All rights reserved. Apache, Apache Spark, Spark and the Spark logo are trademarks of the Apache Software Foundation.
