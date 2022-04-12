Databricks Add-on for Splunk
==========================

This is an add-on powered by the Splunk Add-on Builder.

# OVERVIEW
The Databricks Add-on for Splunk is used to query Databricks data and execute Databricks notebooks from Splunk.

* Author - Databricks, Inc.
* Version - 1.1.0
* Creates Index - False
* Prerequisites - 
  * This application requires appropriate credentials to query data from Databricks platform. For Details refer to Configuration > Add Databricks Credentials section.
* Compatible with:
    * Splunk Enterprise version: 8.1.x and 8.2.x
    * REST API: 1.2 and 2.0
    * OS: Platform independent
    * Browser: Safari, Chrome and Firefox

# RELEASE NOTES VERSION 1.1.0
* Added support for authentication through Azure Active Directory for Azure Databricks instance.
* Introduced an alert action to run a parameterized notebook on Databricks instance.
* Added saved search to delete old notebook run details from lookup.
* Added macro to specify the retaining days in saved search.
* Added a custom command databricksretiredrun to delete specific databricks notebook run details from lookup based on provided parameters.

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
            * On deployer node, extract the app at $SPLUNK_HOME$/etc/shcluster/apps.
            * Create a `shclustering` stanza at path $SPLUNK_HOME$/etc/shcluster/apps/TA-Databricks/local/server.conf and add following information to the stanza: conf_replication_include.ta_databricks_settings = true as shown below.
                * `[shclustering]`
                * `conf_replication_include.ta_databricks_settings = true`
            * Push the bundle to search head members


# INSTALLATION
Databricks Add-on for Splunk can be installed through UI using "Manage Apps" > "Install app from file" or by extracting tarball directly into $SPLUNK_HOME/etc/apps/ folder.

# CONFIGURATION
Users will be required to have admin_all_objects capability in order to configure Databricks Add-on for Splunk. This integration allows a user to configure only one pair of Databricks Instance, its credentials, and Databricks Cluster Name at a time. In case a user is using the integration in search head cluster environment, configuration on all the search cluster nodes will be overwritten as and when a user changes some configuration on any one of the search head cluster members. Hence a user should configure the integration on only one of the search head cluster members.

**Configuration pre-requisites**: 
To configure the Add-on with Azure Active Directory token authentication, you need to provision a service principal in Azure Portal and add it to the target Azure Databricks workspace.

* To provision a service principal, follow [these steps](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--provision-a-service-principal-in-azure-portal)
* When creating a client secret, the default Expiry time for the secret is six months. Six months after the creation of the secret, it will expire and no longer be functional. In this case, the user needs to create a new client secret and configure the Add-on again. Users can also set a custom expiration time larger than the default value while creating the secret. Example: 12 months
* To add the provisioned service principal to the target Azure Databricks workspace, follow [these steps](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/scim/scim-sp#add-service-principal) and refer [this example](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/service-prin-aad-token#--api-access-for-service-principals-that-are-azure-databricks-workspace-users-and-admins)
* Note that the service principals must be Azure Databricks workspace users and admins.

## 1. Add Databricks Credentials
To configure Databricks Add-on for Splunk, navigate to Databricks Add-on for Splunk, click on "Configuration", go to the "Databricks Credentials" tab, fill in the details asked, and click "Save". Field descriptions are as below:

| Field Name                | Field Description                                                                                                                                                                                                                                  |
| ------------------------  | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Databricks Instance\*     | Databricks Instance URL.                                                                                                                                                                                                                           |
| Databricks Cluster Name   | Name of the Databricks cluster to use for query and notebook execution. A user can override this value while executing the custom command.                                                                                                         |
| Authentication Method \*| SingleSelect: Authentication via Azure Active Directory or using a Personal Access Token |
| Databricks Access Token\* | [Auth: Personal Access Token] Databricks personal access token to use for authentication. Refer [Generate Databricks Access Token](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token) document to generate the access token. |                                                                                              |
| Client Id \*| [Auth: Azure Active Directory] Azure Active Directory Client Id from your Azure portal.|
| Tenant Id \*| [Auth: Azure Active Directory] Databricks Application(Tenant) Id from your Azure portal.|
| Client Secret \*| [Auth: Azure Active Directory] Azure Active Directory Client Secret from your Azure portal.|

**Note**: `*` denotes required fields

## 2. Configure Proxy (Required only if the requests should go via proxy server)
Navigate to Databricks Add-on for Splunk, click on "Configuration", go to the "Proxy" tab, fill in the details asked, and click "Save". Field descriptions are as below:

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

**Steps to configure an HTTPS proxy**

* Select Proxy Type as "http" and provide the other required details for proxy configuration.
* To install proxy certificate in the Add-on , Go to folder $SPLUNK_HOME/etc/apps/TA-Databricks/bin/ta_databricks/aob_py3/certifi
* Put the proxy certificate at the end of the file named cacert.pem

Once the above steps are completed, all the folllowing requests would be directed through the proxy.

**Note**: $SPLUNK_HOME denotes the path where Splunk is installed. Ex: /opt/splunk

After enabling proxy, re-visit the "Databricks Credentials" tab, fill in the details, and click on "Save" to verify if the proxy is working.

## 3. Configure Logging (Optional)
Navigate to Databricks Add-on for Splunk, click on "Configuration", go to the "Logging" tab, select the preferred "Log level" value from the dropdown, and click "Save".

# CUSTOM COMMANDS:
Users will be required to have admin_all_objects or list_storage_passwords capability in order to execute the custom command. Once the user configures Databricks Add-on for Splunk successfully, they can execute custom commands. With custom commands, users can:

* Query their data present in the Databricks table from Splunk.
* Execute Databricks notebooks from Splunk.

Currently, Databricks Add-on for Splunk provides four custom commands. Users can open the Splunk search bar and can execute the commands. Below are the command details.

## 1. databricksquery
This custom command helps users to query their data present in the Databricks table from Splunk.

* Command Parameters

| Parameter       | Required | Overview                                                         |
| --------------- | -------- | ---------------------------------------------------------------- |
| query           | Yes      | SQL query to get data from Databricks delta table.               |
| cluster         | No       | Name of the cluster to use for execution.                            |
| command_timeout | No       | Time to wait in seconds for query completion. Default value: 300 |

* Syntax

| databricksquery cluster="<cluster_name>" query="<SQL_query>" command_timeout=<timeout_in_seconds> | table *

* Output

The command gives the output of the query in tabular format. It will return an error message in case any error occurs in query execution.

* Example

| databricksquery query="SELECT * FROM default.people WHERE age>30" cluster="test_cluster" command_timeout=60 | table *

## 2. databricksrun

This custom command helps users to submit a one-time run without creating a job.

* Command Parameters

| Parameter          | Required | Overview                                                                                                    |
| ------------------ | -------- | ----------------------------------------------------------------------------------------------------------- |
| notebook_path      | Yes      | The absolute path of the notebook to be run in the Databricks workspace. This path must begin with a slash. |
| run_name           | No       | Name of the submitted run.                                                                                  |
| cluster            | No       | Name of the cluster to use for execution.                                                                       |
| revision_timestamp | No       | The epoch timestamp of the revision of the notebook.                                                        |
| notebook_params    | No       | Parameters to pass while executing the run. Refer below example to view the format.                         |

* Syntax

| databricksrun notebook_path="<path_to_notebook>" run_name="<run_name>" cluster="<cluster_name>" revision_timestamp=<revision_timestamp> notebook_params="<params_for_job_execution>" | table *

* Output

The command will give the details about the executed run through job.

* Example 1

| databricksrun notebook_path="/path/to/test_notebook" run_name="run_comm" cluster="test_cluster" revision_timestamp=1609146477 notebook_params="key1=value1||key2=value2" | table *

* Example 2

| databricksrun notebook_path="/path/to/test_notebook" run_name="run_comm" cluster="test_cluster" revision_timestamp=1609146477 notebook_params="key1=value with \"double quotes\" in it||key2=value2" | table *

## 3. databricksjob  

This custom command helps users to run an already created job now from Splunk.

* Command Parameters

| Parameter       | Required | Overview                                                                                   |
| --------------- | -------- | ------------------------------------------------------------------------------------------ |
| job_id          | Yes      | Job ID of your existing job in Databricks.                                                 |
| notebook_params | No       | Parameters to pass while executing the job. Refer below example to view the format.        |

* Syntax

| databricksjob job_id=<job_id> notebook_params="<params_for_job_execution>" | table *

* Output

The command will give the details about the executed run through job.

* Example 1

| databricksjob job_id=2 notebook_params="key1=value1||key2=value2" | table *

* Example 2

| databricksjob job_id=2 notebook_params="key1=value with \"double quotes\" in it||key2=value2" | table *

## 4. databricksretiredrun

This command is used to delete the records based on provided parameter from the submit_run_logs lookup, which maintains the details of notebook runs. To run the command at least one of the parameters is required. When all parameters are provided, it will delete the records matching all the parameters together.

* Command Parameters

| Parameter          | Required | Overview                                                                                                    |
| ------------------ | -------- | ----------------------------------------------------------------------------------------------------------- |
| days      | No      | The number of days, records older than which will be deleted from submit_run_log lookup |
| run_id           | No       | ID of the submitted run.                                                                                  |
| user            | No       | Name of an existing databricks user                                            |

* Syntax

| databricksretiredrun days="<number_of_days>" run_id="<run_id>" user="<user_name>"

* Output

The command will delete the details of notebook runs from submit_run_log lookup.

* Example 1

| databricksretiredrun days=90

* Example 2

| databricksretiredrun user="john doe"

* Example 3

| databricksretiredrun run_id="12344"

* Example 4

| databricksretiredrun days=90 user="john doe" run_id="12344"

# Macro
Macro `databricks_run_retiring_days` specifies the days, records older than which will be deleted from submit_run_log lookup using saved search `databricks_retire_run`. The default value configured is 90 days.

To modify Macro from Splunk UI,

1. Go to `Settings` -> `Advanced search` -> `Search Macros`.
2. Select `Databricks Add-on for Splunk` in the App context.
3. Configure the macro by clicking on the `Name` of the Macro, go to the `Definition` field and update it as per requirements.
4. Click on the `Save` button.

# SAVED SEARCH
Saved search `databricks_retire_run` uses databricksretiredrun command to delete the records older than days specified in macro `databricks_run_retiring_days` from the submit_run_logs lookup. By default, it is invoked once every day at 1:00 hrs and deletes records older than 90 days. The `databricks_run_retiring_days` can be modified to change the default 90 days.

# DASHBOARDS 
This app contains the following dashboards:

* Databricks Job Execution Details: The dashboard provides the details about the one-time runs and jobs executed using `databricksrun` and `databricksjob` custom commands respectively.

* Launch Notebook: The dashboard allows users to launch a notebook on their databricks cluster by providing the required parameters. The users can then navigate to the job results page on the databricks instance from the generated link on the dashboard.

The dashboards will be accessible to all the users. A user with admin_all_objects capability can navigate to “<splunk_instance_host_or_ip>:<splunk_web_port>/en-US/app/TA-Databricks/dashboards” to modify the permissions for “Databricks Job Execution Details” dashboard.

# ALERT ACTIONS
The `Launch Notebook` alert action is used to launch a parameterized notebook based on the provided parameters. The alert can be scheduled or run as ad-hoc. It can also be used as Adaptive response action in "Enterprise Security> Incident review dashboard".
When this alert action is run as Adaptive response action from "Enterprise Security > Incident review dashboard", a `launch_notebook` link will be visible in the Adaptive Responses table in the Incident review dashboard which will redirect to the Launch Notebook dashboard.

**Note**: The redirection will work properly only when the status is in Sucess state.

# UPGRADE
## Upgrade from Databricks Add-On for Splunk v1.0.0 to v1.1.0
No special steps required. Upload and install v1.1.0 of the add-on normally.


# OPEN SOURCE COMPONENTS AND LICENSES
Some of the components included in "Databricks Add-on for Splunk" are licensed under free or open source licenses. We wish to thank the contributors to those projects.

* requests version 2.22.0 https://pypi.org/project/requests (LICENSE https://github.com/requests/requests/blob/master/LICENSE)

# KNOWN ISSUES
* When the commands fail, sometimes an indistinct/unclear error message is displayed in UI, not giving a precise reason for the failure. To troubleshoot such cases, please check the logs at $SPLUNK_HOME/var/log/TA-Databricks/<command_name>_command.log to get the precise reason for the failure.
* When the Adaptive response action `Launch Notebook` is run more than once for the same notable event in Enterprise Security security, clicking on any of the `launch_notebook` links will redirect to the Launch Notebook dashboard with the latest run details.

# LIMITATIONS
* The Databricks API used in the `databricksquery` custom command has a limit on the number of results to be returned. Hence, sometimes the results obtained from this custom command may not be complete. 

# TROUBLESHOOTING
* Authentication Failure: Check the network connectivity and verify that the configuration details provided are correct.
* For any other unknown failure, please check the log files $SPLUNK_HOME/var/log/ta_databricks*.log to get more details on the issue.
* The Add-on does not require a restart after the installation for all functionalities to work. However, the icons will be visible after one Splunk restart post-installation. 
* If all custom commands/notebooks fail to run with https response code [403] then most probably the client secret has expired. Please regenerate your client secret in this case on your Azure portal and configure the add-on again with the new client secret. Set the client secret's expiration time to a custom value that you seem fit. Refer this [guide](https://docs.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app#add-a-client-secret) for setting a client secret in Azure Active Directory.


**Note**: $SPLUNK_HOME denotes the path where Splunk is installed. Ex: /opt/splunk

# UNINSTALL & CLEANUP STEPS

* Remove $SPLUNK_HOME/etc/apps/TA-Databricks/
* Remove $SPLUNK_HOME/var/log/TA-Databricks/
* Remove $SPLUNK_HOME/var/log/splunk/**ta_databricks*.log**
* To reflect the cleanup changes in UI, restart Splunk instance. Refer [Start Splunk](https://docs.splunk.com/Documentation/Splunk/8.0.6/Admin/StartSplunk) documentation to get information on how to restart Splunk.

**Note**: $SPLUNK_HOME denotes the path where Splunk is installed. Ex: /opt/splunk

# SUPPORT
* This app is not officially supported by Databricks. Please send an email to cybersecurity@databricks.com for help.

# COPYRIGHT
© Databricks 2022. All rights reserved. Apache, Apache Spark, Spark and the Spark logo are trademarks of the Apache Software Foundation.