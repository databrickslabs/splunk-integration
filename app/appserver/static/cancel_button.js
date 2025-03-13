require([
    'underscore',
    'splunkjs/mvc',
    'splunkjs/mvc/tableview',
    'splunkjs/mvc/searchmanager',
    'splunkjs/mvc/simplexml/ready!'
], function(_, mvc, TableView, SearchManager) {

    var runDetailsTable = mvc.Components.get("databricks_run_table");
    var jobDetailsTable = mvc.Components.get("databricks_job_table");
    customRenderer = TableView.BaseCellRenderer.extend({
        canRender: function (cell) {
            return _(['Cancel Run']).contains(cell.field);
        },
        render: function ($td, cell) {
            if(cell.field == "Cancel Run"){
                let isCancelled = false;
                var run_details = cell.value;
                var array = run_details.split("||");
                var run_ex_status = array[2];

                if(run_ex_status !== "Running" && run_ex_status !== "Initiated" && run_ex_status !== "Pending"){
                    $td.html("<div class='ar_containter'><div class='cancel_button_disabled' value='Cancel'>Cancel Run</div></div>");
                    $td.on('click', function(event) {
                        var popupContainer = document.createElement('div');
                        popupContainer.className = 'popup-container-for-no-cancelation';                
                        var popupContent = document.createElement('div');
                        popupContent.className = 'popup-content-for-no-cancelation';
                        popupContent.innerHTML = 'This can not be canceled as Execution Status is not in Running, Pending or Initiated mode!';
                        popupContainer.appendChild(popupContent);
                        document.body.appendChild(popupContainer);
                        popupContainer.addEventListener('click', function() {
                            document.body.removeChild(popupContainer);
                        });
                    });                   
                }
                else{
                    $td.html("<div class='ar_containter'><div class='cancel_button' value='Cancel'>Cancel Run</div></div>");
                    $td.on('click', function (event) {
                        if (isCancelled){
                            return;
                        }
                        $td.html("<div class='ar_containter'><div class='loading' value='Loading'>Canceling..</div></div>");
                        var fields = {}
                        fields['run_id'] = array[0]
                        var ENDPOINT_URL = '/services/cancel_run'
                        var service = mvc.createService({ owner: "nobody" });
                        fields['account_name'] = array[1]
                        fields['uid'] = array[3]
                        $td.css("pointer-events", "none");

                        service.post(ENDPOINT_URL, fields, function (err, response) {
                            if (response != undefined && (response.data != null || response.data != undefined)) {
                                canceled_response = response.data['canceled']
                                if (canceled_response == "Success"){
                                    var popupContainer = document.createElement('div');
                                    popupContainer.className = 'popup-container-for-successful-cancelation';
                                    var popupContent = document.createElement('div');
                                    popupContent.className = 'popup-content-for-successful-cancelation';
                                    popupContent.innerHTML = 'Successfully Canceled the run! An updated event with canceled execution status will be ingested in Splunk in few minutes.';
                                    popupContainer.appendChild(popupContent);
                                    document.body.appendChild(popupContainer);
                                    popupContainer.addEventListener('click', function() {
                                        document.body.removeChild(popupContainer);
                                    });
                                    $td.html("<div class='ar_containter'><div class='cancel_button_disabled' value='Cancel'>Canceled</div></div>");
                                    isCancelled = true;
                                }
                            }
                            else {
                                var popupContainer = document.createElement('div');
                                popupContainer.className = 'popup-container-for-err-in-cancelation';
                                var popupContent = document.createElement('div');
                                popupContent.className = 'popup-content-for-err-in-cancelation';
                                popupContent.innerHTML = 'Error while Canceling the run! Please try after sometime!';
                                popupContainer.appendChild(popupContent);
                                document.body.appendChild(popupContainer);
                                popupContainer.addEventListener('click', function() {
                                    document.body.removeChild(popupContainer);
                                });
                                $td.html("<div class='ar_containter'><div class='cancel_button' value='Cancel'>Cancel Run</div></div>");
                            }
                            $td.css("pointer-events", "auto");
                        });
                    });                    
                }
            }
        }
    });
    if (runDetailsTable !== undefined) {
        runDetailsTable.getVisualization(function (tableView) {
            tableView.table.addCellRenderer(new customRenderer());
            tableView.table.render();
        });
    }
    if (jobDetailsTable !== undefined) {
        jobDetailsTable.getVisualization(function (tableView) {
            tableView.table.addCellRenderer(new customRenderer());
            tableView.table.render();
        });
    }
})