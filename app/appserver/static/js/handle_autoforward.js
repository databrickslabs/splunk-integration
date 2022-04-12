require([
    'splunkjs/mvc',
    'splunkjs/mvc/simplexml/ready!',
], function(mvc) {
    var submitted_model = mvc.Components.getInstance("submitted")

    const retry_button = document.getElementById("retryButton")

    function rerun() {
        mvc.Components.get("notebookrun").startSearch()
    }

    retry_button.addEventListener('click', rerun, false);

    function redirect_to_result_url(){
        var autoforward = submitted_model.get("autoforward")
        var url = submitted_model.get("url",null)
        if (url && autoforward == "Yes") {
            window.open(url, "_blank");
        }
    }
    function token_handling(param_name){
        var param_value = submitted_model.get(param_name,null)
        var redirected = submitted_model.get("redirected",null)
        if (param_value == "True"){
            submitted_model.unset("url")
            submitted_model.unset("redirected")
        }
        else if(redirected == "True"){
            submitted_model.set(param_name, "True")
        }
        else{
            submitted_model.unset("url")
        }
    }

    submitted_model.on("change:notebook", function() {
        token_handling("notebook_changed")
    })

    submitted_model.on("change:revision_timestamp", function() {
        token_handling("revision_changed")
    })

    submitted_model.on("change:params", function() {
        token_handling("params_changed")
    })

    submitted_model.on("change:cluster", function() {
        token_handling("cluster_changed")
    })

    submitted_model.on("change:url", function() {
        redirect_to_result_url()
    })
    submitted_model.on("change:autoforward", function() {
        redirect_to_result_url()
    })
});