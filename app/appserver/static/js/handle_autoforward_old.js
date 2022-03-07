require([
     "splunkjs/mvc",
     ], function(mvc) {
    var app_name = "TA-Databricks"
 mvc.setFilter("dbquote", function(inputValue) {
     return inputValue.replace(/"/g, '\\"').replace(/'/g, "\\'");
   });
   require.config({
    paths: {
        'jquery_databricks': '../app/' + app_name + '/js/jquery_databricks',
        'underscore_utils': '../app/' +  app_name + '/js/underscore-min'
        },
    });
 });




require(
    [
        'jquery_databricks',
        'underscore_utils',
        'backbone',
        "splunk.util"
    ],
    function(
        $,
        _,
        Backbone,
        splunkUtil
    ) {
        $("#retryButton").click(function(){
            splunkjs.mvc.Components.getInstance("notebookrun").startSearch()
        })
                
        splunkjs.mvc.Components.getInstance("submitted").on("change", function(changeEvent) {
            //console.log("Got a token change", changeEvent)
            if (typeof changeEvent.changed.url != "undefined") {
                //console.log("Got a change of the URL", changeEvent.changed.url)
                if(splunkjs.mvc.Components.getInstance("submitted").toJSON()['autoforward'] && splunkjs.mvc.Components.getInstance("submitted").toJSON()['autoforward'] == "Yes"){
                    //console.log("Redirecting to ", changeEvent.changed.url)
                    window.location.href = changeEvent.changed.url
                }
            }
            if (typeof changeEvent.changed.autoforward != "undefined") {
                //console.log("Got a change of the Token Forwarding", changeEvent.changed.autoforward)
                if(splunkjs.mvc.Components.getInstance("submitted").toJSON()['url'] && splunkjs.mvc.Components.getInstance("submitted").toJSON()['url'] != ""){
                  //  console.log("Redirecting to ", changeEvent.changed.url)
                    window.location.href = changeEvent.changed.url
                }
            }
        })

    })
