(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        //ORDER INFO - SAP
        var sapcols = [{
            id: "orderid",
            alias: "Order ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "orderdatetime",
            alias: "Order Date Time",
            dataType: tableau.dataTypeEnum.datetime
        },{
            id: "totalAmount",
            alias: "Total Amount",
            dataType: tableau.dataTypeEnum.float
        },{
            id: "status",
            alias: "Status",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "trackingnumber",
            alias: "Tracking Number",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "sourcesystem",
            alias: "Source System",
            dataType: tableau.dataTypeEnum.string
        }
        ];

        var tableSchemaSAP = {
            id: "saporderinformation",
            alias: "SAP Order Information",
            columns: sapcols
        };

        //CUSTOMER INFO - SFDC
        var sfdccols = [
        {
            id: "orderidsfdc",
            alias: "Order ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "accountid",
            alias: "Account ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "accountname",
            alias: "Account Name",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "location",
            alias: "Location",
            dataType: tableau.dataTypeEnum.string,
            geoRole: tableau.geographicRoleEnum.country_region
        },{
            id: "accountowner",
            alias: "Account Owner",
            dataType: tableau.dataTypeEnum.string
        }
        ];

        var tableSchemaSFDC = {
            id: "sfdccustomerinfo",
            alias: "SFDC Customer Information",
            columns: sfdccols
        };

        //ORDER ITEMS - SQL
        var sqlcols = [
        {
            id: "orderidsql",
            alias: "Order ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "locationid",
            alias: "Location ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "orderline",
            alias: "Order Line",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "productid",
            alias: "Product ID",
            dataType: tableau.dataTypeEnum.string
        },{
            id: "orderqty",
            alias: "Order Ordered Qty",
            dataType: tableau.dataTypeEnum.int
        },{
            id: "shipqty",
            alias: "Order Shipped Qty",
            dataType: tableau.dataTypeEnum.int
        },{
            id: "delivery",
            alias: "Delivery Method",
            dataType: tableau.dataTypeEnum.string
        }
        ];

        var tableSchemaSQL = {
            id: "sqlorderitems",
            alias: "SQL Order Details Information",
            columns: sqlcols
        };

        schemaCallback([tableSchemaSAP, tableSchemaSFDC, tableSchemaSQL]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {

     /*   $.ajaxSetup({
         headers : {
            'x-rapidapi-host' : 'api-nba-v1.p.rapidapi.com',
            'x-rapidapi-key' : 'd18cdeb3a2mshab6824adf713c9ep130fb2jsncca2d444588d'
          } 
        }); */
        console.log("Step1"); 
        //var herokuURL = 'http://localhost:8081/post';
        var muleURLArray = [];
        muleURLArray.push('http://mule-tableau-demo.sg-s1.cloudhub.io/orders');
        var previousURL = "";
	    if (muleURLArray.length == 0) return doneCallback();

	    $.getJSON(muleURLArray.shift(), function callback(resp) {
            var feat = resp.orders, tableData = [];
	        // Iterate over the JSON object
	        var keys = Object.keys(feat);

            if (table.tableInfo.id == "saporderinformation") {
		        for (var i = 0, len = keys.length; i < len; i++) {
		            tableData.push({
		                "orderid" : feat[i].orderId,
		                "orderdatetime" : feat[i].orderDateTime,
		                "totalAmount" : feat[i].totalAmount,
		                "status" : feat[i].status,
		                "trackingnumber" : feat[i].trackingNumber,
		                "sourcesystem": feat[i].sourceSystem
		            });
		        }

            }

            if (table.tableInfo.id == "sfdccustomerinfo") {
	            // Iterate over the JSON object
	            for (var i = 0, len = feat.length; i < len; i++) { 
		            tableData.push({
		            
		                "orderidsfdc" : feat[i].orderId,
		                "accountid" : feat[i].customer.accountId,
		                "accountname" : feat[i].customer.accountName,
		                "location" : feat[i].customer.location,
		                "accountowner" : feat[i].customer.accountOwner
		            });           
		        } 
            }


            if (table.tableInfo.id == "sqlorderitems") {
	            // Iterate over the JSON object
				for (var i = 0, len = feat.length; i < len; i++) {
		            var orderdetail = feat[i].orderItems
		            for (var z = 0, orderlen = orderdetail.length; z < orderlen; z++) { 
			            tableData.push({
			            
			                "orderidsql" : feat[i].orderId,
			                "locationid" : orderdetail[z].locationID,
			                "orderline" : orderdetail[z].orderLine,
			                "productid" : orderdetail[z].product.id,
			                "orderqty" : orderdetail[z].quantity.ordered,
			                "shipqty" : orderdetail[z].quantity.shipped,
			                "delivery" : orderdetail[z].delivery.method
			            });          
			            }
			    } 
            }   

	        table.appendRows(tableData);

	        console.log(previousURL);
	        console.log(resp.nextPage);
	        if (previousURL == "" || resp.nextPage !== "null") {
				muleURLArray.push(resp.nextPage);
				previousURL = resp.nextPage;
	        }

	        var url = muleURLArray.shift();
	        if (url) return $.getJSON(url, callback); // recursion happens here 

	        doneCallback();
	    });
    };

    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "Mulesoft MFG Dataset"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();
