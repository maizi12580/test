//给定制区创建的表，给子表创建索引
var hostname = "localhost";
var svcname = "11810";
var username = "sdbapp";
var password = "password";
var subCSNamePre = "cq";
var mon = ['01','02','03','04','05','06','07','08','09','10','11','12'];
//var day = "01";
//var CSNAME = "cbe";
//var CLNAME = "aitmib";
//var indexName = "";
//var subShardingKey = "";
var beginYear = 2015;
var endYear = 2030;
//var indexTemp = true;


var db = new Sdb(hostname,svcname,username,password);



for (var y=beginYear;y<=endYear;y++) {
	for (var m=0;m<mon.length;m++) {
		var subCSName = subCSNamePre + y + mon[m];
		var subCLName = CSNAME + "_" + CLNAME;

		try
		{
			 var subCS = db.getCS( subCSName );
		}
		catch( e )
		{
			 println( "failed to get subCS, rc = " + e );
			 throw e;
		}

		try
	        {
			var subCL = subCS.getCL(subCLName);
	        }
	        catch(e)
	        {
	 	        println(" failed to get subCL, rc =  " + e);
			throw e ;
	        }
		try{
			subCL.createIndex(indexName,subShardingKey,indexTemp);
			println(subCL+ " .createIndex(" + indexName+"," +JSON.stringify(subShardingKey)+","+JSON.stringify(indexTemp)+")");
			println("======>create Index success "+ subCLName);
		}
		catch(e)
		{
			println(subCLName+"  create Index is ERROR :" + e);
		}

	}
}


