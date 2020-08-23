//将子表挂载到主表上
var hostname = "localhost";
var svcname = "11810";
var username = "sdbapp";
var password = "password";
var subCSNamePre = "cq";
var mon = ['01','02','03','04','05','06','07','08','09','10','11','12'];
//var MAINCSNAME_at = "cs";
//var MAINCLNAME_at = "cl";
//var mainShardingKey = "tr_date";
//var day = "01";
//var CSNAME = "cbe";
//var CLNAME = "aitmib";
var beginYear = 2015;
var endYear = 2030;


var db = new Sdb(hostname,svcname,username,password);


try
{
      var mainCS_at = db.getCS( MAINCSNAME_at );
      var mainCL_at = mainCS_at.getCL( MAINCLNAME_at );
}
catch( e )
{
      println( "failed to get mainCL_at, rc = " + e );
      throw e;
}



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
		

		fullCLName = subCSName + "." + subCLName;
		if (y==beginYear && mon[m]=='01') {
			beginDate = MinKey();
			endDate = parseInt(y+mon[m+1]+day);
			//endDate = '"'+y+'/'+mon[m+1]+'/'+day+'"';
		} else if (y==endYear && mon[m]=='12') {
			beginDate = parseInt(y+mon[m]+day);
			//beginDate = '"'+y+'/'+mon[m]+'/'+day+'"';
			endDate = MaxKey();
		} else {
			beginDate = parseInt(y+mon[m]+day);
			//beginDate = '"'+y+'/'+mon[m]+'/'+day+'"';
                       if(m == "11"){
				var tmp_y = y + 1;
				var tmp_mon = "01";
				endDate = parseInt(tmp_y+tmp_mon+day);
				//endDate = '"'+tmp_y+'/'+tmp_mon+'/'+day+'"';
			}else{
				endDate = parseInt(y+mon[m+1]+day);
				//endDate = '"'+y+'/'+mon[m+1]+'/'+day+'"';
			}
		}
		println( 'full cl name: ' + fullCLName );
	        println( 'begin date: '  + beginDate + ', end date: ' + endDate );
	        println("mainShardingKey  is "  + mainShardingKey);
		lw = "{"+mainShardingKey+":"+beginDate+"}";
		println("------->LowBound  is   "+ lw);
		up = "{"+mainShardingKey+":"+endDate+"}";
		println("------->UpBound   is   "+ up);
                mainCL_at.attachCL(fullCLName, {LowBound:eval("("+lw+")"), UpBound:eval("("+up+")")});
		println("=======> "+ mainCL_at +" attachCL : "+ fullCLName);
	}
}


