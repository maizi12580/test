var hostname = "localhost";
var svcname = "11810";
var username = "sdbapp";
var password = "password";
//var username = "nlsdbapp";
//var password = "password";
//var subCSNamePre = "nl";
//var mon = ['01','02','03'];
var mon = ['01','02','03','04','05','06','07','08','09','10','11','12'];
//var domainName = "domain";
//var MAINCSNAME = "rcs";
//var MAINCLNAME = "test";
//var mainShardingKey = "tr_date";
//var IndexName = "contract_no_sub_cta_no_rec_flag_PrimIdx";
//var subShardingKey = {"contract_no": 1, "sub_cta_no": 1, "rec_flag": 1};
//var day = "01";
var beginYear = 2015;
var endYear = 2030;


var db = new Sdb(hostname,svcname,username,password);


try
{
   var mainCS = db.createCS( MAINCSNAME,{Domain: domainName}  );
}
catch( e )
{
   if( -33 != e )
   {
      println( "failed to create main cs, rc = " + e );
      throw e;
   }
   else
   {
      var mainCS = db.getCS( MAINCSNAME );
   }
}

try
{
   var mainShardingKeyObj = "{"+mainShardingKey+":"+"1}";
   println("--------> mainShardingKeyObj is  " + mainShardingKeyObj);
   var mainCL = mainCS.createCL( MAINCLNAME, {ShardingKey:eval("("+mainShardingKeyObj+")"), ShardingType:'range', Compressed:true, IsMainCL: true });
}
catch( e )
{
   if( -22 != e )
   {
      println( "failed to create main cl, rc = " + e );
      throw e;
   }
   else
   {
      var mainCL = mainCS.getCL( MAINCLNAME );
   }
}


for (var y=beginYear;y<=endYear;y++) {
	for (var m=0;m<mon.length;m++) {
		var subCSName = subCSNamePre + y + mon[m];
		var subCLName = CSNAME + "_" + CLNAME;

		try
		{
		   var subCS = db.createCS( subCSName, {Domain: domainName} );
		}
		catch( e )
		{
		   if( -33 != e )
		   {
			  println( "failed to create sub cs, rc = " + e );
			  throw e;
		   }
		   else
		   {
			  var subCS = db.getCS( subCSName );
		   }
		}

		try
	        {
	       	          var subCL = subCS.createCL( subCLName, { ShardingKey: subShardingKey,
	 							 ShardingType:'hash', Compressed: true, CompressionType: "lzw", 
								 AutoSplit:true, EnsureShardingIndex: false } );
	        }
	        catch(e)
	        {
	 	         println("create sub cl failed: " + e);
	        }finally{
			var subCL = subCS.getCL(subCLName);
			subCL.createIndex(IndexName,subShardingKey,true);
                        println("subCL.createIndex(IndexName,subShardingKey,true);"+subCS);	
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
     	mainCL.attachCL(fullCLName, {LowBound:eval("("+lw+")"), UpBound:eval("("+up+")")});
	}
}


