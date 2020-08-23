var hostname = "localhost";
var svcname = "11810";
var username = "";
var password = "";

//var old_table = "old_table":
//var new_table = "new_table";


var db = new Sdb(hostname,svcname,username,password);

var oldCSName = old_table.split(".")[0];
var oldCLName = old_table.split(".")[1];
var newCSName = new_table.split(".")[0];
var newCLName = new_table.split(".")[1];

try{
   var oldCS = db.getCS(oldCSName );
}
catch( e ){
	if( -34 != e){
		println( e );
	}
	else{
		println( "old table cs is not exit, rc = " + e );
	}
}

try{
	var oldCL = oldCS.getCL(oldCLName );
}catch( e ){
	if( -23 != e){
		println( e );
	}
	else{
		println( "old table cl is not exit, rc = " + e );
	}
}

// olb table domain
var cursor = db.exec("select Domain from $LIST_CS where not Domain is null and Name =\"+oldCSName+\"");
while(cursor.next()){
	var obj = cursor.current().toObj();
	var domain = obj["Domain"];
}

// old table mainShardingKey
var cursor2 = db.snapshot(8,{"Name":old_table});
while(cursor2.next()){
	var obj2 = cursor2.current().toObj();
	var mainShardingKey = obj2["ShardingKey"];
	var CataInfo = obj2["CataInfo"];
	for(var i= 0 ;i<CataInfo.length;i++){
		var subname = CataInfo[i]["SubCLName"];
		cursor3 = db.snapshot(8,{"Name":subname});
		while(cursor3.next()){
			var obj3 = cursor3.current().toObj();
			var subShardingKey = obj3["ShardingKey"];
		}
	}
}

// create new table CS
try{
	var newCS = db.createCS( newCSName,domain );
}catch( e ){
	if( -33 != e ){
		println( "failed to create sub CS ERROR: " + e );
		throw e;
	}
	else
	{
		var subCS = db.getCS( newCSName );
	}
}

//create new table CL
try{
	var newCL = db.newCS.createCL( newCLName, { ShardingKey: subShardingKey,
	 							 ShardingType:'hash', Compressed: true, CompressionType: "lzw", 
								 AutoSplit:true, EnsureShardingIndex: false }  );
}catch( e ){
	if( -22 != e ){
		println( "failed to create sub CL ERROR: " + e );
		throw e;
	}
	else
	{
		var subCS = db.getCL( newCLName );
	}
}

//detachCL and attachCL
var cursor4 = db.snapshot(8,{"Name":old_table});
while(cursor4.next()){
	var obj4 = cursor4.current().toObj();
	var CataInfo = obj4["CataInfo"];
	for(var i= 0 ;i<CataInfo.length;i++){
		var subname  = CataInfo[i]["SubCLName"];
		var lowBound = CataInfo[i]["LowBound"];
		var upBound  = CataInfo[i]["UpBound"];
		//detachCL
		try{
			db.getCS(oldCSName).getCL(oldCLName).detachCL(subname);
			println("db."+old_table+".detachCL ("+subname+")");
		}catch(e){
			println(" ------->old table detachCL is error :" + e);
		}
		//attachCL
		try{
			db.getCS(newCSName).getCL(newCLName).attachCL(subname,{lowBound,upBound});
			println("db."+new_table+".attachCL ("+subname+")");
		}catch(e){
			println(" -------> new table attachCL is error :" + e);
		}
		
	}
}



