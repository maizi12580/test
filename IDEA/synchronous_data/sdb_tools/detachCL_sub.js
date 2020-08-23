//卸载子表
var hostname = "localhost";
var server = "11810";
var username = "sdbapp";
var password = "password";
//var CSName = "hxtemp";
//var CLName = "cbe_bptfhisa";
//var IndexName = "a_index";
//var subShardingKey = {"a":1};
var csclname = CSName+'.'+CLName ;
println("------>"+ csclname);
//var jsonstr = "{"+"Name"+":"+csclname+"}";
//var table = JSON.parse(jsonstr);


var db = new Sdb(hostname,server,username,password);
var cursor = db.snapshot(8,{"Name":csclname});
while(cursor.next()){
    var obj = cursor.current().toObj();
    var CataInfo = obj["CataInfo"];
    for(var i= 0 ;i<CataInfo.length;i++){
	 var subname = CataInfo[i]["SubCLName"];
	println("======>subname is :" +subname);

	try{
		db.getCS(CSName).getCL(CLName).detachCL(subname);
		println("db."+csclname+".detachCL ("+subname+")");
	}catch(e){
		println(" ------->detachCL is error :" + e);
	}
    }

}
