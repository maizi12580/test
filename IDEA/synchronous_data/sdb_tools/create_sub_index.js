//按照主表挂载子表创建索引
var hostname = "localhost";
var server = "11810";
var username = "sdbapp";
var password = "password";
//var CSName = "hxtemp";
//var CLName = "cbe_bptfhisa";
//var indexName = "a_index";
//var index = {"a":1};
//var indexTemp = "";
var csclname = CSName+'.'+CLName ;
println("------>"+ csclname);


var db = new Sdb(hostname,server,username,password);
var cursor = db.snapshot(8,{"Name":csclname});
while(cursor.next()){
    var obj = cursor.current().toObj();
    var CataInfo = obj["CataInfo"];
    for(var i= 0 ;i<CataInfo.length;i++){
	 var subname = CataInfo[i]["SubCLName"];
	println("======>subname is :" +subname);

	try{
		//db.getCS(CSName).getCL(CLName).createIndex(indexName,index,indexTemp);
                println(db.getCS(CSName).getCL(CLName) + " .createIndex(" + indexName+"," +JSON.stringify(index)+","+JSON.stringify(indexTemp)+")");
                println("======>create Index success "+ subCLName);
	}catch(e){
		println(" ------->create Index is error :" + e);
	}
    }

}
