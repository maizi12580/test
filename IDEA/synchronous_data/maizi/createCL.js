/*
* 	创建主子表并挂载
* */
/*创建为2015到2030年所有的表， 其中主表为mainCS.mainCL,子表模式为mainCS201901.mainCL,每月都创建一张cs，挂载区间为 20180101-20180201,关闭Shard索引，创建强制唯一索引*/
var hostname  =  "localhost";
var svcname = "11810";
var username = "";
var password = "";
var MainCSName = "mainCS";
var MainCLName = "mainCL";

var beginYear = 2018;
var endYear = 2019;
var domain = {PageSize: 4096,Domain:"domain1"};
var mainShardingKey = {"id":1,"Name":1};
var indexName = "idName_PrimIdx";
var subShardingKey = {"id":1,"Name":1};
var mon = ['01','02','03'];

var lowbound = "";
var upbound = "";

var db = new Sdb(hostname,svcname,username,password)
//创建主表
try{
	var maincs = db.createCS(MainCSName,domain);
}catch(e){
	if(e == -33){
		println("cs has exist : " + MainCSName)
		var maincs = db.getCS(MainCSName);
	}else{
		println("====>createMainCS is ERROR :" + e );
	}
}
try{
	//创建主表
	var maincl = maincs.createCL(MainCLName,{IsMainCL:true,ShardingKey:mainShardingKey,ShardingType:"range"});
}catch(e){
	if(e == -22){
		println("cs.cl has exist : " + MainCSName + "." + MainCLName)
		var maincl = maincs.getCL(MainCLName);
	}else{
		println("====>createMainCL is ERROR :" + e );
	}
}

//开始创建子表
for(var year = beginYear;year<= endYear;year++){
	for(a = 0; a<mon.length;a++){
		//createCL 挂载子表
		var subCSName = MainCSName + year + mon[a];
		var subCLName = MainCLName;
		var subTableName = subCSName + "." + subCLName
        try{
			var subcs = db.createCS(subCSName,domain );
			println("create subcs success "+ subCSName);
		}catch(e){
			if(e == -33){
				var subcs = db.getCS(subCSName);
			}else{
				println("====>createMainCS is ERROR :" + e );
			}
		}
		try{
        	//创建子表
			var subcl = subcs.createCL(subCLName,{ShardingKey:subShardingKey,ShardingType:"hash",Compressed:true,CompressionType:"lzw",EnsureShardingIndex:false,AutoSplit:true});
			println("create subcl success "+ subcl);
		}catch(e){
				if(e == -22){

					var subcl = maincs.getCL(subCLName);
					println("cs.cl has exist : " + subcs + "." + subcl)
				}else{
					println("====>createMainCL is ERROR :" + e );
				}
		}	
		//createIndex
		try{
			subcl.createIndex(indexName,subShardingKey,false);
			println("indexName has exist : " + indexName)
		}catch(e ){
			println("createIndex is ERROR : "+ e );
		}
		//attachCL
		var begin_key = ""
		var end_key = ""
		try{
		if(year == 2018 && mon[a]=='01'){
			begin_key = MinKey();
			end_key = year + mon[a]+"01";
		}else if(year == 2019 && mon[a]=='03'){
			begin_key = year + mon[a]+"01";
			end_key = MaxKey();
		}else{
			if(mon[a]=='03'){
				year2 = year +1;
				begin_key = year + mon[a]+"01";
				end_key = year2 + "01"+"01";
			}else{
				begin_key = year + mon[a]+"01";
				end_key = year + mon[a+1]+"01";
			}
		}
		println(begin_key+"-----"+end_key + "---" + subTableName)
			//挂载必须使用索引字段
			maincl.attachCL(subTableName,{LowBound:{id:begin_key},UpBound:{id:end_key}});
		}catch(e){
			println("attachCL  is ERROR "+e);
		}
	}
}











