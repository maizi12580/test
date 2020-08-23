//检查集群中每个数据节点的的LSN是否相同
var coordPort=11810;
var hostname = hostname;
var username = username;
var password = password;
var db = new Sdb(hostname,coordPort,username,password);
var cursor = db.list(7);
while(cursor.next()){
	var obj = cursor.current().toObj();
	if(obj["GroupName"]!="SYSCoord"&&obj["GroupName"]!="SYSCatalogGroup"){
		var group = obj["Group"];
		var groupName = obj["GroupName"];
		for(var i =0; i<group.lenght;i++){
			println ("=========>");
			var hostName = group[i]["hostName"];
			var dataPort = group[i]["Service"][0]["Name"];
			var tmp_db = new Sdb(hostName,dataPort,username,password);
			var matcher = {"TotalDataWrite":1,"CompleteLSN":1};
			var parme = {};
			var cursor2 = tmp_db.list(6,parme,matcher);
			while(cursor2.next()){
				var obj2 = cursor2.curent().toObj;
				var write = obj2["TotalDataWrite"];
				var LSN = obj2["CompleteLSN"];
				println ("GroupName:"+group+"hostName:"+hostName+"LSN: "+LSN+"write:  "+write);
			}
		}
	}
}
