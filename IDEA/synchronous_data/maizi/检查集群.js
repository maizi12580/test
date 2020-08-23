//检查集群可靠性脚本
var db = new Sdb("localhost",11810);
println("localhost 11810");
var sdb_csName="cs"
var sdb_clName="cl"
var cursor = db.listDomains();
var domain = cursor.current().toObj();
var d = domain ["Name"];
println(d);
	if(d =="mydomain2"){
		println(domain);
	}
	else{
	db.createDomain("mydomain2",['dg1','dg2'],{AutoSplit:true});
	println("createdomain success ");
	}
try{
   println("drop cs " + sdb_csName);
   db.dropCS(sdb_csName);
}catch(e){
   println("error is \n"+e);
   if( -34 != e ){
     println("e is "+e)
     println("ignore")
     throw e;
   }
}	
db.createCS(sdb_csName,{Domain:"mydomain2"});
println("create cs success" + sdb_csName);
db.cs.createCL(sdb_clName,{ShardingKey:{"a":1},ShardingType:"hash",AutoSplit:true,AutoIndexId:false,Partition:1024,Compressed:true,CompressionType:"lzw",
    EnsureShardingIndex:false});
println("create cl success" + sdb_clName);
var cs = db.getCS(sdb_csName);
var cl = cs.getCL(sdb_clName);
db.cs.cl.truncate();
println("cs.cl truncate success");
for(var i=0;i<10000;i++){
db.cs.cl.insert({"a":i,age:i})
}
println("insert success");
var a = db.cs.cl.count();
println(a);
var db2 = new Sdb("localhost",11820);
var cs = db2.getCS("cs");
var cl = cs.getCL("cl");
var b = db2.cs.cl.count();
println(b);
var db3 = new Sdb("localhost",11830);
var cs = db3.getCS("cs");
var cl = cs.getCL("cl");
var c = db3.cs.cl.count();
println(c);

var num = b+c;
println("num is "+num);
if(a == num)
println("success");
else
println("error");