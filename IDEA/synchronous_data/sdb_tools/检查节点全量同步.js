//检查是否存在全量同步
var db = new Sdb("localhost", 11810, "username","password")
var cursor = db.listReplicaGroups()
while (cursor.next ()){
   obj = cursor.current ().toObj()
   if (obj["GroupName"] == "SYSCatalogGroup" || obj["GroupName"] == "SYSCoord"){
      continue;
   }else {
      list = obj["Group"]
      for (var i=0; i<list.length; ++i){
         tmpObj = list[i]
         hostname = tmpObj["HostName"]
         service = tmpObj["Service"][0]["Name"]
         var conn = new Sdb(hostname, service)
         var tcur1 = conn.snapshot (6)
         _fullState = tcur1.next ().toObj ()["ServiceStatus"]
         if (_fullState){
            println (hostname + "." + service + " is normal")
         } else {
            println (hostname + "." + service + " is full state")
         }
         tcur1.close ()
         conn.close ()
      }
   }
}
cursor.close ()
db.close ()