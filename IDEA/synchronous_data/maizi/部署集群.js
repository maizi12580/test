//部署集群脚本,需要先赋权
var dbpath = "/opt/database";
//临时协调节点
var coord_port = "18800";
var sdbcm_port = "11790";
var coordgroups = {name:"SYSCoord",gport:"11810",ghosts:["sdb1","sdb2","sdb3"]};
var datagroups = [{name:"group1",gport:"11820",ghosts:["sdb1","sdb2","sdb3"]},{name:"group2",gport:"11830",ghosts:["sdb1","sdb2","sdb3"]},,{name:"group3",gport:"11840",ghosts:["sdb1","sdb2","sdb3"]}];
var catagroups = {name:"SYSCatalogGroup",gport:"11800",ghosts:["sdb1","sdb2","sdb3"]};


println("create 18800");
var oma = new Oma(coordgroups.ghosts[0], sdbcm_port);
try{
    oma.createCoord(coord_port,dbpath+"/coord/"+coord_port);
    println("coord_port node has exist");
}catch(e){
    if(-145==e){
        println("coord node has exist");
    }
}
println("start 18800");
sleep(10000);
oma.startNode(coord_port);

println("link 18800");
try {
    var db = new Sdb(coordgroups.ghosts[0], coord_port);
}catch (e) {
    println(e)
}
println("link 18800 success");

println("create cata");
println(catagroups.gport);
try{
    db.createCataRG(catagroups.ghosts[0],catagroups.gport,dbpath+"/cata/"+catagroups.gport,{logfilesz:64});
}catch(e){
    if(-200==e){
        println("cataRG has exist");
    }
}
var cataRG = db.getRG(catagroups.name);
for(var i in catagroups.ghosts){
    if(i == 0){
        continue;
    }
    try{
        var node = cataRG.createNode(catagroups.ghosts[i],catagroups.gport,dbpath+"/cata/"+catagroups.gport);
        node.start();
    }catch(e){
        if(-145==e){
            println(catagroups.ghosts[i]+"***************** has exist");
        }
    }
    println("………………………………catanode start success");
    sleep(10000);
}

println("create data");
for(var i in datagroups){

    try{
        var dataRG = db.createRG(datagroups[i].name);
    }catch(e){
        if(-153==e){
            println(datagroups[i].name + "********has exist ");
            var dataRG = db.getRG(datagroups[i].name);
        }
    }
    for(var j in datagroups[i].ghosts){
        println("begin create data##################"+datagroups[i].name);
        if(datagroups[i].name=="group1"){
            if(datagroups[i].ghosts[j]=="sdb1"){
                try{
                    dataRG.createNode(datagroups[i].ghosts[j],datagroups[i].gport,dbpath+"/data/"+datagroups[i].gport,{logfilesz:64,weight:80})
                    println("create data success ******************* "+datagroups[i].ghosts[j]);
                }catch(e){
                    if(-60==e){
                        println("group1 has exist");
                    }
                }
            }
            else{
                dataRG.createNode(datagroups[i].ghosts[j],datagroups[i].gport,dbpath+"/data/"+datagroups[i].gport,{logfilesz:64})
                println("create data success ******************* "+datagroups[i].ghosts[j]);
            }
        }
        else if(datagroups[i].name=="group2"){
            if(datagroups[i].ghosts[j]=="sdb1"){
                dataRG.createNode(datagroups[i].ghosts[j],datagroups[i].gport,dbpath+"/data/"+datagroups[i].gport,{logfilesz:64,weight:80})
                println("create data success ******************* "+datagroups[i].ghosts[j]);
            }
        }
        else if(datagroups[i].name=="group3"){
            if(datagroups[i].ghosts[j]=="sdb1"){
                dataRG.createNode(datagroups[i].ghosts[j],datagroups[i].gport,dbpath+"/data/"+datagroups[i].gport,{logfilesz:64,weight:80})
                println("create data success ******************* "+datagroups[i].ghosts[j]);
            }
        }
    }
    dataRG.start();
    sleep(10000);
}

println("create coord");
var rg = db.createCoordRG(coordgroups.name[0]);
for(var j in coordgroups.ghosts){
    println("begin create coord"+coordgroups.ghosts[j]);
    rg.createNode(coordgroups.ghosts[j],coordgroups.gport,dbpath+"/coord/"+coordgroups.gport,{logfilesz:64});
    println("create coord success***********"+coordgroups.ghosts[j]);
}

rg.start();
println("……………………………………………………coordRG start success");
sleep(10000);

println("link sdbcm");
var oma = new Oma(coordgroups.ghosts[0],sdbcm_port);

println("remove 18800");

oma.removeCoord(coord_port);
println("remove coord success");