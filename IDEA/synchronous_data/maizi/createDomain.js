var username = "";
var password = "";
var db = new Sdb("localhost", 11810, username,password)
var domainName = "domain1";
var groups = ['dg1', "dg2"]

createDomain()//直接创建Domain
//reCreateDomain()//重建Domain

function createDomain() {
    try{
        domain = db.getDomain(domainName)
        println(domainName + "is exist")
    }catch (e) {
        if( -214 == e){
            db.createDomain(domainName,groups,{AutoSplit:true})
        }
    }
}
function reCreateDomain() {
    try{
        db.dropDomain(domainName)
        db.createDomain(domainName,groups,{AutoSplit:true})
    }catch (e) {
        if( -214 == e){
            db.createDomain(domainName,groups,{AutoSplit:true})
        }
    }
}

