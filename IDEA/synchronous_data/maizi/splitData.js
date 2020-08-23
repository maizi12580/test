var groups = ["dg1", "dg2"]
var db = new Sdb("localhost", 11810, "", "")
var cata = new Sdb("localhost", 11800)
var css = ["mainCS201801", "mainCS201802", "mainCS201803"]

for (var i = 0; i < css.length; i++) {
    var cs = css[i]
    //获取协调节点的空间集合信息
    var getcl = cata.SYSCAT.SYSCOLLECTIONSPACES.find({Name: cs}).current().toObj().Collection
    for (var m = 0; m < getcl.length; m++) {
        var cl = getcl[m].Name
        var tbl_name = cs + "." + cl
        println("begin cs.cl:" + tbl_name)
        var snap8 = db.snapshot(8, {Name: tbl_name})
        var groups_num = groups.length
        //分区余数和大小
        var remainder = 4096 % groups_num
        var interval = parseInt(4096 / groups_num)
        //开始区间
        var begin = groups_num - remainder
        CataInfo = snap8.current().toObj().CataInfo
        if (CataInfo[0].ID == 0 || CataInfo.length == 1) {
            var targerGroup = CataInfo[0].GroupName
            for (var i = 0; i < CataInfo.length; i++) {
                if (i > 0) {
                    sourceGroup = CataInfo[i].GroupName
                    lowB = CataInfo[i].LowBound[""]
                    upB = CataInfo[i].UpBound[""]
                    db.getCS(cs).getCL(cl).split(sourceGroup, targerGroup, {Partition: lowB}, {Partition: upB})
                    println(sourceGroup + "---" + lowB + "---" + upB)
                }
            }
            var snap8new = db.snapshot(8, {Name: tbl_name}).current().toObj().CataInfo
            var snap8_length = snap8new.length
            var snap8_group = snap8new[0].GroupName
            if (snap8_length == 1 && snap8_group == groups[0]) {
                var flag = 0
                for (var i = 0; i < groups_num; i++) {
                    if (i < begin) {
                        flag = flag + interval
                        if (i != 0) {
                            db.getCS(cs).getCL(cl).splitAsync(snap8_group, groups[i], {Partition: (flag - interval)}, {Partition: flag})
                        }
                    } else {
                        flag = flag + interval + 1
                        db.getCS(cs).getCL(cl).split(snap8_group, groups[i], {Partition: (flag - interval - 1)}, {Partition: flag})
                    }
                }
            }
        }
    }
}
