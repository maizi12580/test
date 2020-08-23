start();
function start(){
	var db = new Sdb("localhost",11810,"","")
	//清理的集合空间名称
	var css = []
	for(var i=0;i<=css.length;i++){
		var cs = css[i]
		var cl = cs + "_DOC"
		findFirstCL(db,cs,cl);
	}
}
function findFirstCL(db,cs,cl){

	//可以对时间自动解析
	var time = new Date()
	var day = ""
	if(time.getDate()<10){
		day = "0"+time.getDate()
	}else
		day = time.getDate()
	var date = time.getFullYear() + "-" + time.getMonth() + "-" + day
	println("Now Time:" + date)
	//筛选符合时间条件的文件夹,字段field
	var cursor = db.getCS(cs).getCL(cl).find({field:{$lte:{"$date":date}}});
	//对集群空间进行遍历
	while(cursor.next()){
        object = cursor.current().toObj();
		//获取文件夹的ID以及主键信息
		folderId = object["folderId"];
		var priKey = object["_id"]["$oid"]
		//通过文件夹ID对文件寻找
		findCL(db,folderId,cs);
		//删除文件夹
		db.getCS(cs).getCL(cl).remove({_id:{"$oid":priKey}},{"folderId":fileId},{"":"INDEX_NAME"})
		println("Now Remove Folder:"+priKey)
		println("End batch:")
    }
	cursor.close();
}
function findCL(db,id,cs){
	var fileCs = cs;
	var fileCl = cs + "_PART";
	var logCl = "";
	//对文件夹的文件进行便利
	var cursor =  db.getCS(fileCs).getCL(fileCl).find({field:id});
	while(cursor.next()){
		var object = cursor.current().toObj();
		//log的id
		var logId =  object["fileId"]["$oid"];
		//寻找log所在的位置
		var logCs = object["Name"] + "_" + object ["TIME"]
		var priKey = object["_id"]["$oid"]

		//删除对应的log大对象记录
		try {
			db.getCS(logCs).getCL(logCl).deleteLob(logId);
			//集合空间快照
			var snap5 = db.snapshot(5,{"Name":logCs})
			var collection = snap5.current().toObj().Collection
			if(collection == 1){
				var lobsid = db.getCS(logCs).getCL(logCl).listLobs()
				var lobnum = 1
				if(!lobsid.next())
					lobnum = 0
				if(lobnum == 0){
					db.dropCS(logCs)
				}
			}
		}catch (e) {
			if(e==-4){
				println("no log file")
			}else
				println(e)
		}
		//删除文件
		try{
			db.getCS(fileCs).getCL(fileCl).remove({_id:{"$oid":priKey}},{"fileId":"fileId"},{"":"$id"});
		}catch (e) {
			if(e==-4){
				println("no file")
			}else
				println(e)
		}

	}
	cursor.close();
}