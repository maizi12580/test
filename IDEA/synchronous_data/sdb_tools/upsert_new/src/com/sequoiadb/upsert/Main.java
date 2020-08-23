package com.sequoiadb.upsert;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.util.JSON;

import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;

public class Main {
	
	
	 
	public static void main(String[] args) throws InterruptedException {
		

			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 2500000; i < 3000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b555999999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 3000000; i < 3500000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b555999999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
		
		
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 3500000; i < 4000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b555999999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 4000000; i < 45000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b55565999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 4500000; i < 5000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b55544999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 5000000; i < 5500000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b55445999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 55000000; i < 6000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'444"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
		
		
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 6500000; i < 7000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b5535999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 7000000; i < 7500000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b553335999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			
			
			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 7500000; i < 8000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b555933399b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 8000000; i < 8500000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b553335999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();

			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 8500000; i < 8000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b555933399b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
		
		
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 9000000; i < 9500000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b5553333999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					Sequoiadb  db=new  Sequoiadb("21.96.19.162:18810", null,null);
					for (int i = 9500000; i < 10000000; i++) {
						String aa="{agt_num:'bb"+i+"'}";
						BSONObject bsonObject=(BSONObject) JSON.parse(aa);
						
					    String set = "{$set:{agt_modif_num:'b5553333999b"+i+"'}}";
					    BSONObject setBsonObject = (BSONObject) JSON.parse(set);
					    
					    String hint = "{'':'agt_num_indx'}";
					    BSONObject hintBsonObject = (BSONObject) JSON.parse(hint);
						
						db.getCollectionSpace("foo").getCollection("bar").upsert(bsonObject ,setBsonObject, hintBsonObject);
					
					}
					db.disconnect();
					
				}
			}).start();
			
			
			
			Thread.sleep(10000L);
			
			
			
			
		
		
	}
}
