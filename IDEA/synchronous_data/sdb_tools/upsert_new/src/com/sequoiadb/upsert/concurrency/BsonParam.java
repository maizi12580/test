package com.sequoiadb.upsert.concurrency;

import com.sequoiadb.upsert.util.Utils;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.util.JSON;

/**
 * Created by yang on 2017/4/17.
 */
public class BsonParam {
    private String data;
    private BSONObject rule;
    private BSONObject cond;
    private BSONObject hint;

    public BsonParam(String data, BSONObject rule, BSONObject cond, BSONObject hint) {
        this.data = data;
        this.rule = rule;
        this.cond = cond;
        this.hint = hint;
    }

    public static BsonParam newInstance(String data, String[] conds) throws ClassCastException{
        BSONObject set = (BSONObject) JSON.parse(data);
        //移除 _id 字段
        set.removeField("_id");
        BSONObject rule = new BasicBSONObject("$set", set);
        BSONObject cond = new BasicBSONObject();
        for (String field : conds) {
            cond.put(field, set.get(field));
        }
        BSONObject hint = new BasicBSONObject("", Utils.getHint());
        return new BsonParam(data, rule, cond, hint);
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public BSONObject getRule() {
        return rule;
    }

    public void setRule(BSONObject rule) {
        this.rule = rule;
    }

    public BSONObject getCond() {
        return cond;
    }

    public void setCond(BSONObject cond) {
        this.cond = cond;
    }

    public BSONObject getHint() {
        return hint;
    }

    public void setHint(BSONObject hint) {
        this.hint = hint;
    }
}
