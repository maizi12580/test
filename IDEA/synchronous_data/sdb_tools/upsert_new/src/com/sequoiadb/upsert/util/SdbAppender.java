package com.sequoiadb.upsert.util;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Priority;

/**
 * 实现按指定级别输出日志到不同文件
 * @author yang
 */
public class SdbAppender extends FileAppender {

	@Override
	public boolean isAsSevereAsThreshold(Priority priority) {
		return this.getThreshold().equals(priority);
	}
}
