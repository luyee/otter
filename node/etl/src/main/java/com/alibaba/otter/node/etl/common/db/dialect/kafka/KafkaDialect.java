package com.alibaba.otter.node.etl.common.db.dialect.kafka;

import org.apache.ddlutils.model.Table;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;

public class KafkaDialect extends AbstraNoSQLDialect {

	@SuppressWarnings("rawtypes")
	private Producer producer=null;
	private NoSqlTemplate nosqlTemplate;
	
	@SuppressWarnings("rawtypes")
	public KafkaDialect(Producer dbconn, String databaseName, int databaseMajorVersion,
			int databaseMinorVersion) {
		this.producer = dbconn;
		this.databaseName = databaseName;
		this.databaseMajorVersion = databaseMajorVersion;
		this.databaseMinorVersion = databaseMinorVersion;
		nosqlTemplate=new KafkaTemplate(producer); 
	}
	
	@Override
	public boolean isEmptyStringNulled() {
		return false;
	}

	public Producer getJdbcTemplate() {
		return producer;
	}

	@Override
	public Table findTable(String schema, String table) {
		return null;
	}

	@Override
	public Table findTable(String schema, String table, boolean useCache) {
		return null;
	}

	@Override
	public void reloadTable(String schema, String table) {
		
	}

	@Override
	public void destory() {
	}



	@SuppressWarnings("unchecked")
	@Override
	public NoSqlTemplate getSqlTemplate() {
		return nosqlTemplate;
	}

	


	


}
