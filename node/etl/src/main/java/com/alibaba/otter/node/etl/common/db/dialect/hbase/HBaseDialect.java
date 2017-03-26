package com.alibaba.otter.node.etl.common.db.dialect.hbase;

import org.apache.ddlutils.model.Table;
import org.apache.hadoop.hbase.client.Connection;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;

public class HBaseDialect  extends AbstraNoSQLDialect  {

	private Connection conn;
	
	private NoSqlTemplate nosqlTemplate;
	
	public HBaseDialect(Connection dbconn, String databaseName, int databaseMajorVersion,
			int databaseMinorVersion){
		this.conn=dbconn;
		this.databaseName = databaseName;
		this.databaseMajorVersion = databaseMajorVersion;
		this.databaseMinorVersion = databaseMinorVersion;
		nosqlTemplate=new HBaseTemplate(conn);
	}
	

	
	
	@Override
	public boolean isEmptyStringNulled() {
		return true;
	}

	@Override
	public Connection getJdbcTemplate() {
		return conn;
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
