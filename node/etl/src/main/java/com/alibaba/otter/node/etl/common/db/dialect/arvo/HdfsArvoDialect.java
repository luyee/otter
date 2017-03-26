package com.alibaba.otter.node.etl.common.db.dialect.arvo;

import java.util.Map;

import org.apache.ddlutils.model.Table;
import org.apache.hadoop.fs.FileSystem;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.shared.etl.model.EventData;

public class HdfsArvoDialect  extends AbstraNoSQLDialect  {

	private NoSqlTemplate nosqlTemplate;
	
	public HdfsArvoDialect(FileSystem dbconn, String databaseName, int databaseMajorVersion, int databaseMinorVersion) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean isEmptyStringNulled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public FileSystem getJdbcTemplate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table findTable(String schema, String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table findTable(String schema, String table, boolean useCache) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reloadTable(String schema, String table) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destory() {
		// TODO Auto-generated method stub
		
	}

	private Map getRecord(String schema, String table,String id) {
		// TODO Auto-generated method stub
		return null;
	}

	private Map convertMap(EventData event) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getSqlTemplate() {
		// TODO Auto-generated method stub
		return null;
	}


}
