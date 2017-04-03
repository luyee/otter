package com.alibaba.otter.node.etl.common.db.dialect.metaq;

import org.apache.ddlutils.model.Table;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;

public class MetaQDialect extends AbstraNoSQLDialect {

	@Override
	public boolean isEmptyStringNulled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public <T> T getJdbcTemplate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getSqlTemplate() {
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

}
