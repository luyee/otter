package com.alibaba.otter.node.etl.common.db.dialect;

import java.util.List;

import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.lob.LobHandler;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import com.alibaba.otter.node.etl.common.datasource.DataSourceService;
import com.google.common.cache.LoadingCache;

public abstract class AbstraNoSQLDialect implements DbDialect {

	protected static final Logger logger = LoggerFactory.getLogger(AbstraNoSQLDialect.class);
	protected int databaseMajorVersion;
	protected int databaseMinorVersion;
	protected String databaseName;
	protected DataSourceService dataSourceService;
	protected LoadingCache<List<String>, Table> tables;

	@Override
	public String getDefaultSchema() {
		return null;
	}

	@Override
	public String getDefaultCatalog() {
		return null;
	}

	@Override
	public boolean isCharSpacePadded() {
		return false;
	}

	@Override
	public boolean isCharSpaceTrimmed() {
		return false;
	}

	@Override
	public boolean isDRDS() {
		return false;
	}
	

	@Override
	public String getShardColumns(String schema, String table) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TransactionTemplate getTransactionTemplate() {
		return null;
	}

	public boolean isNoSqlDB() {
		return true;
	}

	@Override
	public LobHandler getLobHandler() {
		return null;
	}

	@Override
	public boolean isSupportMergeSql() {
		return false;
	}

	public String getName() {
		return databaseName;
	}

	public int getMajorVersion() {
		return databaseMajorVersion;
	}

	@Override
	public int getMinorVersion() {
		return databaseMinorVersion;
	}

	public String getVersion() {
		return databaseMajorVersion + "." + databaseMinorVersion;
	}
}
