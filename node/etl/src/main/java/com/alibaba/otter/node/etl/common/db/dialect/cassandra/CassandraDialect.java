package com.alibaba.otter.node.etl.common.db.dialect.cassandra;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.springframework.util.Assert;

import com.alibaba.otter.node.etl.common.db.dialect.AbstraNoSQLDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class CassandraDialect extends AbstraNoSQLDialect {

	private Cluster client = null;
	protected LoadingCache<List<String>, Table> tables;
	private NoSqlTemplate nosqlTemplate;
	
	public CassandraDialect(Cluster dbconn, String databaseName, int databaseMajorVersion, int databaseMinorVersion) {
		this.client = dbconn;
		this.databaseName = databaseName;
		this.databaseMajorVersion = databaseMajorVersion;
		this.databaseMinorVersion = databaseMinorVersion;
		initTables(client);
		this.nosqlTemplate=new CassandraTemplate(client);
	}

	private void initTables(final Cluster cluster) {
		// soft引用设置，避免内存爆了
		this.tables = CacheBuilder.newBuilder().maximumSize(1000)
				.removalListener(new RemovalListener<List<String>, Table>() {
					@Override
					public void onRemoval(RemovalNotification<List<String>, Table> paramRemovalNotification) {
						logger.warn("Eviction For Table:" + paramRemovalNotification.getValue());
					}
				}).build(new CacheLoader<List<String>, Table>() {
					@Override
					public Table load(List<String> names) throws Exception {
						Assert.isTrue(names.size() == 2);
						try {
							Table table = readTable(names.get(0), names.get(1));
							if (table == null) {
								throw new NestableRuntimeException(
										"no found table [" + names.get(0) + "." + names.get(1) + "] , pls check");
							} else {
								return table;
							}
						} catch (Exception e) {
							throw new NestableRuntimeException(
									"find table [" + names.get(0) + "." + names.get(1) + "] error", e);
						}
					}

				});
	}

	private Table readTable(String schemaName, String tableName) {
		Session session = client.connect();
		BoundStatement bindStatement = session
				.prepare(
						"select keyspace_name,table_name,comment from system_schema.tables where keyspace_name=? and table_name=?")
				.bind(schemaName, tableName);
		com.datastax.driver.core.ResultSet tabresultSet = session.execute(bindStatement);
		Iterator<Row> tabiterator = tabresultSet.iterator();
		Table table = new Table();
		if (tabiterator.hasNext()) {
			Row tabrow = tabiterator.next();
			table = new Table();
			table.setName(tableName);
			table.setType("cassandra");
			table.setCatalog(schemaName);
			table.setSchema(schemaName);
			table.setDescription(tabrow.getString("comment"));
			bindStatement = session
					.prepare(
							"select keyspace_name,table_name,column_name,type,kind from system_schema.columns where keyspace_name=? and table_name=?")
					.bind(schemaName, tableName);
			com.datastax.driver.core.ResultSet resultSet = session.execute(bindStatement);
			Iterator<Row> iterator = resultSet.iterator();
			while (iterator.hasNext()) {
				Row row = iterator.next();
				Column pkColumn = new Column();
				pkColumn.setJavaName(row.getString("column_name"));
				pkColumn.setName(row.getString("column_name"));
				pkColumn.setType(row.getString("type"));
				if (row.getString("kind").equalsIgnoreCase("partition_key")) {
					pkColumn.setPrimaryKey(true);
					pkColumn.setRequired(true);
				}
				table.addColumn(pkColumn);
			}
		}
		return table;
	}

	@Override
	public boolean isEmptyStringNulled() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Cluster getJdbcTemplate() {
		return this.client;
	}

	public Table findTable(String schema, String table, boolean useCache) {
		List<String> key = Arrays.asList(schema, table);
		if (useCache == false) {
			tables.invalidate(key);
		}
		try {
			return tables.get(key);
		} catch (ExecutionException e) {
			return null;
		}
	}

	public Table findTable(String schema, String table) {
		return findTable(schema, table, true);
	}

	@Override
	public void reloadTable(String schema, String table) {
		if (StringUtils.isNotEmpty(table)) {
			tables.invalidateAll(Arrays.asList(schema, table));
		} else {
			// 如果没有存在表名，则直接清空所有的table，重新加载
			tables.invalidateAll();
			tables.cleanUp();
		}
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
