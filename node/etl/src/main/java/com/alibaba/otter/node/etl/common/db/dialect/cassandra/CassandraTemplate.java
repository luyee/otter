package com.alibaba.otter.node.etl.common.db.dialect.cassandra;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.NestableRuntimeException;
import org.apache.ddlutils.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.alibaba.otter.node.etl.common.db.dialect.AbstractDbDialect;
import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.common.utils.meta.DdlUtils;
import com.alibaba.otter.shared.common.utils.meta.DdlUtilsFilter;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class CassandraTemplate implements NoSqlTemplate {

	private Cluster client;
	
	protected static final Logger      logger = LoggerFactory.getLogger(CassandraTemplate.class);
	
	protected LoadingCache<String, Session> sessions;
	
	public CassandraTemplate(Cluster client) {
		this.setClient(client);
		initSessions();
	}
	
	private void initSessions(){
		com.google.common.cache.Cache<String, Table> mapMaker = CacheBuilder.newBuilder().maximumSize(1000).softValues()
        		.removalListener(new RemovalListener<String, Table>(){
			@Override
			public void onRemoval(RemovalNotification<String, Table> paramRemovalNotification) {
				logger.warn("Eviction For Table:" + paramRemovalNotification.getValue());
			}}).build();

        this.sessions =CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<String, Session>(){
			@Override
			public Session load(String name) throws Exception {
                try {
                	return client.connect();
                } catch (Exception e) {
                    throw new NestableRuntimeException("connect to cassandra error",e);
                }
			}});
	}

	@Override
	public List<EventData> batchEventDatas(List<EventData> events) {
		Session session=null;
		try{
			BatchStatement batchStatement = new BatchStatement();
			batchStatement.setConsistencyLevel(ConsistencyLevel.ONE);
			for(EventData event:events){
				if (session==null ||session.isClosed()){
					if (StringUtils.isEmpty(event.getSchemaName())){
						session=sessions.get("cassandra");
					}else{
						session=sessions.get(event.getSchemaName());
					}
				}
				if (event.getEventType().isInsert()){
					String[] columns=new String[event.getColumns().size()+event.getKeys().size()];
					int id=0;
					Object[] values=new Object[event.getColumns().size()+event.getKeys().size()];
					for(EventColumn column:event.getColumns()){
						columns[id]=column.getColumnName();
						values[id]=SqlUtils.stringToSqlValue(column.getColumnValue(),column.getColumnType(),false,false);
						id++;
					}
					for(EventColumn column:event.getKeys()){
						columns[id]=column.getColumnName();
						values[id]=SqlUtils.stringToSqlValue(column.getColumnValue(),column.getColumnType(),false,false);
						id++;
					}
					batchStatement.add(QueryBuilder.insertInto(event.getSchemaName(), event.getTableName())
							.values(columns, values));
				}else if (event.getEventType().isDelete()){
					if (event.getKeys().size()>1){
						List<String> columnNames=new ArrayList<String>();
						List columnValues=new ArrayList();
						for(EventColumn column:event.getKeys()){
							columnNames.add(column.getColumnName());
							columnValues.add(SqlUtils.stringToSqlValue(column.getColumnValue(),column.getColumnType(),false,false));
						}
						batchStatement.add(QueryBuilder.delete().from(event.getSchemaName(),event.getTableName()).where(QueryBuilder.eq(columnNames,columnValues)));
					}else{
						EventColumn column=event.getKeys().get(0);
						batchStatement.add(QueryBuilder.delete().from(event.getSchemaName(),event.getTableName()).where(QueryBuilder.eq(column.getColumnName(),SqlUtils.stringToSqlValue(column.getColumnValue(),column.getColumnType(),false,false))));
					}
				}else if (event.getEventType().isUpdate()){
//					if (event.getOldKeys().equals(event.getKeys())){
						Update update=QueryBuilder.update(event.getSchemaName(),event.getTableName());
						for(EventColumn column:event.getColumns()){
							update.with(QueryBuilder.set(column.getColumnName(),SqlUtils.stringToSqlValue(column.getColumnValue(),column.getColumnType(),false,false)));
						}
						if (event.getKeys().size()>1){
							List<String> columnNames=new ArrayList<String>();
							List columnValues=new ArrayList();
							for(EventColumn column:event.getKeys()){
								columnNames.add(column.getColumnName());
								columnValues.add(SqlUtils.stringToSqlValue(column.getColumnValue(), column.getColumnType(),false, false) );
							}
							batchStatement.add(update.where(QueryBuilder.eq(columnNames,columnValues)));
						}else{
							EventColumn column=event.getKeys().get(0);
							batchStatement.add(update.where(QueryBuilder.eq(column.getColumnName(),SqlUtils.stringToSqlValue(column.getColumnValue(), column.getColumnType(),false, false))));
						}
						
//					}else{
//						Delete delete=QueryBuilder.delete().from(event.getSchemaName(),event.getTableName());
//						List<String> columnNames=new ArrayList<String>();
//						List<String> columnValues=new ArrayList<String>();
//						for(EventColumn column:event.getKeys()){
//							columnNames.add(column.getColumnName());
//							columnValues.add(column.getColumnValue());
//						}
//						batchStatement.add(delete.where(QueryBuilder.eq(columnNames,columnValues)));
//						String[] columns=new String[event.getColumns().size()+event.getKeys().size()];
//						int id=0;
//						Object[] values=new Object[event.getColumns().size()+event.getKeys().size()];
//						for(EventColumn column:event.getColumns()){
//							columns[id]=column.getColumnName();
//							values[id]=column.getColumnValue();
//							id++;
//						}
//						for(EventColumn column:event.getKeys()){
//							columns[id]=column.getColumnName();
//							values[id]=column.getColumnValue();
//							id++;
//						}
//						batchStatement.add(QueryBuilder.insertInto(event.getSchemaName(), event.getTableName())
//								.values(columns, values));
//					}
				}
			}
			session.execute(batchStatement);
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			//session.close();
		}
		return events;
	}

	@Override
	public EventData insertEventData(EventData event) {
		Session session=client.connect();
		try{
			if (event.getEventType().isInsert()){
				String[] columns=new String[event.getColumns().size()+event.getKeys().size()];
				int id=0;
				Object[] values=new Object[event.getColumns().size()+event.getKeys().size()];
				for(EventColumn column:event.getColumns()){
					columns[id]=column.getColumnName();
					values[id]=column.getColumnValue();
					id++;
				}
				for(EventColumn column:event.getKeys()){
					columns[id]=column.getColumnName();
					values[id]=column.getColumnValue();
					id++;
				}
				session.execute(QueryBuilder.insertInto(event.getSchemaName(), event.getTableName())
						.values(columns, values));
			}
			event.setExeResult("插入数据成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:插入数据出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData updateEventData(EventData event) {
		Session session=client.connect();
		try{
			if (event.getEventType().isUpdate()){
				if (event.getOldKeys().equals(event.getKeys())){
					Update update=QueryBuilder.update(event.getSchemaName(),event.getTableName());
					for(EventColumn column:event.getColumns()){
						update.with(QueryBuilder.set(column.getColumnName(),column.getColumnValue()));
					}
					List<String> columnNames=new ArrayList<String>();
					List<String> columnValues=new ArrayList<String>();
					for(EventColumn column:event.getKeys()){
						columnNames.add(column.getColumnName());
						columnValues.add(column.getColumnValue());
					}
					update.where(QueryBuilder.eq(columnNames,columnValues));
					session.execute(update);
				}else{
					Delete delete=QueryBuilder.delete().from(event.getSchemaName(),event.getTableName());
					List<String> columnNames=new ArrayList<String>();
					List<String> columnValues=new ArrayList<String>();
					for(EventColumn column:event.getOldKeys()){
						columnNames.add(column.getColumnName());
						columnValues.add(column.getColumnValue());
					}
					delete.where(QueryBuilder.eq(columnNames,columnValues));
					session.execute(delete);
					String[] columns=new String[event.getColumns().size()+event.getKeys().size()];
					int id=0;
					Object[] values=new Object[event.getColumns().size()+event.getKeys().size()];
					for(EventColumn column:event.getColumns()){
						columns[id]=column.getColumnName();
						values[id]=column.getColumnValue();
						id++;
					}
					for(EventColumn column:event.getKeys()){
						columns[id]=column.getColumnName();
						values[id]=column.getColumnValue();
						id++;
					}
					session.execute(QueryBuilder.insertInto(event.getSchemaName(), event.getTableName())
							.values(columns, values));
				}
			}
			event.setExeResult("更新数据成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:更新数据出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData deleteEventData(EventData event) {
		Session session=client.connect();
		try{
			if (event.getEventType().isDelete()){
				Delete delete=QueryBuilder.delete().from(event.getSchemaName(),event.getTableName());
				List<String> columnNames=new ArrayList<String>();
				List<String> columnValues=new ArrayList<String>();
				for(EventColumn column:event.getKeys()){
					columnNames.add(column.getColumnName());
					columnValues.add(column.getColumnValue());
				}
				delete.where(QueryBuilder.eq(columnNames,columnValues));
				session.execute(delete);
			}
			event.setExeResult("删除数据成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:删除数据出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData createTable(EventData event) {
		Session session=client.connect();
		try{
			if (event.getEventType().isDelete()){
				Delete delete=QueryBuilder.delete().from(event.getSchemaName(),event.getTableName());
				List<String> columnNames=new ArrayList<String>();
				List<String> columnValues=new ArrayList<String>();
				for(EventColumn column:event.getKeys()){
					columnNames.add(column.getColumnName());
					columnValues.add(column.getColumnValue());
				}
				delete.where(QueryBuilder.eq(columnNames,columnValues));
				session.execute(delete);
			}
			event.setExeResult("建表成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:建表出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData alterTable(EventData event) {
		event.setExeResult("cassandra 不支持修改表结构!");
		return event;
	}

	@Override
	public EventData eraseTable(EventData event) {
		Session session=client.connect(event.getSchemaName());
		try{
			if (event.getEventType().isDelete()){
				session.execute("DROP TABLE IF EXISTS  "+event.getTableName());
			}
			event.setExeResult("删除表成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:删除表出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData truncateTable(EventData event) {
		Session session=client.connect();
		try{
			if (event.getEventType().isDelete()){
				session.execute(QueryBuilder.truncate(event.getSchemaName(),event.getTableName()));
			}
			event.setExeResult("清空表成功！");
		}catch(Exception e){
			e.printStackTrace();
			event.setExeResult("失败:清空表出错，exception:"+e.getMessage());
		}finally{
			session.close();
		}
		return event;
	}

	@Override
	public EventData renameTable(EventData event) {
		event.setExeResult("cassandra 不支持修改表名称!");
		return event;
	}

	public Cluster getClient() {
		return client;
	}

	public void setClient(Cluster client) {
		this.client = client;
	}

	@Override
	public void distory() throws ConnClosedException {
		// TODO Auto-generated method stub
		
	}

}
