package com.alibaba.otter.node.etl.common.db.dialect.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

public class HBaseTemplate implements NoSqlTemplate {

	private Connection conn;
	public HBaseTemplate(Connection conn) {
		this.conn=conn;
	}

	@Override
	public List<EventData> batchEventDatas(List<EventData> events) throws ConnClosedException {
		Map<String,List<Put>> putBuffers=new java.util.concurrent.ConcurrentHashMap<String,List<Put>>();
		Map<String,List<Delete>> delBuffers=new java.util.concurrent.ConcurrentHashMap<String,List<Delete>>();
		
		for(EventData event:events){
			String pkVal = SqlUtils.getPriKey(event);
			if (event.getEventType().isInsert()||event.getEventType().isUpdate()){
				Put p = new Put(Bytes.toBytes(pkVal));
				for (EventColumn column : event.getColumns()) {
					if (StringUtils.isNotEmpty(column.getColumnValue())) {
						p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(column.getColumnName()), Bytes.toBytes(column.getColumnValue()));
					}
				}
				List<Put> puts=putBuffers.get(event.getTableName());
				if (puts==null){
					puts=new ArrayList<Put>();
					putBuffers.put(event.getTableName(),puts);
				}
				puts.add(p);
			}else if(event.getEventType().isDelete()){
				Delete del=new Delete(pkVal.getBytes());
				List<Delete> dels=delBuffers.get(event.getTableName());
				if (dels==null){
					dels=new ArrayList<Delete>();
					delBuffers.put(event.getTableName(),dels);
				}
				dels.add(del);
			}
		}
		try{
			for(Map.Entry<String,List<Put>> entry:putBuffers.entrySet()){
				Table tab = conn.getTable(TableName.valueOf(entry.getKey()));
				tab.put(entry.getValue());
			}
			for(Map.Entry<String,List<Delete>> entry:delBuffers.entrySet()){
				Table tab = conn.getTable(TableName.valueOf(entry.getKey()));
				tab.delete(entry.getValue());
			}
			putBuffers.clear();
			delBuffers.clear();
		}catch(IllegalArgumentException iae){
			throw new ConnClosedException(iae.getMessage());
		}catch(Exception e){
			e.printStackTrace();
		}
		return events;
	}

	@Override
	public EventData insertEventData(EventData event) throws ConnClosedException {
		if (event.getEventType().isInsert()||event.getEventType().isUpdate()) {
			String pkVal = null;
			for (EventColumn column : event.getKeys()) {
				if (pkVal == null) {
					pkVal = column.getColumnValue();
				} else {
					pkVal = pkVal + "_" + column.getColumnValue();
				}
			}
			Table tab =null;
			try {
				tab = conn.getTable(TableName.valueOf(event.getTableName()));
				Put p = new Put(Bytes.toBytes(pkVal));
				for (EventColumn column : event.getColumns()) {
					if (StringUtils.isNotEmpty(column.getColumnValue())) {
						p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(column.getColumnName()), Bytes.toBytes(column.getColumnValue()));
					}
				}
				tab.put(p);
				event.setExeResult("成功：新增 "+event.getTableName()+"--"+pkVal);
			}catch(IllegalArgumentException iae){
				throw new ConnClosedException(iae.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				event.setExeResult("失败：新增 "+event.getTableName()+"--"+pkVal+"  失败原因："+e.getMessage());
			}finally{
				if(tab!=null)
					try {
						tab.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				tab=null;
			}
		}
		return event;
	}

	@Override
	public EventData updateEventData(EventData event) throws ConnClosedException {
		return insertEventData(event);
	}

	@Override
	public EventData deleteEventData(EventData event) throws ConnClosedException {
		if (event.getEventType().isDelete()){
			String pkVal = null;
			for (EventColumn column : event.getKeys()) {
				if (pkVal == null) {
					pkVal = column.getColumnValue();
				} else {
					pkVal = pkVal + "_" + column.getColumnValue();
				}
			}
			Table tab =null;
			try {
				tab = conn.getTable(TableName.valueOf(event.getTableName()));
				Delete del=new Delete(pkVal.getBytes());
				tab.delete(del);
				event.setExeResult("成功：删除 "+event.getTableName()+"--"+pkVal);
			}catch(IllegalArgumentException iae){
				throw new ConnClosedException(iae.getMessage());
			} catch (IOException e) {
				e.printStackTrace();
				event.setExeResult("失败：删除 "+event.getTableName()+"--"+pkVal+"  失败原因："+e.getMessage());
			}finally{
				if(tab!=null)
					try {
						tab.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				tab=null;
			}
		}
		return null;
	}

	@Override
	public EventData createTable(EventData event) throws ConnClosedException{
		Admin admin=null;
		try {
			admin = conn.getAdmin();
			if (!admin.tableExists(TableName.valueOf(event.getTableName()))) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(event.getTableName())); 
	            tableDescriptor.addFamily(new HColumnDescriptor("cf")); 
				admin.createTable(tableDescriptor);
			}
			event.setExeResult("成功：建表 "+event.getTableName());
		}catch(IllegalArgumentException iae){
			throw new ConnClosedException(iae.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			event.setExeResult("失败：建表 "+event.getTableName()+" 失败原因："+e.getMessage());
		}
		return event;
	}

	@Override
	public EventData alterTable(EventData event) throws ConnClosedException{
		createTable(event);
		event.setExeResult("hbase不支持修改表名,仅仅新建表！");
		return event;
	}

	@Override
	public EventData eraseTable(EventData event) throws ConnClosedException{
		Admin admin=null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(event.getTableName()))) {
				admin.disableTable(TableName.valueOf(event.getTableName()));
				admin.deleteTable(TableName.valueOf(event.getTableName()));
			}
			event.setExeResult("成功：删除表 "+event.getTableName());
		}catch(IllegalArgumentException iae){
			throw new ConnClosedException(iae.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			event.setExeResult("失败：删除表 "+event.getTableName()+" 失败原因："+e.getMessage());
		}
		return event;
	}

	@Override
	public EventData truncateTable(EventData event) throws ConnClosedException{
		eraseTable(event);
		createTable(event);
		return event;
	}

	@Override
	public EventData renameTable(EventData event) throws ConnClosedException{
		createTable(event);
		event.setExeResult("hbase不支持修改表名,仅仅新建表！");
		return event;
	}

	@Override
	public void distory() throws ConnClosedException {
		// TODO Auto-generated method stub
		
	}

}
