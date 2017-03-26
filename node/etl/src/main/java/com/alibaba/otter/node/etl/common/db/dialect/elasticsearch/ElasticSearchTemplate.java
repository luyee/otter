package com.alibaba.otter.node.etl.common.db.dialect.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventColumn;
import com.alibaba.otter.shared.etl.model.EventData;

/**
 * elasticSearch执行实现
 * 
 * @author admin
 *
 */
public class ElasticSearchTemplate implements NoSqlTemplate {
	private Client client = null;
	protected static final Logger logger = LoggerFactory.getLogger(ElasticSearchTemplate.class);

	public ElasticSearchTemplate(Client dbconn) {
		this.client = dbconn;
		
	}

	@SuppressWarnings({ "rawtypes", "unused" })
	private Map getRecord(String schema, String table, String id) {
		GetResponse response = client.prepareGet(schema, table, id).get();
		return response.getSourceAsMap();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map convertMap(EventData event) {
		Map recordM = new HashMap();
		for (EventColumn column : event.getKeys()) {
			if (StringUtils.isNotEmpty(column.getColumnValue())) {
				recordM.put(column.getColumnName(), column.getColumnValue());
			}
		}
		for (EventColumn column : event.getColumns()) {
			if (StringUtils.isNotEmpty(column.getColumnValue())) {
				recordM.put(column.getColumnName(), column.getColumnValue());
			}
		}
		return recordM;
	}

	@Override
	public List<EventData> batchEventDatas(List<EventData> events) {
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (EventData event : events) {
			String pkVal = SqlUtils.getPriKey(event);
			event.setExeResult(pkVal);
			if (event.getEventType().isDelete()) {// 删除
				bulkRequestBuilder.add(new DeleteRequest(event.getSchemaName(), event.getTableName(), pkVal));
			} else if (event.getEventType().isUpdate()) {
				bulkRequestBuilder.add(
						new UpdateRequest(event.getSchemaName(), event.getTableName(), pkVal).doc(convertMap(event)));
			} else if (event.getEventType().isInsert()) {
				bulkRequestBuilder.add(
						new IndexRequest(event.getSchemaName(), event.getTableName(), pkVal).source(convertMap(event)));
			}
		}
		// if (bulkRequestBuilder.numberOfActions()>1000){
		BulkResponse response = bulkRequestBuilder.execute().actionGet();
		for (BulkItemResponse item : response.getItems()) {
			if (item.isFailed()) {
				EventData event = getEventData(events, item.getId());
				if (event != null) {
					event.setExeResult("失败：id [" + item.getId() + "], message [" + item.getFailureMessage() + "]");
				}
				logger.error("失败：id [" + item.getId() + "], message [" + item.getFailureMessage() + "]");
			}
			// }
		}
		return events;
	}

	private EventData getEventData(List<EventData> events, String pk) {
		for (EventData event : events) {
			if (pk.equalsIgnoreCase(event.getExeResult())) {
				return event;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public EventData insertEventData(EventData event) {
		String pkVal = SqlUtils.getPriKey(event);
		event.setExeResult(pkVal);
		if (event.getEventType().isInsert()) {
			IndexResponse response = client.prepareIndex(event.getSchemaName(), event.getTableName(), pkVal)
					.setSource(convertMap(event)).get();
			if (!response.isCreated()) {
				logger.error("记录ID：" + pkVal + "  建立失败！");
				event.setExeResult("失败:记录ID：" + pkVal + "  建立失败！");
				return event;
			}
		}
		return null;
	}

	@Override
	public EventData updateEventData(EventData event) {
		String pkVal = SqlUtils.getPriKey(event);
		if (event.getEventType().isUpdate()) {
			UpdateResponse updateResponse = client.prepareUpdate(event.getSchemaName(), event.getTableName(), pkVal)
					.setDoc(convertMap(event)).get();
			if (!updateResponse.isCreated()) {
				logger.error("记录ID：" + pkVal + "  更新失败！");
				event.setExeResult("失败: 记录ID：" + pkVal + "  更新失败！");
				return event;
			}
		}
		return null;
	}

	@Override
	public EventData deleteEventData(EventData event) {
		String pkVal = SqlUtils.getPriKey(event);
		if (event.getEventType().isDelete()) {// 删除
			DeleteResponse response = client.prepareDelete(event.getSchemaName(), event.getTableName(), pkVal).get();
			if (!response.isFound()) {
				logger.error("记录ID：" + pkVal + "  删除失败！");
				event.setExeResult("失败:记录ID：" + pkVal + "  删除失败！");
				return event;
			}
		}
		return null;
	}

	// private String getMapping(EventData event){
	// StringBuffer sb=new StringBuffer();
	// sb.append("{");
	// sb.append(" \"CLIENTLOG\":{");
	// sb.append(" \"_all\":{");
	// sb.append(" \"enabled\":false");
	// sb.append(" },");
	// sb.append(" \"_source\":{");
	// sb.append(" \"enabled\":true");
	// sb.append(" },");
	// sb.append(" \"properties\":{");
	// int i=0;
	// for(EventColumn column:event.getColumns()){
	// if (i==0){
	// sb.append(" \""+column.getColumnName()+"\":{");
	// }else{
	// sb.append(" ,\""+column.getColumnName()+"\":{");
	// }
	// if (column.getColumnType() == Types.TIME || column.getColumnType() ==
	// Types.TIMESTAMP || column.getColumnType() == Types.DATE){
	// sb.append(" \"type\":\"date\",");
	// sb.append(" \"format\":\"YYYY-MM-dd HH:mm:ss\",");
	// }
	// sb.append(" \"store\":\"false\",");
	// sb.append(" \"doc_values\":\"true\",");
	// sb.append(" \"index\":\"not_analyzed\"");
	// sb.append(" }");
	// i++;
	// }
	//
	// sb.append( "LOGID":{");
	// sb.append( "type":"long",");
	// sb.append( "store":"false",");
	// sb.append( "doc_values":"true",");
	// sb.append( "index":"not_analyzed"");
	// sb.append( },");
	// sb.append( "USERID":{");
	// sb.append( "type":"string",");
	// sb.append( "store":"false",");
	// sb.append( "doc_values":"true",");
	// sb.append( "index":"not_analyzed"");
	// sb.append(" }");
	// sb.append(" }");
	// sb.append(" }");
	// sb.append("}");
	// return sb.toString();
	// }

	@Override
	public EventData createTable(EventData event) {
		IndicesExistsResponse response = client.admin().indices().prepareExists(event.getDdlSchemaName()).execute()
				.actionGet();
		if (!response.isExists()) {
			// 建立索引
			CreateIndexResponse creaResp = client.admin().indices().prepareCreate(event.getDdlSchemaName())
					.setSettings(Settings.settingsBuilder().put("number_of_shards", 20).put("number_of_replicas", 0))
					.execute().actionGet();
			if (!creaResp.isAcknowledged()) {
				logger.error("建立索引失败:" + creaResp.toString());
				event.setExeResult("失败:建立索引:" + creaResp.toString());
				return event;
			}
			// 建立type和mapping
			PutMappingRequest mapping = Requests.putMappingRequest(event.getDdlSchemaName()).type(event.getTableName());
			PutMappingResponse respMap = client.admin().indices().putMapping(mapping).actionGet();
			if (!respMap.isAcknowledged()) {
				logger.error("建立表失败:" + respMap.toString());
				event.setExeResult("失败:建立表:" + respMap.toString());
				return event;
			} else {
				logger.debug("新建索引：" + event.getDdlSchemaName() + "下的type:" + event.getTableName() + " 成功！");
			}
		} else {

			TypesExistsResponse typeResp = client.admin().indices().prepareTypesExists(event.getDdlSchemaName())
					.setTypes(event.getTableName()).execute().actionGet();
			if (!typeResp.isExists()) {
				// 建立type和mapping
				PutMappingRequest mapping = Requests.putMappingRequest(event.getDdlSchemaName())
						.type(event.getTableName());
				PutMappingResponse respMap = client.admin().indices().putMapping(mapping).actionGet();
				if (!respMap.isAcknowledged()) {
					logger.error("建立表失败:" + respMap.toString());
					event.setExeResult("失败:建立表:" + respMap.toString());
					return event;
				} else {
					logger.debug("新建索引：" + event.getDdlSchemaName() + "下的type:" + event.getTableName() + " 成功！");
				}
			}
		}
		return event;
	}

	@Override
	public EventData alterTable(EventData event) {
		event.setExeResult("elasticsearch不支持修改表结构!");
		logger.info("elasticsearch不支持修改表结构!");
		return event;
	}

	@Override
	public EventData eraseTable(EventData event) {
		logger.info("elasticsearch不支持删除表!");
		event.setExeResult("elasticsearch不支持删除表!");
		return event;
	}

	@Override
	public EventData truncateTable(EventData event) {
		logger.info("保护数据需求，elasticsearch不支持清空表数据!");
		event.setExeResult("保护数据需求，elasticsearch不支持清空表数据!");
		return event;
	}

	@Override
	public EventData renameTable(EventData event) {
		logger.info("保护数据需求，elasticsearch不删除原表，仅仅建立新表!");
		createTable(event);
		return event;
	}

	@Override
	public void distory() throws ConnClosedException {
		
	}

}
