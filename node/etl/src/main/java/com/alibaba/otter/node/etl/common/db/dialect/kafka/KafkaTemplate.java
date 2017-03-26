package com.alibaba.otter.node.etl.common.db.dialect.kafka;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.node.etl.common.db.dialect.NoSqlTemplate;
import com.alibaba.otter.node.etl.common.db.utils.SqlUtils;
import com.alibaba.otter.node.etl.load.exception.ConnClosedException;
import com.alibaba.otter.shared.etl.model.EventData;

public class KafkaTemplate implements NoSqlTemplate {

	@SuppressWarnings("rawtypes")
	private Producer producer = null;
	protected static final Logger logger = LoggerFactory.getLogger(KafkaTemplate.class);

	private ObjectMapper mapper=new ObjectMapper();
	@SuppressWarnings("rawtypes")
	public KafkaTemplate(Producer dbconn) {
		this.producer = dbconn;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private EventData sendMessage(EventData event) throws ConnClosedException {
		String pkVal = SqlUtils.getPriKey(event);
		try {
			event.setLoadDataTime(System.currentTimeMillis());
			if (event.getEventType().isDdl()){
				producer.send(new ProducerRecord(event.getSchemaName()+"_DDL",pkVal,mapper.writeValueAsString(event)),new EventCallBack(event));
			}
			producer.send(new ProducerRecord(event.getSchemaName(),pkVal,mapper.writeValueAsString(event)),new EventCallBack(event));
			logger.info("kafka消息发送完毕时间: event time:"+new Timestamp(event.getExecuteTime())+"  当前时间:"+new Timestamp(System.currentTimeMillis()));
		}catch(IllegalStateException ise){
			throw new ConnClosedException(ise.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
		}
		return event;
	}
	
	class EventCallBack implements Callback{
		
		private EventData event;
		
		public EventCallBack(EventData event){
			this.event=event;
		}
		
		@Override
		public void onCompletion(RecordMetadata metadata, Exception e) {
			if (metadata==null){
				logger.error(e.getMessage());
				event.setExeResult("失败：发送kafka消息 "+e.getMessage());
			}else{
				event.setExeResult("成功：发送kafka消息!");
			}
		}
		
	}
	
	@Override
	public List<EventData> batchEventDatas(List<EventData> events) throws ConnClosedException {
		for(EventData event:events){
			sendMessage(event);
		}
		return events;
	}

	@Override
	public EventData insertEventData(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData updateEventData(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData deleteEventData(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData createTable(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData alterTable(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData eraseTable(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData truncateTable(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public EventData renameTable(EventData event) throws ConnClosedException{
		return sendMessage(event);
	}

	@Override
	public void distory() throws ConnClosedException {
		
	}

}
