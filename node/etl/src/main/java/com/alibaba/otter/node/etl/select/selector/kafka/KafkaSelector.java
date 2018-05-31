package com.alibaba.otter.node.etl.select.selector.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.otter.node.etl.select.selector.Message;
import com.alibaba.otter.node.etl.select.selector.OtterSelector;
import com.alibaba.otter.shared.arbitrate.ArbitrateEventService;
import com.alibaba.otter.shared.arbitrate.model.MainStemEventData;
import com.alibaba.otter.shared.common.model.config.ConfigHelper;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.etl.model.EventData;

public class KafkaSelector implements OtterSelector<EventData> {
	
    private static final Logger     logger           = LoggerFactory.getLogger(KafkaSelector.class);
    
    private volatile boolean running = false;

	private  LinkedBlockingDeque<ConsumerRecord<String, String>> buffer = new LinkedBlockingDeque<ConsumerRecord<String, String>>();
	
	private KafkaConsumer<String, String> consumer =null ;
	
	private ArbitrateEventService arbitrateEventService;
	
	private String brokerServer;
	
	private String destination;
	
	private Pipeline pipeline;
	
	private String topic;
	
	private AtomicLong  batchId = new AtomicLong(0) ;
	
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    
    private ConcurrentHashMap<Long, ConsumerRecords<String, String>> recordsMap = new ConcurrentHashMap<Long, ConsumerRecords<String, String>>();

    private OffsetManager  offsetManager;
	
	public KafkaSelector(String brokerServer,String destination,Pipeline pipeline,String topic){
		this.brokerServer = brokerServer;
		this.destination = destination;
		this.pipeline =pipeline;
		this.topic = topic;
		this.offsetManager = new OffsetManager(destination+"_"+pipeline.getName());
		
	}
	
	private void singleMainStemStatus(){
		MainStemEventData mainStemData = new MainStemEventData();
        mainStemData.setPipelineId(this.pipeline.getId());
        mainStemData.setStatus(MainStemEventData.Status.OVERTAKE);
        arbitrateEventService.mainStemEvent().single(mainStemData);
	}
	
	
	private Map<String,String> getKafkaConsumerConfig(){
		Map<String,String > map = new HashMap<String, String>();
		map.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerServer);
		map.put(ConsumerConfig.GROUP_ID_CONFIG, "canal"+this.destination);
		map.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
		//map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		map.put(ConsumerConfig.GROUP_ID_CONFIG, this.pipeline.getId()+this.pipeline.getName());
		map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		logger.warn(String.format("ConsumerConfig: %s ", com.alibaba.fastjson.JSON.toJSONString(map)));
		
		return  map;
	}
	
	
	/**
	 * todo  结合consumer.pause/resume实现流控 
	 */
	@Override
	public void start() {
		if (running) {
			return;
		}
		consumer = new KafkaConsumer(getKafkaConsumerConfig());
		//设置topic
		consumer.subscribe(Arrays.asList(this.topic.split(",")),
				new OffsetTrackingRebalanceListener(consumer, offsetManager));
		
		this.singleMainStemStatus();
		running = true;
		
	}
	
	
	private Message<EventData> pollFromKafka(){
		ConsumerRecords<String, String> records = consumer.poll(1000);
		List<EventData> eventDatas = new ArrayList<EventData>();
		for (ConsumerRecord<String, String> consumerRecord : records) {
			
			
			logger.warn(String.format("selector value: %s", consumerRecord.value()));
	
			EventData eventData = JSON.parseObject(consumerRecord.value(), new TypeReference<EventData>() {});
			DataMediaPair dataMediaPair = ConfigHelper.findDataMediaPairBySourceName(pipeline,
					eventData.getSchemaName(),
					eventData.getTableName());
			DataMedia dataMedia = dataMediaPair.getSource();
			
			eventData.setTableId(dataMedia.getId());

			
			logger.warn("eventData.getTableName(): "+eventData.getTableName());
			eventDatas.add(eventData);
		}
		
		//更新下位点信息
		logger.warn("size: "+eventDatas.size());
		long msgBatchId = batchId.incrementAndGet();
		Message message= new Message(msgBatchId, eventDatas);
		recordsMap.put(msgBatchId, records);
		
		return message;
	}

	@Override
	public boolean isStart() {
		return running;
	}

	@Override
	public void stop() {
		if(!running){
			return;
		}
		consumer.close();
		running = false;
	}

	@Override
	public Message<EventData> selector() throws InterruptedException {
		return pollFromKafka();
	}

	@Override
	public List<Long> unAckBatchs() {
		return null;
	}

	@Override
	public void rollback(Long batchId) {
		ConsumerRecords<String, String> records = recordsMap.remove(batchId);
		logger.warn(String.format("rollback offset %d", batchId));

	}

	@Override
	public void rollback() {
		logger.warn("rollback happened! ");
		
	}

	@Override
	public void ack(Long batchId) {
		ConsumerRecords<String, String> records = recordsMap.remove(batchId);
		for (ConsumerRecord<String, String> consumerRecord : records) {
			offsetManager.saveOffsetInExternalStore(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
		}
		logger.warn(String.format("ack offset %d", batchId));
		
	}

	@Override
	public Long lastEntryTime() {
		return null;
	}


	public void setArbitrateEventService(ArbitrateEventService arbitrateEventService) {
		this.arbitrateEventService = arbitrateEventService;
	}
	

}
